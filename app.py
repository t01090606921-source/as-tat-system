import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
from datetime import datetime, timedelta

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (고속 엔진)")

# [데이터 정제 함수]
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].replace(" ", "").strip().upper()

def to_pure_date(val):
    try: return pd.to_datetime(val).date()
    except: return None

# --- 2. 사이드바 (DB 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 DB 전체 수량 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("현재 저장된 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if "delete_mode" not in st.session_state: st.session_state.delete_mode = False
    if not st.session_state.delete_mode:
        if st.button("💣 데이터 전체 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True; st.rerun()
    else:
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정", use_container_width=True):
                # 대용량 삭제를 위한 루프
                msg = st.empty()
                while True:
                    fetch = supabase.table("as_history").select("id").limit(1000).execute()
                    ids = [r['id'] for r in fetch.data]
                    if not ids: break
                    supabase.table("as_history").delete().in_("id", ids).execute()
                    msg.warning("🗑️ 데이터 삭제 중...")
                st.session_state.delete_mode = False; st.success("삭제 완료"); st.rerun()
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일", type=['xlsx', 'csv'], key="m_final_v1")
    if m_file and st.button("🔄 마스터 로드"):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {"업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()} for _, row in m_df.iterrows()}
            st.success(f"✅ 마스터 로드 완료 ({len(st.session_state.master_lookup):,}건)")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리
with tab1:
    st.subheader("📥 AS 입고 (CSV 전용)")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_final_v1")
    if i_file and st.button("🚀 입고 시작"):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            try:
                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue
                as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs = []
                for _, row in as_in.iterrows():
                    mat_no = sanitize_code(row.iloc[3])
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    recs.append({
                        "압축코드": sanitize_code(row.iloc[7]), "자재번호": mat_no, "자재명": str(row.iloc[4]).strip(),
                        "규격": str(row.iloc[5]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"), "입고일": str(to_pure_date(row.iloc[1])), "상태": "출고 대기"
                    })
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute(); recs = []
                if recs: supabase.table("as_history").insert(recs).execute()
                st.success("✅ 입고 완료")
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (로딩 멈춤 방지: 서버 사이드 매칭)
with tab2:
    st.subheader("📤 AS 출고 처리 (Direct 엔진)")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_final_v1")
    if o_file and st.button("🚀 출고 반영 시작"):
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            # 디지타스 우선 처리 정렬
            as_out['is_digitas'] = as_out.iloc[:, 15].astype(str).str.contains("주식회사디지타스")
            as_out = as_out.sort_values(by='is_digitas', ascending=False)

            ui_msg = st.empty()
            success_count = 0
            
            # 매칭 로직: 엑셀의 각 행에 대해 DB에 직접 물어봄 (메모리 부하 제로)
            for i, (idx, row) in enumerate(as_out.iterrows()):
                code = sanitize_code(row.iloc[10])
                out_date = str(to_pure_date(row.iloc[6]))
                dest = str(row.iloc[15]).strip()
                
                # 목적지에 따라 DB에서 매칭될 행 1개만 조회
                if dest == "주식회사디지타스":
                    # 디지타스 출고 기록이 아직 없는 가장 오래된 건 찾기
                    res = supabase.table("as_history").select("id").eq("압축코드", code).is_("디지타스_출고일", "null").neq("상태", "벤더 출고 완료").order("입고일").limit(1).execute()
                else:
                    # 벤더 출고: 디지타스 기록이 있는 건 우선 탐색
                    res = supabase.table("as_history").select("id").eq("압축코드", code).not_.is_("디지타스_출고일", "null").neq("상태", "벤더 출고 완료").order("입고일").limit(1).execute()
                    if not res.data: # 없으면 입고 대기건 탐색
                        res = supabase.table("as_history").select("id").eq("압축코드", code).eq("상태", "출고 대기").order("입고일").limit(1).execute()

                if res.data:
                    target_id = res.data[0]['id']
                    if dest == "주식회사디지타스":
                        upd = {"디지타스_출고일": out_date, "상태": "디지타스 출고"}
                    else:
                        upd = {"벤더_출고지": dest, "벤더_출고일": out_date, "상태": "벤더 출고 완료"}
                    
                    supabase.table("as_history").update(upd).eq("id", target_id).execute()
                    success_count += 1
                
                if (i + 1) % 10 == 0 or (i + 1) == len(as_out):
                    ui_msg.info(f"🔄 처리 중... ({i+1}/{len(as_out)} 건 완료)")

            ui_msg.success(f"✅ 총 {success_count}건의 출고 단계가 정상 반영되었습니다.")
        except Exception as e: st.error(f"오류 발생: {e}")

# [TAB 3] 리포트 생성 (기간 필터링 기능 탑재)
with tab3:
    st.subheader("📈 기간별 분석 리포트")
    c1, c2 = st.columns(2)
    with c1: s_d = st.date_input("시작일", datetime.now() - timedelta(days=30))
    with c2: e_d = st.date_input("종료일", datetime.now())

    if st.button("📊 선택 기간 리포트 생성"):
        all_d, offset = [], 0
        status = st.empty()
        
        try:
            while True:
                res = supabase.table("as_history").select("*").gte("입고일", str(s_d)).lte("입고일", str(e_d)).range(offset, offset+1000).order("입고일").execute()
                if not res.data: break
                all_d.extend(res.data); offset += len(res.data)
                status.info(f"📥 데이터를 추출하는 중... ({offset:,}건 완료)")
                if len(res.data) < 1000: break
            
            if all_d:
                df = pd.DataFrame(all_d)
                # 날짜 및 TAT 계산
                in_dt = pd.to_datetime(df['입고일'], errors='coerce')
                dg_dt = pd.to_datetime(df['디지타스_출고일'], errors='coerce')
                vn_dt = pd.to_datetime(df['벤더_출고일'], errors='coerce')
                
                # TAT: 벤더일자 우선, 없으면 디지타스 기준
                df['TAT'] = (vn_dt - in_dt).dt.days
                df.loc[df['TAT'].isna(), 'TAT'] = (dg_dt - in_dt).dt.days
                
                # 리포트용 포맷팅
                df['입고일'] = in_dt.dt.strftime('%Y-%m-%d')
                df['디지타스_출고일'] = dg_dt.dt.strftime('%Y-%m-%d').fillna("-")
                df['벤더_출고일'] = vn_dt.dt.strftime('%Y-%m-%d').fillna("-")
                df['TAT'] = df['TAT'].fillna("-")
                df['벤더_출고지'] = df['벤더_출고지'].fillna("-")
                
                cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
                
                status.success(f"✅ {len(df):,}건의 리포트가 생성되었습니다.")
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                    df[cols].to_excel(wr, index=False)
                st.download_button(f"📥 {s_d}_{e_d}_리포트 다운로드", output.getvalue(), f"AS_Report_{s_d}_{e_d}.xlsx")
                st.dataframe(df[cols].head(100))
            else:
                st.warning("해당 기간에 조회된 데이터가 없습니다.")
        except Exception as e: st.error(f"리포트 생성 오류: {e}")
