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
st.title("📊 AS TAT 통합 관리 시스템 (최종 완결본)")

# [데이터 정제 함수]
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].replace(" ", "").strip().upper()

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).date()
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
                msg = st.empty()
                while True:
                    fetch = supabase.table("as_history").select("id").limit(1000).execute()
                    ids = [r['id'] for r in fetch.data]
                    if not ids: break
                    supabase.table("as_history").delete().in_("id", ids).execute()
                    msg.warning("🗑️ 데이터 소거 중...")
                st.session_state.delete_mode = False; st.success("삭제 완료"); st.rerun()
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 고속 출고(정밀 매칭)", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일(CSV 권장)", type=['csv', 'xlsx'], key="m_v_final")
    if m_file and st.button("🔄 마스터 로드"):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {"업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()} for _, row in m_df.iterrows()}
            st.success("✅ 마스터 로드 완료")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리
with tab1:
    st.subheader("📥 AS 입고 (CSV 전용)")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_v_final")
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
                    if len(recs) >= 500: supabase.table("as_history").insert(recs).execute(); recs = []
                if recs: supabase.table("as_history").insert(recs).execute()
                st.success("✅ 입고 완료")
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (정밀 선입선출 + 벌크 엔진)
with tab2:
    st.subheader("📤 AS 출고 처리 (교차 매칭 방지 엔진)")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="o_v_final")
    if o_file and st.button("🚀 출고 데이터 분석 및 반영"):
        try:
            for enc in ['utf-8-sig', 'cp949']:
                try: o_file.seek(0); df_out = pd.read_csv(o_file, encoding=enc).fillna(""); break
                except: continue
            
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            # [필수 정렬] 출고 데이터 날짜순 정렬
            as_out['temp_date'] = as_out.iloc[:, 6].apply(to_pure_date)
            as_out['is_digitas'] = as_out.iloc[:, 15].astype(str).str.contains("주식회사디지타스")
            as_out = as_out.sort_values(by=['temp_date', 'is_digitas'], ascending=[True, False])

            ui_status = st.empty()
            
            # 1. 미출고 데이터 수집 (입고일 순 정렬)
            db_data = {}
            offset = 0
            while True:
                ui_status.info(f"📥 매칭용 데이터 수집 중... ({offset:,} 건)")
                res = supabase.table("as_history").select("*").neq("상태", "벤더 출고 완료").order("입고일", desc=False).range(offset, offset + 1000).execute()
                if not res.data: break
                for r in res.data:
                    c = sanitize_code(r['압축코드'])
                    if c not in db_data: db_data[c] = []
                    db_data[c].append(r)
                offset += len(res.data)
                if len(res.data) < 1000: break
            
            # 2. 정밀 선입선출(FIFO) 매칭
            success_count = 0
            upsert_list = []
            for i, (idx, row) in enumerate(as_out.iterrows()):
                code = sanitize_code(row.iloc[10]); out_date = to_pure_date(row.iloc[6]); dest = str(row.iloc[15]).strip()
                target_r = None
                
                if code in db_data:
                    candidates = db_data[code]
                    for r in candidates:
                        in_date = to_pure_date(r['입고일'])
                        # 조건: 입고일이 출고일보다 빠르거나 같아야 함 (마이너스 TAT 방지)
                        if in_date and out_date and in_date <= out_date:
                            if dest == "주식회사디지타스":
                                if not r.get('디지타스_출고일'):
                                    target_r = r; break
                            else:
                                # 벤더 출고: 디지타스 거친 건 우선, 없으면 대기건 중 가장 빠른 것
                                if r.get('디지타스_출고일') and not r.get('벤더_출고일'):
                                    target_r = r; break
                                elif not r.get('디지타스_출고일') and not r.get('벤더_출고일'):
                                    target_r = r; break
                
                if target_r:
                    upd_row = target_r.copy()
                    if dest == "주식회사디지타스":
                        upd_row.update({"디지타스_출고일": str(out_date), "상태": "디지타스 출고"})
                        target_r['디지타스_출고일'] = str(out_date)
                    else:
                        upd_row.update({"벤더_출고지": dest, "벤더_출고일": str(out_date), "상태": "벤더 출고 완료"})
                        candidates.remove(target_r)
                    
                    upsert_list.append(upd_row)
                    success_count += 1
                
                # 3. 벌크 UPSERT 실행 (500건 단위)
                if len(upsert_list) >= 500:
                    supabase.table("as_history").upsert(upsert_list).execute()
                    upsert_list = []
                    ui_status.warning(f"⚡ 벌크 DB 반영 중... ({i+1}/{len(as_out)} 건)")

            if upsert_list:
                supabase.table("as_history").upsert(upsert_list).execute()

            ui_status.success(f"✅ {success_count}건 반영 완료! 날짜 교차 매칭 오류가 해결되었습니다.")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트 생성
with tab3:
    st.subheader("📈 분석 리포트 (날짜 필터링)")
    c1, c2 = st.columns(2)
    with c1: s_d = st.date_input("조회 시작일", datetime.now() - timedelta(days=30))
    with c2: e_d = st.date_input("조회 종료일", datetime.now())

    if st.button("📊 리포트 생성"):
        all_d, offset = [], 0
        status = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gte("입고일", str(s_d)).lte("입고일", str(e_d)).range(offset, offset+1000).order("입고일", desc=False).execute()
            if not res.data: break
            all_d.extend(res.data); offset += len(res.data)
            status.info(f"📥 추출 중... ({offset:,}건)")
            if len(res.data) < 1000: break
        
        if all_d:
            df = pd.DataFrame(all_d)
            # TAT 계산 및 포맷팅
            in_dt, dg_dt, vn_dt = pd.to_datetime(df['입고일'], errors='coerce'), pd.to_datetime(df['디지타스_출고일'], errors='coerce'), pd.to_datetime(df['벤더_출고일'], errors='coerce')
            df['TAT'] = (vn_dt - in_dt).dt.days
            df.loc[df['TAT'].isna(), 'TAT'] = (dg_dt - in_dt).dt.days
            
            df['입고일'], df['디지타스_출고일'], df['벤더_출고일'] = in_dt.dt.strftime('%Y-%m-%d'), dg_dt.dt.strftime('%Y-%m-%d').fillna("-"), vn_dt.dt.strftime('%Y-%m-%d').fillna("-")
            df['TAT'], df['벤더_출고지'] = df['TAT'].fillna("-"), df['벤더_출고지'].fillna("-")
            
            cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df[cols].to_excel(wr, index=False)
            st.download_button(f"📥 {s_d}_{e_d}_리포트 다운로드", output.getvalue(), f"AS_Report_{s_d}_{e_d}.xlsx")
            st.dataframe(df[cols].head(100))
        else: st.warning("데이터가 없습니다.")
