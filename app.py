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
st.title("📊 AS TAT 통합 관리 시스템")

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
    if st.button("🔍 DB 데이터 총 수량 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("현재 저장된 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if "delete_mode" not in st.session_state: st.session_state.delete_mode = False
    if not st.session_state.delete_mode:
        if st.button("💣 DB 전체 데이터 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True; st.rerun()
    else:
        st.warning("⚠️ 모든 데이터를 삭제하시겠습니까?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정", use_container_width=True):
                msg = st.empty()
                while True:
                    fetch = supabase.table("as_history").select("id").limit(1000).execute()
                    ids = [r['id'] for r in fetch.data]
                    if not ids: break
                    supabase.table("as_history").delete().in_("id", ids).execute()
                    msg.warning("🗑️ 삭제 중...")
                st.session_state.delete_mode = False; st.success("삭제 완료"); st.rerun()
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일", type=['xlsx', 'csv'], key="m_up")
    if m_file and st.button("🔄 마스터 데이터 로드"):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()
            } for _, row in m_df.iterrows()}
            st.success("✅ 마스터 로드 완료")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리
with tab1:
    st.subheader("📥 AS 입고")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_up")
    if i_file and st.button("🚀 입고 프로세스 시작"):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터 먼저 로드")
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
                        "압축코드": sanitize_code(row.iloc[7]), "자재번호": mat_no,
                        "자재명": str(row.iloc[4]).strip(), "규격": str(row.iloc[5]).strip(),
                        "공급업체명": m_info.get("업체", "미등록"), "분류구분": m_info.get("분류", "수리대상"),
                        "입고일": str(to_pure_date(row.iloc[1])), "상태": "출고 대기"
                    })
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute(); recs = []
                if recs: supabase.table("as_history").insert(recs).execute()
                st.success("✅ 입고 완료")
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (성능 최적화: 타겟 매칭 방식)
with tab2:
    st.subheader("📤 AS 출고 처리 (고속 매칭)")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_up")
    if o_file and st.button("🚀 출고 데이터 반영 시작"):
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            as_out['is_digitas'] = as_out.iloc[:, 15].astype(str).str.contains("주식회사디지타스")
            as_out = as_out.sort_values(by='is_digitas', ascending=False)

            unique_codes = as_out.iloc[:, 10].apply(sanitize_code).unique().tolist()
            db_data = {}
            msg_box = st.empty()

            # 필요한 압축코드만 인덱싱하여 가져옴 (로딩 속도 개선 핵심)
            for i in range(0, len(unique_codes), 400):
                batch = unique_codes[i:i+400]
                res = supabase.table("as_history").select("*").in_("압축코드", batch).neq("상태", "벤더 출고 완료").execute()
                for r in res.data: db_data[r['id']] = r
                msg_box.info(f"🔍 매칭 데이터 준비 중... ({len(db_data)}건)")

            success_count = 0
            for _, row in as_out.iterrows():
                code = sanitize_code(row.iloc[10]); out_date = to_pure_date(row.iloc[6]); dest = str(row.iloc[15]).strip()
                target_id = None
                
                if dest == "주식회사디지타스":
                    target_id = next((rid for rid, r in db_data.items() if sanitize_code(r['압축코드']) == code and not r.get('디지타스_출고일')), None)
                    if target_id:
                        supabase.table("as_history").update({"디지타스_출고일": str(out_date), "상태": "디지타스 출고"}).eq("id", target_id).execute()
                        db_data[target_id]['디지타스_출고일'] = str(out_date)
                        db_data[target_id]['상태'] = "디지타스 출고"
                        success_count += 1
                else:
                    target_id = next((rid for rid, r in db_data.items() if sanitize_code(r['압축코드']) == code and r.get('디지타스_출고일') and not r.get('벤더_출고일')), None)
                    if not target_id:
                        target_id = next((rid for rid, r in db_data.items() if sanitize_code(r['압축코드']) == code and not r.get('디지타스_출고일')), None)
                    if target_id:
                        supabase.table("as_history").update({"벤더_출고지": dest, "벤더_출고일": str(out_date), "상태": "벤더 출고 완료"}).eq("id", target_id).execute()
                        del db_data[target_id]
                        success_count += 1

            msg_box.success(f"✅ {success_count}건 반영 완료되었습니다.")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트 생성 (날짜 필터링 추가)
with tab3:
    st.subheader("📈 AS TAT 분석 리포트")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("조회 시작일", value=datetime.now() - timedelta(days=30))
    with col2:
        end_date = st.date_input("조회 종료일", value=datetime.now())

    if st.button("📊 선택 기간 리포트 생성"):
        all_data = []
        offset = 0
        status_text = st.empty()
        
        try:
            while True:
                # 입고일 기준으로 선택한 기간만 필터링하여 가져옴
                res = supabase.table("as_history").select("*")\
                    .gte("입고일", str(start_date))\
                    .lte("입고일", str(end_date))\
                    .range(offset, offset + 1000).order("입고일", desc=True).execute()
                
                if not res.data: break
                all_data.extend(res.data)
                offset += len(res.data)
                status_text.info(f"📥 데이터 추출 중... ({offset:,}건)")
                if len(res.data) < 1000: break
            
            if all_data:
                df = pd.DataFrame(all_data)
                in_d = pd.to_datetime(df['입고일'], errors='coerce')
                dg_d = pd.to_datetime(df['디지타스_출고일'], errors='coerce')
                vn_d = pd.to_datetime(df['벤더_출고일'], errors='coerce')
                
                df['TAT'] = (vn_d - in_d).dt.days
                df.loc[df['TAT'].isna(), 'TAT'] = (dg_d - in_d).dt.days
                
                df['입고일'] = in_d.dt.strftime('%Y-%m-%d')
                df['디지타스_출고일'] = dg_d.dt.strftime('%Y-%m-%d').fillna("-")
                df['벤더_출고일'] = vn_d.dt.strftime('%Y-%m-%d').fillna("-")
                df['TAT'] = df['TAT'].fillna("-")
                
                cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', 
                        '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
                
                status_text.success(f"✅ {start_date} ~ {end_date} 기간, 총 {len(df):,}건 리포트 생성 완료!")
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                    df[cols].to_excel(wr, index=False)
                st.download_button(f"📥 {start_date}_{end_date}_리포트 다운로드", output.getvalue(), f"AS_TAT_Report_{start_date}_{end_date}.xlsx")
                st.dataframe(df[cols].head(50))
            else:
                st.warning("선택한 기간에 해당하는 데이터가 없습니다.")
        except Exception as e: st.error(f"리포트 생성 중 오류: {e}")
