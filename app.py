import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# [데이터 정제] 원본 보존
def preserve_raw(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).strip()

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).date()
    except: return None

def smart_read_csv(file):
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc).fillna("")
        except: continue
    raise Exception("CSV 인코딩 오류")

# --- 2. 사이드바 (실시간 DB 상태) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어 센터")
    if st.button("🔍 DB 데이터 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 총 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if st.button("💣 DB 전체 초기화", type="primary", use_container_width=True):
        try:
            status = st.empty()
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                status.text("🗑️ 삭제 중...")
            st.success("✅ 초기화 완료"); st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 데이터 전량 입고", "📤 고속 출고 매칭", "📈 리포트"])

# [TAB 0] 마스터 관리 (업체 정보 매핑용)
with tab0:
    st.subheader("📋 마스터 정보 등록")
    st.info("자재번호별 업체명, 분류 정보를 불러옵니다.")
    m_file = st.file_uploader("마스터 엑셀/CSV 업로드", type=['csv', 'xlsx'], key="master_up")
    
    if m_file and st.button("🔄 마스터 데이터 로드"):
        try:
            if m_file.name.endswith('.csv'):
                m_df = smart_read_csv(m_file)
            else:
                m_df = pd.read_excel(m_file).fillna("")
            
            # 자재번호(A열) 기준으로 정보 매핑 (열 순서는 사용자 환경에 맞춰 조정 필요)
            st.session_state.master_lookup = {
                preserve_raw(row.iloc[0]): {
                    "업체": preserve_raw(row.iloc[5]), # F열: 업체명
                    "분류": preserve_raw(row.iloc[10]), # K열: 분류
                    "AS구분": preserve_raw(row.iloc[14]) if len(row) > 14 else "미지정"
                } for _, row in m_df.iterrows()
            }
            st.success(f"✅ 마스터 {len(st.session_state.master_lookup):,}건 로드 완료")
        except Exception as e: st.error(f"마스터 로드 실패: {e}")

# [TAB 1] 전량 입고 (마스터 정보 결합)
with tab1:
    st.subheader("📥 AS 전량 입고 (16,995건 대응)")
    col1, col2 = st.columns(2)
    with col1:
        in_date_col = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        in_code_col = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with col2:
        in_mat_col = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        in_name_col = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_main")
    
    if i_file and st.button("🚀 입고 시작 (마스터 결합)", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ [마스터 관리] 탭에서 마스터 파일을 먼저 로드해주세요.")
        else:
            try:
                df = smart_read_csv(i_file)
                recs = []
                for _, row in df.iterrows():
                    code = preserve_raw(row.iloc[in_code_col])
                    if not code: continue
                    
                    mat_no = preserve_raw(row.iloc[in_mat_col])
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    
                    recs.append({
                        "압축코드": code,
                        "자재번호": mat_no,
                        "자재명": str(row.iloc[in_name_col]).strip(),
                        "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "미등록"),
                        "대상여부": m_info.get("AS구분", "미등록"),
                        "입고일": str(to_pure_date(row.iloc[in_date_col])),
                        "상태": "출고 대기"
                    })
                
                total = len(recs)
                prog_bar = st.progress(0)
                status_msg = st.empty()
                success_count = 0
                
                for i in range(0, total, 200):
                    chunk = recs[i:i + 200]
                    supabase.table("as_history").upsert(chunk, on_conflict="압축코드").execute()
                    success_count += len(chunk)
                    prog_bar.progress(min(success_count / total, 1.0))
                    status_msg.success(f"⏳ 전송 중: {success_count:,} / {total:,} 완료")
                
                st.success(f"🏁 최종 {success_count:,}건 입고 완료!")
            except Exception as e: st.error(f"입고 실패: {e}")

# [TAB 2] 고속 출고 매칭
with tab2:
    st.subheader("📤 AS 고속 출고 매칭")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="out_main")
    if o_file and st.button("🚀 매칭 시작", use_container_width=True):
        try:
            df_out = smart_read_csv(o_file)
            df_out['match_key'] = df_out.iloc[:, 10].apply(preserve_raw)
            
            # DB 로드
            db_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+1000).execute()
                if not res.data: break
                db_data.extend(res.data); offset += 1000
            db_dict = {item['압축코드']: item['id'] for item in db_data}
            
            updates = []
            for _, row in df_out.iterrows():
                code = row['match_key']
                if code in db_dict:
                    dest = str(row.iloc[15]).strip()
                    out_dt = str(to_pure_date(row.iloc[6]))
                    updates.append({
                        "id": db_dict[code],
                        "디지타스_출고일": out_dt if "디지타스" in dest else None,
                        "벤더_출고일": out_dt if "디지타스" not in dest else None,
                        "벤더_출고지": dest,
                        "상태": "출고 완료"
                    })
            
            if updates:
                for i in range(0, len(updates), 200):
                    supabase.table("as_history").upsert(updates[i:i+200]).execute()
                st.success(f"✅ {len(updates):,}건 매칭 완료!")
        except Exception as e: st.error(f"출고 실패: {e}")

# [TAB 3] 리포트
with tab3:
    st.subheader("📈 TAT 리포트")
    if st.button("📊 리포트 생성", use_container_width=True):
        all_data, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").range(offset, offset+1000).order("입고일").execute()
            if not res.data: break
            all_data.extend(res.data); offset += 1000
        
        if all_data:
            df = pd.DataFrame(all_data)
            st.dataframe(df)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df.to_excel(wr, index=False)
            st.download_button("📥 엑셀 다운로드", output.getvalue(), "AS_TAT_Report.xlsx")
