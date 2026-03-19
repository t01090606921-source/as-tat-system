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

# [데이터 정제] 양끝 공백만 제거 (중간 공백/기호는 유지하여 원본 보존)
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

# --- 2. 사이드바 (실시간 DB 상태 감시) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어 센터")
    if st.button("🔍 DB 데이터 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 총 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    st.warning("⚠️ 중복 방지를 위해 입고 전 초기화를 권장합니다.")
    if st.button("💣 DB 전체 데이터 초기화", type="primary", use_container_width=True):
        try:
            status = st.empty()
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                status.text("🗑️ 이전 데이터 소거 중...")
            st.success("✅ 초기화 완료"); time.sleep(1); st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 전량 입고(검수)", "📤 고속 출고 매칭", "📈 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일 업로드", type=['csv', 'xlsx'], key="master_up")
    if m_file and st.button("🔄 마스터 데이터 로드"):
        try:
            m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {
                preserve_raw(row.iloc[0]): {
                    "업체": preserve_raw(row.iloc[5]),
                    "분류": preserve_raw(row.iloc[10]),
                    "AS구분": preserve_raw(row.iloc[14]) if len(row) > 14 else "미지정"
                } for _, row in m_df.iterrows()
            }
            st.success(f"✅ 마스터 {len(st.session_state.master_lookup):,}건 로드 완료")
        except Exception as e: st.error(f"마스터 오류: {e}")

# [TAB 1] 전량 입고 (중복 검수 로직 강화)
with tab1:
    st.subheader("📥 AS 전량 입고 (파일 내 중복 제거 포함)")
    col1, col2 = st.columns(2)
    with col1:
        in_date_col = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        in_code_col = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with col2:
        in_mat_col = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        in_name_col = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_main")
    
    if i_file and st.button("🚀 중복 검수 후 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ [마스터 관리] 탭에서 마스터를 먼저 로드하세요.")
        else:
            try:
                raw_df = smart_read_csv(i_file)
                # 1단계: 파일 내 중복 제거 (압축코드 기준 마지막 행 유지)
                raw_df['tmp_key'] = raw_df.iloc[:, in_code_col].apply(preserve_raw)
                clean_df = raw_df.drop_duplicates(subset=['tmp_key'], keep='last')
                
                diff = len(raw_df) - len(clean_df)
                if diff > 0:
                    st.warning(f"⚠️ 파일 내 중복 데이터 {diff:,}건이 발견되어 자동으로 제외되었습니다.")
                
                recs = []
                for _, row in clean_df.iterrows():
                    code = row['tmp_key']
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
                prog_bar = st.progress(0); status_msg = st.empty()
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
