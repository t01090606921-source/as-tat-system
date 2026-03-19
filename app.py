import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time
import re

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (정밀 리포트 모드)")

# [정제 함수] 모든 공백/제어문자 제거 및 대문자화
def ultimate_sanitize(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    s = str(val).strip().upper()
    s = "".join(s.split())
    s = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', s)
    return s

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
    raise Exception("CSV 읽기 실패")

# --- 2. 사이드바 (DB 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("🔍 DB 데이터 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 실시간 데이터 수", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if st.button("💣 DB 전체 데이터 강제 삭제", type="primary", use_container_width=True):
        try:
            msg = st.empty()
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                msg.warning("🗑️ 데이터 소거 중...")
            st.success("✅ DB 초기화 완료"); time.sleep(1); st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 전량 입고", "📤 고속 출고 매칭", "📈 리포트"])

# [TAB 0] 마스터 관리 (규격명 로직 포함)
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일 업로드(A열 자재번호, G열 규격명)", type=['csv', 'xlsx'], key="m_v_final")
    if m_file and st.button("🔄 마스터 데이터 로드"):
        m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
        # A(0):자재번호, F(5):업체, G(6):규격명, K(10):분류, O(14):AS구분
        st.session_state.master_lookup = {
            ultimate_sanitize(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(),
                "규격명": str(row.iloc[6]).strip(),
                "분류": str(row.iloc[10]).strip(),
                "AS구분": str(row.iloc[14]).strip() if len(row) > 14 else "미지정"
            } for _, row in m_df.iterrows()
        }
        st.success(f"✅ 마스터 로드 완료 (규격명 포함)")

# [TAB 1] 전량 입고
with tab1:
    st.subheader("📥 AS 전량 입고")
    c1, c2 = st.columns(2)
    with c1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고용 CSV 업로드", type=['csv'], key="i_v_final")
    if i_file and st.button("🚀 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            df = smart_read_csv(i_file)
            df['clean_key'] = df.iloc[:, code_idx].apply(ultimate_sanitize)
            clean_df = df[df['clean_key'] != ""].drop_duplicates(subset=['clean_key'], keep='last')
            
            recs = []
            for _, row in clean_df.iterrows():
                code = row['clean_key']
                mat_no = ultimate_sanitize(row.iloc[mat_idx])
                m_info = st.session_state.master_lookup.get(mat_no, {})
                recs.append({
                    "압축코드": code,
                    "자재번호": mat_no,
                    "자재명": str(row.iloc[name_idx]).strip(),
                    "규격명": m_info.get("규격명", "미등록"),
                    "공급업체명": m_info.get("업체", "미등록"),
                    "분류구분": m_info.get("분류", "미등록"),
                    "대상여부": m_info.get("AS구분", "미등록"),
                    "입고일": str(to_pure_date(row.iloc[date_idx])),
                    "상태": "출고 대기"
                })
            
            for i in range(0, len(recs), 200):
                supabase.table("as_history").upsert(recs[i:i+200], on_conflict="압축코드").execute()
            st.success(f"✅ {len(recs):,}건 정상 입고 완료!")

# [TAB 2] 출고 매칭
with tab2:
    st.subheader("📤 AS 고속 출고 매칭")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="o_v_final")
    if o_file and st.button("🚀 매칭 시작"):
        df_out = smart_read_csv(o_file)
        df_out['match_key'] = df_out.iloc[:, 10].apply(ultimate_sanitize)
        db_data, offset = [], 0
        while True:
            res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+999).execute()
            if not res.data: break
            db_data.extend(res.data); offset += 1000
        db_dict = {item['압축코드']: item['id'] for item in db_data}
        updates = []
        for _, row in df_out.iterrows():
            code = row['match_key']
            if code in db_dict:
                dest, out_dt = str(row.iloc[15]).strip(), str(to_pure_date(row.iloc[6]))
                updates.append({
                    "id": db_dict[code],
                    "디지타스_출고일": out_dt if "디지타스" in dest else None,
                    "벤더_출고일": out_dt if "디지타스" not in dest else None,
                    "벤더_출고지": dest, "상태": "출고 완료"
                })
        if updates:
            for i in range(0, len(updates), 200):
                supabase.table("as_history").upsert(updates[i:i+200]).execute()
            st.success(f"✅ {len(updates):,}건 매칭 완료!")

# [TAB 3] 리포트 (컬럼 배열 고정 및 데이터 정제)
with tab3:
    st.subheader("📈 TAT 리포트 (정규 배열 적용)")
    if st.button("📊 최종 리포트 다운로드", use_container_width=True):
        all_d, offset = [], 0
        while True:
            # id 순으로 정렬하여 누락 없이 로드
            res = supabase.table("as_history").select("*").order("id").range(offset, offset+999).execute()
            if not res.data: break
            all_d.extend(res.data); offset += 1000
        
        if all_d:
            df = pd.DataFrame(all_d)
            # 압축코드 기준 최종 중복 제거
            df = df.drop_duplicates(subset=['압축코드'], keep='last')
            
            # --- 리포트 컬럼 순서 강제 지정 ---
            target_cols = [
                "입고일", "자재번호", "자재명", "규격명", "공급업체명", "분류구분", 
                "대상여부", "압축코드", "디지타스_출고일", "벤더_출고일", "벤더_출고지", "상태"
            ]
            
            # 존재하는 컬럼만 필터링하여 순서 재배치
            df = df[[c for c in target_cols if c in df.columns]]
            
            st.write(f"✅ 최종 리포트 수량: {len(df):,} 건")
            st.dataframe(df.head(100))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df.to_excel(wr, index=False)
            st.download_button("📥 정규 리포트 받기", output.getvalue(), f"AS_TAT_Final_{time.strftime('%m%d')}.xlsx")
        else:
            st.error("DB에 데이터가 없습니다.")
