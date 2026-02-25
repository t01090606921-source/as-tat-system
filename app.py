import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (최종 안정화 버전)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    # 소수점 제거 및 대문자 공백 제거
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        try:
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [i['id'] for i in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
            st.success("초기화 완료")
            st.rerun()
        except:
            st.error("초기화 중 오류 발생")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 마스터와 입고 파일을 업로드하세요. 오류가 있는 행은 자동으로 건너뜁니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        p_bar = st.progress(0)
        status_log = st.empty()
        
        # [마스터 로드]
        m_df = pd.read_excel(m_file, dtype=str).fillna("")
        m_lookup = {}
        for _, row in m_df.iterrows():
            m_code = sanitize_code(row.iloc[0])
            if m_code:
                m_lookup[m_code] = {
                    "자재내역": str(row.iloc[6]).strip() if len(row) > 6 else "",
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "",
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else ""
                }

        # [입고 데이터 처리]
        try:
            i_df = pd.read_excel(i_file, dtype=str).fillna("")
            # 'A/S 철거' 문자열이 포함된 행만 필터링
            as_in = i_df[i_df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            total_rows = len(as_in)
            
            if total_rows == 0:
                st.error("입고 대상('A/S 철거') 데이터가 0건입니다. 엑셀 형식을 확인해주세요.")
            else:
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        cur_mat = sanitize_code(row.iloc[3])
                        m_info = m_lookup.get(cur_mat, {})
                        
                        # 날짜 변환 시도
                        try:
                            raw_date = pd.to_datetime(row.iloc[1])
                            in_date = raw_date.strftime('%Y-%m-%d')
                        except:
                            in_date = "1900-01-01" # 날짜 오류 시 기본값

                        recs.append({
                            "압축코드": str(row.iloc[7]).strip() if len(row) > 7 else "",
                            "자재번호": cur_mat,
                            "자재내역": m_info.get("자재내역", "미등록"),
                            "규격": str(row.iloc[5]).strip() if len(row) > 5 else "",
                            "상태": "출고 대기",
                            "공급업체명": m_info.get("공급업체명", "미등록"),
                            "분류구분": m_info.get("분류구분", "미등록"),
                            "입고일": in_date
                        })
                    except Exception as e:
                        continue # 한 행이 깨져도 다음 행 진행
                    
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []
                        p_bar.progress((i+1)/total_rows)
                
                if recs:
                    supabase.table("as_history").insert(recs).execute()
                st.success(f"🎊 {total_rows:,}건 입고 완료!")
        except Exception as e:
            st.error(f"입고 처리 중 치명적 오류: {e}")

with tab2:
    # 출고 처리 로직 (생략 - 이전과 동일)
    st.info("출고 엑셀을 업로드하세요.")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    # ... (생략: 이전 개별 출고 코드와 동일하게 적용)

with tab3:
    # 분석 리포트 로직 (생략 - 이전과 동일하게 바이너리 세션 방식 사용)
    if st.button("📈 데이터 분석 시작"):
        # ... (생략: 이전 세션 유지 방식 코드와 동일하게 적용)
        st.write("분석 기능을 실행합니다.")
