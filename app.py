import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (데이터 로딩 오류 해결)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        res = supabase.table("as_history").select("id").limit(1).execute()
        if res.data:
            supabase.rpc('truncate_as_history').execute() # 필요 시 RPC 함수 사용
            st.success("데이터 삭제 요청 완료")
        st.rerun()

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.warning("⚠️ 엑셀의 첫 번째 행이 제목(Header)이 아닌 경우 오류가 날 수 있습니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 (A:코드, G:내역)", type=['xlsx'])
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀 (A:구분, B:일자...)", type=['xlsx'])

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        p_bar = st.progress(0)
        
        # [마스터 로드] Header=0으로 명시적 지정
        m_df = pd.read_excel(m_file, header=0).fillna("")
        m_lookup = {}
        for _, row in m_df.iterrows():
            try:
                # 인덱스 대신 .iloc 사용 시 열 존재 여부 체크
                m_code = sanitize_code(row.iloc[0])
                if m_code:
                    m_lookup[m_code] = {
                        "자재내역": str(row.iloc[6]).strip() if len(row) > 6 else "",
                        "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "",
                        "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else ""
                    }
            except: continue

        # [입고 데이터 처리]
        try:
            # 입고 파일 로드 (Header=0)
            i_df = pd.read_excel(i_file, header=0).fillna("")
            
            # 첫 번째 열(index 0)에서 'A/S 철거' 필터링
            as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            total_rows = len(as_in)
            
            if total_rows == 0:
                st.error("입고 대상('A/S 철거') 데이터가 없습니다. 엑셀 1열의 내용을 확인하세요.")
            else:
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        cur_mat = sanitize_code(row.iloc[3]) # D열
                        m_info = m_lookup.get(cur_mat, {})
                        
                        # 날짜 처리 강화
                        try:
                            in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except:
                            in_date = "1900-01-01"

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
                    except: continue
                    
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []
                        p_bar.progress((i+1)/total_rows)
                
                if recs:
                    supabase.table("as_history").insert(recs).execute()
                st.success(f"🎊 {total_rows:,}건 입고 완료!")
        except Exception as e:
            st.error(f"데이터 로딩 중 오류 발생: {str(e)}")

# --- 출고 및 분석 탭은 이전 코드의 로직(바이너리 세션 저장)을 그대로 유지해 주세요 ---
