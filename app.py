import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (마스터 로딩 최적화)")

def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# [마스터 데이터 전용 고속 로드]
@st.cache_data(ttl=600) # 10분간 메모리에 결과 저장 (속도 향상)
def get_master_lookup():
    all_data = []
    last_id = -1
    limit = 1000
    try:
        while True:
            res = supabase.table("master_data").select("*").gt("id", last_id).order("id").limit(limit).execute()
            if not res.data: break
            all_data.extend(res.data)
            last_id = res.data[-1]['id']
            if len(all_data) > 150000: break # 안전장치: 너무 많으면 중단
    except Exception as e:
        st.error(f"마스터 DB 연결 오류: {e}")
        return {}
    
    return {str(r['자재번호']): r for r in all_data}

# --- 2. 사이드바 (초기화 및 마스터 등록) ---
with st.sidebar:
    st.header("⚙️ 설정")
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 삭제 중... (ID: {ids[-1]})")
        st.success("초기화 완료")
        st.rerun()

    st.divider()
    m_file = st.file_uploader("📋 마스터 엑셀 등록", type=['xlsx'])
    if m_file and st.button("🚀 마스터 업로드"):
        m_df = pd.read_excel(m_file, dtype=str)
        m_list = []
        for _, row in m_df.iterrows():
            mat_id = sanitize_code(row.iloc[0])
            if mat_id:
                m_list.append({
                    "자재번호": mat_id, 
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락", 
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락"
                })
        if m_list:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_list), 200):
                supabase.table("master_data").insert(m_list[i:i+200]).execute()
            st.cache_data.clear() # 캐시 삭제
            st.success("마스터 등록 성공")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고", "📤 AS 출고", "📊 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_main")
    if in_file and st.button("🚀 정밀 입고 실행"):
        step_msg = st.empty()
        p_bar = st.progress(0)
        status_msg = st.empty()

        # [단계 1] 마스터 데이터 로드 (캐시 활용)
        step_msg.warning("⏳ 1단계: 마스터 데이터를 불러오는 중입니다...")
        m_lookup = get_master_lookup()
        
        if not m_lookup:
            st.error("마스터 데이터를 불러오지 못했습니다. 사이드바에서 먼저 등록해주세요.")
            st.stop()

        # [단계 2] 엑셀 읽기
        step_msg.warning("⏳ 2단계: 엑셀 파일을 분석 중입니다...")
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        total_rows = len(as_in)
        
        if total_rows > 0:
            step_msg.info(f"✅ 3단계: 총 {total_rows:,}건 입고 시작")
            
            recs = []
            for i, (_, row) in enumerate(as_in.iterrows()):
                cur_mat = sanitize_code(row.iloc[3])
                m_info = m_lookup.get(cur_mat)
                
                recs.append({
                    "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                    "자재번호": cur_mat,
                    "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                    "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
                
                # 100건 단위 전송
                if len(recs) >= 100:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    ratio = (i + 1) / total_rows
                    p_bar.progress(ratio)
                    status_msg.markdown(f"### 🏃 입고 중: {i+1:,} / {total_rows:,} 건")
            
            if recs:
                supabase.table("as_history").insert(recs).execute()
                p_bar.progress(1.0)
            
            step_msg.success(f"🎊 총 {total_rows:,}건 입고 완료!")
        else:
            st.error("입고 대상 데이터가 없습니다.")
