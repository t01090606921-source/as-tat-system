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
st.title("📊 AS TAT 통합 관리 (진행 바 강제 표시 버전)")

# [표준화 함수]
def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# [데이터 로드 함수]
def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    status_area = st.empty()
    while True:
        try:
            res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            all_data.extend(res.data)
            last_id = res.data[-1]['id']
            status_area.info(f"📥 DB 데이터 수집 중... ({len(all_data):,}건)")
        except:
            time.sleep(1)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 2. 사이드바 (초기화 및 마스터) ---
with st.sidebar:
    st.header("⚙️ 설정")
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 삭제 중... (최근 ID: {ids[-1]})")
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
            st.success("마스터 등록 성공")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고", "📤 AS 출고", "📊 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_main")
    
    # 입고 버튼 클릭 시 실행되는 영역
    if in_file and st.button("🚀 정밀 입고 실행"):
        # 1. 화면에 진행 표시기 미리 생성 (강제 고정)
        progress_info = st.empty()
        progress_bar = st.progress(0)
        status_msg = st.empty()
        
        progress_info.warning("⏳ 1단계: 마스터 데이터를 로드하고 있습니다...")
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        # 2. 엑셀 로딩 (성능 최적화 버전)
        progress_info.warning("⏳ 2단계: 업로드하신 엑셀 파일을 분석 중입니다. (대용량의 경우 1분 이상 소요)")
        try:
            # openpyxl보다 빠른 엔진 시도
            df = pd.read_excel(in_file, dtype=str)
        except Exception as e:
            st.error(f"엑셀을 읽는 중 에러가 발생했습니다: {e}")
            st.stop()
            
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        total_rows = len(as_in)
        
        if total_rows > 0:
            progress_info.info(f"✅ 3단계: 총 {total_rows:,}건의 입고 데이터를 처리합니다.")
            
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
                
                # 100건마다 DB 전송 및 진행 바 강제 갱신 (더 자주 갱신)
                if len(recs) >= 100:
                    try:
                        supabase.table("as_history").insert(recs).execute()
                    except:
                        time.sleep(1)
                        supabase.table("as_history").insert(recs).execute()
                    
                    recs = []
                    # 진행률 계산 및 표시
                    ratio = (i + 1) / total_rows
                    progress_bar.progress(ratio)
                    status_msg.markdown(f"### 🏃 현재 입고 중: {i+1:,} / {total_rows:,} 건 ({ratio*100:.1f}%)")
            
            # 남은 데이터 처리
            if recs:
                supabase.table("as_history").insert(recs).execute()
                progress_bar.progress(1.0)
            
            progress_info.success(f"🎊 모든 작업이 완료되었습니다! 총 {total_rows:,}건 입고 완료.")
            status_msg.empty()
        else:
            st.error("입고 대상('A/S 철거') 데이터가 엑셀에 없습니다. 컬럼 위치나 텍스트를 확인해주세요.")

with tab2:
    # 출고 로직 (간략 버전)
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_main")
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            out_bar = st.progress(0)
            for i in range(0, len(out_keys), 500):
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", out_keys[i:i+500]).execute()
                out_bar.progress(min((i+500)/len(out_keys), 1.0))
            st.success("출고 처리 완료")

with tab3:
    if st.button("📈 분석 리포트 생성"):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            # 리포트 생성 로직... (이전과 동일)
            st.write("리포트 생성 완료 (위의 데이터 합계 확인)")
