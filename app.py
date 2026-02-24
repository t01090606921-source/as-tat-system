import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 정밀 분석 시스템", layout="wide")
st.title("📊 AS TAT 정밀 분석 시스템 (미등록 해결 버전)")

# --- [정밀 표준화 함수] ---
def standardize_code(val):
    """자재번호 형식을 앞자리 0을 포함한 문자열로 완벽하게 표준화"""
    if pd.isna(val): return ""
    # 숫자로 인식되어 소수점이 붙는 경우(.0) 제거 후 문자열화
    s_val = str(val).split('.')[0].strip().upper()
    # 보통 자재번호가 5~10자리인 경우, 엑셀에서 잘린 0을 복원하기 위해 사용
    # 여기서는 특정 길이를 강제하기보다 원본 문자열 보존에 집중합니다.
    return s_val

# --- 2. 데이터 로드 함수 ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    limit = 1000
    while True:
        res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(limit).execute()
        if not res.data: break
        all_data.extend(res.data)
        last_id = res.data[-1]['id']
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 정밀 등록 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    m_file = st.file_uploader("마스터 엑셀 선택", type=['xlsx'])
    if m_file and st.button("🚀 마스터 정밀 재등록"):
        with st.spinner("마스터 표준화 중..."):
            m_df = pd.read_excel(m_file, dtype=str)
            m_list = []
            for _, row in m_df.iterrows():
                # [로직 적용] 마스터 자재번호 표준화
                mat_id = standardize_code(row.iloc[0])
                if not mat_id: continue
                m_list.append({
                    "자재번호": mat_id,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보누락",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보누락"
                })
            if m_list:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_list), 200):
                    supabase.table("master_data").insert(m_list[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_list):,}건 표준화 등록 완료")
                st.rerun()

    if st.button("⚠️ 데이터 전체 초기화", type="primary"):
        with st.spinner("삭제 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [item['id'] for item in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.rerun()

# --- 4. 메인: 정밀 매칭 입고 ---
tab1, tab2 = st.tabs(["📥 정밀 매칭 입고", "📈 전수 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'])
    if in_file and st.button("🚀 정밀 매칭 입고 실행"):
        # 마스터 데이터 로드 및 표준화 맵 생성
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in = [], len(as_in)
        p_bar = st.progress(0)
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            # [핵심 로직] 입고 자재번호 표준화 (마스터와 동일한 규칙 적용)
            raw_mat = standardize_code(row.iloc[3])
            
            m_info = m_lookup.get(raw_mat)
            
            recs.append({
                "압축코드": str(row.iloc[7]).strip(),
                "자재번호": raw_mat,
                "규격": str(row.iloc[5]).strip(),
                "상태": "출고 대기",
                "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) == 1000:
                supabase.table("as_history").insert(recs).execute()
                recs = []
                p_bar.progress((i+1)/total_in)
        
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("✅ 정밀 매칭 입고 완료")

# --- 5. 분석 리포트 ---
with tab2:
    if st.button("📈 57만 건 분석 실행"):
        df_raw = fetch_all_data("as_history", "id, 입고일, 출고일, 자재번호, 규격, 공급업체명, 압축코드, 분류구분")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').astype(str).str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            st.metric("최종 수리대상 건수", f"{len(df_rep):,} 건")
            st.metric("미등록 건수", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            
            if not df_rep.empty:
                # TAT 및 엑셀 출력 로직 (이전과 동일)
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'], errors='coerce')
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'], errors='coerce')
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 수리대상 상세 엑셀 다운로드", output.getvalue(), "AS_Repair_Report.xlsx")
