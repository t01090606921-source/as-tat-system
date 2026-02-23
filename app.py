import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (158건 최종 소거)")

# --- 2. 사이드바: 관리 및 초기화 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # 1. 마스터 데이터 재등록 (대문자 통일)
    master_file = st.file_uploader("마스터 엑셀", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록"):
        m_df = pd.read_excel(master_file, dtype=str)
        t_col = next((c for c in m_df.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df.columns[0])
        
        m_data = []
        for _, row in m_df.iterrows():
            # [핵심] 모든 공백 제거 및 대문자 통일
            raw_val = str(row[t_col]).strip().replace(" ", "").upper()
            if not raw_val or raw_val == "NAN": continue
            m_data.append({
                "자재번호": raw_val,
                "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"
            })
        
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터(대문자/공백제거) 등록 완료")
            st.rerun()

    st.divider()
    # 2. 158건 강제 보정 (가장 강력한 비교 로직)
    if st.button("🔥 158건 무조건 매칭 실행", type="primary", use_container_width=True):
        m_res = supabase.table("master_data").select("*").execute()
        # 마스터 비교 키도 공백제거/대문자화
        m_lookup = {str(r['자재번호']).replace(" ", "").upper(): r for r in m_res.data}
        
        h_res = supabase.table("as_history").select("id, 자재번호").eq("공급업체명", "미등록").execute()
        
        success_cnt = 0
        for row in h_res.data:
            # 입고 데이터도 공백제거/대문자화 후 비교
            h_val = str(row['자재번호']).strip().replace(" ", "").upper()
            
            if h_val in m_lookup:
                supabase.table("as_history").update({
                    "공급업체명": m_lookup[h_val]['공급업체명'],
                    "분류구분": m_lookup[h_val]['분류구분']
                }).eq("id", row['id']).execute()
                success_cnt += 1
        
        st.success(f"✅ {success_cnt}건 매칭 완료!")
        st.rerun()

# --- 3. 입고 처리 (매칭 최적화) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'])
    if in_file and st.button("입고 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs = []
        for _, row in as_in.iterrows():
            # 입고 시점부터 깨끗하게 저장
            mat_val = str(row.iloc[3]).strip().replace(" ", "").upper()
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), 
                "자재번호": mat_val,
                "규격": str(row.iloc[5]).strip(), 
                "상태": "출고 대기",
                "공급업체명": "미등록", "분류구분": "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
        if recs:
            for i in range(0, len(recs), 200):
                supabase.table("as_history").insert(recs[i:i+200]).execute()
            st.rerun()

# --- 4. 리포트 (생략 없이 복구) ---
st.divider()
res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
if res.data:
    df_res = pd.DataFrame(res.data)
    st.subheader("📊 현황 리포트")
    
    c1, c2, c3 = st.columns(3)
    v_f = c1.multiselect("🏢 공급업체", sorted(df_res['공급업체명'].unique()))
    g_f = c2.multiselect("📂 분류구분", sorted(df_res['분류구분'].unique()))
    s_f = c3.multiselect("🚚 상태", sorted(df_res['상태'].unique()))
    
    dff = df_res.copy()
    if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
    if g_f: dff = dff[dff['분류구분'].isin(g_f)]
    if s_f: dff = dff[dff['상태'].isin(s_f)]

    m1, m2 = st.columns(2)
    m1.metric("총 건수", f"{len(dff)} 건")
    m2.metric("미등록", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
    
    st.dataframe(dff, use_container_width=True, hide_index=True)
