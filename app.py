import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (메모리 직결 매칭)")

# --- 2. 사이드바: 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # 마스터 상태 확인
    try:
        m_count = supabase.table("master_data").select("자재번호", count="exact").execute().count
        st.metric("마스터 DB 등록 건수", f"{m_count if m_count else 0:,} 건")
    except: pass

    st.subheader("1. 마스터 갱신")
    master_file = st.file_uploader("마스터 엑셀", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        t_col = next((c for c in m_df.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df.columns[0])
        m_data = [{"자재번호": str(row[t_col]).strip().upper(), 
                   "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                   "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"} 
                  for _, row in m_df.iterrows() if not pd.isna(row[t_col])]
        
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 등록 완료")
            st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 3. 입고 처리 (핵심: DB에 묻지 않고 엑셀끼리 직접 대조) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_direct")
    if in_file and st.button("입고 실행 (직접 대조 모드)"):
        # 1. 마스터 데이터 DB에서 긁어와서 메모리에 올리기
        m_res = supabase.table("master_data").select("*").execute()
        m_df_local = pd.DataFrame(m_res.data)
        m_df_local['자재번호'] = m_df_local['자재번호'].str.strip().str.upper()
        
        # 2. 입고 파일 읽기
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs = []
        for _, row in as_in.iterrows():
            mat_val = str(row.iloc[3]).strip().upper()
            
            # [VLOOKUP 방식의 메모리 직접 매칭]
            match = m_df_local[m_df_local['자재번호'] == mat_val]
            
            if not match.empty:
                v_name = match.iloc[0]['공급업체명']
                v_type = match.iloc[0]['분류구분']
            else:
                v_name = "미등록"
                v_type = "미등록"
                
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), 
                "자재번호": mat_val,
                "규격": str(row.iloc[5]).strip(), 
                "상태": "출고 대기",
                "공급업체명": v_name, 
                "분류구분": v_type,
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            
        if recs:
            for i in range(0, len(recs), 200):
                supabase.table("as_history").insert(recs[i:i+200]).execute()
            st.success(f"{len(recs)}건 처리 완료!")
            st.rerun()

# --- 4. 출고 처리 (생략 없이 유지) ---
with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out")
    if out_file and st.button("출고 실행"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key = str(row.iloc[10]).strip()
            date = pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id").match({"압축코드": key, "상태": "출고 대기"}).limit(1).execute()
            if target.data:
                supabase.table("as_history").update({"출고일": date.strftime('%Y-%m-%d'), "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.rerun()

# --- 5. 리포트 현황 ---
st.divider()
res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
if res.data:
    df_res = pd.DataFrame(res.data)
    st.subheader("📊 현황 리포트")
    
    m1, m2 = st.columns(2)
    m1.metric("총 건수", f"{len(df_res)} 건")
    m2.metric("미등록", f"{len(df_res[df_res['공급업체명'] == '미등록'])} 건")
    
    st.dataframe(df_res, use_container_width=True, hide_index=True)
