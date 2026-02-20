import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (데이터 대조 진단 모드)")

# --- 2. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count_res = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.metric("현재 DB 내 마스터 건수", f"{m_count_res.count:,} 건")
        
        # 마스터 데이터 샘플 5건 보여주기 (정말 잘 들어갔는지 확인용)
        st.write("---")
        st.write("📂 DB 저장 샘플 (상위 5건)")
        sample = supabase.table("master_data").select("*").limit(5).execute()
        st.table(pd.DataFrame(sample.data))
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        # 이번에는 전처리 없이 문자열 그대로 읽습니다.
        m_df = pd.read_excel(master_file, dtype=str)
        
        # '품목코드' 또는 '자재번호' 열 찾기
        target_col = ""
        for col in m_df.columns:
            if "품목코드" in str(col) or "자재번호" in str(col):
                target_col = col
                break
        
        if target_col:
            m_data = []
            for _, row in m_df.iterrows():
                m_data.append({
                    "자재번호": str(row[target_col]).strip(),
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "N/A",
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "N/A"
                })
            
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
                st.success(f"✅ {len(m_data)}건 등록 완료! (기준열: {target_col})")
                st.rerun()
        else:
            st.error("'품목코드' 열을 찾지 못했습니다.")

    st.divider()
    if st.button("🔥 미등록 203건 전수 재매칭", use_container_width=True):
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {str(r['자재번호']): r for r in m_res.data}
        h_res = supabase.table("as_history").select("id, 자재번호").execute()
        
        for row in h_res.data:
            mat_val = str(row['자재번호']).strip()
            if mat_val in m_lookup:
                supabase.table("as_history").update({
                    "공급업체명": m_lookup[mat_val]['공급업체명'], 
                    "분류구분": m_lookup[mat_val]['분류구분']
                }).eq("id", row['id']).execute()
        st.success("대조 완료")
        st.rerun()

# --- 3. 입고/출고 로직 (간결화 유지) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in")
    if in_file and st.button("입고 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        recs = []
        for _, row in as_in.iterrows():
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), "자재번호": str(row.iloc[3]).strip(),
                "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                "공급업체명": "미등록", "분류구분": "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
        if recs:
            for i in range(0, len(recs), 200):
                supabase.table("as_history").insert(recs[i:i+200]).execute()
            st.rerun()

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

# --- 4. 리포트 & 미등록 리스트 출력 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    if res.data:
        data = pd.DataFrame(res.data)
        
        # [핵심] 미등록된 번호만 따로 모아서 보여주기
        unmatched = data[data['공급업체명'] == '미등록']['자재번호'].unique()
        if len(unmatched) > 0:
            st.warning(f"🚨 현재 미등록된 자재번호 리스트 ({len(unmatched)}건)")
            st.write(unmatched) # 리스트 형태로 화면에 출력
        
        st.subheader("📊 현황 리포트")
        m1, m2 = st.columns(2)
        m1.metric("총 건수", f"{len(data)} 건")
        m2.metric("미등록 건수", f"{len(unmatched)} 건")
        
        st.dataframe(data, use_container_width=True)
except: pass
