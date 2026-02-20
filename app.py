import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (포함 매칭 모드)")

# --- 2. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count_res = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.metric("현재 DB 내 마스터 건수", f"{m_count_res.count:,} 건")
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        target_col = next((col for col in m_df.columns if "품목코드" in str(col) or "자재번호" in str(col)), None)
        
        if target_col:
            m_data = []
            for _, row in m_df.iterrows():
                mat_val = str(row[target_col]).strip().upper()
                if not mat_val or mat_val == "NAN": continue
                m_data.append({
                    "자재번호": mat_val,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보없음",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보없음"
                })
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 100):
                    supabase.table("master_data").insert(m_data[i:i+100]).execute()
                st.success("✅ 마스터 원본 등록 완료")
                st.rerun()

    st.divider()
    st.subheader("2. 미등록 202건 해결")
    if st.button("🔥 유연한 포함 매칭 실행", use_container_width=True):
        with st.spinner("모든 가능성을 열고 재매칭 중..."):
            m_res = supabase.table("master_data").select("*").execute()
            master_list = m_res.data # 리스트로 보관
            
            h_res = supabase.table("as_history").select("id, 자재번호").eq("공급업체명", "미등록").execute()
            
            up_cnt = 0
            for row in h_res.data:
                h_val = str(row['자재번호']).strip().upper()
                
                # [핵심 로직] 1:1 매칭이 안되면 포함 관계로 검색
                match_info = None
                for m_item in master_list:
                    m_val = str(m_item['자재번호']).strip().upper()
                    
                    if h_val == m_val or h_val in m_val or m_val in h_val:
                        match_info = m_item
                        break
                
                if match_info:
                    supabase.table("as_history").update({
                        "공급업체명": match_info['공급업체명'], 
                        "분류구분": match_info['분류구분']
                    }).eq("id", row['id']).execute()
                    up_cnt += 1
            st.success(f"✅ {up_cnt}건의 미등록 데이터가 성공적으로 보정되었습니다!")
            st.rerun()

# --- 3. 입고/출고 (생략 없이 유지) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in")
    if in_file and st.button("입고 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        recs = []
        for _, row in as_in.iterrows():
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), "자재번호": str(row.iloc[3]).strip().upper(),
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

# --- 4. 리포트 표시 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    if res.data:
        dff = pd.DataFrame(res.data)
        st.subheader("📊 현황 리포트")
        
        m1, m2 = st.columns(2)
        m1.metric("총 건수", f"{len(dff)} 건")
        m2.metric("미등록", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        
        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
