import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io

# --- Supabase 설정 ---
# 로컬 테스트 시에는 st.secrets 대신 직접 문자열을 넣어도 되지만, 배포 시에는 secrets 설정을 권장합니다.
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (Cloud DB)")

# --- 1. 사이드바: 마스터 데이터 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    master_file = st.file_uploader("분류구분 마스터 업로드", type=['xlsx'])
    if master_file and st.button("마스터 DB 갱신"):
        try:
            m_df = pd.read_excel(master_file)
            m_data = [
                {
                    "자재번호": str(row.iloc[0]).strip(),
                    "공급업체명": str(row.iloc[5]).strip(),
                    "분류구분": str(row.iloc[10]).strip()
                } for _, row in m_df.iterrows()
            ]
            # Upsert (기존 데이터 덮어쓰기)
            supabase.table("master_data").upsert(m_data).execute()
            st.success("✅ 마스터 정보 반영 완료!")
        except Exception as e:
            st.error(f"마스터 파일 오류: {e}")

# --- 2. 입고/출고 탭 ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_up")
    if in_file and st.button("입고 데이터 처리"):
        df = pd.read_excel(in_file)
        as_in = df[df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
        
        # 마스터 데이터 가져오기
        master_res = supabase.table("master_data").select("*").execute()
        m_df = pd.DataFrame(master_res.data)

        new_recs = []
        for _, row in as_in.iterrows():
            key_val = str(row.iloc[7]).strip()
            mat_no = str(row.iloc[3]).strip()
            in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            
            # 중복 체크 (DB 조회)
            check = supabase.table("as_history").select("id").match({"압축코드": key_val, "입고일": in_date}).execute()
            
            if not check.data:
                vendor = m_df[m_df['자재번호'] == mat_no]['공급업체명'].values[0] if not m_df.empty and mat_no in m_df['자재번호'].values else "미등록"
                cat = m_df[m_df['자재번호'] == mat_no]['분류구분'].values[0] if not m_df.empty and mat_no in m_df['자재번호'].values else "미등록"
                
                new_recs.append({
                    "압축코드": key_val, "자재번호": mat_no, "규격": str(row.iloc[5]).strip(),
                    "공급업체명": vendor, "분류구분": cat, "입고일": in_date, "상태": "출고 대기"
                })
        
        if new_recs:
            supabase.table("as_history").insert(new_recs).execute()
            st.success(f"✅ {len(new_recs)}건 입고 완료")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("출고 매칭 시작"):
        df = pd.read_excel(out_file)
        as_out = df[df.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        
        match_count = 0
        for _, row in as_out.iterrows():
            key_val = str(row.iloc[10]).strip()
            out_date_dt = pd.to_datetime(row.iloc[6])
            
            # FIFO 매칭: 가장 오래된 대기 건 조회
            target = supabase.table("as_history").select("id, 입고일").match({"압축코드": key_val, "상태": "출고 대기"}).order("입고일").limit(1).execute()
            
            if target.data:
                row_id = target.data[0]['id']
                in_dt = pd.to_datetime(target.data[0]['입고일'])
                tat = round((out_date_dt - in_dt).total_seconds() / (24 * 3600), 2)
                
                supabase.table("as_history").update({
                    "출고일": out_date_dt.strftime('%Y-%m-%d'),
                    "tat": tat,
                    "상태": "출고 완료"
                }).eq("id", row_id).execute()
                match_count += 1
        st.success(f"✅ {match_count}건 출고 완료")

# --- 3. 리포트 ---
st.divider()
res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
all_data = pd.DataFrame(res.data)

if not all_data.empty:
    # 필터 및 지표 (이전 로직 동일)
    col1, col2, col3 = st.columns(3)
    v_f = col1.multiselect("🏢 공급업체", options=sorted(all_data['공급업체명'].unique()))
    f_df = all_data.copy()
    if v_f: f_df = f_df[f_df['공급업체명'].isin(v_f)]
    
    st.metric("평균 TAT", f"{round(f_df[f_df['상태']=='출고 완료']['tat'].astype(float).mean(), 1)} 일")
    st.dataframe(f_df, use_container_width=True, hide_index=True)