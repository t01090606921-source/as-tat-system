import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import re

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

def clean_mat_no(val):
    """자재번호를 완벽하게 클리닝: 숫자/문자 혼용 및 소수점(.0) 제거"""
    if pd.isna(val): return ""
    # 문자열화 후 공백 제거 및 대문자 변환
    s = str(val).strip().upper()
    # 소수점 .0으로 끝나는 경우 제거 (예: 12345.0 -> 12345)
    if s.endswith('.0'):
        s = s[:-2]
    # 보이지 않는 특수문자나 줄바꿈 제거
    s = re.sub(r'[^A-Z0-9]', '', s) 
    return s

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (Cloud)")

# --- 2. 사이드바: 설정 및 초기화 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    
    st.subheader("1. 마스터 데이터 관리")
    master_file = st.file_uploader("마스터 업로드 (엑셀)", type=['xlsx'])
    if master_file and st.button("🚀 마스터 DB 갱신", use_container_width=True):
        try:
            m_df = pd.read_excel(master_file)
            m_data = []
            for _, row in m_df.iterrows():
                mat_no = clean_mat_no(row.iloc[0]) # A열
                if not mat_no: continue
                m_data.append({
                    "자재번호": mat_no,
                    "공급업체명": str(row.iloc[5]).strip(), # F열
                    "분류구분": str(row.iloc[10]).strip() # K열
                })
            
            if m_data:
                # 기존 마스터 삭제 후 삽입
                supabase.table("master_data").delete().neq("자재번호", "TEMP").execute()
                for i in range(0, len(m_data), 500):
                    supabase.table("master_data").insert(m_data[i:i+500]).execute()
                st.success(f"✅ 마스터 {len(m_data)}건 동기화 완료!")
        except Exception as e:
            st.error(f"마스터 오류: {e}")

    st.divider()
    st.subheader("2. 정보 보정 (미등록 해결)")
    if st.button("🔄 미등록 정보 강제 재매칭", use_container_width=True):
        with st.spinner("마스터와 전체 대조 중..."):
            # 마스터 로드
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            
            # 전체 이력 로드
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            
            up_cnt = 0
            for row in h_res.data:
                # 현재 저장된 자재번호를 다시 한번 클리닝해서 대조
                c_mat = clean_mat_no(row['자재번호'])
                m_info = m_lookup.get(c_mat)
                
                if m_info:
                    supabase.table("as_history").update({
                        "공급업체명": m_info['공급업체명'], 
                        "분류구분": m_info['분류구분']
                    }).eq("id", row['id']).execute()
                    up_cnt += 1
            st.success(f"✅ {up_cnt}건의 미등록 데이터가 성공적으로 보정되었습니다!")
            st.rerun()

    st.divider()
    st.subheader("3. 데이터 초기화")
    confirm = st.checkbox("데이터 전체 삭제 동의")
    if st.button("⚠️ 시스템 전체 초기화", type="primary", use_container_width=True):
        if confirm:
            supabase.table("as_history").delete().neq("id", -1).execute()
            supabase.table("master_data").delete().neq("자재번호", "TEMP").execute()
            st.warning("🚨 모든 데이터 초기화 완료.")
            st.rerun()

# --- 3. 입고/출고 처리 ---
tab1, tab2 = st.tabs(["📥 AS 입고 처리", "📤 AS 출고 처리"])

with tab1:
    in_file = st.file_uploader("입고 현황 엑셀", type=['xlsx'], key="in_up")
    if in_file and st.button("입고 데이터 처리 실행"):
        with st.spinner("처리 중..."):
            df = pd.read_excel(in_file)
            as_in = df[df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}

            new_recs = []
            for _, row in as_in.iterrows():
                mat_no = clean_mat_no(row.iloc[3]) # D열
                m_info = m_lookup.get(mat_no)
                new_recs.append({
                    "압축코드": str(row.iloc[7]).strip(), 
                    "자재번호": mat_no,
                    "규격": str(row.iloc[5]).strip(),
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d'),
                    "상태": "출고 대기"
                })
            if new_recs:
                for i in range(0, len(new_recs), 500):
                    supabase.table("as_history").insert(new_recs[i:i+500]).execute()
                st.success("✅ 등록 완료!")
                st.rerun()

with tab2:
    out_file = st.file_uploader("출고 현황 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("출고 매칭 실행"):
        with st.spinner("매칭 중..."):
            df = pd.read_excel(out_file)
            as_out = df[df.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            for _, row in as_out.iterrows():
                key = str(row.iloc[10]).strip()
                out_date = pd.to_datetime(row.iloc[6])
                target = supabase.table("as_history").select("id, 입고일").match({"압축코드": key, "상태": "출고 대기"}).order("입고일").limit(1).execute()
                if target.data:
                    in_dt = pd.to_datetime(target.data[0]['입고일'])
                    tat = round((out_date - in_dt).total_seconds() / (24 * 3600), 2)
                    supabase.table("as_history").update({"출고일": out_date.strftime('%Y-%m-%d'), "tat": tat, "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
            st.success("✅ 매칭 완료!")
            st.rerun()

# --- 4. 리포트 영역 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    all_data = pd.DataFrame(res.data)
    if not all_data.empty:
        st.subheader("📊 실시간 분석 리포트")
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("업체", sorted(all_data['공급업체명'].unique()))
        g_f = c2.multiselect("분류", sorted(all_data['분류구분'].unique()))
        s_f = c3.multiselect("상태", ['출고 대기', '출고 완료'])
        
        f_df = all_data.copy()
        if v_f: f_df = f_df[f_df['공급업체명'].isin(v_f)]
        if g_f: f_df = f_df[f_df['분류구분'].isin(g_f)]
        if s_f: f_df = f_df[f_df['상태'].isin(s_f)]
        
        m1, m2, m3 = st.columns(3)
        m1.metric("총 건수", f"{len(f_df)} 건")
        fin = f_df[f_df['상태'] == '출고 완료']
        m2.metric("평균 TAT", f"{round(pd.to_numeric(fin['tat']).mean(), 1) if not fin.empty else 0} 일")
        m3.metric("현재 대기", f"{len(f_df[f_df['상태'] == '출고 대기'])} 건")
        st.dataframe(f_df, use_container_width=True, hide_index=True)
except:
    pass
