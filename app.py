import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import unicodedata
import re

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

def final_match_clean(val):
    """자재번호에서 모든 특수문자, 공백을 제거하고 대문자로 통일 (최종 매칭용)"""
    if pd.isna(val): return ""
    s = str(val).strip()
    s = unicodedata.normalize('NFKC', s)
    # 숫자와 영문자만 남기고 모두 제거 (하이픈, 슬래시 등 완전 제거)
    s = re.sub(r'[^a-zA-Z0-9]', '', s)
    if s.endswith('0') and '.0' in str(val): s = s[:-1] # 엑셀 .0 방지
    return s.upper()

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (매칭 정밀화)")

# --- 2. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count_res = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.info(f"📊 마스터 DB 등록: {m_count_res.count} 건")
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 갱신", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        m_data = []
        for _, row in m_df.iterrows():
            # 매칭 정확도를 위해 기호를 모두 제거한 번호를 키로 저장
            mat_no = final_match_clean(row.iloc[0])
            if not mat_no: continue
            m_data.append({
                "자재번호": mat_no,
                "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보누락",
                "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보누락"
            })
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 갱신 완료")
            st.rerun()

    st.divider()
    st.subheader("2. 데이터 보정")
    if st.button("🔥 최종 정밀 재매칭", use_container_width=True):
        with st.spinner("미등록 건 재분석 중..."):
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            
            up_cnt = 0
            for row in h_res.data:
                # 입고된 자재번호도 기호를 모두 제거하고 비교
                c_val = final_match_clean(row['자재번호'])
                m_info = m_lookup.get(c_val)
                if m_info:
                    supabase.table("as_history").update({
                        "공급업체명": m_info['공급업체명'],
                        "분류구분": m_info['분류구분']
                    }).eq("id", row['id']).execute()
                    up_cnt += 1
            st.success(f"✅ {up_cnt}건 보정 성공!")
            st.rerun()

# --- 3. 입고/출고 처리 ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in")
    if in_file and st.button("입고 처리 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {r['자재번호']: r for r in m_res.data}
        recs = []
        for _, row in as_in.iterrows():
            mat = final_match_clean(row.iloc[3])
            m = m_lookup.get(mat)
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), "자재번호": mat,
                "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                "공급업체명": m['공급업체명'] if m else "미등록",
                "분류구분": m['분류구분'] if m else "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
        if recs:
            for i in range(0, len(recs), 200):
                supabase.table("as_history").insert(recs[i:i+200]).execute()
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out")
    if out_file and st.button("출고 매칭 실행"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key, date = str(row.iloc[10]).strip(), pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id, 입고일").match({"압축코드": key, "상태": "출고 대기"}).order("입고일").limit(1).execute()
            if target.data:
                in_dt = pd.to_datetime(target.data[0]['입고일'])
                tat = round((date - in_dt).total_seconds() / 86400, 2)
                supabase.table("as_history").update({"출고일": date.strftime('%Y-%m-%d'), "tat": tat, "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.rerun()

# --- 4. 리포트 (필터 3종 복구) ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    if res.data:
        data = pd.DataFrame(res.data)
        st.subheader("📊 AS 분석 현황")
        
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("🏢 공급업체", sorted(data['공급업체명'].unique()) if '공급업체명' in data.columns else [])
        g_f = c2.multiselect("📂 분류구분", sorted(data['분류구분'].unique()) if '분류구분' in data.columns else [])
        s_f = c3.multiselect("🚚 상태", sorted(data['상태'].unique()) if '상태' in data.columns else [])
        
        dff = data.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if g_f: dff = dff[dff['분류구분'].isin(g_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]

        m1, m2, m3 = st.columns(3)
        m1.metric("총 건수", f"{len(dff)} 건")
        m2.metric("미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        if 'tat' in dff.columns:
            m3.metric("평균 TAT", f"{round(pd.to_numeric(dff['tat']).mean(), 1)} 일")

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            dff.to_excel(writer, index=False)
        st.download_button("📥 엑셀 결과 다운로드", buffer.getvalue(), "AS_Report.xlsx")
        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
