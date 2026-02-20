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

def ultimate_clean(val):
    """자재번호의 모든 노이즈 제거"""
    if pd.isna(val): return ""
    s = str(val).strip()
    s = unicodedata.normalize('NFKC', s)
    s = re.sub(r'[\x00-\x1f\x7f-\x9f\s]', '', s)
    if s.endswith('.0'): s = s[:-2]
    return s.upper()

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (최종 진단 모드)")

# --- 2. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.info(f"📊 마스터 DB 등록: {m_count.count} 건")
    except: pass

    st.subheader("1. 마스터 갱신")
    master_file = st.file_uploader("마스터 엑셀", type=['xlsx'])
    if master_file and st.button("🚀 마스터 갱신", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        m_data = []
        for _, row in m_df.iterrows():
            mat_no = ultimate_clean(row.iloc[0])
            if not mat_no: continue
            m_data.append({
                "자재번호": mat_no,
                "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) and str(row.iloc[5]).strip() != "" else "정보누락",
                "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) and str(row.iloc[10]).strip() != "" else "정보누락"
            })
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 완료")
            st.rerun()

    st.divider()
    st.subheader("2. 미등록 정밀 진단")
    if st.button("🔥 유연한 재매칭 실행", use_container_width=True):
        with st.spinner("마스터와 정밀 대조 중..."):
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            
            for row in h_res.data:
                cleaned_val = ultimate_clean(row['자재번호'])
                m_info = m_lookup.get(cleaned_val)
                
                # 만약 완전 일치가 없다면 부분 일치 검색 (자재번호가 마스터를 포함하는지)
                if not m_info:
                    for k, v in m_lookup.items():
                        if k in cleaned_val or cleaned_val in k:
                            m_info = v
                            break
                
                if m_info:
                    supabase.table("as_history").update({
                        "자재번호": cleaned_val,
                        "공급업체명": m_info['공급업체명'],
                        "분류구분": m_info['분류구분']
                    }).eq("id", row['id']).execute()
            st.success("✅ 보정 완료")
            st.rerun()

    st.divider()
    st.button("⚠️ 전체 삭제", on_click=lambda: supabase.table("as_history").delete().neq("id", -1).execute())

# --- 3. 입고/출고 ---
tab1, tab2 = st.tabs(["📥 입고", "📤 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in")
    if in_file and st.button("입고 처리"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {r['자재번호']: r for r in m_res.data}
        recs = []
        for _, row in as_in.iterrows():
            mat = ultimate_clean(row.iloc[3])
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
    if out_file and st.button("출고 매칭"):
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

# --- 4. 리포트 및 진단 데이터 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    data = pd.DataFrame(res.data)
    if not data.empty:
        st.subheader("📊 현황 리포트 (미등록 원인 진단)")
        
        # 진단 열 추가
        data['번호길이'] = data['자재번호'].apply(len)
        data.loc[data['공급업체명'] == '미등록', '진단결과'] = '❌ 마스터에 번호 없음'
        data.loc[data['공급업체명'] == '정보누락', '진단결과'] = '⚠️ 마스터에 업체명 비어있음'
        data.loc[data['공급업체명'].notin(['미등록', '정보누락']), '진단결과'] = '✅ 매칭 성공'

        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("🏢 업체", sorted(data['공급업체명'].unique()))
        s_f = c2.multiselect("🚚 상태", sorted(data['상태'].unique()))
        d_f = c3.multiselect("🔍 진단결과", sorted(data['진단결과'].unique()))
        
        dff = data.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]
        if d_f: dff = dff[dff['진단결과'].isin(d_f)]

        st.metric("현재 미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            dff.to_excel(writer, index=False)
        st.download_button("📥 리포트 다운로드", buffer.getvalue(), "AS_Report_Final.xlsx")

        # 테이블 표시
        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
