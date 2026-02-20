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

def super_ultimate_clean(val):
    """자재번호/품목코드에서 기호, 공백 제거"""
    if pd.isna(val): return ""
    s = str(val).strip()
    s = unicodedata.normalize('NFKC', s)
    s = re.sub(r'[^a-zA-Z0-9]', '', s) # 영문, 숫자만 남김
    return s.upper()

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (품목코드 자동 매칭)")

# --- 2. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count_res = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.metric("현재 DB 등록 마스터", f"{m_count_res.count if m_count_res.count else 0:,} 건")
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        # 엑셀 로드
        m_df = pd.read_excel(master_file, dtype=str)
        
        # [중요] 열 이름 리스트에서 '품목코드' 또는 '자재번호'가 포함된 열 찾기
        col_list = list(m_df.columns)
        target_col_idx = -1
        for i, col in enumerate(col_list):
            if "품목코드" in str(col) or "자재번호" in str(col):
                target_col_idx = i
                break
        
        if target_col_idx != -1:
            m_data = []
            for _, row in m_df.iterrows():
                # 찾은 열 인덱스(target_col_idx)를 기준으로 데이터 추출
                mat_no = super_ultimate_clean(row.iloc[target_col_idx])
                if not mat_no: continue
                
                # 공급업체명(F열=index 5), 분류구분(K열=index 10) - 이미지 기준 고정
                m_data.append({
                    "자재번호": mat_no,
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보없음",
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보없음"
                })
            
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
                st.success(f"✅ '{col_list[target_col_idx]}' 열을 기준으로 {len(m_data)}건 등록 완료!")
                st.rerun()
        else:
            st.error("엑셀에서 '품목코드' 또는 '자재번호' 열을 찾을 수 없습니다.")

    st.divider()
    if st.button("🔥 남은 203건 강제 재매칭", use_container_width=True):
        with st.spinner("마스터 대조 중..."):
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            
            for row in h_res.data:
                c_val = super_ultimate_clean(row['자재번호'])
                m_info = m_lookup.get(c_val)
                if m_info:
                    supabase.table("as_history").update({
                        "공급업체명": m_info['공급업체명'], 
                        "분류구분": m_info['분류구분']
                    }).eq("id", row['id']).execute()
            st.success("보정 완료!")
            st.rerun()

# --- 3. 입고/출고 처리 (생략 없이 유지) ---
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
            mat = super_ultimate_clean(row.iloc[3])
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

# --- 4. 리포트 & 데이터 표시 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    if res.data:
        data = pd.DataFrame(res.data)
        st.subheader("📊 AS 분석 현황")
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("🏢 공급업체 필터", sorted(data['공급업체명'].unique()))
        g_f = c2.multiselect("📂 분류구분 필터", sorted(data['분류구분'].unique()))
        s_f = c3.multiselect("🚚 상태 필터", sorted(data['상태'].unique()))
        dff = data.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if g_f: dff = dff[dff['분류구분'].isin(g_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]
        m1, m2, m3 = st.columns(3)
        m1.metric("총 건수", f"{len(dff)} 건")
        m2.metric("미등록", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        if 'tat' in dff.columns:
            m3.metric("평균 TAT", f"{round(pd.to_numeric(dff['tat']).mean(), 1)} 일")
        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
