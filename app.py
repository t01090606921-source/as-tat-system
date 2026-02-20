import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io
import unicodedata

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

def super_clean(val):
    """자재번호의 모든 노이즈 제거 (전각/반각, 공백, 대문자)"""
    if pd.isna(val): return ""
    s = str(val).strip()
    s = unicodedata.normalize('NFKC', s) # 전각/반각 통일
    s = "".join(s.split()).upper()      # 모든 공백 제거 및 대문자화
    if s.endswith('.0'): s = s[:-2]     # 엑셀 숫자 흔적 제거
    return s

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (데이터 강제 교정 모드)")

# --- 2. 사이드바: 관리 및 강력 보정 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    
    try:
        m_count = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.info(f"📊 마스터 DB 등록: {m_count.count} 건")
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 갱신", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        m_data = []
        for _, row in m_df.iterrows():
            mat_no = super_clean(row.iloc[0])
            if not mat_no: continue
            m_data.append({
                "자재번호": mat_no,
                "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보없음",
                "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보없음"
            })
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 갱신 완료")
            st.rerun()

    st.divider()
    st.subheader("2. 강력 정보 보정")
    # 단순 매칭이 아니라 DB에 저장된 자재번호 자체를 다시 클리닝하여 업데이트함
    if st.button("🔥 미등록 강제 교정 및 재매칭", use_container_width=True):
        with st.spinner("DB 데이터 세척 및 대조 중..."):
            # 1. 마스터 로드
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            
            # 2. 히스토리 로드
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            up_cnt = 0
            
            for row in h_res.data:
                # DB에 저장된 자재번호를 다시 한번 super_clean 처리
                cleaned_val = super_clean(row['자재번호'])
                m_info = m_lookup.get(cleaned_val)
                
                # 업데이트 데이터 준비 (자재번호 자체도 깨끗하게 교정)
                update_payload = {"자재번호": cleaned_val}
                if m_info:
                    update_payload["공급업체명"] = m_info['공급업체명']
                    update_payload["분류구분"] = m_info['분류구분']
                
                supabase.table("as_history").update(update_payload).eq("id", row['id']).execute()
                if m_info: up_cnt += 1
                
            st.success(f"✅ {up_cnt}건 매칭 성공 및 DB 교정 완료!")
            st.rerun()

    st.divider()
    st.subheader("3. 초기화")
    if st.button("⚠️ 시스템 전체 초기화", type="primary", use_container_width=True):
        if st.checkbox("정말 삭제하시겠습니까?"):
            supabase.table("as_history").delete().neq("id", -1).execute()
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
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
            mat = super_clean(row.iloc[3])
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
            st.success("입고 완료")
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
        st.success("출고 완료")
        st.rerun()

# --- 4. 리포트 & 필터 & 다운로드 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    data = pd.DataFrame(res.data)
    if not data.empty:
        st.subheader("📊 현황 리포트")
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("공급업체", sorted(data['공급업체명'].unique()))
        g_f = c2.multiselect("분류구분", sorted(data['분류구분'].unique()))
        s_f = c3.multiselect("상태", sorted(data['상태'].unique()))
        
        dff = data.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if g_f: dff = dff[dff['분류구분'].isin(g_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]

        m1, m2, m3 = st.columns(3)
        m1.metric("전체 건수", f"{len(dff)} 건")
        m2.metric("미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        m3.metric("평균 TAT", f"{round(pd.to_numeric(dff['tat']).mean(), 1) if 'tat' in dff else 0} 일")

        # 엑셀 다운로드
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            dff.to_excel(writer, index=False)
        st.download_button("📥 리포트 다운로드", buffer.getvalue(), "AS_Report.xlsx")

        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
