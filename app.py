import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (기능 복구 완료)")

# --- 2. 사이드바: 관리 기능 (버튼들 부활) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # 마스터 건수 확인
    try:
        m_count = supabase.table("master_data").select("자재번호", count="exact").execute().count
        st.info(f"📊 마스터 DB: {m_count if m_count else 0:,} 건")
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 갱신", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        # 품목코드 또는 자재번호 열 찾기 (없으면 첫 열)
        t_col = next((c for c in m_df.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df.columns[0])
        m_data = [{"자재번호": str(row[t_col]).strip(), 
                   "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                   "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"} 
                  for _, row in m_df.iterrows() if not pd.isna(row[t_col])]
        
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 갱신 완료")
            st.rerun()

    st.divider()
    st.subheader("2. 데이터 관리")
    # 🔥 초기화 버튼 부활
    if st.button("⚠️ 전체 데이터 초기화", type="primary", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.warning("입고/출고 데이터가 삭제되었습니다.")
        st.rerun()

# --- 3. 입고/출고 처리 ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in")
    if in_file and st.button("입고 처리 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {str(r['자재번호']): r for r in m_res.data}
        recs = []
        for _, row in as_in.iterrows():
            mat = str(row.iloc[3]).strip()
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
            st.success(f"{len(recs)}건 입고 완료")
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out")
    if out_file and st.button("출고 매칭 실행"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key, date = str(row.iloc[10]).strip(), pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id").match({"압축코드": key, "상태": "출고 대기"}).limit(1).execute()
            if target.data:
                supabase.table("as_history").update({"출고일": date.strftime('%Y-%m-%d'), "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.success("출고 처리 완료")
        st.rerun()

# --- 4. 리포트 & 검색 필터 & 다운로드 (모두 부활) ---
st.divider()
res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
if res.data:
    df_res = pd.DataFrame(res.data)
    
    st.subheader("📊 AS 분석 현황")
    
    # 🔍 검색 필터 3종 부활
    c1, c2, c3 = st.columns(3)
    v_f = c1.multiselect("🏢 공급업체 필터", sorted(df_res['공급업체명'].unique()))
    g_f = c2.multiselect("📂 분류구분 필터", sorted(df_res['분류구분'].unique()))
    s_f = c3.multiselect("🚚 상태 필터", sorted(df_res['상태'].unique()))
    
    dff = df_res.copy()
    if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
    if g_f: dff = dff[dff['분류구분'].isin(g_f)]
    if s_f: dff = dff[dff['상태'].isin(s_f)]

    # 지표 요약
    m1, m2 = st.columns(2)
    m1.metric("전체 건수", f"{len(dff)} 건")
    m2.metric("미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")

    # 📥 엑셀 다운로드 버튼 부활
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dff.to_excel(writer, index=False, sheet_name='Sheet1')
    st.download_button(label="📥 엑셀 결과 다운로드", data=output.getvalue(), file_name="AS_TAT_Report.xlsx")

    st.dataframe(dff, use_container_width=True, hide_index=True)
else:
    st.info("데이터가 없습니다. 입고 파일을 업로드해 주세요.")
