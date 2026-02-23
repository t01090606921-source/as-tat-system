import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (전수 데이터 동기화 모드)")

# --- 2. 사이드바: 마스터 데이터 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [핵심] 1,000건 제한 없이 마스터 데이터 전수 로드 함수
    def get_all_master_data():
        all_data = []
        limit = 1000
        offset = 0
        while True:
            res = supabase.table("master_data").select("*").range(offset, offset + limit - 1).execute()
            all_data.extend(res.data)
            if len(res.data) < limit:
                break
            offset += limit
        return pd.DataFrame(all_data)

    try:
        m_df_local = get_all_master_data()
        st.metric("현재 DB 마스터 로드 완료", f"{len(m_df_local):,}")
    except:
        m_df_local = pd.DataFrame()

    st.subheader("1. 마스터 갱신")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df_raw = pd.read_excel(master_file, dtype=str)
        t_col = next((c for c in m_df_raw.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df_raw.columns[0])
        
        m_data = []
        for _, row in m_df_raw.iterrows():
            # 공백 제거 및 대문자화 (VLOOKUP 호환용)
            mat_val = str(row[t_col]).strip().upper()
            if not mat_val or mat_val == "NAN": continue
            m_data.append({
                "자재번호": mat_val,
                "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"
            })
        
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success(f"✅ {len(m_data)}건 DB 등록 성공!")
            st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 3. 입고 처리 (VLOOKUP 방식 메모리 매칭) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_final")
    if in_file and st.button("입고 및 매칭 실행"):
        if m_df_local.empty:
            st.error("마스터 데이터가 없습니다. 먼저 마스터를 등록해주세요.")
        else:
            # 매칭용 룩업 딕셔너리 생성 (가장 빠르고 정확함)
            m_df_local['자재번호'] = m_df_local['자재번호'].str.strip().str.upper()
            m_lookup = m_df_local.set_index('자재번호').to_dict('index')
            
            df = pd.read_excel(in_file, dtype=str)
            as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            
            recs = []
            for _, row in as_in.iterrows():
                mat_val = str(row.iloc[3]).strip().upper()
                
                # 메모리에서 즉시 VLOOKUP 수행
                m_info = m_lookup.get(mat_val)
                
                recs.append({
                    "압축코드": str(row.iloc[7]).strip(),
                    "자재번호": mat_val,
                    "규격": str(row.iloc[5]).strip(),
                    "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
            
            if recs:
                for i in range(0, len(recs), 200):
                    supabase.table("as_history").insert(recs[i:i+200]).execute()
                st.success(f"{len(recs)}건 입고 완료!")
                st.rerun()

# --- 4. 출고 처리 ---
with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_final")
    if out_file and st.button("출고 실행"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key, date = str(row.iloc[10]).strip(), pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id").match({"압축코드": key, "상태": "출고 대기"}).limit(1).execute()
            if target.data:
                supabase.table("as_history").update({"출고일": date.strftime('%Y-%m-%d'), "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.success("출고 처리 완료")
        st.rerun()

# --- 5. 리포트 현황 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    if res.data:
        df_res = pd.DataFrame(res.data)
        st.subheader("📊 현황 리포트")
        
        c1, c2 = st.columns(2)
        m_v = c1.multiselect("🏢 공급업체 필터", sorted(df_res['공급업체명'].unique()))
        m_s = c2.multiselect("🚚 상태 필터", sorted(df_res['상태'].unique()))
        
        dff = df_res.copy()
        if m_v: dff = dff[dff['공급업체명'].isin(m_v)]
        if m_s: dff = dff[dff['상태'].isin(m_s)]
        
        st.metric("미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
