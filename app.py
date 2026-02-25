import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (VLOOKUP 보강 & 결과 유지)")

def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        for key in list(st.session_state.keys()): del st.session_state[key] # 세션 초기화
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 삭제 진행 중... (ID: {ids[-1]})")
        st.success("초기화 완료")
        st.rerun()

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 마스터(A:코드, G:내역)와 입고 파일을 함께 올려주세요.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        prog_bar = st.progress(0)
        # [VLOOKUP 로직] A열(0) 자재번호 기준 -> G열(6) 자재내역 매칭
        m_df = pd.read_excel(m_file, dtype=str)
        m_lookup = {}
        for _, row in m_df.iterrows():
            mat_id = sanitize_code(row.iloc[0]) # A열
            if mat_id:
                m_lookup[mat_id] = {
                    "자재내역": str(row.iloc[6]).strip() if len(row) > 6 else "내역없음", # G열(Index 6)
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락", # F열
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락" # K열
                }
        
        i_df = pd.read_excel(i_file, dtype=str)
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total = [], len(as_in)
        for i, (_, row) in enumerate(as_in.iterrows()):
            cur_mat = sanitize_code(row.iloc[3]) # D열 자재번호
            m_info = m_lookup.get(cur_mat)
            recs.append({
                "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                "자재번호": cur_mat,
                "자재내역": m_info['자재내역'] if m_info else "미등록",
                "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                "상태": "출고 대기",
                "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) >= 200:
                supabase.table("as_history").insert(recs).execute()
                recs = []
                prog_bar.progress((i+1)/total)
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("입고 및 VLOOKUP 매칭 완료")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            total_out, out_prog, processed = len(as_out), st.progress(0), 0
            for out_date, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    batch = codes[j:j+200]
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                    processed += len(batch)
                    out_prog.progress(processed / total_out)
            st.success("출고 업데이트 완료")

with tab3:
    # [분석 결과 유지 로직] 세션 스테이트 초기화
    if "analysis_done" not in st.session_state:
        st.session_state.analysis_done = False
        st.session_state.df_tat = None
        st.session_state.df_stay = None
        st.session_state.df_total = None

    if st.button("📈 데이터 분석 시작", use_container_width=True):
        df_raw_list = []
        last_id = -1
        load_msg = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            df_raw_list.extend(res.data)
            last_id = res.data[-1]['id']
            load_msg.info(f"📥 데이터 수집 중... ({len(df_raw_list):,}건)")
        
        if df_raw_list:
            df_all = pd.DataFrame(df_raw_list)
            df_all['입고일'] = pd.to_datetime(df_all['입고일'])
            df_all['출고일'] = pd.to_datetime(df_all['출고일'])
            df_all.loc[df_all['입고일'] > df_all['출고일'], '출고일'] = pd.NaT
            df_all['TAT'] = (df_all['출고일'] - df_all['입고일']).dt.days
            
            # 컬럼 배열 설정
            cols_order = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def format_df(df):
                t = df.copy()
                t['입고일자'] = t['입고일'].dt.strftime('%Y-%m-%d')
                t.loc[t['출고일'].isna(), '출고일자'] = "미출고(재입고)"
                return t.reindex(columns=cols_order)

            # 결과 저장
            st.session_state.df_tat = format_df(df_all[(df_all['분류구분'].str.contains('수리대상', na=False)) & (df_all['출고일'].notna())])
            st.session_state.df_stay = format_df(df_all[(df_all['분류구분'].str.contains('수리대상', na=False)) & (df_all['출고일'].isna())])
            st.session_state.df_total = format_df(df_all[df_all['분류구분'].str.contains('수리대상', na=False)])
            st.session_state.analysis_done = True
            load_msg.empty()

    # 분석이 완료된 상태라면 다운로드 버튼들을 계속 노출
    if st.session_state.analysis_done:
        st.divider()
        c1, c2, c3 = st.columns(3)
        
        with c1:
            st.subheader("✅ TAT 완료")
            st.metric("완료", f"{len(st.session_state.df_tat):,}건")
            xlsx_tat = io.BytesIO()
            st.session_state.df_tat.to_excel(xlsx_tat, index=False, engine='xlsxwriter')
            st.download_button("📥 1. 완료 리포트", xlsx_tat.getvalue(), "1_TAT_Completed.xlsx")

        with c2:
            st.subheader("⚠️ 미출고/재입고")
            st.metric("잔류", f"{len(st.session_state.df_stay):,}건")
            xlsx_stay = io.BytesIO()
            st.session_state.df_stay.to_excel(xlsx_stay, index=False, engine='xlsxwriter')
            st.download_button("📥 2. 미출고 명단", xlsx_stay.getvalue(), "2_Not_Shipped.xlsx")

        with c3:
            st.subheader("📊 전체 데이터")
            st.metric("총합", f"{len(st.session_state.df_total):,}건")
            xlsx_all = io.BytesIO()
            st.session_state.df_total.to_excel(xlsx_all, index=False, engine='xlsxwriter')
            st.download_button("📥 3. 전체 리포트", xlsx_all.getvalue(), "3_Total_Data.xlsx")
        
        st.write("🔍 리포트 미리보기")
        st.dataframe(st.session_state.df_total.head(10))
