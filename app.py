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
st.title("📊 AS TAT 통합 관리 (오류 원천 차단 버전)")

def sanitize_code(val):
    if pd.isna(val) or val == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear() # 세션 전체 삭제
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 데이터 삭제 중...")
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
        p_bar = st.progress(0)
        status = st.empty()
        
        m_df = pd.read_excel(m_file, dtype=str)
        m_lookup = {}
        for _, row in m_df.iterrows():
            try:
                mat_id = sanitize_code(row.iloc[0])
                if mat_id:
                    m_lookup[mat_id] = {
                        "자재내역": str(row.iloc[6]).strip() if len(row) > 6 else "내역없음",
                        "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락",
                        "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락"
                    }
            except: continue

        i_df = pd.read_excel(i_file, dtype=str)
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total = [], len(as_in)
        if total > 0:
            for i, (_, row) in enumerate(as_in.iterrows()):
                cur_mat = sanitize_code(row.iloc[3])
                m_info = m_lookup.get(cur_mat, {})
                recs.append({
                    "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                    "자재번호": cur_mat,
                    "자재내역": m_info.get("자재내역", "미등록"),
                    "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                    "상태": "출고 대기",
                    "공급업체명": m_info.get("공급업체명", "미등록"),
                    "분류구분": m_info.get("분류구분", "미등록"),
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
                if len(recs) >= 200:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    p_bar.progress((i+1)/total)
            if recs: supabase.table("as_history").insert(recs).execute()
            st.success(f"🎊 {total:,}건 입고 완료!")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            total_out, processed = len(as_out), 0
            out_p = st.progress(0)
            for out_date, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    batch = codes[j:j+200]
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                    processed += len(batch)
                    out_p.progress(processed / total_out)
            st.success("출고 업데이트 완료")

with tab3:
    # [수정] 분석 데이터를 바이너리 형태로 세션에 저장하여 오류 방지
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False
        st.session_state.bin_tat = None
        st.session_state.bin_stay = None
        st.session_state.bin_total = None
        st.session_state.counts = [0, 0, 0]

    if st.button("📈 데이터 분석 시작 (결과 유지)", use_container_width=True):
        df_list = []
        last_id = -1
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            df_list.extend(res.data)
            last_id = res.data[-1]['id']
            msg.info(f"📥 데이터 수집 중... ({len(df_list):,}건)")
        
        if df_list:
            df = pd.DataFrame(df_list)
            df['입고일'] = pd.to_datetime(df['입고일'])
            df['출고일'] = pd.to_datetime(df['출고일'])
            df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
            df['TAT'] = (df['출고일'] - df['입고일']).dt.days
            
            # 컬럼 재배열
            cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def to_excel_bin(target_df):
                if target_df.empty: return None
                target_df['입고일자'] = target_df['입고일'].dt.strftime('%Y-%m-%d')
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    target_df.reindex(columns=cols).to_excel(writer, index=False)
                return output.getvalue()

            # 필터링
            df_tat = df[(df['분류구분'].str.contains('수리대상', na=False)) & (df['출고일'].notna())]
            df_stay = df[(df['분류구분'].str.contains('수리대상', na=False)) & (df['출고일'].isna())]
            df_total = df[df['분류구분'].str.contains('수리대상', na=False)]

            # 세션에 바이너리로 저장 (에러 방지 핵심)
            st.session_state.bin_tat = to_excel_bin(df_tat)
            st.session_state.bin_stay = to_excel_bin(df_stay)
            st.session_state.bin_total = to_excel_bin(df_total)
            st.session_state.counts = [len(df_tat), len(df_stay), len(df_total)]
            st.session_state.data_ready = True
            msg.empty()

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("✅ TAT 완료")
            st.metric("완료", f"{st.session_state.counts[0]:,}건")
            if st.session_state.bin_tat:
                st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2:
            st.subheader("⚠️ 미출고/재입고")
            st.metric("잔류", f"{st.session_state.counts[1]:,}건")
            if st.session_state.bin_stay:
                st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3:
            st.subheader("📊 전체 데이터")
            st.metric("총합", f"{st.session_state.counts[2]:,}건")
            if st.session_state.bin_total:
                st.download_button("📥 3. 전체 리포트", st.session_state.bin_total, "3_Total_Data.xlsx")
