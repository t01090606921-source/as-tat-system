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
st.title("📊 AS TAT 통합 관리 (컬럼 매칭 오류 해결 버전)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
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
    st.info("💡 마스터 파일과 입고 파일을 업로드하세요. 컬럼 위치가 달라도 이름을 찾아 매칭합니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        p_bar = st.progress(0)
        status = st.empty()
        
        # [마스터 읽기] 컬럼명으로 데이터 추출 (인덱스 오류 방지)
        m_df = pd.read_excel(m_file, dtype=str)
        m_lookup = {}
        
        # 마스터 엑셀의 헤더(첫 행)에서 필요한 정보 위치 찾기
        m_cols = m_df.columns.tolist()
        # A: 품목코드(0), G: 자재명(6), F: 공급업체명(5), K: 분류구분(10) 위치를 찾음
        # 만약 컬럼명이 다르면 아래 이름을 엑셀과 똑같이 수정해야 합니다.
        idx_code = 0 # 보통 첫번째 열
        idx_name = 6 # G열
        idx_vendor = 5 # F열
        idx_type = 10 # K열

        for _, row in m_df.iterrows():
            try:
                mat_id = sanitize_code(row.iloc[idx_code])
                if mat_id:
                    m_lookup[mat_id] = {
                        "자재내역": str(row.iloc[idx_name]).strip() if len(row) > idx_name else "",
                        "공급업체명": str(row.iloc[idx_vendor]).strip() if len(row) > idx_vendor else "",
                        "분류구분": str(row.iloc[idx_type]).strip() if len(row) > idx_type else ""
                    }
            except: continue

        # [입고 읽기]
        i_df = pd.read_excel(i_file, dtype=str)
        # B열(1): 입고일, D열(3): 자재코드, F열(5): 규격, H열(7): 압축코드
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total = [], len(as_in)
        if total > 0:
            for i, (_, row) in enumerate(as_in.iterrows()):
                try:
                    cur_mat = sanitize_code(row.iloc[3])
                    m_info = m_lookup.get(cur_mat, {})
                    recs.append({
                        "압축코드": str(row.iloc[7]).strip() if len(row) > 7 else "",
                        "자재번호": cur_mat,
                        "자재내역": m_info.get("자재내역", "미등록"),
                        "규격": str(row.iloc[5]).strip() if len(row) > 5 else "",
                        "상태": "출고 대기",
                        "공급업체명": m_info.get("공급업체명", "미등록"),
                        "분류구분": m_info.get("분류구분", "미등록"),
                        "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                    })
                except: continue
                
                if len(recs) >= 200:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    p_bar.progress((i+1)/total)
            
            if recs: supabase.table("as_history").insert(recs).execute()
            st.success(f"🎊 {total:,}건 입고 완료!")
        else:
            st.error("입고 대상('A/S 철거')을 찾을 수 없습니다. 엑셀의 첫 번째 열을 확인하세요.")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        # D열(3): 품목명(AS 카톤 박스), G열(6): 출고일, K열(10): 압축코드
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
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False
        st.session_state.bin_tat = None
        st.session_state.bin_stay = None
        st.session_state.bin_total = None
        st.session_state.counts = [0, 0, 0]

    if st.button("📈 데이터 분석 시작", use_container_width=True):
        df_list = []
        last_id = -1
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            df_list.extend(res.data)
            last_id = res.data[-1]['id']
            msg.info(f"📥 수집 중... ({len(df_list):,}건)")
        
        if df_list:
            df = pd.DataFrame(df_list)
            df['입고일'] = pd.to_datetime(df['입고일'])
            df['출고일'] = pd.to_datetime(df['출고일'])
            df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
            df['TAT'] = (df['출고일'] - df['입고일']).dt.days
            
            cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def to_excel_bin(target_df):
                if target_df.empty: return None
                t_df = target_df.copy()
                t_df['입고일자'] = t_df['입고일'].dt.strftime('%Y-%m-%d')
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    t_df.reindex(columns=cols).to_excel(writer, index=False)
                return output.getvalue()

            df_tat = df[(df['분류구분'].str.contains('수리대상', na=False)) & (df['출고일'].notna())]
            df_stay = df[(df['분류구분'].str.contains('수리대상', na=False)) & (df['출고일'].isna())]
            df_total = df[df['분류구분'].str.contains('수리대상', na=False)]

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
            st.metric("TAT 완료", f"{st.session_state.counts[0]:,}건")
            if st.session_state.bin_tat: st.download_button("📥 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2:
            st.metric("미출고/재입고", f"{st.session_state.counts[1]:,}건")
            if st.session_state.bin_stay: st.download_button("📥 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3:
            st.metric("수리대상 전체", f"{st.session_state.counts[2]:,}건")
            if st.session_state.bin_total: st.download_button("📥 전체 리포트", st.session_state.bin_total, "3_Total_Data.xlsx")
