import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (CSV 고속 모드)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        with st.spinner("DB 초기화 중..."):
            supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 3. 메인 탭 ---
tab1, tab2, tab3 = st.tabs(["📥 CSV 고속 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 엑셀을 'CSV(쉼표로 분리)' 형식으로 저장한 후 업로드하세요. 로딩 속도가 압도적으로 빠릅니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 (XLSX 권장)", type=['xlsx', 'csv'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 CSV 업로드", type=['csv'], key="i_up")

    if m_file and i_file and st.button("🚀 CSV 입고 시작"):
        status_text = st.empty()
        p_bar = st.progress(0)
        
        try:
            # [1] 마스터 로드
            status_text.info("🔍 마스터 분석 중...")
            if m_file.name.endswith('.csv'):
                m_df = pd.read_csv(m_file, encoding='cp949').fillna("")
            else:
                m_df = pd.read_excel(m_file).fillna("")
                
            m_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
            } for _, row in m_df.iterrows()}

            # [2] 입고 CSV 로드 (인코딩 예외 처리)
            status_text.info("📄 CSV 파일 읽는 중...")
            try:
                i_df = pd.read_csv(i_file, encoding='cp949').fillna("")
            except:
                i_df = pd.read_csv(i_file, encoding='utf-8-sig').fillna("")
            
            # [3] 필터링 및 변환
            status_text.info("⚙️ 데이터 매칭 및 필터링 중...")
            as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            total = len(as_in)
            
            if total == 0:
                st.error("❌ 'A/S 철거' 항목을 찾지 못했습니다. CSV의 첫 번째 열을 확인하세요.")
            else:
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    if i % 100 == 0:
                        p_bar.progress(min((i + 1) / total, 1.0))
                        status_text.info(f"🚀 DB 저장 중... ({i+1:,} / {total:,}건)")

                    cur_mat = sanitize_code(row.iloc[3])
                    m_info = m_lookup.get(cur_mat, {})
                    
                    try: in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                    except: in_date = "1900-01-01"

                    recs.append({
                        "압축코드": str(row.iloc[7]).strip() if len(row) > 7 else "",
                        "자재번호": cur_mat,
                        "자재내역": str(row.iloc[4]).strip() if len(row) > 4 else "",
                        "규격": str(row.iloc[5]).strip() if len(row) > 5 else "",
                        "상태": "출고 대기",
                        "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"),
                        "입고일": in_date
                    })
                    
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []
                
                if recs:
                    supabase.table("as_history").insert(recs).execute()
                
                p_bar.progress(1.0)
                status_text.success(f"🎊 완료! CSV 기준 총 {total:,}건이 입고되었습니다.")
            
        except Exception as e:
            st.error(f"❌ CSV 오류 발생: {e}")

# --- 출고 및 분석 탭 (동일 로직 유지) ---
with tab2:
    st.info("📤 출고 엑셀 업로드")
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 시작"):
        try:
            df_out = pd.read_excel(out_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            if not as_out.empty:
                as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
                as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
                date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
                for d, codes in date_groups.items():
                    for j in range(0, len(codes), 200):
                        supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+200]).execute()
                st.success(f"✅ 업데이트 완료")
        except Exception as e: st.error(f"오류: {e}")

with tab3:
    if "data_ready" not in st.session_state: st.session_state.data_ready = False
    if st.button("📈 리포트 생성", use_container_width=True):
        res = supabase.table("as_history").select("*").execute()
        if res.data:
            df = pd.DataFrame(res.data)
            df['입고일'] = pd.to_datetime(df['입고일'])
            df['출고일'] = pd.to_datetime(df['출고일'])
            df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
            df['TAT'] = (df['출고일'] - df['입고일']).dt.days
            cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            def make_bin(target_df):
                if target_df.empty: return None
                t = target_df.copy(); t['입고일자'] = t['입고일'].dt.strftime('%Y-%m-%d')
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as wr: t.reindex(columns=cols).to_excel(wr, index=False)
                return out.getvalue()
            f_df = df[df['분류구분'].str.contains('수리대상', na=False)]
            st.session_state.bin_tat = make_bin(f_df[f_df['출고일'].notna()])
            st.session_state.bin_stay = make_bin(f_df[f_df['출고일'].isna()])
            st.session_state.bin_total = make_bin(f_df)
            st.session_state.data_ready = True; st.rerun()
    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 완료 리포트", st.session_state.bin_tat, "1.xlsx")
        with c2: st.download_button("📥 미출고 명단", st.session_state.bin_stay, "2.xlsx")
        with c3: st.download_button("📥 전체 리포트", st.session_state.bin_total, "3.xlsx")
