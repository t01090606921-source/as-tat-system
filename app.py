import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (정밀 데이터 탐색 버전)")

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
tab1, tab2, tab3 = st.tabs(["📥 고속 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 CSV 파일에서 'A/S 철거' 문구를 자동 탐색하여 입고를 진행합니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx', 'csv'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 CSV", type=['csv'], key="i_up")

    if m_file and i_file and st.button("🚀 입고 시작"):
        status_text = st.empty()
        p_bar = st.progress(0)
        
        try:
            # [1] 마스터 로드
            status_text.info("🔍 마스터 분석 중...")
            m_df = pd.read_excel(m_file).fillna("")
            m_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
            } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}

            # [2] CSV 로드 (가장 강력한 인코딩 방식 적용)
            status_text.info("📄 CSV 읽는 중...")
            i_df = None
            # utf-8-sig는 엑셀에서 만든 CSV의 한글 깨짐을 방지하는 가장 좋은 방식입니다.
            for enc in ['utf-8-sig', 'cp949', 'utf-8', 'euc-kr']:
                try:
                    i_df = pd.read_csv(i_file, encoding=enc).fillna("")
                    if i_df.shape[1] > 1: break
                except: continue
            
            if i_df is None:
                st.error("❌ 파일을 읽을 수 없습니다.")
            else:
                # [3] 'A/S 철거' 탐색 로직 (열 위치에 상관없이 행 전체 검사)
                status_text.info("⚙️ 'A/S 철거' 데이터 탐색 중...")
                
                # 모든 열의 내용을 하나로 합쳐서 'A/S'와 '철거'가 있는지 확인
                # 공백을 제거하여 'A / S 철거' 같은 케이스도 대비합니다.
                combined_series = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                mask = combined_series.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)
                as_in = i_df[mask].copy()
                total = len(as_in)
                
                if total == 0:
                    st.error("❌ 'A/S 철거' 데이터를 여전히 찾지 못했습니다.")
                    st.write("상단 5행 데이터 미리보기:", i_df.head())
                else:
                    recs = []
                    for i, (_, row) in enumerate(as_in.iterrows()):
                        if i % 100 == 0:
                            p_bar.progress(min((i + 1) / total, 1.0))
                            status_text.info(f"🚀 입고 중... ({i+1:,} / {total:,}건)")

                        # 이미지 컬럼 기준: B(1)=입고일, D(3)=자재번호, E(4)=자재명, F(5)=규격, H(7)=압축코드
                        cur_mat = sanitize_code(row.iloc[3])
                        m_info = m_lookup.get(cur_mat, {})
                        
                        try:
                            in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except:
                            in_date = "1900-01-01"

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
                    status_text.success(f"🎊 {total:,}건 입고 완료!")
            
        except Exception as e:
            st.error(f"❌ 오류: {e}")

# --- 출고 및 분석 탭 (동일) ---
with tab2:
    st.info("📤 출고 엑셀 업로드")
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 시작"):
        df_out = pd.read_excel(out_file).fillna("")
        as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            for d, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+200]).execute()
            st.success("✅ 완료")

with tab3:
    if "data_ready" not in st.session_state: st.session_state.data_ready = False
    if st.button("📈 리포트 생성", use_container_width=True):
        res = supabase.table("as_history").select("*").execute()
        if res.data:
            df = pd.DataFrame(res.data)
            df['입고일'] = pd.to_datetime(df['입고일'])
            df['출고일'] = pd.to_datetime(df['출고일'])
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
        with c1: st.download_button("📥 완료", st.session_state.bin_tat, "1.xlsx")
        with c2: st.download_button("📥 미출고", st.session_state.bin_stay, "2.xlsx")
        with c3: st.download_button("📥 전체", st.session_state.bin_total, "3.xlsx")
