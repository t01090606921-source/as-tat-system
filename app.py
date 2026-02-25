import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (초고속 필터링 버전)")

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
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.warning("⚠️ 파일이 크면 'A/S 철거' 필터링에 시간이 걸릴 수 있습니다. (진행표시를 확인하세요)")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 초고속 입고 시작"):
        status_text = st.empty()
        p_bar = st.progress(0)
        
        try:
            # [1] 마스터 로드 (Dictionary 활용으로 매칭 속도 극대화)
            status_text.info("🔍 마스터 엑셀 분석 중...")
            m_df = pd.read_excel(m_file, engine='openpyxl').dropna(how='all').fillna("")
            m_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
            } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}

            # [2] 입고 파일 로드 (가장 빠른 방식으로 로드)
            status_text.info("📄 입고 파일 로딩 중... (파일 크기에 따라 1~3분 소요)")
            # 전체를 한 번에 읽지 않고 필요한 열만 리스트로 변환하여 처리
            i_df = pd.read_excel(i_file, engine='openpyxl', usecols="A,B,D,E,F,H").fillna("")
            
            # [3] 리스트 기반 초고속 필터링 (Pandas 필터링보다 안전함)
            status_text.info("⚙️ 'A/S 철거' 대상 추출 및 DB 변환 중...")
            raw_data = i_df.values.tolist() # 리스트로 변환하여 메모리 효율 최적화
            total_raw = len(raw_data)
            
            recs = []
            valid_count = 0
            
            for i, row in enumerate(raw_data):
                # 주기적으로 진행 상태 표시 (사용자 안심용)
                if i % 500 == 0:
                    p_bar.progress(min(i / total_raw, 0.9))
                    status_text.info(f"⚙️ 데이터 검사 중... ({i:,} / {total_raw:,} 행)")

                # A열(0)에 'A/S 철거'가 포함된 경우만 처리
                if 'A/S 철거' in str(row[0]):
                    try:
                        cur_mat = sanitize_code(row[2]) # D열(인덱스 2)
                        m_info = m_lookup.get(cur_mat, {})
                        
                        try: in_date = pd.to_datetime(row[1]).strftime('%Y-%m-%d')
                        except: in_date = "1900-01-01"

                        recs.append({
                            "압축코드": str(row[5]).strip(), # H열(5)
                            "자재번호": cur_mat,
                            "자재내역": str(row[3]).strip(), # E열(3)
                            "규격": str(row[4]).strip(),     # F열(4)
                            "상태": "출고 대기",
                            "공급업체명": m_info.get("업체", "미등록"),
                            "분류구분": m_info.get("분류", "수리대상"),
                            "입고일": in_date
                        })
                        valid_count += 1
                        
                        # 200건 단위 DB 전송
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                    except: continue

            if recs:
                supabase.table("as_history").insert(recs).execute()
            
            p_bar.progress(1.0)
            status_text.success(f"🎊 완료! 총 {valid_count:,}건의 AS 데이터를 입고했습니다.")
            
        except Exception as e:
            st.error(f"❌ 중단됨: {e}")

# --- 출고 및 분석 탭 (안정화된 이전 코드 유지) ---
with tab2:
    st.info("📤 출고 엑셀 업로드 (압축코드 매칭)")
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 시작"):
        df_out = pd.read_excel(out_file, engine='openpyxl').dropna(how='all').fillna("")
        as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            for d, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+200]).execute()
            st.success(f"✅ {len(as_out):,}건 업데이트 완료")

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
        with c1: st.download_button("📥 1. 완료", st.session_state.bin_tat, "1_TAT.xlsx")
        with c2: st.download_button("📥 2. 미출고", st.session_state.bin_stay, "2_Stay.xlsx")
        with c3: st.download_button("📥 3. 전체", st.session_state.bin_total, "3_Total.xlsx")
