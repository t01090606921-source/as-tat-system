import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (명칭 최적화 버전)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        try:
            supabase.table("as_history").delete().neq("id", -1).execute()
            st.success("데이터베이스 초기화 완료")
            st.rerun()
        except Exception as e:
            st.error(f"삭제 오류: {e}")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 '입고일자'와 '자재번호' 등 제목줄 명칭을 자동으로 분석하여 업로드합니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 업로드", type=['xlsx'], key="master_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀 업로드", type=['xlsx'], key="in_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        # [컬럼 위치 탐색 함수]
        def find_idx(columns, keywords):
            for i, col in enumerate(columns):
                if any(k in str(col).replace(" ", "") for k in keywords):
                    return i
            return -1

        # 1. 마스터 데이터 분석
        m_df = pd.read_excel(m_file).dropna(how='all').fillna("")
        m_cols = m_df.columns.tolist()
        
        m_idx_code = find_idx(m_cols, ["품목코드", "자재번호", "MaterialCode"])
        m_idx_name = find_idx(m_cols, ["자재명", "자재내역", "Description"])
        m_idx_vend = find_idx(m_cols, ["공급업체", "Vendor"])
        m_idx_type = find_idx(m_cols, ["분류", "구분", "Type"])

        m_lookup = {}
        for _, row in m_df.iterrows():
            code = sanitize_code(row.iloc[m_idx_code]) if m_idx_code != -1 else ""
            if code:
                m_lookup[code] = {
                    "자재내역": str(row.iloc[m_idx_name]).strip() if m_idx_name != -1 else "",
                    "공급업체명": str(row.iloc[m_idx_vend]).strip() if m_idx_vend != -1 else "",
                    "분류구분": str(row.iloc[m_idx_type]).strip() if m_idx_type != -1 else ""
                }

        # 2. 입고 데이터 분석
        try:
            i_df = pd.read_excel(i_file).dropna(how='all').fillna("")
            i_cols = i_df.columns.tolist()

            # 입고파일 키워드 매칭 (입고일자 추가)
            idx_in_date = find_idx(i_cols, ["입고일자", "입고일", "Date"])
            idx_in_mat  = find_idx(i_cols, ["자재번호", "품목코드", "Material"])
            idx_in_spec = find_idx(i_cols, ["규격", "Spec"])
            idx_in_comp = find_idx(i_cols, ["압축코드", "바코드", "Barcode"])

            # 필수 컬럼 체크 (진단 기능)
            missing = []
            if idx_in_date == -1: missing.append("입고일자(또는 입고일)")
            if idx_in_mat == -1: missing.append("자재번호")
            if idx_in_comp == -1: missing.append("압축코드")

            if missing:
                st.error(f"❌ 엑셀에서 다음 컬럼을 찾을 수 없습니다: {', '.join(missing)}")
                st.info(f"현재 인식된 컬럼명들: {i_cols}")
            else:
                # 'A/S 철거' 포함 행 추출
                as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
                total = len(as_in)
                
                if total == 0:
                    st.warning("⚠️ 'A/S 철거' 데이터가 0건입니다. 1열을 확인하세요.")
                else:
                    recs = []
                    p_bar = st.progress(0)
                    for i, (_, row) in enumerate(as_in.iterrows()):
                        cur_mat = sanitize_code(row.iloc[idx_in_mat])
                        m_info = m_lookup.get(cur_mat, {})
                        
                        try: 
                            # 다양한 날짜 형식 대응
                            dt = pd.to_datetime(row.iloc[idx_in_date])
                            in_date = dt.strftime('%Y-%m-%d')
                        except: in_date = "1900-01-01"

                        recs.append({
                            "압축코드": str(row.iloc[idx_in_comp]).strip(),
                            "자재번호": cur_mat,
                            "자재내역": m_info.get("자재내역", "미등록"),
                            "규격": str(row.iloc[idx_in_spec]).strip() if idx_in_spec != -1 else "",
                            "상태": "출고 대기",
                            "공급업체명": m_info.get("공급업체명", "미등록"),
                            "분류구분": m_info.get("분류구분", "미등록"),
                            "입고일": in_date
                        })
                        
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                            p_bar.progress((i+1)/total)
                    
                    if recs: supabase.table("as_history").insert(recs).execute()
                    st.success(f"🎊 {total:,}건 입고 성공!")
        except Exception as e:
            st.error(f"치명적 오류 발생: {e}")

# --- 출고/분석 탭은 이전 세션 유지 & 바이너리 다운로드 로직 유지 ---
with tab2:
    st.info("📤 출고 엑셀을 업로드하세요. (압축코드 기준 매칭)")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 업데이트 시작"):
        # (기존 출고 업데이트 로직과 동일)
        df_out = pd.read_excel(out_file).dropna(how='all').fillna("")
        as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            for d, c in date_groups.items():
                for j in range(0, len(c), 200):
                    supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", c[j:j+200]).execute()
            st.success("✅ 출고 완료")

with tab3:
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False
    
    if st.button("📈 데이터 분석 시작", use_container_width=True):
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
                t = target_df.copy()
                t['입고일자'] = t['입고일'].dt.strftime('%Y-%m-%d')
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                    t.reindex(columns=cols).to_excel(wr, index=False)
                return out.getvalue()

            f_df = df[df['분류구분'].str.contains('수리대상', na=False)]
            st.session_state.bin_tat = make_bin(f_df[f_df['출고일'].notna()])
            st.session_state.bin_stay = make_bin(f_df[f_df['출고일'].isna()])
            st.session_state.bin_total = make_bin(f_df)
            st.session_state.data_ready = True
            st.rerun()

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2: st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3: st.download_button("📥 3. 전체 데이터", st.session_state.bin_total, "3_Total_Data.xlsx")
