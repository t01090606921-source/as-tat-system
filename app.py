import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (컬럼 이름 자동 매칭 버전)")

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
    st.info("💡 엑셀의 제목줄(Header)을 기준으로 데이터를 찾습니다. 제목줄 위치를 확인하세요.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 업로드", type=['xlsx'])
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀 업로드", type=['xlsx'])

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        # [마스터 로드 및 컬럼 분석]
        m_df = pd.read_excel(m_file).dropna(how='all').fillna("")
        m_lookup = {}
        
        # 컬럼 인덱스 자동 찾기 (이름 기준)
        def find_idx(df, keywords):
            for i, col in enumerate(df.columns):
                if any(k in str(col) for k in keywords): return i
            return -1

        m_idx_code = find_idx(m_df, ["품목코드", "자재번호", "Material"])
        m_idx_name = find_idx(m_df, ["자재내역", "자재명", "Description"])
        m_idx_vend = find_idx(m_df, ["공급업체", "Vendor"])
        m_idx_type = find_idx(m_df, ["분류", "구분"])

        for _, row in m_df.iterrows():
            try:
                code = sanitize_code(row.iloc[m_idx_code])
                if code:
                    m_lookup[code] = {
                        "자재내역": str(row.iloc[m_idx_name]) if m_idx_name != -1 else "",
                        "공급업체명": str(row.iloc[m_idx_vend]) if m_idx_vend != -1 else "",
                        "분류구분": str(row.iloc[m_idx_type]) if m_idx_type != -1 else ""
                    }
            except: continue

        # [입고 데이터 처리]
        try:
            i_df = pd.read_excel(i_file).dropna(how='all').fillna("")
            # 'A/S 철거' 필터링 (첫 번째 열 기준)
            as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            
            i_idx_date = find_idx(i_df, ["입고일", "일자", "Date"])
            i_idx_mat  = find_idx(i_df, ["자재번호", "품목코드"])
            i_idx_spec = find_idx(i_df, ["규격", "Spec"])
            i_idx_comp = find_idx(i_df, ["압축코드", "바코드"])

            recs = []
            for i, (_, row) in enumerate(as_in.iterrows()):
                try:
                    cur_mat = sanitize_code(row.iloc[i_idx_mat])
                    m_info = m_lookup.get(cur_mat, {})
                    
                    try: in_date = pd.to_datetime(row.iloc[i_idx_date]).strftime('%Y-%m-%d')
                    except: in_date = "1900-01-01"

                    recs.append({
                        "압축코드": str(row.iloc[i_idx_comp]).strip() if i_idx_comp != -1 else "",
                        "자재번호": cur_mat,
                        "자재내역": m_info.get("자재내역", "미등록"),
                        "규격": str(row.iloc[i_idx_spec]).strip() if i_idx_spec != -1 else "",
                        "상태": "출고 대기",
                        "공급업체명": m_info.get("공급업체명", "미등록"),
                        "분류구분": m_info.get("분류구분", "미등록"),
                        "입고일": in_date
                    })
                except: continue
                
                if len(recs) >= 200:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
            
            if recs: supabase.table("as_history").insert(recs).execute()
            st.success("🎊 입고 완료!")
        except Exception as e:
            st.error(f"입고 실패: {e}. 엑셀의 제목줄 이름이 '자재번호', '입고일' 등인지 확인하세요.")

with tab2:
    st.info("💡 출고 엑셀 업로드 (압축코드와 출고일자 기준)")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'])
    if out_file and st.button("🚀 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file).dropna(how='all').fillna("")
        as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            # 출고일(G열:6), 압축코드(K열:10) - 이 부분도 이름 기반으로 자동화 가능
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            
            for out_date, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", codes[j:j+200]).execute()
            st.success("✅ 출고 업데이트 완료")

with tab3:
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False
        st.session_state.bin_tat = None
        st.session_state.bin_stay = None
        st.session_state.bin_total = None

    if st.button("📈 데이터 분석 시작", use_container_width=True):
        res = supabase.table("as_history").select("*").execute()
        if res.data:
            df = pd.DataFrame(res.data)
            df['입고일'] = pd.to_datetime(df['입고일'])
            df['출고일'] = pd.to_datetime(df['출고일'])
            df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
            df['TAT'] = (df['출고일'] - df['입고일']).dt.days
            
            cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def to_bin(target_df):
                if target_df.empty: return None
                t = target_df.copy()
                t['입고일자'] = t['입고일'].dt.strftime('%Y-%m-%d')
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                    t.reindex(columns=cols).to_excel(wr, index=False)
                return out.getvalue()

            df_f = df[df['분류구분'].str.contains('수리대상', na=False)]
            st.session_state.bin_tat = to_bin(df_f[df_f['출고일'].notna()])
            st.session_state.bin_stay = to_bin(df_f[df_f['출고일'].isna()])
            st.session_state.bin_total = to_bin(df_f)
            st.session_state.data_ready = True
            st.rerun()

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2: st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3: st.download_button("📥 3. 전체 데이터", st.session_state.bin_total, "3_Total_Data.xlsx")
