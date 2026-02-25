import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
# Streamlit의 Secrets 기능을 사용하여 보안 정보를 가져옵니다.
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("Supabase 접속 정보가 올바르지 않습니다. Secrets 설정을 확인하세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (최종 통합본)")

# 자재번호 등 코드 데이터를 깨끗하게 정리하는 함수
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    st.warning("데이터 삭제는 신중하게 결정하세요.")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        with st.spinner("DB 초기화 중..."):
            # ID가 -1이 아닌 모든 데이터를 삭제 (전체 삭제)
            supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("데이터베이스가 초기화되었습니다.")
        st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 데이터 관리 ---
with tab0:
    st.subheader("📋 마스터 데이터 최신화")
    st.info("마스터 엑셀을 한 번 업로드해두면 세션이 유지되는 동안 입고 시 자동으로 매칭됩니다.")
    m_file_mgmt = st.file_uploader("새로운 마스터 엑셀 업로드 (XLSX/CSV)", type=['xlsx', 'csv'], key="m_mgmt")
    
    if m_file_mgmt and st.button("🔄 마스터 데이터 메모리 로드"):
        with st.spinner("마스터 분석 중..."):
            try:
                if m_file_mgmt.name.endswith('.csv'):
                    m_df = pd.read_csv(m_file_mgmt, encoding='cp949').fillna("")
                else:
                    m_df = pd.read_excel(m_file_mgmt).fillna("")
                
                # 마스터 룩업 테이블 생성
                new_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                
                st.session_state.master_lookup = new_lookup
                st.success(f"✅ 마스터 데이터 로드 완료! (총 {len(new_lookup):,}건)")
            except Exception as e:
                st.error(f"마스터 로드 실패: {e}")

# --- [TAB 1] 입고 처리 ---
with tab1:
    st.info("💡 CSV 파일을 업로드하여 대용량 입고를 진행합니다.")
    col1, col2 = st.columns(2)
    with col1:
        if "master_lookup" in st.session_state:
            st.success("✅ 마스터 데이터가 준비되어 있습니다.")
            use_existing = st.checkbox("로드된 마스터 사용", value=True)
        else:
            use_existing = False
            st.warning("⚠️ [마스터 관리] 탭에서 마스터를 먼저 로드하세요.")
        
        m_file_manual = None
        if not use_existing:
            m_file_manual = st.file_uploader("1. 마스터 엑셀 직접 업로드", type=['xlsx', 'csv'], key="m_manual")
            
    with col2: 
        i_file = st.file_uploader("2. AS 입고 CSV 업로드", type=['csv'], key="i_up")

    if i_file and st.button("🚀 입고 시작"):
        status_text = st.empty()
        p_bar = st.progress(0)
        
        try:
            # 마스터 데이터 준비
            lookup = None
            if use_existing:
                lookup = st.session_state.master_lookup
            elif m_file_manual:
                if m_file_manual.name.endswith('.csv'):
                    m_df_m = pd.read_csv(m_file_manual, encoding='cp949').fillna("")
                else:
                    m_df_m = pd.read_excel(m_file_manual).fillna("")
                lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df_m.iterrows()}

            if not lookup:
                st.error("마스터 데이터가 없습니다.")
            else:
                # 입고 CSV 로드 (인코딩 자동 시도)
                i_df = None
                for enc in ['utf-8-sig', 'cp949', 'utf-8', 'euc-kr']:
                    try:
                        i_df = pd.read_csv(i_file, encoding=enc).fillna("")
                        if i_df.shape[1] > 1: break
                    except: continue
                
                if i_df is not None:
                    status_text.info("⚙️ 데이터 선별 중...")
                    combined_series = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                    mask = combined_series.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)
                    as_in = i_df[mask].copy()
                    total = len(as_in)
                    
                    if total > 0:
                        recs = []
                        for i, (_, row) in enumerate(as_in.iterrows()):
                            if i % 100 == 0:
                                p_bar.progress(min((i + 1) / total, 1.0))
                                status_text.info(f"🚀 DB 저장 중... ({i+1:,} / {total:,}건)")
                            
                            cur_mat = sanitize_code(row.iloc[3])
                            m_info = lookup.get(cur_mat, {})
                            try: in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                            except: in_date = "1900-01-01"

                            recs.append({
                                "압축코드": str(row.iloc[7]).strip() if len(row) > 7 else "",
                                "자재번호": cur_mat,
                                "자재명": str(row.iloc[4]).strip() if len(row) > 4 else "",
                                "규격": str(row.iloc[5]).strip() if len(row) > 5 else "",
                                "공급업체명": m_info.get("업체", "미등록"),
                                "분류구분": m_info.get("분류", "수리대상"),
                                "입고일": in_date,
                                "상태": "출고 대기"
                            })
                            if len(recs) >= 200:
                                supabase.table("as_history").insert(recs).execute()
                                recs = []
                        if recs: supabase.table("as_history").insert(recs).execute()
                        p_bar.progress(1.0)
                        status_text.success(f"🎊 {total:,}건 입고 완료!")
                    else: st.error("❌ 'A/S 철거' 데이터를 찾지 못했습니다.")
        except Exception as e:
            st.error(f"입고 중 오류: {e}")

# --- [TAB 2] 출고 처리 ---
with tab2:
    st.info("📤 출고 엑셀을 업로드하여 압축코드 기준 출고일을 업데이트합니다.")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 반영"):
        try:
            with st.spinner("출고 매칭 중..."):
                df_out = pd.read_excel(out_file).fillna("")
                as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
                if not as_out.empty:
                    as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
                    as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
                    date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
                    for d, codes in date_groups.items():
                        for j in range(0, len(codes), 200):
                            supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+200]).execute()
                    st.success(f"✅ {len(as_out):,}건 출고 업데이트 완료!")
                else: st.warning("출고 대상 데이터가 없습니다.")
        except Exception as e: st.error(f"출고 오류: {e}")

# --- [TAB 3] 분석 리포트 ---
with tab3:
    if "data_ready" not in st.session_state: st.session_state.data_ready = False
    
    if st.button("📈 분석 리포트 생성 시작", use_container_width=True):
        with st.spinner("📊 DB 데이터 취합 중..."):
            try:
                res = supabase.table("as_history").select("*").execute()
                if not res.data:
                    st.warning("DB에 분석할 데이터가 없습니다.")
                else:
                    df = pd.DataFrame(res.data)
                    df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                    df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
                    
                    # TAT 계산 (출고일 - 입고일)
                    df['tat'] = None
                    mask = df['출고일'].notna() & df['입고일'].notna()
                    df.loc[mask, 'tat'] = (df.loc[mask, '출고일'] - df.loc[mask, '입고일']).dt.days
                    
                    def make_bin(target_df):
                        if target_df.empty: return None
                        out = io.BytesIO()
                        cols = ['입고일', '출고일', 'tat', '상태', '자재번호', '자재명', '규격', '압축코드', '공급업체명', '분류구분']
                        existing = [c for c in cols if c in target_df.columns]
                        with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                            target_df[existing].to_excel(wr, index=False)
                        return out.getvalue()

                    st.session_state.bin_tat = make_bin(df[df['출고일'].notna()])
                    st.session_state.bin_stay = make_bin(df[df['출고일'].isna()])
                    st.session_state.bin_total = make_bin(df)
                    st.session_state.data_ready = True
                    st.success(f"✅ 리포트 생성 완료! (전체: {len(df):,}건)")
                    st.rerun()
            except Exception as e: st.error(f"리포트 생성 오류: {e}")

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: 
            if st.session_state.bin_tat: st.download_button("📥 완료 리포트", st.session_state.bin_tat, "completed.xlsx", use_container_width=True)
            else: st.button("완료 데이터 없음", disabled=True, use_container_width=True)
        with c2: 
            if st.session_state.bin_stay: st.download_button("📥 미출고 명단", st.session_state.bin_stay, "pending.xlsx", use_container_width=True)
        with c3: 
            st.download_button("📥 전체 데이터", st.session_state.bin_total, "total_report.xlsx", use_container_width=True)
