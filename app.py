import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time  # 서버 과부하 방지용 대기 시간을 위해 추가

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("Supabase 접속 정보(Secrets)를 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (안정 전송 버전)")

# 코드 데이터 정리 함수
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        with st.spinner("DB 초기화 중..."):
            supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("데이터베이스가 초기화되었습니다.")
        st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 데이터 관리 ---
with tab0:
    st.subheader("📋 마스터 데이터 최신화")
    st.info("입고 전 마스터 엑셀을 먼저 업로드하여 메모리에 로드하세요.")
    m_file_mgmt = st.file_uploader("마스터 엑셀 업로드", type=['xlsx', 'csv'], key="m_mgmt")
    
    if m_file_mgmt and st.button("🔄 마스터 데이터 로드"):
        with st.spinner("마스터 분석 중..."):
            try:
                if m_file_mgmt.name.endswith('.csv'):
                    m_df = pd.read_csv(m_file_mgmt, encoding='cp949').fillna("")
                else:
                    m_df = pd.read_excel(m_file_mgmt).fillna("")
                
                new_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                
                st.session_state.master_lookup = new_lookup
                st.success(f"✅ 마스터 데이터 로드 완료! (총 {len(new_lookup):,}건)")
            except Exception as e:
                st.error(f"마스터 로드 실패: {e}")

# --- [TAB 1] 입고 처리 (서버 안정성 강화) ---
with tab1:
    st.info("💡 CSV 업로드 시 100건씩 끊어서 안전하게 전송합니다.")
    col1, col2 = st.columns(2)
    with col1:
        if "master_lookup" in st.session_state:
            st.success("✅ 마스터 로드됨")
            use_existing = st.checkbox("로드된 마스터 사용", value=True)
        else:
            use_existing = False
            st.warning("⚠️ [마스터 관리] 탭에서 먼저 로드하세요.")
        
        m_file_manual = None
        if not use_existing:
            m_file_manual = st.file_uploader("1. 마스터 직접 업로드", type=['xlsx', 'csv'], key="m_manual")
            
    with col2: 
        i_file = st.file_uploader("2. AS 입고 CSV 업로드", type=['csv'], key="i_up")

    if i_file and st.button("🚀 안전 입고 시작"):
        status_text = st.empty()
        p_bar = st.progress(0)
        
        try:
            lookup = st.session_state.master_lookup if use_existing else None
            if not lookup and m_file_manual:
                m_df_m = pd.read_excel(m_file_manual).fillna("")
                lookup = {sanitize_code(row.iloc[0]): {"업체": str(row.iloc[5]), "분류": str(row.iloc[10])} for _, row in m_df_m.iterrows()}

            if not lookup:
                st.error("마스터 데이터가 없습니다.")
            else:
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
                        batch_size = 100 # 502 에러 방지를 위해 크기 축소
                        for i, (_, row) in enumerate(as_in.iterrows()):
                            if i % batch_size == 0:
                                p_bar.progress(min((i + 1) / total, 1.0))
                                status_text.info(f"🚀 안전 전송 중... ({i+1:,} / {total:,}건)")
                            
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
                            
                            if len(recs) >= batch_size:
                                try:
                                    supabase.table("as_history").insert(recs).execute()
                                    recs = []
                                    time.sleep(0.1) # 서버 숨 고르기
                                except:
                                    time.sleep(1.5) # 오류 발생 시 더 길게 대기 후 재시도
                                    supabase.table("as_history").insert(recs).execute()
                                    recs = []
                        
                        if recs: supabase.table("as_history").insert(recs).execute()
                        p_bar.progress(1.0)
                        status_text.success(f"🎊 {total:,}건 입고 완료!")
                    else: st.error("❌ 'A/S 철거' 데이터를 찾지 못했습니다.")
        except Exception as e: st.error(f"입고 오류: {e}")

# --- [TAB 2] 출고 처리 ---
with tab2:
    st.info("📤 출고 엑셀 업로드 (압축코드 기준 업데이트)")
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
                    st.success(f"✅ {len(as_out):,}건 업데이트 성공")
                else: st.warning("대상 데이터가 없습니다.")
        except Exception as e: st.error(f"출고 오류: {e}")

# --- [TAB 3] 분석 리포트 ---
with tab3:
    if "data_ready" not in st.session_state: st.session_state.data_ready = False
    
    if st.button("📈 리포트 생성 시작", use_container_width=True):
        with st.spinner("📊 전체 데이터 집계 중..."):
            try:
                res = supabase.table("as_history").select("*").execute()
                if not res.data:
                    st.warning("분석할 데이터가 없습니다.")
                else:
                    df = pd.DataFrame(res.data)
                    df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                    df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
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
                    st.success(f"✅ 리포트 생성 완료 (전체 {len(df):,}건)")
                    st.rerun()
            except Exception as e: st.error(f"리포트 오류: {e}")

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: 
            if st.session_state.bin_tat: st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_done.xlsx", use_container_width=True)
            else: st.button("완료건 없음", disabled=True, use_container_width=True)
        with c2: 
            if st.session_state.bin_stay: st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_pending.xlsx", use_container_width=True)
        with c3: 
            st.download_button("📥 3. 전체 데이터", st.session_state.bin_total, "3_total.xlsx", use_container_width=True)
