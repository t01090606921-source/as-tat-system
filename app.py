import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
# Streamlit Cloud의 Secrets 기능을 사용하거나 직접 입력하세요.
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (최종 안정화)")

# 데이터 정제 함수
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (시스템 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        try:
            # 전체 데이터 삭제 (neq -1은 모든 ID를 의미)
            supabase.table("as_history").delete().neq("id", -1).execute()
            st.success("데이터베이스 초기화 완료")
            st.rerun()
        except Exception as e:
            st.error(f"삭제 중 오류: {e}")

# --- 3. 메인 탭 구성 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

# --- TAB 1: 입고 처리 (VLOOKUP 및 데이터 보정) ---
with tab1:
    st.info("💡 마스터와 입고 파일을 업로드하세요. 데이터 구조를 자동으로 분석하여 입고합니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 (A:코드, G:내역)", type=['xlsx'])
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀 (A:구분, B:일자...)", type=['xlsx'])

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        p_bar = st.progress(0)
        
        # 마스터 로드 (A:자재번호, F:공급업체, G:자재내역, K:분류구분)
        m_df = pd.read_excel(m_file).dropna(how='all').fillna("")
        m_lookup = {}
        for _, row in m_df.iterrows():
            try:
                m_code = sanitize_code(row.iloc[0])
                if m_code:
                    m_lookup[m_code] = {
                        "자재내역": str(row.iloc[6]).strip() if len(row) > 6 else "",
                        "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "",
                        "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else ""
                    }
            except: continue

        # 입고 데이터 로드 및 필터링
        try:
            i_df = pd.read_excel(i_file).dropna(how='all').fillna("")
            mask = i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)
            as_in = i_df[mask].copy()
            total_rows = len(as_in)
            
            if total_rows == 0:
                st.error("❌ 'A/S 철거' 항목을 찾지 못했습니다. 엑셀 형식을 확인하세요.")
            else:
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        if len(row) < 8: continue
                        cur_mat = sanitize_code(row.iloc[3])
                        m_info = m_lookup.get(cur_mat, {})
                        
                        # 날짜 변환
                        try:
                            in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except:
                            in_date = "1900-01-01"

                        recs.append({
                            "압축코드": str(row.iloc[7]).strip(),
                            "자재번호": cur_mat,
                            "자재내역": m_info.get("자재내역", "미등록"),
                            "규격": str(row.iloc[5]).strip(),
                            "상태": "출고 대기",
                            "공급업체명": m_info.get("공급업체명", "미등록"),
                            "분류구분": m_info.get("분류구분", "미등록"),
                            "입고일": in_date
                        })
                    except: continue
                    
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []
                        p_bar.progress((i+1)/total_rows)
                
                if recs:
                    supabase.table("as_history").insert(recs).execute()
                st.success(f"🎊 {total_rows:,}건 입고 완료!")
        except Exception as e:
            st.error(f"입고 오류: {e}")

# --- TAB 2: 출고 업데이트 ---
with tab2:
    st.info("💡 출고 엑셀을 업로드하면 압축코드를 기준으로 출고일자를 매칭합니다.")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'])
    
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file).dropna(how='all').fillna("")
        # D열(3): AS 카톤 박스 포함 행 찾기
        as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            
            total_out = len(as_out)
            out_p = st.progress(0)
            processed = 0
            
            for out_date, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    batch = codes[j:j+200]
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                    processed += len(batch)
                    out_p.progress(processed / total_out)
            st.success(f"✅ {total_out:,}건 출고 업데이트 완료")

# --- TAB 3: 분석 리포트 (3종 다운로드 및 세션 유지) ---
with tab3:
    # 세션 상태 초기화 (결과 유지용)
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
        
        # 데이터 수집 (Pagination)
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
            
            # [요청 로직] 입고일 > 출고일인 경우 출고일 제거 (재입고 처리)
            df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
            df['TAT'] = (df['출고일'] - df['입고일']).dt.days
            
            # 컬럼 배열 재구성 (요청하신 순서)
            cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def to_excel_bin(target_df):
                if target_df.empty: return None
                t_df = target_df.copy()
                t_df['입고일자'] = t_df['입고일'].dt.strftime('%Y-%m-%d')
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    t_df.reindex(columns=cols).to_excel(writer, index=False)
                return output.getvalue()

            # 데이터 분리 (수리대상 기준)
            df_filtered = df[df['분류구분'].str.contains('수리대상', na=False)]
            df_tat = df_filtered[df_filtered['출고일'].notna()]
            df_stay = df_filtered[df_filtered['출고일'].isna()]

            # 세션에 바이너리(파일) 형태로 저장
            st.session_state.bin_tat = to_excel_bin(df_tat)
            st.session_state.bin_stay = to_excel_bin(df_stay)
            st.session_state.bin_total = to_excel_bin(df_filtered)
            st.session_state.counts = [len(df_tat), len(df_stay), len(df_filtered)]
            st.session_state.data_ready = True
            msg.empty()

    # 결과가 준비되면 다운로드 버튼 노출
    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("✅ TAT 완료")
            st.metric("완료 건수", f"{st.session_state.counts[0]:,}건")
            if st.session_state.bin_tat:
                st.download_button("📥 1. 완료 리포트 다운로드", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2:
            st.subheader("⚠️ 미출고/재입고")
            st.metric("잔류 건수", f"{st.session_state.counts[1]:,}건")
            if st.session_state.bin_stay:
                st.download_button("📥 2. 미출고 명단 다운로드", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3:
            st.subheader("📊 수리대상 전체")
            st.metric("총합 건수", f"{st.session_state.counts[2]:,}건")
            if st.session_state.bin_total:
                st.download_button("📥 3. 전체 데이터 다운로드", st.session_state.bin_total, "3_Total_Data.xlsx")
        
        st.write("🔍 데이터 미리보기 (상위 10건)")
        # 미리보기용 임시 DF
        preview_df = pd.DataFrame(df_list).head(10)
        st.dataframe(preview_df)
