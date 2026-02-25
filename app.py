import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (마스터 & 입고 데이터 병합 버전)")

# 자재코드 정제 함수 (소수점 제거 및 대문자화)
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (시스템 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        try:
            # 모든 데이터를 삭제하는 쿼리
            supabase.table("as_history").delete().neq("id", -1).execute()
            st.success("데이터베이스 초기화 완료")
            st.rerun()
        except Exception as e:
            st.error(f"삭제 오류: {e}")

# --- 3. 메인 탭 구성 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

# --- TAB 1: 입고 처리 (마스터 + 입고파일 병합) ---
with tab1:
    st.info("💡 마스터에서 '업체/분류'를 가져오고, 입고 파일에서 '자재명/일자'를 가져옵니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 업로드", type=['xlsx'], key="master_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀 업로드", type=['xlsx'], key="in_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        try:
            # [1] 마스터 로드 (A:코드, F:공급업체, K:분류구분)
            m_df = pd.read_excel(m_file).dropna(how='all').fillna("")
            m_lookup = {}
            for _, row in m_df.iterrows():
                m_code = sanitize_code(row.iloc[0])
                if m_code:
                    m_lookup[m_code] = {
                        "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                        "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                    }

            # [2] 입고 파일 로드 (A:구분, B:입고일, D:코드, E:자재명, F:규격, H:압축코드)
            i_df = pd.read_excel(i_file).dropna(how='all').fillna("")
            # 'A/S 철거'가 포함된 행만 필터링
            as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            total = len(as_in)
            
            if total == 0:
                st.warning("⚠️ 입고 파일의 1열(A열)에 'A/S 철거' 데이터가 없습니다.")
            else:
                recs = []
                p_bar = st.progress(0)
                status_text = st.empty()
                
                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        if len(row) < 8: continue
                        cur_mat = sanitize_code(row.iloc[3]) # D열
                        m_info = m_lookup.get(cur_mat, {})
                        
                        # 날짜 변환 (B열)
                        try:
                            in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except:
                            in_date = "1900-01-01"

                        recs.append({
                            "압축코드": str(row.iloc[7]).strip(), # H열
                            "자재번호": cur_mat, # D열
                            "자재내역": str(row.iloc[4]).strip(), # E열(입고 파일 자재명 직접 사용)
                            "규격": str(row.iloc[5]).strip(), # F열
                            "상태": "출고 대기",
                            "공급업체명": m_info.get("공급업체명", "미등록"),
                            "분류구분": m_info.get("분류구분", "수리대상"),
                            "입고일": in_date
                        })
                        
                        # 200건씩 묶어서 업로드 (성능 최적화)
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                            p_bar.progress((i+1)/total)
                            status_text.text(f"처리 중... ({i+1}/{total})")
                    except: continue
                
                if recs:
                    supabase.table("as_history").insert(recs).execute()
                st.success(f"🎊 {total:,}건 입고 및 마스터 매칭 완료!")
        except Exception as e:
            st.error(f"입고 중 오류 발생: {e}")

# --- TAB 2: 출고 업데이트 (압축코드 기준) ---
with tab2:
    st.info("📤 출고 엑셀의 D열 'AS 카톤 박스'를 찾아 K열 압축코드로 매칭합니다.")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    
    if out_file and st.button("🚀 출고 업데이트 시작"):
        try:
            df_out = pd.read_excel(out_file).dropna(how='all').fillna("")
            # D열(3)에 'AS 카톤 박스' 포함된 행만 필터링
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            
            if as_out.empty:
                st.warning("⚠️ 출고 대상('AS 카톤 박스')을 찾을 수 없습니다.")
            else:
                # G열(6): 출고일, K열(10): 압축코드
                as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
                as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
                
                # 날짜별로 그룹화하여 벌크 업데이트
                date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
                total_out = len(as_out)
                processed = 0
                out_p = st.progress(0)
                
                for out_date, codes in date_groups.items():
                    for j in range(0, len(codes), 200):
                        batch = codes[j:j+200]
                        supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                        processed += len(batch)
                        out_p.progress(processed / total_out)
                st.success(f"✅ {total_out:,}건 출고 업데이트 완료!")
        except Exception as e:
            st.error(f"출고 업데이트 중 오류: {e}")

# --- TAB 3: 분석 리포트 (데이터 추출 및 다운로드) ---
with tab3:
    # 세션 상태에 파일 데이터 저장 (다운로드 시 초기화 방지)
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False
        st.session_state.bin_tat = None
        st.session_state.bin_stay = None
        st.session_state.bin_total = None
        st.session_state.counts = [0, 0, 0]

    if st.button("📈 데이터 분석 시작 (DB에서 불러오기)", use_container_width=True):
        with st.spinner("데이터 분석 중..."):
            res = supabase.table("as_history").select("*").execute()
            if res.data:
                df = pd.DataFrame(res.data)
                df['입고일'] = pd.to_datetime(df['입고일'])
                df['출고일'] = pd.to_datetime(df['출고일'])
                
                # 입고일보다 출고일이 빠른 데이터 오류 수정 (입고일 기준 정렬)
                df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
                df['TAT'] = (df['출고일'] - df['입고일']).dt.days
                
                # 리포트용 컬럼 배열 (요청사항 반영)
                cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
                
                def make_excel_bin(target_df):
                    if target_df.empty: return None
                    t = target_df.copy()
                    t['입고일자'] = t['입고일'].dt.strftime('%Y-%m-%d')
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        t.reindex(columns=cols).to_excel(writer, index=False)
                    return output.getvalue()

                # '수리대상' 데이터만 필터링하여 리포트 생성
                f_df = df[df['분류구분'].str.contains('수리대상', na=False)]
                
                st.session_state.bin_tat = make_excel_bin(f_df[f_df['출고일'].notna()])
                st.session_state.bin_stay = make_excel_bin(f_df[f_df['출고일'].isna()])
                st.session_state.bin_total = make_excel_bin(f_df)
                st.session_state.counts = [len(f_df[f_df['출고일'].notna()]), len(f_df[f_df['출고일'].isna()]), len(f_df)]
                st.session_state.data_ready = True
                st.rerun()

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("✅ TAT 완료", f"{st.session_state.counts[0]:,}건")
            if st.session_state.bin_tat:
                st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2:
            st.metric("⚠️ 미출고/재입고", f"{st.session_state.counts[1]:,}건")
            if st.session_state.bin_stay:
                st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3:
            st.metric("📊 수리대상 전체", f"{st.session_state.counts[2]:,}건")
            if st.session_state.bin_total:
                st.download_button("📥 3. 전체 리포트", st.session_state.bin_total, "3_Total_Data.xlsx")
