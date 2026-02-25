import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (진행 상황 실시간 표시)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        with st.spinner("삭제 중..."):
            supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 3. 메인 탭 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 입고 시작 시 하단에 진행률 표시바와 진행 메시지가 나타납니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀 업로드", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀 업로드", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        # 진행 상황 확인용 UI 요소 생성
        p_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # [마스터 로드]
            status_text.text("🔍 마스터 엑셀 분석 중...")
            m_df = pd.read_excel(m_file).dropna(how='all').fillna("")
            m_lookup = {sanitize_code(row.iloc[0]): {
                "공급업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
            } for _, row in m_df.iterrows() if sanitize_code(row.iloc[0])}

            # [입고 데이터 처리]
            status_text.text("📄 입고 파일 로드 중...")
            i_df = pd.read_excel(i_file).dropna(how='all').fillna("")
            as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            total = len(as_in)
            
            if total == 0:
                st.error("❌ 'A/S 철거' 항목을 찾지 못했습니다.")
            else:
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        cur_mat = sanitize_code(row.iloc[3])
                        m_info = m_lookup.get(cur_mat, {})
                        
                        try: in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except: in_date = "1900-01-01"

                        recs.append({
                            "압축코드": str(row.iloc[7]).strip(),
                            "자재번호": cur_mat,
                            "자재내역": str(row.iloc[4]).strip(),
                            "규격": str(row.iloc[5]).strip(),
                            "상태": "출고 대기",
                            "공급업체명": m_info.get("공급업체", "미등록"),
                            "분류구분": m_info.get("분류", "수리대상"),
                            "입고일": in_date
                        })
                        
                        # 진행률 업데이트 및 벌크 업로드 (100건 단위로 최적화)
                        if len(recs) >= 100:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                            progress_val = min((i + 1) / total, 1.0)
                            p_bar.progress(progress_val)
                            status_text.text(f"🚀 입고 진행 중: {i+1} / {total} 건 완료")
                    except: continue
                
                if recs: # 남은 데이터 처리
                    supabase.table("as_history").insert(recs).execute()
                
                p_bar.progress(1.0)
                status_text.success(f"🎊 총 {total:,}건 입고 및 매칭 완료!")
        except Exception as e:
            st.error(f"❌ 입고 중 오류 발생: {e}")

with tab2:
    st.info("📤 출고 엑셀 업로드 (진행 상황 표시 포함)")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file).dropna(how='all').fillna("")
        as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            
            total_out = len(as_out)
            processed = 0
            p_bar_out = st.progress(0)
            status_out = st.empty()
            
            for d, codes in date_groups.items():
                for j in range(0, len(codes), 100):
                    batch = codes[j:j+100]
                    supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                    processed += len(batch)
                    p_bar_out.progress(min(processed / total_out, 1.0))
                    status_out.text(f"📤 출고 업데이트 중: {processed} / {total_out} 건")
            st.success("✅ 출고 업데이트 완료!")

with tab3:
    if "data_ready" not in st.session_state: st.session_state.data_ready = False
    
    if st.button("📈 분석 리포트 생성", use_container_width=True):
        with st.status("데이터 분석 중...", expanded=True) as status:
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
                status.update(label="분석 완료!", state="complete", expanded=False)
                st.rerun()

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2: st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3: st.download_button("📥 3. 전체 데이터", st.session_state.bin_total, "3_Total_Data.xlsx")
