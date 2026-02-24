import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (재입고 필터링 버전)")

def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 삭제 진행 중... (ID: {ids[-1]})")
        st.success("초기화 완료")
        st.rerun()

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 마스터와 입고 파일을 함께 올려주세요.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        prog_bar = st.progress(0)
        m_df = pd.read_excel(m_file, dtype=str)
        m_lookup = {sanitize_code(row.iloc[0]): {"공급업체명": str(row.iloc[5]).strip(), "분류구분": str(row.iloc[10]).strip()} for _, row in m_df.iterrows() if sanitize_code(row.iloc[0])}
        
        i_df = pd.read_excel(i_file, dtype=str)
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total = [], len(as_in)
        for i, (_, row) in enumerate(as_in.iterrows()):
            cur_mat = sanitize_code(row.iloc[3])
            m_info = m_lookup.get(cur_mat)
            recs.append({
                "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                "자재번호": cur_mat, 
                "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                "상태": "출고 대기", 
                "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록", 
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) >= 200:
                supabase.table("as_history").insert(recs).execute()
                recs = []
                prog_bar.progress((i+1)/total)
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("입고 완료")

with tab2:
    st.info("💡 출고 엑셀의 날짜를 개별 매칭합니다.")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            
            total_out = len(as_out)
            out_prog = st.progress(0)
            processed_count = 0
            
            for out_date, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    batch_codes = codes[j:j+200]
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch_codes).execute()
                    processed_count += len(batch_codes)
                    out_prog.progress(processed_count / total_out)
            st.success(f"✅ 총 {total_out:,}건 출고 업데이트 완료")

with tab3:
    if st.button("📈 분석 리포트 생성"):
        df_raw_list = []
        last_id = -1
        load_msg = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            df_raw_list.extend(res.data)
            last_id = res.data[-1]['id']
            load_msg.info(f"📥 데이터 수집 중... ({len(df_raw_list):,}건)")
        
        if df_raw_list:
            df_final = pd.DataFrame(df_raw_list)
            df_final['입고일'] = pd.to_datetime(df_final['입고일'])
            df_final['출고일'] = pd.to_datetime(df_final['출고일'])
            
            # [핵심 로직] 재입고 건 처리: 입고일이 출고일보다 늦으면 출고일을 무효화(NaT)
            # NaT는 엑셀 출력 시 빈칸으로 표시됩니다.
            df_final.loc[df_final['입고일'] > df_final['출고일'], '출고일'] = pd.NaT
            
            # 분석 대상 필터링: 분류구분이 '수리대상'이고, 유효한 출고일이 있는 데이터만
            df_rep = df_final[
                (df_final['분류구분'].str.contains('수리대상', na=False)) & 
                (df_final['출고일'].notna())
            ].copy()
            
            # TAT 계산 (재입고 건은 위에서 필터링되었으므로 정상 건만 계산됨)
            df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
            
            # 전체 통계 표시
            col1, col2, col3 = st.columns(3)
            col1.metric("총 수리대상", f"{len(df_final[df_final['분류구분'].str.contains('수리대상', na=False)]):,}건")
            col2.metric("출고 완료(TAT 분석)", f"{len(df_rep):,}건")
            col3.metric("재입고/미출고 제외", f"{len(df_final[(df_final['분류구분'].str.contains('수리대상', na=False)) & (df_final['출고일'].isna())]):,}건")

            if not df_rep.empty:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    # 입고일/출고일을 보기 좋게 포맷팅하여 저장
                    df_out_xlsx = df_rep.copy()
                    df_out_xlsx['입고일'] = df_out_xlsx['입고일'].dt.strftime('%Y-%m-%d')
                    df_out_xlsx['출고일'] = df_out_xlsx['출고일'].dt.strftime('%Y-%m-%d')
                    df_out_xlsx[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                
                st.download_button("📥 TAT 리포트 다운로드", output.getvalue(), "AS_TAT_Final_Report.xlsx")
                st.dataframe(df_out_xlsx[['입고일', '출고일', '자재번호', 'TAT']].head(50))
            else:
                st.warning("분석 가능한 유효 출고 데이터가 없습니다.")
