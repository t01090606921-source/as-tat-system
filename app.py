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
st.title("📊 AS TAT 통합 관리 (리포트 3종 & 컬럼 최적화)")

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
    st.info("💡 마스터와 입고 파일을 함께 올려주세요. 자재내역이 자동으로 매칭됩니다.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        prog_bar = st.progress(0)
        # 마스터 엑셀 로드 (자재내역 추가 추출)
        m_df = pd.read_excel(m_file, dtype=str)
        m_lookup = {}
        for _, row in m_df.iterrows():
            mat_id = sanitize_code(row.iloc[0])
            if mat_id:
                m_lookup[mat_id] = {
                    "자재내역": str(row.iloc[1]).strip() if len(row) > 1 else "", # B열: 자재내역
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락",
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락"
                }
        
        i_df = pd.read_excel(i_file, dtype=str)
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total = [], len(as_in)
        for i, (_, row) in enumerate(as_in.iterrows()):
            cur_mat = sanitize_code(row.iloc[3])
            m_info = m_lookup.get(cur_mat)
            recs.append({
                "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                "자재번호": cur_mat,
                "자재내역": m_info['자재내역'] if m_info else "미등록", # 추가됨
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
        st.success("입고 및 자재내역 매칭 완료")

with tab2:
    st.info("💡 출고 엑셀 날짜 개별 매칭")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].str.strip()
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            total_out, out_prog, processed = len(as_out), st.progress(0), 0
            for out_date, codes in date_groups.items():
                for j in range(0, len(codes), 200):
                    batch = codes[j:j+200]
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                    processed += len(batch)
                    out_prog.progress(processed / total_out)
            st.success("출고 업데이트 완료")

with tab3:
    if st.button("📈 분석 데이터 분석 시작"):
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
            df_all = pd.DataFrame(df_raw_list)
            df_all['입고일'] = pd.to_datetime(df_all['입고일'])
            df_all['출고일'] = pd.to_datetime(df_all['출고일'])
            
            # 재입고 건 처리 (입고일 > 출고일이면 출고일 비우기)
            df_all.loc[df_all['입고일'] > df_all['출고일'], '출고일'] = pd.NaT
            
            # TAT 계산 (유효 건만)
            df_all['TAT'] = (df_all['출고일'] - df_all['입고일']).dt.days
            
            # 공통 컬럼 배열 및 명칭 정의 (이미지 3번 기준)
            cols_order = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def format_for_excel(df):
                temp = df.copy()
                temp['입고일자'] = temp['입고일'].dt.strftime('%Y-%m-%d')
                temp['출고일자'] = temp['출고일'].dt.strftime('%Y-%m-%d')
                # 재입고 건은 엑셀에 텍스트 표시
                temp.loc[temp['출고일'].isna(), '출고일자'] = "미출고(재입고)"
                return temp.reindex(columns=cols_order)

            # 1. TAT 완료 데이터 (수리대상 + 출고일 있음)
            df_tat = df_all[(df_all['분류구분'].str.contains('수리대상', na=False)) & (df_all['출고일'].notna())].copy()
            # 2. 미출고/재입고 데이터 (수리대상 + 출고일 없음)
            df_stay = df_all[(df_all['분류구분'].str.contains('수리대상', na=False)) & (df_all['출고일'].isna())].copy()
            # 3. 전체 데이터 (수리대상 전체)
            df_total = df_all[df_all['분류구분'].str.contains('수리대상', na=False)].copy()

            st.divider()
            c1, c2, c3 = st.columns(3)
            
            with c1:
                st.subheader("✅ TAT 완료")
                st.metric("완료 건수", f"{len(df_tat):,}건")
                if not df_tat.empty:
                    xlsx_tat = io.BytesIO()
                    format_for_excel(df_tat).to_excel(xlsx_tat, index=False, engine='xlsxwriter')
                    st.download_button("📥 리포트 다운로드", xlsx_tat.getvalue(), "1_TAT_Completed.xlsx", key="dl_1")

            with c2:
                st.subheader("⚠️ 미출고/재입고")
                st.metric("잔류 건수", f"{len(df_stay):,}건")
                if not df_stay.empty:
                    xlsx_stay = io.BytesIO()
                    format_for_excel(df_stay).to_excel(xlsx_stay, index=False, engine='xlsxwriter')
                    st.download_button("📥 명단 다운로드", xlsx_stay.getvalue(), "2_Not_Shipped.xlsx", key="dl_2")

            with c3:
                st.subheader("📊 전체 데이터")
                st.metric("총합 건수", f"{len(df_total):,}건")
                if not df_total.empty:
                    xlsx_all = io.BytesIO()
                    format_for_excel(df_total).to_excel(xlsx_all, index=False, engine='xlsxwriter')
                    st.download_button("📥 전체 다운로드", xlsx_all.getvalue(), "3_Total_Data.xlsx", key="dl_3")

            st.write("🔍 리포트 미리보기 (최근 10건)")
            st.dataframe(format_for_excel(df_total).head(10))
