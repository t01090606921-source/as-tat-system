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
st.title("📊 AS TAT 통합 관리 (미출고 데이터 추출 포함)")

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
                "자재번호": cur_mat, "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                "상태": "출고 대기", "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록", "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) >= 200:
                supabase.table("as_history").insert(recs).execute()
                recs = []
                prog_bar.progress((i+1)/total)
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("입고 완료")

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
            load_msg.info(f"📥 전체 데이터 수집 중... ({len(df_raw_list):,}건)")
        
        if df_raw_list:
            df_all = pd.DataFrame(df_raw_list)
            df_all['입고일'] = pd.to_datetime(df_all['입고일'])
            df_all['출고일'] = pd.to_datetime(df_all['출고일'])
            
            # 1. 재입고 건 처리 (입고일 > 출고일이면 출고일 비우기)
            df_all.loc[df_all['입고일'] > df_all['출고일'], '출고일'] = pd.NaT
            
            # 2. 데이터 분리
            # (A) TAT 분석 대상 : 수리대상이며, 유효한 출고일이 있는 건
            df_tat = df_all[(df_all['분류구분'].str.contains('수리대상', na=False)) & (df_all['출고일'].notna())].copy()
            # (B) 미출고/재입고 대상 : 수리대상이며, 출고일이 없거나 비워진 건
            df_stay = df_all[(df_all['분류구분'].str.contains('수리대상', na=False)) & (df_all['출고일'].isna())].copy()
            
            st.divider()
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("✅ TAT 완료 데이터")
                st.metric("분석 건수", f"{len(df_tat):,} 건")
                if not df_tat.empty:
                    df_tat['TAT'] = (df_tat['출고일'] - df_tat['입고일']).dt.days
                    out_tat = io.BytesIO()
                    with pd.ExcelWriter(out_tat, engine='xlsxwriter') as writer:
                        df_tat_xlsx = df_tat.copy()
                        df_tat_xlsx['입고일'] = df_tat_xlsx['입고일'].dt.strftime('%Y-%m-%d')
                        df_tat_xlsx['출고일'] = df_tat_xlsx['출고일'].dt.strftime('%Y-%m-%d')
                        df_tat_xlsx[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                    st.download_button("📥 TAT 완료 리포트 다운로드", out_tat.getvalue(), "AS_TAT_Completed.xlsx", key="dl_tat")

            with c2:
                st.subheader("⚠️ 미출고/재입고 데이터")
                st.metric("잔류 건수", f"{len(df_stay):,} 건")
                if not df_stay.empty:
                    out_stay = io.BytesIO()
                    with pd.ExcelWriter(out_stay, engine='xlsxwriter') as writer:
                        df_stay_xlsx = df_stay.copy()
                        df_stay_xlsx['입고일'] = df_stay_xlsx['입고일'].dt.strftime('%Y-%m-%d')
                        df_stay_xlsx['출고일'] = "미출고(재입고)" # 엑셀상 표시
                        df_stay_xlsx[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드']].to_excel(writer, index=False)
                    st.download_button("📥 미출고 명단 다운로드", out_stay.getvalue(), "AS_Not_Shipped_List.xlsx", key="dl_stay")

            if df_stay.empty and df_tat.empty:
                st.warning("분류 대상인 '수리대상' 데이터가 없습니다.")
