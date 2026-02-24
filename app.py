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
st.title("📊 AS TAT 통합 관리 (진행 바 보정본)")

def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    status_area = st.empty()
    while True:
        try:
            res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            all_data.extend(res.data)
            last_id = res.data[-1]['id']
            status_area.info(f"📥 마스터 데이터 동기화 중: {len(all_data):,}건")
        except:
            time.sleep(1)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 2. 사이드바 (초기화 및 마스터) ---
with st.sidebar:
    st.header("⚙️ 설정")
    if st.button("⚠️ 데이터 초기화", type="primary"):
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 삭제 중... (ID: {ids[-1]})")
        st.success("완료")
        st.rerun()

    m_file = st.file_uploader("📋 마스터 엑셀", type=['xlsx'])
    if m_file and st.button("마스터 등록"):
        m_df = pd.read_excel(m_file, dtype=str)
        m_list = [{"자재번호": sanitize_code(row.iloc[0]), 
                   "공급업체명": str(row.iloc[5]).strip(), 
                   "분류구분": str(row.iloc[10]).strip()} for _, row in m_df.iterrows() if sanitize_code(row.iloc[0])]
        supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
        for i in range(0, len(m_list), 200):
            supabase.table("master_data").insert(m_list[i:i+200]).execute()
        st.success("마스터 등록 성공")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고", "📤 AS 출고", "📊 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_main")
    if in_file and st.button("🚀 정밀 입고 실행"):
        # [단계 1] 마스터 로드
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        # [단계 2] 엑셀 읽기 (이 구간에서 로딩이 발생할 수 있음)
        load_state = st.info("⏳ 엑셀 파일을 읽는 중입니다... 잠시만 기다려주세요.")
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        load_state.empty()
        
        total_rows = len(as_in)
        if total_rows > 0:
            st.write(f"✅ 입고 대상: **{total_rows:,}** 건")
            
            # --- 실시간 진행 바 레이아웃 ---
            progress_bar = st.progress(0)
            percent_text = st.empty()
            count_text = st.empty()
            
            recs = []
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
                
                # 200건 단위로 더 자주 끊어서 화면 갱신
                if len(recs) >= 200:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    
                    # 실시간 바 갱신
                    ratio = (i + 1) / total_rows
                    progress_bar.progress(ratio)
                    percent_text.subheader(f"⚡ 입고율: {ratio*100:.1f}%")
                    count_text.write(f"🏃 처리 중: {i+1:,} / {total_rows:,} 건")
            
            if recs:
                supabase.table("as_history").insert(recs).execute()
                progress_bar.progress(1.0)
            
            st.success(f"🎊 {total_rows:,}건 입고 완료!")
        else:
            st.error("입고 대상('A/S 철거') 데이터가 없습니다.")

with tab2:
    # 출고 로직 (간결화)
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_main")
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            out_bar = st.progress(0)
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).execute()
                out_bar.progress(min((i+500)/len(out_keys), 1.0))
            st.success("출고 완료")

with tab3:
    if st.button("📈 분석 리포트 생성"):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_rep = df_raw[df_raw['분류구분'].fillna('').str.contains('수리대상', na=False)].copy()
            st.metric("수리대상", f"{len(df_rep):,} 건")
            if not df_rep.empty:
                df_rep['TAT'] = (pd.to_datetime(df_rep['출고일']) - pd.to_datetime(df_rep['입고일'])).dt.days
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 다운로드", output.getvalue(), "AS_TAT_Report.xlsx")
