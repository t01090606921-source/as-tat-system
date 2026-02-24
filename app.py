import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 관리 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# [표준화 함수]
def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# [데이터 로드 함수]
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
            status_area.info(f"📥 데이터 수집 중... (현재 {len(all_data):,} 건)")
        except:
            time.sleep(1)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 2. 사이드바: 관리자 도구 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 전체 초기화", type="primary", use_container_width=True):
        progress_text = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            progress_text.warning(f"🗑️ 삭제 중... (최근 ID: {ids[-1]})")
        st.success("초기화 완료")
        st.rerun()

    st.divider()
    m_file = st.file_uploader("📋 자재 마스터 등록", type=['xlsx'])
    if m_file and st.button("🚀 마스터 업로드"):
        m_df = pd.read_excel(m_file, dtype=str)
        m_list = []
        for _, row in m_df.iterrows():
            mat_id = sanitize_code(row.iloc[0]) # A열: 자재번호
            if mat_id:
                m_list.append({
                    "자재번호": mat_id, 
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락", 
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락"
                })
        if m_list:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_list), 200):
                supabase.table("master_data").insert(m_list[i:i+200]).execute()
            st.success(f"마스터 {len(m_list):,}건 등록 완료")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고 처리", "📤 AS 출고 처리", "📊 TAT 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_upload")
    if in_file and st.button("🚀 입고 실행 (진행 바 표시)"):
        # 마스터 데이터 로드
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        # 엑셀 읽기 알림
        loading_msg = st.warning("⏳ 엑셀 파일을 분석하고 있습니다. 잠시만 기다려주세요...")
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        loading_msg.empty()
        
        total_rows = len(as_in)
        if total_rows == 0:
            st.error("입고 대상('A/S 철거') 데이터가 없습니다.")
        else:
            st.write(f"✅ 총 **{total_rows:,}**건의 입고 대상을 확인했습니다.")
            
            # --- 진행 바 설정 ---
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            batch_recs = []
            for i, (_, row) in enumerate(as_in.iterrows()):
                cur_mat = sanitize_code(row.iloc[3]) # D열: 자재번호
                m_info = m_lookup.get(cur_mat)
                
                batch_recs.append({
                    "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                    "자재번호": cur_mat,
                    "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                    "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
                
                # 400건마다 DB 전송 및 진행 바 갱신
                if len(batch_recs) >= 400:
                    try:
                        supabase.table("as_history").insert(batch_recs).execute()
                    except:
                        time.sleep(2)
                        supabase.table("as_history").insert(batch_recs).execute()
                    
                    batch_recs = []
                    # 실시간 진행 바 및 텍스트 업데이트
                    percent = (i + 1) / total_rows
                    progress_bar.progress(percent)
                    status_text.info(f"🚀 입고 진행 중: **{i+1:,}** / **{total_rows:,}** 건 ({percent*100:.1f}%)")
            
            # 남은 데이터 처리
            if batch_recs:
                supabase.table("as_history").insert(batch_recs).execute()
                progress_bar.progress(1.0)
                status_text.success(f"✅ 총 {total_rows:,}건 입고 처리가 모두 완료되었습니다!")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_upload")
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            
            out_progress = st.progress(0)
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
                out_progress.progress(min((i+500)/len(out_keys), 1.0))
            st.success(f"✅ {len(out_keys):,}건 출고 업데이트 완료")

with tab3:
    if st.button("📈 분석 리포트 생성 및 엑셀 다운로드", use_container_width=True):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            col1, col2 = st.columns(2)
            col1.metric("수리대상(TAT 분석 대상)", f"{len(df_rep):,} 건")
            col2.metric("미등록(확인 필요)", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 분석 결과 엑셀 다운로드", output.getvalue(), "AS_TAT_Report.xlsx", use_container_width=True)
            
            with st.expander("🔍 미등록 샘플 보기"):
                st.dataframe(df_raw[df_raw['분류구분'] == '미등록'][['자재번호', '규격']].head(50))
