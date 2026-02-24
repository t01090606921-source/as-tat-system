import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 고속 분석 시스템", layout="wide")
st.title("⚡ AS TAT 고속 입고 시스템 (멈춤 방지)")

# [정밀 표준화 함수]
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
            status_area.info(f"📥 마스터 데이터 불러오는 중: {len(all_data):,} 건")
        except:
            time.sleep(1)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 2. 사이드바 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 전체 초기화", type="primary", use_container_width=True):
        status = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            status.warning(f"🗑️ 삭제 중... (ID: {ids[-1]})")
        st.success("초기화 완료")
        st.rerun()

    m_file = st.file_uploader("📋 마스터 엑셀 등록", type=['xlsx'])
    if m_file and st.button("🚀 마스터 업로드"):
        m_df = pd.read_excel(m_file, dtype=str)
        m_list = []
        for _, row in m_df.iterrows():
            mat_id = sanitize_code(row.iloc[0])
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
            st.success("마스터 등록 완료")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 입고", "📤 출고 처리", "📊 결과 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드 (57만 건용)", type=['xlsx'])
    if in_file and st.button("🚀 전수 입고 시작"):
        # 1. 마스터 로드 (조회 속도 극대화)
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        # 2. 엑셀 로딩 최적화 (엔진 지정 및 메모리 관리)
        with st.spinner("엑셀 파일을 읽고 있습니다. 잠시만 기다려주세요..."):
            df = pd.read_excel(in_file, dtype=str, engine='openpyxl')
            
        # A/S 철거 데이터만 필터링
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        total_rows = len(as_in)
        st.write(f"✅ 분석 대상: {total_rows: ,} 건 발견")
        
        # 3. 데이터 입력 (연결 끊김 방지 로직)
        batch_recs = []
        p_bar = st.progress(0)
        p_text = st.empty()
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            cur_mat = sanitize_code(row.iloc[3])
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
            
            # 500건 단위로 더 자주 끊어서 전송 (안정성 강화)
            if len(batch_recs) >= 500:
                try:
                    supabase.table("as_history").insert(batch_recs).execute()
                except:
                    time.sleep(2) # 서버 부하 방지
                    supabase.table("as_history").insert(batch_recs).execute()
                
                batch_recs = []
                # 진행 상황을 화면에 강제로 업데이트 (로딩 멈춤 방지)
                percent = (i + 1) / total_rows
                p_bar.progress(percent)
                p_text.info(f"🚀 처리 중: {i+1:,} / {total_rows:,} 건 ({percent*100:.1f}%)")
        
        if batch_recs:
            supabase.table("as_history").insert(batch_recs).execute()
            
        st.success(f"🎊 {total_rows:,} 건 입고 완료!")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'])
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success(f"✅ 출고 업데이트 완료")

with tab3:
    if st.button("📈 분석 리포트 생성"):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            st.metric("수리대상 건수", f"{len(df_rep):,} 건")
            st.metric("미등록 건수", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 상세 결과 다운로드", output.getvalue(), "AS_TAT_Final.xlsx")
