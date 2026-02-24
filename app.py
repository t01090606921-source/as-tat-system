import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 통합 분석 시스템", layout="wide")
st.title("🚀 AS TAT 통합 관리 시스템 (오류 복구 버전)")

# --- [자재번호 정밀 표준화 함수] ---
def sanitize_code(val):
    if pd.isna(val): return ""
    # 모든 입력을 문자열로 변환 후 소수점 제거 및 공백 제거
    s_val = str(val).split('.')[0].strip().upper()
    return s_val

# --- 2. 데이터 전수 로드 함수 (네트워크 오류 재시도 로직 포함) ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    limit = 1000
    status_area = st.empty()
    
    while True:
        try:
            res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(limit).execute()
            batch = res.data
            if not batch: break
            all_data.extend(batch)
            last_id = batch[-1]['id']
            status_area.text(f"📥 DB 데이터 로딩 중: {len(all_data):,} 건...")
        except Exception as e:
            st.error(f"데이터 로드 중 일시적 오류 발생: {e}. 3초 후 재시도합니다.")
            time.sleep(3)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    m_file = st.file_uploader("1. 마스터 엑셀 등록", type=['xlsx'], key="master_file")
    if m_file and st.button("🚀 마스터 정밀 재등록"):
        with st.spinner("마스터 데이터 동기화 중..."):
            m_df = pd.read_excel(m_file, dtype=str)
            m_list = []
            for _, row in m_df.iterrows():
                # '자재번호'가 포함된 열을 찾거나 첫 번째 열 사용
                target_col = [c for c in m_df.columns if '자재번호' in str(c)]
                mat_id = sanitize_code(row[target_col[0]]) if target_col else sanitize_code(row.iloc[0])
                
                if not mat_id or mat_id == 'NAN': continue
                
                m_list.append({
                    "자재번호": mat_id,
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락",
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락"
                })
            
            if m_list:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                # 200개씩 끊어서 안정적으로 입력
                for i in range(0, len(m_list), 200):
                    supabase.table("master_data").insert(m_list[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_list):,}건 등록 성공!")
                st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary"):
        with st.spinner("DB 초기화 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [item['id'] for item in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 4. 메인 기능: 입고 / 출고 / 분석 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고", "📤 AS 출고", "📊 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_file")
    if in_file and st.button("🚀 입고 실행"):
        # 마스터 로드 및 딕셔너리화 (빠른 조회를 위해)
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        df = pd.read_excel(in_file, dtype=str)
        # 'A/S 철거' 문자열 필터링 시 오류 방지 처리
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in, p_bar = [], len(as_in), st.progress(0)
        status_txt = st.empty()
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            try:
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
                
                # 1000건마다 DB 전송 및 재시도 로직
                if len(recs) == 1000:
                    success = False
                    while not success:
                        try:
                            supabase.table("as_history").insert(recs).execute()
                            success = True
                        except:
                            time.sleep(2) # 서버 부하 방지용 대기
                    recs = []
                    p_bar.progress((i+1)/total_in)
                    status_txt.text(f"처리 중: {i+1:,} / {total_in:,} 건")
            except Exception as row_err:
                continue # 특정 행 에러 시 건너뛰고 계속 진행
                
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"✅ {total_in:,}건 입고 및 매칭 완료!")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_file")
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success(f"✅ 출고 처리 완료")

with tab3:
    if st.button("📈 전수 분석 실행", use_container_width=True):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("검출 수리대상", f"{len(df_rep):,} 건")
            c2.metric("미등록 건수", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            c3.metric("전체 데이터", f"{len(df_raw):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 리포트 다운로드", output.getvalue(), "AS_TAT_Final.xlsx", use_container_width=True)
