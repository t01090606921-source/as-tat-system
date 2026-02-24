import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 통합 분석 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 및 전수 분석")

# --- [자재번호 정밀 표준화 함수] ---
def sanitize_code(val):
    if pd.isna(val): return ""
    # 숫자로 읽혀서 .0이 붙는 경우 제거하고 순수 문자열만 추출
    return str(val).split('.')[0].strip().upper()

# --- 2. 데이터 전수 로드 함수 (ID 기반) ---
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
            status_area.text(f"📥 데이터 수집 중: {len(all_data):,} 건...")
        except:
            break
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    m_file = st.file_uploader("1. 마스터 등록", type=['xlsx'], key="m_side")
    if m_file and st.button("🚀 마스터 정밀 재등록"):
        with st.spinner("마스터 동기화 중..."):
            m_df = pd.read_excel(m_file, dtype=str)
            m_list = []
            for _, row in m_df.iterrows():
                mat_id = sanitize_code(row.iloc[0]) # A열
                if not mat_id: continue
                m_list.append({
                    "자재번호": mat_id,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "미등록",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "미등록"
                })
            if m_list:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_list), 200):
                    supabase.table("master_data").insert(m_list[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_list):,}건 완료")
                st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary"):
        with st.spinner("DB 비우는 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [item['id'] for item in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 4. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고", "📤 AS 출고", "📊 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_main")
    if in_file and st.button("🚀 정밀 입고 실행"):
        m_df_local = fetch_all_data("master_data")
        # 마스터 맵 생성 시 자재번호를 다시 한번 sanitize_code 처리하여 완벽 매칭 유도
        m_lookup = {sanitize_code(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in, p_bar = [], len(as_in), st.progress(0)
        for i, (_, row) in enumerate(as_in.iterrows()):
            cur_mat = sanitize_code(row.iloc[3]) # D열
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
            if len(recs) == 1000:
                supabase.table("as_history").insert(recs).execute()
                recs = []
                p_bar.progress((i+1)/total_in)
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"✅ {total_in:,}건 입고 완료")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_main")
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        # 출고 대상 필터링 (AS 카톤 박스 포함 행)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            # 오류 발생 지점 수정: 컬럼 인덱스 10(K열) 안전하게 추출
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            
            p_out = st.progress(0)
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
                p_out.progress(min((i+500)/len(out_keys), 1.0))
            st.success(f"✅ {len(out_keys):,}건 출고 업데이트 완료")

with tab3:
    if st.button("📈 분석 리포트 생성", use_container_width=True):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            c1, c2 = st.columns(2)
            c1.metric("수리대상", f"{len(df_rep):,} 건")
            c2.metric("미등록", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                # 결과 7개 컬럼 엑셀 생성
                df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
                df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_final.to_excel(writer, index=False)
                st.download_button("📥 상세 엑셀 다운로드", output.getvalue(), "AS_TAT_Final.xlsx", use_container_width=True)
            
            with st.expander("🔍 미등록 샘플 (자재번호 대조용)"):
                st.dataframe(df_raw[df_raw['분류구분'] == '미등록'][['자재번호', '규격']].head(100))
