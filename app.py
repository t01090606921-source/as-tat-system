import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 통합 분석 시스템", layout="wide")
st.title("🚀 AS TAT 통합 관리 및 전수 분석")

# --- [자재번호 정밀 표준화 함수] ---
def sanitize_material_code(val):
    if pd.isna(val): return ""
    # 1. 문자열 변환 및 소수점(.0) 제거
    s_val = str(val).split('.')[0].strip().upper()
    # 2. 앞자리 0이 잘린 경우를 대비해 10자리(혹은 원하는 길이)로 맞춤 (zfill)
    # 귀사의 자재번호 자릿수에 맞춰 10 혹은 다른 숫자로 조정 가능합니다. 
    # 여기서는 안전하게 앞자리 0을 최대한 보존하는 방식 위주로 처리합니다.
    return s_val

# --- 2. 데이터 전수 로드 함수 (ID 기반) ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    limit = 1000
    status_area = st.empty()
    while True:
        res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(limit).execute()
        batch = res.data
        if not batch: break
        all_data.extend(batch)
        last_id = batch[-1]['id']
        status_area.text(f"📥 데이터 전수 수집 중: {len(all_data):,} 건...")
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 등록 및 초기화 ---
with st.sidebar:
    st.header("⚙️ 마스터 및 시스템 관리")
    m_file = st.file_uploader("1. 마스터 엑셀 등록", type=['xlsx'], key="m_side")
    if m_file and st.button("🚀 마스터 정밀 재등록"):
        with st.spinner("마스터 표준화 중..."):
            m_df = pd.read_excel(m_file, dtype=str)
            m_list = []
            for _, row in m_df.iterrows():
                mat_id = sanitize_material_code(row.iloc[0]) # A열: 자재번호
                if not mat_id: continue
                m_list.append({
                    "자재번호": mat_id,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보누락",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보누락"
                })
            if m_list:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_list), 200):
                    supabase.table("master_data").insert(m_list[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_list):,}건 등록 완료")
                st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary"):
        with st.spinner("삭제 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [item['id'] for item in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 4. 메인 기능: 입고 / 출고 / 리포트 통합 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고 처리", "📤 AS 출고 처리", "📊 TAT 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_main")
    if in_file and st.button("🚀 정밀 매칭 입고 실행"):
        # 마스터 로드 및 맵 생성
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in, p_bar = [], len(as_in), st.progress(0)
        for i, (_, row) in enumerate(as_in.iterrows()):
            # 입고 자재번호 표준화 (마스터와 동일 규칙)
            raw_mat = sanitize_material_code(row.iloc[3])
            m_info = m_lookup.get(raw_mat)
            
            recs.append({
                "압축코드": str(row.iloc[7]).strip(),
                "자재번호": raw_mat,
                "규격": str(row.iloc[5]).strip(),
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
    if out_file and st.button("🚀 대량 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", out_keys[i:i+500]).eq("상태", "출고 대기").execute()
            st.success(f"✅ {len(out_keys):,}건 출고 완료")

with tab3:
    if st.button("📈 57만 건 전수 분석 실행", use_container_width=True):
        df_raw = fetch_all_data("as_history", "id, 입고일, 출고일, 자재번호, 규격, 공급업체명, 압축코드, 분류구분")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("최종 수리대상 건수", f"{len(df_rep):,} 건")
            c2.metric("미등록 건수 (체크필요)", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'], errors='coerce')
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'], errors='coerce')
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                # 엑셀 파일 생성 (7개 열 포함)
                df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
                df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_final.to_excel(writer, index=False)
                st.download_button("📥 수리대상 상세 엑셀 다운로드", output.getvalue(), "AS_Repair_TAT_Final.xlsx", use_container_width=True)
            
            with st.expander("🔍 미등록 데이터 샘플 확인"):
                st.dataframe(df_raw[df_raw['분류구분'] == '미등록'][['자재번호', '규격']].head(50))
