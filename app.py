import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 정밀 분석 시스템", layout="wide")
st.title("📊 AS TAT 정밀 분석 및 관리 시스템")

# --- 2. [핵심] 데이터 전수 로드 함수 (ID 정렬 방식) ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    limit = 1000
    status_msg = st.empty()
    while True:
        res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(limit).execute()
        batch = res.data
        if not batch: break
        all_data.extend(batch)
        last_id = batch[-1]['id']
        status_msg.text(f"📥 데이터 수집 중: {len(all_data):,} 건 완료...")
    status_msg.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 및 시스템 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [마스터 등록] 자재번호 형식을 문자로 강제 고정
    st.subheader("1. 마스터 데이터 등록")
    m_file = st.file_uploader("마스터 엑셀 선택", type=['xlsx'], key="m_up")
    if m_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        with st.spinner("마스터 동기화 중..."):
            m_df_raw = pd.read_excel(m_file, dtype=str)
            m_list = []
            for _, row in m_df_raw.iterrows():
                mat_id = str(row.iloc[0]).strip().upper() # A열: 자재번호
                if not mat_id or mat_id == "NAN": continue
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
    # [전체 초기화] 57만 건을 안전하게 삭제하는 루프
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        with st.spinner("데이터 대량 삭제 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [item['id'] for item in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("데이터 초기화 완료")
        st.rerun()

# --- 4. 메인: 입고 / 출고 관리 ---
tab1, tab2 = st.tabs(["📥 정밀 입고 처리", "📤 대량 출고 처리"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_up")
    if in_file and st.button("🚀 정밀 매칭 입고 실행"):
        # 1. 마스터 정보 로드 및 매칭 준비
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']).strip().upper(): r for r in m_df_local.to_dict('records')}
        
        # 2. 입고 데이터 로드 (모든 컬럼 문자열로 읽어 '0' 누락 방지)
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in = [], len(as_in)
        p_bar, p_text = st.progress(0), st.empty()
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            raw_mat = str(row.iloc[3]).strip().upper() # D열: 자재번호
            m_info = m_lookup.get(raw_mat)
            
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), # H열: 압축코드
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
                p_text.text(f"처리 중: {i+1:,} / {total_in:,} 건")
        
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"✅ {total_in:,}건 입고 완료 (미등록 방지 로직 적용)")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 대량 출고 매칭 실행"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]] # K열: 압축코드
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success(f"✅ {len(out_keys):,}건 출고 업데이트 완료")

# --- 5. [전수 분석] 8만 건 검증 리포트 ---
st.divider()
st.subheader("📊 수리대상 TAT 전수 분석 리포트")
if st.button("📈 57만 건 분석 실행 (데이터 대조)", use_container_width=True):
    with st.spinner("데이터 전수 수집 및 분석 중..."):
        # 필수 컬럼 전수 로드
        df_raw = fetch_all_data("as_history", "id, 입고일, 출고일, 자재번호, 규격, 공급업체명, 압축코드, 분류구분")
    
    if not df_raw.empty:
        df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').astype(str).str.strip()
        
        # 수리대상 필터링 (8만 건 검출 핵심)
        df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
        
        if not df_rep.empty:
            df_rep['입고일'] = pd.to_datetime(df_rep['입고일'], errors='coerce')
            df_rep['출고일'] = pd.to_datetime(df_rep['출고일'], errors='coerce')
            df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
            
            # 대시보드
            m1, m2, m3 = st.columns(3)
            m1.metric("최종 수리대상", f"{len(df_rep):,} 건")
            m2.metric("미등록 데이터", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            m3.metric("평균 TAT", f"{df_rep['TAT'].mean():.1f} 일")

            # 업체별 성적표
            st.write("### 🏢 업체별 TAT 요약")
            summary = df_rep[df_rep['출고일'].notna()].groupby('공급업체명').agg(
                완료건수=('TAT', 'count'), 평균TAT=('TAT', 'mean')
            ).reset_index()
            summary['평균TAT'] = summary['평균TAT'].round(1)
            st.dataframe(summary.sort_values('완료건수', ascending=False), use_container_width=True, hide_index=True)

            # 상세 엑셀 생성
            df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
            df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False)
            st.download_button("📥 수리대상 상세 엑셀 다운로드", output.getvalue(), "AS_Repair_TAT_Detailed.xlsx", use_container_width=True)
            
            # [미등록 원인 분석용]
            with st.expander("🔍 미등록 데이터 샘플 (마스터 매칭 확인용)"):
                st.write(df_raw[df_raw['분류구분'] == '미등록'][['자재번호', '규격']].head(30))
                st.write("📌 위 자재번호들이 마스터 엑셀에 있는지, 형식이 일치하는지 확인하세요.")
        else:
            st.warning("수리대상 데이터가 0건입니다. '미등록' 건수를 확인하세요.")
