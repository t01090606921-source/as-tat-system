import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("🚀 AS TAT 분석 및 관리 시스템")

# --- 2. [함수] 데이터 전수 로드 (필요 컬럼 확장) ---
def fetch_analysis_data():
    all_data = []
    limit = 1000
    offset = 0
    # 요청하신 상세 항목 출력을 위해 컬럼 추가 (자재번호, 규격, 압축코드 포함)
    columns = "입고일, 출고일, 자재번호, 규격, 공급업체명, 압축코드, 분류구분"
    
    status_area = st.empty()
    while True:
        res = supabase.table("as_history").select(columns).range(offset, offset + limit - 1).execute()
        all_data.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
        status_area.text(f"데이터 로드 중: {offset:,} 건...")
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 등록 및 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    st.subheader("1. 마스터 엑셀 등록")
    master_file = st.file_uploader("마스터 파일 선택", type=['xlsx'], key="m_up_fixed")
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df_raw = pd.read_excel(master_file, dtype=str)
        t_col = next((c for c in m_df_raw.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df_raw.columns[0])
        m_data = [{"자재번호": str(row[t_col]).strip().upper(), 
                   "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                   "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"} 
                  for _, row in m_df_raw.iterrows() if not pd.isna(row[t_col])]
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 등록 완료")
            st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 4. 입고 / 출고 관리 ---
tab1, tab2 = st.tabs(["📥 대량 입고 (수신)", "📤 대량 출고 (송신)"])
# (입/출고 로직은 57만 건 대응 분할 처리 방식 유지)
with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_key")
    if in_file and st.button("🚀 입고 실행"):
        # 마스터 로드 및 매칭 로직 (이전과 동일)
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = pd.DataFrame(m_res.data).set_index('자재번호').to_dict('index')
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        recs = []
        for i, (_, row) in enumerate(as_in.iterrows()):
            mat_val = str(row.iloc[3]).strip().upper()
            m_info = m_lookup.get(mat_val)
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), "자재번호": mat_val,
                "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) == 1000:
                supabase.table("as_history").insert(recs).execute()
                recs = []
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("✅ 입고 완료")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_key")
    if out_file and st.button("🚀 출고 실행"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", out_keys[i:i+500]).eq("상태", "출고 대기").execute()
            st.success("✅ 출고 완료")

# --- 5. [개선] 수리대상 상세 분석 리포트 ---
st.divider()
st.subheader("📊 수리대상 TAT 상세 분석 리포트")
if st.button("📈 분석 실행 및 상세 데이터 생성", use_container_width=True):
    with st.spinner("57만 건 데이터 전수 분석 중..."):
        df_raw = fetch_analysis_data()
    
    if not df_raw.empty:
        df_raw['입고일'] = pd.to_datetime(df_raw['입고일'], errors='coerce')
        df_raw['출고일'] = pd.to_datetime(df_raw['출고일'], errors='coerce')
        
        # '수리대상'만 추출 및 TAT 계산
        df_rep = df_raw[df_raw['분류구분'] == '수리대상'].copy()
        df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
        
        # 출력용 컬럼 순서 재배치 (요청 사항 반영)
        df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
        df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']

        # 요약 지표
        m1, m2, m3 = st.columns(3)
        m1.metric("수리대상 전체", f"{len(df_final):,} 건")
        m2.metric("평균 TAT", f"{df_final['TAT'].mean():.1f} 일")
        m3.metric("수리 미완료(진행중)", f"{df_final['출고일'].isna().sum():, } 건")

        # 업체별 요약 테이블
        st.write("### 🏢 업체별 TAT 통계 요약")
        summary = df_final[df_final['출고일'].notna()].groupby('공급업체명').agg(
            완료건수=('TAT', 'count'), 평균TAT=('TAT', 'mean')
        ).reset_index()
        summary['평균TAT'] = summary['평균TAT'].round(1)
        st.dataframe(summary.sort_values('평균TAT'), use_container_width=True, hide_index=True)
        
        # [개선] 엑셀 다운로드 (요청하신 모든 항목 포함)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False, sheet_name='Repair_TAT_Detail')
        
        st.download_button(
            label="📥 수리대상 상세 엑셀 다운로드 (입고/출고/품목/규격/업체/압축코드/TAT)",
            data=output.getvalue(),
            file_name="AS_Repair_TAT_Detail.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    else:
        st.info("분석할 데이터가 없습니다.")
