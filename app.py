import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("🚀 AS TAT 분석 및 관리 시스템 (초기화 오류 수정)")

# --- 2. [함수] 데이터 전수 로드 ---
def fetch_analysis_data():
    all_data = []
    limit = 1000
    offset = 0
    cols = "입고일, 출고일, 자재번호, 규격, 공급업체명, 압축코드, 분류구분"
    
    status_area = st.empty()
    while True:
        try:
            res = supabase.table("as_history").select(cols).range(offset, offset + limit - 1).execute()
            if not res.data: break
            all_data.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
            status_area.text(f"데이터 로드 중: {offset:,} 건...")
        except Exception as e:
            st.error(f"데이터 로드 중 오류 발생: {e}")
            break
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 등록 및 [개선] 초기화 로직 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    st.subheader("1. 마스터 엑셀 등록")
    master_file = st.file_uploader("마스터 파일 선택", type=['xlsx'], key="m_up_final")
    
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df_raw = pd.read_excel(master_file, dtype=str)
        m_data = []
        for _, row in m_df_raw.iterrows():
            mat_val = str(row.iloc[0]).strip().upper()
            if not mat_val or mat_val == "NAN": continue
            m_data.append({
                "자재번호": mat_val,
                "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"
            })
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 등록 완료")
            st.rerun()

    st.divider()
    
    # [🔥 오류 해결] 대량 데이터 분할 삭제 로직
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        with st.spinner("57만 건 데이터 대량 삭제 중... (잠시만 기다려 주세요)"):
            try:
                # 한 번에 다 지우지 않고, 데이터가 없을 때까지 반복 삭제
                while True:
                    # id를 1,000개씩 가져와서 해당 ID들만 삭제 (가장 안전한 방식)
                    res = supabase.table("as_history").select("id").limit(1000).execute()
                    if not res.data:
                        break
                    ids_to_delete = [item['id'] for item in res.data]
                    supabase.table("as_history").delete().in_("id", ids_to_delete).execute()
                st.success("✅ 모든 데이터가 깨끗하게 삭제되었습니다.")
                st.rerun()
            except Exception as e:
                st.error(f"초기화 중 오류 발생: {e}\n잠시 후 다시 시도해 주세요.")

# --- 4. 입고 / 출고 관리 (기존 유지) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_f")
    if in_file and st.button("🚀 입고 실행"):
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {r['자재번호']: r for r in m_res.data}
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        recs, total_in, p_bar = [], len(as_in), st.progress(0)
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
                p_bar.progress((i+1)/total_in)
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"✅ {total_in:,}건 입고 완료")

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_f")
    if out_file and st.button("🚀 출고 실행"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success("✅ 출고 완료")

# --- 5. 수리대상 상세 분석 리포트 ---
st.divider()
st.subheader("📊 수리대상 TAT 상세 분석 리포트")
if st.button("📈 분석 실행 및 상세 데이터 생성", use_container_width=True):
    with st.spinner("데이터 분석 중..."):
        df_raw = fetch_analysis_data()
    if not df_raw.empty:
        df_raw['입고일'] = pd.to_datetime(df_raw['입고일'], errors='coerce')
        df_raw['출고일'] = pd.to_datetime(df_raw['출고일'], errors='coerce')
        df_rep = df_raw[df_raw['분류구분'] == '수리대상'].copy()
        if not df_rep.empty:
            df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
            df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
            df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']
            
            m1, m2 = st.columns(2)
            m1.metric("수리대상 건수", f"{len(df_final):,} 건")
            m2.metric("평균 TAT", f"{df_final['TAT'].mean():.1f} 일")
            
            st.write("### 🏢 업체별 TAT 요약")
            summary = df_final[df_final['출고일'].notna()].groupby('공급업체명').agg(
                완료건수=('TAT', 'count'), 평균TAT=('TAT', 'mean')
            ).reset_index()
            summary['평균TAT'] = summary['평균TAT'].round(1)
            st.dataframe(summary.sort_values('평균TAT'), use_container_width=True, hide_index=True)
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False)
            st.download_button("📥 수리대상 상세 엑셀 다운로드", output.getvalue(), "Repair_TAT_Report.xlsx", use_container_width=True)
        else:
            st.warning("'수리대상' 데이터가 없습니다.")
