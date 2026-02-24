import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 및 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 전수 분석 시스템", layout="wide")
st.title("📊 AS TAT 전수 분석 시스템 (완성본)")

# --- 2. [함수] 데이터 전수 로드 (ID 기반 누락 방지) ---
def fetch_every_single_row(table_name, columns="*"):
    all_data = []
    last_id = -1
    limit = 1000
    status_area = st.empty()
    
    while True:
        # ID 순서대로 정렬하여 페이징 누락 완벽 차단
        res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(limit).execute()
        batch = res.data
        if not batch:
            break
        all_data.extend(batch)
        last_id = batch[-1]['id']
        status_area.text(f"📥 데이터 전수 수집 중: {len(all_data):,} 건 완료...")
    
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 관리 및 초기화 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    st.subheader("1. 마스터 엑셀 등록")
    master_file = st.file_uploader("마스터 파일 선택", type=['xlsx'], key="master_up")
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        with st.spinner("마스터 동기화 중..."):
            m_df = pd.read_excel(master_file, dtype=str)
            # 마스터 데이터를 리스트로 변환 (자재번호, 업체명, 분류구분)
            m_data = []
            for _, row in m_df.iterrows():
                mat_val = str(row.iloc[0]).strip().upper()
                if not mat_val or mat_val == "NAN": continue
                m_data.append({
                    "자재번호": mat_val,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보없음",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보없음"
                })
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_data):,}건 등록 완료")
                st.rerun()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        with st.spinner("DB 비우는 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [item['id'] for item in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 4. 메인: 입고 및 출고 처리 ---
tab1, tab2 = st.tabs(["📥 대량 입고 (수신)", "📤 대량 출고 (송신)"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_f")
    if in_file and st.button("🚀 입고 및 매칭 시작"):
        # 마스터 데이터 전수 로드 (매칭용)
        m_df_local = fetch_every_single_row("master_data")
        m_lookup = m_df_local.set_index('자재번호').to_dict('index') if not m_df_local.empty else {}
        
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in, p_bar = [], len(as_in), st.progress(0)
        status_txt = st.empty()
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            mat_val = str(row.iloc[3]).strip().upper()
            m_info = m_lookup.get(mat_val)
            
            recs.append({
                "압축코드": str(row.iloc[7]).strip(),
                "자재번호": mat_val,
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
                status_txt.text(f"📥 입고 처리 중: {i+1:,} / {total_in:,} 건")
        
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"✅ {total_in:,}건 입고 완료!")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_f")
    if out_file and st.button("🚀 출고 매칭 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[10]] # 압축코드 열
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            
            total_out = len(out_keys)
            for i in range(0, total_out, 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success(f"✅ {total_out:,}건 출고 처리 완료!")

# --- 5. [정밀 분석] 수리대상 상세 리포트 ---
st.divider()
st.subheader("📊 수리대상 TAT 정밀 분석 (8만 건 검증)")

if st.button("📈 57만 건 전수 분석 실행", use_container_width=True):
    with st.spinner("DB의 모든 데이터를 한 건도 빠짐없이 대조 중입니다..."):
        # ID를 포함한 전수 로드
        df_raw = fetch_every_single_row("as_history", "id, 입고일, 출고일, 자재번호, 규격, 공급업체명, 압축코드, 분류구분")
        
    if not df_raw.empty:
        # 데이터 정제
        df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').astype(str).str.strip()
        
        # '수리대상' 필터링 (8만 건 검출 핵심)
        df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
        
        if not df_rep.empty:
            df_rep['입고일'] = pd.to_datetime(df_rep['입고일'], errors='coerce')
            df_rep['출고일'] = pd.to_datetime(df_rep['출고일'], errors='coerce')
            df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
            
            # 대시보드 출력
            c1, c2, c3 = st.columns(3)
            c1.metric("검출된 수리대상", f"{len(df_rep):,} 건")
            c2.metric("전체 데이터 로드", f"{len(df_raw):,} 건")
            c3.metric("평균 TAT", f"{df_rep['TAT'].mean():.1f} 일")

            # 업체별 요약
            st.write("### 🏢 업체별 평균 TAT (소요기간)")
            summary = df_rep[df_rep['출고일'].notna()].groupby('공급업체명').agg(
                완료건수=('TAT', 'count'), 평균TAT=('TAT', 'mean')
            ).reset_index()
            summary['평균TAT'] = summary['평균TAT'].round(1)
            st.dataframe(summary.sort_values('완료건수', ascending=False), use_container_width=True, hide_index=True)

            # 상세 엑셀 생성 (요청한 7개 컬럼)
            df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
            df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False)
            
            st.download_button("📥 수리대상 상세 엑셀 다운로드", output.getvalue(), "AS_Repair_Final_Report.xlsx", use_container_width=True)
            
            # 데이터 분류 검증기
            with st.expander("🔍 데이터 분류 현황 상세 (누락 원인 분석)"):
                st.write(df_raw['분류구분'].value_counts())
        else:
            st.warning("분류 결과 '수리대상'이 0건입니다. 아래 분류 현황을 확인하세요.")
            st.write(df_raw['분류구분'].value_counts())
