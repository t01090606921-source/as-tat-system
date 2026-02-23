import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템 (대용량 모드)", layout="wide")
st.title("🚀 AS TAT 분석 시스템 (57만 건 대응 버전)")

# --- 2. 사이드바: 마스터 및 히스토리 전수 로드 함수 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [공통] 전수 로드 함수 (Pagination 방식)
    def fetch_all_data(table_name):
        all_data = []
        limit = 1000
        offset = 0
        status_area = st.empty() # 진행 상황 표시용
        while True:
            res = supabase.table(table_name).select("*").range(offset, offset + limit - 1).execute()
            all_data.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
            status_area.text(f"로드 중: {offset:,} 건...")
        status_area.empty()
        return pd.DataFrame(all_data)

    try:
        # 마스터는 상시 로드
        m_df_local = fetch_all_data("master_data")
        st.metric("마스터 데이터 동기화", f"{len(m_df_local):,} 건")
    except:
        m_df_local = pd.DataFrame()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary"):
        with st.spinner("삭제 중..."):
            # 대량 삭제 시에도 제한이 있을 수 있어 반복 처리
            supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("데이터가 초기화되었습니다.")
        st.rerun()

# --- 3. 입고 처리 (57만 건 분할 업로드) ---
tab1, tab2 = st.tabs(["📥 대량 입고", "📤 대량 출고"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드 (최대 57만 건)", type=['xlsx'])
    if in_file and st.button("🚀 전수 입고 실행 (시간 소요)"):
        if m_df_local.empty:
            st.error("마스터 데이터를 먼저 등록해주세요.")
        else:
            m_lookup = m_df_local.set_index('자재번호').to_dict('index')
            df = pd.read_excel(in_file, dtype=str)
            as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            
            recs = []
            total_in = len(as_in)
            progress_bar = st.progress(0)
            status_text = st.empty()
            
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
                
                # 1,000건마다 DB에 전송 (메모리 부족 방지 및 DB 제한 우회)
                if len(recs) == 1000:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    prog = (i + 1) / total_in
                    progress_bar.progress(prog)
                    status_text.text(f"업로드 중... {i+1}/{total_in} 건")
            
            # 남은 데이터 처리
            if recs:
                supabase.table("as_history").insert(recs).execute()
            
            st.success(f"✅ 총 {total_in:,} 건 입고 완료!")
            st.rerun()

with tab2:
    # (출고 로직도 입고와 동일하게 대량 처리 대응 완료)
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'])
    if out_file and st.button("출고 대량 매칭"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            
            # 500건씩 잘라서 업데이트 (DB URL 길이 제한 방지)
            for i in range(0, len(out_keys), 500):
                batch_keys = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"})\
                    .in_("압축코드", batch_keys).eq("상태", "출고 대기").execute()
            
            st.success("출고 처리가 완료되었습니다.")
            st.rerun()

# --- 4. 리포트 (전수 로드 및 필터) ---
st.divider()
if st.button("📊 현황 리포트 불러오기 (전체 데이터 로드)"):
    with st.spinner("57만 건 데이터를 불러오는 중입니다... (약 1~2분 소요)"):
        df_res = fetch_all_data("as_history")
    
    if not df_res.empty:
        st.subheader("📊 TAT 통합 분석 현황")
        
        # 필터 3종
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("🏢 공급업체", sorted(df_res['공급업체명'].unique()))
        g_f = c2.multiselect("📂 분류구분", sorted(df_res['분류구분'].unique()))
        s_f = c3.multiselect("🚚 상태", sorted(df_res['상태'].unique()))
        
        dff = df_res.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if g_f: dff = dff[dff['분류구분'].isin(g_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]

        m1, m2, m3 = st.columns(3)
        m1.metric("전체 데이터", f"{len(dff):,} 건")
        m2.metric("미등록", f"{len(dff[dff['공급업체명'] == '미등록']):,} 건")
        m3.metric("출고 대기", f"{len(dff[dff['상태'] == '출고 대기']):,} 건")

        # 다운로드 (대용량은 다운로드 시에도 메모리 주의)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            dff.to_excel(writer, index=False)
        st.download_button("📥 엑셀 다운로드", output.getvalue(), "AS_Report.xlsx")

        # 데이터가 너무 많으면 상위 1만 건만 표시 (성능상)
        if len(dff) > 10000:
            st.warning("데이터가 너무 많아 상위 10,000건만 화면에 표시합니다. 전체는 다운로드를 이용하세요.")
            st.dataframe(dff.head(10000), use_container_width=True, hide_index=True)
        else:
            st.dataframe(dff, use_container_width=True, hide_index=True)
