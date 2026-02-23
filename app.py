import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (최종 완성본)")

# --- 2. 사이드바: 마스터 데이터 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # 마스터 전수 로드 함수 (Pagination 적용)
    def get_all_master_data():
        all_data = []
        limit = 1000
        offset = 0
        while True:
            res = supabase.table("master_data").select("*").range(offset, offset + limit - 1).execute()
            all_data.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
        return pd.DataFrame(all_data)

    try:
        m_df_local = get_all_master_data()
        st.metric("마스터 데이터 동기화", f"{len(m_df_local):,} 건")
    except:
        m_df_local = pd.DataFrame()

    st.subheader("1. 마스터 갱신")
    master_file = st.file_uploader("마스터 엑셀", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록"):
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
            st.success("✅ 마스터 갱신 완료")
            st.rerun()

    st.divider()
    if st.button("⚠️ 전체 데이터 초기화"):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 3. 입고/출고 로직 (출고 속도 최적화) ---
tab1, tab2 = st.tabs(["📥 AS 입고 (수신)", "📤 AS 출고 (송신)"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_fin")
    if in_file and st.button("입고 및 매칭 실행"):
        if m_df_local.empty:
            st.error("마스터 데이터를 먼저 등록해주세요.")
        else:
            m_lookup = m_df_local.set_index('자재번호').to_dict('index')
            df = pd.read_excel(in_file, dtype=str)
            as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            recs = []
            for _, row in as_in.iterrows():
                mat_val = str(row.iloc[3]).strip().upper()
                m_info = m_lookup.get(mat_val)
                recs.append({
                    "압축코드": str(row.iloc[7]).strip(), "자재번호": mat_val,
                    "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
            if recs:
                for i in range(0, len(recs), 200):
                    supabase.table("as_history").insert(recs[i:i+200]).execute()
                st.success(f"{len(recs)}건 입고 완료")
                st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_fin")
    if out_file and st.button("출고 대량 처리"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            status_text = st.empty()
            # [최적화] 모든 압축코드를 한 번에 리스트화하여 DB 조회 (로딩 방지)
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            
            # DB에서 해당 압축코드를 가진 '출고 대기' 데이터 일괄 조회
            targets = supabase.table("as_history").select("id, 압축코드").in_("압축코드", out_keys).eq("상태", "출고 대기").execute()
            
            if targets.data:
                # 매칭된 ID들에 대해 출고일 및 상태 업데이트
                target_ids = [r['id'] for r in targets.data]
                out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
                
                # 대량 업데이트 (ID 기반)
                for tid in target_ids:
                    supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).eq("id", tid).execute()
                
                st.success(f"✅ {len(target_ids)}건 출고 처리 완료!")
                st.rerun()
            else:
                st.warning("매칭되는 출고 대기 데이터가 없습니다.")

# --- 4. 리포트, 필터 3종, 다운로드 버튼 ---
st.divider()
res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
if res.data:
    df_res = pd.DataFrame(res.data)
    st.subheader("📊 TAT 통합 분석 현황")
    
    # 필터 3종 복구
    c1, c2, c3 = st.columns(3)
    v_f = c1.multiselect("🏢 공급업체 필터", sorted(df_res['공급업체명'].unique()))
    g_f = c2.multiselect("📂 분류구분 필터", sorted(df_res['분류구분'].unique())) # 분류구분 복구
    s_f = c3.multiselect("🚚 상태 필터", sorted(df_res['상태'].unique()))
    
    dff = df_res.copy()
    if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
    if g_f: dff = dff[dff['분류구분'].isin(g_f)]
    if s_f: dff = dff[dff['상태'].isin(s_f)]

    # 상단 지표
    m1, m2, m3 = st.columns(3)
    m1.metric("전체 데이터", f"{len(dff)} 건")
    m2.metric("미등록 (보정필요)", f"{len(dff[dff['공급업체명'] == '미등록'])} 건", delta_color="inverse")
    m3.metric("출고 대기중", f"{len(dff[dff['상태'] == '출고 대기'])} 건")

    # [다운로드 버튼 복구]
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dff.to_excel(writer, index=False, sheet_name='TAT_Report')
    
    st.download_button(
        label="📥 현재 필터링된 결과 다운로드 (Excel)",
        data=output.getvalue(),
        file_name="AS_TAT_Report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.dataframe(dff, use_container_width=True, hide_index=True)
