import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

def raw_clean(val):
    """데이터 왜곡 방지: 형변환 없이 문자열로 강제 고정 후 공백만 제거"""
    if pd.isna(val): return ""
    # 어떤 형태든 문자열로 변환
    s = str(val).strip()
    # 엑셀 특유의 .0 접미사만 제거 (숫자로 읽혔을 경우 대비)
    if s.endswith('.0'):
        s = s[:-2]
    return s.upper()

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (데이터 보존 모드)")

# --- 2. 사이드바: 설정 및 정밀 재매칭 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    
    st.subheader("1. 마스터 데이터 관리")
    master_file = st.file_uploader("마스터 업로드 (엑셀)", type=['xlsx'])
    if master_file and st.button("🚀 마스터 DB 갱신", use_container_width=True):
        try:
            # 모든 열을 '문자열'로 읽어오도록 지정 (dtype=str)
            m_df = pd.read_excel(master_file, dtype=str)
            m_data = []
            for _, row in m_df.iterrows():
                # A(0): 품목코드, F(5): 공급업체, K(10): 분류구분
                mat_no = raw_clean(row.iloc[0])
                if not mat_no: continue
                m_data.append({
                    "자재번호": mat_no,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "미등록",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "미등록"
                })
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 500):
                    supabase.table("master_data").insert(m_data[i:i+500]).execute()
                st.success(f"✅ 마스터 {len(m_data)}건 동기화 완료!")
        except Exception as e:
            st.error(f"마스터 로드 오류: {e}")

    st.divider()
    st.subheader("2. 정보 보정 (1:1 문자열 대조)")
    if st.button("🔄 미등록 정보 정밀 재매칭", use_container_width=True):
        with st.spinner("마스터와 1:1 대조 중..."):
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            up_cnt = 0
            for row in h_res.data:
                # DB에 저장된 번호를 다시 정제하여 마스터와 대조
                clean_key = raw_clean(row['자재번호'])
                m_info = m_lookup.get(clean_key)
                if m_info:
                    supabase.table("as_history").update({
                        "공급업체명": m_info['공급업체명'], 
                        "분류구분": m_info['분류구분']
                    }).eq("id", row['id']).execute()
                    up_cnt += 1
            st.success(f"✅ {up_cnt}건 매칭 성공!")
            st.rerun()

    st.divider()
    st.subheader("3. 시스템 초기화")
    if st.button("⚠️ 전체 삭제", type="primary"):
        if st.checkbox("데이터 삭제 확약"):
            supabase.table("as_history").delete().neq("id", -1).execute()
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            st.rerun()

# --- 3. 입고/출고 탭 ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])

with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_up")
    if in_file and st.button("입고 처리 실행"):
        # 모든 데이터를 문자열로 로드하여 'R' 탈락 방지
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {r['자재번호']: r for r in m_res.data}
        
        new_recs = []
        for _, row in as_in.iterrows():
            # D(3): 품목코드, H(7): 압축코드, F(5): 규격
            mat_no = raw_clean(row.iloc[3])
            m_info = m_lookup.get(mat_no)
            new_recs.append({
                "압축코드": str(row.iloc[7]).strip(), 
                "자재번호": mat_no,
                "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d'),
                "상태": "출고 대기"
            })
        if new_recs:
            for i in range(0, len(new_recs), 500):
                supabase.table("as_history").insert(new_recs[i:i+500]).execute()
            st.success("✅ 입고 완료")
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("출고 매칭"):
        df = pd.read_excel(out_file, dtype=str)
        # D(3): AS 카톤 박스, K(10): 압축코드, G(6): 출고일
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key = str(row.iloc[10]).strip()
            out_date = pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id, 입고일").match({"압축코드": key, "상태": "출고 대기"}).order("입고일").limit(1).execute()
            if target.data:
                in_dt = pd.to_datetime(target.data[0]['입고일'])
                tat = round((out_date - in_dt).total_seconds() / (24 * 3600), 2)
                supabase.table("as_history").update({"출고일": out_date.strftime('%Y-%m-%d'), "tat": tat, "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.success("✅ 출고 매칭 완료")
        st.rerun()

# --- 4. 리포트 & 필터 & 다운로드 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    all_data = pd.DataFrame(res.data)

    if not all_data.empty:
        st.subheader("📊 실시간 분석 리포트")
        
        c1, c2, c3 = st.columns(3)
        v_filter = c1.multiselect("🏢 공급업체", sorted(all_data['공급업체명'].unique()))
        g_filter = c2.multiselect("📂 분류구분", sorted(all_data['분류구분'].unique()))
        s_filter = c3.multiselect("🚚 상태", ['출고 대기', '출고 완료'])

        dff = all_data.copy()
        if v_filter: dff = dff[dff['공급업체명'].isin(v_filter)]
        if g_filter: dff = dff[dff['분류구분'].isin(g_filter)]
        if s_filter: dff = dff[dff['상태'].isin(s_filter)]

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("전체 건수", f"{len(dff):,} 건")
        fin = dff[dff['상태'] == '출고 완료']
        m2.metric("평균 TAT", f"{round(pd.to_numeric(fin['tat']).mean(), 1) if not fin.empty else 0} 일")
        m3.metric("미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록']):,} 건")
        m4.metric("현재 대기", f"{len(dff[dff['상태'] == '출고 대기']):,} 건")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            dff.to_excel(writer, index=False)
        st.download_button("📥 리포트 다운로드", output.getvalue(), f"AS_Report.xlsx")

        st.dataframe(dff, use_container_width=True, hide_index=True)
except:
    pass
