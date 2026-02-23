import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (실시간 정밀 보정)")

# --- 2. 사이드바: 관리 및 초기화 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count = supabase.table("master_data").select("자재번호", count="exact").execute().count
        st.metric("마스터 DB 등록 건수", f"{m_count if m_count else 0:,} 건")
    except: pass

    st.subheader("1. 마스터 갱신")
    master_file = st.file_uploader("마스터 엑셀", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df = pd.read_excel(master_file, dtype=str)
        t_col = next((c for c in m_df.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df.columns[0])
        m_data = [{"자재번호": str(row[t_col]).strip(), 
                   "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                   "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"} 
                  for _, row in m_df.iterrows() if not pd.isna(row[t_col])]
        
        if m_data:
            supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
            for i in range(0, len(m_data), 200):
                supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 갱신 완료")
            st.rerun()

    st.divider()
    st.subheader("2. 미등록 183건 정밀 추적")
    if st.button("🔥 실시간 정밀 보정 시작", type="primary", use_container_width=True):
        # 1. 마스터 전체 로드
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {str(r['자재번호']).strip(): r for r in m_res.data}
        
        # 2. 미등록 상태인 히스토리만 로드
        h_res = supabase.table("as_history").select("id, 자재번호").eq("공급업체명", "미등록").execute()
        
        if not h_res.data:
            st.info("보정할 미등록 데이터가 없습니다.")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            success_cnt = 0
            total = len(h_res.data)
            
            for i, row in enumerate(h_res.data):
                target_val = str(row['자재번호']).strip()
                if target_val in m_lookup:
                    # 건별로 확실하게 업데이트
                    supabase.table("as_history").update({
                        "공급업체명": m_lookup[target_val]['공급업체명'],
                        "분류구분": m_lookup[target_val]['분류구분']
                    }).eq("id", row['id']).execute()
                    success_cnt += 1
                
                # 진행률 표시
                prog = (i + 1) / total
                progress_bar.progress(prog)
                status_text.text(f"진행 중: {i+1}/{total} (성공: {success_cnt})")
                
            st.success(f"✅ 보정 완료! 총 {success_cnt}건의 정보가 업데이트되었습니다.")
            time.sleep(1)
            st.rerun()

    if st.button("⚠️ 데이터 전체 삭제", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 3. 입고/출고 ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in")
    if in_file and st.button("입고 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs = []
        for _, row in as_in.iterrows():
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), "자재번호": str(row.iloc[3]).strip(),
                "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                "공급업체명": "미등록", "분류구분": "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
        if recs:
            for i in range(0, len(recs), 200):
                supabase.table("as_history").insert(recs[i:i+200]).execute()
            st.success("데이터가 입력되었습니다. 사이드바의 '정밀 보정'을 눌러주세요.")
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out")
    if out_file and st.button("출고 실행"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key, date = str(row.iloc[10]).strip(), pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id").match({"압축코드": key, "상태": "출고 대기"}).limit(1).execute()
            if target.data:
                supabase.table("as_history").update({"출고일": date.strftime('%Y-%m-%d'), "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.success("출고 처리 완료")
        st.rerun()

# --- 4. 리포트 & 필터 & 다운로드 ---
st.divider()
res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
if res.data:
    df_res = pd.DataFrame(res.data)
    st.subheader("📊 현황 리포트")
    
    # 필터
    c1, c2, c3 = st.columns(3)
    v_f = c1.multiselect("🏢 공급업체 필터", sorted(df_res['공급업체명'].unique()))
    g_f = c2.multiselect("📂 분류구분 필터", sorted(df_res['분류구분'].unique()))
    s_f = c3.multiselect("🚚 상태 필터", sorted(df_res['상태'].unique()))
    
    dff = df_res.copy()
    if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
    if g_f: dff = dff[dff['분류구분'].isin(g_f)]
    if s_f: dff = dff[dff['상태'].isin(s_f)]

    m1, m2 = st.columns(2)
    m1.metric("총 건수", f"{len(dff)} 건")
    m2.metric("미등록", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dff.to_excel(writer, index=False)
    st.download_button("📥 결과 다운로드", output.getvalue(), "Report.xlsx")
    st.dataframe(dff, use_container_width=True, hide_index=True)
