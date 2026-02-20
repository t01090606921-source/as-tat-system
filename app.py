import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (78건 최종 소거 모드)")

# --- 2. 사이드바: 관리 기능 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    try:
        m_count_res = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.metric("현재 DB 내 마스터 건수", f"{m_count_res.count:,} 건")
    except: pass

    st.subheader("1. 마스터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'])
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        # [중요] engine='openpyxl'을 명시하고 모든 값을 object(문자열)로 강제 로드
        m_df = pd.read_excel(master_file, dtype=str, engine='openpyxl')
        
        # '품목코드' 또는 '자재번호' 열 정확히 찾기
        target_col = ""
        for col in m_df.columns:
            if "품목코드" in str(col) or "자재번호" in str(col):
                target_col = col
                break
        
        if target_col:
            m_data = []
            for _, row in m_df.iterrows():
                # 어떠한 가공도 없이 문자열 그대로 추출
                mat_val = str(row[target_col]).strip()
                if not mat_val or mat_val == "nan": continue
                
                m_data.append({
                    "자재번호": mat_val,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "정보없음",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "정보없음"
                })
            
            if m_data:
                # 데이터가 확실히 있으므로 안전하게 삭제 후 입력
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                # 100건씩 안전 분할 업로드
                for i in range(0, len(m_data), 100):
                    supabase.table("master_data").insert(m_data[i:i+100]).execute()
                st.success(f"✅ {len(m_data)}건 마스터가 원본 그대로 등록되었습니다.")
                st.rerun()

    st.divider()
    if st.button("🔥 남은 78건 끝장 재매칭", use_container_width=True):
        with st.spinner("최종 대조 중..."):
            m_res = supabase.table("master_data").select("*").execute()
            # 딕셔너리 생성 시 문자열 일치 극대화
            m_lookup = {str(r['자재번호']): r for r in m_res.data}
            
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            up_cnt = 0
            for row in h_res.data:
                mat_val = str(row['자재번호']).strip()
                if mat_val in m_lookup:
                    supabase.table("as_history").update({
                        "공급업체명": m_lookup[mat_val]['공급업체명'], 
                        "분류구분": m_lookup[mat_val]['분류구분']
                    }).eq("id", row['id']).execute()
                    up_cnt += 1
            st.success(f"✅ {up_cnt}건 매칭 완료!")
            st.rerun()

# --- 3. 입고/출고 로직 (변동 없음) ---
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
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out")
    if out_file and st.button("출고 실행"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key = str(row.iloc[10]).strip()
            date = pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id").match({"압축코드": key, "상태": "출고 대기"}).limit(1).execute()
            if target.data:
                supabase.table("as_history").update({"출고일": date.strftime('%Y-%m-%d'), "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.rerun()

# --- 4. 리포트 표시 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    if res.data:
        data = pd.DataFrame(res.data)
        st.subheader("📊 현황 리포트")
        # 필터 3종
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("🏢 공급업체", sorted(data['공급업체명'].unique()))
        g_f = c2.multiselect("📂 분류구분", sorted(data['분류구분'].unique()))
        s_f = c3.multiselect("🚚 상태", sorted(data['상태'].unique()))
        
        dff = data.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if g_f: dff = dff[dff['분류구분'].isin(g_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]
        
        m1, m2 = st.columns(2)
        m1.metric("총 건수", f"{len(dff)} 건")
        m2.metric("미등록", f"{len(dff[dff['공급업체명'] == '미등록'])} 건")
        
        st.dataframe(dff, use_container_width=True, hide_index=True)
except: pass
