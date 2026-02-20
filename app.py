import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

def final_clean(val):
    """품목코드 원형 보존 정제: 공백만 제거"""
    if pd.isna(val): return ""
    return str(val).strip().upper()

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (데이터 무결성 모드)")

# --- 2. 사이드바: 설정 및 현황 확인 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    
    # DB 상태 실시간 확인
    try:
        m_count = supabase.table("master_data").select("자재번호", count="exact").execute()
        st.info(f"📊 현재 DB 등록 마스터: {m_count.count} 건")
    except:
        pass

    st.subheader("1. 마스터 데이터 관리")
    master_file = st.file_uploader("마스터 업로드 (엑셀)", type=['xlsx'])
    if master_file and st.button("🚀 마스터 DB 갱신 (전체 삭제 후 재등록)", use_container_width=True):
        try:
            m_df = pd.read_excel(master_file, dtype=str)
            m_data = []
            for _, row in m_df.iterrows():
                mat_no = final_clean(row.iloc[0])
                if not mat_no: continue
                m_data.append({
                    "자재번호": mat_no,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "미등록",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "미등록"
                })
            
            if m_data:
                # 1. 기존 데이터 완전 삭제 (성공할 때까지 확인)
                supabase.table("master_data").delete().neq("자재번호", "EMPTY_KEY").execute()
                
                # 2. 데이터 분할 삽입 (안정적인 200개 단위)
                batch_size = 200
                total = len(m_data)
                progress_bar = st.progress(0)
                for i in range(0, total, batch_size):
                    batch = m_data[i:i+batch_size]
                    supabase.table("master_data").insert(batch).execute()
                    progress_bar.progress(min((i + batch_size) / total, 1.0))
                
                st.success(f"✅ {total}건 마스터 등록 완료! 페이지를 새로고침하세요.")
                st.rerun()
        except Exception as e:
            st.error(f"마스터 오류: {e}")

    st.divider()
    st.subheader("2. 정보 보정")
    if st.button("🔄 미등록 정보 정밀 재매칭", use_container_width=True):
        with st.spinner("DB 직접 대조 중..."):
            # 마스터 전체 다시 로드
            m_res = supabase.table("master_data").select("*").execute()
            m_lookup = {r['자재번호']: r for r in m_res.data}
            
            h_res = supabase.table("as_history").select("id, 자재번호").execute()
            up_cnt = 0
            for row in h_res.data:
                clean_key = final_clean(row['자재번호'])
                m_info = m_lookup.get(clean_key)
                if m_info:
                    supabase.table("as_history").update({
                        "공급업체명": m_info['공급업체명'], 
                        "분류구분": m_info['분류구분']
                    }).eq("id", row['id']).execute()
                    up_cnt += 1
            st.success(f"✅ {up_cnt}건 보정 성공!")
            st.rerun()

# --- 3. 입고/출고 처리 (로직 동일) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])
# ... (기존 입고/출고 로직 유지) ...
with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_up")
    if in_file and st.button("입고 처리 실행"):
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        m_res = supabase.table("master_data").select("*").execute()
        m_lookup = {r['자재번호']: r for r in m_res.data}
        new_recs = []
        for _, row in as_in.iterrows():
            mat_no = final_clean(row.iloc[3])
            m_info = m_lookup.get(mat_no)
            new_recs.append({
                "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
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
            st.success("✅ 완료")
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("출고 매칭"):
        df = pd.read_excel(out_file, dtype=str)
        as_out = df[df.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        for _, row in as_out.iterrows():
            key = str(row.iloc[10]).strip()
            out_date = pd.to_datetime(row.iloc[6])
            target = supabase.table("as_history").select("id, 입고일").match({"압축코드": key, "상태": "출고 대기"}).order("입고일").limit(1).execute()
            if target.data:
                in_dt = pd.to_datetime(target.data[0]['입고일'])
                tat = round((out_date - in_dt).total_seconds() / (24 * 3600), 2)
                supabase.table("as_history").update({"출고일": out_date.strftime('%Y-%m-%d'), "tat": tat, "상태": "출고 완료"}).eq("id", target.data[0]['id']).execute()
        st.success("✅ 완료")
        st.rerun()

# --- 4. 리포트 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    all_data = pd.DataFrame(res.data)
    if not all_data.empty:
        st.subheader("📊 현황 리포트")
        m1, m2, m3 = st.columns(3)
        m1.metric("총 건수", f"{len(all_data):,} 건")
        m2.metric("미등록 건수", f"{len(all_data[all_data['공급업체명'] == '미등록']):,} 건")
        st.dataframe(all_data, use_container_width=True, hide_index=True)
except:
    pass
