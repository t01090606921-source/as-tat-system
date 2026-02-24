import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 통합 분석 시스템", layout="wide")
st.title("🚀 AS TAT 시스템 (대용량 오류 완벽 대응)")

# --- [자재번호 정밀 표준화 함수] ---
def sanitize_code(val):
    if pd.isna(val): return ""
    # .0 제거 및 공백 제거, 대문자화 (미등록 방지 핵심)
    return str(val).split('.')[0].strip().upper()

# --- 2. 데이터 전수 로드 (안정성 강화) ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    last_id = -1
    status_area = st.empty()
    while True:
        try:
            res = supabase.table(table_name).select(columns).gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            all_data.extend(res.data)
            last_id = res.data[-1]['id']
            status_area.text(f"📥 데이터 수집 중: {len(all_data):,} 건...")
        except:
            time.sleep(1)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 등록 및 초기화 ---
with st.sidebar:
    st.header("⚙️ 관리자 도구")
    m_file = st.file_uploader("1. 마스터 등록", type=['xlsx'])
    if m_file and st.button("🚀 마스터 강제 재등록"):
        with st.spinner("마스터 분석 중..."):
            m_df = pd.read_excel(m_file, dtype=str)
            m_list = []
            for _, row in m_df.iterrows():
                # 이미지 기준으로 '자재번호'는 첫 번째 열, '공급업체명'은 6번째, '분류구분'은 11번째 열
                mat_id = sanitize_code(row.iloc[0])
                if not mat_id or mat_id == 'NAN': continue
                m_list.append({
                    "자재번호": mat_id,
                    "공급업체명": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "미등록",
                    "분류구분": str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else "미등록"
                })
            if m_list:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_list), 200):
                    supabase.table("master_data").insert(m_list[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_list):,}건 등록 완료")
                st.rerun()

    if st.button("⚠️ DB 전체 초기화", type="primary"):
        with st.spinner("비우는 중..."):
            while True:
                res = supabase.table("as_history").select("id").limit(1000).execute()
                if not res.data: break
                ids = [i['id'] for i in res.data]
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("초기화 완료")
        st.rerun()

# --- 4. 메인 기능: 입고 / 출고 / 분석 ---
tab1, tab2, tab3 = st.tabs(["📥 AS 입고", "📤 AS 출고", "📊 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'])
    if in_file and st.button("🚀 입고 실행"):
        # 1. 마스터 데이터를 메모리에 로드 (이게 가장 빠르고 확실함)
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        # 2. 입고 엑셀 처리
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in = [], len(as_in)
        p_bar = st.progress(0)
        status_txt = st.empty()
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            try:
                # 자재번호 매칭 (D열 = index 3)
                cur_mat = sanitize_code(row.iloc[3])
                m_info = m_lookup.get(cur_mat)
                
                recs.append({
                    "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                    "자재번호": cur_mat,
                    "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                    "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
                
                # 500건씩 끊어서 입력 (1000건보다 500건이 네트워크상 더 안전함)
                if len(recs) >= 500:
                    try:
                        supabase.table("as_history").insert(recs).execute()
                    except Exception as e:
                        st.warning(f"일부 행 입력 재시도 중... ({i}건 부근)")
                        time.sleep(2)
                        supabase.table("as_history").insert(recs).execute()
                    recs = []
                    p_bar.progress((i+1)/total_in)
                    status_txt.text(f"진행 상황: {i+1:,} / {total_in:,} 건")
            except:
                continue
                
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"✅ {total_in:,}건 입고 완료!")

with tab2:
    # 출고 로직 (이전과 동일하게 유지)
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'])
    if out_file and st.button("🚀 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", out_keys[i:i+500]).execute()
            st.success("✅ 출고 완료")

with tab3:
    if st.button("📈 전수 분석 및 다운로드"):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            st.metric("최종 수리대상 건수", f"{len(df_rep):,} 건")
            st.metric("미등록 건수", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 결과 엑셀 다운로드", output.getvalue(), "AS_TAT_Final.xlsx")
