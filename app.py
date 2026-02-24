import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
# secrets에 SUPABASE_URL과 SUPABASE_KEY가 등록되어 있어야 합니다.
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 통합 분석 시스템", layout="wide")

# --- [자재번호 정밀 표준화 함수] ---
def sanitize_code(val):
    if pd.isna(val): return ""
    # 1. 문자열 변환 및 소수점 제거
    s_val = str(val).split('.')[0].strip().upper()
    # 2. 앞자리 0이 잘린 경우(예: 123 -> 00123)를 대비해 10자리 zfill (필요시 숫자 조정)
    # 만약 자사 자재번호가 고정 길이라면 아래 주석을 해제하세요.
    # s_val = s_val.zfill(10) 
    return s_val

# --- 2. 데이터 전수 로드 함수 (ID 기반 무한 루프 방지) ---
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
            status_area.info(f"📥 DB 데이터 수집 중... (현재 {len(all_data):,} 건)")
        except Exception as e:
            time.sleep(1)
            continue
    status_area.empty()
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 및 시스템 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [마스터 등록]
    st.subheader("1. 마스터 데이터 등록")
    m_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'], key="side_m")
    if m_file and st.button("🚀 마스터 정밀 재등록", use_container_width=True):
        with st.spinner("마스터 표준화 매칭 중..."):
            m_df = pd.read_excel(m_file, dtype=str)
            # 컬럼명 공백 제거
            m_df.columns = [str(c).strip() for c in m_df.columns]
            
            m_list = []
            for _, row in m_df.iterrows():
                # 이미지 기준: '자재번호'(0번), '공급업체명'(5번), '분류구분'(10번)
                mat_id = sanitize_code(row['자재번호']) if '자재번호' in m_df.columns else sanitize_code(row.iloc[0])
                if not mat_id or mat_id == 'NAN': continue
                
                vendor = str(row['공급업체명']).strip() if '공급업체명' in m_df.columns else str(row.iloc[5]).strip()
                category = str(row['분류구분']).strip() if '분류구분' in m_df.columns else str(row.iloc[10]).strip()
                
                m_list.append({
                    "자재번호": mat_id,
                    "공급업체명": vendor,
                    "분류구분": category
                })
            
            if m_list:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_list), 200):
                    supabase.table("master_data").insert(m_list[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_list):,}건 등록 완료")
                st.rerun()

    st.divider()
    # [전체 초기화]
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        status = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            status.warning(f"🗑️ DB 비우는 중... (현재 {ids[-1]}번 삭제 완료)")
        st.success("데이터 초기화 완료")
        st.rerun()

# --- 4. 메인 기능 ---
st.title("📊 AS TAT 전수 관리 대시보드")
tab1, tab2, tab3 = st.tabs(["📥 AS 입고 (정밀 매칭)", "📤 AS 출고 (대량 처리)", "📈 전수 분석 리포트"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드 (57만 건 가능)", type=['xlsx'], key="main_in")
    if in_file and st.button("🚀 정밀 입고 실행"):
        # 마스터 로드 (메모리 매칭)
        m_df_local = fetch_all_data("master_data")
        m_lookup = {str(r['자재번호']): r for r in m_df_local.to_dict('records')}
        
        df = pd.read_excel(in_file, dtype=str)
        # 'A/S 철거' 포함 행 필터링
        as_in = df[df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total_in = [], len(as_in)
        p_bar = st.progress(0)
        p_text = st.empty()
        
        for i, (_, row) in enumerate(as_in.iterrows()):
            # 입고 자재번호 표준화 (D열 = index 3)
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
            
            # 800건 단위 끊어서 입력
            if len(recs) >= 800:
                try:
                    supabase.table("as_history").insert(recs).execute()
                except:
                    time.sleep(2)
                    supabase.table("as_history").insert(recs).execute()
                recs = []
                percent = (i + 1) / total_in
                p_bar.progress(percent)
                p_text.info(f"🚀 처리 중: {i+1:,} / {total_in:,} 건 ({percent*100:.1f}%)")
        
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success(f"🎊 {total_in:,}건 입고 및 매칭 완료!")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="main_out")
    if out_file and st.button("🚀 대량 출고 업데이트"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            # K열(index 10) 압축코드 추출
            out_keys = as_out.iloc[:, 10].dropna().astype(str).str.strip().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            
            p_out = st.progress(0)
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", batch).eq("상태", "출고 대기").execute()
                p_out.progress(min((i+500)/len(out_keys), 1.0))
            st.success(f"✅ {len(out_keys):,}건 출고 업데이트 완료")

with tab3:
    if st.button("📈 57만 건 전수 분석 및 리포트 생성", use_container_width=True):
        df_raw = fetch_all_data("as_history")
        if not df_raw.empty:
            df_raw['분류구분'] = df_raw['분류구분'].fillna('미등록').str.strip()
            # '수리대상' 포함 행 전수 검출
            df_rep = df_raw[df_raw['분류구분'].str.contains('수리대상', na=False)].copy()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("검출 수리대상", f"{len(df_rep):,} 건")
            c2.metric("미등록 건수", f"{len(df_raw[df_raw['분류구분'] == '미등록']):,} 건")
            c3.metric("전체 데이터", f"{len(df_raw):,} 건")
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                # TAT 계산
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                # 리포트용 엑셀 (7개 컬럼)
                df_final = df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].copy()
                df_final.columns = ['입고일', '출고일', '품목코드', '규격', '공급업체명', '압축코드', 'TAT']
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_final.to_excel(writer, index=False)
                st.download_button("📥 수리대상 상세 엑셀 다운로드", output.getvalue(), "AS_Repair_TAT_Final.xlsx", use_container_width=True)
            
            with st.expander("🔍 미등록 데이터 샘플 (자재번호 대조용)"):
                st.dataframe(df_raw[df_raw['분류구분'] == '미등록'][['자재번호', '규격']].head(100))
