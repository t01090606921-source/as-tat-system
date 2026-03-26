import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time
import re

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase Secrets 설정을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (이중 출고 완벽 대응)")

# [정밀 정제 함수] 영문, 숫자만 남김
def ultimate_sanitize(val, length=100):
    if pd.isna(val) or str(val).strip() == "": return ""
    s = str(val).strip().upper()
    s = "".join(s.split())
    s = re.sub(r'[^A-Z0-9]', '', s) 
    return s[:length]

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except: return None

def smart_read_csv(file):
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc).fillna("")
        except: continue
    raise Exception("CSV 읽기 실패")

# --- 2. 사이드바 (DB 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("🔍 DB 실시간 수량 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if st.button("💣 DB 전체 데이터 초기화", type="primary", use_container_width=True):
        with st.spinner("데이터 초기화 중..."):
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("✅ 초기화 완료"); st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 전량 입고", "📤 초정밀 출고 매칭", "📈 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일 업로드 (xlsx/csv)", type=['xlsx', 'csv'], key="m_up")
    if m_file and st.button("🔄 마스터 데이터 로드"):
        m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
        st.session_state.master_lookup = {
            ultimate_sanitize(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(),
                "규격": str(row.iloc[6]).strip(),
                "분류": str(row.iloc[10]).strip(),
                "AS구분": str(row.iloc[14]).strip() if len(row) > 14 else "미지정"
            } for _, row in m_df.iterrows()
        }
        st.success(f"✅ 마스터 {len(st.session_state.master_lookup):,}건 로드 완료")

# [TAB 1] 전량 입고
with tab1:
    st.subheader("📥 AS 전량 입고 (이력 보존)")
    c1, c2 = st.columns(2)
    with c1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_up")
    if i_file and st.button("🚀 입고 시작"):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터를 먼저 로드해 주세요.")
        else:
            df = smart_read_csv(i_file)
            recs = []
            for _, row in df.iterrows():
                code = ultimate_sanitize(row.iloc[code_idx])
                mat_no = ultimate_sanitize(row.iloc[mat_idx])
                if not code: continue
                m_info = st.session_state.master_lookup.get(mat_no, {})
                recs.append({
                    "압축코드": code, "입고일": to_pure_date(row.iloc[date_idx]), "자재번호": mat_no,
                    "자재명": str(row.iloc[name_idx]).strip()[:200],
                    "규격": m_info.get("규격", "미등록"), "공급업체명": m_info.get("업체", "미등록"),
                    "분류구분": m_info.get("분류", "미등록"), "대상여부": m_info.get("AS구분", "미등록"),
                    "상태": "출고 대기"
                })
            for j in range(0, len(recs), 100):
                supabase.table("as_history").insert(recs[j:j+100]).execute()
            st.success(f"✅ {len(recs):,}건 입고 완료")

# [TAB 2] 출고 매칭 (이중 출고 대응 로직 복구)
with tab2:
    st.subheader("📤 초정밀 출고 매칭 (디지타스 & 벤더 이중 출고 대응)")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="o_up")
    if o_file and st.button("🚀 매칭 시작"):
        df_out = smart_read_csv(o_file)
        df_out['m_key'] = df_out.iloc[:, 10].apply(ultimate_sanitize)
        df_out['o_date'] = df_out.iloc[:, 6].apply(to_pure_date)
        
        with st.spinner("DB 분석 및 매칭 중..."):
            # 전체 DB 로드 (현재 상태를 정확히 파악하기 위함)
            db_res, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset+999).execute()
                if not res.data: break
                db_res.extend(res.data); offset += 1000
            db_df = pd.DataFrame(db_res).sort_values(by='입고일')
            
            updates = []
            for _, o_row in df_out.iterrows():
                code = o_row['m_key']
                out_dt = o_row['o_date']
                dest = str(o_row.iloc[15]).strip()
                
                # 핵심: 해당 코드의 입고 건 중 출고일보다 빠른 것들 필터링
                hits = db_df[(db_df['압축코드'] == code) & (db_df['입고일'] <= out_dt)]
                
                if not hits.empty:
                    # [로직 개선] 
                    # 1. 이미 벤더 출고일이 찍힌 건은 제외 (중복 처리 방지)
                    # 2. 디지타스 출고라면 -> 디지타스_출고일이 없는 가장 빠른 건 매칭
                    # 3. 벤더 출고라면 -> 벤더_출고일이 없는 가장 빠른 건 매칭
                    if "디지타스" in dest:
                        valid_hits = hits[hits['디지타스_출고일'].isna()]
                    else:
                        valid_hits = hits[hits['벤더_출고일'].isna()]
                    
                    if not valid_hits.empty:
                        target_idx = valid_hits.index[0] # 가장 먼저 들어온 건(FIFO)
                        target_id = db_df.loc[target_idx, 'id']
                        
                        upd = {"id": int(target_id), "상태": "출고 완료"}
                        if "디지타스" in dest:
                            upd["디지타스_출고일"] = out_dt
                        else:
                            upd["벤더_출고일"] = out_dt
                            upd["벤더_출고지"] = dest
                        
                        updates.append(upd)
                        # 메모리상 업데이트 반영 (다음 출고 행 처리 시 참조)
                        if "디지타스" in dest:
                            db_df.at[target_idx, '디지타스_출고일'] = out_dt
                        else:
                            db_df.at[target_idx, '벤더_출고일'] = out_dt

            if updates:
                for k in range(0, len(updates), 50):
                    supabase.table("as_history").upsert(updates[k:k+50]).execute()
                    time.sleep(0.01)
                st.success(f"✅ {len(updates):,}건 매칭 완료 (이중 출고 업데이트 포함)")

# [TAB 3] 리포트
with tab3:
    if st.button("📊 최종 리포트 다운로드"):
        all_d, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").order("입고일").range(offset, offset+999).execute()
            if not res.data: break
            all_d.extend(res.data); offset += 1000
        df = pd.DataFrame(all_d)
        cols = ["입고일", "자재번호", "자재명", "규격", "공급업체명", "압축코드", "디지타스_출고일", "벤더_출고일", "벤더_출고지", "상태"]
        df = df[[c for c in cols if c in df.columns]]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
            df.to_excel(wr, index=False)
        st.download_button("📥 엑셀 받기", output.getvalue(), "AS_Total_Report.xlsx")
