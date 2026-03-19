import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (정밀 입고 모드)")

# [강력 정제] 모든 공백 제거 및 대문자 통일 (중복 발생의 근본 원인 차단)
def strict_sanitize(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    # 모든 공백 제거 + 대문자 변환 (AS-001, as 001 등을 동일값으로 인식)
    return str(val).replace(" ", "").upper().strip()

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).date()
    except: return None

def smart_read_csv(file):
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc).fillna("")
        except: continue
    raise Exception("CSV 읽기 실패 (인코딩 확인 필요)")

# --- 2. 사이드바 (실시간 DB 감시) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 실시간 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    st.error("⚠️ 데이터 건수가 맞지 않으면 초기화 후 재입고하세요.")
    if st.button("💣 DB 데이터 전체 강제 삭제", type="primary", use_container_width=True):
        try:
            msg = st.empty()
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                msg.warning("🗑️ 데이터 소거 중...")
            st.success("✅ DB 초기화 완료 (0건 확인)"); time.sleep(1); st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 전량 입고", "📤 고속 출고 매칭", "📈 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일 업로드", type=['csv', 'xlsx'], key="master_final")
    if m_file and st.button("🔄 마스터 데이터 로드"):
        try:
            m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            # 자재번호(A열) 기준으로 업체(F열), 분류(K열), 구분(O열) 매핑
            st.session_state.master_lookup = {
                strict_sanitize(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip(),
                    "분류": str(row.iloc[10]).strip(),
                    "AS구분": str(row.iloc[14]).strip() if len(row) > 14 else "미지정"
                } for _, row in m_df.iterrows()
            }
            st.success(f"✅ 마스터 {len(st.session_state.master_lookup):,}건 로드 완료")
        except Exception as e: st.error(f"마스터 로드 실패: {e}")

# [TAB 1] 전량 입고 (16,995건 정밀 입고)
with tab1:
    st.subheader("📥 AS 전량 입고 (정합성 검수)")
    c1, c2 = st.columns(2)
    with c1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_final_safe")
    if i_file and st.button("🚀 정밀 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터를 먼저 로드해 주세요.")
        else:
            try:
                raw_df = smart_read_csv(i_file)
                # 1. 파일 내 중복 제거 (대소문자/공백 무시 기준)
                raw_df['clean_key'] = raw_df.iloc[:, code_idx].apply(strict_sanitize)
                clean_df = raw_df.drop_duplicates(subset=['clean_key'], keep='last')
                
                recs = []
                for _, row in clean_df.iterrows():
                    code = row['clean_key']
                    if not code: continue
                    
                    mat_no = strict_sanitize(row.iloc[mat_idx])
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    
                    recs.append({
                        "압축코드": code,
                        "자재번호": mat_no,
                        "자재명": str(row.iloc[name_idx]).strip(),
                        "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "미등록"),
                        "대상여부": m_info.get("AS구분", "미등록"),
                        "입고일": str(to_pure_date(row.iloc[date_idx])),
                        "상태": "출고 대기"
                    })
                
                total = len(recs)
                st.write(f"📊 검수 결과: 전체 {len(raw_df):,}행 중 유효 데이터 **{total:,}**건 확인")
                
                prog = st.progress(0)
                status = st.empty()
                for i in range(0, total, 200):
                    chunk = recs[i:i+200]
                    # upsert를 통해 동일 압축코드가 있으면 덮어쓰기하여 중복 방지
                    supabase.table("as_history").upsert(chunk, on_conflict="압축코드").execute()
                    prog.progress(min((i+200)/total, 1.0))
                    status.info(f"⏳ 전송 중: {min(i+200, total):,} / {total:,}")
                
                st.success(f"🏁 입고 완료! 현재 DB 총 건수: {total:,}건")
            except Exception as e: st.error(f"입고 실패: {e}")

# [TAB 2] 고속 출고 매칭
with tab2:
    st.subheader("📤 AS 고속 출고 매칭")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="out_final_safe")
    if o_file and st.button("🚀 매칭 시작", use_container_width=True):
        try:
            df_out = smart_read_csv(o_file)
            df_out['match_key'] = df_out.iloc[:, 10].apply(strict_sanitize)
            
            db_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+1000).execute()
                if not res.data: break
                db_data.extend(res.data); offset += 1000
            db_dict = {item['압축코드']: item['id'] for item in db_data}
            
            updates = []
            for _, row in df_out.iterrows():
                code = row['match_key']
                if code in db_dict:
                    dest = str(row.iloc[15]).strip()
                    out_dt = str(to_pure_date(row.iloc[6]))
                    updates.append({
                        "id": db_dict[code],
                        "디지타스_출고일": out_dt if "디지타스" in dest else None,
                        "벤더_출고일": out_dt if "디지타스" not in dest else None,
                        "벤더_출고지": dest,
                        "상태": "출고 완료"
                    })
            
            if updates:
                for i in range(0, len(updates), 200):
                    supabase.table("as_history").upsert(updates[i:i+200]).execute()
                st.success(f"✅ {len(updates):,}건 매칭 완료!")
        except Exception as e: st.error(f"출고 실패: {e}")

# [TAB 3] 리포트
with tab3:
    st.subheader("📈 TAT 리포트 생성")
    if st.button("📊 엑셀 다운로드", use_container_width=True):
        all_d, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").range(offset, offset+1000).order("입고일").execute()
            if not res.data: break
            all_d.extend(res.data); offset += 1000
        
        if all_d:
            df = pd.DataFrame(all_d)
            st.dataframe(df)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df.to_excel(wr, index=False)
            st.download_button("📥 파일 받기", output.getvalue(), "AS_TAT_Final.xlsx")
