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
    st.error("⚠️ Supabase 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (최종 안정화 버전)")

# [정제 함수] 모든 공백/제어문자 제거, 대문자화 및 길이 제한
def ultimate_sanitize(val, length=100):
    if pd.isna(val) or str(val).strip() == "": return ""
    s = str(val).strip().upper()
    s = "".join(s.split())
    s = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', s)
    return s[:length]

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
    raise Exception("CSV 읽기 실패")

# --- 2. 사이드바 (DB 관리 및 모니터링) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("🔍 DB 실시간 수량 확인", use_container_width=True):
        try:
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            st.metric("DB 내 데이터", f"{res.count if res.count is not None else 0:,} 건")
        except Exception as e: st.error(f"조회 실패: {e}")
    
    st.divider()
    if st.button("💣 DB 데이터 강제 초기화", type="primary", use_container_width=True):
        try:
            msg = st.empty()
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                msg.warning("🗑️ 삭제 중...")
            st.success("✅ 초기화 완료"); time.sleep(1); st.rerun()
        except Exception as e: st.error(f"삭제 오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 전량 입고", "📤 고속 출고 매칭", "📈 리포트"])

# [TAB 0] 마스터 관리 (규격명 포함)
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일(A:자재번호, G:규격명)", type=['csv', 'xlsx'], key="m_v_final")
    if m_file and st.button("🔄 마스터 데이터 로드"):
        m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
        st.session_state.master_lookup = {
            ultimate_sanitize(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(),
                "규격명": str(row.iloc[6]).strip(),
                "분류": str(row.iloc[10]).strip(),
                "AS구분": str(row.iloc[14]).strip() if len(row) > 14 else "미지정"
            } for _, row in m_df.iterrows()
        }
        st.success(f"✅ 마스터 {len(st.session_state.master_lookup):,}건 로드 완료")

# [TAB 1] 전량 입고 (API 오류 방지 로직 적용)
with tab1:
    st.subheader("📥 AS 전량 입고 (16,995건 정밀 대응)")
    c1, c2 = st.columns(2)
    with c1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_v_final")
    if i_file and st.button("🚀 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            try:
                df = smart_read_csv(i_file)
                df['clean_key'] = df.iloc[:, code_idx].apply(lambda x: ultimate_sanitize(x, 100))
                clean_df = df[df['clean_key'] != ""].drop_duplicates(subset=['clean_key'], keep='last')
                
                recs = []
                for _, row in clean_df.iterrows():
                    code = row['clean_key']
                    mat_no = ultimate_sanitize(row.iloc[mat_idx], 100)
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    recs.append({
                        "압축코드": code, "자재번호": mat_no,
                        "자재명": str(row.iloc[name_idx]).strip()[:200],
                        "규격명": m_info.get("규격명", "미등록")[:200],
                        "공급업체명": m_info.get("업체", "미등록")[:100],
                        "분류구분": m_info.get("분류", "미등록")[:100],
                        "대상여부": m_info.get("AS구분", "미등록")[:50],
                        "입고일": str(to_pure_date(row.iloc[date_idx])), "상태": "출고 대기"
                    })
                
                prog = st.progress(0); status_txt = st.empty()
                for i in range(0, len(recs), 100): # 안정성을 위해 100건씩 끊어서 전송
                    chunk = recs[i:i+100]
                    try:
                        supabase.table("as_history").upsert(chunk, on_conflict="압축코드").execute()
                    except Exception as e:
                        st.warning(f"⚠️ {i}번 데이터 부근에서 전송 오류 발생. 무시하고 진행합니다.")
                        continue
                    prog.progress(min((i+100)/len(recs), 1.0))
                    status_txt.text(f"⏳ 진행: {min(i+100, len(recs)):,} / {len(recs):,}")
                st.success(f"✅ {len(recs):,}건 입고 프로세스 완료!")
            except Exception as e: st.error(f"입고 실패: {e}")

# [TAB 2] 출고 매칭
with tab2:
    st.subheader("📤 AS 고속 출고 매칭")
    o_file = st.file_uploader("출고 CSV", type=['csv'], key="o_v_final")
    if o_file and st.button("🚀 매칭 시작"):
        try:
            df_out = smart_read_csv(o_file)
            df_out['match_key'] = df_out.iloc[:, 10].apply(lambda x: ultimate_sanitize(x, 100))
            db_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+999).execute()
                if not res.data: break
                db_data.extend(res.data); offset += 1000
            db_dict = {item['압축코드']: item['id'] for item in db_data}
            updates = []
            for _, row in df_out.iterrows():
                code = row['match_key']
                if code in db_dict:
                    dest, out_dt = str(row.iloc[15]).strip(), str(to_pure_date(row.iloc[6]))
                    updates.append({
                        "id": db_dict[code], "디지타스_출고일": out_dt if "디지타스" in dest else None,
                        "벤더_출고일": out_dt if "디지타스" not in dest else None,
                        "벤더_출고지": dest, "상태": "출고 완료"
                    })
            if updates:
                for i in range(0, len(updates), 100):
                    supabase.table("as_history").upsert(updates[i:i+100]).execute()
                st.success(f"✅ {len(updates):,}건 매칭 완료!")
        except Exception as e: st.error(f"출고 오류: {e}")

# [TAB 3] 리포트 (배열 고정 및 규격명 포함)
with tab3:
    st.subheader("📈 TAT 리포트 (정규 배열)")
    if st.button("📊 최종 리포트 다운로드", use_container_width=True):
        all_d, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").order("id").range(offset, offset+999).execute()
            if not res.data: break
            all_d.extend(res.data); offset += 1000
        if all_d:
            df = pd.DataFrame(all_d).drop_duplicates(subset=['압축코드'], keep='last')
            target_cols = ["입고일", "자재번호", "자재명", "규격명", "공급업체명", "분류구분", "대상여부", "압축코드", "디지타스_출고일", "벤더_출고일", "벤더_출고지", "상태"]
            df = df[[c for c in target_cols if c in df.columns]]
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df.to_excel(wr, index=False)
            st.download_button("📥 정규 리포트 받기", output.getvalue(), f"AS_TAT_Report_{time.strftime('%m%d')}.xlsx")
        else:
            st.error("데이터가 없습니다.")
