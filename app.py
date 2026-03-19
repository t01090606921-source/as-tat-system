import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time
import re
from datetime import datetime, timedelta

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (전체 데이터 입고 버전)")

# [데이터 정제 함수] - 공백만 제거하여 매칭률 확보
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).replace(" ", "").upper().strip()

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).date()
    except: return None

# [파일 읽기 함수] - 인코딩 오류 자동 해결
def smart_read_csv(file):
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc).fillna("")
        except UnicodeDecodeError: continue
    raise Exception("파일 인코딩을 인식할 수 없습니다.")

# --- 2. 사이드바 (DB 관리 및 분할 삭제) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 DB 상태 새로고침", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("현재 총 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if "delete_confirm" not in st.session_state: st.session_state.delete_confirm = False
    if not st.session_state.delete_confirm:
        if st.button("💣 데이터 전체 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_confirm = True; st.rerun()
    else:
        st.warning("⚠️ 모든 데이터를 삭제하시겠습니까?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정", use_container_width=True):
                try:
                    msg = st.empty()
                    while True:
                        fetch = supabase.table("as_history").select("id").limit(500).execute()
                        ids = [r['id'] for r in fetch.data]
                        if not ids: break
                        supabase.table("as_history").delete().in_("id", ids).execute()
                        msg.warning(f"🗑️ 데이터 소거 중...")
                        time.sleep(0.1)
                    st.session_state.delete_confirm = False; st.success("✅ 초기화 완료"); time.sleep(1); st.rerun()
                except Exception as e: st.error(f"삭제 오류: {e}")
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_confirm = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 무조건 입고", "📤 정밀 출고", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일", type=['csv', 'xlsx'], key="m_up")
    if m_file and st.button("🔄 마스터 로드"):
        try:
            m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {
                sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip(),
                    "AS구분": str(row.iloc[14]).strip() if len(row) > 14 else "미지정"
                } for _, row in m_df.iterrows()
            }
            st.success(f"✅ 마스터 로드 완료")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 (필터링 제거)
with tab1:
    st.subheader("📥 AS 입고 (전체 행 처리)")
    st.info("💡 H열(8번)에 'AS' 글자가 없어도 모든 데이터를 읽어옵니다.")
    
    col1, col2 = st.columns(2)
    with col1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with col2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_up")
    if i_file and st.button("🚀 16,995건 전량 입고 시작"):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            try:
                i_df = smart_read_csv(i_file)
                recs = []
                for _, row in i_df.iterrows():
                    code = sanitize_code(row.iloc[code_idx])
                    if not code: continue # 압축코드가 아예 없는 행만 제외
                    
                    mat_no = sanitize_code(row.iloc[mat_idx])
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    recs.append({
                        "압축코드": code, "자재번호": mat_no, "자재명": str(row.iloc[name_idx]).strip(),
                        "규격": str(row.iloc[5]).strip() if len(row) > 5 else "",
                        "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"), "대상여부": m_info.get("AS구분", "미등록"),
                        "입고일": str(to_pure_date(row.iloc[date_idx])), "상태": "출고 대기"
                    })
                
                status_msg = st.empty()
                for i in range(0, len(recs), 200):
                    supabase.table("as_history").upsert(recs[i:i+200], on_conflict="압축코드").execute()
                    status_msg.info(f"⏳ 진행 중: {min(i+200, len(recs))}/{len(recs)}")
                st.success(f"✅ 총 {len(recs):,}건 입고 완료!")
            except Exception as e: st.error(f"입고 오류: {e}")

# [TAB 2] 출고 (DB 실시간 매칭)
with tab2:
    st.subheader("📤 AS 출고 (실시간 매칭)")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="o_up")
    if o_file and st.button("🚀 출고 시작"):
        try:
            df_out = smart_read_csv(o_file)
            # 출고는 기존처럼 'AS카톤' 등 키워드가 있는 행만 골라냅니다.
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS|A/S|카톤', case=False)].copy()
            
            prog = st.progress(0); stat = st.empty(); success_count = 0
            for idx, row in as_out.iterrows():
                code = sanitize_code(row.iloc[10]); out_date = to_pure_date(row.iloc[6]); dest = str(row.iloc[15]).strip()
                res = supabase.table("as_history").select("*").eq("압축코드", code).neq("상태", "벤더 출고 완료").execute()
                if res.data:
                    target = res.data[0]
                    update_data = {
                        "디지타스_출고일": str(out_date) if dest == "주식회사디지타스" else target.get("디지타스_출고일"),
                        "벤더_출고일": str(out_date) if dest != "주식회사디지타스" else target.get("벤더_출고일"),
                        "벤더_출고지": dest if dest != "주식회사디지타스" else target.get("벤더_출고지"),
                        "상태": "디지타스 출고" if dest == "주식회사디지타스" else "벤더 출고 완료"
                    }
                    supabase.table("as_history").update(update_data).eq("id", target['id']).execute()
                    success_count += 1
                prog.progress((idx + 1) / len(as_out))
                stat.text(f"처리 중: {idx+1}/{len(as_out)} (성공: {success_count})")
            st.success(f"✅ 최종 {success_count}건 반영 완료!")
        except Exception as e: st.error(f"출고 오류: {e}")

# [TAB 3] 리포트
with tab3:
    st.subheader("📈 분석 리포트")
    c1, c2 = st.columns(2)
    with c1: s_d = st.date_input("시작일", datetime.now() - timedelta(days=30))
    with c2: e_d = st.date_input("종료일", datetime.now())
    if st.button("📊 리포트 생성"):
        try:
            all_d, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").gte("입고일", str(s_d)).lte("입고일", str(e_d)).range(offset, offset+1000).order("입고일").execute()
                if not res.data: break
                all_d.extend(res.data); offset += len(res.data)
                if len(res.data) < 1000: break
            if all_d:
                df = pd.DataFrame(all_d)
                in_dt, dg_dt, vn_dt = pd.to_datetime(df['입고일'], errors='coerce'), pd.to_datetime(df['디지타스_출고일'], errors='coerce'), pd.to_datetime(df['벤더_출고일'], errors='coerce')
                df['TAT'] = (vn_dt - in_dt).dt.days
                df.loc[df['TAT'].isna(), 'TAT'] = (dg_dt - in_dt).dt.days
                df['입고일'], df['디지타스_출고일'], df['벤더_출고일'] = in_dt.dt.strftime('%Y-%m-%d'), dg_dt.dt.strftime('%Y-%m-%d').fillna("-"), vn_dt.dt.strftime('%Y-%m-%d').fillna("-")
                df['TAT'], df['벤더_출고지'] = df['TAT'].fillna("-"), df['벤더_출고지'].fillna("-")
                cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                    df[cols].to_excel(wr, index=False)
                st.download_button("📥 다운로드", output.getvalue(), "AS_TAT_Report.xlsx")
                st.dataframe(df[cols].head(100))
            else: st.warning("데이터가 없습니다.")
        except Exception as e: st.error(f"리포트 오류: {e}")
