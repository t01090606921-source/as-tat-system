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
st.title("📊 AS TAT 통합 관리 시스템 (고속 & 무결성)")

# [공통 정제 함수]
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    # 공백 제거 및 대문자 통일 (매칭 정확도 핵심)
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
        except UnicodeDecodeError: continue
    raise Exception("파일 인코딩을 인식할 수 없습니다.")

# --- 2. 사이드바 (DB 관리) ---
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
                        fetch = supabase.table("as_history").select("id").limit(1000).execute()
                        ids = [r['id'] for r in fetch.data]
                        if not ids: break
                        supabase.table("as_history").delete().in_("id", ids).execute()
                        msg.warning("🗑️ 데이터 소거 중...")
                    st.session_state.delete_confirm = False; st.success("초기화 완료"); st.rerun()
                except Exception as e: st.error(f"오류: {e}")
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_confirm = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 전량 입고", "📤 고속 출고 매칭", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일", type=['csv', 'xlsx'], key="m_up")
    if m_file and st.button("🔄 마스터 로드"):
        m_df = smart_read_csv(m_file) if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
        st.session_state.master_lookup = {
            sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip(),
                "AS구분": str(row.iloc[14]).strip() if len(row) > 14 else "미지정"
            } for _, row in m_df.iterrows()
        }
        st.success(f"✅ 마스터 {len(st.session_state.master_lookup):,}건 로드 완료")

# [TAB 1] 전량 입고 (필터링 제거 & 중복 차단)
with tab1:
    st.subheader("📥 AS 전량 입고")
    c1, c2 = st.columns(2)
    with c1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV", type=['csv'], key="i_up")
    if i_file and st.button("🚀 전량 입고 시작"):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            i_df = smart_read_csv(i_file)
            # 파일 내 중복 제거 (압축코드 기준 마지막 행 유지)
            i_df['clean_code'] = i_df.iloc[:, code_idx].apply(sanitize_code)
            i_df = i_df.drop_duplicates(subset=['clean_code'], keep='last')
            
            recs = []
            for _, row in i_df.iterrows():
                code = row['clean_code']
                if not code: continue
                m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[mat_idx]), {})
                recs.append({
                    "압축코드": code, "자재번호": sanitize_code(row.iloc[mat_idx]),
                    "자재명": str(row.iloc[name_idx]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                    "분류구분": m_info.get("분류", "수리대상"), "대상여부": m_info.get("AS구분", "미등록"),
                    "입고일": str(to_pure_date(row.iloc[date_idx])), "상태": "출고 대기"
                })
            
            for i in range(0, len(recs), 500):
                supabase.table("as_history").upsert(recs[i:i+500], on_conflict="압축코드").execute()
            st.success(f"✅ {len(recs):,}건 입고/갱신 완료")

# [TAB 2] 고속 출고 & 누락 확인
with tab2:
    st.subheader("📤 AS 고속 출고 매칭")
    o_file = st.file_uploader("출고 CSV", type=['csv'], key="o_up")
    
    if o_file and st.button("🚀 전량 출고 매칭 시작"):
        try:
            df_out = smart_read_csv(o_file)
            # 출고는 'AS/A/S/카톤' 키워드 행만 추출 (사용자 요청 시 이 필터도 제거 가능)
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS|A/S|카톤', case=False)].copy()
            as_out['clean_code'] = as_out.iloc[:, 10].apply(sanitize_code)
            # 출고 파일 내 중복 제거
            as_out = as_out.drop_duplicates(subset=['clean_code'], keep='last')
            
            # 1. DB 전체 데이터를 메모리로 로드 (속도 최적화)
            st.info("🔄 DB에서 입고 데이터를 불러오는 중... (잠시만 기다려주세요)")
            all_db, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+1000).execute()
                if not res.data: break
                all_db.extend(res.data)
                offset += 1000
                if len(res.data) < 1000: break
            
            db_dict = {item['압축코드']: item['id'] for item in all_db}
            
            # 2. 메모리 상에서 매칭
            updates, missing = [], []
            for _, row in as_out.iterrows():
                code = row['clean_code']
                out_date = to_pure_date(row.iloc[6])
                dest = str(row.iloc[15]).strip()
                
                if code in db_dict:
                    updates.append({
                        "id": db_dict[code],
                        "디지타스_출고일": str(out_date) if dest == "주식회사디지타스" else None,
                        "벤더_출고일": str(out_date) if dest != "주식회사디지타스" else None,
                        "벤더_출고지": dest if dest != "주식회사디지타스" else None,
                        "상태": "디지타스 출고" if dest == "주식회사디지타스" else "벤더 출고 완료"
                    })
                else:
                    missing.append({"압축코드": code, "출고지": dest, "출고일": out_date})

            # 3. DB 일괄 업데이트 (200건씩 묶어서 전송)
            if updates:
                prog = st.progress(0); stat = st.empty()
                for i in range(0, len(updates), 200):
                    supabase.table("as_history").upsert(updates[i:i+200]).execute()
                    prog.progress(min((i + 200) / len(updates), 1.0))
                    stat.text(f"DB 반영 중... {min(i+200, len(updates))}/{len(updates)}")
                st.success(f"✅ {len(updates):,}건 업데이트 완료!")
            
            if missing:
                st.warning(f"⚠️ 매칭되지 않은 출고 데이터(입고 내역 없음): {len(missing)}건")
                st.dataframe(pd.DataFrame(missing))
                
        except Exception as e: st.error(f"출고 오류: {e}")

# [TAB 3] 리포트 생성 (중복 제거 최종 확인)
with tab3:
    st.subheader("📈 분석 리포트")
    if st.button("📊 최종 리포트 생성"):
        all_d, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").range(offset, offset+1000).order("입고일").execute()
            if not res.data: break
            all_d.extend(res.data)
            offset += 1000
        
        if all_d:
            df = pd.DataFrame(all_d)
            # 리포트 출력 전 압축코드 기준 최종 중복 제거 (방어막)
            df = df.drop_duplicates(subset=['압축코드'], keep='last')
            
            in_dt, dg_dt, vn_dt = pd.to_datetime(df['입고일'], errors='coerce'), pd.to_datetime(df['디지타스_출고일'], errors='coerce'), pd.to_datetime(df['벤더_출고일'], errors='coerce')
            df['TAT'] = (vn_dt - in_dt).dt.days
            df.loc[df['TAT'].isna(), 'TAT'] = (dg_dt - in_dt).dt.days
            
            df['입고일'] = in_dt.dt.strftime('%Y-%m-%d')
            df['디지타스_출고일'] = dg_dt.dt.strftime('%Y-%m-%d').fillna("-")
            df['벤더_출고일'] = vn_dt.dt.strftime('%Y-%m-%d').fillna("-")
            df['TAT'] = df['TAT'].fillna("-")
            
            cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
            st.dataframe(df[cols])
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df[cols].to_excel(wr, index=False)
            st.download_button("📥 엑셀 다운로드", output.getvalue(), "AS_Final_TAT_Report.xlsx")
