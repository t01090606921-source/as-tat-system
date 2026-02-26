import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time
from datetime import datetime

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# [데이터 정제] 모든 공백 제거, 소수점 제거, 대문자화
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].replace(" ", "").strip().upper()

# [날짜 정제] 비교 연산을 위해 순수 date 객체로 변환
def to_pure_date(val):
    try: return pd.to_datetime(val).date()
    except: return None

# --- 2. 사이드바 (DB 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("저장된 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if "delete_mode" not in st.session_state: st.session_state.delete_mode = False
    if not st.session_state.delete_mode:
        if st.button("💣 DB 전체 데이터 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True; st.rerun()
    else:
        st.error("⚠️ 데이터를 전부 삭제하시겠습니까?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정", use_container_width=True):
                msg = st.empty()
                while True:
                    fetch = supabase.table("as_history").select("id").limit(1000).execute()
                    ids = [r['id'] for r in fetch.data]
                    if not ids: break
                    supabase.table("as_history").delete().in_("id", ids).execute()
                    msg.warning(f"🗑️ 삭제 중... ({len(ids)}건씩 처리)")
                st.session_state.delete_mode = False; st.success("삭제 완료"); time.sleep(1); st.rerun()
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리 (규격 정보 수집 추가)
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일(XLSX, CSV)", type=['xlsx', 'csv'], key="m_final")
    if m_file and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            # index 설명: 0(코드), 3(자재번호), 5(업체), 6(규격), 10(분류)
            # 파일 양식에 따라 iloc 인덱스를 조정하세요.
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(),
                "분류": str(row.iloc[10]).strip(),
                "규격": str(row.iloc[6]).strip() 
            } for _, row in m_df.iterrows()}
            st.success(f"✅ 마스터 로드 완료: {len(st.session_state.master_lookup):,}건 (규격 포함)")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리 (DB 저장 시 규격 반영)
with tab1:
    st.subheader("📥 AS 입고")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_final")
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터 데이터를 먼저 로드하세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue
                
                as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    # 마스터에서 규격, 업체, 분류 가져오기
                    m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[3]), {})
                    recs.append({
                        "압축코드": sanitize_code(row.iloc[7]),
                        "자재번호": sanitize_code(row.iloc[3]),
                        "자재명": str(row.iloc[4]).strip(),
                        "규격": m_info.get("규격", "-"),
                        "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"),
                        "입고일": str(to_pure_date(row.iloc[1])),
                        "상태": "출고 대기"
                    })
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []; ui_prog.progress((i+1)/len(as_in))
                if recs: supabase.table("as_history").insert(recs).execute()
                ui_msg.success(f"✅ 입고 완료 (규격 정보 매칭 성공)")
                ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (선입선출 FIFO 매칭)
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영 (1:1 FIFO)")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_final")
    if o_file and st.button("🚀 출고 데이터 반영", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            if len(as_out) == 0: st.error("❌ 'AS 카톤 박스' 행을 찾지 못했습니다.")
            else:
                db_res = supabase.table("as_history").select("id, 압축코드, 입고일, 상태").eq("상태", "출고 대기").order("입고일").execute()
                db_lookup = {}
                for r in db_res.data:
                    c = sanitize_code(r['압축코드'])
                    if c not in db_lookup: db_lookup[c] = []
                    db_lookup[c].append(r)
                
                upd_list = []
                for i, (_, row) in enumerate(as_out.iterrows()):
                    code = sanitize_code(row.iloc[10])
                    out_date = to_pure_date(row.iloc[6])
                    if code in db_lookup and db_lookup[code]:
                        for idx, db_r in enumerate(db_lookup[code]):
                            if to_pure_date(db_r['입고일']) <= out_date:
                                upd_list.append({"id": db_r['id'], "출고일": str(out_date)})
                                db_lookup[code].pop(idx); break
                    if i % 100 == 0: ui_prog.progress(min((i+1)/len(as_out), 1.0))
                
                if upd_list:
                    for item in upd_list:
                        supabase.table("as_history").update({"출고일": item['출고일'], "상태": "출고 완료"}).eq("id", item['id']).execute()
                    ui_msg.success(f"✅ {len(upd_list):,}건 출고 반영 성공")
                else: st.warning("⚠️ 매칭된 데이터가 없습니다.")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트 생성 (정렬 및 필터링)
with tab3:
    st.subheader("📈 분석 리포트 생성")
    
    def fetch_data():
        all_d, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").range(offset, offset+999).execute()
            if not res.data: break
            all_d.extend(res.data); offset += len(res.data)
            if len(res.data) < 1000: break
        return pd.DataFrame(all_d)

    def to_excel(df):
        if df.empty: return None
        df['입고일'] = pd.to_datetime(df['입고일'])
        df['출고일'] = pd.to_datetime(df['출고일'])
        df['TAT'] = (df['출고일'] - df['입고일']).dt.days
        df['입고일'] = df['입고일'].dt.strftime('%Y-%m-%d')
        df['출고일'] = df['출고일'].dt.strftime('%Y-%m-%d').fillna("-")
        df['TAT'] = df['TAT'].fillna("-")
        # 컬럼 순서 고정 (규격 포함)
        cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '분류구분', '출고일', 'TAT', '상태']
        for c in cols: 
            if c not in df.columns: df[c] = "-"
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
            df[cols].to_excel(wr, index=False)
        return output.getvalue()

    c1, c2, c3 = st.columns(3)
    if c1.button("📊 1. 전체 리포트", use_container_width=True):
        st.session_state.r1 = to_excel(fetch_data())
    if c2.button("✅ 2. TAT 매칭건", use_container_width=True):
        df = fetch_data()
        st.session_state.r2 = to_excel(df[df['상태'] == '출고 완료'])
    if c3.button("⚠️ 3. 미등록/재입고", use_container_width=True):
        df = fetch_data()
        st.session_state.r3 = to_excel(df[df['상태'] != '출고 완료'])

    st.divider()
    d1, d2, d3 = st.columns(3)
    if "r1" in st.session_state: d1.download_button("📥 전체 다운로드", st.session_state.r1, "01_전체리포트.xlsx", use_container_width=True)
    if "r2" in st.session_state: d2.download_button("📥 매칭건 다운로드", st.session_state.r2, "02_매칭리포트.xlsx", use_container_width=True)
    if "r3" in st.session_state: d3.download_button("📥 미등록 다운로드", st.session_state.r3, "03_미등록리포트.xlsx", use_container_width=True)
