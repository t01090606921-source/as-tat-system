import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
from datetime import datetime

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (최종 확정본)")

# [데이터 정제 함수]
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].replace(" ", "").strip().upper()

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
                    msg.warning("🗑️ 삭제 진행 중...")
                st.session_state.delete_mode = False; st.success("삭제 완료"); st.rerun()
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일(XLSX, CSV)", type=['xlsx', 'csv'], key="m_v_final")
    if m_file and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            # 마스터: A(0):코드, F(5):공급업체명, K(10):분류구분
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(),
                "분류": str(row.iloc[10]).strip()
            } for _, row in m_df.iterrows()}
            st.success(f"✅ 마스터 로드 완료: {len(st.session_state.master_lookup):,}건")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리 (E:자재명, F:규격 사용)
with tab1:
    st.subheader("📥 AS 입고")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_v_final")
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue
                as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs = []
                for i, (_, row) in enumerate(as_in.iterrows()):
                    mat_no = sanitize_code(row.iloc[3])
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    recs.append({
                        "압축코드": sanitize_code(row.iloc[7]),
                        "자재번호": mat_no,
                        "자재명": str(row.iloc[4]).strip(),   # E(4): 자재명
                        "규격": str(row.iloc[5]).strip(),     # F(5): 규격
                        "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"),
                        "입고일": str(to_pure_date(row.iloc[1])),
                        "상태": "출고 대기"
                    })
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []; ui_prog.progress((i+1)/len(as_in))
                if recs: supabase.table("as_history").insert(recs).execute()
                ui_msg.success("✅ 입고 완료")
                ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (P열 출고지에 따른 분기 로직)
with tab2:
    st.subheader("📤 AS 출고 처리")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_v_final")
    if o_file and st.button("🚀 출고 데이터 반영", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            if len(as_out) == 0: st.error("❌ 'AS 카톤 박스' 행 없음.")
            else:
                # 벤더 출고 완료가 아닌 건들(입고상태 또는 디지타스 출고상태) 조회
                db_res = supabase.table("as_history").select("*").neq("상태", "벤더 출고 완료").order("입고일").execute()
                db_lookup = {}
                for r in db_res.data:
                    c = sanitize_code(r['압축코드'])
                    if c not in db_lookup: db_lookup[c] = []
                    db_lookup[c].append(r)
                
                count = 0
                for i, (_, row) in enumerate(as_out.iterrows()):
                    code = sanitize_code(row.iloc[10])      # K(10): 압축코드
                    out_date = to_pure_date(row.iloc[6])    # G(6): 출고일자
                    dest = str(row.iloc[15]).strip()        # P(15): 출고지
                    
                    if code in db_lookup and db_lookup[code]:
                        for idx, db_r in enumerate(db_lookup[code]):
                            if to_pure_date(db_r['입고일']) <= out_date:
                                # 출고지가 디지타스인 경우와 아닌 경우 분기 저장
                                if dest == "주식회사디지타스":
                                    upd = {"디지타스_출고일": str(out_date), "상태": "디지타스 출고"}
                                else:
                                    upd = {"벤더_출고지": dest, "벤더_출고일": str(out_date), "상태": "벤더 출고 완료"}
                                
                                supabase.table("as_history").update(upd).eq("id", db_r['id']).execute()
                                db_lookup[code].pop(idx)
                                count += 1
                                break
                    ui_prog.progress(min((i+1)/len(as_out), 1.0))
                ui_msg.success(f"✅ {count:,}건 업데이트 완료")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트 생성 (최종 컬럼 구조 및 TAT 계산)
with tab3:
    st.subheader("📈 AS TAT 분석 리포트")
    
    def fetch_all():
        data, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").range(offset, offset+999).execute()
            if not res.data: break
            data.extend(res.data); offset += len(res.data)
            if len(res.data) < 1000: break
        return pd.DataFrame(data)

    def prepare_excel(df):
        if df.empty: return None
        # 날짜 타입 변환
        in_d = pd.to_datetime(df['입고일'], errors='coerce')
        dg_d = pd.to_datetime(df['디지타스_출고일'], errors='coerce')
        vn_d = pd.to_datetime(df['벤더_출고일'], errors='coerce')
        
        # TAT 계산 로직 (벤더 출고일 우선, 없으면 디지타스 기준)
        df['TAT'] = (vn_d - in_d).dt.days
        df.loc[df['TAT'].isna(), 'TAT'] = (dg_d - in_d).dt.days
        
        # 출력 포맷팅
        df['입고일'] = in_d.dt.strftime('%Y-%m-%d')
        df['디지타스_출고일'] = dg_d.dt.strftime('%Y-%m-%d').fillna("-")
        df['벤더_출고일'] = vn_d.dt.strftime('%Y-%m-%d').fillna("-")
        df['TAT'] = df['TAT'].fillna("-")
        df['벤더_출고지'] = df['벤더_출고지'].fillna("-").replace("", "-")
        
        # 요청하신 최종 컬럼 배열
        cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', 
                '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
            df[cols].to_excel(wr, index=False)
        return output.getvalue()

    if st.button("📊 전체 리포트 생성 및 다운로드", use_container_width=True):
        raw_df = fetch_all()
        excel_bin = prepare_excel(raw_df)
        if excel_bin:
            st.download_button("📥 엑셀 다운로드", excel_bin, f"AS_Report_{datetime.now().strftime('%Y%m%d')}.xlsx")
            st.dataframe(raw_df)
