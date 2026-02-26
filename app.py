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
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# 자재번호 정제 함수
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (DB 제어 및 전체 초기화) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    
    # DB 현황 확인
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        try:
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            count = res.count if res.count is not None else 0
            st.metric("저장된 데이터", f"{count:,} 건")
        except Exception as e:
            st.error(f"조회 실패: {e}")
    
    st.divider()
    
    st.subheader("🚨 데이터 초기화")
    # 삭제 확인 세션 상태 관리
    if "delete_mode" not in st.session_state:
        st.session_state.delete_mode = False

    if not st.session_state.delete_mode:
        if st.button("💣 DB 전체 데이터 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True
            st.rerun()
    else:
        st.error("⚠️ 모든 데이터를 삭제하시겠습니까?")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ 확정(삭제)", use_container_width=True):
                status = st.empty()
                prog = st.progress(0)
                try:
                    # 1. 모든 ID 가져오기
                    res = supabase.table("as_history").select("id").execute()
                    ids = [r['id'] for r in res.data]
                    total = len(ids)
                    
                    if total > 0:
                        batch_size = 400
                        for i in range(0, total, batch_size):
                            batch = ids[i:i + batch_size]
                            supabase.table("as_history").delete().in_("id", batch).execute()
                            
                            percent = min((i + batch_size) / total, 1.0)
                            status.warning(f"🗑️ 삭제 중... ({min(i + batch_size, total):,} / {total:,})")
                            prog.progress(percent)
                        
                        status.success(f"✨ {total:,}건 삭제 완료!")
                        time.sleep(1.5)
                        st.session_state.delete_mode = False
                        st.rerun()
                    else:
                        status.info("삭제할 데이터가 없습니다.")
                        st.session_state.delete_mode = False
                except Exception as e:
                    st.error(f"❌ 삭제 실패: {e}")
                    st.session_state.delete_mode = False
        with col2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False
                st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 관리 ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일을 선택하세요 (XLSX, CSV)", type=['xlsx', 'csv'], key="master_v15")
    
    if m_file:
        if st.button("🔄 마스터 데이터 로드", use_container_width=True):
            try:
                msg = st.empty()
                msg.info("⌛ 로드 중...")
                m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
                
                st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                msg.success(f"✅ 완료: {len(st.session_state.master_lookup):,}건")
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 1] 입고 처리 ---
with tab1:
    st.subheader("📥 AS 입고 (중복 체크)")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_v15")
    if i_file and st.button("🚀 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                # DB 중복 체크용 로드
                existing = set()
                offset = 0
                while True:
                    res = supabase.table("as_history").select("입고일, 압축코드").range(offset, offset + 4000).execute()
                    if not res.data: break
                    for r in res.data:
                        existing.add(f"{pd.to_datetime(r['입고일']).strftime('%Y-%m-%d')}|{str(r['압축코드']).upper()}")
                    offset += len(res.data)
                    ui_msg.info(f"🔍 DB 대조 중... ({offset:,}건)")
                    if len(res.data) < 4000: break

                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue

                as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.contains("A/S철거|AS철거")].copy()
                recs, dup, total = [], 0, len(as_in)

                for i, (_, row) in enumerate(as_in.iterrows()):
                    in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                    code = str(row.iloc[7]).strip().upper()
                    if f"{in_date}|{code}" in existing:
                        dup += 1; continue
                    
                    m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[3]), {})
                    recs.append({
                        "압축코드": code, "자재번호": sanitize_code(row.iloc[3]), "자재명": str(row.iloc[4]).strip(),
                        "공급업체명": m_info.get("업체", "미등록"), "분류구분": m_info.get("분류", "수리대상"),
                        "입고일": in_date, "상태": "출고 대기"
                    })
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []; ui_msg.warning(f"🚀 저장 중... ({i+1:,}/{total})"); ui_prog.progress((i+1)/total)
                
                if recs: supabase.table("as_history").insert(recs).execute()
                ui_msg.success(f"✅ 신규 저장: {total-dup:,}건"); ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 2] 출고 처리 ---
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_v15")
    if o_file and st.button("🚀 출고 반영 시작", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스')].copy()
            
            ui_msg.info("🔍 DB 로드 중...")
            db_res = supabase.table("as_history").select("id, 압축코드, 입고일").execute()
            db_lookup = {}
            for r in db_res.data:
                c = str(r['압축코드']).upper()
                if c not in db_lookup: db_lookup[c] = []
                db_lookup[c].append(r)
            
            upd_list, total_o = [], len(as_out)
            for i, (_, row) in enumerate(as_out.iterrows()):
                code = str(row.iloc[10]).upper()
                out_date = pd.to_datetime(row.iloc[6]).strftime('%Y-%m-%d')
                for db_r in db_lookup.get(code, []):
                    if db_r['입고일'] <= out_date:
                        upd_list.append({"id": db_r['id'], "출고일": out_date})
                if i % 100 == 0:
                    ui_msg.info(f"🧪 검증 중... ({i+1:,}/{total_o})"); ui_prog.progress((i+1)/total_o)

            for idx, item in enumerate(upd_list):
                supabase.table("as_history").update({"출고일": item['출고일'], "상태": "출고 완료"}).eq("id", item['id']).execute()
                if idx % 50 == 0:
                    ui_msg.warning(f"🔄 DB 반영 중... ({idx:,}/{len(upd_list)})")
            ui_msg.success(f"✅ {len(upd_list):,}건 반영 완료"); ui_prog.progress(1.0)
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 3] 리포트 ---
with tab3:
    if st.button("📊 리포트 생성", use_container_width=True):
        ui_msg = st.empty()
        try:
            data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset + 999).execute()
                if not res.data: break
                data.extend(res.data); offset += len(res.data)
                ui_msg.info(f"📥 수집 중... ({offset:,}건)")
                if len(res.data) < 1000: break
            df = pd.DataFrame(data)
            df['tat'] = (pd.to_datetime(df['출고일']) - pd.to_datetime(df['입고일'])).dt.days
            
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='xlsxwriter') as wr: df.to_excel(wr, index=False)
            st.session_state.report = out.getvalue(); ui_msg.success("✅ 생성 완료")
        except Exception as e: st.error(f"오류: {e}")

    if "report" in st.session_state:
        st.download_button("📥 리포트 다운로드", st.session_state.report, "AS_TAT_Report.xlsx", use_container_width=True)
