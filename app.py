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

# [핵심] 데이터 정제 함수: 모든 공백 제거, 소수점 제거, 대문자화
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    # 숫자로 인식되어 소수점이 붙는 경우(.0) 제거 후 공백까지 완전히 박멸
    return str(val).split('.')[0].replace(" ", "").strip().upper()

# --- 2. 사이드바 (DB 현황 및 무제한 삭제) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        try:
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            count = res.count if res.count is not None else 0
            st.metric("저장된 데이터", f"{count:,} 건")
        except Exception as e: st.error(f"조회 실패: {e}")
    
    st.divider()
    st.subheader("🚨 데이터 초기화")
    if "delete_mode" not in st.session_state: st.session_state.delete_mode = False

    if not st.session_state.delete_mode:
        if st.button("💣 DB 전체 데이터 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True
            st.rerun()
    else:
        st.error("⚠️ 모든 데이터를 삭제하시겠습니까?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정(0건까지)", use_container_width=True):
                msg, bar = st.empty(), st.progress(0)
                try:
                    res_cnt = supabase.table("as_history").select("id", count="exact").limit(1).execute()
                    total = res_cnt.count if res_cnt.count else 0
                    deleted = 0
                    while True:
                        fetch = supabase.table("as_history").select("id").limit(1000).execute()
                        ids = [r['id'] for r in fetch.data]
                        if not ids: break
                        supabase.table("as_history").delete().in_("id", ids).execute()
                        deleted += len(ids)
                        msg.warning(f"🗑️ 삭제 중... ({deleted:,} / {total:,})")
                        bar.progress(min(deleted/total, 1.0) if total > 0 else 1.0)
                    msg.success("✨ 전체 삭제 완료!"); time.sleep(1); st.session_state.delete_mode = False; st.rerun()
                except Exception as e: st.error(f"실패: {e}"); st.session_state.delete_mode = False
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일 선택", type=['xlsx', 'csv'], key="m_v_final")
    if m_file and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()
            } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
            st.success(f"✅ 마스터 로드 완료: {len(st.session_state.master_lookup):,}건")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리 (강력 필터링)
with tab1:
    st.subheader("📥 AS 입고")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_v_final")
    if i_file and st.button("🚀 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터 데이터를 먼저 로드해주세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                # DB 중복 체크용 데이터 로드
                existing = set()
                offset = 0
                while True:
                    res = supabase.table("as_history").select("입고일, 압축코드").range(offset, offset+4000).execute()
                    if not res.data: break
                    for r in res.data:
                        existing.add(f"{pd.to_datetime(r['입고일']).strftime('%Y-%m-%d')}|{sanitize_code(r['압축코드'])}")
                    offset += len(res.data)
                    ui_msg.info(f"🔍 DB 중복 체크 데이터 수집 중... ({offset:,}건)")
                    if len(res.data) < 4000: break

                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue
                
                # [강화 필터] 공백 무시 검색
                combined_str = i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "")
                as_in = i_df[combined_str.str.contains("A/S철거|AS철거", na=False)].copy()
                
                if len(as_in) == 0:
                    st.warning("⚠️ 'AS철거' 문구를 찾지 못했습니다. 파일 내용을 확인하세요.")
                else:
                    recs, dup_cnt = [], 0
                    for i, (_, row) in enumerate(as_in.iterrows()):
                        in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        code = sanitize_code(row.iloc[7])
                        if f"{in_date}|{code}" in existing:
                            dup_cnt += 1; continue
                        
                        m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[3]), {})
                        recs.append({
                            "압축코드": code, "자재번호": sanitize_code(row.iloc[3]), "자재명": str(row.iloc[4]).strip(),
                            "공급업체명": m_info.get("업체", "미등록"), "분류구분": m_info.get("분류", "수리대상"),
                            "입고일": in_date, "상태": "출고 대기"
                        })
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []; ui_msg.warning(f"🚀 저장 중... ({i+1:,}/{len(as_in)})"); ui_prog.progress((i+1)/len(as_in))
                    
                    if recs: supabase.table("as_history").insert(recs).execute()
                    ui_msg.success(f"✅ 입고 완료 (신규: {len(as_in)-dup_cnt:,}건)"); ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (정밀 매칭 엔진)
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_v_final")
    if o_file and st.button("🚀 출고 반영 시작", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(o_file).fillna("")
            # 'AS 카톤 박스' 필터 (공백 제거 후 비교)
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            if len(as_out) == 0:
                st.error("❌ 'AS 카톤 박스' 데이터를 찾지 못했습니다. 4번째 열을 확인하세요.")
            else:
                ui_msg.info("🔍 DB 데이터 로드 및 해시맵 생성 중...")
                db_res = supabase.table("as_history").select("id, 압축코드, 입고일").execute()
                db_lookup = {}
                for r in db_res.data:
                    c = sanitize_code(r['압축코드'])
                    if c not in db_lookup: db_lookup[c] = []
                    db_lookup[c].append(r)
                
                upd_list = []
                for i, (_, row) in enumerate(as_out.iterrows()):
                    code = sanitize_code(row.iloc[10])
                    try: out_date = pd.to_datetime(row.iloc[6]).strftime('%Y-%m-%d')
                    except: continue

                    if code in db_lookup:
                        for db_r in db_lookup[code]:
                            if str(db_r['입고일']) <= out_date:
                                upd_list.append({"id": db_r['id'], "출고일": out_date})
                    if i % 100 == 0: ui_prog.progress(min((i+1)/len(as_out), 1.0))
                
                if upd_list:
                    # 중복 ID 제거 (동일 품목 중복 업데이트 방지)
                    final_upds = {item['id']: item['출고일'] for item in upd_list}
                    for idx, (tid, tdate) in enumerate(final_upds.items()):
                        supabase.table("as_history").update({"출고일": tdate, "상태": "출고 완료"}).eq("id", tid).execute()
                        if idx % 50 == 0: ui_msg.warning(f"🔄 반영 중... ({idx:,}/{len(final_upds)})")
                    ui_msg.success(f"✅ {len(final_upds):,}건 업데이트 성공!")
                else:
                    st.error("❌ 일치하는 데이터가 없습니다. (압축코드 불일치 또는 입고일 오류)")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트
with tab3:
    if st.button("📊 최종 리포트 생성", use_container_width=True):
        ui_msg = st.empty()
        try:
            data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset+999).execute()
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
        st.download_button("📥 다운로드", st.session_state.report, "AS_TAT_Report.xlsx")
