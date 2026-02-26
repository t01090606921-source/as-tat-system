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

# [날짜 정제] 순수 date 객체로 변환 (비교 연산용)
def to_pure_date(val):
    try:
        return pd.to_datetime(val).date()
    except:
        return None

# --- 2. 사이드바 (DB 제어 및 무제한 삭제) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        try:
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            count = res.count if res.count is not None else 0
            st.metric("저장된 데이터 수량", f"{count:,} 건")
        except Exception as e: st.error(f"조회 실패: {e}")
    
    st.divider()
    st.subheader("🚨 데이터 초기화")
    if "delete_mode" not in st.session_state: st.session_state.delete_mode = False

    if not st.session_state.delete_mode:
        if st.button("💣 DB 전체 데이터 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True
            st.rerun()
    else:
        st.error("⚠️ [경고] 0건이 될 때까지 모든 데이터를 삭제합니까?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정 및 삭제", use_container_width=True):
                status_msg = st.empty()
                prog_bar = st.progress(0)
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
                        status_msg.warning(f"🗑️ 삭제 진행 중... ({deleted:,} / {total:,})")
                        prog_bar.progress(min(deleted/total, 1.0) if total > 0 else 1.0)
                    status_msg.success("✨ 전체 삭제 완료!"); time.sleep(1.5)
                    st.session_state.delete_mode = False; st.rerun()
                except Exception as e: st.error(f"실패: {e}"); st.session_state.delete_mode = False
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
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()
            } for _, row in m_df.iterrows()}
            st.success(f"✅ 마스터 로드 완료: {len(st.session_state.master_lookup):,}건")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리
with tab1:
    st.subheader("📥 AS 입고 (중복 체크 강화)")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_v_final")
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터 데이터를 먼저 로드해주세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                # DB 중복 데이터 수집 (날짜+코드 조합)
                existing, offset = set(), 0
                while True:
                    res = supabase.table("as_history").select("입고일, 압축코드").range(offset, offset+4000).execute()
                    if not res.data: break
                    for r in res.data:
                        d = to_pure_date(r['입고일'])
                        if d: existing.add(f"{d}|{sanitize_code(r['압축코드'])}")
                    offset += len(res.data)
                    ui_msg.info(f"🔍 중복 데이터 수집 중... ({offset:,})")
                    if len(res.data) < 4000: break

                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue
                
                as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs, dup_cnt = [], 0
                for i, (_, row) in enumerate(as_in.iterrows()):
                    in_date_obj = to_pure_date(row.iloc[1])
                    code = sanitize_code(row.iloc[7])
                    if not in_date_obj or f"{in_date_obj}|{code}" in existing:
                        dup_cnt += 1; continue
                    
                    m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[3]), {})
                    recs.append({
                        "압축코드": code, "자재번호": sanitize_code(row.iloc[3]), "자재명": str(row.iloc[4]).strip(),
                        "공급업체명": m_info.get("업체", "미등록"), "분류구분": m_info.get("분류", "수리대상"),
                        "입고일": str(in_date_obj), "상태": "출고 대기"
                    })
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []; ui_msg.warning(f"🚀 저장 중... ({i+1:,}/{len(as_in)})"); ui_prog.progress((i+1)/len(as_in))
                
                if recs: supabase.table("as_history").insert(recs).execute()
                ui_msg.success(f"✅ 입고 완료 (신규 저장: {len(as_in)-dup_cnt:,}건)"); ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (선입선출 FIFO 정밀 매칭)
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영 (1:1 선입선출)")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_v_final")
    if o_file and st.button("🚀 출고 데이터 반영", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            if len(as_out) == 0:
                st.error("❌ 'AS 카톤 박스' 행을 찾지 못했습니다.")
            else:
                ui_msg.info("🔍 DB '출고 대기' 데이터 로드 중 (FIFO 정렬)...")
                # 입고일 순서대로 가져와서 선입선출 보장
                db_res = supabase.table("as_history").select("id, 압축코드, 입고일, 상태").eq("상태", "출고 대기").order("입고일").execute()
                
                db_lookup = {}
                for r in db_res.data:
                    c = sanitize_code(r['압축코드'])
                    if c not in db_lookup: db_lookup[c] = []
                    db_lookup[c].append(r)
                
                upd_list, fail_log = [], []
                for i, (_, row) in enumerate(as_out.iterrows()):
                    code = sanitize_code(row.iloc[10])
                    out_date_obj = to_pure_date(row.iloc[6])
                    if not out_date_obj: continue

                    if code in db_lookup and len(db_lookup[code]) > 0:
                        matched = False
                        # FIFO: 가장 과거 입고건(0번 인덱스)부터 날짜 비교
                        for idx, db_r in enumerate(db_lookup[code]):
                            in_date_obj = to_pure_date(db_r['입고일'])
                            if in_date_obj and in_date_obj <= out_date_obj:
                                upd_list.append({"id": db_r['id'], "출고일": str(out_date_obj)})
                                db_lookup[code].pop(idx) # 매칭된 입고 데이터는 목록에서 제거(1:1 소거)
                                matched = True
                                break
                        if not matched: fail_log.append(f"날짜오류: {code} (출고일보다 빠른 입고건 없음)")
                    else: fail_log.append(f"재고없음: {code}")
                    if i % 100 == 0: ui_prog.progress(min((i+1)/len(as_out), 1.0))
                
                if upd_list:
                    ui_msg.warning(f"🔄 {len(upd_list):,}건 매칭 성공! DB 업데이트 시작...")
                    for idx, item in enumerate(upd_list):
                        supabase.table("as_history").update({"출고일": item['출고일'], "상태": "출고 완료"}).eq("id", item['id']).execute()
                        if idx % 50 == 0: ui_prog.progress(idx/len(upd_list))
                    ui_msg.success(f"✅ {len(upd_list):,}건 최종 반영 완료!")
                else:
                    st.error("❌ 매칭 데이터 0건"); st.write("실패 샘플:", fail_log[:10])
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트 생성
with tab3:
    if st.button("📊 분석 리포트 생성", use_container_width=True):
        ui_msg = st.empty()
        try:
            data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset+999).execute()
                if not res.data: break
                data.extend(res.data); offset += len(res.data)
                ui_msg.info(f"📥 데이터 수집 중... ({offset:,}건)")
                if len(res.data) < 1000: break
            df = pd.DataFrame(data)
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='xlsxwriter') as wr: df.to_excel(wr, index=False)
            st.session_state.report = out.getvalue(); ui_msg.success("✅ 생성 완료")
        except Exception as e: st.error(f"오류: {e}")
    if "report" in st.session_state:
        st.download_button("📥 리포트 다운로드", st.session_state.report, "AS_TAT_Report.xlsx", use_container_width=True)
