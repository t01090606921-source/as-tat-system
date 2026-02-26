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
    st.error("⚠️ Supabase 접속 설정을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# [정제 함수] 공백, 소수점, 대문자 처리
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    # 엑셀에서 숫자로 인식된 경우(123.0) .0 제거 후 공백 박멸
    clean_val = str(val).split('.')[0].replace(" ", "").strip().upper()
    return clean_val

# [날짜 객체화] 비교 연산을 위해 순수 date 객체로 변환
def to_pure_date(val):
    try:
        return pd.to_datetime(val).date()
    except:
        return None

# --- 2. 사이드바 (DB 현황 및 전체 삭제) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        try:
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            st.metric("저장된 데이터", f"{res.count if res.count is not None else 0:,} 건")
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
            if st.button("✅ 확정(비우기)", use_container_width=True):
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
                    msg.success("✨ 삭제 완료!"); time.sleep(1); st.session_state.delete_mode = False; st.rerun()
                except Exception as e: st.error(f"실패: {e}"); st.session_state.delete_mode = False
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 기준 정보")
    m_file = st.file_uploader("마스터 파일(XLSX, CSV)", type=['xlsx', 'csv'], key="m_v_v2")
    if m_file and st.button("🔄 마스터 로드"):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()
            } for _, row in m_df.iterrows()}
            st.success("✅ 로드 완료")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 1] 입고 처리
with tab1:
    st.subheader("📥 AS 입고")
    i_file = st.file_uploader("입고 CSV", type=['csv'], key="i_v_v2")
    if i_file and st.button("🚀 입고 시작"):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                # DB 중복 체크
                existing = set(); offset = 0
                while True:
                    res = supabase.table("as_history").select("입고일, 압축코드").range(offset, offset+4000).execute()
                    if not res.data: break
                    for r in res.data:
                        d = to_pure_date(r['입고일'])
                        if d: existing.add(f"{d}|{sanitize_code(r['압축코드'])}")
                    offset += len(res.data)
                    ui_msg.info(f"🔍 중복 체크 중... ({offset:,})")
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
                ui_msg.success(f"✅ 입고 완료 (신규: {len(as_in)-dup_cnt:,})"); ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (정밀 대조 및 로그 강화)
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_v_v2")
    if o_file and st.button("🚀 출고 반영 시작"):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            if len(as_out) == 0:
                st.error("❌ 'AS 카톤 박스' 행을 찾지 못했습니다. 파일 형식을 확인하세요.")
            else:
                ui_msg.info("🔍 DB에서 '출고 대기' 데이터 로드 중...")
                # 성능 최적화를 위해 '출고 대기' 상태인 것만 가져옴
                db_res = supabase.table("as_history").select("id, 압축코드, 입고일, 상태").eq("상태", "출고 대기").execute()
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

                    if code in db_lookup:
                        matched = False
                        for db_r in db_lookup[code]:
                            in_date_obj = to_pure_date(db_r['입고일'])
                            # [날짜 정밀 비교] 순수 날짜 객체끼리 비교
                            if in_date_obj and in_date_obj <= out_date_obj:
                                upd_list.append({"id": db_r['id'], "출고일": str(out_date_obj)})
                                matched = True
                                break # 한 번 매칭되면 다음 행으로
                        if not matched:
                            fail_log.append(f"날짜오류: {code} (입고 {db_lookup[code][0]['입고일']} > 출고 {out_date_obj})")
                    else:
                        if len(fail_log) < 10: fail_log.append(f"코드없음: {code}")
                    
                    if i % 100 == 0: ui_prog.progress(min((i+1)/len(as_out), 1.0))
                
                if upd_list:
                    final_upds = {item['id']: item['출고일'] for item in upd_list}
                    for idx, (tid, tdate) in enumerate(final_upds.items()):
                        supabase.table("as_history").update({"출고일": tdate, "상태": "출고 완료"}).eq("id", tid).execute()
                        if idx % 50 == 0: ui_msg.warning(f"🔄 업데이트 중... ({idx:,}/{len(final_upds)})")
                    ui_msg.success(f"✅ {len(final_upds):,}건 반영 성공!")
                else:
                    st.error("❌ 일치하는 데이터를 찾지 못했습니다.")
                    with st.expander("🧐 상세 원인 분석 (미매칭 샘플)"):
                        for log in fail_log[:20]: st.write(log)
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트
with tab3:
    if st.button("📊 리포트 생성"):
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
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='xlsxwriter') as wr: df.to_excel(wr, index=False)
            st.session_state.report = out.getvalue(); ui_msg.success("✅ 생성 완료")
        except Exception as e: st.error(f"오류: {e}")
    if "report" in st.session_state:
        st.download_button("📥 다운로드", st.session_state.report, "AS_TAT_Report.xlsx")
