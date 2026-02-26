import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# 자재번호 정제 함수 (소수점 제거 및 대문자화)
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 메인 탭 구성 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 관리 ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    # 업로드 박스 (Key값 고유화로 증발 방지)
    m_file = st.file_uploader("마스터 파일을 선택하세요 (XLSX, CSV)", type=['xlsx', 'csv'], key="master_up_v13")
    
    if m_file:
        if st.button("🔄 마스터 데이터 시스템 로드", use_container_width=True):
            try:
                msg = st.empty()
                msg.info("⌛ 데이터를 분석 중입니다...")
                if m_file.name.endswith('.csv'):
                    m_df = pd.read_csv(m_file, encoding='cp949').fillna("")
                else:
                    m_df = pd.read_excel(m_file).fillna("")
                
                # 0:자재번호, 5:업체명, 10:분류구분
                st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                msg.success(f"✅ 로드 완료: {len(st.session_state.master_lookup):,}건")
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 1] 입고 처리 ---
with tab1:
    st.subheader("📥 AS 입고 (중복 체크 포함)")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_up_v13")
    if i_file and st.button("🚀 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 먼저 [마스터 관리] 탭에서 데이터를 로드해주세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                # 1. 기존 DB 데이터 로드 (중복 체크용)
                existing_combos = set()
                offset, batch_size = 0, 4000
                while True:
                    res = supabase.table("as_history").select("입고일, 압축코드").range(offset, offset + batch_size - 1).execute()
                    if not res.data: break
                    for r in res.data:
                        existing_combos.add(f"{pd.to_datetime(r['입고일']).strftime('%Y-%m-%d')}|{str(r['압축코드']).strip().upper()}")
                    offset += len(res.data)
                    ui_msg.info(f"🔍 DB 대조 데이터 수집 중... ({offset:,}건)")
                    if len(res.data) < batch_size: break

                # 2. 입고 파일 처리
                for enc in ['utf-8-sig', 'cp949', 'utf-8']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue

                combined = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                as_in = i_df[combined.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs, dup_cnt, total_in = [], 0, len(as_in)

                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        code = str(row.iloc[7]).strip().upper()
                        if f"{in_date}|{code}" in existing_combos:
                            dup_cnt += 1; continue
                        
                        m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[3]), {})
                        recs.append({
                            "압축코드": code, "자재번호": sanitize_code(row.iloc[3]), "자재명": str(row.iloc[4]).strip(),
                            "규격": str(row.iloc[5]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                            "분류구분": m_info.get("분류", "수리대상"), "입고일": in_date, "상태": "출고 대기"
                        })
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                            ui_msg.warning(f"🚀 신규 데이터 저장 중... ({i+1:,} / {total_in:,})")
                            ui_prog.progress(min((i+1)/total_in, 1.0))
                    except: continue
                
                if recs: supabase.table("as_history").insert(recs).execute()
                ui_msg.success(f"✅ 완료 (저장: {total_in-dup_cnt:,} / 중복: {dup_cnt:,})")
                ui_prog.progress(1.0)
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 2] 출고 처리 (고속 엔진 적용) ---
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up_v13")
    
    if out_file and st.button("🚀 출고 반영 시작", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(out_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            
            ui_msg.info("🔍 [1/3] DB 데이터 로드 중 (메모리 최적화)...")
            db_res = supabase.table("as_history").select("id, 압축코드, 입고일").execute()
            
            ui_msg.info("⚙️ [2/3] 고속 매칭 엔진 빌드 중...")
            # 딕셔너리로 검색 속도 극대화
            db_lookup = {}
            for r in db_res.data:
                c = str(r['압축코드']).strip().upper()
                if c not in db_lookup: db_lookup[c] = []
                db_lookup[c].append(r)
            
            update_list, total_out = [], len(as_out)
            for i, (_, row) in enumerate(as_out.iterrows()):
                try:
                    code = str(row.iloc[10]).strip().upper()
                    ex_out_date = pd.to_datetime(row.iloc[6]).strftime('%Y-%m-%d')
                    
                    matches = db_lookup.get(code, [])
                    for db_row in matches:
                        if str(db_row['입고일']) <= ex_out_date:
                            update_list.append({"id": db_row['id'], "출고일": ex_out_date})
                    
                    if i % 100 == 0:
                        ui_msg.info(f"🧪 [진행상황] 유효성 검증 중... ({i+1:,} / {total_out:,})")
                        ui_prog.progress(min((i+1)/total_out, 1.0))
                except: continue

            if update_list:
                total_upd = len(update_list)
                for idx, item in enumerate(update_list):
                    try:
                        supabase.table("as_history").update({"출고일": item['출고일'], "상태": "출고 완료"}).eq("id", item['id']).execute()
                        if idx % 50 == 0:
                            ui_msg.warning(f"🔄 [3/3] DB 최종 업데이트 중... ({idx:,} / {total_upd:,})")
                            ui_prog.progress(min(idx/total_upd, 1.0))
                    except: continue
                ui_msg.success(f"✅ 반영 완료: {total_upd:,}건")
                ui_prog.progress(1.0)
            else:
                ui_msg.warning("⚠️ 일치하는 데이터를 찾을 수 없습니다.")
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 3] 리포트 생성 ---
with tab3:
    st.subheader("📈 분석 리포트")
    if st.button("📊 리포트 파일 생성", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            all_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset + 999).execute()
                if not res.data: break
                all_data.extend(res.data)
                offset += len(res.data)
                ui_msg.info(f"📥 데이터 수집 중... ({offset:,}건)")
                ui_prog.progress(min(offset / 120000, 1.0)) # 대략적 비율
                if len(res.data) < 1000: break
            
            df = pd.DataFrame(all_data)
            df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
            df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
            df['tat'] = (df['출고일'] - df['입고일']).dt.days
            
            def make_bin(target_df):
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                    target_df.to_excel(wr, index=False)
                return out.getvalue()

            st.session_state.bin_total = make_bin(df)
            st.session_state.data_ready = True
            ui_msg.success("✅ 파일 생성 완료!")
            st.rerun()
        except Exception as e: st.error(f"오류: {e}")

    if st.session_state.get("data_ready"):
        st.download_button("📥 전체 리포트 다운로드 (XLSX)", st.session_state.bin_total, "AS_TAT_Total_Report.xlsx", use_container_width=True)
