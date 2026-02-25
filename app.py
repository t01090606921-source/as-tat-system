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
st.title("📊 AS TAT 통합 관리 시스템 (최종 완성본)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 관리 (대용량 분할 삭제) ---
with st.sidebar:
    st.header("⚙️ 시스템 엔진 관리")
    if st.button("🔍 현재 DB 데이터 총 개수 확인", use_container_width=True):
        with st.spinner("개수 확인 중..."):
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            st.metric("저장된 데이터", f"{res.count:,} 건")
    
    st.divider()
    
    if st.button("⚠️ DB 데이터 전량 삭제 (분할)", type="primary", use_container_width=True):
        st.session_state.clear()
        status_box = st.empty()
        try:
            while True:
                sub_res = supabase.table("as_history").select("id").limit(5000).execute()
                if not sub_res.data: break
                ids_to_del = [item['id'] for item in sub_res.data]
                supabase.table("as_history").delete().in_("id", ids_to_del).execute()
                status_box.info(f"🗑️ 삭제 중... (현재 처리 ID: {ids_to_del[-1]})")
                time.sleep(0.1)
            st.success("✅ 초기화 완료")
            st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 관리 ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일 (XLSX/CSV)", type=['xlsx', 'csv'], key="m_mgmt")
    if m_file and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
            } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
            st.success(f"✅ {len(st.session_state.master_lookup):,}건 로드 완료")
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 1] 입고 처리 (중복 입고 방지) ---
with tab1:
    st.info("💡 입고 시 [압축코드]가 동일한 기존 데이터는 자동으로 제외됩니다.")
    i_file = st.file_uploader("AS 입고 CSV 파일 업로드", type=['csv'], key="i_up")
    if i_file and st.button("🚀 중복 제외 입고 시작", use_container_width=True):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            status_text = st.empty()
            p_bar = st.progress(0)
            try:
                # DB의 모든 압축코드 로드 (메모리 최적화를 위해 set 사용)
                status_text.info("🔍 중복 검사 준비 중...")
                existing_codes = set()
                offset = 0
                while True:
                    res = supabase.table("as_history").select("압축코드").range(offset, offset + 999).execute()
                    if not res.data: break
                    for r in res.data: existing_codes.add(str(r['압축코드']))
                    if len(res.data) < 1000: break
                    offset += 1000

                i_df = pd.read_csv(i_file, encoding='cp949').fillna("")
                combined = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                as_in = i_df[combined.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                
                recs = []
                dup_cnt = 0
                for i, (_, row) in enumerate(as_in.iterrows()):
                    code = str(row.iloc[7]).strip()
                    if code in existing_codes:
                        dup_cnt += 1
                        continue
                    
                    cur_mat = sanitize_code(row.iloc[3])
                    m_info = st.session_state.master_lookup.get(cur_mat, {})
                    recs.append({
                        "압축코드": code, "자재번호": cur_mat, "자재명": str(row.iloc[4]).strip(),
                        "규격": str(row.iloc[5]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"), "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d'),
                        "상태": "출고 대기"
                    })
                    if len(recs) >= 100:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []
                        p_bar.progress(min((i + 1) / len(as_in), 1.0))
                if recs: supabase.table("as_history").insert(recs).execute()
                st.success(f"✅ 입고 완료 (신규: {len(as_in)-dup_cnt:,}건 / 중복제외: {dup_cnt:,}건)")
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 2] 출고 처리 (중복 방지 & 선후 관계 검증) ---
with tab2:
    st.info("📤 조건 검증: ① 이미 출고된 동일 압축코드 제외 ② 입고일보다 빠른 출고일 제외")
    out_file = st.file_uploader("출고 결과 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 검증 후 출고 반영", use_container_width=True):
        try:
            with st.spinner("데이터 검증 및 업데이트 중..."):
                df_out = pd.read_excel(out_file).fillna("")
                as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
                
                # DB의 입고/출고일/압축코드 정보 로드
                db_res = supabase.table("as_history").select("압축코드", "입고일", "출고일", "상태").execute()
                db_dict = {str(item['압축코드']): item for item in db_res.data}
                
                update_groups = {}
                update_cnt = 0
                skip_cnt = 0
                
                for _, row in as_out.iterrows():
                    code = str(row.iloc[10]).strip()
                    excel_out_date = pd.to_datetime(row.iloc[6]).strftime('%Y-%m-%d')
                    
                    if code in db_dict:
                        db_info = db_dict[code]
                        # 검증 1: 중복 출고 방지 (이미 동일 날짜로 출고 완료된 경우)
                        if db_info['상태'] == "출고 완료" and str(db_info['출고일']) == excel_out_date:
                            skip_cnt += 1
                            continue
                        # 검증 2: 선후 관계 (입고일 > 출고일이면 제외)
                        if str(db_info['입고일']) > excel_out_date:
                            skip_cnt += 1
                            continue
                        
                        if excel_out_date not in update_groups: update_groups[excel_out_date] = []
                        update_groups[excel_out_date].append(code)
                    else: skip_cnt += 1 # DB에 없는 압축코드
                
                for d, codes in update_groups.items():
                    for j in range(0, len(codes), 100):
                        supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+100]).execute()
                        update_cnt += len(codes[j:j+100])
                st.success(f"✅ 반영: {update_cnt:,}건 / 제외(중복/날짜오류): {skip_cnt:,}건")
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 3] 분석 리포트 (요청 순서 고정 및 무제한 추출) ---
with tab3:
    if st.button("📊 리포트 생성 (12만 건 무제한 추출)", use_container_width=True):
        with st.spinner("🚀 대용량 데이터를 수집 중..."):
            all_data = []
            offset = 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset + 999).execute()
                if not res.data: break
                all_data.extend(res.data)
                if len(res.data) < 1000: break
                offset += 1000
            
            if all_data:
                df = pd.DataFrame(all_data)
                df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
                df['tat'] = (df['출고일'] - df['입고일']).dt.days
                
                # 리포트 출력용 컬럼 순서 설정
                cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '분류구분', '압축코드', '출고일', 'tat']
                
                def make_bin(target_df):
                    if target_df.empty: return None
                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                        target_df[[c for c in cols if c in target_df.columns]].to_excel(wr, index=False)
                    return out.getvalue()

                st.session_state.bin_tat = make_bin(df[df['출고일'].notna()])
                st.session_state.bin_stay = make_bin(df[df['출고일'].isna()])
                st.session_state.bin_total = make_bin(df)
                st.session_state.data_ready = True
                st.rerun()

    if st.session_state.get("data_ready"):
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 출고완료", st.session_state.bin_tat, "1_done.xlsx", use_container_width=True)
        with c2: st.download_button("📥 미출고", st.session_state.bin_stay, "2_pending.xlsx", use_container_width=True)
        with c3: st.download_button("📥 전체합계", st.session_state.bin_total, "3_all.xlsx", use_container_width=True)
