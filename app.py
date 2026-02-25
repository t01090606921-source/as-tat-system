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
    st.error("⚠️ Supabase 접속 정보가 올바르지 않습니다. .streamlit/secrets.toml을 확인하세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (최종 검증본)")

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
                # 타임아웃 방지를 위해 5,000건씩 삭제
                sub_res = supabase.table("as_history").select("id").limit(5000).execute()
                if not sub_res.data: break
                ids_to_del = [item['id'] for item in sub_res.data]
                supabase.table("as_history").delete().in_("id", ids_to_del).execute()
                status_box.info(f"🗑️ 삭제 진행 중... (현재 ID: {ids_to_del[-1]})")
                time.sleep(0.1)
            st.success("✅ DB 초기화 완료")
            st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 데이터 관리 ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일 (XLSX/CSV)", type=['xlsx', 'csv'], key="m_mgmt")
    if m_file and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        try:
            m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
            # 자재번호(0), 업체(5), 분류(10) 컬럼 기준 매핑
            st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
            } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
            st.success(f"✅ {len(st.session_state.master_lookup):,}건 로드 완료")
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 1] 입고 처리 (복합 중복 체크 & 진행률 표시) ---
with tab1:
    st.info("💡 [입고일 + 압축코드] 조합이 DB와 일치하면 제외, 입고일이 다르면 추가 입고됩니다.")
    i_file = st.file_uploader("AS 입고 CSV 파일 업로드", type=['csv'], key="i_up")
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        if "master_lookup" not in st.session_state: st.error("⚠️ 마스터를 먼저 로드하세요.")
        else:
            msg = st.empty()
            p_bar = st.progress(0)
            try:
                # 1. 기존 DB의 복합키 로드 (입고일|압축코드)
                existing_combos = set()
                offset = 0
                count_res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
                total_db = count_res.count if count_res.count else 1
                
                while True:
                    res = supabase.table("as_history").select("입고일", "압축코드").range(offset, offset + 999).execute()
                    if not res.data: break
                    for r in res.data:
                        existing_combos.add(f"{r['입고일']}|{str(r['압축코드']).strip()}")
                    offset += len(res.data)
                    msg.info(f"🔍 기존 데이터 대조 중... ({offset:,} / {total_db:,} 건 로드 완료)")
                    p_bar.progress(min(offset / total_db, 1.0))
                    if len(res.data) < 1000: break

                # 2. 파일 분석 (인코딩 자동 감지)
                i_df = None
                for enc in ['utf-8-sig', 'cp949', 'utf-8', 'euc-kr']:
                    try:
                        i_file.seek(0)
                        i_df = pd.read_csv(i_file, encoding=enc).fillna("")
                        break
                    except: continue
                
                if i_df is not None:
                    # 'A/S철거' 또는 'AS철거' 포함 행 필터링
                    combined_text = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                    as_in = i_df[combined_text.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                    
                    recs, dup_cnt = [], 0
                    total_in = len(as_in)
                    for i, (_, row) in enumerate(as_in.iterrows()):
                        try: in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except: in_date = "1900-01-01"
                        code = str(row.iloc[7]).strip()
                        
                        # [복합키 중복 체크]
                        if f"{in_date}|{code}" in existing_combos:
                            dup_cnt += 1; continue
                        
                        cur_mat = sanitize_code(row.iloc[3])
                        m_info = st.session_state.master_lookup.get(cur_mat, {})
                        recs.append({
                            "압축코드": code, "자재번호": cur_mat, "자재명": str(row.iloc[4]).strip(),
                            "규격": str(row.iloc[5]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                            "분류구분": m_info.get("분류", "수리대상"), "입고일": in_date, "상태": "출고 대기"
                        })
                        
                        if len(recs) >= 100: # 100건씩 벌크 입력
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                            msg.warning(f"🚀 신규 데이터 저장 중... ({i+1:,} / {total_in:,} 건)")
                            p_bar.progress(min((i+1)/total_in, 1.0))
                    
                    if recs: supabase.table("as_history").insert(recs).execute()
                    st.success(f"✅ 입고 완료 (신규: {total_in-dup_cnt:,} / 중복제외: {dup_cnt:,})")
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 2] 출고 처리 (선후 관계 검증 & 진행률 표시) ---
with tab2:
    st.info("📤 입고일보다 빠른 출고일은 제외되며, 압축코드가 같은 여러 건의 입고 데이터에 일괄 적용됩니다.")
    out_file = st.file_uploader("출고 결과 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 검증 후 출고 반영", use_container_width=True):
        msg = st.empty()
        p_bar = st.progress(0)
        try:
            df_out = pd.read_excel(out_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            
            # DB의 전체 입고 상태 로드
            msg.info("🔍 DB 데이터를 분석 중입니다...")
            db_res = supabase.table("as_history").select("id", "압축코드", "입고일", "출고일", "상태").execute()
            db_data = db_res.data
            
            update_list, skip_cnt = [], 0
            total_out = len(as_out)
            
            for i, (_, row) in enumerate(as_out.iterrows()):
                code = str(row.iloc[10]).strip()
                ex_out_date = pd.to_datetime(row.iloc[6]).strftime('%Y-%m-%d')
                
                # 해당 압축코드를 가진 모든 DB 행(입고 건) 탐색
                matched = [r for r in db_data if str(r['압축코드']) == code]
                
                valid_match = False
                for db_row in matched:
                    # [선후 관계 검증] 입고일 <= 출고일
                    if str(db_row['입고일']) <= ex_out_date:
                        # 이미 동일 날짜로 출고 완료된 건 중복 업데이트 방지
                        if db_row['상태'] == "출고 완료" and str(db_row['출고일']) == ex_out_date:
                            continue
                        update_list.append({"id": db_row['id'], "출고일": ex_out_date})
                        valid_match = True
                
                if not valid_match: skip_cnt += 1
                if i % 100 == 0: p_bar.progress(min((i+1)/total_out, 1.0))

            if update_list:
                for idx, item in enumerate(update_list):
                    supabase.table("as_history").update({"출고일": item['출고일'], "상태": "출고 완료"}).eq("id", item['id']).execute()
                    if idx % 50 == 0:
                        msg.warning(f"🔄 DB 출고 정보 업데이트 중... ({idx:,} / {len(update_list):,})")
                        p_bar.progress(min(idx/len(update_list), 1.0))
                st.success(f"✅ 반영 완료: {len(update_list):,}건 / 제외 건: {skip_cnt:,}건")
            else:
                st.warning("⚠️ 업데이트할 대상이 없습니다.")
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 3] 분석 리포트 (순서 고정 & 무제한 추출) ---
with tab3:
    if st.button("📊 리포트 생성 (12만 건 무제한 추출)", use_container_width=True):
        msg = st.empty()
        p_bar = st.progress(0)
        try:
            all_data, offset = [], 0
            count_res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            total_db = count_res.count if count_res.count else 1
            
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset + 999).execute()
                if not res.data: break
                all_data.extend(res.data)
                offset += len(res.data)
                msg.info(f"📥 DB 데이터 수집 중... ({offset:,} / {total_db:,})")
                p_bar.progress(min(offset/total_db, 1.0))
                if len(res.data) < 1000: break
            
            if all_data:
                df = pd.DataFrame(all_data)
                df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
                df['tat'] = (df['출고일'] - df['입고일']).dt.days
                
                # 사용자 요청 컬럼 순서 고정
                cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '분류구분', '압축코드', '출고일', 'tat']
                existing = [c for c in cols if c in df.columns]

                def make_bin(target_df):
                    if target_df.empty: return None
                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                        target_df[existing].to_excel(wr, index=False)
                    return out.getvalue()

                st.session_state.bin_tat = make_bin(df[df['출고일'].notna()])
                st.session_state.bin_stay = make_bin(df[df['출고일'].isna()])
                st.session_state.bin_total = make_bin(df)
                st.session_state.data_ready = True
                st.rerun()
        except Exception as e: st.error(f"오류: {e}")

    if st.session_state.get("data_ready"):
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 출고완료", st.session_state.bin_tat, "1_done.xlsx", use_container_width=True)
        with c2: st.download_button("📥 미출고", st.session_state.bin_stay, "2_pending.xlsx", use_container_width=True)
        with c3: st.download_button("📥 전체합계", st.session_state.bin_total, "3_all.xlsx", use_container_width=True)
