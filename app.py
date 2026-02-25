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
st.title("📊 AS TAT 통합 관리 시스템 (대용량 무제한 버전)")

# 코드 데이터 정리 함수
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 엔진 관리")
    
    # 실시간 데이터 카운트 기능 (가장 중요)
    if st.button("🔍 현재 DB 데이터 총 개수 확인", use_container_width=True):
        with st.spinner("개수 확인 중..."):
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            st.metric("저장된 데이터", f"{res.count:,} 건")
    
    st.divider()
    
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        with st.spinner("DB 초기화 중..."):
            supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("데이터베이스가 완전히 비워졌습니다.")
        st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 데이터 관리 ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    st.info("입고 시 업체명과 분류를 매칭하기 위한 마스터 엑셀을 업로드하세요.")
    m_file_mgmt = st.file_uploader("마스터 파일 (XLSX/CSV)", type=['xlsx', 'csv'], key="m_mgmt")
    
    if m_file_mgmt and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        with st.spinner("마스터 데이터를 분석 중..."):
            try:
                if m_file_mgmt.name.endswith('.csv'):
                    m_df = pd.read_csv(m_file_mgmt, encoding='cp949').fillna("")
                else:
                    m_df = pd.read_excel(m_file_mgmt).fillna("")
                
                new_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                
                st.session_state.master_lookup = new_lookup
                st.success(f"✅ 마스터 로드 완료! (기준 데이터: {len(new_lookup):,}건)")
            except Exception as e:
                st.error(f"❌ 마스터 로드 실패: {e}")

# --- [TAB 1] 입고 처리 (12만 건 대응 안전 모드) ---
with tab1:
    st.info("💡 12만 건 이상의 대용량 데이터는 전송에 시간이 걸릴 수 있습니다. 브라우저를 끄지 마세요.")
    col1, col2 = st.columns(2)
    with col1:
        if "master_lookup" in st.session_state:
            st.success(f"✅ 마스터 준비됨 ({len(st.session_state.master_lookup):,}건)")
            use_existing = st.checkbox("이 마스터 데이터 사용", value=True)
        else:
            use_existing = False
            st.warning("⚠️ [마스터 관리] 탭에서 마스터를 먼저 로드해 주세요.")
            
    with col2: 
        i_file = st.file_uploader("AS 입고 CSV 파일 업로드", type=['csv'], key="i_up")

    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        status_text = st.empty()
        p_bar = st.progress(0)
        
        try:
            lookup = st.session_state.master_lookup if use_existing else None
            if not lookup:
                st.error("❌ 마스터 데이터가 로드되지 않았습니다.")
            else:
                # CSV 로드
                i_df = None
                for enc in ['utf-8-sig', 'cp949', 'utf-8', 'euc-kr']:
                    try:
                        i_df = pd.read_csv(i_file, encoding=enc).fillna("")
                        if i_df.shape[1] > 1: break
                    except: continue
                
                if i_df is not None:
                    status_text.info("⚙️ 12만 건 데이터 필터링 중... (잠시만 기다려주세요)")
                    combined = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                    mask = combined.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)
                    as_in = i_df[mask].copy()
                    total = len(as_in)
                    
                    if total > 0:
                        recs = []
                        batch_size = 100 
                        for i, (_, row) in enumerate(as_in.iterrows()):
                            cur_mat = sanitize_code(row.iloc[3])
                            m_info = lookup.get(cur_mat, {})
                            
                            try: in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                            except: in_date = "1900-01-01"

                            recs.append({
                                "압축코드": str(row.iloc[7]).strip() if len(row) > 7 else "",
                                "자재번호": cur_mat,
                                "자재명": str(row.iloc[4]).strip() if len(row) > 4 else "",
                                "규격": str(row.iloc[5]).strip() if len(row) > 5 else "",
                                "공급업체명": m_info.get("업체", "미등록"),
                                "분류구분": m_info.get("분류", "수리대상"),
                                "입고일": in_date,
                                "상태": "출고 대기"
                            })
                            
                            if len(recs) >= batch_size:
                                supabase.table("as_history").insert(recs).execute()
                                recs = []
                                p_bar.progress(min((i + 1) / total, 1.0))
                                status_text.info(f"🚀 DB 안정 전송 중: {i+1:,} / {total:,} 건")
                                time.sleep(0.02) # 초고속 전송 중 아주 짧은 휴식
                        
                        if recs: supabase.table("as_history").insert(recs).execute()
                        p_bar.progress(1.0)
                        status_text.success(f"🎊 완료! 총 {total:,}건이 성공적으로 DB에 저장되었습니다.")
                    else:
                        st.error("❌ 파일 내에서 'A/S 철거' 데이터를 찾을 수 없습니다.")
        except Exception as e:
            st.error(f"❌ 입고 오류 발생: {e}")

# --- [TAB 2] 출고 처리 ---
with tab2:
    st.info("📤 출고 엑셀의 '압축코드'를 찾아 DB 상태를 '출고 완료'로 변경합니다.")
    out_file = st.file_uploader("출고 결과 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 데이터 일괄 반영"):
        try:
            with st.spinner("매칭 및 업데이트 중..."):
                df_out = pd.read_excel(out_file).fillna("")
                as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
                if not as_out.empty:
                    as_out['d'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
                    as_out['c'] = as_out.iloc[:, 10].astype(str).str.strip()
                    
                    count = 0
                    for d, codes in as_out.groupby('d')['c'].apply(list).to_dict().items():
                        for j in range(0, len(codes), 100):
                            supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+100]).execute()
                            count += len(codes[j:j+100])
                    st.success(f"✅ 총 {count:,}건의 출고 정보가 반영되었습니다.")
                else:
                    st.warning("⚠️ 출고 대상 데이터를 찾지 못했습니다.")
        except Exception as e:
            st.error(f"❌ 출고 처리 오류: {e}")

# --- [TAB 3] 분석 리포트 (1,000건 제한 해제 + 무제한 로직) ---
with tab3:
    st.subheader("📈 대용량 데이터 분석 리포트")
    st.info("DB에 저장된 12만 건 이상의 데이터를 모두 수집하여 리포트를 생성합니다.")
    
    if st.button("📊 전체 리포트 생성 (무제한 추출)", use_container_width=True):
        with st.spinner("🚀 DB에서 대용량 데이터를 한 조각씩 가져오고 있습니다..."):
            try:
                all_data = []
                offset = 0
                fetch_size = 1000 # 한 번에 가져올 양
                
                # 프로그레스 바와 상태 텍스트
                fetch_status = st.empty()
                
                while True:
                    # range(시작, 끝)을 사용하여 1,000건씩 끊어서 계속 가져옴 (Pagination)
                    res = supabase.table("as_history").select("*").range(offset, offset + fetch_size - 1).execute()
                    
                    if not res.data:
                        break
                    
                    all_data.extend(res.data)
                    fetch_status.info(f"데이터 수집 중... 현재 {len(all_data):,}건 로드 완료")
                    
                    if len(res.data) < fetch_size:
                        break
                        
                    offset += fetch_size
                
                if not all_data:
                    st.warning("⚠️ 분석할 데이터가 DB에 하나도 없습니다.")
                else:
                    df = pd.DataFrame(all_data)
                    
                    # 날짜 가공 및 TAT 계산
                    df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                    df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
                    df['tat'] = (df['출고일'] - df['입고일']).dt.days
                    
                    def make_bin(target_df):
                        if target_df.empty: return None
                        out = io.BytesIO()
                        # 리포트 표준 컬럼 순서
                        cols = ['입고일', '출고일', 'tat', '상태', '자재번호', '자재명', '규격', '압축코드', '공급업체명', '분류구분']
                        existing = [c for c in cols if c in target_df.columns]
                        with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                            target_df[existing].to_excel(wr, index=False)
                        return out.getvalue()

                    # 파일 생성 및 세션 저장
                    st.session_state.bin_tat = make_bin(df[df['출고일'].notna()])
                    st.session_state.bin_stay = make_bin(df[df['출고일'].isna()])
                    st.session_state.bin_total = make_bin(df)
                    st.session_state.data_ready = True
                    st.session_state.total_count = len(df)
                    
                    st.success(f"✅ 총 {len(df):,}건의 리포트 생성이 완료되었습니다!")
                    st.rerun()
            except Exception as e:
                st.error(f"❌ 리포트 생성 중 오류: {e}")

    # 리포트 다운로드 UI
    if st.session_state.get("data_ready"):
        st.divider()
        st.write(f"📂 **최근 생성된 리포트 (총 {st.session_state.total_count:,}건)**")
        c1, c2, c3 = st.columns(3)
        with c1: 
            if st.session_state.bin_tat:
                st.download_button("📥 1. 출고완료 리포트", st.session_state.bin_tat, "1_done.xlsx", use_container_width=True)
            else: st.button("완료 데이터 없음", disabled=True, use_container_width=True)
        with c2: 
            if st.session_state.bin_stay:
                st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_pending.xlsx", use_container_width=True)
        with c3: 
            st.download_button("📥 3. 전체 데이터 합계", st.session_state.bin_total, "3_all.xlsx", use_container_width=True)
