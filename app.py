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
st.title("📊 AS TAT 통합 관리 시스템 (최종 검수 버전)")

# [검증] 자재번호 정제: 소수점 제거 및 대문자 통일
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 엔진 관리")
    
    # [검증] 실시간 카운트: 12만 건 중 몇 건이 들어갔는지 실시간 확인용
    if st.button("🔍 현재 DB 데이터 총 개수 확인", use_container_width=True):
        with st.spinner("개수 확인 중..."):
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            st.metric("저장된 데이터", f"{res.count:,} 건")
    
    st.divider()
    
    # [검증] 안전 삭제: 보안 정책(RLS) 충돌을 피하기 위한 필터 기반 삭제
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        with st.spinner("데이터를 비우는 중입니다..."):
            try:
                # 조건 없이 삭제 시 발생하는 API 에러를 방지하기 위해 id > 0 조건 사용
                supabase.table("as_history").delete().gt("id", 0).execute()
                st.success("데이터베이스가 완전히 비워졌습니다.")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"초기화 중 오류 발생: {e}")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 데이터 관리 ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file_mgmt = st.file_uploader("마스터 파일 (XLSX/CSV)", type=['xlsx', 'csv'], key="m_mgmt")
    if m_file_mgmt and st.button("🔄 마스터 데이터 로드", use_container_width=True):
        with st.spinner("마스터 로드 중..."):
            try:
                # 인코딩 대응 및 데이터 로드
                m_df = pd.read_csv(m_file_mgmt, encoding='cp949').fillna("") if m_file_mgmt.name.endswith('.csv') else pd.read_excel(m_file_mgmt).fillna("")
                # 업체명(6번째)과 분류(11번째) 매핑
                new_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                
                st.session_state.master_lookup = new_lookup
                st.success(f"✅ {len(new_lookup):,}건 로드 완료")
            except Exception as e: st.error(f"실패: {e}")

# --- [TAB 1] 입고 처리 ---
with tab1:
    st.info("💡 12만 건 데이터 전송 시 '안전 전송' 모드가 자동으로 작동합니다.")
    i_file = st.file_uploader("AS 입고 CSV 파일 업로드", type=['csv'], key="i_up")
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ 마스터 데이터를 먼저 로드하세요.")
        else:
            status_text = st.empty()
            p_bar = st.progress(0)
            try:
                i_df = None
                for enc in ['utf-8-sig', 'cp949', 'utf-8', 'euc-kr']:
                    try:
                        i_df = pd.read_csv(i_file, encoding=enc).fillna("")
                        if i_df.shape[1] > 1: break
                    except: continue
                
                if i_df is not None:
                    # 'A/S철거' 키워드 필터링
                    combined = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                    mask = combined.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)
                    as_in = i_df[mask].copy()
                    total = len(as_in)
                    
                    if total > 0:
                        recs = []
                        # [검증] 배치 사이즈 100: 대용량 전송 시 가장 안정적인 수치
                        batch_size = 100 
                        for i, (_, row) in enumerate(as_in.iterrows()):
                            cur_mat = sanitize_code(row.iloc[3])
                            m_info = st.session_state.master_lookup.get(cur_mat, {})
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
                                status_text.info(f"🚀 전송 중: {i+1:,} / {total:,}")
                                time.sleep(0.01) # API 레이트 리밋 방지
                        if recs: supabase.table("as_history").insert(recs).execute()
                        st.success(f"🎊 {total:,}건 저장 완료!")
            except Exception as e: st.error(f"오류: {e}")

# --- [TAB 2] 출고 처리 ---
with tab2:
    out_file = st.file_uploader("출고 결과 엑셀", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 반영"):
        try:
            with st.spinner("매칭 중..."):
                df_out = pd.read_excel(out_file).fillna("")
                as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
                if not as_out.empty:
                    as_out['d'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
                    as_out['c'] = as_out.iloc[:, 10].astype(str).str.strip()
                    # 날짜별로 그룹화하여 벌크 업데이트
                    for d, codes in as_out.groupby('d')['c'].apply(list).to_dict().items():
                        for j in range(0, len(codes), 100):
                            supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", codes[j:j+100]).execute()
                    st.success("✅ 출고 업데이트 완료")
        except Exception as e: st.error(f"오류: {e}")

# --- [TAB 3] 분석 리포트 ---
with tab3:
    # [검증] Pagination 로직: 1,000건 제한을 완벽히 해제하고 모든 데이터를 로드
    if st.button("📊 리포트 생성 (12만 건 무제한 추출)", use_container_width=True):
        fetch_status = st.empty()
        with st.spinner("🚀 DB에서 데이터를 수집 중..."):
            try:
                all_data = []
                offset = 0
                fetch_size = 1000
                while True:
                    res = supabase.table("as_history").select("*").range(offset, offset + fetch_size - 1).execute()
                    if not res.data: break
                    all_data.extend(res.data)
                    fetch_status.info(f"데이터 로드 중... 현재 {len(all_data):,}건 완료")
                    if len(res.data) < fetch_size: break
                    offset += fetch_size
                
                if all_data:
                    df = pd.DataFrame(all_data)
                    df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                    df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
                    df['tat'] = (df['출고일'] - df['입고일']).dt.days
                    
                    # [검증] 엑셀 저장 함수: 요청하신 컬럼 순서 고정
                    def make_bin(target_df):
                        if target_df.empty: return None
                        out = io.BytesIO()
                        # ★ 컬럼 순서 재배치 ★
                        target_cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '분류구분', '압축코드', '출고일', 'tat']
                        # 실제 존재하는 컬럼만 선별하여 정렬
                        existing = [c for c in target_cols if c in target_df.columns]
                        with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                            target_df[existing].to_excel(wr, index=False)
                        return out.getvalue()

                    st.session_state.bin_tat = make_bin(df[df['출고일'].notna()])
                    st.session_state.bin_stay = make_bin(df[df['출고일'].isna()])
                    st.session_state.bin_total = make_bin(df)
                    st.session_state.data_ready = True
                    st.session_state.total_count = len(df)
                    st.rerun()
            except Exception as e: st.error(f"오류: {e}")

    if st.session_state.get("data_ready"):
        st.divider()
        st.write(f"📂 **분석 리포트 다운로드 (대상: {st.session_state.total_count:,}건)**")
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 출고완료", st.session_state.bin_tat, "1_done.xlsx", use_container_width=True)
        with c2: st.download_button("📥 미출고", st.session_state.bin_stay, "2_pending.xlsx", use_container_width=True)
        with c3: st.download_button("📥 전체합계", st.session_state.bin_total, "3_all.xlsx", use_container_width=True)
