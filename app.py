import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

def clean_mat_no(val):
    """자재번호 전처리: 소수점 제거 및 문자열 통일 (예: 1234.0 -> '1234')"""
    if pd.isna(val): return ""
    try:
        # 숫자형(float)인 경우 소수점 버리고 정수로 변환
        if isinstance(val, (float, int)):
            return str(int(float(val))).strip().upper()
        # 문자열인 경우 공백만 제거
        return str(val).strip().upper()
    except:
        return str(val).strip().upper()

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템 (Cloud)")

# --- 2. 사이드바: 마스터 데이터 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    master_file = st.file_uploader("분류구분 마스터 업로드 (엑셀)", type=['xlsx'])
    if master_file and st.button("마스터 DB 갱신"):
        try:
            m_df = pd.read_excel(master_file)
            m_data = []
            for _, row in m_df.iterrows():
                mat_no = clean_mat_no(row.iloc[0]) # A열: 자재번호
                if not mat_no or mat_no == 'NAN': continue
                
                m_data.append({
                    "자재번호": mat_no,
                    "공급업체명": str(row.iloc[5]).strip(), # F열: 공급업체명
                    "분류구분": str(row.iloc[10]).strip() # K열: 분류구분
                })
            
            if m_data:
                # 안전하게 기존 데이터 삭제 후 새 데이터 삽입
                supabase.table("master_data").delete().neq("자재번호", "TEMP_ZERO").execute()
                # 500건씩 분할 업로드
                for i in range(0, len(m_data), 500):
                    supabase.table("master_data").insert(m_data[i:i+500]).execute()
                st.success(f"✅ 마스터 {len(m_data)}건 동기화 완료!")
        except Exception as e:
            st.error(f"마스터 갱신 오류: {e}")

# --- 3. 메인 화면: 입고/출고 처리 ---
tab1, tab2 = st.tabs(["📥 AS 입고 처리", "📤 AS 출고 처리"])

with tab1:
    in_file = st.file_uploader("입고 현황 엑셀 업로드", type=['xlsx'], key="in_up")
    if in_file and st.button("입고 데이터 처리 시작"):
        with st.spinner("데이터 분석 및 마스터 매칭 중..."):
            try:
                df = pd.read_excel(in_file)
                # 'A/S 철거' 텍스트가 포함된 행만 필터링
                as_in = df[df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
                
                if as_in.empty:
                    st.warning("⚠️ 업로드한 파일에 'A/S 철거' 대상이 없습니다.")
                else:
                    # 마스터 데이터 실시간 로드
                    master_res = supabase.table("master_data").select("*").execute()
                    m_lookup = {r['자재번호']: r for r in master_res.data}

                    new_recs = []
                    for _, row in as_in.iterrows():
                        key_val = str(row.iloc[7]).strip() # H열: 압축코드
                        mat_no = clean_mat_no(row.iloc[3]) # D열: 자재번호
                        
                        if not key_val or key_val == 'nan': continue
                        
                        m_info = m_lookup.get(mat_no)
                        vendor = m_info['공급업체명'] if m_info else "미등록"
                        category = m_info['분류구분'] if m_info else "미등록"
                        
                        new_recs.append({
                            "압축코드": key_val, 
                            "자재번호": mat_no, 
                            "규격": str(row.iloc[5]).strip(), # F열: 규격
                            "공급업체명": vendor, 
                            "분류구분": category, 
                            "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d'),
                            "상태": "출고 대기"
                        })
                    
                    if new_recs:
                        # 중복 방지를 위해 DB에 없는 건만 넣거나, 정책에 따라 insert
                        for i in range(0, len(new_recs), 500):
                            supabase.table("as_history").insert(new_recs[i:i+500]).execute()
                        st.success(f"✅ {len(new_recs)}건 입고 등록 완료!")
                        st.rerun()
            except Exception as e:
                st.error(f"입고 처리 중 오류: {e}")

with tab2:
    out_file = st.file_uploader("출고 현황 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("출고 매칭 및 TAT 계산"):
        with st.spinner("출고 일자 매칭 중..."):
            try:
                df = pd.read_excel(out_file)
                # 'AS 카톤 박스' 텍스트 포함 행 필터링
                as_out = df[df.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
                
                match_count = 0
                for _, row in as_out.iterrows():
                    key_val = str(row.iloc[10]).strip() # K열: 압축코드
                    try:
                        out_date_dt = pd.to_datetime(row.iloc[6]) # G열: 출고일
                    except: continue
                    
                    # FIFO(선입선출) 방식으로 가장 오래된 '출고 대기' 건 찾기
                    target = supabase.table("as_history").select("id, 입고일")\
                        .match({"압축코드": key_val, "상태": "출고 대기"})\
                        .order("입고일").limit(1).execute()
                    
                    if target.data:
                        row_id = target.data[0]['id']
                        in_dt = pd.to_datetime(target.data[0]['입고일'])
                        tat = round((out_date_dt - in_dt).total_seconds() / (24 * 3600), 2)
                        
                        supabase.table("as_history").update({
                            "출고일": out_date_dt.strftime('%Y-%m-%d'),
                            "tat": tat,
                            "상태": "출고 완료"
                        }).eq("id", row_id).execute()
                        match_count += 1
                
                st.success(f"✅ {match_count}건 출고 완료 처리되었습니다.")
                st.rerun()
            except Exception as e:
                st.error(f"출고 처리 중 오류: {e}")

# --- 4. 대시보드 리포트 영역 ---
st.divider()
try:
    # 전체 데이터 불러오기
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    all_data = pd.DataFrame(res.data)

    if not all_data.empty:
        st.subheader("📊 AS 분석 현황")
        
        # 필터링 섹션
        c1, c2, c3 = st.columns(3)
        v_filter = c1.multiselect("공급업체 선택", options=sorted(all_data['공급업체명'].unique()))
        g_filter = c2.multiselect("분류구분 선택", options=sorted(all_data['분류구분'].unique()))
        s_filter = c3.multiselect("진행 상태", options=['출고 대기', '출고 완료'])

        f_df = all_data.copy()
        if v_filter: f_df = f_df[f_df['공급업체명'].isin(v_filter)]
        if g_filter: f_df = f_df[f_df['분류구분'].isin(g_filter)]
        if s_filter: f_df = f_df[f_df['상태'].isin(s_filter)]

        # 주요 지표
        m1, m2, m3 = st.columns(3)
        m1.metric("대상 총계", f"{len(f_df):,} 건")
        
        completed = f_df[f_df['상태'] == '출고 완료']
        avg_tat = round(pd.to_numeric(completed['tat']).mean(), 1) if not completed.empty else 0.0
        m2.metric("평균 TAT", f"{avg_tat} 일")
        
        waiting = f_df[f_df['상태'] == '출고 대기']
        m3.metric("현재 대기", f"{len(waiting):,} 건")

        # 데이터 표 표시
        st.dataframe(f_df, use_container_width=True, hide_index=True)
    else:
        st.info("💡 데이터가 없습니다. 사이드바에서 마스터 정보를 먼저 갱신한 후, 입고 파일을 업로드해 주세요.")

except Exception as e:
    st.error(f"데이터 로드 중 오류: {e}")
