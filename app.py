import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io

# --- Supabase 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("⏱️ AS TAT 분석 시스템")

# --- 1. 사이드바: 마스터 데이터 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 설정")
    master_file = st.file_uploader("분류구분 마스터 업로드", type=['xlsx'])
    if master_file and st.button("마스터 DB 갱신"):
        try:
            m_df = pd.read_excel(master_file)
            m_data = [
                {
                    "자재번호": str(row.iloc[0]).strip(),
                    "공급업체명": str(row.iloc[5]).strip(),
                    "분류구분": str(row.iloc[10]).strip()
                } for _, row in m_df.iterrows()
            ]
            supabase.table("master_data").upsert(m_data).execute()
            st.success("✅ 마스터 정보 반영 완료!")
        except Exception as e:
            st.error(f"마스터 파일 오류: {e}")

# --- 2. 입고/출고 탭 ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 업로드", type=['xlsx'], key="in_up")
    if in_file and st.button("입고 데이터 처리"):
        with st.spinner("데이터 분석 및 업로드 중..."):
            try:
                df = pd.read_excel(in_file)
                # 'A/S 철거' 포함 행 추출
                as_in = df[df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
                
                if as_in.empty:
                    st.warning("⚠️ 'A/S 철거' 대상 데이터가 없습니다.")
                else:
                    # 마스터 데이터 미리 가져오기 (캐싱 효과)
                    master_res = supabase.table("master_data").select("*").execute()
                    m_lookup = {r['자재번호']: r for r in master_res.data}

                    new_recs = []
                    for _, row in as_in.iterrows():
                        key_val = str(row.iloc[7]).strip() # H열
                        if key_val == 'nan' or not key_val: continue
                        
                        mat_no = str(row.iloc[3]).strip() # D열
                        try:
                            in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                        except:
                            in_date = datetime.now().strftime('%Y-%m-%d')
                        
                        m_info = m_lookup.get(mat_no, {})
                        
                        new_recs.append({
                            "압축코드": key_val, 
                            "자재번호": mat_no, 
                            "규격": str(row.iloc[5]).strip(),
                            "공급업체명": m_info.get("공급업체명", "미등록"), 
                            "분류구분": m_info.get("분류구분", "미등록"), 
                            "입고일": in_date, 
                            "상태": "출고 대기"
                        })
                    
                    if new_recs:
                        # 일괄 삽입 (Bulk Insert) - 훨씬 빠름
                        supabase.table("as_history").insert(new_recs).execute()
                        st.success(f"✅ {len(new_recs)}건 입고 데이터가 정상 등록되었습니다.")
                        st.rerun()
            except Exception as e:
                st.error(f"업로드 중 오류 발생: {e}")

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("출고 매칭 시작"):
        with st.spinner("출고 일자 매칭 중..."):
            try:
                df = pd.read_excel(out_file)
                as_out = df[df.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
                
                match_count = 0
                for _, row in as_out.iterrows():
                    key_val = str(row.iloc[10]).strip() # K열
                    try:
                        out_date_dt = pd.to_datetime(row.iloc[6]) # G열
                    except: continue
                    
                    # FIFO 매칭
                    target = supabase.table("as_history").select("id, 입고일").match({"압축코드": key_val, "상태": "출고 대기"}).order("입고일").limit(1).execute()
                    
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
                
                st.success(f"✅ {match_count}건의 출고 매칭이 완료되었습니다.")
                st.rerun()
            except Exception as e:
                st.error(f"출고 처리 중 오류: {e}")

# --- 3. 리포트 영역 ---
st.divider()
try:
    res = supabase.table("as_history").select("*").order("입고일", desc=True).execute()
    all_data = pd.DataFrame(res.data)

    if not all_data.empty:
        col1, col2, col3 = st.columns(3)
        v_f = col1.multiselect("🏢 공급업체 필터", options=sorted(all_data['공급업체명'].unique()))
        c_f = col2.multiselect("📂 분류구분 필터", options=sorted(all_data['분류구분'].unique()))
        s_f = col3.multiselect("🚚 상태 필터", options=['출고 대기', '출고 완료'])

        f_df = all_data.copy()
        if v_f: f_df = f_df[f_df['공급업체명'].isin(v_f)]
        if c_f: f_df = f_df[f_df['분류구분'].isin(c_f)]
        if s_f: f_df = f_df[f_df['상태'].isin(s_f)]

        m1, m2, m3 = st.columns(3)
        m1.metric("전체 건수", f"{len(f_df):,} 건")
        
        fin_df = f_df[f_df['상태'] == '출고 완료']
        avg_tat = round(pd.to_numeric(fin_df['tat']).mean(), 1) if not fin_df.empty else 0.0
        m2.metric("평균 TAT", f"{avg_tat} 일")
        
        wait_cnt = len(f_df[f_df['상태'] == '출고 대기'])
        m3.metric("출고 대기", f"{wait_cnt:,} 건")

        st.dataframe(f_df, use_container_width=True, hide_index=True)
    else:
        st.info("현재 저장된 AS 이력이 없습니다. 입고 데이터를 먼저 업로드해 주세요.")
except Exception as e:
    st.error(f"데이터 조회 오류: {e}")
