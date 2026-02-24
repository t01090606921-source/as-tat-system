import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (출고 일자 정밀 보정)")

def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (관리 기능) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        msg = st.empty()
        while True:
            res = supabase.table("as_history").select("id").limit(1000).execute()
            if not res.data: break
            ids = [i['id'] for i in res.data]
            supabase.table("as_history").delete().in_("id", ids).execute()
            msg.warning(f"🗑️ 삭제 진행 중... (ID: {ids[-1]})")
        st.success("초기화 완료")
        st.rerun()

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 마스터와 입고 파일을 함께 올려주세요.")
    col1, col2 = st.columns(2)
    with col1: m_file = st.file_uploader("1. 마스터 엑셀", type=['xlsx'], key="m_up")
    with col2: i_file = st.file_uploader("2. AS 입고 엑셀", type=['xlsx'], key="i_up")

    if m_file and i_file and st.button("🚀 매칭 및 입고 시작"):
        prog_bar = st.progress(0)
        # (중략: 이전과 동일한 고속 입고 로직)
        m_df = pd.read_excel(m_file, dtype=str)
        m_lookup = {sanitize_code(row.iloc[0]): {"공급업체명": str(row.iloc[5]).strip(), "분류구분": str(row.iloc[10]).strip()} for _, row in m_df.iterrows() if sanitize_code(row.iloc[0])}
        
        i_df = pd.read_excel(i_file, dtype=str)
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        
        recs, total = [], len(as_in)
        for i, (_, row) in enumerate(as_in.iterrows()):
            cur_mat = sanitize_code(row.iloc[3])
            m_info = m_lookup.get(cur_mat)
            recs.append({
                "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                "자재번호": cur_mat, "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                "상태": "출고 대기", "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록", "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) >= 200:
                supabase.table("as_history").insert(recs).execute()
                recs = []
                prog_bar.progress((i+1)/total)
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("입고 완료")

with tab2:
    st.info("💡 출고 엑셀의 각 행에 적힌 날짜를 압축코드별로 각각 매칭하여 업데이트합니다.")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    
    if out_file and st.button("🚀 개별 출고 업데이트 시작"):
        df_out = pd.read_excel(out_file, dtype=str)
        # 'AS 카톤 박스'가 포함된 행만 필터링 (D열 = index 3)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            total_out = len(as_out)
            out_prog = st.progress(0)
            status_txt = st.empty()
            
            # 성능을 위해 50건씩 묶어서 날짜별 업데이트
            # 동일한 날짜를 가진 압축코드끼리 그룹화하여 처리
            as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
            as_out['clean_code'] = as_out.iloc[:, 10].str.strip() # K열 압축코드
            
            # 날짜별로 그룹화하여 DB 업데이트 횟수 최소화
            date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
            
            processed_count = 0
            for out_date, codes in date_groups.items():
                # 한 번에 너무 많은 코드를 업데이트하면 오류가 날 수 있으므로 200개씩 분할
                for j in range(0, len(codes), 200):
                    batch_codes = codes[j:j+200]
                    supabase.table("as_history").update({
                        "출고일": out_date, 
                        "상태": "출고 완료"
                    }).in_("압축코드", batch_codes).execute()
                    
                    processed_count += len(batch_codes)
                    ratio = processed_count / total_out
                    out_prog.progress(ratio)
                    status_txt.info(f"🚚 날짜별 매칭 중: {out_date}자 데이터 처리 중... ({processed_count}/{total_out})")
            
            st.success(f"✅ 총 {total_out:,}건의 출고 일자가 개별적으로 업데이트되었습니다.")
        else:
            st.error("출고 대상('AS 카톤 박스') 데이터가 없습니다.")

with tab3:
    if st.button("📊 TAT 분석 리포트 생성"):
        df_raw_list = []
        last_id = -1
        load_msg = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            df_raw_list.extend(res.data)
            last_id = res.data[-1]['id']
            load_msg.info(f"📥 리포트 데이터 수집 중... ({len(df_raw_list):,}건)")
        
        if df_raw_list:
            df_final = pd.DataFrame(df_raw_list)
            # 분류구분에 '수리대상'이 포함되고, 출고일이 있는 데이터만 필터링
            df_rep = df_final[
                (df_final['분류구분'].str.contains('수리대상', na=False)) & 
                (df_final['출고일'].notna())
            ].copy()
            
            if not df_rep.empty:
                df_rep['입고일'] = pd.to_datetime(df_rep['입고일'])
                df_rep['출고일'] = pd.to_datetime(df_rep['출고일'])
                df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 TAT 리포트 다운로드", output.getvalue(), "AS_TAT_Final_Report.xlsx")
                st.write(df_rep[['입고일', '출고일', 'TAT']].head(20)) # 상위 20건 미리보기로 날짜 확인
            else:
                st.warning("출고 완료된 '수리대상' 데이터가 없습니다.")
