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
st.title("📊 AS TAT 통합 관리 (오류 무력화 버전)")

def sanitize_code(val):
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (초기화 전용) ---
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
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 고속 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 마스터 파일과 입고 파일을 함께 올려주세요. 메모리에서 즉시 매칭하여 입고합니다.")
    
    col1, col2 = st.columns(2)
    with col1:
        m_file_in = st.file_uploader("1. 자재 마스터 엑셀 선택", type=['xlsx'], key="m_up")
    with col2:
        in_file_in = st.file_uploader("2. AS 입고 엑셀 선택", type=['xlsx'], key="i_up")

    if m_file_in and in_file_in and st.button("🚀 매칭 및 입고 시작"):
        prog_msg = st.empty()
        prog_bar = st.progress(0)
        status_txt = st.empty()

        # [1단계] 마스터 읽기 (메모리 로딩)
        prog_msg.warning("⏳ 1/3단계: 마스터 파일을 읽고 있습니다...")
        m_df = pd.read_excel(m_file_in, dtype=str)
        # 자재번호(A열), 공급업체명(F열), 분류구분(K열) 맵핑
        m_lookup = {}
        for _, row in m_df.iterrows():
            mat_id = sanitize_code(row.iloc[0])
            if mat_id:
                m_lookup[mat_id] = {
                    "공급업체명": str(row.iloc[5]).strip() if len(row) > 5 else "정보누락",
                    "분류구분": str(row.iloc[10]).strip() if len(row) > 10 else "정보누락"
                }

        # [2단계] 입고 파일 분석
        prog_msg.warning("⏳ 2/3단계: 입고 파일을 분석하고 마스터와 대조 중입니다...")
        i_df = pd.read_excel(in_file_in, dtype=str)
        as_in = i_df[i_df.iloc[:, 0].fillna('').str.contains('A/S 철거', na=False)].copy()
        total_rows = len(as_in)

        if total_rows > 0:
            prog_msg.info(f"✅ 3/3단계: 매칭 완료! {total_rows:,}건을 DB에 저장합니다.")
            
            recs = []
            for i, (_, row) in enumerate(as_in.iterrows()):
                cur_mat = sanitize_code(row.iloc[3]) # D열 자재번호
                m_info = m_lookup.get(cur_mat)
                
                recs.append({
                    "압축코드": str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else "",
                    "자재번호": cur_mat,
                    "규격": str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else "",
                    "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
                
                # 200건 단위 전송 (안정성 확보)
                if len(recs) >= 200:
                    try:
                        supabase.table("as_history").insert(recs).execute()
                    except:
                        time.sleep(1)
                        supabase.table("as_history").insert(recs).execute()
                    recs = []
                    ratio = (i + 1) / total_rows
                    prog_bar.progress(ratio)
                    status_txt.markdown(f"**🚚 입고 진행 중:** {i+1:,} / {total_rows:,} 건 ({ratio*100:.1f}%)")

            if recs:
                supabase.table("as_history").insert(recs).execute()
                prog_bar.progress(1.0)
            
            prog_msg.success(f"🎊 완료! {total_rows:,}건 입고가 끝났습니다.")
        else:
            st.error("입고 대상('A/S 철거') 데이터가 없습니다.")

with tab2:
    # 출고 로직 (동일)
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'])
    if out_file and st.button("🚀 출고 업데이트 실행"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].fillna('').str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = as_out.iloc[:, 10].dropna().tolist()
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            out_p = st.progress(0)
            for i in range(0, len(out_keys), 500):
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", out_keys[i:i+500]).execute()
                out_p.progress(min((i+500)/len(out_keys), 1.0))
            st.success("출고 처리 완료")

with tab3:
    if st.button("📊 TAT 분석 리포트 생성"):
        # 분석 리포트 생성 시에만 DB 데이터를 가져옵니다.
        df_raw = []
        last_id = -1
        st.write("📥 리포트 데이터 수집 중...")
        while True:
            res = supabase.table("as_history").select("*").gt("id", last_id).order("id").limit(1000).execute()
            if not res.data: break
            df_raw.extend(res.data)
            last_id = res.data[-1]['id']
        
        if df_raw:
            df_final = pd.DataFrame(df_raw)
            df_rep = df_final[df_final['분류구분'].str.contains('수리대상', na=False)].copy()
            st.metric("수리대상 건수", f"{len(df_rep):,} 건")
            if not df_rep.empty:
                df_rep['TAT'] = (pd.to_datetime(df_rep['출고일']) - pd.to_datetime(df_rep['입고일'])).dt.days
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_rep[['입고일', '출고일', '자재번호', '규격', '공급업체명', '압축코드', 'TAT']].to_excel(writer, index=False)
                st.download_button("📥 엑셀 다운로드", output.getvalue(), "AS_TAT_Final.xlsx")
