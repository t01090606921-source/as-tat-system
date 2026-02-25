import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 (내부 자재명 추출 버전)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 (초기화) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("⚠️ DB 데이터 전량 삭제", type="primary", use_container_width=True):
        st.session_state.clear()
        try:
            supabase.table("as_history").delete().neq("id", -1).execute()
            st.success("데이터베이스 초기화 완료")
            st.rerun()
        except Exception as e:
            st.error(f"삭제 오류: {e}")

# --- 3. 메인 기능 ---
tab1, tab2, tab3 = st.tabs(["📥 고속 정밀 입고", "📤 개별 출고 처리", "📈 분석 리포트"])

with tab1:
    st.info("💡 입고 파일의 E열에 있는 '자재명'을 자동으로 가져옵니다.")
    i_file = st.file_uploader("AS 입고 엑셀 업로드", type=['xlsx'], key="in_up")

    if i_file and st.button("🚀 입고 시작"):
        try:
            # 엑셀 로드 및 빈 행 제거
            i_df = pd.read_excel(i_file).dropna(how='all').fillna("")
            
            # 'A/S 철거' 포함 행 추출 (1열 기준)
            as_in = i_df[i_df.iloc[:, 0].astype(str).str.contains('A/S 철거', na=False)].copy()
            total = len(as_in)
            
            if total == 0:
                st.warning("⚠️ 'A/S 철거' 데이터가 0건입니다. 1열(A열)을 확인하세요.")
            else:
                recs = []
                p_bar = st.progress(0)
                
                for i, (_, row) in enumerate(as_in.iterrows()):
                    try:
                        # 컬럼 위치 정의 (고정 인덱스 방식 + 안전장치)
                        # A(0):구분, B(1):입고일자, D(3):자재번호, E(4):자재명, F(5):규격, H(7):압축코드
                        if len(row) < 8: continue

                        # 날짜 변환
                        try:
                            dt = pd.to_datetime(row.iloc[1])
                            in_date = dt.strftime('%Y-%m-%d')
                        except:
                            in_date = "1900-01-01"

                        recs.append({
                            "압축코드": str(row.iloc[7]).strip(),
                            "자재번호": sanitize_code(row.iloc[3]),
                            "자재내역": str(row.iloc[4]).strip(), # E열에서 직접 가져옴
                            "규격": str(row.iloc[5]).strip(),
                            "상태": "출고 대기",
                            "공급업체명": "입고파일참조", # 마스터 미사용 시 기본값
                            "분류구분": "수리대상",      # 리포트 필터링을 위한 기본값 설정
                            "입고일": in_date
                        })
                        
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                            p_bar.progress((i+1)/total)
                    except: continue
                
                if recs:
                    supabase.table("as_history").insert(recs).execute()
                st.success(f"🎊 {total:,}건 입고 성공 (자재명 포함)!")
        except Exception as e:
            st.error(f"입고 실패: {e}")

with tab2:
    st.info("📤 출고 엑셀 업로드 (압축코드 기준 매칭)")
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_up")
    if out_file and st.button("🚀 출고 업데이트 시작"):
        try:
            df_out = pd.read_excel(out_file).dropna(how='all').fillna("")
            # D열(3): AS 카톤 박스 포함 행 찾기
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            
            if not as_out.empty:
                # G열(6): 출고일, K열(10): 압축코드
                as_out['clean_date'] = pd.to_datetime(as_out.iloc[:, 6]).dt.strftime('%Y-%m-%d')
                as_out['clean_code'] = as_out.iloc[:, 10].astype(str).str.strip()
                date_groups = as_out.groupby('clean_date')['clean_code'].apply(list).to_dict()
                
                for d, c in date_groups.items():
                    for j in range(0, len(c), 200):
                        supabase.table("as_history").update({"출고일": d, "상태": "출고 완료"}).in_("압축코드", c[j:j+200]).execute()
                st.success(f"✅ {len(as_out):,}건 출고 완료")
        except Exception as e:
            st.error(f"출고 실패: {e}")

with tab3:
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False
    
    if st.button("📈 데이터 분석 시작", use_container_width=True):
        res = supabase.table("as_history").select("*").execute()
        if res.data:
            df = pd.DataFrame(res.data)
            df['입고일'] = pd.to_datetime(df['입고일'])
            df['출고일'] = pd.to_datetime(df['출고일'])
            df.loc[df['입고일'] > df['출고일'], '출고일'] = pd.NaT
            df['TAT'] = (df['출고일'] - df['입고일']).dt.days
            
            cols = ['입고일자', '자재번호', '자재내역', '규격', '공급업체명', '압축코드', 'TAT']
            
            def make_bin(target_df):
                if target_df.empty: return None
                t = target_df.copy()
                t['입고일자'] = t['입고일'].dt.strftime('%Y-%m-%d')
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                    t.reindex(columns=cols).to_excel(wr, index=False)
                return out.getvalue()

            # 수리대상 분류 데이터만 필터링
            f_df = df[df['분류구분'] == '수리대상']
            st.session_state.bin_tat = make_bin(f_df[f_df['출고일'].notna()])
            st.session_state.bin_stay = make_bin(f_df[f_df['출고일'].isna()])
            st.session_state.bin_total = make_bin(f_df)
            st.session_state.data_ready = True
            st.rerun()

    if st.session_state.data_ready:
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 1. 완료 리포트", st.session_state.bin_tat, "1_TAT_Completed.xlsx")
        with c2: st.download_button("📥 2. 미출고 명단", st.session_state.bin_stay, "2_Not_Shipped.xlsx")
        with c3: st.download_button("📥 3. 전체 데이터", st.session_state.bin_total, "3_Total_Data.xlsx")
