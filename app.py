import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time
import re

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase Secrets 설정을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (FIFO 정밀 매칭)")

# [정밀 정제 함수] 매칭 오류의 주범인 공백/특수문자 완벽 제거
def ultimate_sanitize(val, length=100):
    if pd.isna(val) or str(val).strip() == "": return ""
    s = str(val).strip().upper()
    s = "".join(s.split())
    s = re.sub(r'[^A-Z0-9]', '', s) # 영문, 숫자만 남김
    return s[:length]

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except: return None

def smart_read_csv(file):
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc).fillna("")
        except: continue
    raise Exception("CSV 읽기 실패")

# --- 2. 사이드바 (DB 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    if st.button("🔍 DB 실시간 수량 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if st.button("💣 DB 전체 데이터 초기화", type="primary", use_container_width=True):
        # 대량 데이터 삭제 시 타임아웃 방지를 위한 루프 삭제
        with st.spinner("데이터 초기화 중..."):
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
        st.success("✅ 초기화 완료"); st.rerun()

# --- 3. 메인 기능 탭 ---
tab1, tab2, tab3 = st.tabs(["📥 전량 입고", "📤 초정밀 출고 매칭", "📈 리포트"])

# [TAB 1] 전량 입고 (모든 이력 개별 생성)
with tab1:
    st.subheader("📥 AS 전량 입고 (중복 이력 허용)")
    c1, c2 = st.columns(2)
    with c1:
        date_idx = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        code_idx = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        mat_idx = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        name_idx = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'])
    if i_file and st.button("🚀 입고 시작", use_container_width=True):
        df = smart_read_csv(i_file)
        recs = []
        for _, row in df.iterrows():
            code = ultimate_sanitize(row.iloc[code_idx])
            if not code: continue
            recs.append({
                "압축코드": code,
                "입고일": to_pure_date(row.iloc[date_idx]),
                "자재번호": ultimate_sanitize(row.iloc[mat_idx]),
                "자재명": str(row.iloc[name_idx]).strip()[:200],
                "상태": "출고 대기"
            })
        
        # 중복 입고를 모두 개별 행으로 저장 (중요)
        prog = st.progress(0)
        for j in range(0, len(recs), 100):
            supabase.table("as_history").insert(recs[j:j+100]).execute()
            prog.progress(min((j+100)/len(recs), 1.0))
        st.success(f"✅ {len(recs):,}건 입고 완료")

# [TAB 2] 출고 매칭 (FIFO 로직 적용)
with tab2:
    st.subheader("📤 초정밀 출고 매칭 (FIFO 방식)")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'])
    if o_file and st.button("🚀 매칭 시작", use_container_width=True):
        df_out = smart_read_csv(o_file)
        df_out['m_key'] = df_out.iloc[:, 10].apply(ultimate_sanitize)
        df_out['o_date'] = df_out.iloc[:, 6].apply(to_pure_date)
        
        # 1. DB에서 출고 대기 중인 모든 데이터를 가져와서 정렬(FIFO 준비)
        with st.spinner("DB 분석 중..."):
            db_res, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드, 입고일").eq("상태", "출고 대기").range(offset, offset+999).execute()
                if not res.data: break
                db_res.extend(res.data); offset += 1000
            db_df = pd.DataFrame(db_res)
        
        if db_df.empty:
            st.error("매칭할 입고 데이터가 없습니다.")
        else:
            # 입고일 순으로 정렬 (먼저 들어온 것을 먼저 매칭하기 위함)
            db_df = db_df.sort_values(by='입고일')
            updates = []
            
            # 2. 매칭 루프 (효율화를 위해 압축코드별 그룹화 추천하나 여기선 정밀도 위해 루프 실행)
            for _, o_row in df_out.iterrows():
                code = o_row['m_key']
                out_dt = o_row['o_date']
                
                # 해당 코드의 입고 건 중 가장 빠른 날짜의 행 1개 선택
                mask = (db_df['압축코드'] == code) & (db_df['입고일'] <= out_dt)
                hit_indices = db_df.index[mask]
                
                if len(hit_indices) > 0:
                    target_idx = hit_indices[0] # FIFO: 가장 먼저 입고된 건
                    target_id = db_df.loc[target_idx, 'id']
                    
                    dest = str(o_row.iloc[15]).strip()
                    upd = {"id": int(target_id), "상태": "출고 완료"}
                    if "디지타스" in dest:
                        upd["디지타스_출고일"] = out_dt
                    else:
                        upd["벤더_출고일"] = out_dt
                        upd["벤더_출고지"] = dest
                    
                    updates.append(upd)
                    db_df.drop(target_idx, inplace=True) # 중복 매칭 방지

            # 3. 결과 업데이트
            if updates:
                prog_upd = st.progress(0)
                for k in range(0, len(updates), 50):
                    supabase.table("as_history").upsert(updates[k:k+50]).execute()
                    prog_upd.progress(min((k+50)/len(updates), 1.0))
                    time.sleep(0.01)
                st.success(f"✅ {len(updates):,}건 매칭 성공! (3.4만 건 타겟)")

# [TAB 3] 리포트
with tab3:
    if st.button("📊 최종 리포트 다운로드"):
        all_d, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").order("입고일").range(offset, offset+999).execute()
            if not res.data: break
            all_d.extend(res.data); offset += 1000
        df = pd.DataFrame(all_d)
        # 컬럼 순서 조정
        cols = ["입고일", "자재번호", "자재명", "압축코드", "디지타스_출고일", "벤더_출고일", "벤더_출고지", "상태"]
        df = df[[c for c in cols if c in df.columns]]
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
            df.to_excel(wr, index=False)
        st.download_button("📥 엑셀 파일 받기", output.getvalue(), "AS_TAT_Total_Report.xlsx")
