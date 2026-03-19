import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time
from datetime import datetime

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템")

# [데이터 정제] 엑셀 원본 보존 (공백/특수문자 유지)
def preserve_raw(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).strip()

def to_pure_date(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return None
        return pd.to_datetime(val).date()
    except: return None

def smart_read_csv(file):
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc).fillna("")
        except: continue
    raise Exception("CSV 인코딩 오류")

# --- 2. 사이드바 (실시간 DB 상태 감시) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어 센터")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("DB 내 총 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    st.warning("데이터 정합성을 위해 입고 전 '전체 삭제'를 권장합니다.")
    if st.button("💣 DB 전체 데이터 초기화", type="primary", use_container_width=True):
        try:
            status = st.empty()
            while True:
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                status.text(f"🗑️ 삭제 진행 중...")
            st.success("✅ DB가 비워졌습니다."); time.sleep(1); st.rerun()
        except Exception as e: st.error(f"오류: {e}")

# --- 3. 메인 기능 탭 ---
tab1, tab2, tab3 = st.tabs(["📥 데이터 전량 입고", "📤 고속 출고 매칭", "📈 TAT 분석 리포트"])

# [TAB 1] 전량 입고 (실시간 추적 로직)
with tab1:
    st.subheader("📥 AS 전량 입고 (16,995건 대응)")
    st.info("💡 입고가 시작되면 브라우저를 닫거나 다른 탭으로 이동하지 마세요.")
    
    c1, c2 = st.columns(2)
    with c1:
        in_date_col = st.number_input("📅 입고일 열(A=1)", min_value=1, value=2) - 1
        in_code_col = st.number_input("🔑 압축코드 열(A=1)", min_value=1, value=8) - 1
    with c2:
        in_mat_col = st.number_input("📦 자재번호 열(A=1)", min_value=1, value=4) - 1
        in_name_col = st.number_input("📝 자재명 열(A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고용 CSV 파일 업로드", type=['csv'], key="final_in")
    
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        try:
            df = smart_read_csv(i_file)
            recs = []
            for _, row in df.iterrows():
                code = preserve_raw(row.iloc[in_code_col])
                if not code: continue
                recs.append({
                    "압축코드": code,
                    "자재번호": preserve_raw(row.iloc[in_mat_col]),
                    "자재명": str(row.iloc[in_name_col]).strip(),
                    "입고일": str(to_pure_date(row.iloc[in_date_col])),
                    "상태": "출고 대기"
                })
            
            total_count = len(recs)
            if total_count == 0:
                st.error("입고할 데이터가 없습니다. 열 번호를 확인하세요.")
            else:
                st.write(f"📋 전송 대기 데이터: **{total_count:,}** 건")
                prog_bar = st.progress(0)
                status_msg = st.empty()
                
                success_count = 0
                chunk_size = 200 # 안전한 전송을 위한 분할 단위
                
                for i in range(0, total_count, chunk_size):
                    chunk = recs[i:i + chunk_size]
                    supabase.table("as_history").upsert(chunk, on_conflict="압축코드").execute()
                    success_count += len(chunk)
                    
                    # 실시간 UI 업데이트
                    progress = min(success_count / total_count, 1.0)
                    prog_bar.progress(progress)
                    status_msg.success(f"⏳ 데이터 전송 중: {success_count:,} / {total_count:,} 완료")
                
                st.balloons()
                st.success(f"🏁 최종 완료! 총 {success_count:,}건이 DB에 안전하게 저장되었습니다.")
        except Exception as e:
            st.error(f"⚠️ 입고 실패: {e}")

# [TAB 2] 고속 출고 매칭 (메모리 대조 방식)
with tab2:
    st.subheader("📤 AS 고속 출고 매칭")
    o_file = st.file_uploader("출고용 CSV 파일 업로드", type=['csv'], key="final_out")
    
    if o_file and st.button("🚀 고속 매칭 시작", use_container_width=True):
        try:
            df_out = smart_read_csv(o_file)
            df_out['match_key'] = df_out.iloc[:, 10].apply(preserve_raw)
            
            # 1. DB 전체 데이터 로드
            st.info("🔄 DB에서 입고 데이터를 불러오는 중...")
            db_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+1000).execute()
                if not res.data: break
                db_data.extend(res.data); offset += 1000
            db_dict = {item['압축코드']: item['id'] for item in db_data}
            
            # 2. 매칭 및 업데이트 리스트 생성
            updates = []
            for _, row in df_out.iterrows():
                code = row['match_key']
                if code in db_dict:
                    dest = str(row.iloc[15]).strip()
                    out_dt = str(to_pure_date(row.iloc[6]))
                    updates.append({
                        "id": db_dict[code],
                        "디지타스_출고일": out_dt if "디지타스" in dest else None,
                        "벤더_출고일": out_dt if "디지타스" not in dest else None,
                        "벤더_출고지": dest,
                        "상태": "출고 완료"
                    })
            
            # 3. DB 일괄 반영
            if updates:
                status = st.empty()
                for i in range(0, len(updates), 200):
                    supabase.table("as_history").upsert(updates[i:i+200]).execute()
                    status.info(f"매칭 반영 중... {min(i+200, len(updates))}/{len(updates)}")
                st.success(f"✅ {len(updates):,}건 매칭 완료!")
            else:
                st.warning("매칭된 데이터가 없습니다. 압축코드를 확인하세요.")
        except Exception as e: st.error(f"출고 실패: {e}")

# [TAB 3] 리포트
with tab3:
    st.subheader("📈 TAT 분석 리포트")
    if st.button("📊 엑셀 리포트 생성", use_container_width=True):
        all_data, offset = [], 0
        while True:
            res = supabase.table("as_history").select("*").range(offset, offset+1000).order("입고일").execute()
            if not res.data: break
            all_data.extend(res.data); offset += 1000
        
        if all_data:
            df = pd.DataFrame(all_data)
            st.dataframe(df)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df.to_excel(wr, index=False)
            st.download_button("📥 엑셀 다운로드", output.getvalue(), "AS_TAT_Final_Report.xlsx")
