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
st.title("📊 AS TAT 통합 관리 시스템 (100% 입고 & 고속 매칭)")

# [데이터 정제] 양끝 공백만 제거, 중간 기호/공백은 엑셀 원본 그대로 보존
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
    raise Exception("파일 인코딩 오류 (CSV 형식을 확인하세요)")

# --- 2. 사이드바 (DB 관리 및 상태 확인) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 DB 상태 새로고침", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("현재 DB 총 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if st.button("💣 데이터 전체 삭제", type="primary", use_container_width=True):
        try:
            msg = st.empty()
            while True:
                # 대량 삭제 시 타임아웃 방지를 위해 1000건씩 끊어서 삭제
                fetch = supabase.table("as_history").select("id").limit(1000).execute()
                ids = [r['id'] for r in fetch.data]
                if not ids: break
                supabase.table("as_history").delete().in_("id", ids).execute()
                msg.warning(f"🗑️ 삭제 중... (현재 ID {len(ids)}개 처리)")
            st.success("✅ DB 초기화 완료"); time.sleep(1); st.rerun()
        except Exception as e: st.error(f"삭제 오류: {e}")

# --- 3. 메인 기능 탭 ---
tab1, tab2, tab3 = st.tabs(["📥 전량 입고", "📤 고속 출고 매칭", "📈 TAT 리포트"])

# [TAB 1] 전량 입고 (16,995건 유실 방지 로직)
with tab1:
    st.subheader("📥 AS 전량 입고 (원본 보존 모드)")
    st.info("💡 필터링 없이 파일의 모든 행을 입고합니다. H열(8번)에 데이터가 있다면 무조건 들어갑니다.")
    
    c1, c2 = st.columns(2)
    with c1:
        in_date_col = st.number_input("📅 입고일 열 번호 (A=1)", min_value=1, value=2) - 1
        in_code_col = st.number_input("🔑 압축코드 열 번호 (A=1)", min_value=1, value=8) - 1
    with c2:
        in_mat_col = st.number_input("📦 자재번호 열 번호 (A=1)", min_value=1, value=4) - 1
        in_name_col = st.number_input("📝 자재명 열 번호 (A=1)", min_value=1, value=5) - 1

    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_final_recheck")
    if i_file and st.button("🚀 데이터 유실 없이 입고 시작", use_container_width=True):
        try:
            i_df = smart_read_csv(i_file)
            recs = []
            
            for idx, row in i_df.iterrows():
                # 원본 텍스트 그대로 사용 (공백 제거 로직 제외)
                code = preserve_raw(row.iloc[in_code_col])
                if not code: continue # 압축코드가 아예 없는 행만 제외
                
                recs.append({
                    "압축코드": code,
                    "자재번호": preserve_raw(row.iloc[in_mat_col]),
                    "자재명": str(row.iloc[in_name_col]).strip(),
                    "입고일": str(to_pure_date(row.iloc[in_date_col])),
                    "상태": "출고 대기"
                })
            
            # DB 전송 (Batch 500)
            bar = st.progress(0); msg = st.empty()
            for i in range(0, len(recs), 500):
                supabase.table("as_history").upsert(recs[i:i+500], on_conflict="압축코드").execute()
                bar.progress(min((i + 500) / len(recs), 1.0))
                msg.info(f"진행 중: {min(i+500, len(recs))}/{len(recs)}")
            
            st.success(f"✅ 총 {len(recs):,}건 입고 완료! (파일 전체 행: {len(i_df):,}건)")
        except Exception as e: st.error(f"입고 오류: {e}")

# [TAB 2] 고속 출고 매칭 (메모리 대조 방식)
with tab2:
    st.subheader("📤 AS 고속 출고 매칭 (최적화 버전)")
    st.info("💡 7,000건 대량 매칭 시에도 속도 지연 없이 메모리에서 즉시 대조합니다.")
    o_file = st.file_uploader("출고 CSV 업로드", type=['csv'], key="out_final_recheck")
    
    if o_file and st.button("🚀 고속 매칭 시작", use_container_width=True):
        try:
            df_out = smart_read_csv(o_file)
            # 출고 압축코드 원본 보존 (K열=11번 기준)
            df_out['match_key'] = df_out.iloc[:, 10].apply(preserve_raw)
            
            # 1. DB 전체 로드 (매칭용 Dictionary 생성)
            st.info("🔄 DB에서 입고 데이터를 불러오는 중...")
            all_db, offset = [], 0
            while True:
                res = supabase.table("as_history").select("id, 압축코드").range(offset, offset+1000).execute()
                if not res.data: break
                all_db.extend(res.data); offset += 1000
            db_dict = {item['압축코드']: item['id'] for item in all_db}
            
            # 2. 매칭 수행
            updates, missing = [], []
            for _, row in df_out.iterrows():
                code = row['match_key']
                if code in db_dict:
                    dest = str(row.iloc[15]).strip()
                    out_date = str(to_pure_date(row.iloc[6]))
                    updates.append({
                        "id": db_dict[code],
                        "디지타스_출고일": out_date if "디지타스" in dest else None,
                        "벤더_출고일": out_date if "디지타스" not in dest else None,
                        "벤더_출고지": dest,
                        "상태": "출고 완료"
                    })
                elif code != "":
                    missing.append(code)
            
            # 3. DB 일괄 업데이트
            if updates:
                msg = st.empty()
                for i in range(0, len(updates), 200):
                    supabase.table("as_history").upsert(updates[i:i+200]).execute()
                    msg.info(f"업데이트 중: {min(i+200, len(updates))}/{len(updates)}")
                st.success(f"✅ {len(updates):,}건 매칭 및 업데이트 완료!")
            
            if missing:
                st.warning(f"⚠️ 매칭 실패: {len(set(missing))}건 (DB에 입고 내역이 없음)")
                with st.expander("누락된 압축코드 리스트 보기"):
                    st.write(list(set(missing)))
        except Exception as e: st.error(f"출고 오류: {e}")

# [TAB 3] 리포트
with tab3:
    st.subheader("📈 TAT 분석 리포트")
    if st.button("📊 리포트 생성 및 엑셀 다운로드", use_container_width=True):
        try:
            all_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset+1000).order("입고일").execute()
                if not res.data: break
                all_data.extend(res.data); offset += 1000
            
            if all_data:
                df = pd.DataFrame(all_data)
                # 날짜 형식 정리
                for col in ['입고일', '디지타스_출고일', '벤더_출고일']:
                    df[col] = pd.to_datetime(df[col]).dt.date.fillna("-")
                
                st.dataframe(df)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                    df.to_excel(wr, index=False)
                st.download_button("📥 리포트 다운로드", output.getvalue(), "AS_TAT_Final.xlsx")
            else:
                st.warning("조회된 데이터가 없습니다.")
        except Exception as e: st.error(f"리포트 생성 오류: {e}")
