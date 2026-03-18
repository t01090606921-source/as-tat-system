import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
from datetime import datetime, timedelta

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (초고속 벌크 엔진)")

# [데이터 정제 함수]
def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].replace(" ", "").strip().upper()

def to_pure_date(val):
    try: return pd.to_datetime(val).date()
    except: return None

# --- 2. 사이드바 (DB 관리) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 DB 전체 수량 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("현재 저장된 데이터", f"{res.count if res.count is not None else 0:,} 건")
    
    st.divider()
    if "delete_mode" not in st.session_state: st.session_state.delete_mode = False
    if not st.session_state.delete_mode:
        if st.button("💣 데이터 전체 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True; st.rerun()
    else:
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정"):
                supabase.table("as_history").delete().neq("id", 0).execute()
                st.session_state.delete_mode = False; st.success("삭제 완료"); st.rerun()
        with c2:
            if st.button("❌ 취소"):
                st.session_state.delete_mode = False; st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0/1] 마스터 및 입고 (기존 효율적 로직 유지)
with tab0:
    st.subheader("📋 마스터 정보 등록")
    m_file = st.file_uploader("마스터 파일", type=['xlsx', 'csv'], key="m_v7")
    if m_file and st.button("🔄 마스터 로드"):
        m_df = pd.read_csv(m_file, encoding='cp949').fillna("") if m_file.name.endswith('.csv') else pd.read_excel(m_file).fillna("")
        st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {"업체": str(row.iloc[5]).strip(), "분류": str(row.iloc[10]).strip()} for _, row in m_df.iterrows()}
        st.success("✅ 마스터 로드 완료")

with tab1:
    st.subheader("📥 AS 입고")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="i_v7")
    if i_file and st.button("🚀 입고 시작"):
        if "master_lookup" not in st.session_state: st.error("마스터 로드 필요")
        else:
            try:
                for enc in ['utf-8-sig', 'cp949']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue
                as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs = []
                for _, row in as_in.iterrows():
                    mat_no = sanitize_code(row.iloc[3])
                    m_info = st.session_state.master_lookup.get(mat_no, {})
                    recs.append({
                        "압축코드": sanitize_code(row.iloc[7]), "자재번호": mat_no, "자재명": str(row.iloc[4]).strip(),
                        "규격": str(row.iloc[5]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"), "입고일": str(to_pure_date(row.iloc[1])), "상태": "출고 대기"
                    })
                    if len(recs) >= 200: supabase.table("as_history").insert(recs).execute(); recs = []
                if recs: supabase.table("as_history").insert(recs).execute()
                st.success("✅ 입고 완료")
            except Exception as e: st.error(f"오류: {e}")

# [TAB 2] 출고 처리 (초고속 벌크 매칭 엔진)
with tab2:
    st.subheader("📤 AS 출고 처리 (벌크 가속 엔진)")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="o_v7")
    if o_file and st.button("🚀 출고 반영 시작"):
        try:
            df_out = pd.read_excel(o_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            
            as_out['is_digitas'] = as_out.iloc[:, 15].astype(str).str.contains("주식회사디지타스")
            as_out = as_out.sort_values(by='is_digitas', ascending=False)

            ui_msg = st.empty()
            success_count = 0
            
            # [최적화] 전체 데이터를 한 번에 가져오는 대신, 이번 파일에 있는 압축코드만 골라서 '벌크 로드'
            unique_codes = as_out.iloc[:, 10].apply(sanitize_code).unique().tolist()
            db_data = {}
            for i in range(0, len(unique_codes), 500):
                batch = unique_codes[i:i+500]
                res = supabase.table("as_history").select("*").in_("압축코드", batch).neq("상태", "벤더 출고 완료").order("입고일").execute()
                for r in res.data:
                    c = sanitize_code(r['압축코드'])
                    if c not in db_data: db_data[c] = []
                    db_data[c].append(r)
            
            # 매칭 및 업데이트 리스트 생성
            updates = []
            for i, (idx, row) in enumerate(as_out.iterrows()):
                code = sanitize_code(row.iloc[10])
                out_date = str(to_pure_date(row.iloc[6]))
                dest = str(row.iloc[15]).strip()
                
                target_r = None
                if code in db_data:
                    candidates = db_data[code]
                    if dest == "주식회사디지타스":
                        # 디지타스 날짜가 없는 행 탐색
                        for r in candidates:
                            if not r.get('디지타스_출고일'):
                                target_r = r; break
                    else:
                        # 디지타스 날짜가 있는 행 우선 탐색
                        for r in candidates:
                            if r.get('디지타스_출고일') and not r.get('벤더_출고일'):
                                target_r = r; break
                        if not target_r: # 없으면 입고 대기건
                            for r in candidates:
                                if not r.get('디지타스_출고일') and not r.get('벤더_출고일'):
                                    target_r = r; break
                
                if target_r:
                    if dest == "주식회사디지타스":
                        upd = {"id": target_r['id'], "디지타스_출고일": out_date, "상태": "디지타스 출고"}
                        target_r['디지타스_출고일'] = out_date # 메모리 갱신
                    else:
                        upd = {"id": target_r['id'], "벤더_출고지": dest, "벤더_출고일": out_date, "상태": "벤더 출고 완료"}
                        candidates.remove(target_r) # 완료 건 제거
                    
                    updates.append(upd)
                    success_count += 1
                
                # [벌크 실행] 100건씩 묶어서 DB 업데이트 (속도 향상의 핵심)
                if len(updates) >= 100:
                    for u in updates:
                        id_val = u.pop('id')
                        supabase.table("as_history").update(u).eq("id", id_val).execute()
                    updates = []
                    ui_msg.info(f"⚡ 벌크 가속 처리 중... ({i+1}/{len(as_out)} 건)")

            # 남은 데이터 처리
            for u in updates:
                id_val = u.pop('id')
                supabase.table("as_history").update(u).eq("id", id_val).execute()

            ui_msg.success(f"✅ 총 {success_count}건이 초고속으로 반영되었습니다.")
        except Exception as e: st.error(f"오류: {e}")

# [TAB 3] 리포트 생성 (기간 필터링)
with tab3:
    st.subheader("📈 기간별 분석 리포트")
    c1, c2 = st.columns(2)
    with c1: s_d = st.date_input("조회 시작일", datetime.now() - timedelta(days=30))
    with c2: e_d = st.date_input("조회 종료일", datetime.now())

    if st.button("📊 선택 기간 리포트 생성"):
        all_d, offset = [], 0
        status = st.empty()
        while True:
            res = supabase.table("as_history").select("*").gte("입고일", str(s_d)).lte("입고일", str(e_d)).range(offset, offset+1000).order("입고일").execute()
            if not res.data: break
            all_d.extend(res.data); offset += len(res.data)
            status.info(f"📥 추출 중... ({offset:,}건)")
            if len(res.data) < 1000: break
        
        if all_d:
            df = pd.DataFrame(all_d)
            in_dt = pd.to_datetime(df['입고일'], errors='coerce')
            dg_dt = pd.to_datetime(df['디지타스_출고일'], errors='coerce')
            vn_dt = pd.to_datetime(df['벤더_출고일'], errors='coerce')
            df['TAT'] = (vn_dt - in_dt).dt.days
            df.loc[df['TAT'].isna(), 'TAT'] = (dg_dt - in_dt).dt.days
            
            df['입고일'] = in_dt.dt.strftime('%Y-%m-%d')
            df['디지타스_출고일'] = dg_dt.dt.strftime('%Y-%m-%d').fillna("-")
            df['벤더_출고일'] = vn_dt.dt.strftime('%Y-%m-%d').fillna("-")
            df['TAT'] = df['TAT'].fillna("-")
            
            cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df[cols].to_excel(wr, index=False)
            st.download_button(f"📥 {s_d}_{e_d}_리포트 다운로드", output.getvalue(), f"AS_Report_{s_d}_{e_d}.xlsx")
            st.dataframe(df[cols].head(50))
        else: st.warning("조회된 데이터가 없습니다.")
