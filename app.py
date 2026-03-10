import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
from datetime import datetime

# --- [함수 정의: 데이터 정제 및 로드] ---
def sanitize_code(val):
    """자재번호 및 압축코드 정제"""
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].replace(" ", "").strip().upper()

def to_pure_date(val):
    """날짜 형식 변환"""
    try:
        return pd.to_datetime(val).date()
    except:
        return None

def load_data_file(file):
    """CSV 또는 Excel 파일 읽기 (인코딩 방어)"""
    if file.name.endswith(('.csv')):
        for enc in ['utf-8-sig', 'cp949', 'euc-kr']:
            try:
                file.seek(0)
                return pd.read_csv(file, encoding=enc).fillna("")
            except:
                continue
        return None
    else:
        try:
            return pd.read_excel(file).fillna("")
        except:
            return None

# --- 1. 시스템 설정 및 DB 접속 ---
st.set_page_config(page_title="AS TAT 시스템", layout="wide")

try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
    # 연결 테스트
    supabase.table("as_history").select("id").limit(1).execute()
except Exception as e:
    st.error("❌ Supabase 연결 실패. 대시보드 상태를 확인하거나 secrets 설정을 확인하세요.")
    st.stop()

st.title("📊 AS TAT 통합 관리 시스템")

# --- 2. 사이드바 (시스템 제어) ---
with st.sidebar:
    st.header("⚙️ 시스템 제어")
    if st.button("🔍 현재 DB 데이터 개수 확인", use_container_width=True):
        try:
            res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
            st.metric("저장된 데이터", f"{res.count if res.count is not None else 0:,} 건")
        except Exception as e:
            st.error(f"조회 오류: {e}")
    
    st.divider()
    
    if "delete_mode" not in st.session_state: 
        st.session_state.delete_mode = False

    if not st.session_state.delete_mode:
        if st.button("💣 DB 전체 데이터 삭제", use_container_width=True, type="primary"):
            st.session_state.delete_mode = True
            st.rerun()
    else:
        st.warning("⚠️ 데이터가 영구 삭제됩니다!")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 확정", use_container_width=True):
                supabase.table("as_history").delete().neq("id", 0).execute()
                st.session_state.delete_mode = False
                st.success("삭제 완료")
                st.rerun()
        with c2:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.delete_mode = False
                st.rerun()

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# [TAB 0] 마스터 관리
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    m_file = st.file_uploader("마스터 파일 업로드 (XLSX, CSV)", type=['xlsx', 'csv'], key="master_upload")
    
    if m_file and st.button("🔄 마스터 데이터 로드"):
        m_df = load_data_file(m_file)
        if m_df is not None:
            try:
                # 데이터 맵핑 (O컬럼은 15번째이므로 인덱스 14 사용)
                st.session_state.master_lookup = {
                    sanitize_code(row.iloc[0]): {
                        "업체": str(row.iloc[5]).strip(), 
                        "분류": str(row.iloc[10]).strip(),
                        "대상여부": str(row.iloc[14]).strip() if len(row) > 14 else ""
                    } for _, row in m_df.iterrows()
                }
                st.success(f"✅ 마스터 데이터 {len(st.session_state.master_lookup)}건 로드 완료 (대상여부 포함)")
            except Exception as e:
                st.error(f"데이터 매싱 중 오류 발생: {e}")
        else:
            st.error("파일을 읽을 수 없습니다.")

# [TAB 1] 입고 처리
with tab1:
    st.subheader("📥 AS 입고 프로세스")
    i_file = st.file_uploader("입고 CSV 업로드", type=['csv'], key="in_upload")
    
    if i_file and st.button("🚀 입고 데이터 반영"):
        if "master_lookup" not in st.session_state:
            st.warning("⚠️ 마스터 데이터를 먼저 로드해주세요.")
        else:
            i_df = load_data_file(i_file)
            if i_df is not None:
                try:
                    as_in = i_df[i_df.astype(str).apply(lambda x: "".join(x), axis=1).str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                    
                    recs = []
                    for _, row in as_in.iterrows():
                        mat_no = sanitize_code(row.iloc[3])
                        m_info = st.session_state.master_lookup.get(mat_no, {})
                        
                        recs.append({
                            "압축코드": sanitize_code(row.iloc[7]), 
                            "자재번호": mat_no,
                            "자재명": str(row.iloc[4]).strip(), 
                            "규격": str(row.iloc[5]).strip(),
                            "공급업체명": m_info.get("업체", "미등록"), 
                            "분류구분": m_info.get("분류", "수리대상"),
                            "대상여부": m_info.get("대상여부", ""), # 마스터에서 불러온 대상여부 추가
                            "입고일": str(to_pure_date(row.iloc[1])), 
                            "상태": "출고 대기"
                        })
                        
                        if len(recs) >= 200:
                            supabase.table("as_history").insert(recs).execute()
                            recs = []
                    
                    if recs:
                        supabase.table("as_history").insert(recs).execute()
                    st.success(f"✅ 총 {len(as_in)}건 입고 완료")
                except Exception as e:
                    st.error(f"처리 중 오류: {e}")

# [TAB 2] 출고 처리
with tab2:
    st.subheader("📤 AS 출고 처리 (순차 누적 반영)")
    o_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_upload")
    
    if o_file and st.button("🚀 출고 데이터 반영"):
        try:
            df_out = load_data_file(o_file)
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.replace(" ", "").str.contains('AS카톤박스', case=False)].copy()
            as_out['is_digitas'] = as_out.iloc[:, 15].astype(str).str.contains("주식회사디지타스")
            as_out = as_out.sort_values(by='is_digitas', ascending=False)

            db_res = supabase.table("as_history").select("*").neq("상태", "벤더 출고 완료").order("입고일").execute()
            db_data = {r['id']: r for r in db_res.data}
            
            success_count = 0
            for _, row in as_out.iterrows():
                code = sanitize_code(row.iloc[10])
                out_date = to_pure_date(row.iloc[6])
                dest = str(row.iloc[15]).strip()
                target_id = None
                
                if dest == "주식회사디지타스":
                    for rid, rdata in db_data.items():
                        if sanitize_code(rdata['압축코드']) == code and not rdata.get('디지타스_출고일'):
                            target_id = rid; break
                    if target_id:
                        upd = {"디지타스_출고일": str(out_date), "상태": "디지타스 출고"}
                        supabase.table("as_history").update(upd).eq("id", target_id).execute()
                        db_data[target_id].update(upd)
                        success_count += 1
                else:
                    for rid, rdata in db_data.items():
                        if sanitize_code(rdata['압축코드']) == code and not rdata.get('벤더_출고일'):
                            target_id = rid; break
                    if target_id:
                        upd = {"벤더_출고지": dest, "벤더_출고일": str(out_date), "상태": "벤더 출고 완료"}
                        supabase.table("as_history").update(upd).eq("id", target_id).execute()
                        del db_data[target_id]
                        success_count += 1

            st.success(f"✅ {success_count}건 출고 반영 완료")
        except Exception as e:
            st.error(f"출고 처리 중 오류: {e}")

# [TAB 3] 리포트 생성
with tab3:
    st.subheader("📈 AS TAT 분석 리포트")
    if st.button("📊 리포트 생성"):
        res = supabase.table("as_history").select("*").order("입고일").execute()
        df = pd.DataFrame(res.data)
        
        if not df.empty:
            in_d = pd.to_datetime(df['입고일'], errors='coerce')
            dg_d = pd.to_datetime(df['디지타스_출고일'], errors='coerce')
            vn_d = pd.to_datetime(df['벤더_출고일'], errors='coerce')
            
            df['TAT'] = (vn_d - in_d).dt.days
            df.loc[df['TAT'].isna(), 'TAT'] = (dg_d - in_d).dt.days
            
            df['입고일'] = in_d.dt.strftime('%Y-%m-%d')
            df['디지타스_출고일'] = dg_d.dt.strftime('%Y-%m-%d').fillna("-")
            df['벤더_출고일'] = vn_d.dt.strftime('%Y-%m-%d').fillna("-")
            df['벤더_출고지'] = df['벤더_출고지'].replace("", "-").fillna("-")
            df['대상여부'] = df['대상여부'].replace("", "-").fillna("-")
            df['TAT'] = df['TAT'].apply(lambda x: f"{int(x)}일" if pd.notna(x) else "-")
            
            # 컬럼 순서 조정: 분류구분과 디지타스_출고일 사이에 [대상여부] 추가
            cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '압축코드', '분류구분', 
                    '대상여부', '디지타스_출고일', '벤더_출고지', '벤더_출고일', 'TAT', '상태']
            
            st.dataframe(df[cols], use_container_width=True)
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as wr:
                df[cols].to_excel(wr, index=False, sheet_name='TAT_Report')
            st.download_button("📥 리포트 엑셀 다운로드", output.getvalue(), "AS_TAT_REPORT.xlsx")
        else:
            st.info("조회된 데이터가 없습니다.")
