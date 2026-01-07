import os
import json
import requests
import streamlit as st

# -----------------------------
# 기본 설정
# -----------------------------
st.set_page_config(page_title="S-RIM (KOSPI) Viewer", layout="wide")
st.title("S-RIM 기반 코스피 가격평가 (리서치/학습 모드)")

# -----------------------------
# API_BASE 설정(Secrets 없어도 동작)
# -----------------------------
def resolve_api_base() -> str:
    # 1) 환경변수 우선(로컬/배포 모두 편함)
    env = os.getenv("API_BASE")
    if env:
        return env

    # 2) secrets.toml이 있는 경우에만 사용(없으면 예외 -> 기본값)
    try:
        return st.secrets.get("API_BASE", "http://127.0.0.1:8000")
    except Exception:
        return "http://127.0.0.1:8000"

API_BASE = resolve_api_base()

@st.cache_data(ttl=300)
def api_get(path: str, params: dict | None = None):
    """FastAPI GET 호출 공통 함수(예외 발생 시 Streamlit이 에러를 보여줌)"""
    url = f"{API_BASE}{path}"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# -----------------------------
# flags -> 품질 분류(OK/WARN/EXCLUDE)
# - 서버에도 구현할 수 있으나, MVP에서는 Streamlit에서 분류해도 충분합니다.
# -----------------------------
EXCLUDE_FLAG_KEYS = {
    "FLAG_MISSING_SHARES_OUT",
    "FLAG_MISSING_EQUITY",
    "FLAG_MISSING_NET_INCOME",
}

WARN_FLAG_KEYS = {
    "FLAG_ROE_BELOW_R",
    "FLAG_ROE_NEGATIVE",
    "FLAG_NEGATIVE_RESIDUAL_CLAMPED",
}

def classify_quality(flags: dict) -> tuple[str, list[str]]:
    """
    flags(JSON)를 보고 품질 등급을 분류
    반환: (quality, reasons)
    """
    if not isinstance(flags, dict):
        return ("WARN", ["FLAG_INVALID_FLAGS_FORMAT"])

    reasons = [k for k in EXCLUDE_FLAG_KEYS if k in flags]
    if reasons:
        return ("EXCLUDE", reasons)

    reasons = [k for k in WARN_FLAG_KEYS if k in flags]
    if reasons:
        return ("WARN", reasons)

    return ("OK", [])

def render_quality_badge(quality: str) -> str:
    """
    Streamlit에서 배지처럼 보이도록 HTML span 생성
    - Streamlit 기본 컴포넌트는 배지 UI가 제한적이므로 HTML을 사용합니다.
    """
    # 색상은 지정하지만, 표 자체 색상 요구가 아니므로 UX 목적의 최소 사용입니다.
    if quality == "OK":
        bg, fg = "#E7F5EA", "#1B5E20"
        label = "OK"
    elif quality == "WARN":
        bg, fg = "#FFF4E5", "#8A4B00"
        label = "WARN"
    else:
        bg, fg = "#FDECEC", "#8E0000"
        label = "EXCLUDE"

    return f"""
    <span style="
        display:inline-block;
        padding:2px 10px;
        border-radius:999px;
        background:{bg};
        color:{fg};
        font-weight:700;
        font-size:12px;
        border:1px solid rgba(0,0,0,0.06);
    ">{label}</span>
    """

# -----------------------------
# flags(요약) 한글 설명 매핑
# - 여기서는 "대표적인 FLAG_*"만 한국어로 풀어쓰고,
#   그 외 키는 그대로 표시합니다.
# -----------------------------
FLAG_DESC_KO = {
    "FLAG_ROE_BELOW_R": "ROE가 요구수익률(r)보다 낮음(초과이익이 작거나 음수일 수 있어 해석 주의)",
    "FLAG_ROE_NEGATIVE": "ROE가 음수(적자/자본 구조 영향으로 밸류 해석 왜곡 가능)",
    "FLAG_NEGATIVE_RESIDUAL_CLAMPED": "음수 초과이익을 0으로 처리(방어적 정책 적용)",
    "FLAG_MISSING_SHARES_OUT": "발행주식수/유통주식수 정보 누락(계산 신뢰 낮음)",
    "FLAG_MISSING_EQUITY": "지배주주지분(또는 대체값) 누락(계산 신뢰 낮음)",
    "FLAG_MISSING_NET_INCOME": "지배주주순이익(또는 대체값) 누락(계산 신뢰 낮음)",
}

def summarize_flags_korean(flags: dict, max_items: int = 3) -> str:
    """
    flags를 'FLAG_*' 중심으로 한국어 요약 문자열로 변환.
    """
    if not isinstance(flags, dict):
        return ""

    # 1) FLAG_*만 뽑아서 요약 (내부 계산용 키들은 길고 가독성을 해침)
    flag_keys = [k for k in flags.keys() if str(k).startswith("FLAG_")]

    if not flag_keys:
        return ""

    # 2) 대표 flag만 max_items개까지 한국어 설명으로
    out = []
    for k in flag_keys[:max_items]:
        out.append(FLAG_DESC_KO.get(k, k))

    # 3) 더 많으면 “외 N개” 표시
    if len(flag_keys) > max_items:
        out.append(f"외 {len(flag_keys) - max_items}개")

    return " / ".join(out)

# -----------------------------
# 한국어 컬럼명 매핑
# -----------------------------
COL_KO = {
    "ticker": "티커",
    "name": "종목명",
    "market_price": "시장가",
    "fair_price": "이론가(S-RIM)",
    "gap_pct": "괴리율(%)",
    "roe": "ROE",
    "discount_rate": "요구수익률(r)",
    "quality": "품질",
    "flags_summary": "주의사항(요약)",
}

# -----------------------------
# Snapshot 목록 로드
# -----------------------------
snapshots_resp = api_get("/srim/snapshots")
snapshots = snapshots_resp.get("items", [])
snapshot_ids = [s.get("snapshot_id") for s in snapshots if s.get("snapshot_id")]

if not snapshot_ids:
    st.error("snapshots 데이터가 없습니다. ETL을 먼저 실행하세요.")
    st.stop()

# -----------------------------
# 사이드바 UI
# -----------------------------
st.sidebar.header("필터")
snapshot_choice = st.sidebar.selectbox("Snapshot 선택", options=snapshot_ids, index=0)

mode = st.sidebar.radio("모드", ["전체 조회", "스크리너(추천)"], index=0)

# 정렬(sort) 선택 UI 추가 (요청사항 1)
sort = st.sidebar.selectbox(
    "정렬 기준",
    options=[
        "gap_desc", "gap_asc",
        "fair_desc", "fair_asc",
        "roe_derived_desc", "roe_derived_asc",   # ✅ 추가
        "mcap_desc", "mcap_asc",                 # ✅ 추가
        "pbr_asc", "pbr_desc",                   # ✅ 추가
    ],
    index=0,
    help="추천 고도화를 위해 roe_derived, pbr(derived), 시총 정렬도 제공합니다."
)
only_ok = st.sidebar.checkbox("품질 OK만 보기(추천 후보군)", value=False)

# flags 옵션 자동 로드(해당 snapshot에 실제로 존재하는 key만)
flags_resp = api_get(f"/srim/{snapshot_choice}/flags", params={"limit": 200})
flag_options = [x.get("key") for x in flags_resp.get("items", []) if isinstance(x.get("key"), str)]

# -----------------------------
# 학습용 가이드(접기)
# -----------------------------
with st.expander("S-RIM 공식/해석 가이드(학습용)", expanded=False):
    st.markdown(
        """
**핵심 개념**
- BPS = 지배주주지분 / 발행주식수  
- ROE = 지배주주순이익 / 지배주주지분  
- 요구수익률 r = discount_rate_snapshot.rate (스냅샷마다 확정)

**잔여이익(Residual Income)**
- RI = (ROE - r) × BPS

**가치(개념)**
- fair_price = BPS + 미래 RI의 현재가치(PV)

**flags는 ‘정답’이 아니라 ‘주의 신호’**
- 모델 한계/데이터 품질/정책(clamp 등)을 명시적으로 드러내는 장치
        """
    )


def fmt_int(x):
    """천 단위 콤마(정수형 표시). None이면 빈 문자열."""
    if x is None:
        return ""
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)

def fmt_float2(x):
    """소수 2자리 표시. None이면 빈 문자열."""
    if x is None:
        return ""
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)

def fmt_pct2(x):
    """퍼센트(%) 소수 2자리. gap_pct가 이미 '퍼센트 단위'라면 그대로 % 표시."""
    if x is None:
        return ""
    try:
        return f"{float(x):,.2f}%"
    except Exception:
        return str(x)

def quality_label(q: str) -> str:
    """테이블에서 배지 느낌을 주기 위한 안정적 라벨(이모지)."""
    if q == "OK":
        return "✅ OK"
    if q == "WARN":
        return "⚠️ WARN"
    return "⛔ EXCLUDE"


# =========================================================
# 모드 1) 전체 조회
# =========================================================
def render_full_table():
    st.subheader(f"결과: {snapshot_choice}")

    only_calc_ready = st.sidebar.checkbox("계산 성공만 보기", value=True)
    min_gap = st.sidebar.number_input("min 괴리율(%)", value=0.0)
    max_gap = st.sidebar.number_input("max 괴리율(%)", value=9999.0)

    exclude_flags = st.sidebar.multiselect("제외 flags", options=flag_options, default=[])
    limit = st.sidebar.slider("표시 개수", 50, 1000, 200, 50)

    # ✅ 정렬 선택(sort) UI는 이미 sidebar에서 sort 변수로 받고 있다고 가정
    params = {
        "only_calc_ready": str(only_calc_ready).lower(),
        "min_gap_pct": min_gap,
        "max_gap_pct": max_gap,
        "limit": limit,
        "offset": 0,
        "sort": sort,
    }
    if exclude_flags:
        params["exclude_flags"] = exclude_flags

    data = api_get(f"/srim/{snapshot_choice}", params=params)
    items = data.get("items", [])

    st.caption(f"rows={len(items)} (정렬={sort}, limit={limit})")

    filtered_items = []
    for it in items:
        flags = it.get("flags") or {}
        if isinstance(flags, str):
            try:
                flags = json.loads(flags)
            except Exception:
                flags = {}

        q, _ = classify_quality(flags)
        it["_quality"] = q  # 임시 저장

        if only_ok and q != "OK":
            continue

        filtered_items.append(it)
        
    # -----------------------------
    # 테이블 표시용 행 구성(한글 컬럼명 + 숫자 포맷 + flags 한글 요약 + 품질 라벨)
    # -----------------------------
    table_rows = []
    ticker_by_row = []  # 행 인덱스 -> ticker 매핑(행 선택 시 사용)
    for it in filtered_items:
        flags = it.get("flags") or {}
        if isinstance(flags, str):
            try:
                flags = json.loads(flags)
            except Exception:
                flags = {}

        q, reasons = classify_quality(flags)
        flags_summary = summarize_flags_korean(flags, max_items=3)

        ticker = it.get("ticker")
        ticker_by_row.append(ticker)

        table_rows.append({
            "티커": it.get("ticker"),
            "종목명": it.get("name"),
            "시장가": fmt_int(it.get("market_price")),
            "이론가(S-RIM)": fmt_int(it.get("fair_price")),
            "괴리율(%)": fmt_pct2(it.get("gap_pct")),
            "ROE(파생)": fmt_float2(it.get("roe_derived")),     # ✅ 추가
            "PBR(파생)": fmt_float2(it.get("pbr_derived")),     # ✅ 추가
            "시총": fmt_int(it.get("market_cap")),              # ✅ 추가
            "품질": quality_label(it.get("_quality")),
            "주의사항(요약)": summarize_flags_korean(flags, max_items=3),
        })

    # -----------------------------
    # ✅ “테이블 클릭 → 상세 자동 업데이트”
    # - Streamlit 버전에 따라 selection 기능 지원이 다를 수 있어 try/fallback 처리
    # -----------------------------
    selected_ticker = None

    try:
        # Streamlit의 dataframe selection 기능(지원되는 버전에서 동작)
        event = st.dataframe(
            table_rows,
            use_container_width=True,
            height=520,
            selection_mode="single-row",
            on_select="rerun",
        )
        # 선택된 행 인덱스 얻기
        sel_rows = getattr(getattr(event, "selection", None), "rows", None)
        if sel_rows and len(sel_rows) > 0:
            idx = sel_rows[0]
            if 0 <= idx < len(ticker_by_row):
                selected_ticker = ticker_by_row[idx]
    except TypeError:
        # selection_mode/on_select 인자를 지원하지 않는 Streamlit 버전
        st.dataframe(table_rows, use_container_width=True, height=520)
    except Exception:
        # 기타 예외는 테이블 표시 자체는 유지
        st.dataframe(table_rows, use_container_width=True, height=520)

    # selection이 안 된 경우: 기존 selectbox로 fallback
    if not selected_ticker:
        tickers = [t for t in ticker_by_row if t]
        st.divider()
        st.info("표에서 행 클릭 선택이 지원되지 않는 환경이면 아래에서 티커를 선택하세요.")
        selected_ticker = st.selectbox("티커 선택", options=tickers) if tickers else None

    if not selected_ticker:
        return

    # -----------------------------
    # 상세 영역: /srim/{snapshot}/ticker/{ticker} 호출
    # -----------------------------
    st.divider()
    st.subheader("종목 상세(원천값 → 파생값 → 결과)")

    detail = api_get(f"/srim/{snapshot_choice}/ticker/{selected_ticker}")

    flags_detail = detail.get("flags") or {}
    if isinstance(flags_detail, str):
        try:
            flags_detail = json.loads(flags_detail)
        except Exception:
            flags_detail = {}

    q, reasons = classify_quality(flags_detail)

    # 배지(상세 헤더)
    st.markdown(f"품질 등급: {render_quality_badge(q)}", unsafe_allow_html=True)
    if reasons:
        ko_reasons = [FLAG_DESC_KO.get(r, r) for r in reasons]
        st.caption("분류 근거: " + " / ".join(ko_reasons))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 1) 원천 입력값")
        st.json({
            "종목": f"{detail.get('name')} ({detail.get('ticker')})",
            "시장": detail.get("market"),
            "섹터": detail.get("sector_name"),
            "시장가(close_price)": detail.get("close_price"),
            "시총(market_cap)": detail.get("market_cap"),
            "발행주식수(shares_out)": detail.get("shares_out"),
            "자기주식(treasury_shares)": detail.get("treasury_shares"),
            "유통주식수(float_shares)": detail.get("float_shares"),
        })

    with col2:
        st.markdown("### 2) 재무 입력값/품질")
        st.json({
            "fs_year": detail.get("fs_year"),
            "report_code": detail.get("report_code"),
            "is_consolidated": detail.get("is_consolidated"),
            "지배주주지분(equity_parent)": detail.get("equity_parent"),
            "지배주주순이익(net_income_parent)": detail.get("net_income_parent"),
            "data_quality": detail.get("data_quality") or {},
        })

    with col3:
        st.markdown("### 3) 파생값/결과")
        st.json({
            "bps(파생)": detail.get("bps_derived"),
            "roe(파생)": detail.get("roe_derived"),
            "스냅샷 r(discount_rate_snapshot)": detail.get("discount_rate_snapshot"),
            "결과 r(r_used)": detail.get("r_used"),
            "이론가(fair_price)": detail.get("fair_price"),
            "괴리율(gap_pct)": detail.get("gap_pct"),
            "computed_at": detail.get("computed_at"),
        })

    st.markdown("### flags(전체)")
    st.json(flags_detail or {})



# =========================================================
# 모드 2) 스크리너(추천)
# - 가능하면 서버 스크리너(/screen)를 사용하지만,
#   서버에 아직 구현 전이면 아래처럼 /srim/{snapshot} 결과로도 대체 가능
# =========================================================
def render_screen():
    st.subheader(f"스크리너: {snapshot_choice} (저평가 후보)")

    min_gap = st.sidebar.number_input("min 괴리율(%) (스크리너)", value=20.0)
    only_positive_gap = st.sidebar.checkbox("저평가 후보만(gap_pct > 0)", value=True)
    exclude_exclude = st.sidebar.checkbox("EXCLUDE 등급 제외", value=True)
    warn_only = st.sidebar.checkbox("WARN만 보기(학습용)", value=False)
    limit = st.sidebar.slider("표시 개수(스크리너)", 50, 1000, 200, 50)

    # 서버에 /screen이 구현되어 있다면 사용(추천)
    try:
        data = api_get(
            f"/srim/{snapshot_choice}/screen",
            params={
                "min_gap_pct": min_gap,
                "only_positive_gap": str(only_positive_gap).lower(),
                "exclude_quality": str(exclude_exclude).lower(),
                "warn_only": str(warn_only).lower(),
                "limit": limit,
                "offset": 0,
            },
        )
        items = data.get("items", [])
        qc = data.get("quality_counts", {})
        st.caption(f"필터 후 total={data.get('total_after_filter')} | OK={qc.get('OK',0)} WARN={qc.get('WARN',0)} EXCLUDE={qc.get('EXCLUDE',0)}")

    except Exception:
        # /screen이 없다면 fallback: 전체 조회로 받아서 Streamlit에서 필터/분류 수행
        base = api_get(
            f"/srim/{snapshot_choice}",
            params={
                "only_calc_ready": "true",
                "limit": 2000,
                "offset": 0,
                "sort": "gap_desc",
            },
        )
        items0 = base.get("items", [])
        items = []
        for it in items0:
            gap = it.get("gap_pct")
            if gap is None:
                continue
            if gap < float(min_gap):
                continue
            if only_positive_gap and gap <= 0:
                continue

            flags = it.get("flags") or {}
            if isinstance(flags, str):
                try:
                    flags = json.loads(flags)
                except Exception:
                    flags = {}

            q, _ = classify_quality(flags)
            if exclude_exclude and q == "EXCLUDE":
                continue
            if warn_only and q != "WARN":
                continue

            it2 = dict(it)
            it2["quality"] = q
            items.append(it2)

        st.caption(f"(fallback) 필터 후 total={len(items)}")

    # 테이블 표시(한글 컬럼 + 품질)
    rows = []
    for it in items[:limit]:
        flags = it.get("flags") or {}
        if isinstance(flags, str):
            try:
                flags = json.loads(flags)
            except Exception:
                flags = {}

        q, reasons = classify_quality(flags)
        rows.append({
            COL_KO["ticker"]: it.get("ticker"),
            COL_KO["name"]: it.get("name"),
            COL_KO["market_price"]: it.get("market_price"),
            COL_KO["fair_price"]: it.get("fair_price"),
            COL_KO["gap_pct"]: it.get("gap_pct"),
            COL_KO["quality"]: q,
            "품질 근거(요약)": " / ".join([FLAG_DESC_KO.get(r, r) for r in reasons]) if reasons else "",
        })

    st.dataframe(rows, use_container_width=True, height=520)

    st.divider()
    st.markdown(
        """
**해석 팁**
- **OK**: 비교적 해석이 깔끔한 편(모델 한계는 항상 존재)
- **WARN**: 계산은 되었으나 해석 주의(ROE<r, 음수 ROE, 음수 초과이익 클램프 등)
- **EXCLUDE**: 핵심 입력 누락 등으로 신뢰 낮음(기본 제외 권장)
        """
    )


# -----------------------------
# 실행
# -----------------------------
if mode == "전체 조회":
    render_full_table()
else:
    render_screen()
