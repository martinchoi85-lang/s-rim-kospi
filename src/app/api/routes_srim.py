# src/app/api/routes_srim.py
from __future__ import annotations

from collections import Counter
from typing import Tuple
from fastapi import HTTPException
"""
S-RIM 조회 API (FastAPI)

원칙:
- 조회 전용(Read-only)
- DB 접근은 SQLAlchemy Session + text SQL로 단순화
- DDL 정합성 유지(srim_result 컬럼 기준)
- flags(jsonb) 기반 필터 제공

주의:
- 기존 업로드된 routes_srim.py에는 srim_result에 없는 컬럼(srim_price, spread 등) 참조가 있었음.
  이 파일은 현재 DDL/모델에 맞게 수정한 버전.
"""

from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db  # 공통 get_db 사용 (중복 제거)


router = APIRouter(prefix="/srim", tags=["srim"])

# -----------------------------------------
# (A) flags 기반 품질 분류 정책(서버측)
# -----------------------------------------
# - 학습/리서치용 "기본 정책"이며, 나중에 언제든 조정 가능
EXCLUDE_FLAG_KEYS = {
    # 계산 자체가 신뢰 불가/불완전한 경우에 가까운 것들(예시)
    "FLAG_MISSING_SHARES_OUT",
    "FLAG_MISSING_EQUITY",
    "FLAG_MISSING_NET_INCOME",
}

WARN_FLAG_KEYS = {
    # 계산은 되었으나 해석상 주의가 필요한 경우(예시)
    "FLAG_ROE_BELOW_R",
    "FLAG_ROE_NEGATIVE",
    "FLAG_NEGATIVE_RESIDUAL_CLAMPED",
}

# -----------------------------
# 공통 헬퍼
# -----------------------------
def latest_snapshot_id(db: Session) -> str:
    """snapshots에서 최신 snapshot_id를 가져온다."""
    row = db.execute(
        text("""
            select snapshot_id
            from snapshots
            order by as_of_date desc, created_at desc
            limit 1
        """)
    ).fetchone()
    if not row:
        raise ValueError("snapshots 테이블에 데이터가 없습니다.")
    return str(row[0])


def normalize_ticker(t: str) -> str:
    """ticker는 항상 6자리 문자열로 정규화."""
    return str(t).strip().zfill(6)


# -----------------------------
# Endpoints
# -----------------------------
@router.get("/snapshots")
def list_snapshots(db: Session = Depends(get_db)):
    """
    스냅샷 목록(최신순) 조회

    Streamlit UI에서 snapshot 선택 dropdown에 사용
    """
    rows = db.execute(
        text("""
            select snapshot_id, as_of_date, created_at, note
            from snapshots
            order by as_of_date desc, created_at desc
        """)
    ).mappings().all()
    return {"count": len(rows), "items": rows}


@router.get("/latest")
def get_latest(
    db: Session = Depends(get_db),
    only_calc_ready: bool = Query(True, description="fair_price/gap_pct가 있는 행만"),
    min_gap_pct: Optional[float] = Query(None),
    max_gap_pct: Optional[float] = Query(None),
    exclude_flags: Optional[List[str]] = Query(None, description="제외할 flags key 목록"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    sort: str = Query("gap_desc", description="gap_desc|gap_asc|fair_desc|fair_asc|roe_desc|roe_asc"),
):
    """
    최신 snapshot_id 기준 SRIM 결과 조회
    """
    sid = latest_snapshot_id(db)
    return get_snapshot(
        snapshot_id=sid,
        db=db,
        only_calc_ready=only_calc_ready,
        min_gap_pct=min_gap_pct,
        max_gap_pct=max_gap_pct,
        exclude_flags=exclude_flags,
        limit=limit,
        offset=offset,
        sort=sort,
    )


@router.get("/{snapshot_id}")
def get_snapshot(
    snapshot_id: str,
    db: Session = Depends(get_db),
    only_calc_ready: bool = Query(True),
    min_gap_pct: Optional[float] = Query(None),
    max_gap_pct: Optional[float] = Query(None),
    exclude_flags: Optional[List[str]] = Query(None),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    sort: str = Query("gap_desc"),
):
    """
    특정 snapshot_id 기준 SRIM 결과 조회

    추가(추천 고도화용):
    - roe_derived: net_income_parent / equity_parent
    - bps_derived: equity_parent / shares_out
    - pbr_derived: market_cap / equity_parent (가능한 경우)
    """

    # (1) 정렬 화이트리스트(SQL injection 방지)
    # - 파생 컬럼도 정렬 가능하도록 확장
    sort_map = {
        "gap_desc": "sr.gap_pct desc nulls last",
        "gap_asc": "sr.gap_pct asc nulls last",
        "fair_desc": "sr.fair_price desc nulls last",
        "fair_asc": "sr.fair_price asc nulls last",
        "roe_desc": "sr.roe desc nulls last",
        "roe_asc": "sr.roe asc nulls last",

        # 파생 ROE 정렬(추천에 유용)
        "roe_derived_desc": "roe_derived desc nulls last",
        "roe_derived_asc": "roe_derived asc nulls last",

        # 시총 정렬(대형주 위주 스크리닝 등에 유용)
        "mcap_desc": "ms.market_cap desc nulls last",
        "mcap_asc": "ms.market_cap asc nulls last",

        # PBR 정렬(낮을수록 저PBR) — equity/market_cap이 있는 경우만 의미
        "pbr_asc": "pbr_derived asc nulls last",
        "pbr_desc": "pbr_derived desc nulls last",
    }
    order_by = sort_map.get(sort, sort_map["gap_desc"])

    # (2) WHERE 동적 구성(파라미터 바인딩)
    where = ["sr.snapshot_id = :sid"]
    params: Dict[str, Any] = {"sid": snapshot_id, "limit": limit, "offset": offset}

    if only_calc_ready:
        where.append("sr.fair_price is not null")
        where.append("sr.gap_pct is not null")

    if min_gap_pct is not None:
        where.append("sr.gap_pct >= :min_gap")
        params["min_gap"] = float(min_gap_pct)

    if max_gap_pct is not None:
        where.append("sr.gap_pct <= :max_gap")
        params["max_gap"] = float(max_gap_pct)

    if exclude_flags:
        for i, f in enumerate(exclude_flags):
            key = f"exf_{i}"
            where.append(f"not (sr.flags ? :{key})")
            params[key] = str(f)

    # (3) 파생 컬럼(roe_derived, bps_derived, pbr_derived)을 SELECT에서 계산
    # - nullif로 0 나눗셈 방지
    sql = text(f"""
        select
          sr.snapshot_id,
          sr.ticker,
          t.name,

          ms.close_price as market_price,
          ms.market_cap,
          ms.shares_out,

          fs.equity_parent,
          fs.net_income_parent,

          -- 파생 지표(추천/학습용)
          (fs.net_income_parent / nullif(fs.equity_parent, 0)) as roe_derived,
          (fs.equity_parent / nullif(ms.shares_out, 0)) as bps_derived,
          (ms.market_cap / nullif(fs.equity_parent, 0)) as pbr_derived,

          sr.fair_price,
          sr.gap_pct,

          sr.bps,
          sr.roe,
          sr.r as discount_rate,
          sr.flags,
          sr.computed_at
        from srim_result sr
        left join tickers t
          on t.ticker = sr.ticker
        left join market_snapshot ms
          on ms.snapshot_id = sr.snapshot_id
         and ms.ticker = sr.ticker
        left join fundamental_snapshot fs
          on fs.snapshot_id = sr.snapshot_id
         and fs.ticker = sr.ticker
        where {" and ".join(where)}
        order by {order_by}
        limit :limit offset :offset
    """)

    rows = db.execute(sql, params).mappings().all()
    return {"snapshot_id": snapshot_id, "count": len(rows), "items": rows}


@router.get("/tickers/{ticker}/latest")
def get_ticker_latest(
    ticker: str,
    db: Session = Depends(get_db),
):
    """
    특정 ticker의 최신 SRIM 결과 1건

    - snapshot_id 최신 순으로 1개 반환
    """
    tk = normalize_ticker(ticker)

    row = db.execute(
        text("""
            select
              sr.snapshot_id,
              sr.ticker,
              t.name,
              ms.close_price as market_price,
              ms.market_cap,
              sr.fair_price,
              sr.gap_pct,
              sr.bps,
              sr.roe,
              sr.r as discount_rate,
              sr.flags,
              sr.computed_at
            from srim_result sr
            left join tickers t
              on t.ticker = sr.ticker
            left join market_snapshot ms
              on ms.snapshot_id = sr.snapshot_id
             and ms.ticker = sr.ticker
            where sr.ticker = :tk
            order by sr.snapshot_id desc
            limit 1
        """),
        {"tk": tk},
    ).mappings().first()

    return row


def classify_flags(flags: dict) -> Tuple[str, list[str]]:
    """
    flags(JSONB)를 보고 결과를 OK/WARN/EXCLUDE로 분류한다.

    반환:
      - quality: "OK" | "WARN" | "EXCLUDE"
      - reasons: 분류 근거 flag 목록

    설계 의도:
    - EXCLUDE: 아예 스크리너에서 기본적으로 제외할 케이스
    - WARN: 스크리너에는 포함할 수 있으나, UI에서 경고로 표시할 케이스
    - OK: 상대적으로 해석이 깔끔한 케이스
    """
    if not isinstance(flags, dict):
        return ("WARN", ["FLAG_INVALID_FLAGS_FORMAT"])

    reasons = []

    # 1) EXCLUDE 우선
    for k in EXCLUDE_FLAG_KEYS:
        if k in flags:
            reasons.append(k)
    if reasons:
        return ("EXCLUDE", reasons)

    # 2) WARN
    for k in WARN_FLAG_KEYS:
        if k in flags:
            reasons.append(k)
    if reasons:
        return ("WARN", reasons)

    # 3) OK
    return ("OK", [])


@router.get("/{snapshot_id}/flags")
def list_flags_for_snapshot(
    snapshot_id: str,
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=500),
):
    """
    특정 snapshot_id에서 등장하는 flags key들을 빈도순으로 반환.

    Streamlit의 '제외 flags' 옵션을 자동화하기 위한 API.
    """
    rows = db.execute(
        text("""
            select flags
            from srim_result
            where snapshot_id = :sid
        """),
        {"sid": snapshot_id},
    ).mappings().all()

    counter = Counter()
    for r in rows:
        flags = r.get("flags") or {}
        if isinstance(flags, dict):
            counter.update(flags.keys())

    items = [{"key": k, "count": c} for k, c in counter.most_common(limit)]
    return {"snapshot_id": snapshot_id, "count": len(items), "items": items}


@router.get("/{snapshot_id}/screen")
def screen_snapshot(
    snapshot_id: str,
    db: Session = Depends(get_db),
    # 스크리너 조건들(리서치/학습용)
    min_gap_pct: float = Query(0.0, description="gap_pct 최소"),
    only_positive_gap: bool = Query(True, description="저평가 후보만: gap_pct > 0"),
    exclude_quality: bool = Query(True, description="EXCLUDE 등급은 기본 제외"),
    warn_only: bool = Query(False, description="WARN만 보기(학습용)"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """
    리서치/학습용 스크리너 API.

    - 기본: gap_pct가 양수(저평가 후보) + min_gap_pct 이상
    - flags 기반으로 OK/WARN/EXCLUDE 분류
    - exclude_quality=True면 EXCLUDE는 제외
    """
    # 우선 snapshot 결과를 넉넉히 가져온 뒤(정렬 포함),
    # 서버에서 quality 분류와 필터링을 적용하는 구조.
    # (추후 성능이 필요하면 SQL에서 더 밀어넣는 최적화 가능)
    base = get_snapshot(
        snapshot_id=snapshot_id,
        db=db,
        only_calc_ready=True,    # 스크리너는 계산 성공만 기준
        min_gap_pct=None,        # 아래에서 파이썬 필터로 처리
        max_gap_pct=None,
        exclude_flags=None,
        limit=2000,              # 서버측에서 먼저 풀로 받고 필터
        offset=0,
        sort="gap_desc",
    )

    items = base["items"]

    screened = []
    for it in items:
        gap = it.get("gap_pct")
        if gap is None:
            continue

        # (1) min_gap_pct 필터
        if gap < float(min_gap_pct):
            continue

        # (2) 저평가 후보만 보기 옵션
        if only_positive_gap and gap <= 0:
            continue

        # (3) flags 기반 품질 분류
        flags = it.get("flags") or {}
        quality, reasons = classify_flags(flags)

        # (4) exclude_quality 옵션
        if exclude_quality and quality == "EXCLUDE":
            continue

        # (5) warn_only 옵션(학습용)
        if warn_only and quality != "WARN":
            continue

        # 결과에 품질/근거를 덧붙여 반환
        it2 = dict(it)
        it2["quality"] = quality
        it2["quality_reasons"] = reasons
        screened.append(it2)

    # 페이징
    sliced = screened[offset: offset + limit]

    # 간단한 통계도 함께 반환(학습에 도움)
    counts = Counter([x["quality"] for x in screened])
    return {
        "snapshot_id": snapshot_id,
        "total_after_filter": len(screened),
        "quality_counts": dict(counts),
        "items": sliced,
    }
        
    
@router.get("/{snapshot_id}/ticker/{ticker}")
def get_ticker_detail(
    snapshot_id: str,
    ticker: str,
    db: Session = Depends(get_db),
):
    """
    학습/리서치용 종목 상세 API

    한 번의 호출로 아래를 모두 반환:
    - tickers(종목명/시장/섹터)
    - market_snapshot(가격/시총/주식수)
    - fundamental_snapshot(지배주주지분/지배주주순이익 + data_quality)
    - srim_result(계산 결과 + flags)
    - 파생값(학습용): bps_derived, roe_derived
    """
    tk = normalize_ticker(ticker)

    row = db.execute(
        text("""
            select
              t.ticker,
              t.name,
              t.market,
              t.sector_name,

              ms.close_price,
              ms.market_cap,
              ms.shares_out,
              ms.treasury_shares,
              ms.float_shares,

              fs.fs_year,
              fs.report_code,
              fs.is_consolidated,
              fs.equity_parent,
              fs.net_income_parent,
              fs.data_quality,

              dr.rate as discount_rate_snapshot,

              sr.bps as bps_stored,
              sr.roe as roe_stored,
              sr.r as r_used,
              sr.fair_price,
              sr.gap_pct,
              sr.flags,
              sr.computed_at
            from tickers t
            left join market_snapshot ms
              on ms.snapshot_id = :sid and ms.ticker = t.ticker
            left join fundamental_snapshot fs
              on fs.snapshot_id = :sid and fs.ticker = t.ticker
            left join discount_rate_snapshot dr
              on dr.snapshot_id = :sid
            left join srim_result sr
              on sr.snapshot_id = :sid and sr.ticker = t.ticker
            where t.ticker = :tk
            limit 1
        """),
        {"sid": snapshot_id, "tk": tk},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Ticker not found")

    data = dict(row)

    # ------------------------------
    # 학습용 파생값 계산
    # - DB에 저장된 bps/roe가 없어도 원천값으로 계산해 이해를 돕는다.
    # ------------------------------
    equity = data.get("equity_parent")
    ni = data.get("net_income_parent")
    shares_out = data.get("shares_out")

    bps_derived = None
    roe_derived = None

    try:
        if equity is not None and shares_out not in (None, 0):
            bps_derived = float(equity) / float(shares_out)
        if ni is not None and equity not in (None, 0):
            roe_derived = float(ni) / float(equity)
    except Exception:
        # 숫자형 캐스팅 이슈가 있어도 상세 API가 죽지 않도록 방어
        pass

    data["bps_derived"] = bps_derived
    data["roe_derived"] = roe_derived

    return data