# src/app/api/routes_srim.py

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import SessionLocal  # SW 관점: 기존 세션 생성 방식 재사용

router = APIRouter()


def get_db():
    """SW 관점: FastAPI DI로 DB 세션 수명 관리"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/snapshots/{snapshot_id}/srim")
def get_srim_snapshot(
    snapshot_id: str,
    top_n: int = Query(100, ge=1, le=1000),
    sort: str = Query("gap_desc"),  # gap_desc | gap_asc | srim_desc | srim_asc | mcap_desc(추가 가능)
    db: Session = Depends(get_db),
):
    """
    S-RIM 결과 스냅샷 조회

    S-RIM 의미: “저평가/고평가 상위”를 빠르게 보기 위한 API
    SW 관점: sort 파라미터로 정렬 정책을 서버에서 통제
    """
    order_by = "gap_pct desc nulls last"
    if sort == "gap_asc":
        order_by = "gap_pct asc nulls last"
    elif sort == "srim_desc":
        order_by = "srim_price desc nulls last"
    elif sort == "srim_asc":
        order_by = "srim_price asc nulls last"

    rows = db.execute(
    text(f"""
        select
          r.ticker,
          t.name,
          m.close_price as market_price,
          r.fair_price,
          r.gap_pct,
          r.bps,
          r.roe,
          r.r as discount_rate,
          r.flags,
          r.computed_at
        from srim_result r
        left join tickers t
          on t.ticker = r.ticker
        left join market_snapshot m
          on m.snapshot_id = r.snapshot_id
         and m.ticker = r.ticker
        where r.snapshot_id = :sid
        order by {order_by}
        limit :n
        """),
        {"sid": snapshot_id, "n": top_n},
    ).mappings().all()

    return {"snapshot_id": snapshot_id, "rows": rows}


@router.get("/tickers/{ticker}/srim/latest")
def get_srim_latest(
    ticker: str,
    db: Session = Depends(get_db),
):
    """
    특정 종목의 최신 S-RIM 결과 1건

    S-RIM 의미: 개별 종목을 빠르게 조회
    SW 관점: snapshot_id를 최신 순으로 정렬해 1건 반환
    """
    tk = str(ticker).strip().zfill(6)  # SW 관점: ticker 정규화

    row = db.execute(
        text("""
            select
              r.snapshot_id,
              r.ticker,
              t.name,
              r.market_price,
              r.fair_price,
              r.gap_pct,
              r.bps,
              r.roe,
              r.spread,
              r.data_quality
            from srim_result r
            left join tickers t
              on t.ticker = r.ticker
            where r.ticker = :tk
            order by r.snapshot_id desc
            limit 1
        """),
        {"tk": tk},
    ).mappings().first()

    return row
