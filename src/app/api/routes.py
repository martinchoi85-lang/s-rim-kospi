from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from ..db import get_db
from ..models import SRimResult, MarketSnapshot, Ticker

router = APIRouter()

@router.get("/snapshots/{snapshot_id}/srim")
def list_srim(
    snapshot_id: str,
    top_n: int | None = Query(default=None, ge=1, le=500),
    only_kospi: bool = True,
    db: Session = Depends(get_db),
):
    """
    - top_n: 상위 N(기본: 전체)
    - 정렬: gap_pct desc
    """
    q = (
        db.query(
            Ticker.ticker,
            Ticker.name,
            Ticker.sector_name,
            SRimResult.fair_price,
            SRimResult.gap_pct,
            SRimResult.roe,
            SRimResult.bps,
            MarketSnapshot.close_price,
            MarketSnapshot.market_cap,
            SRimResult.flags,
        )
        .join(SRimResult, SRimResult.ticker == Ticker.ticker)
        .join(MarketSnapshot, (MarketSnapshot.ticker == Ticker.ticker) & (MarketSnapshot.snapshot_id == snapshot_id))
        .filter(SRimResult.snapshot_id == snapshot_id)
        .order_by(SRimResult.gap_pct.desc().nullslast())
    )

    if top_n:
        q = q.limit(top_n)

    rows = q.all()
    return [
        {
            "ticker": r[0],
            "name": r[1],
            "sector": r[2],
            "fair_price": float(r[3]) if r[3] is not None else None,
            "gap_pct": float(r[4]) if r[4] is not None else None,
            "roe": float(r[5]) if r[5] is not None else None,
            "bps": float(r[6]) if r[6] is not None else None,
            "close_price": float(r[7]) if r[7] is not None else None,
            "market_cap": float(r[8]) if r[8] is not None else None,
            "flags": r[9] or {},
        }
        for r in rows
    ]
