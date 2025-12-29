from __future__ import annotations

from datetime import date, datetime
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.config import settings

from app.etl.sources_krx import fetch_kospi_universe
from app.etl.load import upsert_snapshot, upsert_discount_rate, upsert_tickers, upsert_market_snapshot


def snapshot_id_for(dt: date) -> str:
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"


def run(as_of: date):
    """
    1) 스냅샷 메타 생성
    2) r 저장
    3) pykrx로 코스피 시장데이터 수집
    4) tickers / market_snapshot 업서트
    """
    sid = snapshot_id_for(as_of)

    db: Session = SessionLocal()
    try:
        upsert_snapshot(db, sid, as_of, note="Quarterly market snapshot (KOSPI) - stage1")
        upsert_discount_rate(db, sid, as_of, float(settings.DEFAULT_DISCOUNT_RATE), source="manual")

        market_df = fetch_kospi_universe(as_of)

        # tickers 업서트
        tickers_df = market_df[["ticker", "name", "market"]].drop_duplicates()
        upsert_tickers(db, tickers_df)

        # market_snapshot 업서트
        upsert_market_snapshot(db, sid, market_df)

        return {"snapshot_id": sid, "as_of": str(as_of), "rows": int(len(market_df))}

    finally:
        db.close()


if __name__ == "__main__":
    # 수동 기준일: YYYY-MM-DD 입력
    # 예: python -m app.etl.run_etl 2025-12-29
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m app.etl.run_etl YYYY-MM-DD")

    as_of = date.fromisoformat(sys.argv[1])
    print(run(as_of))
