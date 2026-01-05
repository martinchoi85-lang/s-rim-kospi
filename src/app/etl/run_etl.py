from __future__ import annotations

from datetime import date
from sqlalchemy.orm import Session
import pandas as pd

from app.db import SessionLocal
from app.config import settings

from app.etl.sources_krx import fetch_kospi_universe
from app.etl.sources_dart import fetch_latest_annual_fundamentals
from app.etl.load import (
    upsert_snapshot,
    upsert_discount_rate,
    upsert_tickers,
    upsert_market_snapshot,
    upsert_fundamental_snapshot,
)

def snapshot_id_for(dt: date) -> str:
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"

def run(as_of: date, dart_limit: int | None = None):
    sid = snapshot_id_for(as_of)

    db: Session = SessionLocal()
    try:
        upsert_snapshot(db, sid, as_of, note="Quarterly snapshot (KOSPI) - stage1+2")
        upsert_discount_rate(db, sid, as_of, float(settings.DEFAULT_DISCOUNT_RATE), source="manual")

        # 1) Market data
        market_df = fetch_kospi_universe(as_of)
        tickers_df = market_df[["ticker", "name", "market"]].drop_duplicates()
        upsert_tickers(db, tickers_df)
        upsert_market_snapshot(db, sid, market_df)

        # 2) Fundamentals (DART)
        if not settings.DART_API_KEY:
            raise RuntimeError("DART_API_KEY is not set in .env")

        tickers_raw = market_df["ticker"].tolist()
        tickers = [str(x).strip().zfill(6) for x in tickers_raw]
        ticker_to_name = {str(t).strip().zfill(6): n for t, n in zip(tickers_raw, market_df["name"].tolist())}

        fundamental_rows = fetch_latest_annual_fundamentals(
            api_key=settings.DART_API_KEY,
            tickers=tickers,
            ticker_to_name=ticker_to_name,
            max_companies=dart_limit
        )


        fund_df = pd.DataFrame([r.__dict__ for r in fundamental_rows])
        upsert_fundamental_snapshot(db, sid, fund_df)

        return {"snapshot_id": sid, "as_of": str(as_of), "market_rows": int(len(market_df)), "fund_rows": int(len(fund_df))}

    finally:
        db.close()


if __name__ == "__main__":
    import sys
    # 사용법:
    # python -m app.etl.run_etl YYYY-MM-DD [--limit N]
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m app.etl.run_etl YYYY-MM-DD [--limit N]")

    as_of = date.fromisoformat(sys.argv[1])
    dart_limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        dart_limit = int(sys.argv[idx + 1])

    print(run(as_of, dart_limit=dart_limit))
