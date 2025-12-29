from datetime import date
from sqlalchemy.orm import Session
import pandas as pd

from ..db import SessionLocal
from ..config import settings
from .sources_krx import fetch_kospi_universe
from .sources_dart import fetch_latest_annual_fundamentals
from .compute import compute_srim
from .load import upsert_snapshot, upsert_discount_rate

def snapshot_id_for(dt: date) -> str:
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"

def run(as_of: date | None = None):
    as_of = as_of or date.today()
    sid = snapshot_id_for(as_of)

    db: Session = SessionLocal()
    try:
        upsert_snapshot(db, sid, as_of, note="Quarterly SRIM snapshot")
        upsert_discount_rate(db, sid, as_of, settings.DEFAULT_DISCOUNT_RATE, source="manual")

        # 1) KOSPI 유니버스 + 시장데이터
        market_df = fetch_kospi_universe(as_of)  # DataFrame

        # 2) DART 재무(최신 연간)
        tickers = market_df["ticker"].tolist()
        fundamental_rows = fetch_latest_annual_fundamentals(tickers)

        fund_df = pd.DataFrame([r.__dict__ for r in fundamental_rows])

        # 3) 계산
        result_df = compute_srim(market_df, fund_df, settings.DEFAULT_DISCOUNT_RATE)

        # 4) DB 적재 (여기서는 골격)
        # - tickers upsert
        # - market_snapshot upsert
        # - fundamental_snapshot upsert
        # - srim_result upsert
        # TODO: 구현

        return {"snapshot_id": sid, "as_of": str(as_of), "rows": len(result_df)}

    finally:
        db.close()

if __name__ == "__main__":
    print(run())
