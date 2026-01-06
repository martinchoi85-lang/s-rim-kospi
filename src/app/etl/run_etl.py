from __future__ import annotations

from datetime import date
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd

from app.db import SessionLocal
from app.config import settings

from app.etl.sources_krx import fetch_kospi_universe
from app.etl.sources_dart import (
    fetch_latest_annual_fundamentals,
    load_corpcode_df,
    fetch_shares_out_for_tickers
)
from app.etl.load import (
    upsert_snapshot,
    upsert_discount_rate,
    upsert_tickers,
    upsert_market_snapshot,
    upsert_fundamental_snapshot,
)
from app.etl.stage3_srim import run_stage3_srim


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

        # fundamental_rows에서 성공한 것만 모아서
        preferred_year_report = {}
        for r in fundamental_rows:
            if r.fs_year and r.report_code:
                preferred_year_report[str(r.ticker).zfill(6)] = (int(r.fs_year), str(r.report_code))

        corp_df = load_corpcode_df(settings.DART_API_KEY)

        # 시장 전체 958개를 다 해도 되지만, 우선은 테스트로 limit 구간만 하려면 tickers[:max_companies]를 사용
        shares_target = tickers[:dart_limit] if dart_limit else tickers
        shares_map = fetch_shares_out_for_tickers(
            api_key=settings.DART_API_KEY,
            tickers=shares_target,  # tickers는 이미 6자리 문자열로 정규화된 리스트여야 함
            corp_df=corp_df,
            ticker_to_name=ticker_to_name,
            preferred_year_report=preferred_year_report,
        )

        # ---- shares_out 채우기 (세션 기반 UPDATE) ----
        updated = 0
        for tk, v in shares_map.items():
            if v.get("shares_out") is None:
                continue

            res = db.execute(
                text("""
                    update market_snapshot
                       set shares_out = :shares_out,
                           treasury_shares = :treasury_shares,
                           float_shares = :float_shares
                     where snapshot_id = :snapshot_id
                       and ticker = :ticker
                """),
                {
                    "snapshot_id": sid,
                    "ticker": tk,
                    "shares_out": v.get("shares_out"),
                    "treasury_shares": v.get("treasury_shares"),
                    "float_shares": v.get("float_shares"),
                },
            )
            if getattr(res, "rowcount", 0) == 1:
                updated += 1

        db.commit()
        print(f"[shares_out] updated rows = {updated}")

        fund_df = pd.DataFrame([r.__dict__ for r in fundamental_rows])

        # SW 관점: pandas가 int 컬럼을 float로 바꾸는 것을 방지(2024.0 같은 형태 제거)
        if "fs_year" in fund_df.columns:
            fund_df["fs_year"] = pd.to_numeric(fund_df["fs_year"], errors="coerce").astype("Int64")

        # S-RIM 의미: 지배지분/지배순익이 비정상 크기면 계산 신뢰도 낮음 → NULL 처리 + flag
        MAX_ABS = 1e18
        for col in ["equity_parent", "net_income_parent"]:
            if col in fund_df.columns:
                fund_df[col] = pd.to_numeric(fund_df[col], errors="coerce")
                fund_df.loc[fund_df[col].abs() > MAX_ABS, col] = None

        upsert_fundamental_snapshot(db, sid, fund_df)

        # --- Stage3: S-RIM 계산 실행 ---
        srim_summary = run_stage3_srim(
            db=db,                                      # SW 관점: 동일 세션/트랜잭션 컨텍스트 재사용
            snapshot_id=sid,                             # S-RIM 의미: 동일 스냅샷에서 계산값을 확정
            default_discount_rate=float(settings.DEFAULT_DISCOUNT_RATE),  # S-RIM 의미: r 기본값
            persistence=1.0,                             # S-RIM 의미: MVP는 단순(영구 지속)로 시작
            clamp_negative_residual=True,                # S-RIM 의미: 음수 초과이익은 보수적으로 0 처리
        )

        return {
            "snapshot_id": sid,
            "as_of": str(as_of),
            "market_rows": int(len(market_df)),
            "fund_rows": int(len(fund_df)),
            "srim": srim_summary,                        # SW 관점: stage3 결과 요약을 함께 반환(디버깅 용이)
        }
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
