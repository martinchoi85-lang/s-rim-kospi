# src/app/etl/run_etl.py
from __future__ import annotations

"""
S-RIM ETL Runner (stage 기반)

목표(운영 단순화):
- 월 1회: Stage0/1/2 실행 후 Stage3 실행
- 로직 수정/재계산: Stage3만 재실행

Stage 정의:
0) snapshot 생성 + discount_rate_snapshot 저장
1) KRX 시장 데이터 적재(tickers, market_snapshot close/market_cap 등)
2) DART 재무 + 주식수 보강(fundamental_snapshot 업서트 + market_snapshot 주식수 UPDATE)
3) S-RIM 계산(stage3_srim.py) 후 srim_result 업서트
"""

import argparse
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.config import settings

from app.etl.sources_krx import fetch_kospi_universe
from app.etl.sources_dart import (
    fetch_latest_annual_fundamentals,
    load_corpcode_df,
    fetch_shares_out_for_tickers,
)
from app.etl.load import (
    upsert_snapshot,
    upsert_discount_rate,
    upsert_tickers,
    upsert_market_snapshot,
    upsert_fundamental_snapshot,
)
from app.etl.stage3_srim import run_stage3_srim


# -----------------------------
# Snapshot ID 규칙 (기존 그대로)
# -----------------------------
def snapshot_id_for(dt: date) -> str:
    """as_of_date 기준 분기 스냅샷 ID 생성 (예: 2026-01-04 -> 2026Q1)"""
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"


# -----------------------------
# Stage별 함수
# -----------------------------
def stage0(db: Session, sid: str, as_of: date, *, r: float, note: str | None) -> None:
    """
    Stage0: snapshots + discount_rate_snapshot 저장

    SW 관점:
    - discount_rate_snapshot는 스냅샷 단위로 '정책 변수(r)'를 고정해두는 테이블
    - 초기 운영에서는 자동 산출보다 수동 확정이 가장 단순하고 안전
    """
    upsert_snapshot(db, sid, as_of, note=note)
    upsert_discount_rate(db, sid, as_of, rate=float(r), source="manual")


def stage1(db: Session, sid: str, as_of: date) -> pd.DataFrame:
    """
    Stage1: KRX 시장 데이터 적재

    반환:
    - market_df (ticker/name/market/close_price/market_cap ...) 를 Stage2에서 재사용 가능
    """
    market_df = fetch_kospi_universe(as_of)  # 기존 코드 유지
    tickers_df = market_df[["ticker", "name", "market"]].drop_duplicates()

    upsert_tickers(db, tickers_df)
    upsert_market_snapshot(db, sid, market_df)

    return market_df


def stage2(
    db: Session,
    sid: str,
    *,
    market_df: pd.DataFrame,
    dart_limit: int | None = None,
) -> dict:
    """
    Stage2: DART 재무 + 주식수 보강

    - fundamental_snapshot 업서트
    - market_snapshot의 shares_out/treasury/float UPDATE
    """
    if not settings.DART_API_KEY:
        raise RuntimeError("DART_API_KEY is not set in .env")

    # ticker 정규화(항상 6자리)
    tickers_raw = market_df["ticker"].tolist()
    tickers = [str(x).strip().zfill(6) for x in tickers_raw]
    ticker_to_name = {
        str(t).strip().zfill(6): n
        for t, n in zip(tickers_raw, market_df["name"].tolist())
    }

    # 1) 재무(사업보고서 우선, 내부에서 fallback)
    fundamental_rows = fetch_latest_annual_fundamentals(
        api_key=settings.DART_API_KEY,
        tickers=tickers,
        ticker_to_name=ticker_to_name,
        max_companies=dart_limit,
    )

    # DART 주식수 fetch를 위한 preferred_year_report 구성(기존 그대로)
    preferred_year_report = {}
    for r in fundamental_rows:
        if r.fs_year and r.report_code:
            preferred_year_report[str(r.ticker).zfill(6)] = (int(r.fs_year), str(r.report_code))

    corp_df = load_corpcode_df(settings.DART_API_KEY)

    # 2) 주식수 (필요 시 limit 적용)
    shares_target = tickers[:dart_limit] if dart_limit else tickers
    shares_map = fetch_shares_out_for_tickers(
        api_key=settings.DART_API_KEY,
        tickers=shares_target,  # 이미 6자리 정규화된 리스트
        corp_df=corp_df,
        ticker_to_name=ticker_to_name,
        preferred_year_report=preferred_year_report,
    )

    # ---- shares_out UPDATE (기존 로직 유지) ----
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
    print(f"[stage2:shares_out] updated rows = {updated}")

    # ---- fundamental_snapshot 업서트 준비 ----
    fund_df = pd.DataFrame([r.__dict__ for r in fundamental_rows])

    # pandas가 Int 컬럼을 float로 바꾸는 문제 방지(기존 그대로)
    if "fs_year" in fund_df.columns:
        fund_df["fs_year"] = pd.to_numeric(fund_df["fs_year"], errors="coerce").astype("Int64")

    # 비정상 크기 제거(기존 그대로)
    MAX_ABS = 1e18
    for col in ["equity_parent", "net_income_parent"]:
        if col in fund_df.columns:
            fund_df[col] = pd.to_numeric(fund_df[col], errors="coerce")
            fund_df.loc[fund_df[col].abs() > MAX_ABS, col] = None

    upsert_fundamental_snapshot(db, sid, fund_df)

    return {
        "fund_rows": int(len(fund_df)),
        "shares_updated": int(updated),
    }


def stage3(db: Session, sid: str, *, persistence: float, clamp_negative_residual: bool) -> dict:
    """
    Stage3: S-RIM 계산

    주의:
    - stage3_srim.py 내부에서 discount_rate_snapshot를 먼저 조회하고,
      없으면 default_discount_rate로 fallback 함.
    - 따라서 Stage0를 실행하지 않아도 계산은 되지만,
      운영 정책상 'snapshot별 r 확정'을 위해 Stage0 실행을 권장.
    """
    srim_summary = run_stage3_srim(
        db=db,
        snapshot_id=sid,
        default_discount_rate=float(settings.DEFAULT_DISCOUNT_RATE),
        persistence=float(persistence),
        clamp_negative_residual=bool(clamp_negative_residual),
    )
    return srim_summary


# -----------------------------
# Orchestrator
# -----------------------------
def run(
    *,
    as_of: date,
    snapshot_id: str | None,
    stages: list[int],
    r: float,
    note: str | None,
    dart_limit: int | None,
    persistence: float,
    clamp_negative_residual: bool,
) -> dict:
    sid = snapshot_id or snapshot_id_for(as_of)

    db: Session = SessionLocal()
    try:
        market_df: pd.DataFrame | None = None

        if 0 in stages:
            stage0(db, sid, as_of, r=r, note=note)

        if 1 in stages:
            market_df = stage1(db, sid, as_of)

        if 2 in stages:
            # Stage2는 market_df가 필요(티커 유니버스)
            # 운영 단순화를 위해: Stage2 단독 실행은 막고, Stage1과 같이 실행하도록 유도
            if market_df is None:
                raise RuntimeError("Stage2는 Stage1 결과(market_df)가 필요합니다. stages에 1을 포함하세요.")
            stage2_summary = stage2(db, sid, market_df=market_df, dart_limit=dart_limit)
        else:
            stage2_summary = None

        if 3 in stages:
            stage3_summary = stage3(db, sid, persistence=persistence, clamp_negative_residual=clamp_negative_residual)
        else:
            stage3_summary = None

        return {
            "snapshot_id": sid,
            "as_of": str(as_of),
            "stages": stages,
            "stage2": stage2_summary,
            "stage3": stage3_summary,
        }
    finally:
        db.close()


def parse_stages(s: str) -> list[int]:
    """'0,1,2,3' 같은 입력을 [0,1,2,3]으로 파싱"""
    out: list[int] = []
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(int(tok))
    return out


def main() -> None:
    """
    실행 예시:
    - 전체 실행:
      python -m app.etl.run_etl --as-of 2026-01-04 --stages 0,1,2,3 --r 0.10

    - stage3만 재실행:
      python -m app.etl.run_etl --snapshot-id 2026Q1 --as-of 2026-01-04 --stages 3
      (as-of는 snapshot_id가 주어지면 사실상 참고값이지만, CLI 일관성을 위해 남겨둠)

    - DART 테스트용 limit:
      python -m app.etl.run_etl --as-of 2026-01-04 --stages 0,1,2 --dart-limit 50
    """
    p = argparse.ArgumentParser(description="S-RIM ETL runner (stage-based)")

    p.add_argument("--as-of", type=str, required=True, help="YYYY-MM-DD")
    p.add_argument("--snapshot-id", type=str, default=None, help="예: 2026Q1 (미입력 시 as-of로 자동 생성)")
    p.add_argument("--stages", type=str, default="0,1,2,3", help="예: 0,1,2 또는 3")

    # 운영 정책 변수
    p.add_argument("--r", type=float, default=float(settings.DEFAULT_DISCOUNT_RATE), help="요구수익률 r (Stage0에서 DB 저장)")
    p.add_argument("--note", type=str, default="Quarterly snapshot (KOSPI) - stage1+2", help="snapshots.note")

    # DART 실행 옵션
    p.add_argument("--dart-limit", type=int, default=None, help="테스트용 DART 종목 제한 (예: 50)")

    # S-RIM 계산 옵션
    p.add_argument("--persistence", type=float, default=1.0, help="초과이익 지속계수(0~1), MVP 기본 1.0")
    p.add_argument("--no-clamp-negative-residual", action="store_true", help="음수 초과이익 클램프 비활성화(실험용)")

    args = p.parse_args()

    as_of = date.fromisoformat(args.as_of)
    stages = parse_stages(args.stages)

    result = run(
        as_of=as_of,
        snapshot_id=args.snapshot_id,
        stages=stages,
        r=float(args.r),
        note=args.note,
        dart_limit=args.dart_limit,
        persistence=float(args.persistence),
        clamp_negative_residual=not bool(args.no_clamp_negative_residual),
    )

    print(result)


if __name__ == "__main__":
    main()
