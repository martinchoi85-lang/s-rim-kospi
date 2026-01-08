# src/app/etl/stage3_srim.py

from __future__ import annotations

import json
from typing import Dict, Any, List

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import SrimInput, compute_srim
from app.utils.json_sanitize import sanitize_for_json, safe_float_or_none


def load_discount_rate(db: Session, snapshot_id: str, default_rate: float) -> float:
    """
    snapshot_id 기준 요구수익률 r 로드

    S-RIM 의미: r은 ROE와 비교되는 기준선이자, 초과이익 현재가치 계산의 분모
    SW 관점: discount_rate_snapshot 스키마 변화에 안전하게 동작하도록 단건 조회 + fallback
    """
    row = db.execute(
        text("""
            select rate
            from discount_rate_snapshot
            where snapshot_id = :sid
            order by as_of_date desc nulls last   -- SW 관점: as_of_date가 있으면 가장 최신값 사용
            limit 1
        """),
        {"sid": snapshot_id},
    ).fetchone()

    if row and row[0] is not None:
        return float(row[0])  # SW 관점: DB에 값이 있으면 그것을 신뢰

    return float(default_rate)  # SW 관점: 값이 없으면 default로 진행(배치 중단 방지)


def load_calc_ready_rows(db: Session, snapshot_id: str) -> pd.DataFrame:
    """
    market_snapshot + fundamental_snapshot 조인 후, 계산 가능한 row만 로드

    S-RIM 의미: fair_price 계산에 필요한 (equity, net_income, shares_out, close_price)를 확보
    SW 관점: DB에서 calc_ready를 확정해 Python 계산 과정의 예외를 최소화
    """
    rows = db.execute(
        text("""
            select
                m.snapshot_id,                  -- SW 관점: 스냅샷 키
                m.ticker,                       -- SW 관점: 종목 키
                t.name as name,                 -- SW 관점: 종목명은 tickers에서 조회
                m.close_price as market_price,  -- S-RIM 의미: 시장가격(종가) = 괴리율 계산에 필요
                m.shares_out,                   -- S-RIM 의미: 발행주식수 = 주당 환산 분모
                f.equity_parent,                -- S-RIM 의미: 지배주주지분 = BPS 분자
                f.net_income_parent             -- S-RIM 의미: 지배주주순이익 = ROE 산출
            from market_snapshot m
            join fundamental_snapshot f
              on f.snapshot_id = m.snapshot_id  -- SW 관점: 동일 스냅샷에서만 결합
             and f.ticker = m.ticker            -- SW 관점: 동일 종목에서만 결합
            left join tickers t
              on t.ticker = m.ticker            -- SW 관점: 종목 메타 조인
            where m.snapshot_id = :sid
              and m.close_price is not null
              and m.shares_out is not null
              and f.equity_parent is not null
              and f.net_income_parent is not null
        """),
        {"sid": snapshot_id},
    ).fetchall()

    df = pd.DataFrame(
        rows,
        columns=["snapshot_id", "ticker", "name", "market_price", "shares_out", "equity_parent", "net_income_parent"],
    )
    return df


def upsert_srim_result(db: Session, snapshot_id: str, rows: List[Dict[str, Any]]) -> int:
    """
    srim_result 업서트(DDL 정합)

    DDL:
      snapshot_id, ticker (PK)
      bps, roe, r, fair_price, gap_pct, flags(jsonb), computed_at(default now())

    SW 관점: 동일 snapshot_id+ticker는 항상 최신 계산값으로 덮어씀
    S-RIM 의미: 스냅샷별 이론가(fair_price)와 주요 지표(ROE, BPS, r)를 기록
    """
    if not rows:
        return 0

    stmt = text("""
        insert into srim_result (
            snapshot_id,
            ticker,
            bps,
            roe,
            r,
            fair_price,
            gap_pct,
            flags
        ) values (
            :snapshot_id,
            :ticker,
            :bps,
            :roe,
            :r,
            :fair_price,
            :gap_pct,
            cast(:flags as jsonb)
        )
        on conflict (snapshot_id, ticker)
        do update set
            bps        = excluded.bps,
            roe        = excluded.roe,
            r          = excluded.r,
            fair_price = excluded.fair_price,
            gap_pct    = excluded.gap_pct,
            flags      = excluded.flags,
            computed_at = now()  -- SW 관점: 재계산 시각 갱신
    """)

    count = 0
    for r in rows:
        db.execute(stmt, r)  # SW 관점: 월 1회 배치이므로 row 단위 실행도 충분히 단순/안전
        count += 1

    db.commit()  # SW 관점: 트랜잭션 커밋으로 결과 영속화
    return count


def run_stage3_srim(
    db: Session,
    snapshot_id: str,
    *,
    default_discount_rate: float,
    persistence: float = 1.0,
    clamp_negative_residual: bool = True,
) -> Dict[str, Any]:
    """
    Stage3: S-RIM 계산 실행

    SW 관점: (read join) → (compute) → (upsert)
    S-RIM 의미: 종목별 fair_price(이론가)와 gap(괴리율)을 산출하여 서비스의 핵심 결과를 생성
    """
    r = load_discount_rate(db, snapshot_id, default_discount_rate)  # S-RIM 의미: 이번 스냅샷의 요구수익률 확정
    df = load_calc_ready_rows(db, snapshot_id)  # SW 관점: 계산 가능한 row만 확보

    out_rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        # SW 관점: ticker를 항상 6자리 문자열로 정규화(조인/PK 일관성)
        ticker = str(row["ticker"]).strip().zfill(6)

        # S-RIM 계산 입력 구성
        x = SrimInput(
            ticker=ticker,                                 # SW 관점: 종목 식별자
            equity_parent=float(row["equity_parent"]),      # S-RIM 의미: 자기자본(지배주주지분)
            net_income_parent=float(row["net_income_parent"]),  # S-RIM 의미: 순이익(지배주주순이익)
            shares_out=float(row["shares_out"]),            # S-RIM 의미: 발행주식수
            market_price=float(row["market_price"]),        # S-RIM 의미: 시장가격(종가)
            discount_rate=float(r),                         # S-RIM 의미: 요구수익률 r
        )

        # S-RIM 계산 실행
        y = compute_srim(
            x,
            persistence=persistence,                         # S-RIM 의미: 초과이익 지속 가정(기본 1.0)
            clamp_negative_residual=clamp_negative_residual, # S-RIM 의미: ROE<r일 때 보수적 처리(기본 True)
        )

        # SW 관점: srim_result 스키마에 맞춘 flags 구성(flags는 JSONB에 저장되므로 NaN/Inf를 반드시 제거해야 함)
        flags: Dict[str, Any] = dict(y.flags)
        flags["market_price_used"] = x.market_price
        flags["persistence_used"] = persistence

        flags = sanitize_for_json(flags)  # SW 관점: NaN/Inf -> None으로 치환

        if safe_float_or_none(y.bps) is None or safe_float_or_none(y.roe) is None:
            flags["FLAG_SUSPICIOUS_NUMERIC"] = True  # SW 관점: UI에서 경고/필터링에 사용

        flags.update({
            "roe_method": "NI_PARENT / EQUITY_PARENT",
            "roe_is_annualized": False,
            "equity_is_average": False,
        })

        out_rows.append(
            {
                "snapshot_id": snapshot_id,
                "ticker": ticker,

                # SW 관점: numeric 컬럼에는 NaN을 넣을 수 없으니 NULL로 저장
                "bps": safe_float_or_none(y.bps),
                "roe": safe_float_or_none(y.roe),
                "r": safe_float_or_none(r),
                "fair_price": safe_float_or_none(y.srim_price),
                "gap_pct": safe_float_or_none(y.gap_pct),

                # SW 관점: allow_nan=False로 “NaN 토큰 생성”을 원천 차단
                "flags": json.dumps(flags, ensure_ascii=False, allow_nan=False),
            }
        )

    upserted = upsert_srim_result(db, snapshot_id, out_rows)  # SW 관점: 결과를 srim_result에 저장

    return {
        "snapshot_id": snapshot_id,     # SW 관점: 실행 컨텍스트 반환
        "discount_rate": r,             # S-RIM 의미: 이번 실행에 사용된 r
        "calc_rows": int(len(df)),      # SW 관점: 계산 대상 수
        "upserted_rows": int(upserted), # SW 관점: 저장 성공 수
    }
