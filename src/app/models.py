# src/app/srim/model.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Date, Numeric, Boolean, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from datetime import datetime, date


@dataclass
class SrimInput:
    """S-RIM 계산에 필요한 입력값을 구조화 (SW 관점: 함수 인자 폭발 방지)"""
    ticker: str
    equity_parent: float            # S-RIM 의미: 지배주주지분(자기자본) = BPS의 분자
    net_income_parent: float        # S-RIM 의미: 지배주주순이익 = ROE 산출에 필요
    shares_out: float               # S-RIM 의미: 발행주식수 = BPS를 주당으로 환산
    market_price: float             # S-RIM 의미: 현재 시장가격과 이론가의 갭 계산에 필요
    discount_rate: float            # S-RIM 의미: 요구수익률 r (초과이익 계산의 기준선)


@dataclass
class SrimOutput:
    """계산 결과를 구조화 (SW 관점: DB 업서트/응답 변환이 단순해짐)"""
    srim_price: Optional[float]
    bps: Optional[float]
    roe: Optional[float]
    spread: Optional[float]
    gap_pct: Optional[float]
    flags: Dict[str, Any]


def compute_srim(
    x: SrimInput,
    *,
    persistence: float = 1.0,
    clamp_negative_residual: bool = True,
) -> SrimOutput:
    """
    MVP S-RIM(Residual Income Model) 주당가치 계산

    S-RIM 의미(단순화 버전):
    - BPS = Equity / Shares
    - ROE = NetIncome / Equity
    - 초과이익(Residual) = (ROE - r) * Equity
    - 초과이익의 현재가치(영구지속 가정) = Residual / r
    - 기업가치 = Equity + PV(Residual)
    - 주당가치 = (Equity + PV(Residual)) / Shares
    - persistence는 초과이익 지속계수(0~1). MVP에서는 1.0(단순)로 시작.

    SW 관점:
    - 계산 불능/이상치를 flags로 기록하여, UI/API에서 “주의/제외” 처리 가능
    """

    flags: Dict[str, Any] = {}

    # --- 입력값 검증 (SW 관점: 런타임 에러 방지 / S-RIM 관점: 분모 0 방지) ---
    if x.equity_parent is None or x.net_income_parent is None or x.shares_out is None or x.discount_rate is None:
        flags["FLAG_MISSING_INPUT"] = True  # S-RIM 의미: 핵심 입력값 누락 → 계산 불가
        return SrimOutput(None, None, None, None, None, flags)

    if x.equity_parent <= 0:
        flags["FLAG_EQUITY_NON_POSITIVE"] = True  # S-RIM 의미: 자기자본이 0/음수면 ROE/BPS 왜곡
        return SrimOutput(None, None, None, None, None, flags)

    if x.shares_out <= 0:
        flags["FLAG_SHARES_NON_POSITIVE"] = True  # S-RIM 의미: 주당가치 환산 불가
        return SrimOutput(None, None, None, None, None, flags)

    if x.discount_rate <= 0:
        flags["FLAG_DISCOUNT_RATE_NON_POSITIVE"] = True  # S-RIM 의미: 요구수익률 r이 0이면 PV 계산 불가
        return SrimOutput(None, None, None, None, None, flags)

    # --- 핵심 지표 계산 (S-RIM 의미: ROE, BPS, Spread는 모델의 뼈대) ---
    bps = x.equity_parent / x.shares_out  # S-RIM 의미: 자본의 주당 가치(장부가 기반)
    roe = x.net_income_parent / x.equity_parent  # S-RIM 의미: 자본 수익성(초과이익의 출발점)
    spread = roe - x.discount_rate  # S-RIM 의미: ROE가 요구수익률을 초과하는 정도(초과이익)

    # --- 초과이익(Residual Income) 현재가치 계산 (S-RIM 의미: 장부가 + 초과이익의 PV) ---
    residual_income_total = spread * x.equity_parent  # S-RIM 의미: 초과이익(총액 기준)
    flags["residual_income_total"] = residual_income_total  # SW 관점: 디버깅/설명가능성 확보

    if clamp_negative_residual and residual_income_total < 0:
        # S-RIM 의미: ROE < r이면 초과이익이 음수 → “장부가 이하”가 가능하지만 MVP는 보수적으로 0으로 클램프
        flags["FLAG_NEGATIVE_RESIDUAL_CLAMPED"] = True
        residual_income_total = 0.0

    # S-RIM 의미: 영구지속 가정의 PV = residual / r, persistence로 가중
    pv_residual = (residual_income_total / x.discount_rate) * persistence
    flags["pv_residual_total"] = pv_residual  # SW 관점: 계산 내역을 결과에 남겨 해석 가능

    # S-RIM 의미: 기업가치(총액) = Equity + PV(Residual)
    intrinsic_total = x.equity_parent + pv_residual

    # S-RIM 의미: 주당가치 = 총가치 / 주식수
    srim_price = intrinsic_total / x.shares_out

    # --- 시장가격 대비 괴리율 계산 (S-RIM 의미: “싸다/비싸다” 표시용) ---
    if x.market_price and x.market_price > 0:
        gap_pct = (srim_price / x.market_price - 1.0) * 100.0  # S-RIM 의미: +면 이론가>시장가(저평가 신호)
    else:
        gap_pct = None
        flags["FLAG_BAD_MARKET_PRICE"] = True  # SW 관점: market price 없으면 gap 계산 제외

    # --- 참고 플래그 (해석용) ---
    if roe < 0:
        flags["FLAG_ROE_NEGATIVE"] = True  # S-RIM 의미: 적자 기업 → 모델 해석 주의
    if spread < 0:
        flags["FLAG_ROE_BELOW_R"] = True  # S-RIM 의미: ROE가 요구수익률 미만 → 장부가 이하 가능성

    return SrimOutput(
        srim_price=float(srim_price),
        bps=float(bps),
        roe=float(roe),
        spread=float(spread),
        gap_pct=float(gap_pct) if gap_pct is not None else None,
        flags=flags,
    )


class Base(DeclarativeBase):
    pass

class Ticker(Base):
    __tablename__ = "tickers"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    market: Mapped[str] = mapped_column(String, nullable=False)
    sector_name: Mapped[str | None] = mapped_column(String)
    last_seen_date: Mapped[date] = mapped_column(Date, nullable=False, default=date.today)

class Snapshot(Base):
    __tablename__ = "snapshots"
    snapshot_id: Mapped[str] = mapped_column(String, primary_key=True)
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    note: Mapped[str | None] = mapped_column(Text)

class MarketSnapshot(Base):
    __tablename__ = "market_snapshot"
    snapshot_id: Mapped[str] = mapped_column(String, ForeignKey("snapshots.snapshot_id", ondelete="CASCADE"), primary_key=True)
    ticker: Mapped[str] = mapped_column(String, ForeignKey("tickers.ticker"), primary_key=True)
    close_price: Mapped[float | None] = mapped_column(Numeric)
    market_cap: Mapped[float | None] = mapped_column(Numeric)
    shares_out: Mapped[float | None] = mapped_column(Numeric)

class FundamentalSnapshot(Base):
    __tablename__ = "fundamental_snapshot"
    snapshot_id: Mapped[str] = mapped_column(String, ForeignKey("snapshots.snapshot_id", ondelete="CASCADE"), primary_key=True)
    ticker: Mapped[str] = mapped_column(String, ForeignKey("tickers.ticker"), primary_key=True)

    fs_year: Mapped[int | None] = mapped_column()
    report_code: Mapped[str | None] = mapped_column(String)
    is_consolidated: Mapped[bool | None] = mapped_column(Boolean)

    equity_parent: Mapped[float | None] = mapped_column(Numeric)
    net_income_parent: Mapped[float | None] = mapped_column(Numeric)

    data_quality: Mapped[dict] = mapped_column(JSONB, default=dict)

class DiscountRateSnapshot(Base):
    __tablename__ = "discount_rate_snapshot"
    snapshot_id: Mapped[str] = mapped_column(String, ForeignKey("snapshots.snapshot_id", ondelete="CASCADE"), primary_key=True)
    rate: Mapped[float] = mapped_column(Numeric, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)

class SRimResult(Base):
    __tablename__ = "srim_result"
    snapshot_id: Mapped[str] = mapped_column(String, ForeignKey("snapshots.snapshot_id", ondelete="CASCADE"), primary_key=True)
    ticker: Mapped[str] = mapped_column(String, ForeignKey("tickers.ticker"), primary_key=True)

    bps: Mapped[float | None] = mapped_column(Numeric)
    roe: Mapped[float | None] = mapped_column(Numeric)
    r: Mapped[float | None] = mapped_column(Numeric)
    fair_price: Mapped[float | None] = mapped_column(Numeric)
    gap_pct: Mapped[float | None] = mapped_column(Numeric)

    flags: Mapped[dict] = mapped_column(JSONB, default=dict)
    computed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.utcnow)
