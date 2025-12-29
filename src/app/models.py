from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Date, Numeric, Boolean, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from datetime import datetime, date

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
