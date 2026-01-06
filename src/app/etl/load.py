from __future__ import annotations
import json
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import date
from ..models import Ticker, Snapshot, MarketSnapshot, FundamentalSnapshot, DiscountRateSnapshot, SRimResult



def upsert_snapshot(db: Session, snapshot_id: str, as_of: date, note: str | None = None):
    obj = db.get(Snapshot, snapshot_id)
    if obj is None:
        obj = Snapshot(snapshot_id=snapshot_id, as_of_date=as_of, note=note)
        db.add(obj)
    else:
        obj.as_of_date = as_of
        obj.note = note
    db.commit()


def upsert_discount_rate(db: Session, snapshot_id: str, as_of: date, rate: float, source: str = "manual"):
    obj = db.get(DiscountRateSnapshot, snapshot_id)
    if obj is None:
        obj = DiscountRateSnapshot(snapshot_id=snapshot_id, as_of_date=as_of, rate=rate, source=source)
        db.add(obj)
    else:
        obj.as_of_date = as_of
        obj.rate = rate
        obj.source = source
    db.commit()


def upsert_tickers(db: Session, tickers_df: pd.DataFrame):
    """
    tickers 테이블 업서트
    입력 DF 컬럼: ticker, name, market
    """
    sql = text("""
        insert into tickers (ticker, name, market, last_seen_date)
        values (:ticker, :name, :market, current_date)
        on conflict (ticker) do update
        set name = excluded.name,
            market = excluded.market,
            last_seen_date = excluded.last_seen_date
    """)
    rows = tickers_df[["ticker", "name", "market"]].to_dict(orient="records")
    db.execute(sql, rows)
    db.commit()


def upsert_market_snapshot(db: Session, snapshot_id: str, market_df: pd.DataFrame):
    """
    market_snapshot 테이블 업서트
    입력 DF 컬럼: ticker, close_price, market_cap, shares_out
    """
    sql = text("""
        insert into market_snapshot (snapshot_id, ticker, close_price, market_cap, shares_out)
        values (:snapshot_id, :ticker, :close_price, :market_cap, :shares_out)
        on conflict (snapshot_id, ticker) do update
        set close_price = excluded.close_price,
            market_cap = excluded.market_cap,
            shares_out = excluded.shares_out
    """)
    df = market_df.copy()
    df["snapshot_id"] = snapshot_id

    rows = df[["snapshot_id", "ticker", "close_price", "market_cap", "shares_out"]].to_dict(orient="records")
    db.execute(sql, rows)
    db.commit()


def upsert_fundamental_snapshot(db: Session, snapshot_id: str, fund_df: pd.DataFrame):
    sql = text("""
        insert into fundamental_snapshot
          (snapshot_id, ticker, fs_year, report_code, is_consolidated,
           equity_parent, net_income_parent, data_quality)
        values
          (:snapshot_id, :ticker, :fs_year, :report_code, :is_consolidated,
           :equity_parent, :net_income_parent, cast(:data_quality as jsonb))
        on conflict (snapshot_id, ticker) do update
        set fs_year = excluded.fs_year,
            report_code = excluded.report_code,
            is_consolidated = excluded.is_consolidated,
            equity_parent = excluded.equity_parent,
            net_income_parent = excluded.net_income_parent,
            data_quality = excluded.data_quality
    """)
    df = fund_df.copy()
    df["snapshot_id"] = snapshot_id
    df["data_quality"] = df["data_quality"].apply(lambda x: json.dumps(x or {}, ensure_ascii=False))
    rows = df[
        ["snapshot_id", "ticker", "fs_year", "report_code", "is_consolidated",
         "equity_parent", "net_income_parent", "data_quality"]
    ].to_dict(orient="records")
    db.execute(sql, rows)
    db.commit()
