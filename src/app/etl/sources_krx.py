from __future__ import annotations

from datetime import date, datetime, timedelta
import pandas as pd
from pykrx import stock


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def resolve_last_trading_day(d: date, lookback_days: int = 14) -> date:
    """
    사용자가 준 날짜 d가 휴일/주말이면,
    d부터 과거로 lookback_days만큼 탐색해서
    가장 최근 거래일을 찾아 반환.
    """
    for i in range(lookback_days + 1):
        cand = d - timedelta(days=i)
        ymd = _to_yyyymmdd(cand)
        try:
            tickers = stock.get_market_ticker_list(ymd, market="KOSPI")
            if tickers:  # 거래일이면 티커 목록이 나옴
                return cand
        except Exception:
            pass
    raise RuntimeError(f"Could not resolve trading day within lookback_days={lookback_days} from {d}")


def fetch_kospi_universe(as_of: date) -> pd.DataFrame:
    """
    코스피 전체 종목에 대해 아래 컬럼을 반환:
      - ticker (str)
      - name (str)
      - market (str) = 'KOSPI'
      - close_price (numeric)
      - market_cap (numeric)
      - shares_out (numeric)
      - as_of (date)

    주의:
    - as_of가 비거래일이면 내부적으로 가장 최근 거래일로 보정합니다.
    """
    trading_day = resolve_last_trading_day(as_of)
    ymd = _to_yyyymmdd(trading_day)

    tickers = stock.get_market_ticker_list(ymd, market="KOSPI")
    if not tickers:
        raise RuntimeError(f"No tickers returned for {ymd}. Check market or date.")

    # 종가 (OHLCV)
    ohlcv = stock.get_market_ohlcv_by_ticker(ymd, market="KOSPI")
    # 시총/상장주식수
    cap = stock.get_market_cap_by_ticker(ymd, market="KOSPI")

    # pykrx DF index가 ticker 문자열임
    df = pd.DataFrame({"ticker": tickers})
    df["name"] = df["ticker"].apply(stock.get_market_ticker_name)
    df["market"] = "KOSPI"

    # join
    df = df.merge(
        ohlcv[["종가"]].reset_index().rename(columns={"티커": "ticker", "종가": "close_price"}),
        on="ticker",
        how="left",
    )
    df = df.merge(
        cap[["시가총액", "상장주식수"]].reset_index().rename(
            columns={"티커": "ticker", "시가총액": "market_cap", "상장주식수": "shares_out"}
        ),
        on="ticker",
        how="left",
    )

    df["as_of"] = pd.to_datetime(trading_day)
    return df
