from __future__ import annotations

from datetime import date, timedelta
import pandas as pd
import FinanceDataReader as fdr


def resolve_recent_business_day(d: date, lookback_days: int = 14) -> date:
    """
    FDR은 거래일이 아니면 데이터가 비거나 에러가 날 수 있으므로,
    d부터 과거로 lookback_days 범위에서 '데이터가 존재하는' 가장 최근 날짜를 찾는다.
    """
    # 삼성전자(005930)를 기준으로 거래일 여부를 판단(가장 안정적)
    test_code = "005930"
    for i in range(lookback_days + 1):
        cand = d - timedelta(days=i)
        start = cand.strftime("%Y-%m-%d")
        end = cand.strftime("%Y-%m-%d")
        try:
            df = fdr.DataReader(test_code, start, end)
            if df is not None and len(df) > 0:
                return cand
        except Exception:
            pass
    raise RuntimeError(f"Could not find a business day within {lookback_days} days from {d}")


def fetch_kospi_universe(as_of: date) -> pd.DataFrame:
    """
    FDR 기반 코스피 전체 종목 시장데이터:
      - ticker, name, market, close_price, market_cap(NULL), shares_out(NULL), as_of
    주의:
      - FDR listing에서 KOSPI 종목을 가져옴
      - 종가(close)는 각 티커별 DataReader로 조회 (시간이 조금 걸릴 수 있음)
      - 시총/주식수는 1차에서는 NULL로 둠 (pykrx가 복구되면 다시 채우거나 다른 소스 추가)
    """
    trading_day = resolve_recent_business_day(as_of, lookback_days=40)

    # 1) 종목 리스트
    listing = fdr.StockListing("KOSPI")
    # listing 컬럼은 환경에 따라 'Code','Name' 또는 'Symbol','Name' 등 변형 가능
    code_col = "Code" if "Code" in listing.columns else ("Symbol" if "Symbol" in listing.columns else None)
    name_col = "Name" if "Name" in listing.columns else None
    if code_col is None or name_col is None:
        raise RuntimeError(f"Unexpected listing columns: {listing.columns.tolist()}")

    df = listing[[code_col, name_col]].copy()
    df.columns = ["ticker", "name"]
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    df["market"] = "KOSPI"

    # 2) 종가(해당일 종가). 많은 종목을 개별 호출하면 느릴 수 있으니,
    #    우선 MVP로는 "Close만" 채우고 나중에 최적화(캐시/병렬)를 붙임.
    t = trading_day.strftime("%Y-%m-%d")
    closes = []
    for tk in df["ticker"].tolist():
        try:
            px = fdr.DataReader(tk, t, t)
            if px is not None and len(px) > 0 and "Close" in px.columns:
                closes.append(float(px["Close"].iloc[-1]))
            else:
                closes.append(None)
        except Exception:
            closes.append(None)

    df["close_price"] = closes
    df["market_cap"] = None
    df["shares_out"] = None
    df["as_of"] = pd.to_datetime(trading_day)

    return df
