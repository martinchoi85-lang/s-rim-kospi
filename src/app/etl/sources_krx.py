from datetime import date
import pandas as pd

def fetch_kospi_universe(as_of: date) -> pd.DataFrame:
    """
    Return columns:
      ticker, name, market, sector_name, close_price, market_cap, shares_out
    """
    # NOTE: 실제 구현에서는 pykrx의 함수들을 사용합니다.
    # 예: from pykrx import stock
    # tickers = stock.get_market_ticker_list(as_of.strftime("%Y%m%d"), market="KOSPI")
    # name = stock.get_market_ticker_name(ticker)
    # ohlcv = stock.get_market_ohlcv_by_ticker(as_of.strftime("%Y%m%d"), market="KOSPI")
    # cap = stock.get_market_cap_by_ticker(as_of.strftime("%Y%m%d"), market="KOSPI")
    # sector = stock.get_market_sector_classifications(...) 등
    #
    # 여기서는 골격만 유지합니다.
    raise NotImplementedError
