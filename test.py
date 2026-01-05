from pykrx import stock
from pykrx import bond

tickers = stock.get_market_ticker_list("20260104", market="KOSPI")
print(tickers)