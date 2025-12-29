from dataclasses import dataclass
import pandas as pd

@dataclass
class FundamentalRow:
    ticker: str
    fs_year: int | None
    report_code: str | None
    is_consolidated: bool | None
    equity_parent: float | None
    net_income_parent: float | None
    data_quality: dict

def fetch_latest_annual_fundamentals(tickers: list[str]) -> list[FundamentalRow]:
    """
    For each ticker, fetch latest available annual consolidated statements and extract:
      - equity attributable to owners of parent
      - net income attributable to owners of parent

    In practice:
      - map ticker -> corp_code
      - choose latest FY with 사업보고서(연간) 우선
      - parse accounts
    """
    # NOTE: 실제 구현에서는 OpenDartReader()로 fs 데이터를 받아
    # 계정명 매핑(지배주주지분/지배주주순이익)을 수행.
    raise NotImplementedError
