import pandas as pd

FINANCE_KEYWORDS = ["금융", "은행", "보험", "증권", "지주"]

def compute_srim(
    market_df: pd.DataFrame,
    fund_df: pd.DataFrame,
    discount_rate: float,
) -> pd.DataFrame:
    df = market_df.merge(fund_df, on="ticker", how="left")
    flags_list = []

    bps = []
    roe = []
    fair = []
    gap = []

    for _, row in df.iterrows():
        flags = {}

        equity = row.get("equity_parent")
        ni = row.get("net_income_parent")
        shares = row.get("shares_out")
        close = row.get("close_price")
        sector = (row.get("sector_name") or "")

        if any(k in sector for k in FINANCE_KEYWORDS):
            flags["FLAG_FINANCE_OR_HOLDING"] = True

        if equity is None or ni is None or shares is None or close is None:
            flags["FLAG_MISSING_DATA"] = True
            bps.append(None); roe.append(None); fair.append(None); gap.append(None)
            flags_list.append(flags)
            continue

        if equity <= 0:
            flags["FLAG_EQUITY_LE_0"] = True

        _bps = float(equity) / float(shares) if shares else None
        _roe = float(ni) / float(equity) if equity else None

        if _roe is not None and _roe <= 0:
            flags["FLAG_ROE_LE_0"] = True

        if _bps is None or _roe is None or discount_rate <= 0:
            bps.append(None); roe.append(None); fair.append(None); gap.append(None)
            flags_list.append(flags)
            continue

        _fair = _bps * _roe / discount_rate
        _gap = (_fair / float(close) - 1.0) if close else None

        bps.append(_bps)
        roe.append(_roe)
        fair.append(_fair)
        gap.append(_gap)
        flags_list.append(flags)

    df["bps"] = bps
    df["roe"] = roe
    df["r"] = discount_rate
    df["fair_price"] = fair
    df["gap_pct"] = gap
    df["flags"] = flags_list

    return df[["ticker", "bps", "roe", "r", "fair_price", "gap_pct", "flags"]]
