from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import zipfile
import io
import time

import pandas as pd
import requests


@dataclass
class FundamentalRow:
    ticker: str
    fs_year: Optional[int]
    report_code: Optional[str]
    is_consolidated: Optional[bool]
    equity_parent: Optional[float]
    net_income_parent: Optional[float]
    data_quality: dict


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return float(x)
    except Exception:
        return None


def load_corpcode_df(api_key: str, cache_path: str = "data/corpCode.xml") -> pd.DataFrame:
    """
    DART corpCode.xml (zip) 다운로드 후 캐시. DataFrame 로드.
    """
    p = Path(cache_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if not p.exists():
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        resp = requests.get(url, params={"crtfc_key": api_key}, timeout=60)
        resp.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        xml_name = z.namelist()[0]
        p.write_bytes(z.read(xml_name))

    df = pd.read_xml(p)
    if not {"corp_code", "corp_name", "stock_code"}.issubset(set(df.columns)):
        raise RuntimeError(f"Unexpected corpCode.xml columns: {df.columns.tolist()}")

    df["stock_code"] = df["stock_code"].astype(str).str.strip()
    df["corp_name"] = df["corp_name"].astype(str).str.strip()
    df = df[df["stock_code"].str.len() > 0].copy()

    df["corp_code"] = df["corp_code"].astype(str).str.strip().str.zfill(8)
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    return df


def _guess_common_stock_code(ticker: str) -> Optional[str]:
    # 005935 -> 005930 같은 보통주 추정
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    return ticker[:5] + "0"


def resolve_corp_code(corp_df: pd.DataFrame, ticker: str, ticker_name: Optional[str] = None) -> Optional[str]:
    m = corp_df[corp_df["stock_code"] == ticker]
    if len(m) > 0:
        return m.iloc[0]["corp_code"]

    common = _guess_common_stock_code(ticker)
    if common:
        m2 = corp_df[corp_df["stock_code"] == common]
        if len(m2) > 0:
            return m2.iloc[0]["corp_code"]

    # ticker_name 기반 매칭은 일단 생략(MVP), 필요시 추가 가능
    return None


def dart_fnltt_all(api_key: str, corp_code: str, bsns_year: int, reprt_code: str, fs_div: str) -> pd.DataFrame:
    """
    DART 재무제표(전체 계정) 공식 API:
    /api/fnlttSinglAcntAll.json

    reprt_code:
      11011 사업보고서(연간)
      11012 반기보고서
      11013 1분기보고서
      11014 3분기보고서

    fs_div:
      CFS 연결
      OFS 별도
    """
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(bsns_year),
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    status = data.get("status")
    if status != "000":
        # 예: 013(데이터없음), 020(조회한도) 등
        raise RuntimeError(f"DART status={status}, message={data.get('message')}")

    lst = data.get("list") or []
    return pd.DataFrame(lst)


def _pick_value_from_dart_df(df: pd.DataFrame, account_nm_keys: list[str]) -> Optional[float]:
    """
    DART JSON list는 보통 account_nm / thstrm_amount 같은 키를 가짐.
    """
    if df is None or len(df) == 0:
        return None
    if "account_nm" not in df.columns:
        return None

    # 금액 컬럼 후보(케이스별로 다름)
    amt_col = None
    for c in ["thstrm_amount", "thstrm_add_amount", "amount"]:
        if c in df.columns:
            amt_col = c
            break
    if not amt_col:
        return None

    s = df["account_nm"].astype(str)
    for key in account_nm_keys:
        m = df[s.str.contains(key, na=False)]
        if len(m) > 0:
            return _safe_float(m.iloc[0][amt_col])
    return None


def fetch_latest_annual_fundamentals(
    api_key: str,
    tickers: list[str],
    ticker_to_name: dict[str, str] | None = None,
    max_companies: int | None = None,
) -> list[FundamentalRow]:
    
    # ticker 타입이 numpy/int로 들어와도 항상 문자열 6자리로 정규화
    tickers = [str(x).strip().zfill(6) for x in tickers]

    corp_df = load_corpcode_df(api_key)

    # 연간 사업보고서
    reprt_codes = ["11011", "11014", "11012", "11013"]  # 연간 -> 3Q -> 반기 -> 1Q
    candidate_years = [current_year - 2, current_year - 3, current_year - 1]


    from datetime import date as _date
    current_year = _date.today().year
    # 연초에는 2025 사업보고서가 없을 수 있으니 2024부터 시도
    candidate_years = [current_year - 2, current_year - 1, current_year - 3]

    use_tickers = tickers[:max_companies] if max_companies else tickers
    results: list[FundamentalRow] = []

    for tk in use_tickers:
        tk = str(tk).strip().zfill(6)

        # ticker 정합성: DART는 6자리 숫자만 stock_code 매칭이 안정적
        if not (isinstance(tk, str) and len(tk) == 6 and tk.isdigit()):
            results.append(FundamentalRow(
                ticker=tk, fs_year=None, report_code=reprt_code, is_consolidated=None,
                equity_parent=None, net_income_parent=None,
                data_quality={"FLAG_BAD_TICKER": True}
            ))
            continue

        name = ticker_to_name.get(tk) if ticker_to_name else None
        corp_code = resolve_corp_code(corp_df, tk, name)

        if not corp_code:
            results.append(FundamentalRow(
                ticker=tk, fs_year=None, report_code=reprt_code, is_consolidated=None,
                equity_parent=None, net_income_parent=None,
                data_quality={"FLAG_NO_CORP_CODE": True, "name": name}
            ))
            continue

        picked_year = None
        fs_df = None
        flags = {}

        found = False
        for rc in reprt_codes:
            # 연결 우선
            for y in candidate_years:
                try:
                    fs_df = dart_fnltt_all(api_key, corp_code, y, rc, fs_div="CFS")
                    if fs_df is not None and len(fs_df) > 0:
                        picked_year = y
                        flags["is_consolidated"] = True
                        flags["report_code_used"] = rc
                        found = True
                        break
                except Exception as e:
                    flags["LAST_ERR_CFS"] = f"{type(e).__name__}: {e}"
                    time.sleep(0.2)
            if found:
                break

            # 별도 대체
            for y in candidate_years:
                try:
                    fs_df = dart_fnltt_all(api_key, corp_code, y, rc, fs_div="OFS")
                    if fs_df is not None and len(fs_df) > 0:
                        picked_year = y
                        flags["is_consolidated"] = False
                        flags["report_code_used"] = rc
                        found = True
                        break
                except Exception as e:
                    flags["LAST_ERR_OFS"] = f"{type(e).__name__}: {e}"
                    time.sleep(0.2)
            if found:
                break

        if not found or fs_df is None or len(fs_df) == 0:
            results.append(FundamentalRow(
                ticker=tk, fs_year=None, report_code="11011",  # 저장용 기본값은 유지
                is_consolidated=flags.get("is_consolidated"),
                equity_parent=None, net_income_parent=None,
                data_quality={"FLAG_NO_FS": True, **flags, "name": name}
            ))
            continue

        # 지배주주지분 우선, 없으면 자본총계 대체
        equity_parent = _pick_value_from_dart_df(fs_df, ["지배기업소유주지분", "지배주주지분"])
        if equity_parent is None:
            equity_total = _pick_value_from_dart_df(fs_df, ["자본총계"])
            equity_parent = equity_total
            if equity_total is not None:
                flags["FLAG_EQUITY_SUB_TOTAL_EQUITY"] = True

        # 지배주주순이익 우선, 없으면 당기순이익 대체
        ni_parent = _pick_value_from_dart_df(fs_df, ["지배기업소유주지분당기순이익", "지배주주순이익"])
        if ni_parent is None:
            ni_total = _pick_value_from_dart_df(fs_df, ["당기순이익"])
            ni_parent = ni_total
            if ni_total is not None:
                flags["FLAG_NI_SUB_NET_INCOME"] = True

        results.append(FundamentalRow(
            ticker=tk,
            fs_year=picked_year,
            report_code=flags.get("report_code_used"),
            is_consolidated=flags.get("is_consolidated"),
            equity_parent=equity_parent,
            net_income_parent=ni_parent,
            data_quality=flags
        ))

    return results
