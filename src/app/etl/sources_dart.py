from __future__ import annotations
import zipfile
import io
import time
import requests
import pandas as pd
from dataclasses import dataclass
from typing import Optional
from pathlib import Path


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
    """
    DART 재무를 최대한 넓게 가져오기 위한 MVP:
    - ticker 입력이 numpy/int로 들어와도 6자리 문자열로 정규화
    - corp_code는 corpCode.xml로 매핑
    - 보고서 코드 reprt_code는 연간(11011) -> 3Q(11014) -> 반기(11012) -> 1Q(11013) 순으로 fallback
    - 연도는 (current_year-2) -> (current_year-3) -> (current_year-1) 순으로 fallback (연초 대응)
    - 연결(CFS) 우선, 실패 시 별도(OFS) fallback
    """

    # 0) ticker 타입/포맷 방어: 항상 "6자리 문자열"로 통일
    tickers_norm = [str(x).strip().zfill(6) for x in tickers]

    # 1) 기준 연도 계산 (candidate_years보다 먼저!)
    from datetime import date as _date
    current_year = _date.today().year

    # 2) 보고서/연도 후보
    reprt_codes = ["11011", "11014", "11012", "11013"]  # 연간 -> 3Q -> 반기 -> 1Q
    candidate_years = [current_year - 2, current_year - 3, current_year - 1]

    # 3) corpCode 로드
    corp_df = load_corpcode_df(api_key)

    # 4) 실행 범위(테스트 limit)
    use_tickers = tickers_norm[:max_companies] if max_companies else tickers_norm
    results: list[FundamentalRow] = []

    # 5) 기본값(에러/스킵 케이스에서 report_code 필드 채우기용)
    default_report_code = "11011"

    for tk in use_tickers:
        # 루프에서도 한 번 더 정규화(이중 안전장치)
        tk = str(tk).strip().zfill(6)

        name = ticker_to_name.get(tk) if ticker_to_name else None

        # ticker 정합성: DART stock_code 매칭은 "6자리 숫자"가 가장 안정적
        if not (len(tk) == 6 and tk.isdigit()):
            results.append(
                FundamentalRow(
                    ticker=tk,
                    fs_year=None,
                    report_code=default_report_code,
                    is_consolidated=None,
                    equity_parent=None,
                    net_income_parent=None,
                    data_quality={"FLAG_BAD_TICKER": True, "name": name},
                )
            )
            continue

        corp_code = resolve_corp_code(corp_df, tk, name)
        if not corp_code:
            results.append(
                FundamentalRow(
                    ticker=tk,
                    fs_year=None,
                    report_code=default_report_code,
                    is_consolidated=None,
                    equity_parent=None,
                    net_income_parent=None,
                    data_quality={"FLAG_NO_CORP_CODE": True, "name": name},
                )
            )
            continue

        picked_year: int | None = None
        fs_df = None
        flags: dict = {}
        found = False

        # 보고서 코드 -> 연도 -> (CFS/OFS) 순으로 탐색
        for rc in reprt_codes:
            # 연결(CFS) 우선
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
                    # 마지막 에러만 남기되, 디버깅용으로 report/year도 같이 남김
                    flags["LAST_ERR_CFS"] = f"{type(e).__name__}: {e}"
                    flags["LAST_TRY_CFS"] = {"year": y, "reprt_code": rc}
                    time.sleep(0.2)
            if found:
                break

            # 별도(OFS) fallback
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
                    flags["LAST_TRY_OFS"] = {"year": y, "reprt_code": rc}
                    time.sleep(0.2)
            if found:
                break

        if not found or fs_df is None or len(fs_df) == 0:
            results.append(
                FundamentalRow(
                    ticker=tk,
                    fs_year=None,
                    report_code=default_report_code,
                    is_consolidated=flags.get("is_consolidated"),
                    equity_parent=None,
                    net_income_parent=None,
                    data_quality={"FLAG_NO_FS": True, **flags, "name": name},
                )
            )
            continue

        # ---- 값 추출 ----
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

        results.append(
            FundamentalRow(
                ticker=tk,
                fs_year=picked_year,
                report_code=flags.get("report_code_used", default_report_code),
                is_consolidated=flags.get("is_consolidated"),
                equity_parent=equity_parent,
                net_income_parent=ni_parent,
                data_quality=flags,
            )
        )

    return results


def dart_stock_total_status(api_key: str, corp_code: str, bsns_year: int, reprt_code: str) -> pd.DataFrame:
    """
    DART 정기보고서 주요정보 - 주식의 총수 현황
    GET https://opendart.fss.or.kr/api/stockTotqySttus.json
    """
    url = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(bsns_year),
        "reprt_code": reprt_code,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if data.get("status") != "000":
        raise RuntimeError(f"DART status={data.get('status')}, message={data.get('message')}")

    return pd.DataFrame(data.get("list") or [])


def _to_num(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return float(x)
    except Exception:
        return None


def pick_issued_shares(df: pd.DataFrame) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """
    df(list)에서 발행주식수(istc_totqy), 자기주식수(tesstk_co), 유통주식수(distb_stock_co) 추출.
    케이스에 따라 여러 행이 있을 수 있는데, 보통 '보통주' 행을 우선으로 잡고 없으면 첫 행.
    """
    if df is None or len(df) == 0:
        return None, None, None

    # 주식구분/종류 컬럼명이 문서/회사에 따라 달라질 수 있어 방어적으로 처리
    cols = set(df.columns)

    # 후보: se(구분), stock_knd(종류) 같은 컬럼이 있을 수도 있음
    # 여기서는 "보통" 문자열이 들어있는 행을 우선 선택
    pick = None
    for col in ["se", "stock_knd", "stck_knd", "class", "reprt_ty"]:
        if col in cols:
            m = df[df[col].astype(str).str.contains("보통", na=False)]
            if len(m) > 0:
                pick = m.iloc[0]
                break
    if pick is None:
        pick = df.iloc[0]

    issued = _to_num(pick.get("istc_totqy"))
    treasury = _to_num(pick.get("tesstk_co"))
    float_sh = _to_num(pick.get("distb_stock_co"))
    return issued, treasury, float_sh


def fetch_shares_out_for_tickers(
    api_key: str,
    tickers: list[str],
    corp_df: pd.DataFrame,
    ticker_to_name: dict[str, str] | None,
    # fundamentals에서 성공한 (year, report_code_used) 정보를 주면 그걸 우선 사용
    preferred_year_report: dict[str, tuple[int, str]] | None = None,
) -> dict[str, dict]:
    """
    ticker -> {shares_out, treasury_shares, float_shares, data_quality}
    """
    from datetime import date as _date
    current_year = _date.today().year

    reprt_codes = ["11011", "11014", "11012", "11013"]
    candidate_years = [current_year - 2, current_year - 3, current_year - 1]

    out: dict[str, dict] = {}

    for tk in tickers:
        tk = str(tk).strip().zfill(6)
        name = ticker_to_name.get(tk) if ticker_to_name else None

        corp_code = resolve_corp_code(corp_df, tk, name)
        if not corp_code:
            out[tk] = {"shares_out": None, "treasury_shares": None, "float_shares": None,
                       "data_quality": {"FLAG_NO_CORP_CODE": True, "name": name}}
            continue

        flags = {"name": name}
        found = False
        issued = treasury = float_sh = None

        # fundamentals에서 성공한 year/report가 있으면 그 조합부터 시도
        first_tries = []
        if preferred_year_report and tk in preferred_year_report:
            y0, rc0 = preferred_year_report[tk]
            first_tries.append((y0, rc0))

        # 그 다음 fallback
        for rc in reprt_codes:
            for y in candidate_years:
                first_tries.append((y, rc))

        # 중복 제거(순서 유지)
        seen = set()
        tries = []
        for y, rc in first_tries:
            if (y, rc) not in seen:
                seen.add((y, rc))
                tries.append((y, rc))

        for y, rc in tries:
            try:
                df = dart_stock_total_status(api_key, corp_code, y, rc)
                issued, treasury, float_sh = pick_issued_shares(df)
                if issued is not None:
                    flags["year_used"] = y
                    flags["report_code_used"] = rc
                    found = True
                    break
            except Exception as e:
                flags["LAST_ERR"] = f"{type(e).__name__}: {e}"
                flags["LAST_TRY"] = {"year": y, "reprt_code": rc}
                time.sleep(0.2)

        if not found:
            out[tk] = {"shares_out": None, "treasury_shares": None, "float_shares": None,
                       "data_quality": {"FLAG_NO_SHARES": True, **flags}}
            continue

        out[tk] = {"shares_out": issued, "treasury_shares": treasury, "float_shares": float_sh,
                   "data_quality": flags}

    return out
