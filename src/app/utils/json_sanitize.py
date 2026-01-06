# src/app/utils/json_sanitize.py

from __future__ import annotations

import math
from typing import Any


def _is_bad_number(x: Any) -> bool:
    """SW 관점: JSON/DB에 넣으면 문제되는 부동소수(NaN/Inf) 판정"""
    return isinstance(x, float) and (math.isnan(x) or math.isinf(x))


def sanitize_for_json(obj: Any) -> Any:
    """
    JSON에 안전하게 들어가도록 값을 정리

    - NaN/Inf -> None (json의 null)
    - dict/list는 재귀 처리
    - 그 외는 그대로
    """
    if _is_bad_number(obj):
        return None

    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]

    return obj


def safe_float_or_none(x: Any) -> float | None:
    """
    S-RIM 산출물(bps/roe/fair/gap 등)을 DB numeric에 안전하게 넣기 위한 변환

    S-RIM 의미: 계산 불능이면 NULL로 저장하고, flags로 원인을 남기는 전략이 운영상 안전
    """
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v
