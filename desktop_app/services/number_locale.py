from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Optional


_NON_DIGIT_RE = re.compile(r"[^\d]")


def _infer_decimal_separator(text: str) -> Optional[str]:
    last_dot = text.rfind(".")
    last_comma = text.rfind(",")

    if last_dot >= 0 and last_comma >= 0:
        return "." if last_dot > last_comma else ","

    if last_dot >= 0:
        return _infer_single_separator(text, ".")
    if last_comma >= 0:
        return _infer_single_separator(text, ",")
    return None


def _infer_single_separator(text: str, sep: str) -> Optional[str]:
    positions = [idx for idx, ch in enumerate(text) if ch == sep]
    if not positions:
        return None

    if len(positions) == 1:
        pos = positions[0]
        digits_before = len(_NON_DIGIT_RE.sub("", text[:pos]))
        digits_after = len(_NON_DIGIT_RE.sub("", text[pos + 1 :]))
        if digits_after == 0:
            return None
        if digits_after in (1, 2):
            return sep
        if digits_after == 3 and digits_before >= 1:
            return None
        return sep

    groups = [_NON_DIGIT_RE.sub("", part) for part in text.split(sep)]
    if all(len(group) == 3 for group in groups[1:] if group):
        return None
    if len(groups[-1]) <= 2:
        return sep
    return None


def _to_decimal(value: Any) -> Optional[Decimal]:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return Decimal(str(value))
    return parse_locale_number(value)


def _quantize(value: Decimal, decimals: int) -> Decimal:
    safe_decimals = max(int(decimals), 0)
    quantum = Decimal(1).scaleb(-safe_decimals)
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def _group_thousands(integer_part: str) -> str:
    sign = ""
    digits = integer_part
    if digits.startswith("-"):
        sign = "-"
        digits = digits[1:]
    if not digits:
        return f"{sign}0"
    chunks = []
    while digits:
        chunks.append(digits[-3:])
        digits = digits[:-3]
    return sign + ".".join(reversed(chunks))


def parse_locale_number(text: Any) -> Optional[Decimal]:
    if isinstance(text, Decimal):
        return text
    if isinstance(text, int):
        return Decimal(text)
    if isinstance(text, float):
        if not math.isfinite(text):
            return None
        return Decimal(str(text))
    if text is None:
        return None

    raw = str(text).strip()
    if not raw:
        return None

    normalized = (
        raw.replace("\u00a0", "")
        .replace(" ", "")
        .replace("$", "")
        .replace("%", "")
        .replace("AR$", "")
        .replace("ARS", "")
    )

    negative = False
    if normalized.startswith("(") and normalized.endswith(")"):
        negative = True
        normalized = normalized[1:-1]
    if normalized.startswith("-"):
        negative = True
    normalized = normalized.lstrip("+-")

    if not any(ch.isdigit() for ch in normalized):
        return None

    decimal_sep = _infer_decimal_separator(normalized)
    if decimal_sep:
        int_raw, frac_raw = normalized.rsplit(decimal_sep, 1)
        int_digits = _NON_DIGIT_RE.sub("", int_raw) or "0"
        frac_digits = _NON_DIGIT_RE.sub("", frac_raw)
        decimal_text = int_digits if not frac_digits else f"{int_digits}.{frac_digits}"
    else:
        digits = _NON_DIGIT_RE.sub("", normalized)
        if not digits:
            return None
        decimal_text = digits

    if negative and decimal_text != "0":
        decimal_text = f"-{decimal_text}"

    try:
        return Decimal(decimal_text)
    except InvalidOperation:
        return None


def format_decimal(value: Any, decimals: int = 2) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "—" if value is None else str(value)

    safe_decimals = max(int(decimals), 0)
    quantized = _quantize(parsed, safe_decimals)
    sign = "-" if quantized < 0 else ""
    absolute = -quantized if quantized < 0 else quantized
    raw = f"{absolute:.{safe_decimals}f}"
    integer_part, dot, fraction_part = raw.partition(".")
    grouped = _group_thousands(integer_part)

    if safe_decimals == 0:
        return f"{sign}{grouped}"
    if not dot:
        fraction_part = "0" * safe_decimals
    return f"{sign}{grouped},{fraction_part}"


def format_currency(value: Any, symbol: str = "$") -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "—" if value is None else str(value)
    return f"{symbol}{format_decimal(parsed, decimals=2)}"


def format_percent(value: Any, decimals: int = 2) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "—" if value is None else str(value)
    return f"{format_decimal(parsed, decimals=decimals)}%"


def normalize_input_value(text: Any, decimals: int = 2, use_grouping: bool = True) -> str:
    parsed = parse_locale_number(text)
    if parsed is None:
        return ""

    safe_decimals = max(int(decimals), 0)
    quantized = _quantize(parsed, safe_decimals)
    sign = "-" if quantized < 0 else ""
    absolute = -quantized if quantized < 0 else quantized
    raw = f"{absolute:.{safe_decimals}f}"
    integer_part, dot, fraction_part = raw.partition(".")
    integer_rendered = _group_thousands(integer_part) if use_grouping else integer_part

    if safe_decimals == 0:
        return f"{sign}{integer_rendered}"
    if not dot:
        fraction_part = "0" * safe_decimals
    return f"{sign}{integer_rendered},{fraction_part}"

