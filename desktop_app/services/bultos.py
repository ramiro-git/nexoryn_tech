from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Optional


def _to_integral_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            return None
        return int(value)
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = Decimal(raw.replace(",", "."))
        except InvalidOperation:
            return None
        if parsed != parsed.to_integral_value():
            return None
        return int(parsed)
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    if parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def calculate_bultos(cantidad: Any, unidades_por_bulto: Any, mode: str = "strict_exact") -> Optional[int]:
    """
    Calculates logistic packages count.

    Current business default is `strict_exact`:
    - invalid/null values return None
    - only exact multiples are displayed
    - non-exact divisions return None

    Other modes are kept ready for a future PO decision:
    - floor
    - round
    """
    cantidad_int = _to_integral_int(cantidad)
    unidades_int = _to_integral_int(unidades_por_bulto)
    if cantidad_int is None or unidades_int is None or unidades_int <= 0:
        return None

    if mode == "strict_exact":
        if cantidad_int % unidades_int != 0:
            return None
        return cantidad_int // unidades_int

    if mode == "floor":
        return cantidad_int // unidades_int

    if mode == "round":
        ratio = Decimal(cantidad_int) / Decimal(unidades_int)
        return int(ratio.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    raise ValueError(f"Unknown bultos calculation mode: {mode}")

