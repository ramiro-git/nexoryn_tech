from __future__ import annotations

from typing import Any


def normalize_price_tipo(tipo: Any) -> str:
    raw = str(tipo or "").strip().upper()
    if "DESC" in raw:
        return "DESCUENTO"
    return "MARGEN"


def calc_price_from_cost_pct(cost: Any, pct: Any, tipo: Any) -> float:
    try:
        safe_cost = max(0.0, float(cost or 0))
    except Exception:
        safe_cost = 0.0
    try:
        safe_pct = max(0.0, float(pct or 0))
    except Exception:
        safe_pct = 0.0

    if normalize_price_tipo(tipo) == "DESCUENTO":
        return max(0.0, safe_cost * (1 - (safe_pct / 100.0)))
    return max(0.0, safe_cost * (1 + (safe_pct / 100.0)))


def calc_pct_from_cost_price(cost: Any, price: Any, tipo: Any) -> float:
    try:
        safe_cost = float(cost or 0)
    except Exception:
        safe_cost = 0.0
    try:
        safe_price = max(0.0, float(price or 0))
    except Exception:
        safe_price = 0.0

    if safe_cost <= 0:
        return 0.0

    if normalize_price_tipo(tipo) == "DESCUENTO":
        pct = (1 - (safe_price / safe_cost)) * 100.0
    else:
        pct = ((safe_price / safe_cost) - 1) * 100.0
    return max(0.0, pct)
