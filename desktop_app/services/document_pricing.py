from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Literal, Tuple

DiscountMode = Literal["percentage", "amount"]
PricingMode = Literal["tax_added", "tax_included"]

ZERO = Decimal("0")
ONE_HUNDRED = Decimal("100")
Q4 = Decimal("0.0001")
Q2 = Decimal("0.01")


def to_decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        if isinstance(value, str):
            value = value.strip().replace(",", ".")
            if value == "":
                return default
        return Decimal(str(value))
    except Exception:
        return default


def quantize_4(value: Decimal) -> Decimal:
    return value.quantize(Q4, rounding=ROUND_HALF_UP)


def quantize_2(value: Decimal) -> Decimal:
    return value.quantize(Q2, rounding=ROUND_HALF_UP)


def _sign_for_discount(base_amount: Decimal) -> Decimal:
    return Decimal("1") if base_amount >= ZERO else Decimal("-1")


def normalize_discount_pair(
    *,
    base_amount: Any,
    descuento_porcentaje: Any = ZERO,
    descuento_importe: Any = ZERO,
    mode: DiscountMode = "percentage",
) -> Tuple[Decimal, Decimal]:
    base = to_decimal(base_amount)
    base_abs = abs(base)
    if base_abs <= ZERO:
        return ZERO, ZERO

    pct = max(ZERO, to_decimal(descuento_porcentaje))
    imp = max(ZERO, to_decimal(descuento_importe))

    if mode == "amount":
        imp = min(imp, base_abs)
        pct = (imp * ONE_HUNDRED / base_abs) if base_abs > ZERO else ZERO
    else:
        pct = min(pct, ONE_HUNDRED)
        imp = base_abs * pct / ONE_HUNDRED

    imp = quantize_4(min(imp, base_abs))
    pct = quantize_4((imp * ONE_HUNDRED / base_abs) if base_abs > ZERO else ZERO)
    return pct, imp


def calculate_document_totals(
    *,
    items: Iterable[Dict[str, Any]],
    descuento_global_porcentaje: Any = ZERO,
    descuento_global_importe: Any = ZERO,
    descuento_global_mode: DiscountMode = "percentage",
    sena: Any = ZERO,
    pricing_mode: PricingMode = "tax_added",
) -> Dict[str, Any]:
    normalized_items: List[Dict[str, Any]] = []
    subtotal_bruto = ZERO
    subtotal_neto_lineas = ZERO
    descuento_lineas_importe = ZERO
    iva_breakdown_map: Dict[str, Dict[str, Decimal]] = {}

    for raw in items:
        cantidad = to_decimal(raw.get("cantidad"))
        precio_unitario = to_decimal(raw.get("precio_unitario"))
        porcentaje_iva = max(ZERO, to_decimal(raw.get("porcentaje_iva")))
        porcentaje_iva_fiscal = max(ZERO, to_decimal(raw.get("porcentaje_iva_fiscal"), porcentaje_iva))
        descuento_mode = raw.get("descuento_mode") or raw.get("descuento_modo") or "percentage"
        if descuento_mode not in ("percentage", "amount"):
            descuento_mode = "percentage"

        base_bruta = cantidad * precio_unitario
        desc_pct, desc_imp = normalize_discount_pair(
            base_amount=base_bruta,
            descuento_porcentaje=raw.get("descuento_porcentaje"),
            descuento_importe=raw.get("descuento_importe"),
            mode=descuento_mode,
        )
        signo_linea = _sign_for_discount(base_bruta)
        linea_neta_pre_global = base_bruta - (signo_linea * desc_imp)

        subtotal_bruto += base_bruta
        subtotal_neto_lineas += linea_neta_pre_global
        descuento_lineas_importe += desc_imp

        normalized_items.append(
            {
                **raw,
                "cantidad": quantize_4(cantidad),
                "precio_unitario": quantize_4(precio_unitario),
                "porcentaje_iva": quantize_4(porcentaje_iva),
                "porcentaje_iva_fiscal": quantize_4(porcentaje_iva_fiscal),
                "descuento_porcentaje": desc_pct,
                "descuento_importe": desc_imp,
                "linea_bruta": quantize_4(base_bruta),
                "linea_neta_pre_global": quantize_4(linea_neta_pre_global),
                # Persisted line total: net after line discount (without global discount).
                "total_linea": quantize_4(linea_neta_pre_global),
            }
        )

    global_pct, global_imp = normalize_discount_pair(
        base_amount=subtotal_neto_lineas,
        descuento_porcentaje=descuento_global_porcentaje,
        descuento_importe=descuento_global_importe,
        mode=descuento_global_mode,
    )

    subtotal_abs = abs(subtotal_neto_lineas)
    global_allocated = ZERO
    iva_total = ZERO
    neto_fiscal_total = ZERO
    signo_subtotal = _sign_for_discount(subtotal_neto_lineas)

    for idx, item in enumerate(normalized_items):
        base_linea = to_decimal(item["linea_neta_pre_global"])
        base_linea_abs = abs(base_linea)

        if idx == len(normalized_items) - 1:
            alloc = global_imp - global_allocated
        elif subtotal_abs > ZERO and base_linea_abs > ZERO and global_imp > ZERO:
            alloc = quantize_4(global_imp * base_linea_abs / subtotal_abs)
            global_allocated += alloc
        else:
            alloc = ZERO

        if alloc < ZERO:
            alloc = ZERO
        if alloc > base_linea_abs:
            alloc = base_linea_abs

        signo_linea = _sign_for_discount(base_linea)
        linea_neta = base_linea - (signo_linea * alloc)
        if pricing_mode == "tax_included":
            tasa_fiscal = max(ZERO, to_decimal(item.get("porcentaje_iva_fiscal")))
            divisor = Decimal("1") + (tasa_fiscal / ONE_HUNDRED)
            if tasa_fiscal > ZERO and divisor != ZERO:
                neto_fiscal_linea = quantize_4(linea_neta / divisor)
                iva_linea = quantize_4(linea_neta - neto_fiscal_linea)
            else:
                neto_fiscal_linea = quantize_4(linea_neta)
                iva_linea = ZERO
            neto_fiscal_total += neto_fiscal_linea
            if tasa_fiscal > ZERO:
                breakdown_key = str(quantize_4(tasa_fiscal))
                if breakdown_key not in iva_breakdown_map:
                    iva_breakdown_map[breakdown_key] = {
                        "porcentaje_iva": quantize_4(tasa_fiscal),
                        "base_imponible": ZERO,
                        "importe": ZERO,
                    }
                iva_breakdown_map[breakdown_key]["base_imponible"] += neto_fiscal_linea
                iva_breakdown_map[breakdown_key]["importe"] += iva_linea
            item["neto_fiscal_linea"] = neto_fiscal_linea
        else:
            iva_linea = quantize_4(linea_neta * to_decimal(item["porcentaje_iva"]) / ONE_HUNDRED)
            neto_fiscal_total += quantize_4(linea_neta)
            tasa_vis = max(ZERO, to_decimal(item.get("porcentaje_iva")))
            if tasa_vis > ZERO:
                breakdown_key = str(quantize_4(tasa_vis))
                if breakdown_key not in iva_breakdown_map:
                    iva_breakdown_map[breakdown_key] = {
                        "porcentaje_iva": quantize_4(tasa_vis),
                        "base_imponible": ZERO,
                        "importe": ZERO,
                    }
                iva_breakdown_map[breakdown_key]["base_imponible"] += quantize_4(linea_neta)
                iva_breakdown_map[breakdown_key]["importe"] += iva_linea

        item["descuento_global_prorrateado"] = quantize_4(alloc)
        item["linea_neta"] = quantize_4(linea_neta)
        item["iva_linea"] = iva_linea

        iva_total += iva_linea

    total_operacional = quantize_4(subtotal_neto_lineas - (signo_subtotal * global_imp))
    iva_total = quantize_4(iva_total)
    neto = quantize_4(neto_fiscal_total)
    if pricing_mode == "tax_included":
        total = total_operacional
        neto = quantize_4(total - iva_total)
        ui_subtotal = total
        ui_iva_total = ZERO
    else:
        total = quantize_4(total_operacional + iva_total)
        ui_subtotal = total_operacional
        ui_iva_total = iva_total
    sena_dec = quantize_4(max(ZERO, to_decimal(sena)))
    saldo = quantize_4(max(ZERO, total - sena_dec))

    iva_breakdown: List[Dict[str, Decimal]] = []
    breakdown_items = list(iva_breakdown_map.values())
    breakdown_items.sort(key=lambda it: (to_decimal(it["porcentaje_iva"]), to_decimal(it["base_imponible"])))
    if breakdown_items:
        for row in breakdown_items:
            iva_breakdown.append(
                {
                    "porcentaje_iva": quantize_4(to_decimal(row["porcentaje_iva"])),
                    "base_imponible": quantize_4(to_decimal(row["base_imponible"])),
                    "importe": quantize_4(to_decimal(row["importe"])),
                }
            )
        if pricing_mode == "tax_included":
            sum_base = quantize_4(sum((to_decimal(b["base_imponible"]) for b in iva_breakdown), ZERO))
            sum_iva = quantize_4(sum((to_decimal(b["importe"]) for b in iva_breakdown), ZERO))
            diff_base = quantize_4(neto - sum_base)
            diff_iva = quantize_4(iva_total - sum_iva)
            if diff_base != ZERO or diff_iva != ZERO:
                iva_breakdown[-1]["base_imponible"] = quantize_4(to_decimal(iva_breakdown[-1]["base_imponible"]) + diff_base)
                iva_breakdown[-1]["importe"] = quantize_4(to_decimal(iva_breakdown[-1]["importe"]) + diff_iva)

    return {
        "items": normalized_items,
        "pricing_mode": pricing_mode,
        "subtotal_bruto": quantize_4(subtotal_bruto),
        "descuento_lineas_importe": quantize_4(descuento_lineas_importe),
        "subtotal_neto_lineas": quantize_4(subtotal_neto_lineas),
        "descuento_global_porcentaje": global_pct,
        "descuento_global_importe": global_imp,
        "ui_subtotal": quantize_4(ui_subtotal),
        "ui_iva_total": quantize_4(ui_iva_total),
        "iva_breakdown": iva_breakdown,
        "neto": neto,
        "iva_total": iva_total,
        "total": total,
        "sena": sena_dec,
        "saldo": saldo,
    }
