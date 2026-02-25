from __future__ import annotations

from pathlib import Path
from datetime import datetime
import base64
import json
import atexit
import inspect
import shutil
import socket
import sys
import time
import threading
import unicodedata
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote
from venv import logger

import flet as ft
try:
    from flet.core.datatable import DataCell as CoreDataCell
except Exception:
    CoreDataCell = ft.DataCell

if not getattr(CoreDataCell.before_update, "_nexoryn_patched_v3", False):
    _original_before_update_cell = CoreDataCell.before_update
    
    def _patched_before_update_cell(self):
        try:
            # Ensure __content exists and has visible=True to avoid Flet's AssertionError: content must be visible
            if hasattr(self, '_DataCell__content'):
                content = self._DataCell__content
                if hasattr(content, 'visible') and not content.visible:
                    content.visible = True
            return _original_before_update_cell(self)
        except AssertionError as e:
            if "content must be visible" in str(e):
                return
            raise
        except Exception:
            pass
    
    _patched_before_update_cell._nexoryn_patched_v3 = True
    CoreDataCell.before_update = _patched_before_update_cell
    ft.DataCell.before_update = _patched_before_update_cell

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    PROJECT_ROOT = Path(sys._MEIPASS)
else:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from desktop_app.config import load_config
    from desktop_app.database import Database
    from desktop_app.services.afip_service import AfipService
    from desktop_app.services.backup_service import BackupService
    from desktop_app.components.backup_professional_view import BackupProfessionalView
    from desktop_app.components.dashboard_view import DashboardView
    from desktop_app.components.generic_table import (
        AdvancedFilterControl,
        ColumnConfig,
        GenericTable,
        SimpleFilterConfig,
    )
    from desktop_app.components.toast import ToastManager
    from desktop_app.components.mass_update_view import MassUpdateView
    from desktop_app.services.print_service import generate_pdf, generate_pdf_and_open, generate_pdf_and_print
    from desktop_app.services.document_pricing import calculate_document_totals, normalize_discount_pair, quantize_2, to_decimal
    from desktop_app.services.article_price_autocalc import (
        calc_pct_from_cost_price,
        calc_price_from_cost_pct,
        normalize_price_tipo,
    )
    from desktop_app.services.number_locale import (
        format_currency,
        format_percent,
        normalize_input_value,
        parse_locale_number,
    )
    from desktop_app.services.bultos import calculate_bultos
    from desktop_app.components.async_select import AsyncSelect
    from desktop_app.components.button_styles import cancel_button
    from desktop_app.enums import (
        DocumentoEstado, RemotoEstado, BackupEstado, ClaseDocumento,
        DOCUMENTO_ESTADOS_CONFIRMADOS, DOCUMENTO_ESTADOS_PENDIENTES
    )
except ImportError:
    from config import load_config  # type: ignore
    from database import Database  # type: ignore
    from services.afip_service import AfipService # type: ignore
    from services.backup_service import BackupService # type: ignore
    from components.backup_professional_view import BackupProfessionalView # type: ignore
    from components.dashboard_view import DashboardView # type: ignore
    from components.toast import ToastManager # type: ignore
    from components.generic_table import (  # type: ignore
        AdvancedFilterControl,
        ColumnConfig,
        GenericTable,
        SimpleFilterConfig,
    )
    from components.mass_update_view import MassUpdateView # type: ignore
    from services.print_service import generate_pdf, generate_pdf_and_open, generate_pdf_and_print # type: ignore
    from services.document_pricing import calculate_document_totals, normalize_discount_pair, quantize_2, to_decimal  # type: ignore
    from services.article_price_autocalc import calc_pct_from_cost_price, calc_price_from_cost_pct, normalize_price_tipo  # type: ignore
    from services.number_locale import format_currency, format_percent, normalize_input_value, parse_locale_number  # type: ignore
    from services.bultos import calculate_bultos  # type: ignore
    from components.async_select import AsyncSelect  # type: ignore
    from components.button_styles import cancel_button # type: ignore
    from enums import (  # type: ignore
        DocumentoEstado, RemotoEstado, BackupEstado, ClaseDocumento,
        DOCUMENTO_ESTADOS_CONFIRMADOS, DOCUMENTO_ESTADOS_PENDIENTES
    )
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


ICONS = ft.icons


# Modern Design System
COLOR_ACCENT = "#6366F1"       # Indigo 500
COLOR_ACCENT_HOVER = "#4F46E5" # Indigo 600
COLOR_PANEL = "#0F172A"       # Deep Slate 900
COLOR_SIDEBAR_TEXT = "#94A3B8"
COLOR_SIDEBAR_ACTIVE = "#FFFFFF"
COLOR_BG = "#F1F5F9"          # Slate 100
COLOR_CARD = "#FFFFFF"
COLOR_BORDER = "#E2E8F0"
COLOR_TEXT = "#1E293B"        # Slate 800
COLOR_TEXT_MUTED = "#64748B"  # Slate 500
COLOR_SUCCESS = "#10B981"
COLOR_ERROR = "#EF4444"
COLOR_WARNING = "#EA580C"  # Deep Orange 600 (definitely not yellow)
COLOR_INFO = "#3B82F6"     # Blue 500

REMITO_ESTADOS = [
    (RemotoEstado.PENDIENTE.value, "Pendiente"),
    (RemotoEstado.DESPACHADO.value, "Despachado"),
    (RemotoEstado.ENTREGADO.value, "Entregado"),
    (RemotoEstado.ANULADO.value, "Anulado"),
]

class SafeDataTable(ft.DataTable):
    """Subclass of DataTable to fix TypeErrors and AssertionErrors in Flet updates"""
    def before_update(self):
        try:
            # Ensure index is int or None before parent check
            if hasattr(self, "sort_column_index") and self.sort_column_index is not None:
                try:
                    self.sort_column_index = int(self.sort_column_index)
                except:
                    self.sort_column_index = None
            
            # Forzar visibilidad de la tabla
            self.visible = True
            super().before_update()
        except (AssertionError, Exception):
            # The global DataCell patch should handle the visible assertion,
            # but we catch everything here as a last resort.
            pass


def _parse_float(value: Any, label: str = "valor") -> float:
    if value is None or (isinstance(value, str) and not value.strip()):
        return 0.0
    parsed = parse_locale_number(value)
    if parsed is None:
        raise ValueError(f"El campo '{label}' debe ser un número válido. Recibido: '{value}'")
    return float(parsed)


def _parse_int_quantity(value: Any, label: str = "Cantidad") -> int:
    if value is None or (isinstance(value, str) and not value.strip()):
        return 0
    parsed = parse_locale_number(value)
    if parsed is None:
        raise ValueError(f"El campo '{label}' debe ser un número entero. Recibido: '{value}'")
    if parsed != parsed.to_integral_value():
        raise ValueError(f"El campo '{label}' debe ser un número entero (sin decimales).")
    return int(parsed)


def _parse_positive_float_optional(value: Any, label: str = "valor") -> float:
    if value is None or (isinstance(value, str) and not value.strip()):
        return 0.0
    parsed = parse_locale_number(value)
    if parsed is None:
        raise ValueError(f"El campo '{label}' debe ser un número válido. Recibido: '{value}'")
    numeric = float(parsed)
    if numeric <= 0:
        raise ValueError(f"El campo '{label}' debe ser mayor a 0 o dejarse vacío.")
    return numeric


def _normalize_price_tipo(tipo: Any) -> str:
    return normalize_price_tipo(tipo)


def _calc_price_from_cost_pct(cost: Any, pct: Any, tipo: Any) -> float:
    return calc_price_from_cost_pct(cost, pct, tipo)


def _calc_pct_from_cost_price(cost: Any, price: Any, tipo: Any) -> float:
    return calc_pct_from_cost_price(cost, price, tipo)


def _format_money(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
    if value is None:
        return "—"
    try:
        val = float(value)
        if abs(val) < 0.001:
            val = 0.0
        return format_currency(val)
    except Exception:
        return str(value)


def _format_bool(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
    if value is None:
        return "—"
    return "Verdadero" if bool(value) else "Falso"


def _bool_pill(value: Any) -> ft.Control:
    ok = bool(value)
    return ft.Container(
        padding=ft.padding.symmetric(horizontal=10, vertical=5),
        border_radius=999,
        bgcolor="#DCFCE7" if ok else "#FEE2E2",
        content=ft.Text(
            "Activo" if ok else "Inactivo",
            size=11,
            weight=ft.FontWeight.BOLD,
            color="#166534" if ok else "#991B1B",
        ),
    )

def _status_pill(value: Any, row: Optional[Dict[str, Any]] = None) -> ft.Control:
    status = str(value or "").upper()
    
    # Check if 'CONFIRMADO' should be 'FACTURADO'
    if status == DocumentoEstado.CONFIRMADO.value and row and row.get("cae"):
        status = "FACTURADO"

    colors = {
        DocumentoEstado.PAGADO.value: ("#DCFCE7", "#166534"),
        DocumentoEstado.CONFIRMADO.value: ("#E0F2FE", "#075985"),
        "FACTURADO": ("#CCFBF1", "#0F766E"), # Teal colors for FACTURADO
        DocumentoEstado.BORRADOR.value: ("#F1F5F9", "#475569"),
        DocumentoEstado.ANULADO.value: ("#FEE2E2", "#991B1B"),
    }
    bg, fg = colors.get(status, ("#F3F4F6", "#374151"))
    return ft.Container(
        padding=ft.padding.symmetric(horizontal=10, vertical=4),
        border_radius=20,
        bgcolor=bg,
        content=ft.Text(status, size=11, weight=ft.FontWeight.W_600, color=fg),
    )


def _remito_status_pill(value: Any) -> ft.Control:
    status = str(value or "").upper()
    colors = {
        RemotoEstado.PENDIENTE.value: ("#FEF3C7", "#92400E"),
        RemotoEstado.DESPACHADO.value: ("#DBEAFE", "#1D4ED8"),
        RemotoEstado.ENTREGADO.value: ("#DCFCE7", "#166534"),
        RemotoEstado.ANULADO.value: ("#FEE2E2", "#991B1B"),
    }
    bg, fg = colors.get(status, ("#F3F4F6", "#374151"))
    return ft.Container(
        padding=ft.padding.symmetric(horizontal=10, vertical=4),
        border_radius=20,
        bgcolor=bg,
        content=ft.Text(status, size=11, weight=ft.FontWeight.W_600, color=fg),
    )


def _format_datetime(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
    if not value:
        return "—"
    try:
        # If it's already a datetime object
        if isinstance(value, datetime):
            dt = value
        else:
            # Parse from ISO string
            # Handle potentially different formats or 'T' separator
            s = str(value).replace("T", " ")
            # Truncate timezone if present (simplistic approach for display)
            if "+" in s:
                s = s.split("+")[0]
            elif "-" in s and s.count("-") > 2: # 2023-01-01-03:00
                 s = s.rsplit("-", 1)[0]
            
            # Try parsing with split seconds
            if "." in s:
                dt = datetime.strptime(s.split(".")[0], "%Y-%m-%d %H:%M:%S")
            else:
                try:
                    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    dt = datetime.strptime(s, "%Y-%m-%d")

        # Format: DD/MM/YYYY HH:MM
        # If time is 00:00:00, maybe just show date? User asked for "fecha completa + la hora pero mejor"
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(value)


def _normalize_datetime_input(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    s = str(value).strip()
    if not s:
        return ""

    s = s.replace("T", " ")
    if "+" in s:
        s = s.split("+")[0]
    elif "-" in s and s.count("-") > 2:
        s = s.rsplit("-", 1)[0]
    s = s.strip()

    if "." in s:
        s = s.split(".", 1)[0]

    if " " in s:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return s

    try:
        datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return s
    return s


def _format_quantity(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
    if value is None:
        return "0"
    try:
        val = float(value)
        # Display as integer
        return f"{int(val):,}".replace(",", ".") # European/Arg style dots for thousands? Or just clean int?
        # User said "son enteros no flotantes". Let's just use standard int string.
        # But thousands separator is nice.
        # Let's stick to simple int() for now, or f"{int(val)}"
        return str(int(val))
    except Exception:
        return str(value)



def _icon_button_or_spacer(visible: bool, **kwargs: Any) -> ft.Control:
    if visible:
        return ft.IconButton(**kwargs)
    return ft.Container(width=24, height=24)


def _maybe_set(obj: Any, name: str, value: Any) -> None:
    if hasattr(obj, name):
        try:
            setattr(obj, name, value)
        except Exception:
            return


def _safe_update_control(control: Any) -> bool:
    """
    Safely update a control if it's attached to the page.
    
    Returns True if update was successful, False otherwise.
    Prevents "Text Control must be added to the page first" errors.
    """
    if control is None:
        return False
    
    try:
        # Check if control has a page reference (is attached to page)
        if not hasattr(control, 'page') or control.page is None:
            return False
        
        # Try to update the control
        control.update()
        return True
    except Exception as e:
        # Log but don't raise - many controls may not be on page yet
        logger.debug(f"Could not update control: {type(control).__name__} - {e}")
        return False


def _safe_update_multiple(*controls: Any) -> int:
    """
    Safely update multiple controls.
    
    Returns the number of controls successfully updated.
    """
    updated_count = 0
    for control in controls:
        if _safe_update_control(control):
            updated_count += 1
    return updated_count


def _is_event_loop_closed(exc: BaseException) -> bool:
    return isinstance(exc, RuntimeError) and "Event loop is closed" in str(exc)


def _safe_page_open(page: ft.Page, dialog: ft.Control, context: str) -> bool:
    if page is None:
        return False
    try:
        if hasattr(page, "open"):
            page.open(dialog)
        else:
            page.dialog = dialog
            dialog.open = True
            page.update()
        return True
    except AssertionError as exc:
        logger.debug("Page open skipped (%s): %s", context, exc)
    except Exception as exc:
        if _is_event_loop_closed(exc):
            logger.debug("Page open skipped (%s): event loop closed", context)
        else:
            logger.exception("Page open failed (%s)", context)
    return False


def _safe_page_close(page: ft.Page, dialog: ft.Control, context: str) -> bool:
    if page is None:
        return False
    try:
        if hasattr(page, "close"):
            page.close(dialog)
        else:
            dialog.open = False
            page.update()
        return True
    except AssertionError as exc:
        logger.debug("Page close skipped (%s): %s", context, exc)
    except Exception as exc:
        if _is_event_loop_closed(exc):
            logger.debug("Page close skipped (%s): event loop closed", context)
        else:
            logger.exception("Page close failed (%s)", context)
    return False


# Flet Tab API compatibility across versions.
try:
    _TAB_PARAMS = set(inspect.signature(ft.Tab).parameters)
except (TypeError, ValueError):
    _TAB_PARAMS = set()


def _tab_header_content(text: Optional[str], icon: Optional[str]) -> ft.Control:
    if icon and text:
        return ft.Row(
            [ft.Icon(icon), ft.Text(text)],
            spacing=6,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        )
    if icon:
        return ft.Icon(icon)
    return ft.Text(text or "")


def make_tab(
    *,
    text: Optional[str] = None,
    icon: Optional[str] = None,
    content: Optional[ft.Control] = None,
) -> ft.Tab:
    kwargs: Dict[str, Any] = {}
    if "content" in _TAB_PARAMS:
        kwargs["content"] = content
    if text is not None and "data" in _TAB_PARAMS:
        kwargs["data"] = text
    if "text" in _TAB_PARAMS:
        kwargs["text"] = text
        if icon is not None and "icon" in _TAB_PARAMS:
            kwargs["icon"] = icon
        return ft.Tab(**kwargs)
    if "label" in _TAB_PARAMS:
        kwargs["label"] = text
        if icon is not None and "icon" in _TAB_PARAMS:
            kwargs["icon"] = icon
        return ft.Tab(**kwargs)
    if "tab_content" in _TAB_PARAMS:
        kwargs["tab_content"] = _tab_header_content(text, icon)
        return ft.Tab(**kwargs)
    tab = ft.Tab(**kwargs)
    if text is not None:
        _maybe_set(tab, "text", text)
        _maybe_set(tab, "label", text)
        _maybe_set(tab, "data", text)
    if icon is not None:
        _maybe_set(tab, "icon", icon)
    _maybe_set(tab, "tab_content", _tab_header_content(text, icon))
    _maybe_set(tab, "content", content)
    return tab


def _style_input(control: Any) -> None:
    name = getattr(control, "__class__", type("x", (), {})).__name__.lower()
    is_dropdown = "dropdown" in name
    is_textfield = "textfield" in name

    # HIGH VISIBILITY STYLE - "Round & Contrast"
    _maybe_set(control, "border_color", "#475569") # Slate 600
    _maybe_set(control, "focused_border_color", COLOR_ACCENT)
    _maybe_set(control, "border_radius", 12)
    _maybe_set(control, "text_size", 14)
    _maybe_set(control, "label_style", ft.TextStyle(color="#1E293B", size=13, weight=ft.FontWeight.BOLD))
    if not is_dropdown:
        _maybe_set(control, "content_padding", ft.padding.symmetric(horizontal=12))

    if is_dropdown:
        _maybe_set(control, "bgcolor", "#F8FAFC")
        _maybe_set(control, "filled", True)
        _maybe_set(control, "border_width", 2)
        # Only force height if NOT dense
        if not getattr(control, "dense", False):
            _maybe_set(control, "height", 50)
        return

    _maybe_set(control, "filled", True)
    _maybe_set(control, "bgcolor", "#F8FAFC")
    _maybe_set(control, "border_width", 1)

    if is_textfield and not is_dropdown:
        # Only force height if NOT dense
        if not getattr(control, "dense", False):
            _maybe_set(control, "height", 50)
        _maybe_set(control, "cursor_color", COLOR_ACCENT)
        _maybe_set(control, "selection_color", "#C7D2FE")


def _number_field(label: str, *, width: int = 160) -> ft.TextField:
    kwargs: Dict[str, Any] = {"label": label, "width": width}
    keyboard_type = getattr(ft, "KeyboardType", None)
    if keyboard_type is not None and hasattr(keyboard_type, "NUMBER"):
        kwargs["keyboard_type"] = keyboard_type.NUMBER
    field = ft.TextField(**kwargs)
    _style_input(field)
    return field


def _dropdown(label: str, options: List[Tuple[Any, str]], value: Any = None, width: Optional[int] = None, on_change: Optional[Callable] = None) -> ft.Dropdown:
    dd = ft.Dropdown(
        label=label,
        value=value,
        hint_text=f"Seleccionar {label.replace('Filtrar ', '').replace('*', '').strip().lower()}..." + (" *" if "*" in label else ""),
        options=[ft.dropdown.Option(str(v) if v is not None else "", t) for v, t in options],
        width=width,
    )
    if on_change is not None:
        _maybe_set(dd, "on_change", on_change)
    # _maybe_set(dd, "enable_search", True)
    _style_input(dd)
    return dd


def _cancel_button(label: str, on_click: Optional[Callable], icon: Optional[Any] = ft.icons.CLOSE_ROUNDED) -> ft.ElevatedButton:
    return cancel_button(
        label,
        on_click,
        icon=icon,
        text_color=COLOR_TEXT,
        bgcolor="#F1F5F9",
        radius=8,
    )


def _date_field(*args, **kwargs) -> ft.TextField:
    page = kwargs.pop("page", None)
    width = kwargs.pop("width", 180)
    label = None
    if args:
        if isinstance(args[0], ft.Page):
            page = args[0]
            if len(args) > 1:
                label = args[1]
            if len(args) > 2:
                width = args[2]
        else:
            label = args[0]
            if len(args) > 1:
                width = args[1]
    if label is None:
        label = kwargs.pop("label", "Fecha")

    tf = ft.TextField(label=str(label), width=width)
    _style_input(tf)
    
    def on_date_change(e):
        if e.control.value:
            new_date = e.control.value.strftime("%Y-%m-%d")
            # Preserve time if already exists in the field
            current_val = str(tf.value or "").strip()
            if " " in current_val:
                # Assuming format "YYYY-MM-DD HH:MM:SS" or similar
                try:
                    time_part = current_val.split(" ", 1)[1]
                    tf.value = f"{new_date} {time_part}"
                except IndexError:
                    tf.value = new_date
            else:
                tf.value = new_date
            
            tf.update()
            if hasattr(tf, "on_submit") and tf.on_submit:
                try:
                    tf.on_submit(None)
                except Exception as e:
                    logger.warning(f"Fallo al llamar on_submit: {e}")
    
    dp = ft.DatePicker(
        on_change=on_date_change,
        help_text="SELECCIONAR FECHA... *",
        cancel_text="CANCELAR",
        confirm_text="ACEPTAR",
        error_format_text="Formato inválido",
        error_invalid_text="Fecha fuera de rango",
    )
    safe_min = datetime(1970, 1, 1)
    safe_max = datetime(2100, 12, 31)
    _maybe_set(dp, "first_date", safe_min)
    _maybe_set(dp, "last_date", safe_max)
    _maybe_set(dp, "current_date", datetime.now())

    def open_picker(e):
        target_page = page or e.control.page
        if not target_page:
            return

        try:
            if hasattr(target_page, "open"):
                target_page.open(dp)
            else:
                # Legacy Flet fallback
                if dp not in target_page.overlay:
                    target_page.overlay.append(dp)
                    target_page.update()
                dp.open = True
                target_page.update()
        except AssertionError:
            pass
        except Exception:
            pass

    tf.suffix = ft.IconButton(
        icon=ft.icons.CALENDAR_MONTH_ROUNDED,
        icon_size=18,
        on_click=open_picker,
    )

    if page is not None and dp not in page.overlay:
        try:
            page.overlay.append(dp)
            page.update()
        except Exception:
            pass
    return tf


def main(page: ft.Page) -> None:
    # 1. Load config FIRST to ensure environment variables (DB_PASSWORD) are available for SchemaSync
    try:
        config = load_config()
    except Exception as e:
        page.add(ft.Text(f"Error loading configuration: {e}", color="red"))
        page.update()
        return

    # --- EARLY SCHEMA SYNC (Added to prevent deadlocks) ---
    # This runs BEFORE any database connection is opened.
    try:
        from desktop_app.services.schema_sync import SchemaSync
        
        # Pass db=None creates no connection pool.
        # We assume schema_sync creates its own connection/subprocess.
        schema_sync_svc = SchemaSync(
            None, 
            sql_path=PROJECT_ROOT / "database" / "database.sql", 
            logs_dir=PROJECT_ROOT / "logs"
        )
        
        if schema_sync_svc.needs_sync():
            # Show a simple loading screen
            page.clean()
            page.add(
                ft.Container(
                    content=ft.Column([
                        ft.ProgressRing(),
                        ft.Text("Verificando y actualizando base de datos...", size=16)
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    alignment=ft.alignment.center,
                    expand=True
                )
            )
            page.update()
            
            result = schema_sync_svc.apply()
            
            if not result.success:
                page.clean()
                page.add(
                    ft.Container(
                        content=ft.Column([
                            ft.Icon(ft.icons.ERROR_OUTLINE, color="red", size=50),
                            ft.Text("Error crítico al actualizar base de datos", size=20, weight=ft.FontWeight.BOLD),
                            ft.Text(result.error or "Error desconocido", color="red"),
                            ft.Text("Revise 'database.sql' o la conexión."),
                        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                        alignment=ft.alignment.center,
                        expand=True
                    )
                )
                page.update()
                # Stop execution here
                return

            page.clean()
            page.update()
            
    except Exception as e:
        print(f"Warning: Schema sync check failed: {e}")
    # -----------------------------------------------------
    # Config already loaded at start of main

    db = Database(
        config.database_url,
        pool_min_size=config.db_pool_min,
        pool_max_size=config.db_pool_max,
    )

    # Load system configuration from DB
    system_name = db.get_config("nombre_sistema", "Nexoryn Tech")
    system_slogan = db.get_config("slogan", "TECH SOLUTION")
    logo_path = db.get_config("logo_path", "")

    # Branding controls for live updates
    sidebar_brand_name = ft.Text(system_name, size=18, weight=ft.FontWeight.W_900, color="#FFFFFF")
    sidebar_brand_slogan = ft.Text(system_slogan, size=10, weight=ft.FontWeight.W_600, color=COLOR_SIDEBAR_TEXT, visible=bool(system_slogan))
    sidebar_brand_logo = ft.Container(
        width=42, height=42,
        bgcolor=COLOR_ACCENT,
        border_radius=12,
        alignment=ft.alignment.center,
        content=(
            ft.Image(src=logo_path, width=30, height=30, fit=ft.ImageFit.CONTAIN)
            if logo_path and Path(logo_path).exists()
            else ft.Icon(ft.icons.BOLT_ROUNDED, color="#FFFFFF", size=24)
        ),
    )

    login_brand_name = ft.Text(system_name, size=28, weight=ft.FontWeight.W_900, color=COLOR_TEXT)
    login_brand_logo = ft.Container(
        content=(
            ft.Image(src=logo_path, width=80, height=80, fit=ft.ImageFit.CONTAIN)
            if logo_path and Path(logo_path).exists()
            else ft.Icon(ft.icons.STOREFRONT_ROUNDED, size=56, color=COLOR_ACCENT)
        ),
        bgcolor=f"{COLOR_ACCENT}15",
        padding=20,
        border_radius=20,
    )

    def update_branding(name, slogan, logo):
        """Update branding elements in the sidebar and login immediately."""
        sidebar_brand_name.value = name
        sidebar_brand_slogan.value = slogan
        sidebar_brand_slogan.visible = bool(slogan)
        
        login_brand_name.value = name
        
        if logo and logo.strip() and Path(logo).exists():
            sidebar_brand_logo.content = ft.Image(src=logo, width=30, height=30, fit=ft.ImageFit.CONTAIN)
            login_brand_logo.content = ft.Image(src=logo, width=80, height=80, fit=ft.ImageFit.CONTAIN)
            login_brand_logo.padding = 0
        else:
            sidebar_brand_logo.content = ft.Icon(ft.icons.BOLT_ROUNDED, color="#FFFFFF", size=24)
            login_brand_logo.content = ft.Icon(ft.icons.STOREFRONT_ROUNDED, size=56, color=COLOR_ACCENT)
            login_brand_logo.padding = 20
        
        try:
            _safe_update_multiple(
                sidebar_brand_name,
                sidebar_brand_slogan,
                sidebar_brand_logo,
                login_brand_name,
                login_brand_logo
            )
        except Exception as e:
            logger.warning(f"Falló al actualizar elementos de marca: {e}")

    page.title = system_name
    page.window_width = 1280
    page.window_height = 860
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    page.fonts = {"Roboto": "Roboto-Regular.ttf"}

    def get_company_config() -> dict:
        """Get company configuration for PDF generation."""
        if not db:
            return {}
        try:
            cfg = db.fetch_config_sistema()
            return {
                "nombre_sistema": cfg.get("nombre_sistema", {}).get("valor", "NEXORYN TECH"),
                "razon_social": cfg.get("razon_social", {}).get("valor", ""),
                "cuit_empresa": cfg.get("cuit_empresa", {}).get("valor", ""),
                "domicilio_empresa": cfg.get("domicilio_empresa", {}).get("valor", ""),
                "telefono_empresa": cfg.get("telefono_empresa", {}).get("valor", ""),
                "email_empresa": cfg.get("email_empresa", {}).get("valor", ""),
                "slogan": cfg.get("slogan", {}).get("valor", "Soluciones tecnológicas y logísticas"),
            }
        except Exception:
            return {}

    pending_invoice_pdf_download: Optional[Dict[str, Any]] = None

    def _safe_filename_token(value: Any, fallback: str = "") -> str:
        raw = str(value or "").strip()
        if not raw:
            return fallback
        invalid = '<>:"/\\|?*'
        cleaned = "".join(ch for ch in raw if ch not in invalid)
        compact = " ".join(cleaned.split())
        return compact or fallback

    def _build_invoice_pdf_filename(doc: Dict[str, Any]) -> str:
        doc_type = _safe_filename_token(doc.get("tipo_documento"), "Comprobante")
        serie = _safe_filename_token(doc.get("numero_serie") or doc.get("numero"), "")
        if not serie:
            serie = _safe_filename_token(doc.get("id"), datetime.now().strftime("%Y%m%d_%H%M%S"))
        base = f"{doc_type}_{serie}".strip("_")
        return f"{base}.pdf"

    def _build_invoice_pdf_payload(doc_id: int) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]]:
        if not db:
            return None

        doc = db.get_document_full(doc_id)
        if not doc:
            show_toast("Error al recuperar datos del documento", kind="error")
            return None

        ent = db.get_entity_detail(doc.get("id_entidad_comercial")) if db else {}

        # Recalcular desglose fiscal para impresión/exportación.
        # Esto evita que PDFs de documentos con IVA histórico en 0 (por detalle legado)
        # muestren un IVA incorrecto cuando el artículo sí tiene alícuota fiscal.
        fiscal_lines: List[Dict[str, Any]] = []
        try:
            fiscal_pricing = _build_doc_fiscal_pricing_for_afip(db, doc)
            doc["neto"] = float(quantize_2(to_decimal(fiscal_pricing.get("neto"), to_decimal("0"))))
            doc["iva_total"] = float(quantize_2(to_decimal(fiscal_pricing.get("iva_total"), to_decimal("0"))))
            doc["total"] = float(quantize_2(to_decimal(fiscal_pricing.get("total"), to_decimal("0"))))
            doc["iva_breakdown"] = fiscal_pricing.get("iva_breakdown") or []
            raw_lines = fiscal_pricing.get("items") if isinstance(fiscal_pricing, dict) else []
            fiscal_lines = raw_lines if isinstance(raw_lines, list) else []
        except Exception as exc:
            logger.warning(f"No se pudo recalcular desglose fiscal para impresión/exportación: {exc}")
            doc["iva_breakdown"] = doc.get("iva_breakdown") or []
            fiscal_lines = []

        items_data: List[Dict[str, Any]] = []
        for idx, item in enumerate(doc.get("items", [])):
            article_id = item.get("id_articulo")
            art = db.get_article_simple(article_id) if article_id is not None else None
            item_copy = item.copy()
            item_copy["articulo_nombre"] = art["nombre"] if art else f"Artículo {article_id or '-'}"
            raw_code = (art or {}).get("codigo")
            article_code = str(raw_code).strip() if raw_code is not None else ""
            if not article_code:
                article_code = str(article_id or "-")
            item_copy["articulo_codigo"] = article_code
            item_copy["unidades_por_bulto"] = (
                item.get("unidades_por_bulto_historico")
                if item.get("unidades_por_bulto_historico") is not None
                else (art or {}).get("unidades_por_bulto")
            )

            raw_unit_abbr = str((art or {}).get("unidad_abreviatura") or "").strip()
            if raw_unit_abbr:
                unit_abbr = raw_unit_abbr.upper()
            else:
                raw_unit_name = str((art or {}).get("unidad_medida") or "").strip()
                unit_abbr = raw_unit_name[:3].upper() if raw_unit_name else "UNI"
            item_copy["unidad_abreviatura"] = unit_abbr or "UNI"

            fiscal_line = fiscal_lines[idx] if idx < len(fiscal_lines) and isinstance(fiscal_lines[idx], dict) else {}
            alicuota_iva = float(
                quantize_2(
                    to_decimal(
                        fiscal_line.get(
                            "porcentaje_iva_fiscal",
                            fiscal_line.get("porcentaje_iva", item.get("porcentaje_iva", 0)),
                        ),
                        to_decimal(item.get("porcentaje_iva"), to_decimal("0")),
                    )
                )
            )
            bonificacion_pct = float(
                quantize_2(
                    to_decimal(
                        fiscal_line.get("descuento_porcentaje", item.get("descuento_porcentaje", 0)),
                        to_decimal(item.get("descuento_porcentaje"), to_decimal("0")),
                    )
                )
            )
            subtotal_sin_iva = float(
                quantize_2(
                    to_decimal(
                        fiscal_line.get(
                            "neto_fiscal_linea",
                            fiscal_line.get("linea_neta", item.get("total_linea", 0)),
                        ),
                        to_decimal(item.get("total_linea"), to_decimal("0")),
                    )
                )
            )
            subtotal_con_iva = float(
                quantize_2(
                    to_decimal(
                        fiscal_line.get("linea_neta", item.get("total_linea", 0)),
                        to_decimal(item.get("total_linea"), to_decimal("0")),
                    )
                )
            )
            item_copy["afip_alicuota_iva"] = alicuota_iva
            item_copy["afip_bonificacion_pct"] = bonificacion_pct
            item_copy["afip_subtotal_sin_iva"] = subtotal_sin_iva
            item_copy["afip_subtotal_con_iva"] = subtotal_con_iva
            items_data.append(item_copy)

        return doc, (ent or {}), items_data

    def _on_invoice_pdf_save_result(e: ft.FilePickerResultEvent) -> None:
        nonlocal pending_invoice_pdf_download
        payload = pending_invoice_pdf_download
        pending_invoice_pdf_download = None
        if not payload:
            return

        selected_path = str(getattr(e, "path", "") or "").strip()
        if not selected_path:
            return

        target_path = Path(selected_path)
        if target_path.suffix.lower() != ".pdf":
            target_path = target_path.with_suffix(".pdf")

        generated_temp_path: Optional[str] = None
        try:
            generated_temp_path = generate_pdf(
                payload["doc_data"],
                payload["entity_data"],
                payload["items_data"],
                kind="invoice",
                company_config=get_company_config(),
                show_prices=bool(payload.get("include_prices", True)),
            )
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(generated_temp_path, str(target_path))
            show_toast(f"PDF guardado: {target_path.name}", kind="success")
        except Exception as exc:
            show_toast(f"Error al guardar PDF: {exc}", kind="error")
        finally:
            if generated_temp_path:
                try:
                    Path(generated_temp_path).unlink(missing_ok=True)
                except Exception:
                    pass

    invoice_pdf_save_picker = ft.FilePicker(on_result=_on_invoice_pdf_save_result)
    try:
        if invoice_pdf_save_picker not in page.overlay:
            page.overlay.append(invoice_pdf_save_picker)
    except Exception as exc:
        logger.warning(f"Falló al registrar selector de guardado PDF: {exc}")

    def print_document_external(doc_id: int, *, include_prices: bool = True, copies: int = 1) -> None:
        """Global helper to print an invoice/receipt document."""
        try:
            payload = _build_invoice_pdf_payload(int(doc_id))
            if not payload:
                return
            doc, ent, items_data = payload

            # Generate PDF and print on default Windows printer (with fallback to open PDF).
            _, printed_directly = generate_pdf_and_print(
                doc,
                ent,
                items_data,
                kind="invoice",
                company_config=get_company_config(),
                show_prices=include_prices,
                copies=copies,
            )
            if printed_directly:
                show_toast("Comprobante enviado a impresión.", kind="success")
            else:
                show_toast(
                    "No se pudo enviar a la impresora/spooler en forma directa. Se abrió el PDF para impresión manual.",
                    kind="warning",
                )
        except Exception as e:
            show_toast(f"Error al imprimir: {e}", kind="error")

    def save_document_pdf_external(doc_id: int, *, include_prices: bool = True) -> None:
        nonlocal pending_invoice_pdf_download
        try:
            payload = _build_invoice_pdf_payload(int(doc_id))
            if not payload:
                return
            doc, ent, items_data = payload
            pending_invoice_pdf_download = {
                "doc_data": doc,
                "entity_data": ent,
                "items_data": items_data,
                "include_prices": bool(include_prices),
            }
            filename = _build_invoice_pdf_filename(doc)
            try:
                invoice_pdf_save_picker.save_file(
                    dialog_title="Guardar PDF de comprobante",
                    file_name=filename,
                )
            except TypeError:
                invoice_pdf_save_picker.save_file(file_name=filename)
        except Exception as exc:
            pending_invoice_pdf_download = None
            show_toast(f"Error al preparar descarga de PDF: {exc}", kind="error")

    def ask_print_options(
        doc_label: str,
        on_print: Callable[[bool], None],
        *,
        action_label: str = "Imprimir",
        action_icon: str = ft.icons.PRINT_ROUNDED,
        error_action_label: str = "procesar",
    ) -> None:
        """Display options with price visibility toggle before generating PDF."""
        include_prices_switch = ft.Switch(
            label="Incluir precios e importes",
            value=True,
            active_color=COLOR_ACCENT,
        )
        
        def _close(_: Any) -> None:
            _safe_page_close(page, print_options_dialog, "ask_print_options")

        def _confirm_print(_: Any) -> None:
            include_prices = bool(include_prices_switch.value)
            _close(None)
            try:
                on_print(include_prices)
            except Exception as exc:
                show_toast(f"Error al {error_action_label} {doc_label}: {exc}", kind="error")

        print_options_dialog.title = ft.Text(f"Opciones: {doc_label}", size=20, weight=ft.FontWeight.BOLD)
        print_options_dialog.content = ft.Container(
            content=ft.Column(
                [
                    ft.Text(
                        "Por seguridad, podés ocultar precios e importes en el PDF.",
                        size=13,
                        color=COLOR_TEXT_MUTED,
                    ),
                    include_prices_switch,
                ],
                spacing=10,
                tight=True,
            ),
            width=460,
            padding=ft.padding.symmetric(vertical=8),
        )
        print_options_dialog.shape = ft.RoundedRectangleBorder(radius=16)
        print_options_dialog.actions = [
            _cancel_button("Cancelar", on_click=_close),
            ft.ElevatedButton(
                action_label,
                icon=action_icon,
                bgcolor=COLOR_ACCENT,
                color="#FFFFFF",
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                on_click=_confirm_print,
            ),
        ]
        _safe_page_open(page, print_options_dialog, "ask_print_options")

    def request_invoice_print(doc_id: int, *, copies: int = 1) -> None:
        ask_print_options(
            "comprobante",
            lambda include_prices: print_document_external(
                int(doc_id),
                include_prices=include_prices,
                copies=copies,
            ),
            action_label="Imprimir",
            action_icon=ft.icons.PRINT_ROUNDED,
            error_action_label="imprimir",
        )

    def request_invoice_download(doc_id: int) -> None:
        ask_print_options(
            "comprobante",
            lambda include_prices: save_document_pdf_external(
                int(doc_id),
                include_prices=include_prices,
            ),
            action_label="Guardar PDF",
            action_icon=ft.icons.DOWNLOAD_ROUNDED,
            error_action_label="guardar PDF de",
        )


    # --- SHARED DIALOGS ---
    # --- SHARED DIALOGS (Custom Modal to allow nesting) ---
    _FORM_SCROLL_BOTTOM_KEY = "form_scroll_bottom_anchor"
    _form_title = ft.Text(size=18, weight=ft.FontWeight.BOLD)
    _form_content_area = ft.Container()
    _form_actions_area = ft.Row(alignment=ft.MainAxisAlignment.END, spacing=10)
    _form_header = ft.Row([_form_title, ft.IconButton(ft.icons.CLOSE_ROUNDED, icon_size=20, on_click=lambda _: close_form())], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
    _form_scroll_bottom_anchor = ft.Container(key=_FORM_SCROLL_BOTTOM_KEY, height=1, opacity=0)
    _form_scroll_column = ft.Column(
        [
            _form_header,
            _form_content_area,
            _form_actions_area,
            _form_scroll_bottom_anchor,
        ],
        tight=True,
        spacing=15,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
    
    # This replaces the native AlertDialog to allow multiple layers of modals
    form_dialog = ft.Container(
        content=ft.Card(
            elevation=20,
            shape=ft.RoundedRectangleBorder(radius=12),
            content=ft.Container(
                padding=24,
                # Flexible sizing based on content
                content=_form_scroll_column
            )
        ),
        bgcolor="#80000000",
        alignment=ft.alignment.center,
        visible=False,
        expand=True,
        left=0, top=0, right=0, bottom=0
    )
    page.overlay.append(form_dialog)

    def close_form(e=None):
        # Avoid UI updates while the window is closing and guard against
        # sporadic Flet uid assertions during updates.
        if window_is_closing:
            return
        form_dialog.visible = False
        if _safe_update_control(form_dialog):
            return
        try:
            page.update()
        except AssertionError as exc:
            logger.debug(f"Close form update skipped (uid missing): {exc}")
        except Exception as exc:
            if _is_event_loop_closed(exc):
                logger.debug("Close form update skipped: event loop closed")
            else:
                logger.exception(f"Close form update failed: {exc}")

    def open_form(title, content, actions):
        _form_title.value = title
        _form_content_area.content = content
        _form_actions_area.controls = actions
        _form_header.visible = True
        
        form_dialog.visible = True
        # Move to front of overlay
        if form_dialog in page.overlay:
            page.overlay.remove(form_dialog)
        page.overlay.append(form_dialog)
        page.update()
    
    # ----------------------
    page.spacing = 0
    try:
        AsyncSelect.set_default_page(page)
    except Exception:
        pass
    
    # Set Spanish locale for date pickers and other components
    page.locale_configuration = ft.LocaleConfiguration(
        current_locale=ft.Locale("es", "AR"),
        supported_locales=[ft.Locale("es", "AR")],
    )
    page.bgcolor = COLOR_BG
    page.window_prevent_close = False
    window_is_closing = False
    logout_logged = False

    def _run_on_ui(fn: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        if window_is_closing or page is None:
            return
        try:
            if hasattr(page, "run_task"):
                async def _do() -> None:
                    try:
                        fn(*args, **kwargs)
                    except Exception as exc:
                        logger.debug(f"UI task error: {exc}")
                task = _do()
                try:
                    page.run_task(task)
                except TypeError:
                    task.close()
                    page.run_task(_do)
                except Exception:
                    task.close()
                    raise
                return
        except Exception as exc:
            logger.debug(f"UI scheduling error: {exc}")
        try:
            fn(*args, **kwargs)
        except Exception as exc:
            logger.debug(f"UI update error: {exc}")

    def _run_in_background(fn: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        if window_is_closing:
            return
        try:
            if hasattr(page, "run_thread"):
                if kwargs:
                    def _wrapped() -> None:
                        fn(*args, **kwargs)
                    page.run_thread(_wrapped)
                else:
                    page.run_thread(fn, *args)
                return
        except Exception as exc:
            logger.debug(f"Background scheduling error: {exc}")
        threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()

    # Session state
    current_user: Dict[str, Any] = {}
    
    # Inactivity Timeout Configuration (5 minutes = 300 seconds)
    INACTIVITY_TIMEOUT = 300
    last_activity_time = time.time()

    def mark_activity(e=None):
        nonlocal last_activity_time
        # Only track activity if a user is logged in
        if current_user and current_user.get("id"):
            last_activity_time = time.time()

    # Best-effort fallback. Keep generic event tracking, but avoid global
    # keyboard hook because it can interfere with numpad digit input on
    # Windows/Flet in some environments.
    page.on_event = mark_activity
    
    db_error: Optional[str] = None
    local_ip = "127.0.0.1"
    try:
        # DB compatibility checks are handled by schema sync on login
    
        # Initialize Backup Service & Scheduler
        # Old standard backup system disabled
        # backup_service = BackupService(pg_bin_path=config.pg_bin_path)

        # Initialize Professional Backup System Scheduler
        professional_scheduler = None
        try:
            # Import and initialize professional backup system
            try:
                from desktop_app.services.backup_manager import BackupManager
            except ImportError:
                from services.backup_manager import BackupManager  # type: ignore
            from apscheduler.schedulers.background import BackgroundScheduler as ProScheduler
            from apscheduler.triggers.cron import CronTrigger as ProCronTrigger

            professional_backup_manager = BackupManager(db, pg_bin_path=config.pg_bin_path)
            professional_scheduler = ProScheduler(timezone='America/Argentina/Buenos_Aires')

            # Schedule professional backups: FULL, DIFERENCIAL, INCREMENTAL
            def run_professional_backup(backup_type):
                try:
                    resultado = professional_backup_manager.execute_scheduled_backup(backup_type)
                    if resultado['exitoso']:
                        logger.info(f"Backup profesional {backup_type} completado exitosamente")
                    else:
                        logger.error(f"Backup profesional {backup_type} fallo: {resultado['mensaje']}")
                except Exception as e:
                    logger.error(f"Error en backup profesional {backup_type}: {e}")

            # FULL backup: Día 1 de cada mes a las 00:00
            professional_scheduler.add_job(
                lambda: run_professional_backup('FULL'),
                ProCronTrigger(day=1, hour=0, minute=0),
                id='professional_backup_full',
                name='Backup FULL (Mensual)',
                max_instances=1,
                replace_existing=True
            )

            # DIFERENCIAL backup: Domingo a las 23:30
            professional_scheduler.add_job(
                lambda: run_professional_backup('DIFERENCIAL'),
                ProCronTrigger(day_of_week='sun', hour=23, minute=30),
                id='professional_backup_diferencial',
                name='Backup DIFERENCIAL (Semanal)',
                max_instances=1,
                replace_existing=True
            )

            # INCREMENTAL backup: Diario a las 23:00
            professional_scheduler.add_job(
                lambda: run_professional_backup('INCREMENTAL'),
                ProCronTrigger(hour=23, minute=0),
                id='professional_backup_incremental',
                name='Backup INCREMENTAL (Diario)',
                max_instances=1,
                replace_existing=True
            )

            # Validación diaria de backups
            def run_backup_validation():
                try:
                    resultado = professional_backup_manager.validate_all_backups()
                    logger.info(f"Validacion de backups completada: {resultado['validos']}/{resultado['total']} valid")
                except Exception as e:
                    logger.error(f"Error en validación de backups: {e}")

            professional_scheduler.add_job(
                run_backup_validation,
                ProCronTrigger(hour=1, minute=0),
                id='backup_validation',
                name='Validación de Backups',
                max_instances=1,
                replace_existing=True
            )

            professional_scheduler.start()
            logger.info("Planificador del sistema profesional de backups iniciado correctamente")

        except Exception as e:
            logger.warning(f"No se pudo inicializar sistema profesional de backups: {e}")
            # Fallback to legacy system - initialize with basic imports
            professional_scheduler = BackgroundScheduler()
            from apscheduler.triggers.cron import CronTrigger

            # Try to use legacy backup service for fallback
            try:
                backup_service_fallback = BackupService(pg_bin_path=config.pg_bin_path, db=db)
                
                def run_scheduled_backup(btype):
                    try:
                        logger.info(f"Sistema de backups en fallback: ejecutando backup {btype}")
                        backup_service_fallback.backup()
                    except Exception as e:
                        logger.error(f"Backup programado {btype} falló: {e}")
                
                # Fallback jobs using legacy BackupService
                professional_scheduler.add_job(lambda: run_scheduled_backup("daily"), CronTrigger(hour=23, minute=0), id="backup_daily", max_instances=1, replace_existing=True)
                professional_scheduler.add_job(lambda: run_scheduled_backup("weekly"), CronTrigger(day_of_week="sun", hour=23, minute=30), id="backup_weekly", max_instances=1, replace_existing=True)
                professional_scheduler.add_job(lambda: run_scheduled_backup("monthly"), CronTrigger(day=1, hour=0, minute=0), id="backup_monthly", max_instances=1, replace_existing=True)
                logger.info("Sistema de backups en fallback: jobs programados con BackupService")
            except Exception as fallback_err:
                logger.warning(f"No se pudo configurar jobs de fallback: {fallback_err}")

            professional_scheduler.start()

        # Use professional scheduler as main scheduler
        scheduler = professional_scheduler
        
        afip: Optional[AfipService] = None
        if config.afip_cuit and config.afip_cert and config.afip_key:
            afip = AfipService(
                cuit=config.afip_cuit,
                cert_path=config.afip_cert,
                key_path=config.afip_key,
                production=config.afip_prod,
            )
        
        # Try to get local IP for logging
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
        except Exception as e:
            logger.warning(f"Falló al determinar IP local: {e}")
        db.current_ip = local_ip  # Set IP for login logging before auth
        
        page.bgcolor = COLOR_BG # Avoid white flashes on background
    except Exception as exc:
        db_error = str(exc)

    # Toast Manager initialization is handled on demand
    def show_toast(message: str, kind: str = "info") -> None:
        if not hasattr(page, "toast_manager"):
            page.toast_manager = ToastManager(page)
        
        page.toast_manager.show(message, kind)

    def provider_error() -> Exception:
        return RuntimeError(db_error or "No se pudo conectar a la base de datos.")

    def get_db_or_toast() -> Optional[Database]:
        if db is None or db_error:
            show_toast(db_error or "No se pudo conectar a la base de datos.", kind="error")
            return None
        return db

    status_badge = ft.Container(
        padding=ft.padding.symmetric(horizontal=10, vertical=6),
        border_radius=999,
        bgcolor="#DCFCE7" if db and not db_error else "#FEE2E2",
        border=ft.border.all(1, "#BBF7D0" if db and not db_error else "#FECACA"),
        content=ft.Row([], spacing=6, tight=True),
    )
    if db_error:
        status_badge.tooltip = db_error

    confirm_dialog = ft.AlertDialog(modal=True)
    print_options_dialog = ft.AlertDialog(modal=True)
    discount_limit_dialog = ft.AlertDialog(modal=True)
    confirm_dialog_state: Dict[str, Any] = {"on_confirm": None, "is_open": False}

    def _is_keydown_event(event: Any) -> bool:
        event_type = str(
            getattr(event, "event_type", "") or getattr(event, "type", "") or getattr(event, "data", "")
        ).strip().lower()
        if not event_type:
            return True
        if event_type in {"keyup", "up", "key_up"}:
            return False
        if event_type in {"keydown", "down", "key_down"}:
            return True
        return "up" not in event_type

    def _is_keyup_event(event: Any) -> bool:
        event_type = str(
            getattr(event, "event_type", "") or getattr(event, "type", "") or getattr(event, "data", "")
        ).strip().lower()
        if not event_type:
            return False
        if event_type in {"keyup", "up", "key_up"}:
            return True
        if event_type in {"keydown", "down", "key_down"}:
            return False
        return "up" in event_type and "down" not in event_type

    def _cancel_windows_menu_mode() -> None:
        if not str(sys.platform).lower().startswith("win"):
            return
        try:
            import ctypes

            user32 = ctypes.windll.user32
            hwnd = user32.GetForegroundWindow()
            if not hwnd:
                return
            wm_cancelmode = 0x001F
            wm_exitmenuloop = 0x0212
            user32.PostMessageW(hwnd, wm_cancelmode, 0, 0)
            user32.PostMessageW(hwnd, wm_exitmenuloop, 0, 0)
        except Exception:
            logger.debug("No se pudo cancelar el modo menú de Windows", exc_info=True)

    def _cancel_windows_menu_mode_debounced() -> None:
        _cancel_windows_menu_mode()

        def _delayed_cancel() -> None:
            for _ in range(10):
                time.sleep(0.03)
                _cancel_windows_menu_mode()

        _run_in_background(_delayed_cancel)

    def _is_confirm_dialog_open() -> bool:
        return bool(
            confirm_dialog_state.get("is_open")
            or getattr(confirm_dialog, "open", False)
            or getattr(confirm_dialog, "visible", False)
        )

    def _confirm_dialog_from_shortcut() -> bool:
        if not _is_confirm_dialog_open():
            return False
        on_confirm = confirm_dialog_state.get("on_confirm")
        if not callable(on_confirm):
            return False
        confirm_dialog_state["on_confirm"] = None
        confirm_dialog_state["is_open"] = False
        _safe_page_close(page, confirm_dialog, "ask_confirm_shortcut")
        try:
            on_confirm()
        except Exception as exc:
            show_toast(f"Error: {exc}", kind="error")
        return True

    def show_discount_limit_modal(message: str) -> None:
        def close(_: Any = None) -> None:
            _safe_page_close(page, discount_limit_dialog, "discount_limit_modal")

        discount_limit_dialog.title = ft.Text("Descuento inválido", size=20, weight=ft.FontWeight.BOLD)
        discount_limit_dialog.content = ft.Container(
            content=ft.Text(message, size=14, color=COLOR_TEXT_MUTED),
            padding=ft.padding.symmetric(vertical=10),
        )
        discount_limit_dialog.shape = ft.RoundedRectangleBorder(radius=16)
        discount_limit_dialog.actions = [
            ft.ElevatedButton(
                "Entendido",
                bgcolor=COLOR_WARNING,
                color="#FFFFFF",
                on_click=close,
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
            ),
        ]
        _safe_page_open(page, discount_limit_dialog, "discount_limit_modal")

    def ask_confirm(title: str, message: str, confirm_label: str, on_confirm, button_color: str = None) -> None:
        # Create refs to control focus programmatically
        confirm_btn_ref = ft.Ref[ft.ElevatedButton]()
        cancel_btn_ref = ft.Ref[ft.ElevatedButton]()
        confirm_focus_state: Dict[str, str] = {"action": "confirm"}
        
        # Track previous handler to restore it later
        previous_handler = getattr(page, "on_keyboard_event", None)

        def _set_focused_action(action: str) -> None:
            confirm_focus_state["action"] = action

        def _is_confirm_keydown_event(event: Any) -> bool:
            return _is_keydown_event(event)

        def close(_: Any = None) -> None:
            # Restore previous keyboard handler
            if previous_handler:
                page.on_keyboard_event = previous_handler
            else:
                page.on_keyboard_event = None
            
            confirm_dialog_state["on_confirm"] = None
            confirm_dialog_state["is_open"] = False
            _safe_page_close(page, confirm_dialog, "ask_confirm")

        def do_confirm(_: Any = None) -> None:
            close(None)
            try:
                on_confirm()
            except Exception as exc:
                show_toast(f"Error: {exc}", kind="error")

        def _on_confirm_dialog_key(e: ft.KeyboardEvent):
            key = str(getattr(e, "key", "") or "").strip().lower().replace("_", "").replace("-", "").replace(" ", "")

            if key in {"f10"}:
                if _is_confirm_keydown_event(e):
                    do_confirm(None)
                elif _is_keyup_event(e):
                    _cancel_windows_menu_mode_debounced()
                return

            if not _is_confirm_keydown_event(e):
                return

            if key in {"arrowleft", "left"}:
                _set_focused_action("cancel")
                if cancel_btn_ref.current:
                    cancel_btn_ref.current.focus()
                return

            if key in {"arrowright", "right"}:
                _set_focused_action("confirm")
                if confirm_btn_ref.current:
                    confirm_btn_ref.current.focus()
                return

            if key in {"enter", "numpadenter", "return"}:
                if confirm_focus_state.get("action") == "cancel":
                    close(None)
                else:
                    do_confirm(None)
                return

            if key in {"esc", "escape"}:
                close(None)
                return

        # Install local handler
        page.on_keyboard_event = _on_confirm_dialog_key

        final_color = button_color if button_color else COLOR_ERROR
        confirm_dialog_state["on_confirm"] = on_confirm
        confirm_dialog_state["is_open"] = True
        confirm_dialog.title = ft.Text(title, size=20, weight=ft.FontWeight.BOLD)
        confirm_dialog.content = ft.Container(
            content=ft.Text(message, size=14, color=COLOR_TEXT_MUTED),
            padding=ft.padding.symmetric(vertical=10)
        )
        confirm_dialog.shape = ft.RoundedRectangleBorder(radius=16)
        
        confirm_dialog.actions = [
            ft.TextButton(
                "Cancelar", 
                ref=cancel_btn_ref,
                on_click=close,
                on_focus=lambda _: _set_focused_action("cancel"),
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
            ),
            ft.ElevatedButton(
                confirm_label, 
                ref=confirm_btn_ref,
                bgcolor=final_color, 
                color="#FFFFFF", 
                on_click=do_confirm,
                on_focus=lambda _: _set_focused_action("confirm"),
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                autofocus=True # Focus this button by default
            ),
        ]
        opened = _safe_page_open(page, confirm_dialog, "ask_confirm")
        if not opened:
            confirm_dialog_state["is_open"] = False
            # Restore handler if open failed
            if previous_handler:
                 page.on_keyboard_event = previous_handler
            else:
                page.on_keyboard_event = None
        else:
            # Force focus on confirm button; run an immediate and delayed pass so
            # Enter works as soon as the dialog appears on slower clients.
            def _focus_confirm_button() -> None:
                if confirm_btn_ref.current:
                    _set_focused_action("confirm")
                    confirm_btn_ref.current.focus()

            def _delayed_focus():
                try:
                    time.sleep(0.08)
                    _run_on_ui(_focus_confirm_button)
                except Exception:
                    pass
            _run_on_ui(_focus_confirm_button)
            _run_in_background(_delayed_focus)

    marcas_values: List[str] = []
    rubros_values: List[str] = []
    tipos_iva_values: List[Dict[str, Any]] = []
    unidades_values: List[Dict[str, Any]] = []
    proveedores_values: List[Dict[str, Any]] = []
    tipos_porcentaje_values: List[Dict[str, Any]] = []
    
    def reload_catalogs() -> None:
        nonlocal marcas_values, rubros_values, tipos_iva_values, unidades_values, proveedores_values, tipos_porcentaje_values
        if db is None or db_error:
            marcas_values = []
            rubros_values = []
            tipos_iva_values = []
            unidades_values = []
            proveedores_values = []
            tipos_porcentaje_values = []
            return
        marcas_values = db.list_marcas()
        rubros_values = db.list_rubros()
        tipos_iva_values = db.fetch_tipos_iva(limit=100)
        unidades_values = db.list_unidades_medida()
        proveedores_values = db.list_proveedores()
        tipos_porcentaje_values = db.fetch_tipos_porcentaje(limit=100)
        
        # Also refresh filter dropdowns if they exist
        try:
            refresh_articles_catalogs()
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de artículos: {e}")
        try:
            refresh_movimientos_catalogs()
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de movimientos: {e}")
        try:
            refresh_remitos_catalogs()
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de remitos: {e}")
        try:
            refresh_documentos_catalogs()
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de documentos: {e}")
        try:
            refresh_pagos_catalogs()
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de pagos: {e}")

    # reload_catalogs() will be called at the end of main after all controls are defined

    def dropdown_editor(values_provider: Callable[[], Sequence[str]], *, width: int, empty_label: str = "Seleccionar opción...") -> Any:
        def build(value: Any, row: Dict[str, Any], setter) -> ft.Control:
            values = list(values_provider() or [])
            options: List[ft.dropdown.Option] = [ft.dropdown.Option(name, name) for name in values]

            selected = None
            if isinstance(value, str) and value.strip() and value.strip() != "—":
                selected = value.strip()
                if selected not in values:
                    options.insert(0, ft.dropdown.Option(selected, selected))

            dd = ft.Dropdown(
                options=options,
                value=selected,
                hint_text=empty_label,
                width=width,
                on_change=lambda e: setter(e.control.value),
            )
            _style_input(dd)
            return dd

        return build

    def unidad_medida_editor(*, width: int = 140) -> Any:
        def build(value: Any, row: Dict[str, Any], setter) -> ft.Control:
            options: List[ft.dropdown.Option] = []
            option_keys: set = set()
            for u in unidades_values:
                nombre = str(u.get("nombre") or "").strip()
                abreviatura = str(u.get("abreviatura") or "").strip()
                key = abreviatura or nombre
                if not key:
                    continue
                label = f"{nombre} ({abreviatura})" if abreviatura else nombre
                key_str = str(key)
                options.append(ft.dropdown.Option(key_str, label))
                option_keys.add(key_str)

            selected = row.get("unidad_abreviatura") or row.get("unidad_medida") or value
            selected_key = str(selected).strip() if selected is not None else ""
            if selected_key and selected_key not in option_keys:
                options.insert(0, ft.dropdown.Option(selected_key, selected_key))
                option_keys.add(selected_key)

            dd = ft.Dropdown(
                options=options,
                value=selected_key if selected_key in option_keys else None,
                hint_text="Seleccionar unidad...",
                width=width,
                on_change=lambda e: setter(e.control.value),
            )
            _style_input(dd)
            return dd

        return build

    def async_select_editor(loader: Callable[[str, int, int], Any], *, label: str, width: int) -> Any:
        def build(value: Any, row: Dict[str, Any], setter) -> ft.Control:
            return AsyncSelect(
                label=label,
                loader=loader,
                width=width,
                value=value,
                on_change=setter,
            )
        return build

    def boolean_editor() -> Any:
        """Editor para campos booleanos usando Switch."""
        def build(value: Any, row: Dict[str, Any], setter) -> ft.Control:
            return ft.Switch(
                value=bool(value),
                on_change=lambda e: setter(e.control.value),
            )
        return build

    def _entity_tipo_label(tipo: Any) -> str:
        tipo_upper = str(tipo or "").strip().upper()
        if tipo_upper == "CLIENTE":
            return "Cliente"
        if tipo_upper == "PROVEEDOR":
            return "Proveedor"
        if tipo_upper == "AMBOS":
            return "Cliente/Proveedor"
        return ""

    def _format_entity_option(
        row: Dict[str, Any],
        *,
        include_tipo: bool = True,
        force_tipo: Optional[str] = None,
    ) -> Dict[str, Any]:
        entity_id = row.get("id")
        entity_id_text = str(entity_id).strip() if entity_id is not None else ""
        name = str(row.get("nombre_completo") or row.get("razon_social") or "").strip() or "Sin nombre"
        tipo_label = force_tipo if force_tipo is not None else (_entity_tipo_label(row.get("tipo")) if include_tipo else "")
        tipo_suffix = f" ({tipo_label})" if tipo_label else ""
        primary = f"{name} (Cod: {entity_id_text}){tipo_suffix}" if entity_id_text else f"{name}{tipo_suffix}"

        address = str(row.get("domicilio") or "").strip() or "Sin dirección"
        secondary = f"Dirección: {address}"
        full_label = f"{primary}\n{secondary}"
        selected_label = f"{address}: {name} ({entity_id_text}){tipo_suffix}" if entity_id_text else f"{address}: {name}{tipo_suffix}"

        return {
            "value": entity_id,
            "label": full_label,
            "selected_label": selected_label,
            "tooltip": full_label,
        }

    # --- AsyncSelect Loaders ---
    COMPROBANTE_ENTITY_SORTS: List[Tuple[str, str]] = [("id", "asc"), ("nombre_completo", "asc")]
    COMPROBANTE_ARTICLE_SORTS: List[Tuple[str, str]] = [("codigo", "asc"), ("nombre", "asc")]

    def _entity_comprobante_sort_key(row: Dict[str, Any]) -> Tuple[int, str]:
        raw_id = row.get("id")
        try:
            entity_id = int(raw_id)
        except Exception:
            entity_id = 10**12
        name_key = str(row.get("nombre_completo") or row.get("razon_social") or "").strip().casefold()
        return entity_id, name_key

    def _article_codigo_sort_key(row: Dict[str, Any]) -> Tuple[int, int, str, str, int]:
        raw_code = str(row.get("codigo") or "").strip()
        is_numeric_code = raw_code.isdigit()
        numeric_code = int(raw_code) if is_numeric_code else 0
        code_key = raw_code.casefold()
        name_key = str(row.get("nombre") or "").strip().casefold()
        raw_id = row.get("id")
        try:
            item_id = int(raw_id)
        except Exception:
            item_id = 10**12
        return (0 if is_numeric_code else 1, numeric_code, code_key, name_key, item_id)

    def article_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_articles(search=query, offset=offset, limit=limit)
        items = [{"value": r["id"], "label": f"{r['nombre']} (Cod: {r['id']})"} for r in rows]
        return items, len(rows) >= limit

    def entity_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_entities(search=query, offset=offset, limit=limit)
        items = [_format_entity_option(r, include_tipo=True) for r in rows]
        return items, len(rows) >= limit

    def comprobante_entity_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_entities(
            search=query,
            tipo="CLIENTE",
            sorts=COMPROBANTE_ENTITY_SORTS,
            offset=offset,
            limit=limit,
        )
        items = [_format_entity_option(r, include_tipo=True) for r in rows]
        return items, len(rows) >= limit

    def comprobante_article_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_articles(
            search=query,
            activo_only=True,
            sorts=COMPROBANTE_ARTICLE_SORTS,
            offset=offset,
            limit=limit,
        )
        items = [{"value": r["id"], "label": f"{r['nombre']} (Cod: {r['id']})"} for r in rows]
        return items, len(rows) >= limit

    def supplier_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_entities(search=query, tipo="PROVEEDOR", offset=offset, limit=limit)
        items = [_format_entity_option(r, include_tipo=False, force_tipo="Proveedor") for r in rows]
        return items, len(rows) >= limit

    def price_list_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_listas_precio(search=query, offset=offset, limit=limit)
        # Solo activas
        items = [{"value": r["id"], "label": r["nombre"]} for r in rows if r.get("activa", True)]
        return items, len(rows) >= limit

    def province_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_provincias(search=query, limit=limit, offset=offset)
        items = [{"value": r["id"], "label": r["nombre"]} for r in rows]
        return items, len(rows) >= limit

    def localidad_search_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_localidades(search=query, offset=offset, limit=limit)
        items = [{"value": r["id"], "label": f"{r['nombre']} ({r['provincia']})"} for r in rows]
        return items, len(rows) >= limit

    def locality_loader(query, offset, limit):
        if not db: return [], False
        pid = nueva_entidad_provincia.value
        if not pid:
            return [], False
        rows = db.fetch_localidades_by_provincia(int(pid))
        if query:
            q = query.lower()
            rows = [r for r in rows if q in (r.get("nombre") or "").lower()]
        slice_rows = rows[offset:offset + limit]
        items = [{"value": r["id"], "label": r["nombre"]} for r in slice_rows]
        return items, offset + limit < len(rows)
    status_icon_value = ft.icons.CHECK_CIRCLE_ROUNDED if db and not db_error else ft.icons.ERROR_OUTLINE_ROUNDED
    status_color = "#166534" if db and not db_error else "#991B1B"
    status_badge.content.controls.extend(
        [
            ft.Icon(status_icon_value, size=16, color=status_color) if status_icon_value is not None else ft.Container(width=16, height=16),
            ft.Text("DB OK" if db and not db_error else "DB ERROR", size=12, color=status_color),
        ]
    )

    card_registry: Dict[str, ft.Text] = {}

    def make_stat_card(label: str, value: str, icon_name: str, color: str = COLOR_ACCENT, key: str = None) -> ft.Control:
        icon_value = getattr(ft.icons, icon_name, ft.icons.QUESTION_MARK_ROUNDED)
        val_text = ft.Text(value, size=20, weight=ft.FontWeight.W_900, color=COLOR_TEXT)
        if key:
            card_registry[key] = val_text
            
        return ft.Container(
            content=ft.Row([
                ft.Container(
                    content=ft.Icon(icon_value, color=color, size=24),
                    bgcolor=f"{color}1A",
                    padding=10,
                    border_radius=12,
                ),
                ft.Column([
                    ft.Text(label, size=12, color=COLOR_TEXT_MUTED, weight=ft.FontWeight.W_500),
                    val_text,
                ], spacing=-2),
            ], spacing=12),
            padding=16,
            bgcolor=COLOR_CARD,
            border_radius=16,
            border=ft.border.all(1, COLOR_BORDER),
            shadow=ft.BoxShadow(
                blur_radius=10,
                color="#00000008",
                offset=ft.Offset(0, 4),
            ),
            expand=True,
        )

    def make_card(title: str, subtitle: str, content: ft.Control, actions: Optional[List[ft.Control]] = None) -> ft.Control:
        header_row = ft.Row(
            [
                ft.Column([
                    ft.Text(title, size=24, weight=ft.FontWeight.W_800, color=COLOR_TEXT),
                    ft.Text(subtitle, size=13, color=COLOR_TEXT_MUTED),
                ], spacing=2, expand=True),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )
        if actions:
            header_row.controls.append(ft.Row(actions, spacing=10))

        return ft.Column(
            [
                ft.Container(
                    content=header_row,
                    padding=ft.padding.only(bottom=15)
                ),
                ft.Container(
                    content=content,
                    expand=1,
                    padding=ft.padding.all(20),
                    bgcolor=COLOR_CARD,
                    border_radius=16,
                    border=ft.border.all(1, COLOR_BORDER),
                    shadow=ft.BoxShadow(
                        blur_radius=20,
                        spread_radius=1,
                        color="#0F172A08",
                        offset=ft.Offset(0, 4),
                    ),
                ),
            ],
            expand=True,
            spacing=0,
        )

    # ---- Artículos (GenericTable) ----
    articulos_table = None  # Late-binding placeholder

    def _art_live(e):
        try:
            if articulos_table:
                articulos_table.trigger_refresh()
        except Exception as e:
            logger.warning(f"Falló al actualizar tabla de artículos: {e}")

    def _art_slider_change(e):
        # Update label real-time
        s = articulos_advanced_costo_slider
        articulos_advanced_costo_label.value = f"Costo: entre {_format_money(s.start_value)} y {_format_money(s.end_value)}"
        articulos_advanced_costo_min_field.value = str(s.start_value)
        articulos_advanced_costo_max_field.value = str(s.end_value)
        try: 
            _safe_update_multiple(
                articulos_advanced_costo_label,
                articulos_advanced_costo_min_field,
                articulos_advanced_costo_max_field
            )
        except Exception as e:
            logger.warning(f"Falló al actualizar UI de filtro de costo: {e}")

    def _reset_cost_filter(ctrl, val):
        s = articulos_advanced_costo_slider
        s.start_value = s.min
        s.end_value = s.max
        articulos_advanced_costo_label.value = f"Costo: entre {_format_money(s.min)} y {_format_money(s.max)}"
        articulos_advanced_costo_min_field.value = str(s.min)
        articulos_advanced_costo_max_field.value = str(s.max)
        try: 
            _safe_update_multiple(
                s,
                articulos_advanced_costo_label,
                articulos_advanced_costo_min_field,
                articulos_advanced_costo_max_field
            )
        except Exception as e:
            logger.warning(f"Falló al resetear filtro de costo: {e}")

    def _art_costo_manual_change(e):
        try:
            val_min = float(articulos_advanced_costo_min_field.value or 0)
            val_max = float(articulos_advanced_costo_max_field.value or 10000)
            s = articulos_advanced_costo_slider
            s.start_value = max(s.min, min(val_min, s.max))
            s.end_value = min(s.max, max(val_max, s.min))
            _art_slider_change(None)
            s.update()
            _art_live(None)
        except Exception as e:
            logger.warning(f"Error en cambio manual de filtro de costo: {e}")

    articulos_advanced_costo_min_field = ft.TextField(label="Mín. (Costo)", width=120, dense=True, on_submit=_art_costo_manual_change); _style_input(articulos_advanced_costo_min_field)
    articulos_advanced_costo_max_field = ft.TextField(label="Máx. (Costo)", width=120, dense=True, on_submit=_art_costo_manual_change); _style_input(articulos_advanced_costo_max_field)

    def _get_costo_min_value(_: Any) -> Optional[float]:
        slider = articulos_advanced_costo_slider
        start = slider.start_value
        min_value = getattr(slider, "min", None)
        if start is None:
            return None
        if min_value is not None and start <= min_value:
            return None
        return start

    def _get_costo_max_value(_: Any) -> Optional[float]:
        slider = articulos_advanced_costo_slider
        end = slider.end_value
        max_value = getattr(slider, "max", None)
        if end is None:
            return None
        if max_value is not None and end >= max_value:
            return None
        return end

    articulos_advanced_nombre = ft.TextField(label="Nombre contiene", width=220, on_change=_art_live)
    _style_input(articulos_advanced_nombre)
    articulos_advanced_codigo = ft.TextField(label="Código contiene", width=180, on_change=_art_live)
    _style_input(articulos_advanced_codigo)
    articulos_advanced_marca = _dropdown("Filtrar Marca", [("", "Todas")], value="", width=200, on_change=_art_live)
    articulos_advanced_rubro = _dropdown("Filtrar Rubro", [("", "Todos")], value="", width=200, on_change=_art_live)
    articulos_advanced_proveedor = AsyncSelect(
        label="Filtrar Proveedor",
        loader=supplier_loader,
        width=200,
        on_change=lambda _: _art_live(None),
        show_label=False,
    )
    articulos_advanced_ubicacion = _dropdown("Filtrar Ubicación", [("", "Todas")], value="", width=200, on_change=_art_live)
    
    articulos_advanced_costo_slider = ft.RangeSlider(
        min=0,
        max=10000,
        start_value=0,
        end_value=10000,
        divisions=100,
        width=250,
        inactive_color="#E2E8F0",
        active_color=COLOR_ACCENT,
        label="{value}",
        on_change=_art_slider_change,
        on_change_end=_art_live,
    )
    
    articulos_advanced_costo_label = ft.Text(f"Costo: entre {_format_money(0)} y {_format_money(10000)}", size=12, weight=ft.FontWeight.BOLD)
    articulos_advanced_costo_ctrl = ft.Column([
        articulos_advanced_costo_label,
        ft.Row([articulos_advanced_costo_min_field, articulos_advanced_costo_max_field], spacing=10, alignment=ft.MainAxisAlignment.START),
        ft.Container(articulos_advanced_costo_slider, padding=ft.padding.only(left=5, right=5))
    ], spacing=8, width=260, horizontal_alignment=ft.CrossAxisAlignment.START)

    # Stock range filter
    def _art_stock_slider_change(e):
        s = articulos_advanced_stock_slider
        articulos_advanced_stock_label.value = f"Stock: entre {int(s.start_value)} y {int(s.end_value)} un."
        articulos_advanced_stock_min_field.value = str(int(s.start_value))
        articulos_advanced_stock_max_field.value = str(int(s.end_value))
        try: 
            _safe_update_multiple(
                articulos_advanced_stock_label,
                articulos_advanced_stock_min_field,
                articulos_advanced_stock_max_field
            )
        except Exception as e:
            logger.warning(f"Falló al actualizar UI de filtro de stock: {e}")

    def _reset_stock_filter(ctrl, val):
        s = articulos_advanced_stock_slider
        s.start_value = s.min
        s.end_value = s.max
        articulos_advanced_stock_label.value = f"Stock: entre {int(s.min)} y {int(s.max)} un."
        articulos_advanced_stock_min_field.value = str(int(s.min))
        articulos_advanced_stock_max_field.value = str(int(s.max))
        try:
            _safe_update_multiple(
                s,
                articulos_advanced_stock_label,
                articulos_advanced_stock_min_field,
                articulos_advanced_stock_max_field
            )
        except Exception as e:
            logger.warning(f"Falló al resetear filtro de stock: {e}")

    def _art_stock_manual_change(e):
        try:
            val_min = float(articulos_advanced_stock_min_field.value or -1000)
            val_max = float(articulos_advanced_stock_max_field.value or 10000)
            s = articulos_advanced_stock_slider
            s.start_value = max(s.min, min(val_min, s.max))
            s.end_value = min(s.max, max(val_max, s.min))
            _art_stock_slider_change(None)
            s.update()
            _art_live(None)
        except Exception as e:
            logger.warning(f"Error en cambio manual de filtro de stock: {e}")

    articulos_advanced_stock_min_field = ft.TextField(label="Mín. (Stock)", width=120, dense=True, on_submit=_art_stock_manual_change); _style_input(articulos_advanced_stock_min_field)
    articulos_advanced_stock_max_field = ft.TextField(label="Máx. (Stock)", width=120, dense=True, on_submit=_art_stock_manual_change); _style_input(articulos_advanced_stock_max_field)

    def _get_stock_min_value(_: Any) -> Optional[float]:
        slider = articulos_advanced_stock_slider
        start = slider.start_value
        min_value = getattr(slider, "min", None)
        if start is None:
            return None
        if min_value is not None and start <= min_value:
            return None
        return start

    def _get_stock_max_value(_: Any) -> Optional[float]:
        slider = articulos_advanced_stock_slider
        end = slider.end_value
        max_value = getattr(slider, "max", None)
        if end is None:
            return None
        if max_value is not None and end >= max_value:
            return None
        return end

    articulos_advanced_stock_slider = ft.RangeSlider(
        min=-1000, max=10000,
        start_value=-1000, end_value=10000,
        divisions=200,
        width=250,
        inactive_color="#E2E8F0",
        active_color=COLOR_ACCENT,
        label="{value}",
        on_change=_art_stock_slider_change,
        on_change_end=_art_live,
    )
    articulos_advanced_stock_label = ft.Text(f"Stock: entre -1000 y 10000 un.", size=12, weight=ft.FontWeight.BOLD)
    articulos_advanced_stock_ctrl = ft.Column([
        articulos_advanced_stock_label,
        ft.Row([articulos_advanced_stock_min_field, articulos_advanced_stock_max_field], spacing=10, alignment=ft.MainAxisAlignment.START),
        ft.Container(articulos_advanced_stock_slider, padding=ft.padding.only(left=5, right=5))
    ], spacing=8, width=260, horizontal_alignment=ft.CrossAxisAlignment.START)

    articulos_advanced_stock_bajo = ft.Switch(label="Solo bajo mínimo (stock)", value=False, on_change=_art_live)
    
    articulos_advanced_iva = _dropdown("Alicuota IVA", [("", "Todas")], value="", width=250, on_change=_art_live)
    articulos_advanced_unidad = _dropdown("Unidad Medida", [("", "Todas")], value="", width=250, on_change=_art_live)
    articulos_advanced_redondeo = _dropdown("Redondeo", [("", "Todos"), ("SI", "Sí"), ("NO", "No")], value="", width=200, on_change=_art_live)
    
    articulos_advanced_lista_precio = AsyncSelect(label="Precios de lista", loader=price_list_loader, width=350, on_change=lambda _: _art_live(None))
    
    articulos_advanced_estado = _dropdown(
        "Estado",
        [("", "Todos"), ("ACTIVO", "Activos"), ("INACTIVO", "Inactivos")],
        value="",
        on_change=_art_live,
        width=200
    )

    def refresh_articles_catalogs():
        try:
            if db: db.invalidate_catalog_cache()
            # lists = db.fetch_listas_precio() # AsyncSelect handles it
            
            ivas = db.fetch_tipos_iva()
            articulos_advanced_iva.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(i["id"]), f"{i['descripcion']} ({i['porcentaje']}%)") for i in ivas
            ]
            
            unidades = db.fetch_unidades_medida()
            articulos_advanced_unidad.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(u["id"]), f"{u['nombre']} ({u['abreviatura']})") for u in unidades
            ]
            
            # provs = db.list_proveedores() # AsyncSelect handles it

            marcas = db.list_marcas_full()
            articulos_advanced_marca.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(m["id"]), m["nombre"]) for m in marcas
            ]

            rubros = db.list_rubros_full()
            articulos_advanced_rubro.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(str(r["id"]), r["nombre"]) for r in rubros
            ]

            depositos = db.fetch_depositos()
            articulos_advanced_ubicacion.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(d["nombre"], d["nombre"]) for d in depositos
            ]

            max_c = db.get_max_cost()
            if max_c < 100: max_c = 100
            articulos_advanced_costo_slider.max = max_c
            articulos_advanced_costo_slider.end_value = max_c
            articulos_advanced_costo_label.value = f"Costo: Filas entre $0 y ${int(max_c):,}"
            
            # Update all controls if they are already on the page
            for ctrl in [
                articulos_advanced_lista_precio, articulos_advanced_iva, articulos_advanced_unidad,
                articulos_advanced_proveedor, articulos_advanced_marca, articulos_advanced_rubro,
                articulos_advanced_ubicacion, articulos_advanced_costo_slider, articulos_advanced_costo_label
            ]:
                try: 
                    if ctrl.page: ctrl.update()
                except Exception as e:
                    logger.warning(f"Falló al actualizar control de filtro de artículo: {e}")
            
        except Exception as e: 
            print(f"Error refreshing article filters: {e}")
    def _ent_live(e):
        try:
            entidades_table.trigger_refresh()
        except Exception as e:
            logger.warning(f"Falló al actualizar tabla de entidades: {e}")

    entidades_advanced_cuit = ft.TextField(label="CUIT contiene", width=200, on_change=_ent_live)
    _style_input(entidades_advanced_cuit)
    entidades_advanced_apellido = ft.TextField(label="Apellido", width=180, on_change=_ent_live)
    _style_input(entidades_advanced_apellido)
    entidades_advanced_nombre = ft.TextField(label="Nombre", width=180, on_change=_ent_live)
    _style_input(entidades_advanced_nombre)
    entidades_advanced_razon = ft.TextField(label="Razón Social", width=200, on_change=_ent_live)
    _style_input(entidades_advanced_razon)
    entidades_advanced_domicilio = ft.TextField(label="Domicilio", width=200, on_change=_ent_live)
    _style_input(entidades_advanced_domicilio)
    entidades_advanced_localidad = AsyncSelect(
        label="Localidad",
        loader=localidad_search_loader,
        width=300,
        on_change=lambda _: _ent_live(None),
        show_label=False,
    )
    entidades_advanced_provincia = AsyncSelect(
        label="Provincia",
        loader=province_loader,
        width=250,
        on_change=lambda _: _ent_live(None),
        show_label=False,
    )
    entidades_advanced_email = ft.TextField(label="Email", width=200, on_change=_ent_live)
    _style_input(entidades_advanced_email)
    entidades_advanced_telefono = ft.TextField(label="Teléfono", width=150, on_change=_ent_live)
    _style_input(entidades_advanced_telefono)
    entidades_advanced_notas = ft.TextField(label="Notas contiene", width=200, on_change=_ent_live)
    _style_input(entidades_advanced_notas)

    entidades_advanced_activo = _dropdown(
        "Activo",
        [("", "Todos"), ("ACTIVO", "Activos"), ("INACTIVO", "Inactivos")],
        value="",
        on_change=_ent_live
    )
    entidades_advanced_desde = _date_field("Alta desde", width=150)
    entidades_advanced_hasta = _date_field("Alta hasta", width=150)
    # Set on_submit for date fields to trigger refresh upon selection
    entidades_advanced_desde.on_submit = _ent_live
    entidades_advanced_hasta.on_submit = _ent_live

    entidades_advanced_iva = _dropdown("Condición IVA", [("", "Todos")], on_change=_ent_live, width=250)
    entidades_advanced_lista_precio = AsyncSelect(
        label="Lista Precio",
        loader=price_list_loader,
        width=300,
        on_change=lambda _: _ent_live(None),
        show_label=False,
    )

    entidades_advanced_tipo = _dropdown(
        "Tipo",
        [("", "Todos"), ("CLIENTE", "Cliente"), ("PROVEEDOR", "Proveedor"), ("AMBOS", "Ambos")],
        value="",
        on_change=_ent_live,
        width=180
    )

    def entidades_provider(
        offset: int,
        limit: int,
        search: Optional[str],
        simple: Optional[str],
        advanced: Dict[str, Any],
        sorts: List[Tuple[str, str]],
    ):
        if db is None:
            raise provider_error()
        db.log_activity("ENTIDAD", "SELECT", detalle={"search": search, "tipo": simple, "offset": offset})
        rows = db.fetch_entities(
            search=search,
            tipo=simple,
            advanced=advanced,
            sorts=sorts,
            limit=limit,
            offset=offset,
        )
        total = db.count_entities(search=search, tipo=simple, advanced=advanced)
        return rows, total

    def deactivate_entity(entity_id: int) -> None:
        if db is None:
            raise provider_error()
        db.update_entity_fields(int(entity_id), {"activo": False})
        show_toast("Entidad desactivada", kind="success")
        entidades_table.refresh()

    entidades_table = GenericTable(
        columns=[
            ColumnConfig(
                key="codigo",
                label="Código",
                width=90,
                formatter=lambda v, row: str(v or row.get("id") or "—"),
            ),
            ColumnConfig(key="apellido", label="Apellido", width=120, editable=True),
            ColumnConfig(key="nombre", label="Nombre", width=120, editable=True),
            ColumnConfig(key="razon_social", label="Razón Social", width=180, editable=True),
            ColumnConfig(
                key="tipo", 
                label="Tipo", 
                formatter=lambda v, _: v or "—", 
                width=100,
                editable=True,
                inline_editor=dropdown_editor(lambda: ["CLIENTE", "PROVEEDOR", "AMBOS"], width=150, empty_label="Seleccionar tipo... *")
            ),
            ColumnConfig(key="cuit", label="CUIT", width=110, editable=True),
            ColumnConfig(
                key="condicion_iva", 
                label="IVA", 
                width=140,
                editable=True,
                inline_editor=dropdown_editor(
                    lambda: [c["nombre"] for c in db.fetch_condiciones_iva(limit=100)], 
                    width=200, 
                    empty_label="Seleccionar tipo... *"
                )
            ),
            ColumnConfig(
                key="id_lista_precio",
                label="Lista Precio",
                width=140,
                editable=True,
                formatter=lambda _, row: row.get("lista_precio") or "—",
                inline_editor=lambda value, row, setter: AsyncSelect(
                    label="Lista Precio",
                    loader=price_list_loader,
                    width=240,
                    value=value,
                    on_change=setter,
                    initial_items=(
                        [{"value": row.get("id_lista_precio"), "label": row.get("lista_precio")}]
                        if row.get("id_lista_precio") and row.get("lista_precio")
                        else None
                    ),
                ),
            ),
            ColumnConfig(key="domicilio", label="Domicilio", width=180, editable=True),
            ColumnConfig(key="telefono", label="Teléfono", width=120, editable=True),
            ColumnConfig(key="email", label="Email", width=180, editable=True),
            ColumnConfig(
                key="id_localidad",
                label="Localidad",
                width=140,
                editable=True,
                formatter=lambda _, row: row.get("localidad") or "—",
                inline_editor=lambda value, row, setter: AsyncSelect(
                    label="Localidad",
                    loader=localidad_search_loader,
                    width=300,
                    value=value,
                    on_change=setter,
                    initial_items=(
                        [{"value": row.get("id_localidad"), "label": f"{row.get('localidad')} ({row.get('provincia')})"}]
                        if row.get("id_localidad") and row.get("localidad")
                        else None
                    ),
                ),
            ),
            ColumnConfig(
                key="id_provincia",
                label="Provincia",
                width=110,
                editable=True,
                formatter=lambda _, row: row.get("provincia") or "—",
                inline_editor=lambda value, row, setter: AsyncSelect(
                    label="Provincia",
                    loader=province_loader,
                    width=240,
                    value=value,
                    on_change=setter,
                    initial_items=(
                        [{"value": row.get("id_provincia"), "label": row.get("provincia")}]
                        if row.get("id_provincia") and row.get("provincia")
                        else None
                    ),
                ),
            ),
            ColumnConfig(
                key="notas",
                label="Notas",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.INFO_OUTLINE_ROUNDED,
                    tooltip="Ver notas" if row.get("notas") else "Sin notas",
                    icon_color=COLOR_INFO if row.get("notas") else ft.Colors.GREY_400,
                    on_click=lambda _: open_form(
                        "Notas de Entidad",
                        ft.Column([ft.Text(row.get("notas") or "", selectable=True)], scroll=ft.ScrollMode.ADAPTIVE, height=300),
                        [],
                    ) if row.get("notas") else None,
                ),
                width=50,
            ),
            ColumnConfig(
                key="fecha_creacion", 
                label="Fecha Alta", 
                formatter=_format_datetime,
                width=110
            ),
            ColumnConfig(key="saldo_cuenta", label="Saldo", formatter=_format_money, width=100),
            ColumnConfig(
                key="activo",
                label="Estado",
                editable=True,
                renderer=lambda row: _bool_pill(row.get("activo")),
                inline_editor=lambda value, row, setter: ft.Switch(
                    value=bool(value),
                    on_change=lambda e: setter(e.control.value),
                ),
                width=90,
            ),
            ColumnConfig(
                key="_edit",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.EDIT_ROUNDED,
                    tooltip="Editar entidad completa",
                    icon_color=COLOR_ACCENT,
                    visible=(CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    on_click=lambda e, rid=row.get("id"): open_editar_entidad(int(rid)),
                ),
                width=40,
            ),
            ColumnConfig(
                key="_toggle_active",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.CHECK_CIRCLE_OUTLINE_ROUNDED if not row.get("activo") else ft.icons.DO_NOT_DISTURB_ON_ROUNDED,
                    tooltip="Activar entidad" if not row.get("activo") else "Desactivar entidad",
                    icon_color=COLOR_SUCCESS if not row.get("activo") else COLOR_WARNING,
                    on_click=lambda e, rid=row.get("id"), is_act=row.get("activo"): (
                        ask_confirm(
                            "Activar entidad" if not is_act else "Desactivar entidad",
                            "¿Deseas activar esta entidad?" if not is_act else "¿Deseas desactivar esta entidad? Podrás volver a activarla más tarde.",
                            "Activar" if not is_act else "Desactivar",
                            lambda: (
                                db.update_entity_fields(int(rid), {"activo": not is_act}),
                                show_toast(f"Entidad {'activada' if not is_act else 'desactivada'}", kind="success"),
                                entidades_table.refresh()
                            ) if db else None,
                            button_color=COLOR_SUCCESS if not is_act else COLOR_ERROR
                        )
                        if rid is not None
                        else None
                    ),
                ),
                width=40,
            ),
        ],
        data_provider=entidades_provider,
        advanced_filters=[
            AdvancedFilterControl("tipo", entidades_advanced_tipo),
            AdvancedFilterControl("apellido", entidades_advanced_apellido),
            AdvancedFilterControl("nombre", entidades_advanced_nombre),
            AdvancedFilterControl("razon_social", entidades_advanced_razon),
            AdvancedFilterControl("cuit", entidades_advanced_cuit),
            AdvancedFilterControl("domicilio", entidades_advanced_domicilio),
            AdvancedFilterControl("id_localidad", entidades_advanced_localidad),
            AdvancedFilterControl("id_provincia", entidades_advanced_provincia),
            AdvancedFilterControl("email", entidades_advanced_email),
            AdvancedFilterControl("telefono", entidades_advanced_telefono),
            AdvancedFilterControl("notas", entidades_advanced_notas),
            AdvancedFilterControl("activo", entidades_advanced_activo),
            AdvancedFilterControl("desde", entidades_advanced_desde),
            AdvancedFilterControl("hasta", entidades_advanced_hasta),
            AdvancedFilterControl("condicion_iva", entidades_advanced_iva),
            AdvancedFilterControl("id_lista_precio", entidades_advanced_lista_precio),
        ],
        inline_edit_callback=lambda row_id, changes: db.update_entity_fields(int(row_id), changes) if db else None,
        mass_edit_callback=lambda ids, updates: db.bulk_update_entities([int(i) for i in ids], updates) if db else None,
        mass_activate_callback=lambda ids: db.bulk_update_entities([int(i) for i in ids], {"activo": True}) if db else None,
        mass_deactivate_callback=lambda ids: db.bulk_update_entities([int(i) for i in ids], {"activo": False}) if db else None,
        show_inline_controls=True,
        show_mass_actions=True,
        show_selection=True,
        auto_load=False,
        page_size=12,
        page_size_options=(10, 25, 50),
        show_export_button=True,
        show_export_scope=True,
    )
    entidades_table.search_field.hint_text = "Búsqueda global (código/nombre/razón social/cuit)…"
    
    entidades_view = ft.Column([
        ft.Row([
            make_stat_card("Clientes Activos", "0", "PEOPLE_ROUNDED", COLOR_ACCENT, key="entidades_clientes"),
            make_stat_card("Proveedores", "0", "LOCAL_SHIPPING_ROUNDED", COLOR_WARNING, key="entidades_proveedores"),
            make_stat_card("Total Entidades", "0", "ACCOUNT_BALANCE_ROUNDED", COLOR_SUCCESS, key="entidades_activos"),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Entidades Comerciales", 
            "Gestión integral de clientes y proveedores.", 
            entidades_table.build(),
            actions=[
                btn_nueva_entidad := ft.ElevatedButton(
                    "Nueva Entidad", 
                    icon=ft.icons.ADD_ROUNDED, 
                    bgcolor=COLOR_ACCENT, 
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=lambda _: open_nueva_entidad()
                )
            ]
        )
    ], spacing=10, expand=True)
    
    entidades_view = ft.Container(
        content=entidades_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # Filters and catalogs defined above to avoid circular dependency

    def articulos_provider(
        offset: int,
        limit: int,
        search: Optional[str],
        simple: Optional[str],
        advanced: Dict[str, Any],
        sorts: List[Tuple[str, str]],
    ):
        if db is None:
            raise provider_error()
        db.log_activity("ARTICULO", "SELECT", detalle={"search": search, "offset": offset})
        
        # Read from advanced filters now
        adv_status = advanced.get("activo")
        if adv_status == "ACTIVO":
            activo = True
        elif adv_status == "INACTIVO":
            activo = False
        else:
            activo = None
            
        rows = db.fetch_articles(
            search=search,
            activo_only=activo,
            advanced=advanced,
            sorts=sorts,
            limit=limit,
            offset=offset,
        )
        total = db.count_articles(search=search, activo_only=activo, advanced=advanced)
        return rows, total

    def _current_lista_precio_id() -> int:
        raw = getattr(articulos_advanced_lista_precio, "value", None)
        if raw in (None, "", 0, "0"):
            return 1
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 1

    def _update_articulo_precio_lista(article_id: int, value: Any) -> None:
        if db is None:
            raise provider_error()
        if value is None or (isinstance(value, str) and not value.strip()):
            precio_val = None
        else:
            precio_val = _parse_float(value, "Precio lista")
            if precio_val < 0:
                raise ValueError("El precio lista no puede ser negativo.")

        lista_id = _current_lista_precio_id()
        existing = db.fetch_article_prices(article_id)
        existing_row = next(
            (p for p in existing if int(p.get("id_lista_precio") or 0) == int(lista_id)),
            None,
        )
        db.update_article_prices(
            article_id,
            [
                {
                    "id_lista_precio": lista_id,
                    "precio": precio_val,
                    "porcentaje": existing_row.get("porcentaje") if existing_row else None,
                    "id_tipo_porcentaje": existing_row.get("id_tipo_porcentaje") if existing_row else None,
                }
            ],
        )

    def _ajustar_stock_desde_inventario(article_id: int, value: Any) -> None:
        if db is None:
            raise provider_error()
        nuevo_stock = _parse_float(value, "Stock")
        current_art_data = db.fetch_article_by_id(article_id)
        current_stock = float(current_art_data.get("stock_actual", 0)) if current_art_data else 0.0
        diff = nuevo_stock - current_stock
        if abs(diff) <= 0.01:
            return

        mtypes = db.fetch_tipos_movimiento_articulo()
        target_sign = 1 if diff > 0 else -1
        adj_type_id = None
        for mt in mtypes:
            if "ajuste" in str(mt.get("nombre", "")).lower() and mt.get("signo_stock") == target_sign:
                adj_type_id = mt.get("id")
                break
        if not adj_type_id:
            for mt in mtypes:
                if mt.get("signo_stock") == target_sign:
                    adj_type_id = mt.get("id")
                    break
        if not adj_type_id:
            raise ValueError("No se encontró un tipo de movimiento para ajustar stock.")

        db.create_stock_movement(
            id_articulo=article_id,
            id_tipo_movimiento=adj_type_id,
            cantidad=abs(diff),
            id_deposito=1,
            observacion=f"Ajuste manual desde inventario (Stock: {current_stock} -> {nuevo_stock})",
        )

    def _apply_articulo_inline_update(row_id: Any, changes: Dict[str, Any]) -> None:
        if db is None:
            raise provider_error()
        pending = dict(changes or {})
        if "precio_lista" in pending:
            _update_articulo_precio_lista(int(row_id), pending.pop("precio_lista"))
        if "stock_actual" in pending:
            _ajustar_stock_desde_inventario(int(row_id), pending.pop("stock_actual"))
        if "unidad_abreviatura" in pending:
            pending["id_unidad_medida"] = pending.pop("unidad_abreviatura")
        if pending:
            db.update_article_fields(int(row_id), pending)

    def _apply_articulo_mass_update(ids: List[Any], updates: Dict[str, Any]) -> None:
        for rid in ids:
            _apply_articulo_inline_update(rid, dict(updates))

    def deactivate_article(article_id: int) -> None:
        if db is None:
            raise provider_error()
        db.update_article_fields(int(article_id), {"activo": False})
        show_toast("Artículo desactivado", kind="success")
        articulos_table.refresh()

    articulos_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Nombre", editable=True, width=240),
            ColumnConfig(
                key="codigo",
                label="Código",
                editable=True,
                formatter=lambda v, _: v or "—",
                width=120,
            ),
            ColumnConfig(
                key="marca",
                label="Marca",
                editable=True,
                formatter=lambda v, _: v or "—",
                inline_editor=dropdown_editor(lambda: marcas_values, width=160, empty_label="(Sin marca)"),
                width=140,
            ),
            ColumnConfig(
                key="rubro",
                label="Rubro",
                editable=True,
                formatter=lambda v, _: v or "—",
                inline_editor=dropdown_editor(lambda: rubros_values, width=160, empty_label="(Sin rubro)"),
                width=140,
            ),
            ColumnConfig(
                key="costo",
                label="Costo",
                editable=True,
                formatter=lambda v, _: _format_money(v),
                width=110,
            ),
            ColumnConfig(
                key="precio_lista",
                label="Precio Lista",
                editable=True,
                formatter=lambda v, _: _format_money(v),
                width=110,
            ),
            ColumnConfig(
                key="unidad_abreviatura",
                label="UM",
                editable=True,
                inline_editor=unidad_medida_editor(width=180),
                width=60,
            ),
            ColumnConfig(
                key="unidades_por_bulto",
                label="Bultos",
                editable=True,
                formatter=lambda v, _: "—" if v in (None, "") else str(v),
                width=85,
            ),
            ColumnConfig(
                key="stock_minimo",
                label="Mínimo (stock)",
                editable=True,
                formatter=_format_quantity,
                width=90,
            ),
            ColumnConfig(
                key="stock_actual",
                label="Stock",
                editable=True,
                formatter=_format_quantity,
                width=90,
            ),
            ColumnConfig(
                key="id_tipo_iva",
                label="Alicuota IVA",
                editable=True,
                formatter=lambda v, row: next((i["descripcion"] for i in tipos_iva_values if str(i["id"]) == str(v or row.get("id_tipo_iva"))), "—"),
                inline_editor=dropdown_editor(lambda: [i["descripcion"] for i in tipos_iva_values], width=200, empty_label="Seleccionar IVA... *"),
                width=150,
            ),
            ColumnConfig(
                key="id_proveedor",
                label="Proveedor",
                editable=True,
                formatter=lambda v, row: next((p["nombre"] for p in proveedores_values if str(p["id"]) == str(v or row.get("id_proveedor"))), "—"),
                inline_editor=async_select_editor(supplier_loader, label="Proveedor", width=300),
                width=180,
            ),
            ColumnConfig(
                key="ubicacion",
                label="Ubicación",
                width=120,
                editable=True,
                inline_editor=dropdown_editor(lambda: [d["nombre"] for d in (db.fetch_depositos() if db else [])], width=200, empty_label="Seleccionar depósito... *"),
            ),
            ColumnConfig(
                key="activo",
                label="Estado",
                editable=True,
                renderer=lambda row: _bool_pill(row.get("activo")),
                inline_editor=lambda value, row, setter: ft.Switch(
                    value=bool(value),
                    on_change=lambda e: setter(e.control.value),
                ),
                width=90,
            ),
            ColumnConfig(
                key="_details",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.INFO_OUTLINE_ROUNDED,
                    tooltip="Ver detalles completos",
                    icon_color=COLOR_TEXT_MUTED,
                    on_click=lambda e, rid=row.get("id"): open_detalle_articulo(int(rid)),
                ),
                width=40,
            ),
            ColumnConfig(
                key="_edit",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.EDIT_ROUNDED,
                    tooltip="Editar artículo completo",
                    icon_color=COLOR_ACCENT,
                    visible=(CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    on_click=lambda e, rid=row.get("id"): open_editar_articulo(int(rid)),
                ),
                width=40,
            ),
            ColumnConfig(
                key="_toggle_active",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.CHECK_CIRCLE_OUTLINE_ROUNDED if not row.get("activo") else ft.icons.DO_NOT_DISTURB_ON_ROUNDED,
                    tooltip="Activar artículo" if not row.get("activo") else "Desactivar artículo",
                    icon_color=COLOR_SUCCESS if not row.get("activo") else COLOR_WARNING,
                    visible=(CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    on_click=lambda e, rid=row.get("id"), is_act=row.get("activo"): (
                        ask_confirm(
                            "Activar artículo" if not is_act else "Desactivar artículo",
                            "¿Deseas activar este artículo?" if not is_act else "¿Deseas desactivar este artículo? Podrás volver a activarlo más tarde.",
                            "Activar" if not is_act else "Desactivar",
                            lambda: (
                                db.update_article_fields(int(rid), {"activo": not is_act}),
                                show_toast(f"Artículo {'activado' if not is_act else 'desactivada'}", kind="success"),
                                articulos_table.refresh()
                            ) if db else None,
                            button_color=COLOR_SUCCESS if not is_act else COLOR_ERROR
                        )
                        if rid is not None
                        else None
                    ),
                ),
                width=40,
            ),
        ],
        data_provider=articulos_provider,
        advanced_filters=[
            AdvancedFilterControl("nombre", articulos_advanced_nombre),
            AdvancedFilterControl("codigo", articulos_advanced_codigo),
            AdvancedFilterControl("id_marca", articulos_advanced_marca),
            AdvancedFilterControl("id_rubro", articulos_advanced_rubro),
            AdvancedFilterControl("id_proveedor", articulos_advanced_proveedor),
            AdvancedFilterControl("ubicacion_exacta", articulos_advanced_ubicacion),
            AdvancedFilterControl("costo_min", articulos_advanced_costo_ctrl, getter=_get_costo_min_value, setter=_reset_cost_filter),
            AdvancedFilterControl("costo_max", ft.Container(), getter=_get_costo_max_value),
            AdvancedFilterControl("stock_min", articulos_advanced_stock_ctrl, getter=_get_stock_min_value, setter=_reset_stock_filter),
            AdvancedFilterControl("stock_max", ft.Container(), getter=_get_stock_max_value),
            AdvancedFilterControl("stock_bajo_minimo", articulos_advanced_stock_bajo),
            AdvancedFilterControl("id_lista_precio", articulos_advanced_lista_precio),
            AdvancedFilterControl("activo", articulos_advanced_estado),
            AdvancedFilterControl("id_tipo_iva", articulos_advanced_iva),
            AdvancedFilterControl("id_unidad_medida", articulos_advanced_unidad),
            AdvancedFilterControl("redondeo", articulos_advanced_redondeo),
        ],
        inline_edit_callback=_apply_articulo_inline_update,
        mass_edit_callback=_apply_articulo_mass_update,
        mass_activate_callback=lambda ids: db.bulk_update_articles([int(i) for i in ids], {"activo": True}) if db else None,
        mass_deactivate_callback=lambda ids: db.bulk_update_articles([int(i) for i in ids], {"activo": False}) if db else None,
        show_inline_controls=True,
        show_mass_actions=True,
        show_selection=True,
        auto_load=True,
        page_size=10,
        page_size_options=(10, 25, 50),
        show_export_button=True,
        show_export_scope=True,
    )
    articulos_table.search_field.hint_text = "Búsqueda global (nombre/código)…"
    
    articulos_view = ft.Column([
        ft.Row([
            make_stat_card("Artículos en Stock", "0", "INVENTORY_ROUNDED", COLOR_ACCENT, key="articulos_total"),
            make_stat_card("Stock Crítico", "0", "WARNING_AMBER_ROUNDED", COLOR_ERROR, key="articulos_bajo_stock"),
            make_stat_card("Valor Inventario", "$0", "ATTACH_MONEY_ROUNDED", COLOR_INFO, key="articulos_valor"),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Inventario de Artículos", 
            "Stock, costos y listas de precios.", 
            articulos_table.build(),
            actions=[
                btn_nuevo_articulo := ft.ElevatedButton(
                    "Nuevo Artículo", 
                    icon=ft.icons.ADD_ROUNDED, 
                    bgcolor=COLOR_ACCENT, 
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=lambda _: open_nuevo_articulo()
                )
            ]
        )
    ], spacing=10, expand=True)

    articulos_view = ft.Container(
        content=articulos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )



    # ---- Crear entidad / artículo ----
    # (Using the unified form_dialog defined above at the start of main)

    nueva_entidad_nombre = ft.TextField(label="Nombre *", width=250)
    _style_input(nueva_entidad_nombre)
    nueva_entidad_apellido = ft.TextField(label="Apellido *", width=250)
    _style_input(nueva_entidad_apellido)
    nueva_entidad_razon_social = ft.TextField(label="Razón social *", width=510)
    _style_input(nueva_entidad_razon_social)
    razon_social_state = {"manual": False, "auto": ""}

    def _compute_razon_social() -> str:
        nombre = (nueva_entidad_nombre.value or "").strip()
        apellido = (nueva_entidad_apellido.value or "").strip()
        if not nombre or not apellido:
            return ""
        return f"{nombre} {apellido}".strip()

    def _maybe_autofill_razon_social(_: Any = None) -> None:
        if editing_entity_id is not None:
            return
        auto_val = _compute_razon_social()
        if not auto_val:
            return
        current = (nueva_entidad_razon_social.value or "").strip()
        if razon_social_state["manual"] and current:
            return
        if current and current != razon_social_state["auto"]:
            return
        razon_social_state["auto"] = auto_val
        razon_social_state["manual"] = False
        nueva_entidad_razon_social.value = auto_val
        try:
            nueva_entidad_razon_social.update()
        except Exception:
            pass

    def _on_razon_social_change(_: Any = None) -> None:
        if editing_entity_id is not None:
            return
        current = (nueva_entidad_razon_social.value or "").strip()
        auto_val = _compute_razon_social()
        razon_social_state["auto"] = auto_val
        if not current:
            razon_social_state["manual"] = False
            return
        razon_social_state["manual"] = current != auto_val

    nueva_entidad_nombre.on_change = _maybe_autofill_razon_social
    nueva_entidad_apellido.on_change = _maybe_autofill_razon_social
    nueva_entidad_razon_social.on_change = _on_razon_social_change
    nueva_entidad_tipo = _dropdown(
        "Tipo *",
        [("", "—"), ("CLIENTE", "Cliente"), ("PROVEEDOR", "Proveedor"), ("AMBOS", "Ambos")],
        value="",
        width=250,
    )
    nueva_entidad_cuit = ft.TextField(label="CUIT *", width=250)
    _style_input(nueva_entidad_cuit)
    nueva_entidad_telefono = ft.TextField(label="Teléfono *", width=250)
    _style_input(nueva_entidad_telefono)
    nueva_entidad_email = ft.TextField(label="Email", width=510)
    _style_input(nueva_entidad_email)
    nueva_entidad_domicilio = ft.TextField(label="Domicilio", width=510)
    _style_input(nueva_entidad_domicilio)
    nueva_entidad_lista_precio = AsyncSelect(label="Lista de Precios", loader=price_list_loader, width=250)
    nueva_entidad_descuento = _number_field("Desc. (%)", width=120)
    nueva_entidad_limite_credito = _number_field("Límite Crédito ($)", width=180)
    nueva_entidad_activo = ft.Switch(label="Activo", value=True)
    
    # New Fields for Entity
    nueva_entidad_provincia = AsyncSelect(label="Provincia *", loader=province_loader, width=250, on_change=lambda _: _on_provincia_change(None))
    nueva_entidad_localidad = AsyncSelect(label="Localidad *", loader=locality_loader, width=250, disabled=True)
    nueva_entidad_condicion_iva = ft.Dropdown(label="Condición IVA *", width=250, options=[])
    _style_input(nueva_entidad_condicion_iva)
    nueva_entidad_notas = ft.TextField(label="Notas", width=510, multiline=True, min_lines=2, max_lines=4)
    _style_input(nueva_entidad_notas)

    def _reload_entity_dropdowns():
        """Populate Province and Condición IVA dropdowns."""
        if not db:
            return
        try:
            condiciones = db.fetch_condiciones_iva(limit=50)
            nueva_entidad_condicion_iva.options = [ft.dropdown.Option(str(c["id"]), c["nombre"]) for c in condiciones]
            
            # Also update the advanced filter dropdown and trigger UI refresh
            entidades_advanced_iva.options = [ft.dropdown.Option("", "Todos")] + [ft.dropdown.Option(c["nombre"], c["nombre"]) for c in condiciones]
            
            try:
                # Only attempt to update if controls are added to a page
                if nueva_entidad_condicion_iva.page and entidades_advanced_iva.page:
                    nueva_entidad_condicion_iva.update()
                    entidades_advanced_iva.update()
            except Exception as e:
                logger.debug(f"No se pudieron actualizar campos IVA (controles no agregados a la página): {e}")

            try:
                nueva_entidad_provincia.prefetch()
                nueva_entidad_lista_precio.prefetch()
            except Exception as e:
                logger.warning(f"Falló al precargar dropdowns de entidad: {e}")
        except Exception as e:
            print(f"Error loading entity dropdowns: {e}")

    # Cascading logic for Province -> City
    def _on_provincia_change(e):
        pid = nueva_entidad_provincia.value
        # Reset locality
        nueva_entidad_localidad.value = ""
        nueva_entidad_localidad.clear_cache()
        if not pid:
            nueva_entidad_localidad.disabled = True
            nueva_entidad_localidad.set_busy(False)
            nueva_entidad_localidad.update()
            return

        nueva_entidad_localidad.set_busy(True)
        def _done():
            nueva_entidad_localidad.set_busy(False)
            nueva_entidad_localidad.disabled = False
            try:
                nueva_entidad_localidad.update()
            except Exception:
                pass
        nueva_entidad_localidad.prefetch(on_done=_done)
        nueva_entidad_localidad.update()
    
    nueva_entidad_provincia.on_change = _on_provincia_change

    editing_entity_id: Optional[int] = None

    def crear_entidad(_: Any = None) -> None:
        db_conn = get_db_or_toast()
        if db_conn is None:
            return
        
        # Validation
        has_name = bool((nueva_entidad_nombre.value or "").strip() and (nueva_entidad_apellido.value or "").strip())
        has_razon = bool((nueva_entidad_razon_social.value or "").strip())
        
        # Extended validation
        f_tipo = bool((nueva_entidad_tipo.value or "").strip())
        f_cuit = bool((nueva_entidad_cuit.value or "").strip())
        f_iva = bool((nueva_entidad_condicion_iva.value or "").strip())
        f_tel = bool((nueva_entidad_telefono.value or "").strip())
        f_prov = bool(str(nueva_entidad_provincia.value or "").strip())
        f_loc = bool(str(nueva_entidad_localidad.value or "").strip())

        if not (has_name or has_razon):
            show_toast("Completá Nombre y Apellido O Razón Social.", kind="warning")
            return
        
        if not all([f_tipo, f_cuit, f_iva, f_tel, f_prov, f_loc]):
            show_toast("Faltan campos obligatorios (*).", kind="warning")
            return

        try:
            # Atomic creation
            eid = db_conn.create_entity_full(
                nombre=nueva_entidad_nombre.value,
                apellido=nueva_entidad_apellido.value,
                razon_social=nueva_entidad_razon_social.value,
                cuit=nueva_entidad_cuit.value,
                telefono=nueva_entidad_telefono.value,
                email=nueva_entidad_email.value,
                domicilio=nueva_entidad_domicilio.value,
                tipo=nueva_entidad_tipo.value if nueva_entidad_tipo.value else None,
                activo=bool(nueva_entidad_activo.value),
                id_localidad=int(nueva_entidad_localidad.value) if nueva_entidad_localidad.value else None,
                id_condicion_iva=int(nueva_entidad_condicion_iva.value) if nueva_entidad_condicion_iva.value else None,
                notas=nueva_entidad_notas.value,
                # Pricing
                id_lista_precio=nueva_entidad_lista_precio.value,
                descuento=_parse_float(nueva_entidad_descuento.value, "Descuento"),
                limite_credito=_parse_positive_float_optional(nueva_entidad_limite_credito.value, "Límite de Crédito")
            )
            close_form()
            if db_conn:
                db_conn.log_activity("ENTIDAD", "INSERT", id_entidad=eid, detalle={"nombre": nueva_entidad_nombre.value, "apellido": nueva_entidad_apellido.value})
            show_toast("Entidad creada", kind="success")
            entidades_table.refresh()
        except Exception as exc:
            show_toast(f"Error al crear: {exc}", kind="error")

    def open_nueva_entidad(_: Any = None) -> None:
        nonlocal editing_entity_id
        editing_entity_id = None
        razon_social_state["manual"] = False
        razon_social_state["auto"] = ""
        nueva_entidad_nombre.value = ""
        nueva_entidad_apellido.value = ""
        nueva_entidad_razon_social.value = ""
        nueva_entidad_tipo.value = ""
        nueva_entidad_cuit.value = ""
        nueva_entidad_telefono.value = ""
        nueva_entidad_email.value = ""
        nueva_entidad_domicilio.value = ""
        nueva_entidad_lista_precio.value = None
        nueva_entidad_descuento.value = "0"
        nueva_entidad_limite_credito.value = ""
        nueva_entidad_activo.value = True

        nueva_entidad_descuento.value = "0"
        nueva_entidad_limite_credito.value = ""
        nueva_entidad_activo.value = True
        
        # Reset new fields
        nueva_entidad_provincia.value = None
        nueva_entidad_localidad.value = None
        nueva_entidad_localidad.options = []
        nueva_entidad_localidad.disabled = True
        nueva_entidad_condicion_iva.value = ""
        nueva_entidad_notas.value = ""

        try:
            nueva_entidad_localidad.clear_cache()
        except Exception:
            pass

        _reload_entity_dropdowns()
        open_form("Nueva entidad", _prepare_entity_form_content(), [
            _cancel_button("Cancelar", on_click=close_form),
            ft.ElevatedButton(
                "Crear", 
                icon=ft.icons.ADD, 
                bgcolor=COLOR_ACCENT, 
                color="#FFFFFF", 
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)), 
                on_click=crear_entidad
            ),
        ])

    def open_editar_entidad(ent_id: int) -> None:
        nonlocal editing_entity_id
        editing_entity_id = ent_id
        db_conn = get_db_or_toast()
        if db_conn is None: return
        
        try:
            ent = db_conn.fetch_entity_by_id(ent_id)
            if not ent:
                show_toast("Entidad no encontrada", kind="error")
                return
            
            _reload_entity_dropdowns()
            
            nueva_entidad_nombre.value = ent.get("nombre", "")
            nueva_entidad_apellido.value = ent.get("apellido", "")
            nueva_entidad_razon_social.value = ent.get("razon_social", "")
            nueva_entidad_tipo.value = ent.get("tipo") or ""
            nueva_entidad_cuit.value = ent.get("cuit", "")
            nueva_entidad_telefono.value = ent.get("telefono", "")
            nueva_entidad_email.value = ent.get("email", "")
            nueva_entidad_domicilio.value = ent.get("domicilio", "")
            lp_id = ent.get("id_lista_precio")
            if lp_id:
                nueva_entidad_lista_precio.options = [ft.dropdown.Option(str(lp_id), ent.get("lista_precio") or "")]
                nueva_entidad_lista_precio.value = str(lp_id)
            else:
                nueva_entidad_lista_precio.options = []
                nueva_entidad_lista_precio.value = None
            nueva_entidad_descuento.value = str(ent.get("descuento", 0))
            limite_credito = ent.get("limite_credito")
            if limite_credito is None or float(limite_credito or 0) <= 0:
                nueva_entidad_limite_credito.value = ""
            else:
                nueva_entidad_limite_credito.value = str(limite_credito)
            nueva_entidad_activo.value = bool(ent.get("activo", True))
            
            # Load new fields
            nueva_entidad_notas.value = ent.get("notas", "")
            nueva_entidad_condicion_iva.value = str(ent["id_condicion_iva"]) if ent.get("id_condicion_iva") else ""
            
            # Handle Location
            pid = ent.get("id_provincia")
            lid = ent.get("id_localidad")
            if pid:
                nueva_entidad_provincia.options = [ft.dropdown.Option(str(pid), ent.get("provincia") or "")]
                nueva_entidad_provincia.value = str(pid)
                # Manually trigger locality reload
                _on_provincia_change(None)
                if lid:
                    nueva_entidad_localidad.options = [ft.dropdown.Option(str(lid), ent.get("localidad") or "")]
                    nueva_entidad_localidad.value = str(lid)
                    nueva_entidad_localidad.disabled = False
            else:
                nueva_entidad_provincia.value = None
                nueva_entidad_provincia.options = []
                nueva_entidad_localidad.value = None
                nueva_entidad_localidad.options = []
                nueva_entidad_localidad.disabled = True

            try:
                nueva_entidad_provincia.prefetch()
                nueva_entidad_lista_precio.prefetch()
                if pid:
                    nueva_entidad_localidad.prefetch()
            except Exception:
                pass
            
            open_form("Editar entidad", _prepare_entity_form_content(), [
                _cancel_button("Cancelar", on_click=close_form),
                ft.ElevatedButton("Guardar Cambios", icon=ft.icons.SAVE_ROUNDED, bgcolor=COLOR_ACCENT, color="#FFFFFF",
                                  style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)), on_click=guardar_edicion_entidad),
            ])
        except Exception as exc:
            show_toast(f"Error: {exc}", kind="error")

    def guardar_edicion_entidad(_: Any = None) -> None:
        nonlocal editing_entity_id
        if not editing_entity_id: return
        
        db_conn = get_db_or_toast()
        if db_conn is None: return

        # Validation
        has_name = bool((nueva_entidad_nombre.value or "").strip() and (nueva_entidad_apellido.value or "").strip())
        has_razon = bool((nueva_entidad_razon_social.value or "").strip())
        
        # Extended validation
        f_tipo = bool((nueva_entidad_tipo.value or "").strip())
        f_cuit = bool((nueva_entidad_cuit.value or "").strip())
        f_iva = bool((nueva_entidad_condicion_iva.value or "").strip())
        f_tel = bool((nueva_entidad_telefono.value or "").strip())
        f_prov = bool(str(nueva_entidad_provincia.value or "").strip())
        f_loc = bool(str(nueva_entidad_localidad.value or "").strip())

        if not (has_name or has_razon):
            show_toast("Completá Nombre y Apellido O Razón Social.", kind="warning")
            return
        
        if not all([f_tipo, f_cuit, f_iva, f_tel, f_prov, f_loc]):
            show_toast("Faltan campos obligatorios (*).", kind="warning")
            return

        try:
            updates = {
                "nombre": nueva_entidad_nombre.value,
                "apellido": nueva_entidad_apellido.value,
                "razon_social": nueva_entidad_razon_social.value,
                "cuit": nueva_entidad_cuit.value,
                "telefono": nueva_entidad_telefono.value,
                "email": nueva_entidad_email.value,
                "domicilio": nueva_entidad_domicilio.value,
                "tipo": nueva_entidad_tipo.value if nueva_entidad_tipo.value else None,
                "activo": bool(nueva_entidad_activo.value),
                "id_localidad": int(nueva_entidad_localidad.value) if nueva_entidad_localidad.value else None,
                "id_condicion_iva": int(nueva_entidad_condicion_iva.value) if nueva_entidad_condicion_iva.value else None,
                "notas": nueva_entidad_notas.value,
            }
            descuento_val = _parse_float(nueva_entidad_descuento.value, "Descuento")
            limite_credito_val = _parse_positive_float_optional(
                nueva_entidad_limite_credito.value,
                "Límite de Crédito",
            )
            changes = dict(updates)
            changes["id_lista_precio"] = nueva_entidad_lista_precio.value or None
            changes["descuento"] = descuento_val
            changes["limite_credito"] = limite_credito_val
            
            # Atomic update
            db_conn.update_entity_full(
                editing_entity_id,
                updates=updates,
                id_lista_precio=nueva_entidad_lista_precio.value,
                descuento=descuento_val,
                limite_credito=limite_credito_val,
            )
            
            close_form()
            if db_conn:
                db_conn.log_activity("ENTIDAD", "UPDATE", id_entidad=editing_entity_id, detalle=changes)
            show_toast("Entidad actualizada", kind="success")
            entidades_table.refresh()
        except Exception as exc:
            show_toast(f"Error al actualizar: {exc}", kind="error")

        if not db: return
        lists = db.fetch_listas_precio(limit=100)
        nueva_entidad_lista_precio.options = [ft.dropdown.Option("", "—")] + [
            ft.dropdown.Option(str(l["id"]), l["nombre"]) for l in lists
        ]
        
        # Load Provinces and Condicion IVA
        provs = db.fetch_provincias()
        nueva_entidad_provincia.options = [ft.dropdown.Option(str(p["id"]), p["nombre"]) for p in provs]
        
        conds = db.fetch_condiciones_iva()
        nueva_entidad_condicion_iva.options = [ft.dropdown.Option(str(c["id"]), c["nombre"]) for c in conds]

    def _prepare_entity_form_content() -> ft.Control:
        section_title = lambda text: ft.Column([
            ft.Text(text, weight=ft.FontWeight.BOLD, size=14, color=COLOR_ACCENT),
            ft.Divider(height=1, thickness=1, color=ft.Colors.GREY_300),
        ], spacing=5)

        return ft.Container(
            width=550,
            padding=ft.padding.only(bottom=10),
            content=ft.Column(
                [
                    section_title("Información de la Entidad"),
                    ft.Row([nueva_entidad_nombre, nueva_entidad_apellido], spacing=10),
                    ft.Row([nueva_entidad_razon_social], spacing=10),
                    ft.Row([nueva_entidad_tipo, nueva_entidad_condicion_iva], spacing=10),
                    ft.Row([nueva_entidad_cuit], spacing=10),
                    
                    ft.Container(height=10),
                    section_title("Contacto y Domicilio"),
                    ft.Row([nueva_entidad_telefono], spacing=10),
                    ft.Row([nueva_entidad_email], spacing=10),
                    ft.Row([nueva_entidad_provincia, nueva_entidad_localidad], spacing=10),
                    ft.Row([nueva_entidad_domicilio], spacing=10),
                    
                    ft.Container(height=10),
                    section_title("Configuración Comercial"),
                    ft.Row([nueva_entidad_lista_precio], spacing=10),
                    ft.Row([nueva_entidad_descuento, nueva_entidad_limite_credito], spacing=10),
                    ft.Row([nueva_entidad_activo], spacing=10),

                    ft.Container(height=10),
                    section_title("Notas"),
                    ft.Row([nueva_entidad_notas], spacing=10),
                ],
                spacing=10,
                tight=True,
                scroll=ft.ScrollMode.AUTO,
            ),
        )

    
    # State for editing
    editing_article_id: Optional[int] = None

    nuevo_articulo_nombre = ft.TextField(label="Nombre *", width=560)
    _style_input(nuevo_articulo_nombre)
    nuevo_articulo_codigo = ft.TextField(label="Código", width=275)
    _style_input(nuevo_articulo_codigo)
    nuevo_articulo_marca = ft.Dropdown(label="Marca *", width=275, options=[], value="")
    _style_input(nuevo_articulo_marca)
    nuevo_articulo_rubro = ft.Dropdown(label="Rubro *", width=275, options=[], value="")
    _style_input(nuevo_articulo_rubro)
    nuevo_articulo_tipo_iva = ft.Dropdown(label="Alicuota IVA *", width=275, options=[], value="")
    _style_input(nuevo_articulo_tipo_iva)
    nuevo_articulo_unidad = ft.Dropdown(label="Unidad Medida *", width=275, options=[], value="")
    _style_input(nuevo_articulo_unidad)
    nuevo_articulo_proveedor = AsyncSelect(
        label="Proveedor Habitual",
        loader=supplier_loader,
        width=560,
        placeholder="Seleccionar proveedor",
    )
    
    nuevo_articulo_costo = _number_field("Costo *", width=275)
    nuevo_articulo_stock_minimo = _number_field("Stock mínimo *", width=275)
    nuevo_articulo_stock_actual = _number_field("Stock *", width=275)
    nuevo_articulo_unidades_por_bulto = ft.TextField(label="Unid./Bulto", width=275)
    _style_input(nuevo_articulo_unidades_por_bulto)
    nuevo_articulo_ubicacion = ft.Dropdown(label="Ubicación *", width=560, options=[], value="")
    _style_input(nuevo_articulo_ubicacion)
    nuevo_articulo_descuento_base = _number_field("Descuento Base (%)", width=180)
    nuevo_articulo_ganancia_2 = _number_field("Ganancia 2 (%)", width=180)
    nuevo_articulo_redondeo = ft.Switch(label="Redondeo", value=False)
    nuevo_articulo_observacion = ft.TextField(label="Observaciones", width=560, multiline=True, min_lines=2, max_lines=4)
    _style_input(nuevo_articulo_observacion)
    nuevo_articulo_activo = ft.Switch(label="Activo", value=True)
    articulo_precios_container = ft.Column(spacing=10)

    def _article_decimal_input(value: Any) -> str:
        return normalize_input_value(value, decimals=2, use_grouping=False) or "0,00"

    def _default_tipo_porcentaje_id() -> str:
        margin_id = next(
            (
                str(t["id"])
                for t in tipos_porcentaje_values
                if _normalize_price_tipo(t.get("tipo")) == "MARGEN"
            ),
            "",
        )
        if margin_id:
            return margin_id
        return str(tipos_porcentaje_values[0]["id"]) if tipos_porcentaje_values else ""

    def _resolve_tipo_porcentaje_label(tipo_id: Any) -> str:
        tipo_entry = next(
            (
                t
                for t in tipos_porcentaje_values
                if str(t.get("id")) == str(tipo_id)
            ),
            None,
        )
        if tipo_entry:
            return _normalize_price_tipo(tipo_entry.get("tipo"))
        return _normalize_price_tipo(tipo_id)

    def _normalize_article_numeric_field(field: ft.TextField) -> None:
        field.value = _article_decimal_input(field.value)
        _safe_update_control(field)

    def _parse_optional_positive_int(value: Any, field_name: str) -> Optional[int]:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        if not raw.lstrip("+-").isdigit():
            raise ValueError(f"{field_name} debe ser un entero.")
        parsed = int(raw)
        if parsed <= 0:
            raise ValueError(f"{field_name} debe ser mayor a 0.")
        return parsed

    def _iter_article_price_rows() -> List[ft.Container]:
        rows: List[ft.Container] = []
        for ctrl in articulo_precios_container.controls:
            if isinstance(ctrl, ft.Container) and hasattr(ctrl, "price_data") and hasattr(ctrl, "price_refs"):
                rows.append(ctrl)
        return rows

    def _sync_row_price_from_pct(row_cont: ft.Container, *, normalize_inputs: bool = False) -> None:
        refs = getattr(row_cont, "price_refs", {}) or {}
        guard = refs.get("sync_guard")
        tf_precio = refs.get("precio")
        tf_porc = refs.get("porcentaje")
        dd_tipo = refs.get("tipo")
        if not isinstance(guard, dict) or tf_precio is None or tf_porc is None or dd_tipo is None:
            return
        if guard.get("active"):
            return

        try:
            cost_val = _parse_float(nuevo_articulo_costo.value, "Costo")
            pct_val = _parse_float(tf_porc.value, "Porcentaje")
        except Exception:
            return

        tipo_label = _resolve_tipo_porcentaje_label(dd_tipo.value)
        price_val = _calc_price_from_cost_pct(cost_val, pct_val, tipo_label)

        guard["active"] = True
        try:
            if normalize_inputs:
                tf_porc.value = _article_decimal_input(pct_val)
                _safe_update_control(tf_porc)
            tf_precio.value = _article_decimal_input(price_val)
            _safe_update_control(tf_precio)
        finally:
            guard["active"] = False

    def _sync_row_pct_from_price(row_cont: ft.Container, *, normalize_inputs: bool = False) -> None:
        refs = getattr(row_cont, "price_refs", {}) or {}
        guard = refs.get("sync_guard")
        tf_precio = refs.get("precio")
        tf_porc = refs.get("porcentaje")
        dd_tipo = refs.get("tipo")
        if not isinstance(guard, dict) or tf_precio is None or tf_porc is None or dd_tipo is None:
            return
        if guard.get("active"):
            return

        try:
            cost_val = _parse_float(nuevo_articulo_costo.value, "Costo")
            price_val = _parse_float(tf_precio.value, "Precio")
        except Exception:
            return

        tipo_label = _resolve_tipo_porcentaje_label(dd_tipo.value)
        pct_val = _calc_pct_from_cost_price(cost_val, price_val, tipo_label)

        guard["active"] = True
        try:
            if normalize_inputs:
                tf_precio.value = _article_decimal_input(price_val)
                _safe_update_control(tf_precio)
            tf_porc.value = _article_decimal_input(pct_val)
            _safe_update_control(tf_porc)
        finally:
            guard["active"] = False

    def _sync_all_article_prices_from_cost() -> None:
        for row_cont in _iter_article_price_rows():
            _sync_row_price_from_pct(row_cont)

    def _build_article_price_row(
        *,
        list_name: Any,
        lista_id: Any,
        precio_value: Any = 0,
        porcentaje_value: Any = 0,
        tipo_id: Any = None,
    ) -> ft.Container:
        tf_p = ft.TextField(
            label="Precio",
            value=_article_decimal_input(precio_value),
            width=110,
            prefix_text="$",
        )
        _style_input(tf_p)

        tf_per = ft.TextField(
            label="%",
            value=_article_decimal_input(porcentaje_value),
            width=90,
        )
        _style_input(tf_per)

        dd_default = str(tipo_id) if tipo_id else _default_tipo_porcentaje_id()
        dd_tp = ft.Dropdown(
            label="Tipo de Calculo",
            width=180,
            options=[
                ft.dropdown.Option(str(t["id"]), t["tipo"]) for t in tipos_porcentaje_values
            ],
            value=dd_default,
        )
        _style_input(dd_tp)

        row_cont = ft.Container(
            content=ft.Row(
                [
                    ft.Text(str(list_name or "—"), size=13, width=120),
                    tf_p,
                    tf_per,
                    dd_tp,
                ],
                spacing=10,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            )
        )
        row_cont.price_data = {"lp_id": lista_id}
        row_cont.price_refs = {
            "precio": tf_p,
            "porcentaje": tf_per,
            "tipo": dd_tp,
            "sync_guard": {"active": False},
        }

        def _on_pct_or_tipo_change(_: Any = None) -> None:
            _sync_row_price_from_pct(row_cont)

        def _on_precio_change(_: Any = None) -> None:
            _sync_row_pct_from_price(row_cont)

        def _on_pct_commit(_: Any = None) -> None:
            _normalize_article_numeric_field(tf_per)
            _sync_row_price_from_pct(row_cont, normalize_inputs=True)

        def _on_precio_commit(_: Any = None) -> None:
            _normalize_article_numeric_field(tf_p)
            _sync_row_pct_from_price(row_cont, normalize_inputs=True)

        tf_per.on_change = _on_pct_or_tipo_change
        dd_tp.on_change = _on_pct_or_tipo_change
        tf_p.on_change = _on_precio_change
        tf_per.on_submit = _on_pct_commit
        tf_p.on_submit = _on_precio_commit
        if hasattr(tf_per, "on_blur"):
            tf_per.on_blur = _on_pct_commit  # type: ignore[attr-defined]
        if hasattr(tf_p, "on_blur"):
            tf_p.on_blur = _on_precio_commit  # type: ignore[attr-defined]

        return row_cont

    def _wire_article_cost_handlers() -> None:
        def _on_cost_change(_: Any = None) -> None:
            _sync_all_article_prices_from_cost()

        def _on_cost_commit(_: Any = None) -> None:
            _normalize_article_numeric_field(nuevo_articulo_costo)
            _sync_all_article_prices_from_cost()

        nuevo_articulo_costo.on_change = _on_cost_change
        nuevo_articulo_costo.on_submit = _on_cost_commit
        if hasattr(nuevo_articulo_costo, "on_blur"):
            nuevo_articulo_costo.on_blur = _on_cost_commit  # type: ignore[attr-defined]

    def crear_articulo(_: Any = None) -> None:
        db_conn = get_db_or_toast()
        if db_conn is None:
            return
        try:
            if not nuevo_articulo_marca.value or not nuevo_articulo_rubro.value or not nuevo_articulo_nombre.value:
                show_toast("Campos obligatorios marcados con * son requeridos", kind="warning")
                return

            costo_val = _parse_float(nuevo_articulo_costo.value, "Costo")
            if costo_val <= 0:
                show_toast("El costo debe ser mayor a 0", kind="warning")
                return

            art_id = db_conn.create_article(
                nombre=nuevo_articulo_nombre.value or "",
                codigo=nuevo_articulo_codigo.value,
                marca=nuevo_articulo_marca.value,
                rubro=nuevo_articulo_rubro.value,
                costo=costo_val,
                stock_minimo=_parse_float(nuevo_articulo_stock_minimo.value, "Stock mínimo"),
                ubicacion=nuevo_articulo_ubicacion.value,
                activo=bool(nuevo_articulo_activo.value),
                id_tipo_iva=int(nuevo_articulo_tipo_iva.value) if nuevo_articulo_tipo_iva.value else None,
                id_unidad_medida=int(nuevo_articulo_unidad.value) if nuevo_articulo_unidad.value else None,
                id_proveedor=int(nuevo_articulo_proveedor.value) if nuevo_articulo_proveedor.value else None,
                observacion=nuevo_articulo_observacion.value,
                descuento_base=_parse_float(nuevo_articulo_descuento_base.value, "Descuento Base"),
                redondeo=bool(nuevo_articulo_redondeo.value),
                porcentaje_ganancia_2=_parse_float(nuevo_articulo_ganancia_2.value, "Ganancia 2"),
                unidades_por_bulto=_parse_optional_positive_int(
                    nuevo_articulo_unidades_por_bulto.value,
                    "Unidades por bulto",
                ),
            )

            # Save prices
            if art_id:
                price_updates = []
                any_price = False
                for ctrl in articulo_precios_container.controls:
                    if isinstance(ctrl, ft.Container) and hasattr(ctrl, "price_data"):
                        lp_id = ctrl.price_data["lp_id"]
                        row = ctrl.content
                        tf_precio = row.controls[1]
                        tf_porc = row.controls[2]
                        dd_tipo = row.controls[3]
                        try:
                            price_val = _parse_float(tf_precio.value, "Precio") if tf_precio.value else None
                            if price_val and price_val > 0:
                                any_price = True
                            price_updates.append({
                                "id_lista_precio": lp_id,
                                "precio": price_val,
                                "porcentaje": _parse_float(tf_porc.value, "Porcentaje") if tf_porc.value else None,
                                "id_tipo_porcentaje": int(dd_tipo.value) if dd_tipo.value else None
                            })
                        except Exception as e:
                            # Re-raise to be caught by outer try/except and shown as toast
                            raise e
                
                if not any_price:
                    show_toast("Al menos una lista de precio debe tener un valor mayor a 0", kind="warning")
                    return
                
                db_conn.update_article_prices(art_id, price_updates)
            
            # Initial Stock Movement
            stock_ini = _parse_float(nuevo_articulo_stock_actual.value, "Stock actual")
            if stock_ini > 0:
                # Assuming type_id=1 for Adjustment/Initial Stock. 
                # Better to lookup 'Saldo Inicial' or similar if dynamic, but hardcoded ID 1 is common or needs verifiction.
                # Actually, let's check mtype_table or just use a known "Ajuste" type if available, otherwise just standard entry.
                # The user asked for "Saldo Inicial". I'll use a safe fallback logic.
                try:
                    # Try to find a suitable movement type or create one
                    mtypes = db_conn.fetch_tipos_movimiento_articulo()
                    
                    # Look for explicit "Saldo Inicial" or "Inicial" with positive sign
                    adj_type = next((t["id"] for t in mtypes if "inicial" in t["nombre"].lower() and t["signo_stock"] == 1), None)
                    
                    # Fallback: Look for "Ajuste" with positive sign
                    if not adj_type:
                        adj_type = next((t["id"] for t in mtypes if "ajuste" in t["nombre"].lower() and t["signo_stock"] == 1), None)
                    
                    # Fallback: Any positive type
                    if not adj_type:
                        adj_type = next((t["id"] for t in mtypes if t["signo_stock"] == 1), None)

                    if adj_type:
                        db_conn.create_stock_movement(
                            id_articulo=art_id,
                            id_tipo_movimiento=adj_type,
                            cantidad=stock_ini,
                            id_deposito=1, # Default deposito
                            observacion="Saldo inicial al crear artículo"
                        )
                    else:
                        print("Warning: No positive stock movement type found for initial stock.")
                except Exception as e:
                    print(f"Error creating initial stock: {e}")

            close_form()
            if db_conn:
                db_conn.log_activity("ARTICULO", "INSERT", id_entidad=art_id, detalle={"nombre": nuevo_articulo_nombre.value})
            show_toast("Artículo creado", kind="success")
            articulos_table.refresh()
        except Exception as exc:
            show_toast(f"Error: {exc}", kind="error")

    def guardar_edicion_articulo(_: Any = None) -> None:
        nonlocal editing_article_id
        db_conn = get_db_or_toast()
        if db_conn is None or editing_article_id is None:
            return
        try:
            if not nuevo_articulo_marca.value or not nuevo_articulo_rubro.value or not nuevo_articulo_nombre.value:
                show_toast("Campos obligatorios marcados con * son requeridos", kind="warning")
                return

            costo_val = _parse_float(nuevo_articulo_costo.value, "Costo")
            if costo_val <= 0:
                show_toast("El costo debe ser mayor a 0", kind="warning")
                return

            updates = {
                "nombre": nuevo_articulo_nombre.value or "",
                "codigo": nuevo_articulo_codigo.value,
                "marca": nuevo_articulo_marca.value,
                "rubro": nuevo_articulo_rubro.value,
                "costo": costo_val,
                "stock_minimo": _parse_float(nuevo_articulo_stock_minimo.value, "Stock mínimo"),
                "ubicacion": nuevo_articulo_ubicacion.value,
                "activo": bool(nuevo_articulo_activo.value),
                "id_tipo_iva": int(nuevo_articulo_tipo_iva.value) if nuevo_articulo_tipo_iva.value else None,
                "id_unidad_medida": int(nuevo_articulo_unidad.value) if nuevo_articulo_unidad.value else None,
                "id_proveedor": int(nuevo_articulo_proveedor.value) if nuevo_articulo_proveedor.value else None,
                "observacion": nuevo_articulo_observacion.value,
                "descuento_base": _parse_float(nuevo_articulo_descuento_base.value, "Descuento Base"),
                "redondeo": bool(nuevo_articulo_redondeo.value),
                "porcentaje_ganancia_2": _parse_float(nuevo_articulo_ganancia_2.value, "Ganancia 2"),
                "unidades_por_bulto": _parse_optional_positive_int(
                    nuevo_articulo_unidades_por_bulto.value,
                    "Unidades por bulto",
                ),
            }
            changes = dict(updates)
            db_conn.update_article_fields(editing_article_id, updates)
            
            # Update complex prices
            price_updates = []
            any_price = False
            for ctrl in articulo_precios_container.controls:
                if isinstance(ctrl, ft.Container) and hasattr(ctrl, "price_data"):
                    lp_id = ctrl.price_data["lp_id"]
                    row = ctrl.content
                    tf_precio = row.controls[1]
                    tf_porc = row.controls[2]
                    dd_tipo = row.controls[3]
                    try:
                        price_val = _parse_float(tf_precio.value, "Precio") if tf_precio.value else None
                        if price_val and price_val > 0:
                            any_price = True
                        price_updates.append({
                            "id_lista_precio": lp_id,
                            "precio": price_val,
                            "porcentaje": _parse_float(tf_porc.value, "Porcentaje") if tf_porc.value else None,
                            "id_tipo_porcentaje": int(dd_tipo.value) if dd_tipo.value else None
                        })
                    except Exception as e:
                        raise e
            
            if not any_price:
                show_toast("Al menos una lista de precio debe tener un valor mayor a 0", kind="warning")
                return

            # Prices
            if price_updates:
                db_conn.update_article_prices(editing_article_id, price_updates)
                
            # Handle Stock Change (Auto-Adjustment)
            new_stock = _parse_float(nuevo_articulo_stock_actual.value, "Stock")

            # Fetch current stock (fresh)
            # We need to know the current stock to calc diff. 
            # `fetch_article_by_id` usually joins stock, let's verify or fetch separate.
            # `v_articulo_detallado` has `stock_actual`.
            current_art_data = db_conn.fetch_article_by_id(editing_article_id)
            current_stock = float(current_art_data.get("stock_actual", 0)) if current_art_data else 0.0
            
            diff = new_stock - current_stock
            
            if abs(diff) > 0.01: # Aumentado el epsilon para evitar micro-ajustes por decimales y errores de redondeo
                try:
                    mtypes = db_conn.fetch_tipos_movimiento_articulo()
                    # Find Adjustment types
                    # Positive diff -> Adjustment Positive (Sign +1)
                    # Negative diff -> Adjustment Negative (Sign -1)
                    
                    target_sign = 1 if diff > 0 else -1
                    
                    # Try to find a type that matches "Ajuste" and the sign
                    # This is heuristic. Ideally we have fixed IDs or Codes.
                    # Looking for "Ajuste" in name
                    adj_type_id = None
                    for mt in mtypes:
                        if "ajuste" in mt["nombre"].lower() and mt["signo_stock"] == target_sign:
                            adj_type_id = mt["id"]
                            break
                    
                    # Fallback strategies if "Ajuste" not found specifically
                    if not adj_type_id:
                        # Find ANY type with the correct sign
                        for mt in mtypes:
                             if mt["signo_stock"] == target_sign:
                                 adj_type_id = mt["id"]
                                 break
                                 
                    if adj_type_id:
                        db_conn.create_stock_movement(
                            id_articulo=editing_article_id,
                            id_tipo_movimiento=adj_type_id,
                            cantidad=abs(diff), # Movement amount is always positive, sign determines effect? 
                            # Wait, create_stock_movement usually takes positive quantity. Sign comes from Type.
                            # Standard logic: Qty is absolute. Type has sign.
                            id_deposito=1, # Default
                            observacion=f"Ajuste manual desde edición (Stock: {current_stock} -> {new_stock})"
                        )
                except Exception as e:
                    print(f"Error creating stock adjustment: {e}")
                    show_toast(f"Error ajustando stock: {e}", kind="error")
            
            close_form()
            if db_conn:
                db_conn.log_activity("ARTICULO", "UPDATE", id_entidad=editing_article_id, detalle=changes)
            show_toast("Artículo actualizado", kind="success")
            articulos_table.refresh()
        except Exception as exc:
            show_toast(f"Error: {exc}", kind="error")

    def _prepare_article_form_content() -> ft.Control:
        section_title = lambda text: ft.Column([
            ft.Text(text, weight=ft.FontWeight.BOLD, size=14, color=COLOR_ACCENT),
            ft.Divider(height=1, thickness=1, color=ft.Colors.GREY_300),
        ], spacing=5)

        return ft.Container(
            width=620,
            padding=ft.padding.only(bottom=20),
            content=ft.Column(
                [
                    section_title("Información General"),
                    ft.Row([nuevo_articulo_nombre], spacing=10),
                    ft.Row([nuevo_articulo_codigo], spacing=10),
                    ft.Row([nuevo_articulo_marca, nuevo_articulo_rubro], spacing=10),
                    ft.Row([nuevo_articulo_tipo_iva, nuevo_articulo_unidad], spacing=10),
                    ft.Row([nuevo_articulo_proveedor], spacing=10),
                    
                    ft.Container(height=10),
                    section_title("Costos y Stock"),
                    ft.Row([nuevo_articulo_costo, nuevo_articulo_stock_minimo], spacing=10),
                    ft.Row([nuevo_articulo_stock_actual, nuevo_articulo_unidades_por_bulto], spacing=10),
                    ft.Row([nuevo_articulo_descuento_base, nuevo_articulo_ganancia_2, nuevo_articulo_redondeo], spacing=20, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                    
                    ft.Container(height=10),
                    section_title("Logística y Notas"),
                    ft.Row([nuevo_articulo_ubicacion], spacing=10),
                    ft.Row([nuevo_articulo_observacion], spacing=10),
                    
                    ft.Container(height=10),
                    ft.Row([
                        ft.Text("Lista", width=120, weight=ft.FontWeight.BOLD, size=12),
                        ft.Text("Precio ($)", width=110, weight=ft.FontWeight.BOLD, size=12),
                        ft.Text("Porc. (%)", width=90, weight=ft.FontWeight.BOLD, size=12),
                        ft.Text("Tipo", width=180, weight=ft.FontWeight.BOLD, size=12),
                    ], spacing=10),
                    articulo_precios_container,
                    
                    ft.Container(height=10),
                    ft.Row([nuevo_articulo_activo], spacing=10),
                ],
                spacing=10,
                tight=True,
                scroll=ft.ScrollMode.HIDDEN,
            ),
        )

    def _populate_dropdowns():
        depositos = db.fetch_depositos()
        nuevo_articulo_marca.options = [ft.dropdown.Option(m, m) for m in marcas_values]
        nuevo_articulo_rubro.options = [ft.dropdown.Option(r, r) for r in rubros_values]
        nuevo_articulo_tipo_iva.options = [ft.dropdown.Option(str(t["id"]), f"{t['descripcion']} ({t['porcentaje']}%)") for t in tipos_iva_values]
        nuevo_articulo_unidad.options = [ft.dropdown.Option(str(u["id"]), f"{u['nombre']} ({u['abreviatura']})") for u in unidades_values]
        try:
            supplier_rows = db.fetch_entities(tipo="PROVEEDOR", limit=500, offset=0)
            supplier_options: List[Dict[str, Any]] = [
                {
                    "value": "",
                    "label": "Seleccionar proveedor",
                    "selected_label": "Seleccionar proveedor",
                    "tooltip": "Seleccionar proveedor",
                }
            ]
            supplier_options.extend(
                _format_entity_option(row, include_tipo=False, force_tipo="Proveedor")
                for row in supplier_rows
            )
            nuevo_articulo_proveedor.options = supplier_options
        except Exception:
            nuevo_articulo_proveedor.options = [ft.dropdown.Option("", "Seleccionar proveedor")] + [ft.dropdown.Option(str(p["id"]), p["nombre"]) for p in proveedores_values]
        nuevo_articulo_ubicacion.options = [ft.dropdown.Option(d["nombre"], d["nombre"]) for d in depositos]
        if depositos:
            nuevo_articulo_ubicacion.value = depositos[0]["nombre"]

    def open_nuevo_articulo(_: Any = None) -> None:
        nonlocal editing_article_id
        editing_article_id = None
        db_conn = get_db_or_toast()
        if db_conn is None: return
        try: reload_catalogs()
        except Exception as exc: show_toast(f"Error cargando catálogos: {exc}", kind="error")

        _populate_dropdowns()
        
        nuevo_articulo_nombre.value = ""
        nuevo_articulo_codigo.value = ""
        nuevo_articulo_marca.value = ""
        nuevo_articulo_rubro.value = ""
        # Default IVA to 21% if found
        def_iva = next((str(t["id"]) for t in tipos_iva_values if "21" in str(t["porcentaje"])), "")
        nuevo_articulo_tipo_iva.value = def_iva
        
        # Default Unidad to 'Unidad' if found
        def_un = next((str(u["id"]) for u in unidades_values if "unidad" in u["nombre"].lower()), "")
        nuevo_articulo_unidad.value = def_un
        
        nuevo_articulo_proveedor.value = ""
        nuevo_articulo_costo.value = "0"
        nuevo_articulo_stock_minimo.value = "0"
        nuevo_articulo_stock_actual.value = "0"
        nuevo_articulo_stock_actual.read_only = False # Enabled for creation
        nuevo_articulo_unidades_por_bulto.value = ""
        nuevo_articulo_descuento_base.value = "0"
        nuevo_articulo_ganancia_2.value = "0"
        nuevo_articulo_redondeo.value = False
        nuevo_articulo_ubicacion.value = ""
        nuevo_articulo_observacion.value = ""
        nuevo_articulo_activo.value = True

        # Fetch and build empty prices for all lists
        try:
            lists = db_conn.fetch_listas_precio()
            articulo_precios_container.controls.clear()
            for l in lists:
                if not l.get("activa", True):
                    continue
                articulo_precios_container.controls.append(
                    _build_article_price_row(
                        list_name=l.get("nombre"),
                        lista_id=l.get("id"),
                        precio_value=0,
                        porcentaje_value=0,
                        tipo_id=None,
                    )
                )
        except Exception as e:
            logger.warning(f"Falló al llenar precios de artículo: {e}")

        _wire_article_cost_handlers()

        open_form(
            "Nuevo artículo",
            _prepare_article_form_content(),
            [
                _cancel_button("Cancelar", on_click=close_form),
                ft.ElevatedButton(
                    "Crear",
                    icon=ft.icons.ADD,
                    bgcolor=COLOR_ACCENT,
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=crear_articulo,
                ),
            ],
        )

    def open_detalle_articulo(art_id: int) -> None:
        db_conn = get_db_or_toast()
        if db_conn is None: return
        
        try:
            art = db_conn.get_article_details(art_id)
            if not art:
                show_toast("Artículo no encontrado", kind="error")
                return
            
            # Helper for info rows
            def info_row(label, value, icon=None):
                return ft.Row([
                    ft.Icon(icon, size=16, color=COLOR_TEXT_MUTED) if icon else ft.Container(width=16),
                    ft.Text(f"{label}:", size=14, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED, width=120),
                    ft.Text(str(value or "—"), size=14, color=COLOR_TEXT, weight=ft.FontWeight.W_500, expand=True),
                ], spacing=10)

            # Price List Table
            price_rows = []
            for p in art.get('precios', []):
                price_rows.append(ft.DataRow(cells=[
                    ft.DataCell(ft.Text(p['lista_nombre'], weight=ft.FontWeight.BOLD)),
                    ft.DataCell(ft.Text(_format_money(p['precio']))),
                    ft.DataCell(ft.Text(f"{format_percent(p['porcentaje'], decimals=2)} ({p['tipo_porcentaje']})")),
                    ft.DataCell(ft.Text(p['fecha_actualizacion'].strftime("%d/%m/%Y %H:%M") if p['fecha_actualizacion'] else "—", size=11)),
                ]))

            prices_table = ft.Row(
                [
                    SafeDataTable(
                        columns=[
                            ft.DataColumn(ft.Text("Lista")),
                            ft.DataColumn(ft.Text("Precio")),
                            ft.DataColumn(ft.Text("Margen/Desc")),
                            ft.DataColumn(ft.Text("Actualizado")),
                        ],
                        rows=price_rows,
                        heading_row_color="#F1F5F9",
                        border_radius=10,
                        border=ft.border.all(1, COLOR_BORDER),
                    )
                ],
                scroll=ft.ScrollMode.ADAPTIVE,
            )

            # Toggle active button
            is_active = bool(art.get('activo', True))
            def toggle_status(_):
                new_status = not is_active
                if db_conn.set_article_active(art_id, new_status):
                    show_toast(f"Artículo {'activado' if new_status else 'desactivado'}", kind="success")
                    close_form()
                    articulos_table.refresh()
                else:
                    show_toast("Error al cambiar estado", kind="error")

            status_btn = ft.ElevatedButton(
                "Desactivar" if is_active else "Activar",
                icon=ft.icons.DO_NOT_DISTURB_ON_ROUNDED if is_active else ft.icons.CHECK_CIRCLE_ROUNDED,
                bgcolor=COLOR_ERROR if is_active else COLOR_SUCCESS,
                color="#FFFFFF",
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                on_click=toggle_status
            )

            content = ft.Column([
                ft.Container(
                    content=ft.Column([
                        ft.Text("Información General", size=16, weight=ft.FontWeight.BOLD),
                        ft.Divider(height=1, color=COLOR_BORDER),
                                ft.Row([
                                    ft.Column([
                                        info_row("Código", art.get('codigo') or "—", ft.icons.INFO_OUTLINE_ROUNDED),
                                        info_row("Marca", art.get('marca'), ft.icons.LABEL_ROUNDED),
                                        info_row("Rubro", art.get('rubro'), ft.icons.CATEGORY_ROUNDED),
                                        info_row("Proveedor", art.get('proveedor'), ft.icons.BUSINESS_ROUNDED),
                                        info_row(
                                            "PGan 2",
                                            format_percent(art.get("porcentaje_ganancia_2", 0), decimals=2)
                                            if art.get("porcentaje_ganancia_2") is not None
                                            else "—",
                                            ft.icons.PERCENT_ROUNDED,
                                        ),
                                        info_row("Notas", art.get('observacion'), ft.icons.NOTE_ROUNDED),
                                    ], expand=True),
                                    ft.Column([
                                        info_row("Costo", _format_money(art.get('costo')), ft.icons.MONEY_ROUNDED),
                                        info_row("Stock Actual", f"{float(art.get('stock_actual', 0)):.2f} {art.get('unidad_abreviatura') or ''}", ft.icons.INVENTORY_ROUNDED),
                                        info_row("Unid./Bulto", art.get("unidades_por_bulto") if art.get("unidades_por_bulto") is not None else "—", ft.icons.INVENTORY_ROUNDED),
                                        info_row("Ubicación", art.get('ubicacion'), ft.icons.LOCATION_ON_ROUNDED),
                                    ], expand=True),
                                ], spacing=40),
                    ], spacing=15),
                    padding=20,
                    bgcolor="#F1F5F9",
                    border_radius=15,
                ),
                ft.Container(height=10),
                ft.Text("Estructura de Precios", size=16, weight=ft.FontWeight.BOLD),
                ft.Container(content=ft.Column([prices_table], scroll=ft.ScrollMode.ADAPTIVE), height=250),
                ft.Container(height=10),
            ], spacing=10, width=750, scroll=ft.ScrollMode.ADAPTIVE)

            open_form(
                f"Detalles: {art.get('nombre')}",
                content,
                [
                    status_btn,
                    _cancel_button("Cerrar", on_click=close_form),
                ]
            )
            
        except Exception as exc:
            show_toast(f"Error al cargar detalles: {exc}", kind="error")

    def open_editar_articulo(art_id: int) -> None:
        nonlocal editing_article_id
        editing_article_id = art_id
        db_conn = get_db_or_toast()
        if db_conn is None: return
        try:
            reload_catalogs()
        except Exception as e:
            logger.warning(f"Falló al recargar catálogos en editar artículo: {e}")

        _populate_dropdowns()

        # Fetch data
        try:
            art = db_conn.fetch_article_by_id(art_id)
            if not art:
                show_toast("Artículo no encontrado", kind="error")
                return

            nuevo_articulo_nombre.value = art.get("nombre", "")
            nuevo_articulo_codigo.value = art.get("codigo") or ""
            nuevo_articulo_marca.value = art.get("marca_nombre") or ""
            nuevo_articulo_rubro.value = art.get("rubro_nombre") or ""
            nuevo_articulo_tipo_iva.value = str(art["id_tipo_iva"]) if art.get("id_tipo_iva") else ""
            nuevo_articulo_unidad.value = str(art["id_unidad_medida"]) if art.get("id_unidad_medida") else ""
            nuevo_articulo_proveedor.value = str(art["id_proveedor"]) if art.get("id_proveedor") else ""
            nuevo_articulo_costo.value = str(art.get("costo", 0))
            nuevo_articulo_stock_minimo.value = str(art.get("stock_minimo", 0))
            nuevo_articulo_stock_actual.value = str(art.get("stock_actual", 0))
            nuevo_articulo_stock_actual.read_only = False # Enabled to allow adjustments
            nuevo_articulo_unidades_por_bulto.value = (
                str(art.get("unidades_por_bulto"))
                if art.get("unidades_por_bulto") is not None
                else ""
            )
            nuevo_articulo_descuento_base.value = str(art.get("descuento_base", 0))
            nuevo_articulo_ganancia_2.value = str(art.get("porcentaje_ganancia_2") or 0)
            nuevo_articulo_redondeo.value = bool(art.get("redondeo", False))
            nuevo_articulo_ubicacion.value = art.get("ubicacion") or ""
            nuevo_articulo_observacion.value = art.get("observacion") or ""
            nuevo_articulo_activo.value = bool(art.get("activo", True))

            # Fetch and build prices
            prices = db_conn.fetch_article_prices(art_id)
            articulo_precios_container.controls.clear()
            for p in prices:
                articulo_precios_container.controls.append(
                    _build_article_price_row(
                        list_name=p.get("lista_nombre") or "—",
                        lista_id=p.get("id_lista_precio"),
                        precio_value=p.get("precio") or 0,
                        porcentaje_value=p.get("porcentaje") or 0,
                        tipo_id=p.get("id_tipo_porcentaje"),
                    )
                )

        except Exception as exc:
            show_toast(f"Error al cargar artículo: {exc}", kind="error")
            return

        _wire_article_cost_handlers()

        open_form(
            "Editar artículo",
            _prepare_article_form_content(),
            [
                _cancel_button("Cancelar", on_click=close_form),
                ft.ElevatedButton(
                    "Guardar Cambios",
                    icon=ft.icons.SAVE_ROUNDED,
                    bgcolor=COLOR_ACCENT,
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=guardar_edicion_articulo,
                ),
            ],
        )

    # ---- Sistema Configuration ----
    sys_nombre = ft.TextField(label="Nombre del Sistema", width=300)
    sys_razon_social = ft.TextField(label="Razón Social", width=300)
    sys_cuit = ft.TextField(label="CUIT Empresa", width=200)
    sys_domicilio = ft.TextField(label="Domicilio", width=400)
    sys_telefono = ft.TextField(label="Teléfono", width=200)
    sys_email = ft.TextField(label="Email", width=300)
    sys_slogan = ft.TextField(label="Slogan", width=400)
    sys_logo_path = ""  # Will store the actual path
    
    # Style all inputs
    for ctrl in [sys_nombre, sys_razon_social, sys_cuit, sys_domicilio, sys_telefono, sys_email, sys_slogan]:
        _style_input(ctrl)

    # Logo preview components
    sys_logo_preview = ft.Image(src="", width=120, height=120, fit=ft.ImageFit.CONTAIN, visible=False)
    sys_logo_label = ft.Text("", size=12, color=COLOR_TEXT_MUTED)
    
    def clear_logo(_: Any = None):
        nonlocal sys_logo_path
        sys_logo_path = ""
        sys_logo_preview.src = ""
        sys_logo_preview.visible = False
        sys_logo_label.value = ""
        btn_clear_logo.visible = False
        # Optional: update branding immediately for preview
        update_branding(sys_nombre.value, sys_slogan.value, "")
        page.update()

    btn_clear_logo = ft.TextButton(
        "Quitar logo", 
        icon=ft.icons.DELETE_OUTLINE, 
        on_click=clear_logo, 
        visible=False,
        style=ft.ButtonStyle(color=COLOR_ERROR)
    )

    def load_sistema_config():
        """Load system configuration from database into fields."""
        nonlocal sys_logo_path
        if not db:
            return
        try:
            cfg = db.fetch_config_sistema()
            sys_nombre.value = cfg.get("nombre_sistema", {}).get("valor", "")
            sys_razon_social.value = cfg.get("razon_social", {}).get("valor", "")
            sys_cuit.value = cfg.get("cuit_empresa", {}).get("valor", "")
            sys_domicilio.value = cfg.get("domicilio_empresa", {}).get("valor", "")
            sys_telefono.value = cfg.get("telefono_empresa", {}).get("valor", "")
            sys_email.value = cfg.get("email_empresa", {}).get("valor", "")
            sys_slogan.value = cfg.get("slogan", {}).get("valor", "")
            sys_logo_path = cfg.get("logo_path", {}).get("valor", "")
            
            # Update logo preview if path exists
            if sys_logo_path and sys_logo_path.strip():
                sys_logo_preview.src = sys_logo_path
                sys_logo_preview.visible = True
                sys_logo_label.value = sys_logo_path.split("\\")[-1].split("/")[-1]  # Just filename
                btn_clear_logo.visible = True
            else:
                sys_logo_preview.visible = False
                sys_logo_label.value = ""
                btn_clear_logo.visible = False
            
            # Update page title if a name is configured
            nombre = sys_nombre.value
            if nombre and nombre.strip():
                page.title = nombre
            
            page.update()
        except Exception as exc:
            print(f"Error loading sistema config: {exc}")

    def save_sistema_config(_: Any = None):
        """Save system configuration to database."""
        if not db:
            show_toast("Sin conexión a la base de datos", kind="error")
            return
        try:
            updates = {
                "nombre_sistema": (sys_nombre.value or "").strip(),
                "razon_social": (sys_razon_social.value or "").strip(),
                "cuit_empresa": (sys_cuit.value or "").strip(),
                "domicilio_empresa": (sys_domicilio.value or "").strip(),
                "telefono_empresa": (sys_telefono.value or "").strip(),
                "email_empresa": (sys_email.value or "").strip(),
                "slogan": (sys_slogan.value or "").strip(),
                "logo_path": sys_logo_path or "",
            }
            db.update_config_sistema_bulk(updates)
            
            # Update branding immediately
            update_branding(updates["nombre_sistema"], updates["slogan"], updates["logo_path"])
            
            # Update page title immediately
            if updates["nombre_sistema"]:
                page.title = updates["nombre_sistema"]
            
            page.update()
            show_toast("Configuración del sistema guardada", kind="success")
        except Exception as exc:
            show_toast(f"Error al guardar: {exc}", kind="error")

    # Drag and drop for logo
    def on_logo_dropped(e: ft.DragTargetAcceptEvent):
        nonlocal sys_logo_path
        # e.data contains the file path from the drag
        if e.data:
            sys_logo_path = e.data
            sys_logo_preview.src = e.data
            sys_logo_preview.visible = True
            sys_logo_label.value = e.data.split("\\")[-1].split("/")[-1]  # Just filename
            btn_clear_logo.visible = True
            update_branding(sys_nombre.value, sys_slogan.value, e.data)
            page.update()

    def on_logo_picked(e: ft.FilePickerResultEvent):
        nonlocal sys_logo_path
        if e.files and len(e.files) > 0:
            selected = e.files[0].path
            sys_logo_path = selected
            sys_logo_preview.src = selected
            sys_logo_preview.visible = True
            sys_logo_label.value = selected.split("\\")[-1].split("/")[-1]
            btn_clear_logo.visible = True
            update_branding(sys_nombre.value, sys_slogan.value, selected)
            page.update()
    
    logo_picker = ft.FilePicker(on_result=on_logo_picked)
    page.overlay.append(logo_picker)

    def select_logo_click(_: Any = None):
        logo_picker.pick_files(
            dialog_title="Seleccionar Logo",
            allowed_extensions=["png", "jpg", "jpeg", "gif", "svg", "webp"],
            allow_multiple=False
        )

    # Drop zone for logo
    logo_drop_zone = ft.Container(
        content=ft.Column([
            ft.Icon(ft.icons.CLOUD_UPLOAD_ROUNDED, size=40, color=COLOR_TEXT_MUTED),
            ft.Text("Arrastrá una imagen aquí", size=14, color=COLOR_TEXT_MUTED, text_align=ft.TextAlign.CENTER),
            ft.Text("o", size=12, color=COLOR_TEXT_MUTED),
            ft.TextButton("Seleccionar archivo", on_click=select_logo_click),
        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=5),
        width=200,
        height=150,
        border=ft.border.all(2, COLOR_BORDER),
        border_radius=10,
        alignment=ft.alignment.center,
        on_click=select_logo_click,
    )

    logo_section = ft.Row([
        logo_drop_zone,
        ft.Container(width=20),
        ft.Column([
            sys_logo_preview,
            sys_logo_label,
            btn_clear_logo,
        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=5),
    ], spacing=20, vertical_alignment=ft.CrossAxisAlignment.START)

    sistema_tab_content = ft.Container(
        content=ft.Column([
            ft.Container(height=10), # Extra space top
            ft.Text("Información General", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT),

            ft.Row([sys_nombre, sys_slogan], spacing=20, wrap=True),
            ft.Divider(height=40, color=COLOR_BORDER, thickness=1), # More space in divider
            ft.Text("Datos de la Empresa", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT),
            ft.Row([sys_razon_social, sys_cuit], spacing=20, wrap=True),
            ft.Row([sys_domicilio], spacing=20, wrap=True),
            ft.Row([sys_telefono, sys_email], spacing=20, wrap=True),
            ft.Divider(height=40, color=COLOR_BORDER, thickness=1),
            ft.Text("Logo de la Empresa", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT),
            logo_section,
            ft.Container(height=30),
            ft.ElevatedButton("Guardar Configuración del Sistema", icon=ft.icons.SAVE_ROUNDED, on_click=save_sistema_config, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))),
            ft.Container(height=30), # Padding at bottom
        ], spacing=15, scroll=ft.ScrollMode.AUTO, expand=True),
        padding=ft.padding.only(left=10, right=10),
        expand=True
    )

    # ---- Config (catálogos) ----
    nueva_marca = ft.TextField(label="Nueva marca", width=260)
    _style_input(nueva_marca)

    def agregar_marca(_: Any = None) -> None:
        db_conn = get_db_or_toast()
        if db_conn is None:
            return
        nombre = (nueva_marca.value or "").strip()
        if not nombre:
            show_toast("Escribe un nombre para la marca.", kind="error")
            return
        try:
            db_conn.create_marca(nombre)
            nueva_marca.value = ""
            reload_catalogs()
            show_toast("Marca creada", kind="success")
            marcas_table.refresh()
            articulos_table.refresh()
        except Exception as exc:
            show_toast(f"Error: {exc}", kind="error")

    nueva_rubro = ft.TextField(label="Nuevo rubro", width=260)
    _style_input(nueva_rubro)

    def agregar_rubro(_: Any = None) -> None:
        db_conn = get_db_or_toast()
        if db_conn is None:
            return
        nombre = (nueva_rubro.value or "").strip()
        if not nombre:
            show_toast("Escribe un nombre para el rubro.", kind="error")
            return
        try:
            db_conn.create_rubro(nombre)
            nueva_rubro.value = ""
            reload_catalogs()
            show_toast("Rubro creado", kind="success")
            rubros_table.refresh()
            articulos_table.refresh()
        except Exception as exc:
            show_toast(f"Error: {exc}", kind="error")

    def marcas_provider(
        offset: int,
        limit: int,
        search: Optional[str],
        simple: Optional[str],
        advanced: Dict[str, Any],
        sorts: List[Tuple[str, str]],
    ):
        if db is None:
            raise provider_error()
        rows = db.fetch_marcas(search=search, sorts=sorts, limit=limit, offset=offset)
        total = db.count_marcas(search=search)
        return rows, total

    def rubros_provider(
        offset: int,
        limit: int,
        search: Optional[str],
        simple: Optional[str],
        advanced: Dict[str, Any],
        sorts: List[Tuple[str, str]],
    ):
        if db is None:
            raise provider_error()
        rows = db.fetch_rubros(search=search, sorts=sorts, limit=limit, offset=offset)
        total = db.count_rubros(search=search)
        return rows, total

    def delete_marca(marca_id: int) -> None:
        if db is None:
            raise provider_error()
        db.delete_marcas([int(marca_id)])
        reload_catalogs()
        show_toast("Marca eliminada", kind="success")
        marcas_table.refresh()
        articulos_table.refresh()

    def delete_rubro(rubro_id: int) -> None:
        if db is None:
            raise provider_error()
        db.delete_rubros([int(rubro_id)])
        reload_catalogs()
        show_toast("Rubro eliminado", kind="success")
        rubros_table.refresh()
        articulos_table.refresh()

    def delete_provincia(pid: int) -> None:
        if db: db.delete_provincias([int(pid)]); reload_catalogs(); provincias_table.refresh(); show_toast("Provincia eliminada", kind="success")

    def delete_localidad(lid: int) -> None:
        if db: db.delete_localidades([int(lid)]); reload_catalogs(); localidades_table.refresh(); show_toast("Localidad eliminada", kind="success")

    def delete_unidad(uid: int) -> None:
        if db: db.delete_unidades_medida([int(uid)]); reload_catalogs(); unidades_table.refresh(); show_toast("Unidad eliminada", kind="success")

    def delete_civa(cid: int) -> None:
        if db: db.delete_condiciones_iva([int(cid)]); reload_catalogs(); civa_table.refresh(); show_toast("Condición eliminada", kind="success")

    def delete_tiva(tid: int) -> None:
        if db: db.delete_tipos_iva([int(tid)]); reload_catalogs(); tiva_table.refresh(); show_toast("Tipo IVA eliminado", kind="success")

    def delete_deposito(did: int) -> None:
        if db: db.delete_depositos([int(did)]); reload_catalogs(); depo_table.refresh(); show_toast("Depósito eliminado", kind="success")

    def delete_fpay(fid: int) -> None:
        if db: db.delete_formas_pago([int(fid)]); reload_catalogs(); fpay_table.refresh(); show_toast("Forma de pago eliminada", kind="success")

    def delete_lista_precio(lid: int) -> None:
        if db: db.delete_listas_precio([int(lid)]); reload_catalogs(); precios_table.refresh(); show_toast("Lista de precio eliminada", kind="success")

    def delete_ptype(pid: int) -> None:
        if db: db.delete_tipos_porcentaje([int(pid)]); reload_catalogs(); ptype_table.refresh(); show_toast("Tipo porcentaje eliminado", kind="success")

    def delete_dtype(did: int) -> None:
        if db: db.delete_tipos_documento([int(did)]); reload_catalogs(); dtype_table.refresh(); show_toast("Tipo documento eliminado", kind="success")

    def delete_mtype(mid: int) -> None:
        if db: db.delete_tipos_movimiento_articulo([int(mid)]); reload_catalogs(); mtype_table.refresh(); show_toast("Tipo movimiento eliminado", kind="success")

    def delete_usuario(uid: int) -> None:
        # Users might be sensitive, but consistent with others
        if db: db.update_user_fields(int(uid), {"activo": False}); usuarios_table.refresh(); show_toast("Usuario desactivado correctamente", kind="success")

    def toggle_usuario(uid: int, is_active: bool) -> None:
        if not db: return
        if db.current_user_id and int(uid) == int(db.current_user_id):
            show_toast("No puedes desactivar tu propio usuario", kind="error")
            return
        if is_active:
            ask_confirm("Desactivar", "¿Desactivar usuario?", "Sí, desactivar", lambda: (db.update_user_fields(int(uid), {"activo": False}), usuarios_table.refresh(), show_toast("Usuario desactivado correctamente", kind="success")))
        else:
            ask_confirm("Reactivar", "¿Reactivar usuario?", "Sí, reactivar", lambda: (db.update_user_fields(int(uid), {"activo": True}), usuarios_table.refresh(), show_toast("Usuario reactivado correctamente", kind="success")), button_color=COLOR_SUCCESS)


    marcas_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Marca", editable=True, width=320),
            ColumnConfig(
                key="_delete",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE,
                    tooltip="Eliminar marca",
                    icon_color="#DC2626",
                    on_click=lambda e, rid=row.get("id"): (
                        ask_confirm(
                            "Eliminar marca",
                            "¿Estás seguro que deseas eliminar la marca seleccionada? Puede fallar si está en uso por artículos.",
                            "Eliminar",
                            lambda: delete_marca(int(rid)),
                        )
                        if rid is not None
                        else None
                    ),
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container(),
                width=40,
            ),
        ],
        data_provider=marcas_provider,
        inline_edit_callback=lambda row_id, changes: db.update_marca_fields(int(row_id), changes) if db else None,
        show_inline_controls=True,
        show_mass_actions=False,
        show_selection=False,
        auto_load=False,
        page_size=12,
        page_size_options=(10, 25, 50),
    )
    marcas_table.search_field.hint_text = "Buscar marca…"

    rubros_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Rubro", editable=True, width=320),
            ColumnConfig(
                key="_delete",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE,
                    tooltip="Eliminar rubro",
                    icon_color="#DC2626",
                    on_click=lambda e, rid=row.get("id"): (
                        ask_confirm(
                            "Eliminar rubro",
                            "¿Estás seguro que deseas eliminar el rubro seleccionado? Puede fallar si está en uso por artículos.",
                            "Eliminar",
                            lambda: delete_rubro(int(rid)),
                        )
                        if rid is not None
                        else None
                    ),
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container(),
                width=40,
            ),
        ],
        data_provider=rubros_provider,
        inline_edit_callback=lambda row_id, changes: db.update_rubro_fields(int(row_id), changes) if db else None,
        show_inline_controls=True,
        show_mass_actions=False,
        show_selection=False,
        auto_load=False,
        page_size=12,
        page_size_options=(10, 25, 50),
    )
    rubros_table.search_field.hint_text = "Buscar rubro…"

    # NEW: Generic providers and tables for expanded config
    def create_catalog_provider(fetch_fn, count_fn):
        def provider(offset, limit, search, simple, advanced, sorts):
            if db is None or db.is_closing: return [], 0
            try:
                entidad_log = fetch_fn.__name__.replace("fetch_", "").upper()
                db.log_activity(entidad_log, "SELECT", detalle={"search": search, "offset": offset})
                
                # Pass all arguments to be robust
                try:
                    data = fetch_fn(search=search, limit=limit, offset=offset, simple=simple, advanced=advanced, sorts=sorts)
                except TypeError:
                    # Fallback for functions that don't take all arguments
                    data = fetch_fn(search=search, limit=limit, offset=offset)
                    
                try:
                    count = count_fn(search=search, simple=simple, advanced=advanced)
                except TypeError:
                    count = count_fn(search=search)
                    
                return data, count
            except (Exception, RuntimeError):
                return [], 0 # Silence errors on shutdown or connection loss
        return provider

    # Provinces
    provincias_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Provincia", editable=True, width=320),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar esta provincia?", "Eliminar", lambda: delete_provincia(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_provincias, db.count_provincias),
        inline_edit_callback=lambda rid, changes: db.update_provincia_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_mass_actions=False, show_selection=False, auto_load=False, page_size=12,
    )
    nueva_provincia_input = ft.TextField(label="Nueva Provincia", width=220)
    _style_input(nueva_provincia_input)

    def agregar_provincia(_: Any = None):
        nom = (nueva_provincia_input.value or "").strip()
        if not nom: return
        try:
            db.create_provincia(nom)
            nueva_provincia_input.value = ""
            provincias_table.refresh()
            show_toast("Provincia agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Localities
    localidades_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Localidad", editable=True, width=200),
            ColumnConfig(key="provincia", label="Provincia", editable=False, width=200),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar esta localidad?", "Eliminar", lambda: delete_localidad(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_localidades, db.count_localidades),
        inline_edit_callback=lambda rid, changes: db.update_localidad_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_mass_actions=False, show_selection=False, auto_load=False, page_size=12,
    )
    nueva_loc_nombre = ft.TextField(label="Nombre Localidad", width=220)
    nueva_loc_prov = ft.Dropdown(label="Provincia", width=220)
    _style_input(nueva_loc_nombre); _style_input(nueva_loc_prov)

    def refresh_loc_provs():
        if not db: return
        provs = db.list_provincias()
        nueva_loc_prov.options = [ft.dropdown.Option(str(p["id"]), p["nombre"]) for p in provs]

    def agregar_localidad(_: Any = None):
        nom = (nueva_loc_nombre.value or "").strip()
        pid = nueva_loc_prov.value
        if not nom or not pid:
            show_toast("Completa nombre y provincia", kind="error")
            return
        try:
            db.create_localidad(nom, int(pid))
            nueva_loc_nombre.value = ""
            localidades_table.refresh()
            show_toast("Localidad agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Units
    unidades_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Unidad", editable=True, width=200),
            ColumnConfig(key="abreviatura", label="Abr.", editable=True, width=150),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar esta unidad?", "Eliminar", lambda: delete_unidad(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_unidades_medida, db.count_unidades_medida),
        inline_edit_callback=lambda rid, changes: db.update_unidad_medida_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_mass_actions=False, show_selection=False, auto_load=False, page_size=12,
    )
    nueva_uni_nombre = ft.TextField(label="Nombre Unidad", width=180)
    nueva_uni_abr = ft.TextField(label="Abreviatura", width=150)
    _style_input(nueva_uni_nombre); _style_input(nueva_uni_abr)

    def agregar_unidad(_: Any = None):
        nom = (nueva_uni_nombre.value or "").strip()
        abr = (nueva_uni_abr.value or "").strip()
        if not nom or not abr: return
        try:
            db.create_unidad_medida(nom, abr)
            nueva_uni_nombre.value = ""; nueva_uni_abr.value = ""
            unidades_table.refresh(); show_toast("Unidad agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # IVA Conditions
    civa_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Condición IVA", editable=True, width=320),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar este registro?", "Eliminar", lambda: delete_civa(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_condiciones_iva, db.count_condiciones_iva),
        inline_edit_callback=lambda rid, changes: db.update_condicion_iva_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    nueva_civa = ft.TextField(label="Nueva Condición", width=220); _style_input(nueva_civa)

    def agregar_civa(_: Any = None):
        nom = (nueva_civa.value or "").strip()
        if not nom: return
        try:
            db.create_condicion_iva(nom)
            nueva_civa.value = ""; civa_table.refresh(); show_toast("Condición agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # IVA Types
    tiva_table = GenericTable(
        columns=[
            ColumnConfig(key="porcentaje", label="%", editable=True, width=80),
            ColumnConfig(key="descripcion", label="Descripción", editable=True, width=200),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar este tipo de IVA?", "Eliminar", lambda: delete_tiva(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_tipos_iva, db.count_tipos_iva),
        inline_edit_callback=lambda rid, changes: db.update_tipo_iva_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    nueva_tiva_porc = ft.TextField(label="%", width=80)
    nueva_tiva_desc = ft.TextField(label="Desc.", width=180)
    _style_input(nueva_tiva_porc); _style_input(nueva_tiva_desc)

    def agregar_tiva(_: Any = None):
        try:
            # Auto-generate code
            existing = db.fetch_tipos_iva()
            max_code = max([int(t["codigo"]) for t in existing if str(t["codigo"]).isdigit()], default=0)
            new_code = max_code + 1
            
            db.create_tipo_iva(new_code, float(nueva_tiva_porc.value), nueva_tiva_desc.value)
            nueva_tiva_porc.value = ""; nueva_tiva_desc.value = ""
            tiva_table.refresh(); show_toast("Tipo IVA agregado", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Deposits
    depo_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Depósito", editable=True, width=200),
            ColumnConfig(key="ubicacion", label="Ubicación", editable=True, width=200),
            ColumnConfig(key="activo", label="Activo", editable=True, width=100, formatter=_format_bool, inline_editor=boolean_editor()),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar este rubro?", "Eliminar", lambda: delete_deposito(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_depositos, db.count_depositos),
        inline_edit_callback=lambda rid, changes: db.update_deposito_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    nuevo_depo_nom = ft.TextField(label="Nombre", width=180); nuevo_depo_ubi = ft.TextField(label="Ubicación", width=180)
    _style_input(nuevo_depo_nom); _style_input(nuevo_depo_ubi)

    def agregar_deposito(_: Any = None):
        nom = (nuevo_depo_nom.value or "").strip()
        if not nom: return
        try:
            db.create_deposito(nom, nuevo_depo_ubi.value or "")
            nuevo_depo_nom.value = ""; nuevo_depo_ubi.value = ""
            depo_table.refresh(); show_toast("Depósito agregado", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Payment Methods
    fpay_table = GenericTable(
        columns=[
            ColumnConfig(key="descripcion", label="Forma de Pago", editable=True, width=320),
            ColumnConfig(key="activa", label="Activa", editable=True, width=100, formatter=_format_bool, inline_editor=boolean_editor()),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar esta forma de pago?", "Eliminar", lambda: delete_fpay(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_formas_pago, db.count_formas_pago),
        inline_edit_callback=lambda rid, changes: db.update_forma_pago_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    nueva_fpay = ft.TextField(label="Nueva Forma", width=220); _style_input(nueva_fpay)

    def agregar_fpay(_: Any = None):
        nom = (nueva_fpay.value or "").strip()
        if not nom: return
        try:
            db.create_forma_pago(nom)
            nueva_fpay.value = ""; fpay_table.refresh(); show_toast("Forma agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    def toggle_lista_precio(lid: int, is_active: bool) -> None:
        if not db: return
        # If currently active, we want to deactivate (rojo)
        # If currently inactive, we want to activate (verde)
        if is_active:
            ask_confirm("Desactivar", "¿Desactivar esta lista de precios?", "Desactivar", 
                       lambda: (db.update_lista_precio_fields(int(lid), {"activa": False}), 
                               precios_table.refresh(), 
                               show_toast("Lista desactivada", kind="success")))
        else:
            ask_confirm("Activar", "¿Activar esta lista de precios?", "Activar", 
                       lambda: (db.update_lista_precio_fields(int(lid), {"activa": True}), 
                               precios_table.refresh(), 
                               show_toast("Lista activada", kind="success")), 
                       button_color=COLOR_SUCCESS)

    # Price Lists (Separate module-like view)
    precios_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Lista", editable=True, width=200),
            ColumnConfig(key="orden", label="Orden", editable=True, width=80),
            ColumnConfig(key="activa", label="Activa", editable=False, width=100, formatter=_format_bool),
            ColumnConfig(
                key="_toggle", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.UNPUBLISHED_OUTLINED if row.get("activa") else ft.icons.CHECK_CIRCLE_OUTLINE,
                    tooltip="Desactivar" if row.get("activa") else "Activar",
                    icon_color="#DC2626" if row.get("activa") else "#10B981", # Rojo si esta activa (para desactivar), Verde si inactiva (para activar)
                    on_click=lambda e: toggle_lista_precio(int(row["id"]), row.get("activa", False))
                )
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_listas_precio, db.count_listas_precio),
        inline_edit_callback=lambda rid, changes: db.update_lista_precio_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=20, show_export_scope=False,
    )
    nueva_lp_nom = ft.TextField(label="Nombre Lista", width=220); _style_input(nueva_lp_nom)
    def _next_lp_order_value() -> str:
        if not db:
            return "1"
        try:
            return str(db.get_next_lista_precio_orden())
        except Exception:
            return "1"

    nueva_lp_orden = ft.TextField(
        label="Orden",
        width=80,
        value=_next_lp_order_value(),
        input_filter=ft.InputFilter(allow=True, regex_string=r"[0-9]"),
    )
    _style_input(nueva_lp_orden)

    def agregar_lp(_: Any = None):
        nom = (nueva_lp_nom.value or "").strip()
        orden_val = (nueva_lp_orden.value or "0").strip()
        if not nom: return
        try:
            db.create_lista_precio(nom, orden=int(orden_val))
            nueva_lp_nom.value = ""; nueva_lp_orden.value = _next_lp_order_value()
            precios_table.refresh(); show_toast("Lista agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Percentage Types
    ptype_table = GenericTable(
        columns=[
            ColumnConfig(key="tipo", label="Tipo", editable=True, width=320),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar este tipo de porcentaje?", "Eliminar", lambda: delete_ptype(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_tipos_porcentaje, db.count_tipos_porcentaje),
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    # New Percentage Type Input
    nuevo_ptype = ft.TextField(label="Tipo", width=220); _style_input(nuevo_ptype)
    
    def agregar_ptype(_: Any = None):
        val = (nuevo_ptype.value or "").strip()
        if not val: return
        try:
            db.create_tipo_porcentaje(val)
            nuevo_ptype.value = ""; ptype_table.refresh(); show_toast("Tipo Porcentaje agregado", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Doc Types
    dtype_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Nombre", editable=True, width=160),
            ColumnConfig(key="clase", label="Clase", editable=True, width=100, inline_editor=dropdown_editor(lambda: ["VENTA", "COMPRA"], width=120, empty_label="Seleccionar clase... *")),
            ColumnConfig(key="letra", label="Letra", editable=True, width=60),
            ColumnConfig(key="afecta_stock", label="Stk", editable=True, width=60, formatter=_format_bool, inline_editor=boolean_editor()),
            ColumnConfig(key="afecta_cuenta_corriente", label="Cta", editable=True, width=60, formatter=_format_bool, inline_editor=boolean_editor()),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar este tipo de doc?", "Eliminar", lambda: delete_dtype(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_tipos_documento, db.count_tipos_documento),
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    # New Doc Type Inputs
    nuevo_dtype_nom = ft.TextField(label="Nombre", width=180); _style_input(nuevo_dtype_nom)
    nuevo_dtype_clase = ft.Dropdown(
        label="Clase", width=120, 
        options=[ft.dropdown.Option("VENTA"), ft.dropdown.Option("COMPRA")],
        text_style=ft.TextStyle(size=14)
    ); _style_input(nuevo_dtype_clase)
    nuevo_dtype_letra = ft.TextField(label="Letra", width=60); _style_input(nuevo_dtype_letra)
    nuevo_dtype_stock = ft.Switch(label="Stk", value=False)
    nuevo_dtype_cta = ft.Switch(label="Cta", value=False)
    
    def agregar_dtype(_: Any = None):
        nom = (nuevo_dtype_nom.value or "").strip()
        clase = nuevo_dtype_clase.value
        letra = (nuevo_dtype_letra.value or "").strip()
        if not nom or not clase: 
            show_toast("Faltan campos obligatorios", kind="warning")
            return
        try:
            db.create_tipo_documento(nom, clase, letra, nuevo_dtype_stock.value, nuevo_dtype_cta.value)
            nuevo_dtype_nom.value = ""; nuevo_dtype_letra.value = ""
            dtype_table.refresh(); show_toast("Tipo Documento agregado", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Movement Types
    mtype_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Nombre", editable=True, width=240),
            ColumnConfig(key="signo_stock", label="Signo", editable=True, width=80, inline_editor=dropdown_editor(lambda: ["1", "-1"], width=100, empty_label="Seleccionar signo... *")),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar este tipo de mov?", "Eliminar", lambda: delete_mtype(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_tipos_movimiento_articulo, db.count_tipos_movimiento_articulo),
        show_inline_controls=True, show_selection=False, show_mass_actions=False, auto_load=False, page_size=12,
    )
    # New Movement Type Inputs
    nuevo_mtype_nom = ft.TextField(label="Nombre", width=180); _style_input(nuevo_mtype_nom)
    nuevo_mtype_signo = ft.Dropdown(
        label="Signo", width=140,
        options=[ft.dropdown.Option("1", "Suma (+1)"), ft.dropdown.Option("-1", "Resta (-1)")],
        text_style=ft.TextStyle(size=14)
    ); _style_input(nuevo_mtype_signo)
    
    def agregar_mtype(_: Any = None):
        nom = (nuevo_mtype_nom.value or "").strip()
        signo = nuevo_mtype_signo.value
        if not nom or not signo: return
        try:
            db.create_tipo_movimiento_articulo(nom, int(signo))
            nuevo_mtype_nom.value = ""
            mtype_table.refresh(); show_toast("Tipo Movimiento agregado", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Users View
    usuarios_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Usuario", editable=True, width=200),
            ColumnConfig(key="email", label="Email", editable=True, width=200),
            ColumnConfig(key="rol", label="Rol", editable=False, width=100),
            ColumnConfig(key="activo", label="Estado", width=110, renderer=lambda row: _bool_pill(row.get("activo"))),
            ColumnConfig(key="ultimo_login", label="Últ. Acceso", width=160, formatter=_format_datetime),
            ColumnConfig(
                key="_toggle", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.PERSON_OFF_ROUNDED if row.get("activo") else ft.icons.PERSON_ADD_ROUNDED,
                    tooltip="Desactivar Usuario" if row.get("activo") else "Reactivar Usuario",
                    icon_color=("#94A3B8" if (db.current_user_id and int(row.get("id")) == int(db.current_user_id)) else ("#DC2626" if row.get("activo") else "#10B981")),
                    disabled=True if (db.current_user_id and int(row.get("id")) == int(db.current_user_id)) else False,
                    on_click=lambda e, rid=row.get("id"), is_active=row.get("activo"): toggle_usuario(int(rid), is_active) if rid else None
                )
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_users, db.count_users),
        inline_edit_callback=lambda rid, changes: db.update_user_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_mass_actions=False, auto_load=False, page_size=20, show_export_scope=False,
    )

    # User creation fields
    nuevo_user_nombre = ft.TextField(label="Nombre *", width=380); _style_input(nuevo_user_nombre)
    nuevo_user_email = ft.TextField(label="Email *", width=380); _style_input(nuevo_user_email)
    nuevo_user_password = ft.TextField(label="Contraseña *", password=True, can_reveal_password=True, width=380); _style_input(nuevo_user_password)
    nuevo_user_rol = ft.Dropdown(label="Rol *", width=380, options=[], text_style=ft.TextStyle(size=14)); _style_input(nuevo_user_rol)

    def refresh_user_roles():
        if not db: return
        try:
            roles = db.fetch_roles()
            nuevo_user_rol.options = [ft.dropdown.Option(str(r["id"]), r["nombre"]) for r in roles]
            if roles and not nuevo_user_rol.value:
                nuevo_user_rol.value = str(roles[0]["id"])
        except Exception:
            pass

    def open_nuevo_usuario(_: Any = None):
        refresh_user_roles()
        nuevo_user_nombre.value = ""
        nuevo_user_email.value = ""
        nuevo_user_password.value = ""
        
        def crear_usuario(_: Any = None):
            nombre = (nuevo_user_nombre.value or "").strip()
            email = (nuevo_user_email.value or "").strip()
            password = nuevo_user_password.value or ""
            id_rol = nuevo_user_rol.value
            
            if not nombre or not email or not password or not id_rol:
                show_toast("Por favor complete todos los campos", kind="warning")
                return
            
            if len(password) < 6:
                show_toast("La contraseña debe tener al menos 6 caracteres", kind="warning")
                return
            
            try:
                db.create_user(nombre, email, password, int(id_rol))
                show_toast(f"Usuario '{nombre}' creado exitosamente", kind="success")
                close_form()
                usuarios_table.refresh()
            except Exception as e:
                show_toast(f"Error al crear usuario: {e}", kind="error")
        
        content = ft.Column([
            nuevo_user_nombre,
            ft.Container(height=15),
            nuevo_user_email,
            ft.Container(height=15),
            nuevo_user_password,
            ft.Container(height=15),
            nuevo_user_rol,
        ], spacing=0, width=400)
        
        open_form("Nuevo Usuario", content, [
            _cancel_button("Cancelar", on_click=close_form),
            ft.ElevatedButton("Crear Usuario", bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)), on_click=crear_usuario)
        ])

    
    usuarios_tabs = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        expand=True,
        tabs=[
            make_tab(
                text="Lista de Usuarios",
                icon=ft.icons.PEOPLE_OUTLINE_ROUNDED,
                content=ft.Container(
                    padding=10,
                    content=make_card(
                        "Usuarios del Sistema", 
                        "Gestión de acceso, roles y permisos.", 
                        usuarios_table.build(),
                        actions=[
                            ft.ElevatedButton(
                                "Nuevo Usuario", 
                                icon=ft.icons.PERSON_ADD_ROUNDED, 
                                bgcolor=COLOR_ACCENT, 
                                color="#FFFFFF",
                                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                                on_click=open_nuevo_usuario
                            )
                        ]
                    )
                )
            ),
        ],
    )

    usuarios_view = ft.Column([
        ft.Row([
            make_stat_card("Último Acceso", "N/A", "SHIELD_ROUNDED", COLOR_SUCCESS, key="usuarios_ultimo"),
            make_stat_card("Estado Servidor", "ONLINE", "SECURITY_ROUNDED", COLOR_WARNING),
        ], spacing=20),
        ft.Container(height=10),
        usuarios_tabs
    ], expand=True, spacing=10)

    # Backup Config View (Replaced with Advanced BackupView)
    def set_conn_adapter(connected: bool, msg: str):
        # Adapter to match BackupView signature with ui_basic's global global vars approach is tricky.
        # ui_basic uses a global status_badge. We can just update it here if we want, or pass a dummy.
        # For now, let's just piggyback on existing UI update if possible, or ignore.
        # Actually, let's reuse the logic that exists if we can, or just print log.
        pass

    backup_view_component = BackupProfessionalView(
        page,
        db,
        show_toast,
        ask_confirm,
        pg_bin_path=config.pg_bin_path,
    )
    
    # Wrap in a container to match layout expectations
    backups_view = ft.Container(
        content=backup_view_component.build(),
        padding=10
    )

    # Ensure initial load when starting or switching
    backup_view_component.load_data()

    # Documents View
    def _can_authorize_afip(doc_row: Dict[str, Any]) -> bool:
        estado = str(doc_row.get("estado") or "").upper()
        return estado in (DocumentoEstado.CONFIRMADO.value, DocumentoEstado.PAGADO.value) and doc_row.get("codigo_afip") and not doc_row.get("cae")

    def _build_doc_fiscal_pricing_for_afip(db_local: Database, doc_full: Dict[str, Any]) -> Dict[str, Any]:
        items_src = doc_full.get("items") or []
        if not items_src:
            raise ValueError("El comprobante no tiene líneas para autorizar en AFIP.")

        article_iva_cache: Dict[int, Any] = {}
        calc_items: List[Dict[str, Any]] = []
        for item in items_src:
            fiscal_iva = max(to_decimal("0"), to_decimal(item.get("porcentaje_iva"), to_decimal("0")))
            art_id = item.get("id_articulo")
            try:
                art_id_int = int(art_id) if art_id is not None else None
            except Exception:
                art_id_int = None
            if fiscal_iva <= to_decimal("0") and art_id_int is not None:
                if art_id_int not in article_iva_cache:
                    art = db_local.get_article_simple(art_id_int)
                    article_iva_cache[art_id_int] = max(
                        to_decimal("0"),
                        to_decimal((art or {}).get("porcentaje_iva"), to_decimal("0")),
                    )
                fiscal_iva = article_iva_cache.get(art_id_int, to_decimal("0"))

            desc_pct = float(item.get("descuento_porcentaje") or 0)
            desc_imp = float(item.get("descuento_importe") or 0)
            discount_mode = "amount" if desc_imp > 0 and desc_pct <= 0 else "percentage"
            calc_items.append(
                {
                    "id_articulo": art_id_int,
                    "cantidad": float(item.get("cantidad") or 0),
                    "precio_unitario": float(item.get("precio_unitario") or 0),
                    "porcentaje_iva": 0.0,  # IVA visible del modo "incluido"
                    "porcentaje_iva_fiscal": float(fiscal_iva),
                    "descuento_porcentaje": desc_pct,
                    "descuento_importe": desc_imp,
                    "descuento_mode": discount_mode,
                }
            )

        global_desc_pct = float(doc_full.get("descuento_porcentaje") or 0)
        global_desc_imp = float(doc_full.get("descuento_importe") or 0)
        global_discount_mode = "amount" if global_desc_imp > 0 and global_desc_pct <= 0 else "percentage"
        return calculate_document_totals(
            items=calc_items,
            descuento_global_porcentaje=global_desc_pct,
            descuento_global_importe=global_desc_imp,
            descuento_global_mode=global_discount_mode,
            sena=float(doc_full.get("sena") or 0),
            pricing_mode="tax_included",
        )

    def _build_afip_iva_payload(
        db_local: Database,
        iva_breakdown: List[Dict[str, Any]],
        imp_neto: Any,
        imp_iva: Any,
    ) -> List[Dict[str, Any]]:
        tipos_iva = db_local.fetch_tipos_iva(limit=250)
        tasa_to_codigo: Dict[str, int] = {}
        for tipo in tipos_iva:
            try:
                codigo_afip = int(tipo.get("codigo"))
            except Exception:
                continue
            if codigo_afip <= 0:
                continue
            tasa_key = str(quantize_2(to_decimal(tipo.get("porcentaje"), to_decimal("0"))))
            tasa_to_codigo[tasa_key] = codigo_afip

        payload: List[Dict[str, Any]] = []
        for row in iva_breakdown or []:
            tasa = quantize_2(to_decimal(row.get("porcentaje_iva"), to_decimal("0")))
            if tasa <= to_decimal("0"):
                continue
            codigo = tasa_to_codigo.get(str(tasa))
            if codigo is None:
                tasa_text = normalize_input_value(tasa, decimals=2, use_grouping=False) or "0,00"
                raise ValueError(f"No se encontró código AFIP para la alícuota IVA {tasa_text}%.")

            base_imp = quantize_2(to_decimal(row.get("base_imponible"), to_decimal("0")))
            importe = quantize_2(to_decimal(row.get("importe"), to_decimal("0")))
            if base_imp == to_decimal("0") and importe == to_decimal("0"):
                continue
            payload.append(
                {
                    "Id": codigo,
                    "BaseImp": float(base_imp),
                    "Importe": float(importe),
                }
            )

        imp_neto_dec = quantize_2(to_decimal(imp_neto))
        imp_iva_dec = quantize_2(to_decimal(imp_iva))
        if imp_iva_dec > to_decimal("0") and not payload:
            raise ValueError("No se pudo armar el desglose de IVA para AFIP.")

        if payload:
            sum_base = quantize_2(sum((to_decimal(p["BaseImp"]) for p in payload), to_decimal("0")))
            sum_iva = quantize_2(sum((to_decimal(p["Importe"]) for p in payload), to_decimal("0")))
            diff_base = quantize_2(imp_neto_dec - sum_base)
            diff_iva = quantize_2(imp_iva_dec - sum_iva)
            if diff_base != to_decimal("0") or diff_iva != to_decimal("0"):
                payload[-1]["BaseImp"] = float(quantize_2(to_decimal(payload[-1]["BaseImp"]) + diff_base))
                payload[-1]["Importe"] = float(quantize_2(to_decimal(payload[-1]["Importe"]) + diff_iva))

        return payload

    def _authorize_afip_doc(
        doc_row: Dict[str, Any],
        *,
        close_after: bool = False,
        on_success: Optional[Callable[[], None]] = None,
    ) -> None:
        if not _can_authorize_afip(doc_row):
            show_toast("El comprobante no está listo para autorizar.", kind="error")
            return
        if not afip:
            show_toast("Servicio AFIP no configurado. Verifique CUIT y certificados en .env", kind="error")
            return
        db_local = get_db_or_toast()
        if not db_local:
            return

        try:
            show_toast("Solicitando CAE...", kind="info")
            doc_id = int(doc_row["id"])
            codigo_afip = int(doc_row.get("codigo_afip"))
            punto_venta = int(getattr(config, "afip_punto_venta", 1) or 1)
            last = afip.get_last_voucher_number(punto_venta, codigo_afip)
            next_num = last + 1

            doc_full = db_local.get_document_full(doc_id)
            if not doc_full:
                show_toast("No se pudo cargar el comprobante completo para AFIP.", kind="error")
                return
            pricing = _build_doc_fiscal_pricing_for_afip(db_local, doc_full)
            total = float(quantize_2(to_decimal(pricing.get("total"), to_decimal("0"))))
            neto = float(quantize_2(to_decimal(pricing.get("neto"), to_decimal("0"))))
            iva_total = float(quantize_2(to_decimal(pricing.get("iva_total"), to_decimal("0"))))
            iva_payload = _build_afip_iva_payload(
                db_local,
                pricing.get("iva_breakdown") or [],
                pricing.get("neto"),
                pricing.get("iva_total"),
            )

            entity = None
            ent_id = doc_row.get("id_entidad") or doc_full.get("id_entidad_comercial")
            if ent_id:
                entity = db_local.fetch_entity_by_id(int(ent_id))

            letra = str(doc_row.get("letra") or "").strip().upper()
            es_letra_a = letra == "A" or codigo_afip in (1, 2, 3)
            cuit_raw = (doc_row.get("cuit_receptor") or (entity or {}).get("cuit") or "").strip()
            digits = "".join(ch for ch in cuit_raw if ch.isdigit())
            doc_tipo = 99
            doc_nro = 0
            if digits:
                if len(digits) == 11:
                    doc_tipo = 80
                    doc_nro = int(digits)
                elif len(digits) <= 8:
                    doc_tipo = 96
                    doc_nro = int(digits)
                else:
                    show_toast("CUIT/DNI del receptor inválido.", kind="error")
                    return

            if es_letra_a and doc_tipo != 80:
                show_toast("Para comprobantes letra A se requiere CUIT válido del receptor.", kind="error")
                return

            condicion_nombre = (entity or {}).get("condicion_iva")
            condicion_id = afip.get_condicion_iva_receptor_id(condicion_nombre) if condicion_nombre else None
            if es_letra_a and not condicion_id:
                show_toast("Falta la condición IVA del receptor (requerida para letra A).", kind="error")
                return

            invoice_data = {
                "CantReg": 1,
                "PtoVta": punto_venta,
                "CbteTipo": codigo_afip,
                "Concepto": 1,
                "DocTipo": doc_tipo,
                "DocNro": doc_nro,
                "CbteDesde": next_num,
                "CbteHasta": next_num,
                "CbteFch": datetime.now().strftime("%Y%m%d"),
                "ImpTotal": total,
                "ImpTotConc": 0,
                "ImpNeto": neto,
                "ImpOpEx": 0,
                "ImpIVA": iva_total,
                "ImpTrib": 0,
                "MonId": "PES",
                "MonCotiz": 1,
            }
            if iva_payload:
                invoice_data["Iva"] = iva_payload
            if condicion_id is not None:
                invoice_data["CondicionIVAReceptorId"] = condicion_id

            res = afip.authorize_invoice(invoice_data)
            if res.get("success"):
                cuit_emisor = "".join(ch for ch in str(getattr(afip, "cuit", "") or config.afip_cuit or "").strip() if ch.isdigit())
                cae = res.get("CAE") or res.get("cae")
                qr_data = None
                try:
                    if not cae:
                        raise ValueError("CAE ausente para generar QR.")
                    fecha_doc = str(doc_full.get("fecha") or doc_row.get("fecha") or datetime.now().strftime("%Y-%m-%d"))[:10]
                    qr_payload = {
                        "ver": 1,
                        "fecha": fecha_doc,
                        "cuit": int(cuit_emisor) if cuit_emisor else 0,
                        "ptoVta": int(punto_venta),
                        "tipoCmp": int(codigo_afip),
                        "nroCmp": int(next_num),
                        "importe": float(quantize_2(to_decimal(total))),
                        "moneda": "PES",
                        "ctz": 1,
                        "tipoDocRec": int(doc_tipo),
                        "nroDocRec": int(doc_nro),
                        "tipoCodAut": "E",
                        "codAut": cae,
                    }
                    qr_json = json.dumps(qr_payload, separators=(",", ":"), ensure_ascii=False)
                    qr_base64 = base64.b64encode(qr_json.encode("utf-8")).decode("ascii")
                    qr_param = quote(qr_base64, safe="")
                    qr_data = f"https://www.afip.gob.ar/fe/qr/?p={qr_param}"
                except Exception:
                    qr_data = None
                    show_toast("No se pudo generar el QR fiscal del comprobante.", kind="warning")

                db_local.update_document_afip_data(
                    doc_id,
                    res["CAE"],
                    res["CAEFchVto"],
                    punto_venta,
                    codigo_afip,
                    cuit_emisor=cuit_emisor or None,
                    qr_data=qr_data,
                )
                show_toast("Facturado exitosamente", kind="success")
                if callable(on_success):
                    on_success()
                if close_after:
                    close_form()
                if hasattr(documentos_summary_table, "refresh"):
                    documentos_summary_table.refresh()
                refresh_all_stats()
            else:
                show_toast(f"Error AFIP: {res.get('error')}", kind="error")
        except Exception as e:
            show_toast(f"Error: {e}", kind="error")

    _authorize_afip_doc_core = _authorize_afip_doc

    def _confirm_afip_authorization(
        doc_row: Dict[str, Any],
        *,
        close_after: bool = False,
        on_success: Optional[Callable[[], None]] = None,
    ) -> None:
        ask_confirm(
            "Autorizar AFIP",
            "Vas a facturar electrónicamente este comprobante en AFIP. Esta acción es irreversible y no se puede volver atrás. ¿Deseás continuar?",
            "Autorizar AFIP",
            lambda: _authorize_afip_doc(doc_row, close_after=close_after, on_success=on_success),
            button_color=COLOR_WARNING,
        )

    def _confirm_document(
        doc_id: int,
        *,
        close_after: bool = False,
        on_success: Optional[Callable[[], None]] = None,
    ) -> None:
        def on_confirm_real():
            try:
                if not db:
                    return
                db.confirm_document(doc_id)
                if db:
                    db.log_activity("DOCUMENTO", "CONFIRM", id_entidad=doc_id)
                show_toast("Comprobante confirmado", kind="success")
                if callable(on_success):
                    on_success()
                if close_after:
                    close_form()
                if hasattr(documentos_summary_table, "refresh"):
                    documentos_summary_table.refresh()
                refresh_all_stats()
            except Exception as exc:
                show_toast(f"Error al confirmar: {exc}", kind="error")

        ask_confirm(
            "Confirmar Comprobante",
            "¿Está seguro que desea confirmar este comprobante? Esto generará movimientos de stock y afectará la cuenta corriente.",
            "Confirmar [Enter/F10]",
            on_confirm_real,
            button_color=COLOR_SUCCESS,
        )

    def view_doc_detail(doc_row: Dict[str, Any]):
        doc_id = int(doc_row["id"])
        estado = doc_row.get("estado", "BORRADOR")
        if db:
            db.log_activity("DOCUMENTO", "VIEW_DETAIL", id_entidad=doc_id)
        # Adapter to match BackupView signature with ui_basic's global global vars approach is tricky.
        # ui_basic uses a global status_badge. We can just update it here if we want, or pass a dummy.
        # For now, let's just piggyback on existing UI update if possible, or ignore.
        # Actually, let's reuse the logic that exists if we can, or just print log.
        pass

    backup_view_component = BackupProfessionalView(
        page,
        db,
        show_toast,
        ask_confirm,
        pg_bin_path=config.pg_bin_path,
    )
    
    # Wrap in a container to match layout expectations
    backups_view = ft.Container(
        content=backup_view_component.build(),
        padding=10
    )

    # Ensure initial load when starting or switching
    backup_view_component.load_data()

    # Documents View
    def _can_authorize_afip(doc_row: Dict[str, Any]) -> bool:
        estado = str(doc_row.get("estado") or "").upper()
        return estado in (DocumentoEstado.CONFIRMADO.value, DocumentoEstado.PAGADO.value) and doc_row.get("codigo_afip") and not doc_row.get("cae")

    def _authorize_afip_doc(
        doc_row: Dict[str, Any],
        *,
        close_after: bool = False,
        on_success: Optional[Callable[[], None]] = None,
    ) -> None:
        _authorize_afip_doc_core(doc_row, close_after=close_after, on_success=on_success)

    def _confirm_afip_authorization(
        doc_row: Dict[str, Any],
        *,
        close_after: bool = False,
        on_success: Optional[Callable[[], None]] = None,
    ) -> None:
        ask_confirm(
            "Autorizar AFIP",
            "Vas a facturar electrónicamente este comprobante en AFIP. Esta acción es irreversible y no se puede volver atrás. ¿Deseás continuar?",
            "Autorizar AFIP",
            lambda: _authorize_afip_doc(doc_row, close_after=close_after, on_success=on_success),
            button_color=COLOR_WARNING,
        )

    def _confirm_document(
        doc_id: int,
        *,
        close_after: bool = False,
        on_success: Optional[Callable[[], None]] = None,
    ) -> None:
        def on_confirm_real():
            try:
                if not db:
                    return
                db.confirm_document(doc_id)
                if db:
                    db.log_activity("DOCUMENTO", "CONFIRM", id_entidad=doc_id)
                show_toast("Comprobante confirmado", kind="success")
                if callable(on_success):
                    on_success()
                if close_after:
                    close_form()
                if hasattr(documentos_summary_table, "refresh"):
                    documentos_summary_table.refresh()
                refresh_all_stats()
            except Exception as exc:
                show_toast(f"Error al confirmar: {exc}", kind="error")

        ask_confirm(
            "Confirmar Comprobante",
            "¿Está seguro que desea confirmar este comprobante? Esto generará movimientos de stock y afectará la cuenta corriente.",
            "Confirmar [Enter/F10]",
            on_confirm_real,
            button_color=COLOR_SUCCESS,
        )

    def view_doc_detail(doc_row: Dict[str, Any]):
        doc_id = int(doc_row["id"])
        estado = doc_row.get("estado", "BORRADOR")
        if db:
            db.log_activity("DOCUMENTO", "VIEW_DETAIL", id_entidad=doc_id)
        try:
            details = db.fetch_documento_detalle(doc_id)
            # Improved content with more details
            total_doc = float(doc_row.get("total", 0))
            line_discount_total = sum(float(d.get("descuento_importe", 0) or 0) for d in details)
            global_discount_total = float(doc_row.get("descuento_importe", 0) or 0)
            global_discount_pct = float(doc_row.get("descuento_porcentaje", 0) or 0)
            
            col_widths = {
                "articulo": 300,
                "lista": 120,
                "cant": 70,
                "unitario": 120,
                "desc_pct": 90,
                "desc_imp": 120,
                "total": 130,
            }
            table_min_width = sum(col_widths.values()) + 220

            body = ft.Column([
                    ft.Container(
                        content=ft.Row([
                            ft.Column([
                                ft.Text("CLIENTE / PROVEEDOR", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                                ft.Text(doc_row.get("entidad", "—"), size=16, weight=ft.FontWeight.W_600),
                            ], spacing=2, expand=True),
                            ft.Container(
                                content=ft.Column([
                                    ft.Text("FORMA DE PAGO", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED, text_align=ft.TextAlign.CENTER),
                                    ft.Text(doc_row.get("forma_pago", "No especificada"), size=14, weight=ft.FontWeight.W_500, text_align=ft.TextAlign.CENTER),
                                ], spacing=2, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                                padding=ft.padding.symmetric(horizontal=20),
                            ),
                            ft.Column([
                                ft.Text("FECHA", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED, text_align=ft.TextAlign.RIGHT),
                                ft.Text(_format_datetime(doc_row.get("fecha")), size=14, text_align=ft.TextAlign.RIGHT),
                            ], spacing=2, width=100),
                        ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                        padding=ft.padding.only(bottom=15),
                        border=ft.border.only(bottom=ft.BorderSide(1, "#E2E8F0"))
                    ),
                    ft.Container(height=10),
                    ft.Row([
                        ft.Text("ÍTEMS DEL COMPROBANTE *", size=11, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT),
                        ft.Container(
                            content=ft.Text(
                                doc_row.get("estado", ""), 
                                size=10, 
                                weight=ft.FontWeight.BOLD, 
                                color="#FFFFFF"
                            ),
                            bgcolor=COLOR_SUCCESS if doc_row.get("estado") == DocumentoEstado.PAGADO.value else (COLOR_ERROR if doc_row.get("estado") == DocumentoEstado.ANULADO.value else COLOR_INFO),
                            padding=ft.padding.symmetric(horizontal=10, vertical=4),
                            border_radius=20
                        )
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    ft.Container(
                        border=ft.border.all(1, "#E2E8F0"),
                        border_radius=8,
                        clip_behavior=ft.ClipBehavior.HARD_EDGE,
                        content=ft.Row(
                            scroll=ft.ScrollMode.AUTO,
                            expand=True,
                            controls=[
                                SafeDataTable(
                                    width=table_min_width,
                                    heading_row_color="#F8FAFC",
                                    heading_row_height=40,
                                    data_row_min_height=40,
                                    column_spacing=12,
                                    columns=[
                                        ft.DataColumn(ft.Text("Artículo", size=12, weight=ft.FontWeight.BOLD)),
                                        ft.DataColumn(ft.Text("Lista", size=12, weight=ft.FontWeight.BOLD)),
                                        ft.DataColumn(ft.Text("Cant.", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                        ft.DataColumn(ft.Text("Unitario", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                        ft.DataColumn(ft.Text("Desc. %", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                        ft.DataColumn(ft.Text("Desc. $", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                        ft.DataColumn(ft.Text("Total", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                    ],
                                    rows=[
                                        ft.DataRow(cells=[
                                            ft.DataCell(ft.Container(
                                                width=col_widths["articulo"],
                                                content=ft.Text(
                                                    f"{d['articulo']} ({d.get('codigo_art', d.get('id_articulo'))})",
                                                    size=13,
                                                    max_lines=1,
                                                    overflow=ft.TextOverflow.ELLIPSIS,
                                                ),
                                            )),
                                            ft.DataCell(ft.Container(
                                                width=col_widths["lista"],
                                                content=ft.Text(
                                                    d.get("lista_nombre") or (doc_row.get("lista_precio") if doc_row.get("id_lista_precio") == d.get("id_lista_precio") else "---"),
                                                    size=12,
                                                    color=COLOR_TEXT_MUTED,
                                                    max_lines=1,
                                                    overflow=ft.TextOverflow.ELLIPSIS,
                                                ),
                                            )),
                                            ft.DataCell(ft.Container(
                                                width=col_widths["cant"],
                                                content=ft.Text(_format_quantity(d["cantidad"]), size=13, text_align=ft.TextAlign.RIGHT),
                                            )),
                                            ft.DataCell(ft.Container(
                                                width=col_widths["unitario"],
                                                content=ft.Text(_format_money(d["precio_unitario"]), size=13, text_align=ft.TextAlign.RIGHT),
                                            )),
                                            ft.DataCell(ft.Container(
                                                width=col_widths["desc_pct"],
                                                content=ft.Text(
                                                    format_percent(d.get("descuento_porcentaje", 0) or 0, decimals=2),
                                                    size=13,
                                                    text_align=ft.TextAlign.RIGHT,
                                                    color=COLOR_ERROR,
                                                ),
                                            )),
                                            ft.DataCell(ft.Container(
                                                width=col_widths["desc_imp"],
                                                content=ft.Text(_format_money(d.get("descuento_importe", 0)), size=13, text_align=ft.TextAlign.RIGHT, color=COLOR_ERROR),
                                            )),
                                            ft.DataCell(ft.Container(
                                                width=col_widths["total"],
                                                content=ft.Text(_format_money(d["total_linea"]), size=13, weight=ft.FontWeight.W_500, text_align=ft.TextAlign.RIGHT),
                                            )),
                                        ]) for d in details
                                    ],
                                )
                            ],
                        ),
                    ),
                     ft.Container(height=10),
                     ft.Row([
                         ft.Container(
                             content=ft.Column([
                                 ft.Text("OBSERVACIONES:", size=11, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                                 ft.Text(doc_row.get("observacion") or "Sin observaciones", size=12, italic=True if not doc_row.get("observacion") else False),
                             ], spacing=2),
                             expand=True,
                             padding=ft.padding.only(right=20)
                         ) if doc_row.get("observacion") else ft.Container(expand=True),
                         ft.Container(
                             content=ft.Column([
                                ft.Row([
                                    ft.Text("SUBTOTAL BRUTO:", size=11, color=COLOR_TEXT_MUTED),
                                    ft.Text(_format_money(doc_row.get("subtotal", 0)), size=11),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250),
                                ft.Row([
                                    ft.Text("DESCUENTO LÍNEAS:", size=11, color=COLOR_ERROR, weight=ft.FontWeight.BOLD),
                                    ft.Text(f"- {_format_money(line_discount_total)}", size=11, color=COLOR_ERROR, weight=ft.FontWeight.BOLD),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250) if line_discount_total > 0 else ft.Container(),
                                ft.Row([
                                    ft.Text(
                                        f"DESCUENTO GLOBAL ({global_discount_pct:.2f}%):" if global_discount_pct > 0 else "DESCUENTO GLOBAL:",
                                        size=11,
                                        color=COLOR_ERROR,
                                        weight=ft.FontWeight.BOLD,
                                    ),
                                    ft.Text(f"- {_format_money(global_discount_total)}", size=11, color=COLOR_ERROR, weight=ft.FontWeight.BOLD),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250) if global_discount_total > 0 else ft.Container(),
                                ft.Row([
                                    ft.Text("NETO GRAVADO:", size=12, color=COLOR_TEXT_MUTED),
                                    ft.Text(_format_money(doc_row.get("neto", 0)), size=12),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250),
                                ft.Row([
                                    ft.Text("IVA TOTAL:", size=12, color=COLOR_TEXT_MUTED),
                                    ft.Text(_format_money(doc_row.get("iva_total", 0)), size=12),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250),
                                ft.Divider(height=1, color="#CBD5E1"),
                                ft.Row([
                                    ft.Text("TOTAL:", size=16, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT),
                                    ft.Text(_format_money(total_doc), size=18, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250),
                                ft.Row([
                                    ft.Text("SEÑA / A CUENTA:", size=12, color=COLOR_SUCCESS),
                                    ft.Text(_format_money(doc_row.get("sena", 0)), size=12, color=COLOR_SUCCESS),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250) if float(doc_row.get("sena", 0)) > 0 else ft.Container(),
                                ft.Row([
                                    ft.Text("SALDO PENDIENTE:", size=13, weight=ft.FontWeight.BOLD, color=COLOR_WARNING),
                                    ft.Text(_format_money(max(0, total_doc - float(doc_row.get("sena", 0)))), size=14, weight=ft.FontWeight.BOLD, color=COLOR_WARNING),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250) if float(doc_row.get("sena", 0)) > 0 else ft.Container(),
                            ], spacing=5, horizontal_alignment=ft.CrossAxisAlignment.END),
                            padding=15,
                            bgcolor="#F1F5F9",
                            border_radius=12,
                        )
                    ])
                ], spacing=5)

            content = ft.Container(
                content=body,
                padding=10,
                width=900,
            )
            
            actions = [_cancel_button("Cerrar", on_click=close_form)]
            if estado == DocumentoEstado.BORRADOR.value:
                actions.insert(0, ft.ElevatedButton(
                    "Confirmar Comprobante",
                    icon=ft.icons.CHECK_CIRCLE,
                    bgcolor=COLOR_SUCCESS,
                    color="#FFFFFF",
                    on_click=lambda _: _confirm_document(doc_id, close_after=True),
                ))
            
            # AFIP Authorization
            if _can_authorize_afip(doc_row):
                actions.insert(0, ft.ElevatedButton(
                    "Autorizar AFIP",
                    icon=ft.icons.SECURITY,
                    bgcolor=COLOR_ACCENT,
                    color="#FFFFFF",
                    on_click=lambda _: _confirm_afip_authorization(doc_row, close_after=True),
                ))

            cae = doc_row.get("cae")
            if cae:
                body.controls.append(ft.Container(
                    content=ft.Column([
                        ft.Text(f"CAE: {cae}", weight=ft.FontWeight.BOLD),
                        ft.Text(f"Vencimiento CAE: {doc_row.get('cae_vencimiento')}")
                    ]),
                    padding=10,
                    bgcolor=ft.Colors.GREY_100,
                    border_radius=5
                ))

            open_form(f"Detalle: {doc_row.get('tipo_documento','')} {doc_row.get('numero_serie','')}", content, actions)
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    def view_remito_detail(rem_row: Dict[str, Any]):
        if not rem_row:
            return
        remito_id = int(rem_row["id"])

        try:
            if db:
                db.log_activity("app.remito", "VIEW_DETAIL", id_entidad=remito_id)
            details = db.fetch_remito_detalle(remito_id) if db else []

            header = ft.Row([
                ft.Column([
                    ft.Text("CLIENTE", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                    ft.Text(rem_row.get("entidad") or "—", size=16, weight=ft.FontWeight.W_600),
                ], spacing=2, expand=True),
                ft.Column([
                    ft.Text("DEPÓSITO", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED, text_align=ft.TextAlign.RIGHT),
                    ft.Text(rem_row.get("deposito") or "—", size=13, text_align=ft.TextAlign.RIGHT),
                ], spacing=2, width=200),
                ft.Column([
                    ft.Text("DOCUMENTO", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED, text_align=ft.TextAlign.RIGHT),
                    ft.Text(rem_row.get("documento_numero") or "—", size=13, text_align=ft.TextAlign.RIGHT),
                ], spacing=2, width=200),
            ], spacing=20, alignment=ft.MainAxisAlignment.SPACE_BETWEEN)

            info_row = ft.Row([
                ft.Column([
                    ft.Text("NÚMERO REMITO", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                    ft.Text(rem_row.get("numero") or "—", size=16, weight=ft.FontWeight.W_700),
                ], spacing=2),
                ft.Column([
                    ft.Text("FECHA", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                    ft.Text(_format_datetime(rem_row.get("fecha")), size=13),
                ], spacing=2),
                ft.Column([
                    ft.Text("ESTADO", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                    _remito_status_pill(rem_row.get("estado")),
                ], spacing=2),
                ft.Column([
                    ft.Text("ENTREGA", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                    ft.Text(_format_datetime(rem_row.get("fecha_entrega")), size=13),
                ], spacing=2),
            ], spacing=20, alignment=ft.MainAxisAlignment.SPACE_BETWEEN)

            table_body: ft.Control
            if details:
                table_body = SafeDataTable(
                    heading_row_color="#F8FAFC",
                    heading_row_height=40,
                    data_row_min_height=40,
                    column_spacing=16,
                    columns=[
                        ft.DataColumn(ft.Text("Artículo", size=12, weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Cantidad", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                    ],
                    rows=[
                        ft.DataRow(cells=[
                            ft.DataCell(ft.Text(d.get("articulo") or "—", size=13)),
                            ft.DataCell(ft.Text(_format_quantity(d.get("cantidad") or 0), size=13)),
                        ]) for d in details
                    ],
                )
            else:
                table_body = ft.Container(
                    content=ft.Text("Este remito no tiene líneas registradas.", color=COLOR_TEXT_MUTED),
                    padding=ft.padding.symmetric(vertical=20, horizontal=10),
                )

            body = ft.Column([
                header,
                info_row,
                ft.Divider(color="#E2E8F0"),
                table_body,
                ft.Container(
                    content=ft.Column([
                        ft.Text("DIRECCIÓN DE ENTREGA", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                        ft.Text(rem_row.get("direccion_entrega") or "—", size=13),
                        ft.Container(height=6),
                        ft.Text("OBSERVACIONES", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MUTED),
                        ft.Text(rem_row.get("observacion") or "Sin observaciones", size=13, color=COLOR_TEXT_MUTED),
                    ], spacing=5),
                    padding=ft.padding.symmetric(vertical=12, horizontal=10),
                    bgcolor="#F8FAFC",
                    border_radius=12,
                ),
            ], spacing=15, scroll=ft.ScrollMode.AUTO, expand=True)

            content = ft.Container(
                content=body,
                padding=20,
                width=720,
                height=600,
            )
            def _print_remito_with_options(include_prices: bool) -> None:
                try:
                    doc_for_print: Dict[str, Any] = dict(rem_row or {})
                    entity_detail = db.get_entity_detail(rem_row.get("id_entidad_comercial")) if db else {}
                    items_data: List[Dict[str, Any]] = []
                    for idx, line in enumerate(details or []):
                        row = dict(line or {})
                        row["nro_linea"] = row.get("nro_linea") or (idx + 1)
                        row["articulo_nombre"] = (
                            row.get("articulo_nombre")
                            or row.get("descripcion_historica")
                            or row.get("articulo")
                            or f"Artículo {row.get('id_articulo', '-')}"
                        )
                        row["articulo_codigo"] = str(
                            row.get("articulo_codigo")
                            or row.get("id_articulo")
                            or "-"
                        ).strip() or "-"
                        items_data.append(row)

                    doc_lines: List[Dict[str, Any]] = []
                    doc_summary: Dict[str, Any] = {}
                    if db and rem_row.get("id_documento"):
                        try:
                            doc_id = int(rem_row.get("id_documento"))
                            raw_doc_lines = db.fetch_documento_detalle(doc_id)
                            doc_lines = raw_doc_lines if isinstance(raw_doc_lines, list) else []
                            raw_doc_summary = db.fetch_documento_resumen_by_id(doc_id)
                            doc_summary = raw_doc_summary if isinstance(raw_doc_summary, dict) else {}
                        except Exception as exc:
                            logger.warning(f"No se pudieron cargar datos del comprobante para remito: {exc}")

                    if doc_summary:
                        for key in ("neto", "total", "descuento_porcentaje", "descuento_importe"):
                            value = doc_summary.get(key)
                            if value is not None:
                                doc_for_print[key] = value
                        rem_obs = str(doc_for_print.get("observacion") or "").strip()
                        doc_obs = str(doc_summary.get("observacion") or "").strip()
                        if not rem_obs and doc_obs:
                            doc_for_print["observacion"] = doc_obs

                    if doc_lines:
                        doc_lines_by_nro: Dict[int, Dict[str, Any]] = {}
                        for doc_line in doc_lines:
                            try:
                                line_no = int((doc_line or {}).get("nro_linea"))
                            except (TypeError, ValueError):
                                continue
                            doc_lines_by_nro[line_no] = doc_line

                        for idx, row in enumerate(items_data):
                            try:
                                row_line_no = int(row.get("nro_linea"))
                            except (TypeError, ValueError):
                                row_line_no = None

                            source_line: Optional[Dict[str, Any]] = None
                            if row_line_no is not None:
                                source_line = doc_lines_by_nro.get(row_line_no)
                            if source_line is None and idx < len(doc_lines):
                                source_line = doc_lines[idx]
                            if not source_line:
                                continue

                            unit_price = source_line.get("precio_unitario")
                            if unit_price is not None:
                                row["precio_unitario"] = unit_price

                            line_total = source_line.get("total_linea")
                            if line_total is None and unit_price is not None:
                                try:
                                    qty_val = float(row.get("cantidad") or source_line.get("cantidad") or 0.0)
                                    line_total = qty_val * float(unit_price)
                                except (TypeError, ValueError):
                                    line_total = None
                            if line_total is not None:
                                row["total_linea"] = line_total

                            discount_amount = source_line.get("descuento_importe")
                            if discount_amount is not None:
                                row["descuento_importe"] = discount_amount
                            discount_pct = source_line.get("descuento_porcentaje")
                            if discount_pct is not None:
                                row["descuento_porcentaje"] = discount_pct
                            hist_unidades_bulto = source_line.get("unidades_por_bulto_historico")
                            if hist_unidades_bulto is not None:
                                row["unidades_por_bulto"] = hist_unidades_bulto

                            if not row.get("articulo_codigo"):
                                row["articulo_codigo"] = str(
                                    source_line.get("id_articulo")
                                    or row.get("id_articulo")
                                    or "-"
                                )
                            if not row.get("articulo_nombre"):
                                row["articulo_nombre"] = (
                                    source_line.get("articulo")
                                    or source_line.get("descripcion_historica")
                                    or source_line.get("descripcion")
                                    or row.get("articulo")
                                    or f"Artículo {source_line.get('id_articulo', '-')}"
                                )

                    generate_pdf_and_open(
                        doc_for_print,
                        entity_detail or {},
                        items_data,
                        kind="remito",
                        company_config=get_company_config(),
                        show_prices=include_prices,
                    )
                    show_toast("Remito generado correctamente.", kind="success")
                except Exception as exc:
                    show_toast(f"Error al imprimir remito: {exc}", kind="error")

            def _print_remito(_=None):
                ask_print_options("remito", _print_remito_with_options)

            actions = [
                _cancel_button("Cerrar", on_click=lambda _: close_form())
            ]
            if db:
                actions.insert(0, ft.ElevatedButton(
                    "Imprimir remito",
                    icon=ft.icons.PRINT_ROUNDED,
                    bgcolor=COLOR_ACCENT,
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=_print_remito
                ))
            open_form(f"Remito {rem_row.get('numero', '')}", content, actions)
        except Exception as exc:
            show_toast(f"Error al mostrar remito: {exc}", kind="error")

    # Movement filters (Move up to be accessible via reload_catalogs if needed)
    def _mov_live(_=None):
        try:
            movimientos_table.trigger_refresh()
        except Exception as e:
            logger.warning(f"Falló al actualizar tabla de movimientos: {e}")

    def movimientos_data_provider(offset, limit, search, simple, advanced, sorts):
        if not db:
            return [], 0
        try:
            rows = db.fetch_movimientos_stock(
                search=search,
                simple=simple,
                advanced=advanced,
                sorts=sorts,
                limit=limit,
                offset=offset,
            )
            total = db.count_movimientos_stock(search=search, simple=simple, advanced=advanced)
            return rows, total
        except Exception:
            return [], 0

    mov_adv_art = AsyncSelect(label="Artículo", loader=article_loader, width=220, on_change=lambda _: _mov_live(None))
    mov_adv_tipo = ft.Dropdown(label="Tipo Mov.", width=180, on_change=_mov_live); _style_input(mov_adv_tipo)
    mov_adv_depo = ft.Dropdown(label="Depósito", width=180, on_change=_mov_live); _style_input(mov_adv_depo)
    mov_adv_user = ft.Dropdown(label="Usuario", width=180, on_change=_mov_live); _style_input(mov_adv_user)
    mov_adv_desde = _date_field("Desde", width=140); mov_adv_desde.on_submit = _mov_live
    mov_adv_hasta = _date_field("Hasta", width=140); mov_adv_hasta.on_submit = _mov_live

    def refresh_documentos_catalogs():
        if not db: return
        try:
            tipos_doc = db.list_tipos_documento()
            doc_adv_tipo.options = [ft.dropdown.Option("Todos", "Todos")] + [
                ft.dropdown.Option(t["nombre"], t["nombre"]) for t in tipos_doc
            ]
            
            ent_list = db.list_entidades_simple()
            shared_ent_options: List[Dict[str, Any]] = [
                {"value": "0", "label": "Todas", "selected_label": "Todas", "tooltip": "Todas"}
            ]
            for entity in ent_list:
                shared_ent_options.append(_format_entity_option(entity, include_tipo=True))
            
            doc_adv_entidad.options = shared_ent_options
            pago_adv_entidad.options = shared_ent_options
            
            for ctrl in [doc_adv_tipo, doc_adv_entidad, pago_adv_entidad]:
                try:
                    if ctrl.page: ctrl.update()
                except Exception as e:
                    logger.warning(f"Falló al actualizar control de filtro documento/pago: {e}")
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de documentos: {e}")

    def refresh_pagos_catalogs():
        if not db: return
        try:
            formas = db.fetch_formas_pago(limit=100)
            pago_adv_forma.options = [ft.dropdown.Option("0", "Todas")] + [
                ft.dropdown.Option(str(f["id"]), f["descripcion"]) for f in formas
            ]
            if pago_adv_forma.page: pago_adv_forma.update()
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de pagos: {e}")

    def refresh_movimientos_catalogs():
        if not db: return
        try:
            tipos = db.list_tipos_movimiento_simple()
            mov_adv_tipo.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(t["nombre"], t["nombre"]) for t in tipos
            ]
            depos = db.fetch_depositos()
            mov_adv_depo.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(d["nombre"], d["nombre"]) for d in depos
            ]
            users = db.list_usuarios_simple()
            mov_adv_user.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(u["nombre"], u["nombre"]) for u in users
            ]
            
            # Add articles dropdown
            arts = db.list_articulos_simple(limit=500)
            art_options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(str(a["id"]), f"{a['nombre']} (Cod: {a['id']})") for a in arts
            ]
            selected_art = mov_adv_art.value
            if selected_art and not any(opt.key == str(selected_art) for opt in art_options):
                try:
                    missing_art = db.get_article_simple(int(selected_art))
                    if missing_art:
                        art_options.append(
                            ft.dropdown.Option(
                                str(missing_art["id"]),
                                f"{missing_art['nombre']} (Cod: {missing_art['id']})",
                            )
                        )
                except Exception as e:
                    logger.warning(f"Falló al cargar artículo faltante {selected_art}: {e}")
            mov_adv_art.options = art_options

            for ctrl in [mov_adv_tipo, mov_adv_depo, mov_adv_user, mov_adv_art]:
                try: 
                    if ctrl.page: ctrl.update()
                except Exception as e:
                    logger.warning(f"Falló al actualizar control de filtro de movimientos: {e}")
        except Exception as e:
            logger.warning(f"Falló al actualizar catálogos de movimientos: {e}")

    def refresh_remitos_catalogs():
        if not db: return
        try:
            depositos = db.fetch_depositos()
            rem_adv_deposito.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(str(d["id"]), d["nombre"]) for d in depositos
            ]
            _safe_update_control(rem_adv_deposito)
        except Exception as e:
            logger.warning(f"Falló al actualizar interfaz: {e}")

    # Documents View
    # fetch document types and entities for dropdowns
    try:
        tipos_doc = db.list_tipos_documento()
        tipo_options = [ft.dropdown.Option("Todos", "Todos")] + [ft.dropdown.Option(t["nombre"], t["nombre"]) for t in tipos_doc]
        
        entidades = db.list_entidades_simple()
        ent_options = [ft.dropdown.Option("0", "Todas")]
        for entity in entidades:
            option = _format_entity_option(entity, include_tipo=True)
            ent_options.append(ft.dropdown.Option(str(option["value"]), option["label"]))
    except:
        tipo_options = [ft.dropdown.Option("Todos", "Todos")]
        ent_options = [ft.dropdown.Option("0", "Todas")]

    doc_adv_entidad = AsyncSelect(label="Entidad", loader=entity_loader, width=280, on_change=lambda _: _doc_live(None))
    doc_adv_tipo = ft.Dropdown(label="Tipo", options=tipo_options, width=160, value="Todos"); _style_input(doc_adv_tipo)
    
    doc_adv_letra = ft.Dropdown(
        label="Letra", 
        width=100, 
        options=[ft.dropdown.Option("Todos", "Todas")] + [ft.dropdown.Option(l, l) for l in ["A", "B", "C", "M", "R", "X"]],
        value="Todos"
    ); _style_input(doc_adv_letra)

    doc_adv_numero = ft.TextField(label="Número", width=120); _style_input(doc_adv_numero)

    doc_adv_estado = ft.Dropdown(
        label="Estado", 
        width=140, 
        options=[
            ft.dropdown.Option("Todos", "Todos"),
            ft.dropdown.Option(DocumentoEstado.BORRADOR.value, "Borrador"),
            ft.dropdown.Option(DocumentoEstado.CONFIRMADO.value, "Confirmado"),
            ft.dropdown.Option(DocumentoEstado.ANULADO.value, "Anulado"),
            ft.dropdown.Option(DocumentoEstado.PAGADO.value, "Pagado"),
        ],
        value="Todos"
    ); _style_input(doc_adv_estado)

    doc_adv_desde = _date_field("Desde", width=130)
    doc_adv_hasta = _date_field("Hasta", width=130)
    
    documentos_summary_table: Optional[GenericTable] = None

    def _doc_live(_=None):
        if not documentos_summary_table:
            return
        try:
            documentos_summary_table.trigger_refresh()
        except Exception:
            pass
    
    # Range slider for Total
    max_total = 1000000.0
    try: max_total = db.get_max_document_total()
    except Exception as e:
        logger.warning(f"Falló al actualizar: {e}")
    if max_total < 1000: max_total = 1000.0

    # Label for Range Slider (Matches inventory style)
    range_label = ft.Text(f"Total: entre $0 y ${max_total:,.0f}", size=12, weight=ft.FontWeight.BOLD)
    
    def on_range_change(e):
        s = e.control
        range_label.value = f"Total: entre {_format_money(s.start_value)} y {_format_money(s.end_value)}"
        _safe_update_control(range_label)

    doc_adv_total = ft.RangeSlider(
        min=0, max=max_total,
        start_value=0, end_value=max_total,
        divisions=100,
        inactive_color="#E2E8F0",
        active_color=COLOR_ACCENT,
        label="{value}",
        width=300,
        on_change=on_range_change,
        on_change_end=lambda _: documentos_summary_table.refresh()
    )
    
    doc_adv_total_container = ft.Column([
        range_label,
        doc_adv_total
    ], spacing=0, width=320)

    # Setter for Range Slider Reset
    def reset_range_slider(container, _):
        doc_adv_total.start_value = 0
        doc_adv_total.end_value = max_total
        range_label.value = f"Total: entre $0 y ${max_total:,.0f}"
        _safe_update_multiple(doc_adv_total, range_label)

    documentos_summary_table = GenericTable(
        columns=[
            ColumnConfig(key="fecha", label="Fecha", width=120, formatter=_format_datetime),
            ColumnConfig(key="letra", label="Letra", width=60),
            ColumnConfig(key="tipo_documento", label="Tipo", width=120),
            ColumnConfig(key="numero_serie", label="Número", width=100),
            ColumnConfig(key="entidad", label="Entidad", width=200),
            ColumnConfig(key="total", label="Total", width=120, formatter=_format_money),
            ColumnConfig(key="forma_pago", label="Forma de Pago", width=130),
            ColumnConfig(key="estado", label="Estado", width=120, renderer=lambda row: _status_pill(row.get("estado"), row)),
            ColumnConfig(key="usuario", label="Usuario", width=120),
            ColumnConfig(
                key="_confirm", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") == DocumentoEstado.BORRADOR.value and (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    icon=ft.icons.CHECK_CIRCLE,
                    tooltip="Confirmar comprobante",
                    icon_color=COLOR_SUCCESS,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: _confirm_document(int(rid)),
                )
            ),
            ColumnConfig(
                key="_detail", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.INFO_OUTLINE, tooltip="Ver detalle",
                    icon_color=COLOR_TEXT_MUTED,
                    on_click=lambda e: view_doc_detail(row)
                )
            ),
            ColumnConfig(
                key="_edit", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") == DocumentoEstado.BORRADOR.value and not row.get("cae") and (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    icon=ft.icons.EDIT_ROUNDED,
                    tooltip="Editar borrador",
                    icon_color=COLOR_ACCENT,
                    on_click=lambda e, rid=row["id"]: open_nuevo_comprobante(edit_doc_id=rid),
                )
            ),
            ColumnConfig(
                key="_copy", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.COPY_ALL_ROUNDED,
                    tooltip="Copiar como nuevo",
                    icon_color=ft.Colors.BLUE_400,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: open_nuevo_comprobante(copy_doc_id=rid),
                )
            ),
            ColumnConfig(
                key="_print", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.PRINT_ROUNDED,
                    tooltip="Imprimir",
                    icon_color=COLOR_TEXT_MUTED,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: request_invoice_print(int(rid)),
                )
            ),
            ColumnConfig(
                key="_download_pdf", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.DOWNLOAD_ROUNDED,
                    tooltip="Guardar PDF",
                    icon_color=COLOR_INFO,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: request_invoice_download(int(rid)),
                )
            ),
            ColumnConfig(
                key="_afip", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    _can_authorize_afip(row) and (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    icon=ft.icons.SECURITY,
                    tooltip="Autorizar AFIP",
                    icon_color=COLOR_ACCENT,
                    icon_size=18,
                    on_click=lambda e, r=row: _confirm_afip_authorization(r),
                )
            ),
            ColumnConfig(
                key="_annul", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") != "ANULADO" and not row.get("cae") and (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    icon=ft.icons.BLOCK_ROUNDED,
                    tooltip="Anular comprobante",
                    icon_color=COLOR_ERROR,
                    on_click=lambda e: ask_confirm(
                        "Anular Comprobante",
                        f"¿Estás seguro que deseas anular el comprobante {row['numero_serie']}? Esta acción revertirá el stock.",
                        "Anular",
                        lambda: (db.anular_documento(row["id"]), show_toast("Comprobante anulado", kind="success"), documentos_summary_table.refresh()) if db else None
                    ),
                )
            ),
            ColumnConfig(
                key="_nc", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") == DocumentoEstado.CONFIRMADO.value and row.get("cae"),
                    icon=ft.icons.RECEIPT_LONG_OUTLINED,
                    tooltip="Generar Nota de Crédito",
                    icon_color=COLOR_WARNING,
                    on_click=lambda e, rid=row["id"]: open_nuevo_comprobante(copy_doc_id=rid),
                )
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_documentos_resumen, db.count_documentos_resumen),
        advanced_filters=[
            AdvancedFilterControl("id_entidad", doc_adv_entidad),
            AdvancedFilterControl("tipo", doc_adv_tipo),
            AdvancedFilterControl("letra", doc_adv_letra),
            AdvancedFilterControl("numero", doc_adv_numero),
            AdvancedFilterControl("desde", doc_adv_desde),
            AdvancedFilterControl("hasta", doc_adv_hasta),
            AdvancedFilterControl("estado", doc_adv_estado),
            AdvancedFilterControl("total_min", doc_adv_total_container, getter=lambda _: doc_adv_total.start_value, setter=reset_range_slider),
            AdvancedFilterControl("total_max", doc_adv_total_container, getter=lambda _: doc_adv_total.end_value, setter=reset_range_slider),
        ],
        show_inline_controls=False, show_mass_actions=False, auto_load=True, page_size=50, show_export_button=True, show_export_scope=True,
    )
    documentos_summary_table.search_field.hint_text = "Buscar comprobantes (entidad, tipo, número, usuario)"
    # Manual wire for RangeSlider since it's inside a container in AdvancedFilterControl
    # Reset on_change to standard refreshing
    documentos_view = ft.Column([
        ft.Row([
            make_stat_card("Facturación Mes", "$0", "RECEIPT_LONG_ROUNDED", COLOR_ACCENT, key="docs_ventas"),
            make_stat_card("Pagos Pendientes", "0", "DRIVING_ROUNDED", COLOR_WARNING, key="docs_pendientes"),
            make_stat_card("Compras Mes", "$0", "SHOPPING_CART_ROUNDED", COLOR_SUCCESS, key="docs_compras"),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Comprobantes y Facturación", 
            "Consulta de facturas, presupuestos y compras.", 
            documentos_summary_table.build(),
            actions=[
                btn_nuevo_comprobante := ft.ElevatedButton("Nuevo Comprobante", icon=ft.icons.ADD_ROUNDED, bgcolor=COLOR_ACCENT, color="#FFFFFF", 
                                   on_click=lambda e: open_nuevo_comprobante(),
                                   style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))),
            ]
        )
    ], spacing=10, expand=True)

    documentos_view = ft.Container(
        content=documentos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    def _rem_live(_=None):
        try:
            remitos_table.trigger_refresh()
        except Exception:
            pass

    def _open_remito_estado_dialog(rem_row: Dict[str, Any]):
        if not rem_row:
            return

        current_state = rem_row.get("estado") or RemotoEstado.PENDIENTE.value
        dropdown = ft.Dropdown(
            label="Estado",
            value=current_state,
            width=240,
            options=[ft.dropdown.Option(code, label) for code, label in REMITO_ESTADOS],
        )
        _style_input(dropdown)

        dialog = ft.AlertDialog(
            title=ft.Text("Cambiar estado del remito"),
            content=ft.Column(
                [
                    ft.Text(f"Estado actual: {current_state}", size=12, color=COLOR_TEXT_MUTED),
                    dropdown,
                ],
                spacing=10,
            ),
            actions=[]
        )

        def _save_state(_=None):
            selected_state = dropdown.value
            if not selected_state or selected_state == current_state:
                page.close(dialog)
                return
            if not db:
                show_toast("Base de datos no disponible", kind="error")
                return
            try:
                db.update_remito_estado(int(rem_row["id"]), selected_state)
                show_toast("Estado actualizado", kind="success")
                remitos_table.trigger_refresh()
                page.close(dialog)
            except Exception as exc:
                show_toast(f"Error actualizando estado: {exc}", kind="error")

        dialog.actions = [
            _cancel_button("Cancelar", on_click=lambda _: page.close(dialog)),
            ft.ElevatedButton(
                "Guardar",
                icon=ft.icons.CHECK,
                bgcolor=COLOR_ACCENT,
                color="#FFFFFF",
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                on_click=_save_state,
            ),
        ]
        page.open(dialog)

    def _movimiento_observacion_dialog(observacion: Optional[str]):
        if not observacion:
            return
        dlg = ft.AlertDialog(
            title=ft.Text("Observación del movimiento"),
            content=ft.Text(observacion),
        )
        dlg.actions = [
            _cancel_button("Cerrar", on_click=lambda e, dialog=dlg: page.close(dialog)),
        ]
        page.open(dlg)

    def _movimiento_observacion_icon(row: Dict[str, Any]) -> ft.Control:
        texto = row.get("observacion") or ""
        return ft.IconButton(
            icon=ft.icons.INFO_OUTLINE,
            tooltip=texto or "Sin observaciones",
            icon_color=COLOR_ACCENT if texto else COLOR_TEXT_MUTED,
            icon_size=18,
            disabled=not bool(texto.strip()),
            on_click=lambda e, value=texto: _movimiento_observacion_dialog(value),
        )

    rem_adv_entidad = AsyncSelect(
        label="Entidad",
        loader=entity_loader,
        width=280,
        initial_items=[{"value": "", "label": "Todas"}],
        on_change=lambda _: _rem_live(None)
    )
    _style_input(rem_adv_entidad)
    rem_adv_estado = ft.Dropdown(
        label="Estado",
        options=[
            ft.dropdown.Option("", "Todos"),
            ft.dropdown.Option(RemotoEstado.PENDIENTE.value, "Pendiente"),
            ft.dropdown.Option(RemotoEstado.DESPACHADO.value, "Despachado"),
            ft.dropdown.Option(RemotoEstado.ENTREGADO.value, "Entregado"),
            ft.dropdown.Option(RemotoEstado.ANULADO.value, "Anulado"),
        ],
        width=200,
        value="",
        on_change=_rem_live,
    ); _style_input(rem_adv_estado)
    rem_adv_deposito = ft.Dropdown(
        label="Depósito",
        options=[ft.dropdown.Option("", "Todos")],
        width=200,
        on_change=_rem_live,
    ); _style_input(rem_adv_deposito)
    rem_adv_documento = ft.TextField(label="Documento / Nº", width=180); _style_input(rem_adv_documento)
    rem_adv_documento.on_change = lambda _: _rem_live(None)
    rem_adv_documento.on_submit = lambda _: _rem_live(None)
    rem_adv_desde = _date_field("Desde", width=140); rem_adv_desde.on_submit = _rem_live
    rem_adv_hasta = _date_field("Hasta", width=140); rem_adv_hasta.on_submit = _rem_live

    remitos_table = GenericTable(
        columns=[
            ColumnConfig(key="numero", label="Remito", width=120),
            ColumnConfig(key="fecha", label="Fecha", width=120, formatter=_format_datetime),
            ColumnConfig(key="estado", label="Estado", width=120, renderer=lambda row: _remito_status_pill(row.get("estado"))),
            ColumnConfig(key="entidad", label="Entidad", width=220),
            ColumnConfig(key="deposito", label="Depósito", width=160),
            ColumnConfig(key="documento_numero", label="Documento", width=160),
            ColumnConfig(key="total_unidades", label="Unidades", width=90, formatter=_format_quantity),
            ColumnConfig(
                key="_detail",
                label="",
                sortable=False,
                width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.INFO_OUTLINE,
                    tooltip="Ver remito",
                    icon_color=COLOR_TEXT_MUTED,
                    on_click=lambda e, r=row: view_remito_detail(r),
                )
            ),
            ColumnConfig(
                key="_estado",
                label="",
                sortable=False,
                width=60,
                renderer=lambda row: ft.IconButton(
                    icon=ft.icons.SWAP_HORIZ_ROUNDED,
                    tooltip="Cambiar estado",
                    icon_color=COLOR_ACCENT,
                    visible=(CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]),
                    on_click=lambda e, r=row: _open_remito_estado_dialog(r),
                )
            ),
            ColumnConfig(key="fecha_despacho", label="Despacho", width=120, formatter=_format_datetime),
            ColumnConfig(key="fecha_entrega", label="Entrega", width=120, formatter=_format_datetime),
            ColumnConfig(key="direccion_entrega", label="Dirección", width=220),
        ],
        data_provider=create_catalog_provider(db.fetch_remitos, db.count_remitos),
        advanced_filters=[
            AdvancedFilterControl("entidad", rem_adv_entidad),
            AdvancedFilterControl("estado", rem_adv_estado),
            AdvancedFilterControl("deposito", rem_adv_deposito),
            AdvancedFilterControl("documento", rem_adv_documento),
            AdvancedFilterControl("desde", rem_adv_desde),
            AdvancedFilterControl("hasta", rem_adv_hasta),
        ],
        show_inline_controls=False,
        show_mass_actions=False,
        auto_load=True,
        page_size=40,
        page_size_options=(20, 40, 80),
        show_export_button=True,
        show_export_scope=True,
    )
    remitos_table.search_field.hint_text = "Buscar remito o cliente..."

    def refresh_remito_summary() -> None:
        if not db:
            return
        try:
            total = db.count_remitos()
            if "remitos_total" in card_registry:
                card_registry["remitos_total"].value = f"{total:,}"
        except Exception:
            pass

    remito_refresh_orig = remitos_table.refresh

    def _remitos_table_refresh(*args, **kwargs):
        remito_refresh_orig(*args, **kwargs)
        refresh_remito_summary()

    remitos_table.refresh = _remitos_table_refresh  # type: ignore[attr-defined]

    remitos_view = ft.Column([
        ft.Row([
            make_stat_card("Remitos Pendientes", "0", "LOCAL_SHIPPING_ROUNDED", COLOR_WARNING, key="remitos_pend"),
            make_stat_card("Entregas Hoy", "0", "DONE_ALL_ROUNDED", COLOR_SUCCESS, key="remitos_entregas"),
            make_stat_card("Total Remitos", "0", "INVENTORY_2_ROUNDED", COLOR_INFO, key="remitos_total"),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Remitos",
            "Seguimiento de entregas y despachos generados automáticamente.",
            remitos_table.build(),
        )
    ], spacing=10, expand=True)

    remitos_view = ft.Container(
        content=remitos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # Movements View
    # (Filters moved up)

    movimientos_table = GenericTable(
        columns=[
            ColumnConfig(key="fecha", label="Fecha", width=120, formatter=_format_datetime),
            ColumnConfig(
                key="articulo", label="Artículo", width=260,
                renderer=lambda row: ft.Text(
                    f"{row.get('articulo', '')} (Stock: {_format_quantity(row.get('stock_resultante'))})" 
                    if row.get('stock_resultante') is not None 
                    else row.get('articulo', ''),
                    size=13
                )
            ),
            ColumnConfig(key="tipo_movimiento", label="Tipo", width=120),
            ColumnConfig(key="cantidad", label="Cant.", width=80, formatter=_format_quantity),
            ColumnConfig(
                key="comprobante", label="Comprobante", width=180,
                renderer=lambda row: ft.Text(f"{row.get('tipo_documento') or ''} {row.get('nro_comprobante') or ''}".strip() or "---", size=13)
            ),
            ColumnConfig(key="entidad", label="Entidad", width=180),
            ColumnConfig(key="deposito", label="Depósito", width=120),
            ColumnConfig(key="usuario", label="Usuario", width=120),
            ColumnConfig(
                key="observacion",
                label="",
                sortable=False,
                width=48,
                renderer=_movimiento_observacion_icon,
            ),
        ],
        data_provider=movimientos_data_provider,
        advanced_filters=[
            AdvancedFilterControl("articulo", mov_adv_art),
            AdvancedFilterControl("tipo_movimiento", mov_adv_tipo),
            AdvancedFilterControl("deposito", mov_adv_depo),
            AdvancedFilterControl("usuario", mov_adv_user),
            AdvancedFilterControl("desde", mov_adv_desde),
            AdvancedFilterControl("hasta", mov_adv_hasta),
        ],
        show_inline_controls=False,
        show_mass_actions=False,
        auto_load=True,
        page_size=50,
        page_size_options=(20, 50, 100),
        show_export_button=True,
        show_export_scope=True,
    )
    movimientos_table.search_field.hint_text = "Buscar movimientos (artículo, tipo, entidad)"
    movimientos_view = ft.Column([
        ft.Row([
            make_stat_card("Ingresos Hoy", "0", "NORTH_EAST_ROUNDED", COLOR_SUCCESS, key="movs_ingresos"),
            make_stat_card("Salidas Hoy", "0", "SOUTH_WEST_ROUNDED", COLOR_ERROR, key="movs_salidas"),
            make_stat_card("Ajustes Hoy", "0", "SWAP_HORIZ_ROUNDED", COLOR_ACCENT, key="movs_ajustes"),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Movimientos de Stock", 
            "Registro histórico de entradas, salidas y transferencias.", 
            movimientos_table.build()
        )
    ], spacing=10, expand=True)

    movimientos_view = ft.Container(
        content=movimientos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # Payments View
    # Payments View
    pago_adv_ref = ft.TextField(label="Referencia", width=200, on_change=lambda _: pagos_table.trigger_refresh()); _style_input(pago_adv_ref)
    pago_adv_desde = _date_field("Desde", width=140); pago_adv_desde.on_submit = lambda _: pagos_table.trigger_refresh()
    pago_adv_hasta = _date_field("Hasta", width=140); pago_adv_hasta.on_submit = lambda _: pagos_table.trigger_refresh()
    pago_adv_entidad = AsyncSelect(label="Entidad", loader=entity_loader, width=280, on_change=lambda _: pagos_table.trigger_refresh())
    
    # Forma de pago filter
    try:
        fp_list = db.fetch_formas_pago(limit=100)
        fp_options = [ft.dropdown.Option("0", "Todas")] + [ft.dropdown.Option(str(f["id"]), f["descripcion"]) for f in fp_list]
    except: fp_options = [ft.dropdown.Option("0", "Todas")]
    pago_adv_forma = ft.Dropdown(label="Forma Pago", options=fp_options, width=200, value="0", on_change=lambda _: pagos_table.trigger_refresh()); _style_input(pago_adv_forma)

    # Range slider for Monto
    max_monto = 500000.0
    try:
        # We don't have a direct get_max_monto_pago, using a safe default or querying later
        pass
    except Exception as e:
        logger.warning(f"Falló al actualizar: {e}")
    
    monto_range_label = ft.Text(f"Monto: entre $0 y ${max_monto:,.0f}", size=12, weight=ft.FontWeight.BOLD)
    
    def on_pago_monto_change(e):
        s = e.control
        monto_range_label.value = f"Monto: entre {_format_money(s.start_value)} y {_format_money(s.end_value)}"
        _safe_update_control(monto_range_label)

    pago_adv_monto = ft.RangeSlider(
        min=0, max=max_monto,
        start_value=0, end_value=max_monto,
        divisions=100,
        inactive_color="#E2E8F0",
        active_color=COLOR_ACCENT,
        label="{value}",
        width=300,
        on_change=on_pago_monto_change,
        on_change_end=lambda _: pagos_table.trigger_refresh()
    )
    
    pago_adv_monto_container = ft.Column([
        monto_range_label,
        pago_adv_monto
    ], spacing=0, width=320)

    def reset_pago_monto(container, _):
        pago_adv_monto.start_value = 0
        pago_adv_monto.end_value = max_monto
        monto_range_label.value = f"Monto: entre $0 y ${max_monto:,.0f}"
        _safe_update_multiple(pago_adv_monto, monto_range_label)

    def show_payment_info(text):
        if not text: return
        dlg = ft.AlertDialog(
            title=ft.Text("Observaciones del Pago"),
            content=ft.Text(text),
            actions=[ft.TextButton("Cerrar", on_click=lambda e: page.close(dlg))], # type: ignore
        )
        page.open(dlg)

    pagos_table = GenericTable(
        columns=[
            ColumnConfig(key="fecha", label="Fecha", width=120, formatter=_format_datetime),
            ColumnConfig(key="monto", label="Monto", width=100, formatter=_format_money),
            ColumnConfig(key="forma", label="Forma Pago", width=120),
            ColumnConfig(key="documento", label="Comprobante", width=120),
            ColumnConfig(key="entidad", label="Entidad", width=200),
            ColumnConfig(key="referencia", label="Referencia", width=150, renderer=lambda row: ft.Text(row.get("referencia") or "---", tooltip="Dato adicional del pago (ej. nro cheque, banco, etc.)")),
            ColumnConfig(
                key="observacion",
                label="Info",
                width=60,
                sortable=False,
                renderer=lambda row: ft.IconButton(ft.icons.INFO_OUTLINE, tooltip="Ver observaciones", icon_color=COLOR_ACCENT if row.get("observacion") else "grey", on_click=lambda _: show_payment_info(row.get("observacion"))),
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_pagos, db.count_pagos),
        advanced_filters=[
            AdvancedFilterControl("referencia", pago_adv_ref),
            AdvancedFilterControl("entidad", pago_adv_entidad),
            AdvancedFilterControl("forma", pago_adv_forma),
            AdvancedFilterControl("desde", pago_adv_desde),
            AdvancedFilterControl("hasta", pago_adv_hasta),
            AdvancedFilterControl("monto_min", pago_adv_monto_container, getter=lambda _: pago_adv_monto.start_value, setter=reset_pago_monto),
            AdvancedFilterControl("monto_max", pago_adv_monto_container, getter=lambda _: pago_adv_monto.end_value, setter=reset_pago_monto),
        ],
        show_inline_controls=False,
        show_mass_actions=False, # Deshabilitar acciones masivas
        auto_load=True, 
        page_size=20,
        show_export_button=True,
        show_export_scope=True,
    )
    pagos_table.search_field.hint_text = "Buscar pagos (referencia, forma, documento, entidad)"

    def open_nuevo_pago(_=None):
        if not db: return
        try:
            formas = db.fetch_formas_pago(limit=100)
            # We need pending documents to pay... or just select Entity and then show Documents?
            # For simplicity: Select Entity -> Show list of unpaid documents in a dropdown or Table?
            # Or simplified flow: Just register a payment linked to a document ID manually (too hard).
            # Let's do: Select Entity -> Fetch Pending Docs -> Select Doc -> Pay.
            
            entidades = db.list_proveedores() # Reusing this, returns id/nombre. Actually we need clients too.
            # list_entidades_simple returns all.
            entidades = db.list_entidades_simple(only_active=False)
            
        except Exception as e:
            show_toast(f"Error cargando datos: {e}", kind="error"); return

        pago_entidad = AsyncSelect(
            label="Entidad *", 
            loader=entity_loader, 
            width=400,
            initial_items=[_format_entity_option(e, include_tipo=True) for e in entidades]
        )
        
        doc_totals: Dict[str, float] = {}
        
        def pending_doc_loader(query, offset, limit):
            eid = pago_entidad.value
            if not eid:
                return [], False
            try:
                rows = db.fetch_documentos_pendientes(
                    int(eid),
                    search=query,
                    limit=limit,
                    offset=offset,
                )
                items = []
                for r in rows:
                    val = str(r["id"])
                    total = float(r.get("total") or 0)
                    doc_totals[val] = total
                    items.append({
                        "value": r["id"],
                        "label": f"{r.get('numero_serie', 'N/A')} - {_format_money(total)} ({_format_datetime(r.get('fecha'))})",
                    })
                return items, len(rows) >= limit
            except Exception:
                return [], False

        pago_monto = _number_field("Monto *", width=200)

        def on_doc_change(val):
            # val is the document ID as a string or int
            if val and str(val) in doc_totals:
                pago_monto.value = str(doc_totals[str(val)]).replace(".", ",")
            else:
                pago_monto.value = ""
            _safe_update_control(pago_monto)

        pago_documento = AsyncSelect(label="Comprobante Pendiente *", loader=pending_doc_loader, width=400, disabled=True, on_change=on_doc_change)
        
        pago_forma = ft.Dropdown(label="Forma de Pago *", options=[ft.dropdown.Option(str(f["id"]), f["descripcion"]) for f in formas], width=250)
        _style_input(pago_forma)
        
        # pago_monto already defined above to be used in on_doc_change
        pago_fecha = _date_field("Fecha *", width=200)
        pago_fecha.value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pago_ref = ft.TextField(label="Referencia", width=250); _style_input(pago_ref)
        pago_obs = ft.TextField(label="Observaciones", multiline=True, width=500); _style_input(pago_obs)

        def on_entidad_change(val):
            pago_documento.value = ""
            pago_documento.clear_cache()
            pago_monto.value = ""
            if val:
                pago_documento.set_busy(True)
                pago_documento.prefetch(on_done=lambda: (pago_documento.set_busy(False), _safe_update_control(pago_documento)))
                pago_documento.disabled = False
            else:
                pago_documento.set_busy(False)
                pago_documento.disabled = True
            
            _safe_update_multiple(pago_documento, pago_monto)

        pago_entidad.on_change = on_entidad_change
        
        def _save_pago(_):
            if not pago_documento.value or not pago_forma.value or not pago_monto.value or not (pago_fecha.value and str(pago_fecha.value).strip()):
                show_toast("Campos obligatorios faltantes", kind="warning"); return
            try:
                # Convert fecha
                f_str = pago_fecha.value
                # Let's assume passed validation.
                
                monto_val = pago_monto.value.replace(",", ".")
                
                pid = db.create_payment(
                    id_documento=int(pago_documento.value), # This will fail if placeholder
                    id_forma_pago=int(pago_forma.value),
                    monto=float(monto_val),
                    fecha=f_str,
                    referencia=pago_ref.value,
                    observacion=pago_obs.value
                )
                if db:
                    db.log_activity("PAGO", "INSERT", id_entidad=pid, detalle={"monto": monto_val})
                show_toast("Pago registrado", kind="success")
                close_form()
                pagos_table.refresh()
            except ValueError:
                show_toast("Error: Formato de monto inválido", kind="error")
            except Exception as e:
                show_toast(f"Error: {e}", kind="error")

        content = ft.Container(
            width=550,
            height=450,
            content=ft.Column([
                pago_entidad, pago_documento,
                ft.Row([pago_forma, pago_monto], spacing=10),
                ft.Row([pago_fecha, pago_ref], spacing=10),
                pago_obs,
                ft.Text("El pago no puede deshacerse. Revisá bien antes de confirmar.", size=12, color=COLOR_WARNING),
            ], spacing=15, scroll=ft.ScrollMode.ADAPTIVE)
        )

        open_form("Nuevo Pago", content, [
            _cancel_button("Cancelar", on_click=close_form),
            ft.ElevatedButton(
                "Registrar Pago", 
                bgcolor=COLOR_ACCENT, 
                color="#FFFFFF", 
                on_click=_save_pago,
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
            )
        ])

    pagos_view = ft.Column([
        ft.Row([
            make_stat_card("Cobrado Hoy", "$0", "ACCOUNT_BALANCE_WALLET_ROUNDED", COLOR_SUCCESS, key="pagos_hoy"),
            make_stat_card("Recientes (7d)", "0", "PAYMENTS_ROUNDED", COLOR_ACCENT, key="pagos_recientes"),
            make_stat_card("Estado Caja", "ABIERTA", "OUTPUT_ROUNDED", COLOR_WARNING),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Caja y Gestión de Pagos", 
            "Registro de ingresos y egresos de caja.", 
            pagos_table.build(),
            actions=[
                 btn_nuevo_pago := ft.ElevatedButton(
                     "Nuevo Pago", 
                     icon=ft.icons.ADD_ROUNDED, 
                     bgcolor=COLOR_ACCENT, 
                     color="#FFFFFF", 
                     on_click=open_nuevo_pago,
                     style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                 )
            ]
        )
    ], spacing=10, expand=True)

    pagos_view = ft.Container(
        content=pagos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # =========================================================================
    # CUENTAS CORRIENTES VIEW
    # =========================================================================

    def _cc_live(e):
        try:
            cuentas_table.trigger_refresh()
        except Exception as e:
            logger.warning(f"Falló al actualizar cuentas table: {e}")

    cc_adv_tipo = _dropdown("Tipo", [("", "Todos"), ("CLIENTE", "Clientes"), ("PROVEEDOR", "Proveedores")], value="", width=180, on_change=_cc_live)
    cc_adv_estado = _dropdown("Estado", [("", "Todos"), ("DEUDOR", "Deudores"), ("A_FAVOR", "A Favor"), ("AL_DIA", "Al Día")], value="", width=180, on_change=_cc_live)
    cc_adv_solo_saldo = ft.Switch(label="Solo con saldo", value=False, on_change=_cc_live)

    def cuentas_provider(offset, limit, search, simple, advanced, sorts):
        if db is None:
            raise provider_error()
        rows = db.fetch_cuentas_corrientes(search=search, simple=simple, advanced=advanced, sorts=sorts, limit=limit, offset=offset)
        total = db.count_cuentas_corrientes(search=search, simple=simple, advanced=advanced)
        return rows, total

    def _saldo_pill(value: Any, row: Optional[Dict[str, Any]] = None) -> ft.Control:
        saldo = float(value or 0)
        if saldo > 0:
            bg, fg, label = "#FEE2E2", "#991B1B", f"Debe {_format_money(saldo)}"
        elif saldo < 0:
            bg, fg, label = "#DCFCE7", "#166534", f"A favor {_format_money(abs(saldo))}"
        else:
            bg, fg, label = "#F1F5F9", "#475569", "Al día"
        return ft.Container(
            padding=ft.padding.symmetric(horizontal=10, vertical=4),
            border_radius=20,
            bgcolor=bg,
            content=ft.Text(label, size=11, weight=ft.FontWeight.W_600, color=fg),
        )

    def ver_movimientos_entidad(e, entidad_id):
        """Muestra los movimientos de una entidad específica con formato contable."""
        if not db: return
        try:
            # Obtenemos movimientos (ahora el límite es 500 por defecto en database.py)
            movimientos = db.get_movimientos_entidad(int(entidad_id))
            
            mov_rows = []
            for m in movimientos:
                tipo = m.get("tipo_movimiento", "")
                monto = float(m.get("monto", 0))
                
                # Definición contable:
                # DEBE (+ deuda): DEBITO, AJUSTE_DEBITO (Rojo)
                # HABER (- deuda): CREDITO, AJUSTE_CREDITO, ANULACION (Verde)
                
                es_debe = tipo in ("DEBITO", "AJUSTE_DEBITO")
                
                monto_debe = _format_money(monto) if es_debe else ""
                monto_haber = _format_money(monto) if not es_debe else ""
                
                # Colores: Rojo para lo que aumenta la deuda, Verde para lo que la baja
                color_monto = COLOR_ERROR if es_debe else COLOR_SUCCESS
                
                mov_rows.append(
                    ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(m.get("fecha", ""))[:16], size=11)),
                        ft.DataCell(ft.Text(m.get("concepto", ""), width=250, size=11)),
                        ft.DataCell(ft.Text(monto_debe, color=COLOR_ERROR, weight=ft.FontWeight.BOLD if es_debe else None, size=11)),
                        ft.DataCell(ft.Text(monto_haber, color=COLOR_SUCCESS, weight=ft.FontWeight.BOLD if not es_debe else None, size=11)),
                        ft.DataCell(ft.Text(_format_money(m.get("saldo_nuevo", 0)), weight=ft.FontWeight.W_600, size=11)),
                    ])
                )
            
            mov_table = SafeDataTable(
                columns=[
                    ft.DataColumn(ft.Text("Fecha/Hora")),
                    ft.DataColumn(ft.Text("Concepto")),
                    ft.DataColumn(ft.Text("Debe (+)")),
                    ft.DataColumn(ft.Text("Haber (-)")),
                    ft.DataColumn(ft.Text("Saldo Acum.")),
                ],
                rows=mov_rows, # Sin el límite de [:30]
                column_spacing=20,
                heading_row_color=ft.colors.with_opacity(0.05, COLOR_ACCENT),
                heading_row_height=45,
            )
            
            dlg = ft.AlertDialog(
                title=ft.Row([
                    ft.Icon(ft.icons.HISTORY_ROUNDED, color=COLOR_ACCENT),
                    ft.Text("Historial de Movimientos"),
                ], spacing=10),
                content=ft.Container(
                    content=ft.Column([
                        ft.Text("Mostrando los últimos movimientos (más recientes primero).", size=12, italic=True, color=COLOR_TEXT_MUTED),
                        ft.Divider(height=1, color=COLOR_BORDER),
                        mov_table
                    ], scroll=ft.ScrollMode.ALWAYS, spacing=10),
                    width=950,
                    height=550,
                    padding=10,
                ),
                actions=[
                    ft.TextButton("Cerrar", on_click=lambda _: page.close(dlg))
                ],
            )
            page.open(dlg)
        except Exception as ex:
            show_toast(f"Error cargando movimientos: {ex}", kind="error")

    def open_pago_cc(_=None):
        """Abre formulario para registrar pago directo a cuenta corriente."""
        if not db: return
        try:
            entidades = db.list_entidades_simple(only_active=True)
            formas = db.fetch_formas_pago(limit=100)
        except Exception as e:
            show_toast(f"Error cargando datos: {e}", kind="error")
            return

        pcc_entidad = AsyncSelect(
            label="Entidad *",
            loader=entity_loader,
            width=400,
        )
        
        pcc_saldo = ft.Text("Saldo: $0,00", size=12, color=COLOR_TEXT_MUTED)
        
        def on_entidad_change(e):
            if pcc_entidad.value:
                info = db.get_saldo_entidad(int(pcc_entidad.value))
                pcc_saldo.value = f"Saldo actual: {_format_money(info.get('saldo', 0))}"
                pcc_saldo.update()
        # (pcc_entidad.on_change is already wired in constructor)
        pcc_entidad.on_change = on_entidad_change
        
        pcc_forma = ft.Dropdown(
            label="Forma de Pago *",
            options=[ft.dropdown.Option(str(f["id"]), f["descripcion"]) for f in formas],
            width=250
        )
        _style_input(pcc_forma)
        
        pcc_monto = _number_field("Monto *", width=180)
        pcc_concepto = ft.TextField(label="Concepto", width=300, value="Pago recibido")
        _style_input(pcc_concepto)
        pcc_referencia = ft.TextField(label="Referencia (Nro cheque, etc)", width=250)
        _style_input(pcc_referencia)
        pcc_obs = ft.TextField(label="Observaciones", multiline=True, width=500)
        _style_input(pcc_obs)

        def _save_pago_cc(_):
            if not pcc_entidad.value or not pcc_forma.value or not pcc_monto.value:
                show_toast("Complete los campos obligatorios", kind="warning")
                return
            try:
                monto = _parse_float(pcc_monto.value, "Monto")
                pago_id = db.registrar_pago_cuenta_corriente(
                    id_entidad=int(pcc_entidad.value),
                    id_forma_pago=int(pcc_forma.value),
                    monto=monto,
                    concepto=pcc_concepto.value or "Pago recibido",
                    referencia=pcc_referencia.value,
                    observacion=pcc_obs.value
                )
                if db and pago_id:
                    db.log_activity("PAGO_CC", "INSERT", id_entidad=pago_id, detalle={"entidad": pcc_entidad.value, "monto": monto, "forma": pcc_forma.value})
                show_toast("Pago registrado correctamente", kind="success")
                close_form()
                cuentas_table.refresh()
                refresh_cc_stats()
            except Exception as ex:
                show_toast(f"Error: {ex}", kind="error")

        content = ft.Container(
            width=550,
            height=400,
            content=ft.Column([
                ft.Row([pcc_entidad], spacing=10),
                pcc_saldo,
                ft.Row([pcc_forma, pcc_monto], spacing=10),
                ft.Row([pcc_concepto, pcc_referencia], spacing=10),
                pcc_obs
            ], spacing=15, scroll=ft.ScrollMode.ADAPTIVE)
        )

        open_form("Registrar Pago/Cobro", content, [
            _cancel_button("Cancelar", on_click=close_form),
            ft.ElevatedButton("Registrar", bgcolor=COLOR_SUCCESS, color="#FFFFFF", on_click=_save_pago_cc, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))
        ])

    def open_ajuste_cc(_=None):
        """Abre formulario para ajuste manual de saldo."""
        if not db: return
        try:
            entidades = db.list_entidades_simple(only_active=False)
        except Exception as e:
            show_toast(f"Error cargando datos: {e}", kind="error")
            return

        aj_entidad = AsyncSelect(
            label="Entidad *",
            loader=entity_loader,
            width=400,
        )
        
        aj_saldo = ft.Text("Saldo actual: $0,00", size=12, color=COLOR_TEXT_MUTED)
        
        def on_ent_change(e):
            if aj_entidad.value:
                info = db.get_saldo_entidad(int(aj_entidad.value))
                aj_saldo.value = f"Saldo actual: {_format_money(info.get('saldo', 0))}"
                aj_saldo.update()
        aj_entidad.on_change = on_ent_change
        
        aj_tipo = _dropdown("Tipo de Ajuste *", [("AJUSTE_CREDITO", "Reducir Deuda (Crédito)"), ("AJUSTE_DEBITO", "Aumentar Deuda (Débito)")], width=280)
        aj_monto = _number_field("Monto *", width=180)
        aj_concepto = ft.TextField(label="Concepto/Motivo *", width=400)
        _style_input(aj_concepto)
        aj_obs = ft.TextField(label="Observaciones", multiline=True, width=500)
        _style_input(aj_obs)

        def _save_ajuste(_):
            if not aj_entidad.value or not aj_tipo.value or not aj_monto.value or not aj_concepto.value:
                show_toast("Complete todos los campos obligatorios", kind="warning")
                return
            try:
                monto = _parse_float(aj_monto.value, "Monto")
                ajuste_id = db.ajustar_saldo_cc(
                    id_entidad=int(aj_entidad.value),
                    tipo=aj_tipo.value,
                    monto=monto,
                    concepto=aj_concepto.value,
                    observacion=aj_obs.value
                )
                if db and ajuste_id:
                    db.log_activity("AJUSTE_CC", "INSERT", id_entidad=ajuste_id, detalle={"entidad": aj_entidad.value, "tipo": aj_tipo.value, "monto": monto})
                show_toast("Ajuste aplicado correctamente", kind="success")
                close_form()
                cuentas_table.refresh()
                refresh_cc_stats()
            except Exception as ex:
                show_toast(f"Error: {ex}", kind="error")

        content = ft.Container(
            width=550,
            height=350,
            content=ft.Column([
                aj_entidad,
                aj_saldo,
                ft.Row([aj_tipo, aj_monto], spacing=10),
                aj_concepto,
                aj_obs
            ], spacing=15, scroll=ft.ScrollMode.ADAPTIVE)
        )

        open_form("Ajuste de Saldo", content, [
            _cancel_button("Cancelar", on_click=close_form),
            ft.ElevatedButton("Aplicar Ajuste", bgcolor=COLOR_WARNING, color="#FFFFFF", on_click=_save_ajuste, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))
        ])

    cuentas_table = GenericTable(
        columns=[
            ColumnConfig(key="entidad", label="Entidad", width=250),
            ColumnConfig(key="tipo_entidad", label="Tipo", width=100),
            ColumnConfig(key="cuit", label="CUIT", width=120),
            ColumnConfig(key="saldo_actual", label="Saldo", width=150, renderer=lambda row: _saldo_pill(row.get("saldo_actual"))),
            ColumnConfig(key="limite_credito", label="Límite Créd.", width=120, formatter=_format_money),
            ColumnConfig(key="ultimo_movimiento", label="Últ. Movimiento", width=150, formatter=_format_datetime),
            ColumnConfig(key="total_movimientos", label="Movs.", width=80),
            ColumnConfig(key="acciones", label="", width=80, renderer=lambda row: ft.IconButton(
                ft.icons.HISTORY_ROUNDED, 
                tooltip="Ver movimientos",
                icon_color=COLOR_ACCENT,
                on_click=lambda e, eid=row.get("id_entidad_comercial"): ver_movimientos_entidad(e, eid)
            )),
        ],
        data_provider=cuentas_provider,
        advanced_filters=[
            AdvancedFilterControl("tipo_entidad", cc_adv_tipo),
            AdvancedFilterControl("estado", cc_adv_estado),
            AdvancedFilterControl("solo_con_saldo", cc_adv_solo_saldo, getter=lambda c: c.value),
        ],
        id_field="id_entidad_comercial",
        show_inline_controls=False,
        show_mass_actions=False,
        auto_load=True,
        page_size=20,
        show_export_button=True,
        show_export_scope=True,
    )

    cc_stat_deuda = ft.Text("$0", size=20, weight=ft.FontWeight.W_900, color=COLOR_TEXT)
    cc_stat_deudores = ft.Text("0", size=20, weight=ft.FontWeight.W_900, color=COLOR_TEXT)
    cc_stat_cobros = ft.Text("$0", size=20, weight=ft.FontWeight.W_900, color=COLOR_TEXT)
    cc_stat_movs = ft.Text("0", size=20, weight=ft.FontWeight.W_900, color=COLOR_TEXT)

    def refresh_cc_stats():
        if not db: return
        try:
            stats = db.get_stats_cuenta_corriente()
            cc_stat_deuda.value = _format_money(stats.get("deuda_clientes", 0))
            cc_stat_deudores.value = str(stats.get("clientes_deudores", 0))
            cc_stat_cobros.value = _format_money(stats.get("cobros_hoy", 0))
            cc_stat_movs.value = str(stats.get("movimientos_hoy", 0))
            _safe_update_multiple(cc_stat_deuda, cc_stat_deudores, cc_stat_cobros, cc_stat_movs)
        except Exception as e:
            logger.warning(f"Falló al actualizar interfaz: {e}")

    cuentas_view = ft.Column([
        ft.Row([
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.icons.ACCOUNT_BALANCE_ROUNDED, color=COLOR_ERROR, size=24), bgcolor=f"{COLOR_ERROR}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Deuda Clientes", size=12, color=COLOR_TEXT_MUTED), cc_stat_deuda], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.icons.PEOPLE_ALT_ROUNDED, color=COLOR_WARNING, size=24), bgcolor=f"{COLOR_WARNING}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Clientes Deudores", size=12, color=COLOR_TEXT_MUTED), cc_stat_deudores], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.icons.PAYMENTS_ROUNDED, color=COLOR_SUCCESS, size=24), bgcolor=f"{COLOR_SUCCESS}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Cobros Hoy", size=12, color=COLOR_TEXT_MUTED), cc_stat_cobros], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.icons.SWAP_VERT_ROUNDED, color=COLOR_ACCENT, size=24), bgcolor=f"{COLOR_ACCENT}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Movimientos Hoy", size=12, color=COLOR_TEXT_MUTED), cc_stat_movs], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
        ], spacing=20),
        ft.Container(height=10),
        make_card(
            "Cuentas Corrientes",
            "Gestión de saldos de clientes y proveedores.",
            cuentas_table.build(),
            actions=[
                btn_registrar_pago_cc := ft.ElevatedButton(
                    "Registrar Pago",
                    icon=ft.icons.ATTACH_MONEY_ROUNDED,
                    bgcolor=COLOR_SUCCESS,
                    color="#FFFFFF",
                    on_click=open_pago_cc,
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                ),
                btn_ajuste_saldo_cc := ft.ElevatedButton(
                    "Ajuste de Saldo",
                    icon=ft.icons.TUNE_ROUNDED,
                    bgcolor=COLOR_WARNING,
                    color="#FFFFFF",
                    on_click=open_ajuste_cc,
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                ),
            ]
        )
    ], spacing=10, expand=True)

    cuentas_view = ft.Container(
        content=cuentas_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    precios_view = make_card(
        "Listas de Precio", "Definición y actualización de listas.",
        ft.Column([
            ft.Row([nueva_lp_nom, nueva_lp_orden, ft.ElevatedButton("Crear Lista", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_lp, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10),
            precios_table.build()
        ], expand=True, spacing=10)
    )

    nueva_marca.on_submit = agregar_marca  # type: ignore[attr-defined]
    nueva_rubro.on_submit = agregar_rubro  # type: ignore[attr-defined]

    def on_config_tab_change(e):
        if db:
            tab = config_tabs.tabs[config_tabs.selected_index]
            tab_name = getattr(tab, "text", None) or getattr(tab, "label", None) or getattr(tab, "data", None)
            db.log_activity("CONFIG_TAB", "VIEW", detalle={"tab": tab_name})
            
            # Lazy loading: refresh only the table in the current tab
            idx = config_tabs.selected_index
            tab_to_table = {
                1: marcas_table, 2: rubros_table, 3: unidades_table,
                4: provincias_table, 5: localidades_table, 6: civa_table,
                7: tiva_table, 8: depo_table, 9: fpay_table,
                10: ptype_table, 11: dtype_table, 12: mtype_table
            }
            if idx == 0:
                load_sistema_config()
            elif idx in tab_to_table:
                _run_in_background(_run_on_ui, tab_to_table[idx].refresh)
    
    config_tabs = ft.Tabs(
        scrollable=True,
        on_change=on_config_tab_change,
        expand=True,
        height=600, # Force height to debug web view layout issue
        tabs=[
            make_tab(
                text="Sistema",
                content=sistema_tab_content
            ),
            make_tab(
                text="Marcas",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_marca, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_marca, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        marcas_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Rubros",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_rubro, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_rubro, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        rubros_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Unidades",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_uni_nombre, nueva_uni_abr, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_unidad, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        unidades_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Provincias",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_provincia_input, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_provincia, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        provincias_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Localidades",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_loc_nombre, nueva_loc_prov, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_localidad, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        localidades_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Condiciones IVA",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_civa, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_civa, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        civa_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Tipos IVA",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_tiva_porc, nueva_tiva_desc, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_tiva, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        tiva_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Depósitos",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nuevo_depo_nom, nuevo_depo_ubi, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_deposito, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        depo_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Formas Pago",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_fpay, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_fpay, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        fpay_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Tipos Porcentaje",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nuevo_ptype, ft.ElevatedButton("Agregar", height=40, icon=ft.icons.ADD_ROUNDED, on_click=agregar_ptype, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        ptype_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Tipos Documento",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([
                            nuevo_dtype_nom, 
                            nuevo_dtype_clase, 
                            nuevo_dtype_letra, 
                            nuevo_dtype_stock,
                            nuevo_dtype_cta,
                            ft.ElevatedButton("Agregar", icon=ft.icons.ADD_ROUNDED, on_click=agregar_dtype, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))
                        ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        dtype_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
            make_tab(
                text="Tipos Movimiento",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nuevo_mtype_nom, nuevo_mtype_signo, ft.ElevatedButton("Agregar", icon=ft.icons.ADD_ROUNDED, on_click=agregar_mtype, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        mtype_table.build(),
                    ],
                    expand=True, spacing=10,
                ),
            ),
        ],
    )

    # Wire filter controls -> refresh on Enter / change
    def wire_refresh(table: GenericTable, controls: List[ft.Control]) -> None:
        for ctrl in controls:
            if isinstance(ctrl, ft.TextField) and hasattr(ctrl, "on_submit"):
                ctrl.on_submit = lambda e, t=table: t.refresh()  # type: ignore[attr-defined]
                continue
            if hasattr(ctrl, "on_change"):
                ctrl.on_change = lambda e, t=table: t.refresh()  # type: ignore[attr-defined]

    wire_refresh(
        entidades_table,
        [
            entidades_advanced_cuit,
            entidades_advanced_localidad,
            entidades_advanced_provincia,
            entidades_advanced_lista_precio,
            entidades_advanced_activo,
        ],
    )
    wire_refresh(
        articulos_table,
        [
            articulos_advanced_nombre,
            articulos_advanced_marca,
            articulos_advanced_rubro,
            articulos_advanced_proveedor,
            articulos_advanced_ubicacion,
            articulos_advanced_stock_bajo,
        ],
    )
    wire_refresh(
        documentos_summary_table,
        [doc_adv_entidad, doc_adv_tipo, doc_adv_desde, doc_adv_hasta],
    )
    wire_refresh(
        movimientos_table,
        [mov_adv_art, mov_adv_tipo, mov_adv_depo, mov_adv_user, mov_adv_desde, mov_adv_hasta],
    )
    # Note: pagos_table advanced filters redefined inline; wire_live_search handles them.

    # Entidades and Artículos views are already defined above with stats and actions.

    config_view = make_card(
        "Configuración del Sistema",
        "Configura parámetros generales, catálogos y referencias del negocio.",
        config_tabs
    )

    # Component load handled in appropriate place

    # Note: entidades_view and articulos_view are already defined above with stats and actions.
    dashboard_view_component: Optional[DashboardView] = None

    def ensure_dashboard():
        nonlocal dashboard_view_component
        if dashboard_view_component is None and db:
            dashboard_view_component = DashboardView(db, CURRENT_USER_ROLE)
            dashboard_view_component.on_navigate = lambda x: set_view(x)
        return dashboard_view_component

    masivos_view = None

    def ensure_masivos_view():
        nonlocal masivos_view
        if not masivos_view and db:
            masivos_view = MassUpdateView(
                db, 
                show_toast, 
                supplier_loader=supplier_loader, 
                price_list_loader=price_list_loader
            )
        return masivos_view

    # content_holder starts with dashboard_view if possible
    content_holder = ft.Container(
        expand=True, 
        content=ft.ProgressRing(color=COLOR_ACCENT, width=40, height=40, stroke_width=3), 
        alignment=ft.alignment.center
    )
    current_view = {"key": "dashboard"}

    stats_refresh_lock = threading.Lock()

    def refresh_all_stats():
        def _bg_work():
            if not db:
                return
            if not stats_refresh_lock.acquire(blocking=False):
                return
            try:
                # Fetch all stats in one go using the new role-based method
                stats = db.get_full_dashboard_stats(CURRENT_USER_ROLE, force_refresh=True)

                se = stats.get("entidades", {})
                sa = stats.get("stock", {})
                sv = stats.get("ventas", {})
                so = stats.get("sistema", {})
                sm = stats.get("movimientos", {})
                soper = stats.get("operativas", {})
                sf = stats.get("finanzas", {}) if "finanzas" in stats else {}

                v_mes = sv.get('mes_total', 0)
                val_inventario = sa.get('valor_inventario', 0)

                rem_total: Optional[int] = None
                if "remitos_total" in card_registry:
                    try:
                        rem_total = db.count_remitos()
                    except Exception:
                        rem_total = None

                def _apply_stats():
                    try:
                        # Entidades
                        if "entidades_clientes" in card_registry: card_registry["entidades_clientes"].value = f"{se.get('clientes_total', 0):,}"
                        if "entidades_proveedores" in card_registry: card_registry["entidades_proveedores"].value = f"{se.get('proveedores_total', 0):,}"
                        if "entidades_activos" in card_registry: card_registry["entidades_activos"].value = f"{se.get('clientes_total', 0) + se.get('proveedores_total', 0):,}"

                        # Articulos
                        if "articulos_total" in card_registry: card_registry["articulos_total"].value = f"{sa.get('total', 0):,}"
                        if "articulos_bajo_stock" in card_registry: card_registry["articulos_bajo_stock"].value = f"{sa.get('bajo_stock', 0):,}"
                        if "articulos_valor" in card_registry:
                            card_registry["articulos_valor"].value = _format_money(val_inventario)

                        # Facturacion / Ventas
                        if "docs_ventas" in card_registry:
                            card_registry["docs_ventas"].value = _format_money(v_mes) if isinstance(v_mes, (int, float)) else v_mes
                        if "docs_pendientes" in card_registry:
                            card_registry["docs_pendientes"].value = f"{sv.get('docs_pendientes', 0):,}"

                        # Finanzas (if available)
                        if sf:
                            if "docs_compras" in card_registry: card_registry["docs_compras"].value = _format_money(sf.get('egresos_mes', 0))
                            if "pagos_hoy" in card_registry: card_registry["pagos_hoy"].value = _format_money(sf.get('ingresos_hoy', 0))
                            if "pagos_recientes" in card_registry: card_registry["pagos_recientes"].value = f"{sf.get('pagos_recientes', 0):,}"

                        # Usuarios
                        if "usuarios_ultimo" in card_registry: card_registry["usuarios_ultimo"].value = so.get('ultimo_login', "N/A")

                        # Movimientos
                        if "movs_ingresos" in card_registry: card_registry["movs_ingresos"].value = f"{sm.get('ingresos', 0):,}"
                        if "movs_salidas" in card_registry: card_registry["movs_salidas"].value = f"{sm.get('salidas', 0):,}"
                        if "movs_ajustes" in card_registry: card_registry["movs_ajustes"].value = f"{sm.get('ajustes', 0):,}"

                        if "remitos_pend" in card_registry: card_registry["remitos_pend"].value = f"{soper.get('remitos_pend', 0):,}"
                        if "remitos_entregas" in card_registry: card_registry["remitos_entregas"].value = f"{soper.get('entregas_hoy', 0):,}"
                        if rem_total is not None and "remitos_total" in card_registry:
                            card_registry["remitos_total"].value = f"{rem_total:,}"

                        if not window_is_closing:
                            page.update()
                    except (Exception, RuntimeError) as e:
                        if not window_is_closing and db and not db.is_closing:
                            err_msg = str(e).lower()
                            if "'nonetype' object has no attribute 'connection'" in err_msg:
                                return
                            if "content must be visible" not in err_msg and "page is not visible" not in err_msg:
                                print(f"Error refreshing stats: {e}")

                _run_on_ui(_apply_stats)
            except (Exception, RuntimeError) as e:
                # Suppress transient Flet errors like "content must be visible" during transitions
                if not window_is_closing and db and not db.is_closing:
                    err_msg = str(e).lower()
                    # Skip noise during schema sync (pool is None)
                    if "'nonetype' object has no attribute 'connection'" in err_msg:
                        return
                    if "content must be visible" not in err_msg and "page is not visible" not in err_msg:
                        print(f"Error refreshing stats: {e}")
            finally:
                stats_refresh_lock.release()

        # Run in a background thread to avoid UI lag on tab switches
        _run_in_background(_bg_work)

    # Re-declare refresh_all_stats for set_view to use

    # =========================================================================
    # LOGIN VIEW & AUTHENTICATION
    # =========================================================================
    CURRENT_USER_ROLE = "EMPLEADO"  # Default, will be set on login
    monitor_started = False
    admin_export_tables = [
        entidades_table, 
        articulos_table, 
        documentos_summary_table, 
        remitos_table, 
        movimientos_table, 
        pagos_table, 
        cuentas_table,
    ]

    def apply_role_permissions() -> None:
        """Centralizes UI permission enforcement based on CURRENT_USER_ROLE."""
        role_clean = str(CURRENT_USER_ROLE or "").strip().upper()
        is_admin = (role_clean == "ADMIN")
        is_manager = (role_clean == "GERENTE")
        is_privileged = is_admin or is_manager

        # 1. Export Buttons Visibility
        for table in admin_export_tables:
            if table is None:
                continue
            
            # Determine if this specific table should be visible for the current role
            # User specifically asked for 'ADMIN' only for export buttons.
            should_show_export = is_admin
            
            # Update internal flag for future rebuilds
            table.show_export_button = should_show_export
            table.show_export_scope = should_show_export

            # Apply visibility to the actual control and update immediately
            try:
                table.set_export_visibility(should_show_export)
            except Exception as e:
                logger.warning(f"Failed to set export visibility: {e}")

            # 1.1 Enforcement of Interactive Controls for non-admins
            # (EMPLEADO should only see data, not edit/delete mass)
            if hasattr(table, "show_inline_controls"):
                table.show_inline_controls = is_privileged
            if hasattr(table, "show_mass_actions"):
                table.show_mass_actions = is_privileged

        # 2. Side Bar specialized items (already handled in update_nav, but good to ensure)
        update_nav()

        # 3. Action Buttons Visibility
        critical_action_btns = [
            btn_nueva_entidad, 
            btn_nuevo_articulo, 
            btn_nuevo_comprobante, 
            btn_nuevo_pago,
            btn_registrar_pago_cc,
            btn_ajuste_saldo_cc
        ]
        for btn in critical_action_btns:
            if btn:
                btn.visible = is_privileged

        # 5. Commit all visibility changes to the page
        try:
            page.update()
        except Exception as e:
            logger.warning(f"Falló al actualizar page with visibility changes: {e}")
    
    login_email = ft.TextField(
        label="Email o Usuario",
        width=320,
        prefix_icon=ft.icons.EMAIL_ROUNDED,
        border_radius=12,
        filled=True,
        bgcolor="#FFFFFF",
        border_color="#E2E8F0",
        focused_border_color=COLOR_ACCENT,
        text_size=15,
        height=55,
    )
    login_password = ft.TextField(
        label="Contraseña",
        password=True,
        can_reveal_password=True,
        width=320,
        prefix_icon=ft.icons.LOCK_ROUNDED,
        border_radius=12,
        filled=True,
        bgcolor="#FFFFFF",
        border_color="#E2E8F0",
        focused_border_color=COLOR_ACCENT,
        text_size=15,
        height=55,
    )
    login_error = ft.Text("", color=COLOR_ERROR, size=13, visible=False)
    login_loading = ft.ProgressRing(width=20, height=20, stroke_width=2, visible=False)
    
    main_app_container = ft.Container(visible=False, expand=True, left=0, top=0, right=0, bottom=0)
    login_container = ft.Container(visible=True, expand=True, left=0, top=0, right=0, bottom=0)
    
    # Backup Blocking Overlay
    backup_status_title = ft.Text("Verificando Respaldos...", size=24, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)
    backup_status_detail = ft.Text("Esto puede tardar unos segundos...", size=14, color=COLOR_TEXT_MUTED)
    backup_progress_bar = ft.ProgressBar(width=400, color=COLOR_ACCENT, bgcolor="#E2E8F0", value=0)
    backup_type_text = ft.Text("", size=11, weight=ft.FontWeight.BOLD, color="#FFFFFF")
    backup_type_badge = ft.Container(
        content=backup_type_text,
        bgcolor=COLOR_ACCENT,
        padding=ft.padding.symmetric(horizontal=12, vertical=6),
        border_radius=8,
        visible=False
    )
    
    backup_overlay = ft.Container(
        content=ft.Column(
            [
                ft.Container(
                    content=ft.Icon(ft.icons.CLOUD_SYNC_ROUNDED, size=64, color=COLOR_ACCENT),
                    bgcolor=f"{COLOR_ACCENT}15",
                    padding=25,
                    border_radius=30,
                    margin=ft.margin.only(bottom=20)
                ),
                backup_status_title,
                ft.Container(height=10),
                backup_status_detail,
                ft.Container(height=30),
                backup_type_badge,
                ft.Container(height=20),
                backup_progress_bar,
                ft.Container(height=10),
                ft.Text(
                    "CONSERVANDO TU INFORMACIÓN SEGURA", 
                    style=ft.TextStyle(
                        size=10, 
                        weight=ft.FontWeight.BOLD, 
                        color=COLOR_TEXT_MUTED, 
                        letter_spacing=1.5
                    )
                ),
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            alignment=ft.MainAxisAlignment.CENTER,
        ),
        visible=False,
        expand=True,
        bgcolor="#F1F5F9E6", # Slate 100 with high opacity for glass effect
        padding=40,
        alignment=ft.alignment.center,
        left=0, top=0, right=0, bottom=0
    )

    def _set_overlay_state(
        title: str,
        detail: str,
        *,
        progress: Optional[float] = None,
        badge: Optional[str] = None,
        badge_color: Optional[str] = None,
    ) -> None:
        backup_overlay.visible = True
        login_container.disabled = True
        login_container.visible = False
        main_app_container.visible = False
        backup_status_title.value = title
        backup_status_detail.value = detail
        backup_progress_bar.visible = True
        backup_progress_bar.value = progress
        if badge:
            backup_type_text.value = badge
            backup_type_badge.bgcolor = badge_color or COLOR_ACCENT
            backup_type_badge.visible = True
        else:
            backup_type_badge.visible = False
        page.update()

    def _hide_overlay() -> None:
        backup_overlay.visible = False
        backup_progress_bar.value = 0
        backup_type_badge.visible = False
        page.update()

    def run_startup_maintenance(on_success: Callable[[], None]) -> None:
        def _backup_progress(backup_type: str, status: str, current: int, total: int) -> None:
            label = backup_type.upper()
            if status == "running":
                progress = (current - 1) / max(total, 1)
                _run_on_ui(
                    _set_overlay_state,
                    "Ejecutando respaldos pendientes...",
                    f"{label} en progreso",
                    progress=progress,
                    badge=label,
                    badge_color=COLOR_ACCENT,
                )
            elif status == "completed":
                progress = current / max(total, 1)
                _run_on_ui(
                    _set_overlay_state,
                    "Ejecutando respaldos pendientes...",
                    f"{label} completado",
                    progress=progress,
                    badge=label,
                    badge_color=COLOR_SUCCESS,
                )
            elif status == "failed":
                progress = current / max(total, 1)
                _run_on_ui(
                    _set_overlay_state,
                    "Error en respaldos",
                    f"{label} fallido",
                    progress=progress,
                    badge="ERROR",
                    badge_color=COLOR_ERROR,
                )

        def _schema_progress(payload: Dict[str, Any]) -> None:
            phase = payload.get("phase")
            if phase in {"extensions", "schemas"}:
                _run_on_ui(
                    _set_overlay_state,
                    "Actualizando esquema...",
                    payload.get("message", "Sincronizando..."),
                    progress=None,
                    badge="SCHEMA",
                    badge_color=COLOR_INFO,
                )
                return
            if phase in {"tables", "indexes"}:
                current = int(payload.get("current", 0))
                total = int(payload.get("total", 1)) or 1
                progress = current / total if total else 0
                _run_on_ui(
                    _set_overlay_state,
                    "Actualizando esquema...",
                    payload.get("message", "Sincronizando..."),
                    progress=progress,
                    badge="SCHEMA",
                    badge_color=COLOR_INFO,
                )
                return

        def _run() -> None:
            try:
                _run_on_ui(
                    _set_overlay_state,
                    "Preparando sistema...",
                    "Verificando respaldos y esquema...",
                    progress=None,
                )
                # print("DEBUG: Starting startup sequence...", flush=True)

                try:
                    from desktop_app.services.backup_manager import BackupManager
                except ImportError:
                    from services.backup_manager import BackupManager  # type: ignore

                backup_manager = BackupManager(db, pg_bin_path=config.pg_bin_path)
                
                # Limpiar registros de backups cuyos archivos físicos no existen
                # DESACTIVADO: La purga automática en inicio causa falsos positivos si OneDrive/FS está ocupado,
                # lo que borra el registro y dispara el backup de nuevo.
                # try:
                #     purged = backup_manager.purge_invalid_backups()
                #     # if purged > 0:
                #     #     print(f"DEBUG: Se limpiaron {purged} registros de backups inexistentes.", flush=True)
                # except Exception as e:
                #     pass # print(f"DEBUG: Error al purgar backups: {e}", flush=True)

                # print("DEBUG: Checking missed backups...", flush=True)
                missed = backup_manager.check_missed_backups()
                # print(f"DEBUG: Missed backups result: {missed}", flush=True)
                if missed:
                    results = backup_manager.execute_missed_backups(
                        missed,
                        progress_callback=_backup_progress,
                    )
                    if not results or not all(results.values()):
                        _run_on_ui(
                            _set_overlay_state,
                            "Error en respaldos",
                            "Revisa el log antes de continuar.",
                            progress=1.0,
                            badge="ERROR",
                            badge_color=COLOR_ERROR,
                        )
                        return

                # Schema sync moved to application startup (see main())
                # to prevent deadlocks with open DB connections.


                def _finalize():
                    _hide_overlay()
                    login_container.disabled = False
                    login_container.visible = False
                    main_app_container.visible = True
                    page.update()
                    on_success()

                _run_on_ui(_finalize)
            except Exception as exc:
                _run_on_ui(
                    _set_overlay_state,
                    "Error de mantenimiento",
                    str(exc),
                    progress=1.0,
                    badge="ERROR",
                    badge_color=COLOR_ERROR,
                )

        _run_in_background(_run)

    def _complete_login(user: Dict[str, Any], *, mode: str) -> None:
        nonlocal CURRENT_USER_ROLE, current_user, logout_logged, monitor_started
        current_user = user
        CURRENT_USER_ROLE = user.get("rol") or "EMPLEADO"
        logout_logged = False
        mark_activity()
        db.set_context(user["id"], local_ip)

        set_sidebar_session_state(True, user)
        apply_role_permissions()

        def start_background_monitor():
            nonlocal monitor_started
            last_check_wrapper = {"ts": time.time()}

            def background_monitor():
                while not window_is_closing:
                    try:
                        if db and db.current_user_id:
                            tables = [
                                "DOCUMENTO", "PAGO", "ENTIDAD", "ARTICULO", "PAGO_CC", "AJUSTE_CC",
                                "REMITO", "MOVIMIENTO", "USUARIO", "SISTEMA", "CONFIG",
                                "app.documento", "app.pago", "app.entidad_comercial", "app.articulo",
                                "app.remito", "app.movimiento_articulo", "seguridad.usuario", "ref.lista_precio"
                            ]
                            if db.check_recent_activity(last_check_wrapper["ts"], tables):
                                last_check_wrapper["ts"] = time.time()

                                refresh_all_stats()

                                key = current_view.get("key")
                                if key == "entidades":
                                    _run_in_background(_run_on_ui, entidades_table.refresh, silent=True)
                                elif key == "articulos":
                                    _run_in_background(_run_on_ui, articulos_table.refresh, silent=True)
                                elif key == "cuentas":
                                    _run_in_background(_run_on_ui, cuentas_table.refresh, silent=True)
                                    _run_in_background(_run_on_ui, refresh_cc_stats)
                                elif key == "documentos":
                                    _run_in_background(_run_on_ui, documentos_summary_table.refresh, silent=True)
                                elif key == "remitos":
                                    _run_in_background(_run_on_ui, remitos_table.refresh, silent=True)
                                elif key == "movimientos":
                                    _run_in_background(_run_on_ui, movimientos_table.refresh, silent=True)
                                elif key == "pagos":
                                    _run_in_background(_run_on_ui, pagos_table.refresh, silent=True)
                                elif key == "precios":
                                    if hasattr(globals().get("precios_table"), "refresh"):
                                        _run_in_background(_run_on_ui, globals()["precios_table"].refresh, silent=True)
                                elif key == "usuarios":
                                    try:
                                        _run_in_background(_run_on_ui, usuarios_table.refresh)
                                    except Exception as e:
                                        logger.warning(f"Falló al actualizar: {e}")
                                elif key == "dashboard":
                                    if dashboard_view_component:
                                        _run_in_background(dashboard_view_component.request_auto_refresh)
                    except Exception:
                        pass
                    for _ in range(5):
                        if window_is_closing:
                            break
                        time.sleep(1)

            threading.Thread(target=background_monitor, daemon=True).start()

        start_background_monitor()

        db.log_activity("SISTEMA", "LOGIN_OK", detalle={"modo": "BASIC_UI", "usuario": user["nombre"], "acceso": mode})
        try:
            nombre_sistema = db.get_config_value("nombre_sistema")
            if nombre_sistema and nombre_sistema.strip():
                page.title = nombre_sistema
        except Exception:
            pass

        def finalize_login() -> None:
            update_nav()
            set_view("dashboard")
            show_toast(f"Bienvenido, {user['nombre']}", kind="success")
            page.update()

        run_startup_maintenance(finalize_login)

    def do_login(_=None):
        login_error.visible = False
        login_loading.visible = True
        page.update()

        email = login_email.value.strip() if login_email.value else ""
        password = login_password.value if login_password.value else ""

        if not email or not password:
            login_error.value = "Por favor complete todos los campos"
            login_error.visible = True
            login_loading.visible = False
            page.update()
            return

        if db is None:
            login_error.value = f"Error de conexión: {db_error or 'Base de datos no disponible'}"
            login_error.visible = True
            login_loading.visible = False
            page.update()
            return

        user = db.authenticate_user(email, password)
        login_loading.visible = False

        if user is None:
            login_error.value = "Credenciales inválidas. Verifique su email y contraseña."
            login_error.visible = True
            login_password.value = ""
            page.update()
            return

        _complete_login(user, mode="credenciales")

    def do_guest_login(_=None):
        login_error.visible = False
        login_loading.visible = True
        page.update()

        if db is None:
            login_error.value = f"Error de conexión: {db_error or 'Base de datos no disponible'}"
            login_error.visible = True
            login_loading.visible = False
            page.update()
            return

        user = db.authenticate_guest_user()
        login_loading.visible = False

        if user is None:
            login_error.value = "No se pudo iniciar en modo invitado."
            login_error.visible = True
            page.update()
            return

        login_email.value = ""
        login_password.value = ""
        _complete_login(user, mode="invitado")
    
    def do_logout(_=None):
        nonlocal CURRENT_USER_ROLE, current_user, logout_logged
        # Log logout event
        if db and current_user.get("id"):
            nombre = current_user.get("nombre")
            ok = db.log_logout("logout_usuario", usuario=nombre, use_pool=True)
            if not ok:
                ok = db.log_logout("logout_usuario", usuario=nombre, use_pool=False)
            logout_logged = ok
        
        # Clear session
        current_user = {}
        CURRENT_USER_ROLE = "EMPLEADO"
        apply_role_permissions()

        if db:
            db.set_context(None, None)

        # Reset sidebar info
        set_sidebar_session_state(False)
        
        # Reset login fields
        login_email.value = ""
        login_password.value = ""
        login_error.visible = False
        login_loading.visible = False
        
        # Switch to login
        main_app_container.visible = False
        login_container.visible = True
        
        show_toast("Sesión cerrada", kind="info")
        page.update()
    
    login_password.on_submit = do_login
    login_email.on_submit = lambda _: login_password.focus()
    
    login_view = ft.Container(
        expand=True,
        bgcolor="#F1F5F9",
        content=ft.Column(
            [
                ft.Container(height=80),
                ft.Container(
                    padding=40,
                    bgcolor=COLOR_CARD,
                    border_radius=24,
                    width=420,
                    shadow=ft.BoxShadow(
                        blur_radius=60,
                        spread_radius=5,
                        color="#0F172A20",
                        offset=ft.Offset(0, 20),
                    ),
                    content=ft.Column(
                        [
                            login_brand_logo,
                            ft.Container(height=16),
                            login_brand_name,
                            ft.Text("Sistema de Gestión Comercial", size=14, color=COLOR_TEXT_MUTED),
                            ft.Container(height=24),
                            login_email,
                            ft.Container(height=12),
                            login_password,
                            ft.Container(height=8),
                            login_error,
                            ft.Container(height=20),
                            ft.ElevatedButton(
                                content=ft.Row(
                                    [
                                        ft.Text("Iniciar Sesión", size=15, weight=ft.FontWeight.BOLD),
                                        login_loading,
                                    ],
                                    alignment=ft.MainAxisAlignment.CENTER,
                                    spacing=10,
                                ),
                                width=320,
                                height=50,
                                bgcolor=COLOR_ACCENT,
                                color="#FFFFFF",
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=12),
                                    elevation=4,
                                ),
                                on_click=do_login,
                            ),
                            ft.Container(height=10),
                            ft.OutlinedButton(
                                "Iniciar como invitado",
                                icon=ft.icons.PERSON_OUTLINE_ROUNDED,
                                width=320,
                                height=46,
                                on_click=do_guest_login,
                            ),
                        ],
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        spacing=0,
                    ),
                ),
                ft.Container(height=30),
                ft.Text(
                    f"© {datetime.now().year} Nexoryn Tech. Todos los derechos reservados.",
                    size=12,
                    color=COLOR_TEXT_MUTED,
                ),
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            alignment=ft.MainAxisAlignment.START,
        ),
    )
    login_container.content = login_view



    def set_view(key: str) -> None:
        if key in ["usuarios", "backups"] and CURRENT_USER_ROLE != "ADMIN":
            show_toast("Acceso restringido a administradores", kind="error")
            return
            
        if key == "config" and CURRENT_USER_ROLE not in ["ADMIN", "GERENTE"]:
             show_toast("Acceso restringido", kind="error")
             return

        if key == "masivos" and CURRENT_USER_ROLE not in ["ADMIN", "GERENTE"]:
             show_toast("Acceso restringido a Gerencias", kind="error")
             return

        current_view["key"] = key
        
        # Log View Action
        if db:
            db.log_activity("SISTEMA", "NAVEGACION", detalle={"vista": key.upper()})
            refresh_all_stats()

        if key == "dashboard":
            content_holder.content = ensure_dashboard()
        elif key == "entidades":
            content_holder.content = entidades_view
            _reload_entity_dropdowns()
        elif key == "config":
            content_holder.content = config_view
            refresh_loc_provs()
        elif key == "precios":
            content_holder.content = precios_view
        elif key == "usuarios":
            content_holder.content = usuarios_view
        elif key == "backups":
            content_holder.content = backups_view
            # load_backup_data() is now handled by backup_view_component.load_data() or auto_load
        elif key == "articulos":
            content_holder.content = articulos_view
            refresh_articles_catalogs()
        elif key == "documentos":
            content_holder.content = documentos_view
        elif key == "remitos":
            content_holder.content = remitos_view
        elif key == "movimientos":
            content_holder.content = movimientos_view
        elif key == "pagos":
            content_holder.content = pagos_view
        elif key == "cuentas":
            content_holder.content = cuentas_view
            refresh_cc_stats()
        elif key == "masivos":
            content_holder.content = ensure_masivos_view()
            try:
                content_holder.content.load_catalogs()
            except Exception as e:
                logger.warning(f"Operacion fallo: {e}")
        else:
            content_holder.content = articulos_view
        
        update_nav()

        def delayed_update():
            time.sleep(0.1)  # 100ms de retraso
            _run_on_ui(page.update)
        
        _run_in_background(delayed_update)
        
        # Trigger refresh on the target table
        table_map = {
            "entidades": entidades_table,
            "precios": precios_table,
            "usuarios": usuarios_table,
            "documentos": documentos_summary_table,
            "remitos": remitos_table,
            "movimientos": movimientos_table,
            "pagos": pagos_table,
            "articulos": articulos_table,
            "dashboard": ensure_dashboard()
        }

        def safe_table_refresh(tab):
            try:
                time.sleep(0.2)  # Retraso adicional para tablas
                if hasattr(tab, "refresh"):
                    _run_on_ui(tab.refresh)
                elif hasattr(tab, "load_data"):
                    _run_on_ui(tab.load_data)
            except (RuntimeError, Exception):
                pass

        if key == "usuarios":
            def safe_refresh():
                try:
                    _run_on_ui(usuarios_table.refresh)
                except (RuntimeError, Exception):
                    pass
            _run_in_background(safe_refresh)
        elif key == "dashboard":
            if dashboard_view_component:
                dashboard_view_component.role = CURRENT_USER_ROLE
                dashboard_view_component.on_navigate = lambda x: set_view(x)
                dashboard_view_component.load_data()
        elif key in table_map:
            _run_in_background(safe_table_refresh, table_map[key])
        elif key == "config":
            # Initial load for the selected tab only
            try:
                on_config_tab_change(None)
            except Exception as e:
                print(f"Error enitializing config tabs: {e}")
            
            try:
                refresh_loc_provs()
            except Exception as e:
                print(f"Error refreshing locations/provinces: {e}")

    nav_items: Dict[str, ft.Container] = {}
    admin_only_keys = {"usuarios", "backups"}

    def nav_item(key: str, label: str, icon_name: str):
        icon_value = getattr(ft.icons, icon_name, ft.icons.QUESTION_MARK_ROUNDED)
        
        # Start admin-only items as hidden (will be shown after login if ADMIN)
        if key in admin_only_keys:
             is_admin_only = True
        elif key == "config":
             # Config only for Admin and Manager
             # But here we handle visibility logic
             is_admin_only = False # Handled in update_nav
        else:
             is_admin_only = False
        
        item = ft.Container(
            content=ft.Row([
                ft.Icon(icon_value, size=20, color=COLOR_SIDEBAR_TEXT),
                ft.Text(label, size=14, weight=ft.FontWeight.W_500, color=COLOR_SIDEBAR_TEXT),
            ], spacing=12),
            padding=ft.padding.symmetric(horizontal=16, vertical=12),
            border_radius=12,
            on_click=lambda e: set_view(key),
            on_hover=lambda e: on_nav_hover(e, key),
            animate=ft.Animation(200, ft.AnimationCurve.EASE_OUT),
            visible=not is_admin_only,  # Hide admin items initially
        )
        nav_items[key] = item
        return item

    def on_nav_hover(e, key):
        if key == current_view["key"]: return
        e.control.bgcolor = "#1E293B" if e.data == "true" else None
        e.control.update()

    def update_nav() -> None:
        if 'sidebar_list_view' not in locals() and 'sidebar_list_view' not in globals():
            return

        # Define item order
        common_keys = [
            "dashboard", "articulos", "entidades", "documentos", 
            "remitos", "movimientos", "pagos", "cuentas", "precios"
        ]
        
        new_controls = []
        
        # 1. Header Principal
        if 'header_principal' in locals() or 'header_principal' in globals():
            header_principal.visible = True
            new_controls.append(header_principal)
            
        # 2. Common Items
        for key in common_keys:
            if key in nav_items:
                item = nav_items[key]
                item.visible = True
                item.height = None
                
                # Update selection style
                selected = key == current_view["key"]
                item.bgcolor = "#312E81" if selected else None
                try:
                    row = item.content
                    icon = row.controls[0]
                    text = row.controls[1]
                    icon.color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                    text.color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                    text.weight = ft.FontWeight.BOLD if selected else ft.FontWeight.W_500
                except Exception as e:
                    logger.warning(f"Operacion fallo: {e}")
                
                new_controls.append(item)
                
        # 3. Masivos (Special case)
        if "masivos" in nav_items:
            # Only for ADMIN/GERENTE
            if CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]:
                item = nav_items["masivos"]
                item.visible = True
                item.height = None
                
                selected = "masivos" == current_view["key"]
                item.bgcolor = "#312E81" if selected else None
                try:
                    row = item.content
                    icon = row.controls[0]
                    text = row.controls[1]
                    icon.color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                    text.color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                    text.weight = ft.FontWeight.BOLD if selected else ft.FontWeight.W_500
                except Exception as e:
                    logger.warning(f"Operacion fallo: {e}")
                
                new_controls.append(item)

        # 4. Sistema Section
        # Determine strict visibility for system items
        show_config = CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]
        show_admin_items = CURRENT_USER_ROLE == "ADMIN"
        
        has_system_items = show_config or show_admin_items
        
        if has_system_items:
            # Separator
            if 'sistema_separator' in locals() or 'sistema_separator' in globals():
                sistema_separator.visible = True
                sistema_separator.height = 15
                new_controls.append(sistema_separator)
                
            # Header System
            if 'header_sistema' in locals() or 'header_sistema' in globals():
                header_sistema.visible = True
                header_sistema.height = None
                new_controls.append(header_sistema)
            
            # Config
            if show_config and "config" in nav_items:
                item = nav_items["config"]
                item.visible = True
                item.height = None
                selected = "config" == current_view["key"]
                item.bgcolor = "#312E81" if selected else None
                # Style update for config...
                try:
                    row = item.content
                    row.controls[0].color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                    row.controls[1].color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                    row.controls[1].weight = ft.FontWeight.BOLD if selected else ft.FontWeight.W_500
                except Exception as e:
                    logger.warning(f"Operacion fallo: {e}")
                new_controls.append(item)
            
            # Admin Only Items
            admin_keys = ["usuarios", "backups"]
            if show_admin_items:
                for key in admin_keys:
                    if key in nav_items:
                        item = nav_items[key]
                        item.visible = True
                        item.height = None
                        selected = key == current_view["key"]
                        item.bgcolor = "#312E81" if selected else None
                        try:
                            row = item.content
                            row.controls[0].color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                            row.controls[1].color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                            row.controls[1].weight = ft.FontWeight.BOLD if selected else ft.FontWeight.W_500
                        except Exception as e:
                            logger.warning(f"Operacion fallo: {e}")
                        new_controls.append(item)

        # Apply new controls to ListView
        sidebar_list_view.controls = new_controls
        _safe_update_control(sidebar_list_view)
        
        # Also update sidebar container just in case
        _safe_update_control(sidebar)

    # User info display (updated after login)
    sidebar_user_name = ft.Text("", size=12, color=COLOR_SIDEBAR_TEXT, weight=ft.FontWeight.W_500)
    sidebar_user_role = ft.Text("", size=10, color=COLOR_SIDEBAR_TEXT)

    sidebar_user_block = ft.Container(
        content=ft.Row([
            ft.Container(
                width=36, height=36,
                bgcolor="#4F46E5",
                border_radius=18,
                alignment=ft.alignment.center,
                content=ft.Icon(ft.icons.PERSON_ROUNDED, color="#FFFFFF", size=20),
            ),
            ft.Column([
                sidebar_user_name,
                sidebar_user_role,
            ], spacing=0, expand=True),
            ft.IconButton(
                ft.icons.LOGOUT_ROUNDED,
                icon_color="#EF4444",
                icon_size=22,
                tooltip="Cerrar Sesión",
                on_click=do_logout,
            ),
        ], spacing=10),
        padding=ft.padding.symmetric(horizontal=5, vertical=8),
        border_radius=12,
        bgcolor="#1E293B",
        visible=False,
    )

    def set_sidebar_session_state(logged_in: bool, user: Optional[Dict[str, Any]] = None) -> None:
        if logged_in:
            user = user or {}
            display_name = (user.get("nombre") or "").strip()
            if not display_name:
                display_name = (user.get("email") or "").strip()
            if not display_name and user.get("id"):
                display_name = f"ID {user.get('id')}"
            sidebar_user_name.value = display_name
            sidebar_user_role.value = f"Rol: {CURRENT_USER_ROLE or 'EMPLEADO'}"
            sidebar_user_block.visible = True
        else:
            sidebar_user_name.value = ""
            sidebar_user_role.value = ""
            sidebar_user_block.visible = False

        _safe_update_multiple(sidebar_user_name, sidebar_user_role, sidebar_user_block)

    sidebar_list_view = ft.ListView(
        controls=[
            header_principal := ft.Container(
                content=ft.Text("NAVEGACIÓN PRINCIPAL", size=11, weight=ft.FontWeight.W_700, color=COLOR_SIDEBAR_TEXT),
                padding=ft.padding.only(left=16, bottom=5)
            ),
            nav_item("dashboard", "Tablero de Control", "DASHBOARD_ROUNDED"),
            nav_item("articulos", "Inventario", "INVENTORY_2_ROUNDED"),
            nav_item("entidades", "Entidades", "PEOPLE_ALT_ROUNDED"),
            nav_item("documentos", "Comprobantes", "RECEIPT_LONG_ROUNDED"),
            nav_item("remitos", "Remitos", "LOCAL_SHIPPING_ROUNDED"),
            nav_item("movimientos", "Movimientos", "SWAP_HORIZ_ROUNDED"),
            nav_item("pagos", "Caja y Pagos", "ACCOUNT_BALANCE_WALLET_ROUNDED"),
            nav_item("cuentas", "Cuentas Corrientes", "ACCOUNT_BALANCE_ROUNDED"),
            nav_item("precios", "Lista de Precios", "LOCAL_OFFER_ROUNDED"),
            nav_item("masivos", "Actualización Masiva", "PRICE_CHANGE_ROUNDED"),
            
            sistema_separator := ft.Container(height=15),
            header_sistema := ft.Container(
                content=ft.Text("SISTEMA", size=11, weight=ft.FontWeight.W_700, color=COLOR_SIDEBAR_TEXT),
                padding=ft.padding.only(left=16, bottom=5)
            ),
            nav_item("config", "Configuración", "SETTINGS_SUGGEST_ROUNDED"),
            nav_item("usuarios", "Usuarios", "ADMIN_PANEL_SETTINGS_ROUNDED"),
            nav_item("backups", "Respaldos", "CLOUD_SYNC_ROUNDED"),
        ],
        spacing=6,
        padding=ft.padding.only(right=10),
        auto_scroll=False,
    )

    sidebar = ft.Container(
        width=270,
        bgcolor=COLOR_PANEL,
        padding=ft.padding.all(20),
        content=ft.Column(
            [
                ft.Container(
                    content=ft.Row([
                        sidebar_brand_logo,
                        ft.Column([
                            sidebar_brand_name,
                            sidebar_brand_slogan,
                        ], spacing=-2),
                    ], spacing=12),
                    padding=ft.padding.only(bottom=20, top=10, left=16)
                ),
                ft.Container(
                    content=sidebar_list_view,
                    padding=0,
                    expand=True,
                    clip_behavior=ft.ClipBehavior.HARD_EDGE,
                ),
                # Logout section at bottom
                ft.Container(
                    content=ft.Column([
                        ft.Divider(color="#334155", height=1),
                        ft.Container(height=10),
                        sidebar_user_block,
                    ]),
                    padding=ft.padding.only(top=10),
                ),
            ],
            spacing=10,
            expand=True,
        ),
    )

    # Wrap main app in container
    main_app_content = ft.Row(
        [
            sidebar,
            ft.Column(
                [
                    ft.Container(
                        content=content_holder,
                        expand=True,
                        padding=ft.padding.all(30),
                        bgcolor=COLOR_BG,
                    )
                ],
                expand=True,
            ),
        ],
        expand=True,
        spacing=0,
        vertical_alignment=ft.CrossAxisAlignment.STRETCH,
    )
    main_app_container.content = main_app_content

    # Add both login and main containers to page
    root_stack = ft.Stack(
        [
            main_app_container,
            login_container,
            backup_overlay,
        ],
        expand=True,
    )
    activity_detector = ft.GestureDetector(
        content=root_stack,
        on_hover=mark_activity,
        on_tap=mark_activity,
        on_pan_update=mark_activity,
        on_scroll=mark_activity,
        hover_interval=500,
        drag_interval=500,
        expand=True,
    )
    page.add(activity_detector)
    def open_nuevo_comprobante(edit_doc_id=None, copy_doc_id=None):
        db = get_db_or_toast()
        if not db: return

        try:
            tipos = db.fetch_tipos_documento()
            entidades = db.fetch_entities(
                tipo="CLIENTE",
                sorts=COMPROBANTE_ENTITY_SORTS,
                limit=100,
                offset=0,
            )  # Performance limit
            depositos = db.fetch_depositos()
            articulos = db.fetch_articles(
                activo_only=True,
                sorts=COMPROBANTE_ARTICLE_SORTS,
                limit=100,
                offset=0,
            )  # Performance limit with price info
            listas = db.fetch_listas_precio(limit=50)
        except Exception as e:
            show_toast(f"Error cargando datos: {e}", kind="error")
            return

        # Load existing data first to ensure referenced items are available
        doc_data = None
        if edit_doc_id:
            doc_data = db.get_document_full(edit_doc_id)
        elif copy_doc_id:
            doc_data = db.get_document_full(copy_doc_id)

        active_doc_id_ref = {"value": int(edit_doc_id) if edit_doc_id else None}
        current_doc_row_ref: Dict[str, Optional[Dict[str, Any]]] = {"value": None}
        is_read_only_ref = {"value": False}
        btn_add_line: Optional[ft.ElevatedButton] = None
        btn_modal_print: Optional[ft.ElevatedButton] = None
        btn_modal_print_no_prices: Optional[ft.ElevatedButton] = None
        btn_modal_confirm: Optional[ft.ElevatedButton] = None
        btn_modal_afip: Optional[ft.ElevatedButton] = None
        btn_modal_save: Optional[ft.ElevatedButton] = None
        btn_modal_reset: Optional[ft.ElevatedButton] = None
        btn_modal_close: Optional[ft.ElevatedButton] = None
            
        if doc_data:
            # Ensure Entity exists
            eid = doc_data.get("id_entidad_comercial")
            if eid and not any(e["id"] == eid for e in entidades):
                missing_ent = db.get_entity_simple(eid)
                if missing_ent:
                    if not missing_ent.get("activo", True):
                        missing_ent["nombre_completo"] += " (Inactivo)"
                    entidades.append(missing_ent)
                    entidades.sort(key=_entity_comprobante_sort_key)

            # Ensure Price List exists
            lid = doc_data.get("id_lista_precio")
            if lid and not any(l["id"] == lid for l in listas):
                missing_list = db.get_lista_precio_simple(lid)
                if missing_list:
                    if not missing_list.get("activa", True):
                        missing_list["nombre"] += " (Inactiva)"
                    listas.append(missing_list)

            # Ensure Articles and their Price Lists exist in the list
            for item in doc_data.get("items", []):
                # Article
                aid = item["id_articulo"]
                if aid and not any(a["id"] == aid for a in articulos):
                    missing_art = db.get_article_simple(aid)
                    if missing_art:
                        if not missing_art.get("activo", True):
                            missing_art["nombre"] += " (Inactivo)"
                        articulos.append(missing_art)
                    else:
                        # Fallback for migrated articles not in database
                        hist_desc = item.get("descripcion_historica") or "Artículo Desconocido"
                        articulos.append({
                            "id": aid,
                            "nombre": f"{hist_desc} (Migrado)",
                            "activo": False,
                            "porcentaje_iva": item.get("porcentaje_iva", 0)
                        })
                
                # Price List for the item
                # Schema fallback: app.documento_detalle doesn't assume list, 
                # so we use the header's list primarily.
                lid_item = item.get("id_lista_precio") or doc_data.get("id_lista_precio")
                if lid_item and not any(l["id"] == lid_item for l in listas):
                    missing_list_item = db.get_lista_precio_simple(lid_item)
                    if missing_list_item:
                        if not missing_list_item.get("activa", True):
                            missing_list_item["nombre"] += " (Inactiva)"
                        listas.append(missing_list_item)
            
            # Re-sort articles for better UX
            articulos.sort(key=_article_codigo_sort_key)

        
        # Form Fields
        field_fecha = _date_field(page, "Fecha *", width=160)
        field_fecha.value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        field_vto = _date_field(page, "Vencimiento", width=160)
        field_vto.value = ""
        lista_options = [ft.dropdown.Option("", "Automático")] + [ft.dropdown.Option(str(l["id"]), l["nombre"]) for l in listas]
        lista_initial_items = [{"value": l["id"], "label": l["nombre"]} for l in listas]
        field_saldo = ft.Text("", size=12, color=COLOR_ACCENT, weight=ft.FontWeight.W_500)
        comprobante_border_color = "#334155"
        comprobante_focus_color = "#1D4ED8"
        comprobante_selection_color = "#BFDBFE"
        comprobante_bgcolor = "#F8FAFC"
        comprobante_button_border_color = "#64748B"
        comprobante_button_focus_color = "#F59E0B"
        comprobante_button_focus_overlay = "#FDE68A"
        comprobante_button_focus_shadow = "#FCD34D"
        comprobante_async_select_style = {
            "bgcolor": comprobante_bgcolor,
            "border_color": comprobante_border_color,
            "focused_border_color": comprobante_focus_color,
            "border_width": 2,
        }

        def _style_comprobante_control(control: Any) -> None:
            if control is None:
                return
            _maybe_set(control, "border_color", comprobante_border_color)
            _maybe_set(control, "focused_border_color", comprobante_focus_color)
            _maybe_set(control, "border_width", 2)
            _maybe_set(control, "focused_border_width", 2)
            _maybe_set(control, "filled", True)
            _maybe_set(control, "bgcolor", comprobante_bgcolor)
            _maybe_set(control, "cursor_color", comprobante_focus_color)
            _maybe_set(control, "selection_color", comprobante_selection_color)
        
        def _update_entidad_info(e=None, preserve_values=False):
            if dropdown_entidad.value:
                try:
                    ent_id = int(dropdown_entidad.value)
                    # Fetch full entity details to get address and default price list
                    entity = db.fetch_entity_by_id(ent_id) or {}
                    
                    # 1. Update Balance
                    bal = float(entity.get("saldo_cuenta", 0))
                    field_saldo.value = f"Saldo actual: {_format_money(bal)}"
                    if bal < 0:
                        field_saldo.color = COLOR_SUCCESS
                    elif bal > 0:
                        field_saldo.color = COLOR_ERROR
                    else:
                        field_saldo.color = COLOR_TEXT_MUTED
                    
                    if not preserve_values:
                        # 2. auto-select Default Price List (if configured in client)
                        # User requirement: "que la lista global la ponga por default la que tiene el cliente (si es que tiene una asignada, sino que no ponga nada)"
                        if entity.get("id_lista_precio"):
                            dropdown_lista_global.value = str(entity["id_lista_precio"])
                        else:
                            dropdown_lista_global.value = None
                            
                        # 3. Auto-fill Address
                        # User requirement: "en la direccion, lo mismo, que ponga la del cliente (y sino tiene que no ponga nada)"
                        field_direccion.value = entity.get("domicilio") or ""

                    _safe_update_multiple(field_saldo, dropdown_lista_global, field_direccion)
                    
                    # Manual trigger to update existing line items with the new global list
                    try:
                        # We use a lambda to defer execution in case _on_global_list_change is not defined yet,
                        # though in this scope it will be available when the callback runs.
                        if "_on_global_list_change" in locals() or "_on_global_list_change" in globals():
                             _on_global_list_change(None)
                    except Exception:
                        pass
                except Exception as ex:
                    logger.error(f"Error updating entity info: {ex}")
        
        ent_initial_items = [_format_entity_option(e, include_tipo=True) for e in entidades]
        dropdown_entidad = AsyncSelect(
            label="Entidad *",
            loader=comprobante_entity_loader,
            width=500,
            on_change=None,
            initial_items=ent_initial_items,
            keyboard_accessible=True,
            **comprobante_async_select_style,
        )

        deposito_options = [ft.dropdown.Option(str(d["id"]), d["nombre"]) for d in depositos]
        dropdown_deposito = ft.Dropdown(label="Depósito *", options=deposito_options, width=200); _style_input(dropdown_deposito); _style_comprobante_control(dropdown_deposito)
        if depositos:
            dropdown_deposito.value = str(depositos[0]["id"])
        
        # Lista de precios global (opcional, se aplica a todos los ítems)
        dropdown_lista_global = AsyncSelect(
            label="Lista de Precios (Global)", 
            loader=price_list_loader,
            width=500,
            initial_items=lista_initial_items,
            keyboard_accessible=True,
            **comprobante_async_select_style,
        )
        
        field_obs = ft.TextField(label="Observaciones (Internas)", multiline=True, expand=True, height=80); _style_input(field_obs); _maybe_set(field_obs, "shift_enter", True)
        field_direccion = ft.TextField(label="Dirección de Entrega", expand=True); _style_input(field_direccion)
        field_numero = ft.TextField(
            label="Número/Serie",
            width=200,
            hint_text="Automático",
            read_only=True,
            disabled=True,
        )
        _style_input(field_numero)
        field_descuento_global_pct = ft.TextField(label="Desc. Global %", width=130, value=""); _style_input(field_descuento_global_pct)
        field_descuento_global_imp = ft.TextField(label="Desc. Global $", width=130, value=""); _style_input(field_descuento_global_imp)
        field_sena = ft.TextField(label="Seña $", width=120, value="0,00", on_change=lambda _: _recalc_total()); _style_input(field_sena)
        for comprobante_ctrl in [
            field_fecha,
            field_vto,
            field_obs,
            field_direccion,
            field_numero,
            field_descuento_global_pct,
            field_descuento_global_imp,
            field_sena,
        ]:
            _style_comprobante_control(comprobante_ctrl)
        global_discount_mode = {"value": "percentage"}
        global_discount_last_edited = {"value": "percentage"}

        # Filter tipos: NC/ND only allowed if it's a copy of an already "facturado" (with CAE) doc
        # AND the source document was a Factura (not Presupuesto, Remito, etc)
        is_facturado = doc_data and doc_data.get("cae") is not None
        source_is_factura = False
        allowed_tipos = []
        
        # Helper to check if source type was valid for NC/ND
        def _check_source_is_factura(sid):
            found = next((t for t in tipos if str(t["id"]) == str(sid)), None)
            if found:
                n = found["nombre"].upper()
                return "FACTURA" in n or "TICKET" in n
            return False

        if doc_data:
             source_is_factura = _check_source_is_factura(doc_data.get("id_tipo_documento"))

        for t in tipos:
            name = t["nombre"].upper()
            is_nc_nd = "NOTA CREDITO" in name or "NOTA DEBITO" in name
            # Allow NC/ND if the referenced doc (copy) or the current doc (edit) is facturado,
            # OR if we are editing an existing document that is already of this type.
            is_current_type = doc_data and str(t["id"]) == str(doc_data.get("id_tipo_documento"))
            
            if is_nc_nd:
                # ONLY allow if source has CAE AND source was actually a Factura/Ticket
                if (is_facturado and source_is_factura) or is_current_type:
                    allowed_tipos.append(t)
            else:
                allowed_tipos.append(t)

        dropdown_tipo = ft.Dropdown(
            label="Tipo *", 
            options=[ft.dropdown.Option(str(t["id"]), t["nombre"]) for t in allowed_tipos], 
            width=200,
        ); _style_input(dropdown_tipo); _style_comprobante_control(dropdown_tipo)
        
        if doc_data:
            dropdown_tipo.value = str(doc_data["id_tipo_documento"])
            dropdown_entidad.value = str(doc_data["id_entidad_comercial"])
            if depositos:
                # Requisito de UX: siempre iniciar con el primer depósito disponible.
                dropdown_deposito.value = str(depositos[0]["id"])
            field_obs.value = doc_data["observacion"]
            
            if edit_doc_id:
                field_numero.value = doc_data["numero_serie"]
                field_fecha.value = _normalize_datetime_input(doc_data.get("fecha")) or field_fecha.value
            elif copy_doc_id:
                field_obs.value = f"Copia de {doc_data.get('numero_serie','')}. " + (doc_data.get('observacion','') or "")
                field_numero.value = ""
                field_fecha.value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            field_descuento_global_pct.value = normalize_input_value(doc_data.get("descuento_porcentaje", 0), decimals=2, use_grouping=True)
            field_descuento_global_imp.value = normalize_input_value(doc_data.get("descuento_importe", 0), decimals=2, use_grouping=True)
            if _parse_float(field_descuento_global_pct.value, "Desc. Global %") <= 0:
                field_descuento_global_pct.value = ""
            if _parse_float(field_descuento_global_imp.value, "Desc. Global $") <= 0:
                field_descuento_global_imp.value = ""
            if _parse_float(field_descuento_global_pct.value, "Desc. Global %") > 0:
                global_discount_mode["value"] = "percentage"
            elif _parse_float(field_descuento_global_imp.value, "Desc. Global $") > 0:
                global_discount_mode["value"] = "amount"
            global_discount_last_edited["value"] = global_discount_mode["value"]
            field_vto.value = doc_data.get("fecha_vencimiento") or ""
            field_direccion.value = doc_data.get("direccion_entrega", "") or ""
            
            # En edición preservamos la lista histórica del documento;
            # en copia se prioriza la lista configurada en el cliente.
            if edit_doc_id and doc_data.get("id_lista_precio"):
                dropdown_lista_global.value = str(doc_data["id_lista_precio"])
            
            field_sena.value = normalize_input_value(doc_data.get("sena", 0), decimals=2, use_grouping=True)
            
            _update_entidad_info(None, preserve_values=bool(edit_doc_id))
        else:
            if not dropdown_tipo.value and allowed_tipos:
                presupuesto_tipo = next(
                    (t for t in allowed_tipos if "PRESUPUESTO" in str(t.get("nombre", "")).upper()),
                    None,
                )
                dropdown_tipo.value = str((presupuesto_tipo or allowed_tipos[0])["id"])

        # Financial Summary
        manual_mode = ft.Switch(label="Manual", value=False)
        
        sum_subtotal = ft.TextField(value="0,00", width=120, read_only=True, text_align=ft.TextAlign.RIGHT, label="Subtotal")
        sum_iva = ft.TextField(value="0,00", width=100, read_only=True, text_align=ft.TextAlign.RIGHT, label="IVA")
        sum_total = ft.TextField(value="0,00", width=140, read_only=True, text_align=ft.TextAlign.RIGHT, text_style=ft.TextStyle(weight=ft.FontWeight.BOLD, color=COLOR_ACCENT), label="TOTAL")
        sum_saldo = ft.TextField(value="0,00", width=140, read_only=True, text_align=ft.TextAlign.RIGHT, text_style=ft.TextStyle(weight=ft.FontWeight.BOLD, color=COLOR_WARNING), label="SALDO")
        sum_desc_lineas = ft.TextField(value="0,00", width=130, read_only=True, text_align=ft.TextAlign.RIGHT, label="Desc. Líneas $")
        sum_desc_global = ft.TextField(value="0,00", width=130, read_only=True, text_align=ft.TextAlign.RIGHT, label="Desc. Global $")
        for resumen_ctrl in [sum_subtotal, sum_iva, sum_total, sum_saldo, sum_desc_lineas, sum_desc_global]:
            _style_comprobante_control(resumen_ctrl)
        keyboard_nav_state: Dict[str, List[Any]] = {"all": [], "capture": []}
        modal_focus_state: Dict[str, Any] = {
            "current_control": None,
            "cursor_index": None,
            "prev_keyboard_handler": None,
            "keyboard_page_ref": None,
            "tracked_controls": set(),
        }
        shortcut_state: Dict[str, Any] = {
            "f8_pressed": False,
            "last_f8_ts": 0.0,
            "f9_pressed": False,
            "last_f9_ts": 0.0,
            "last_f10_ts": 0.0,
            "f10_pending_token": 0,
            "confirming": False,
        }

        def _control_name(control: Any) -> str:
            return str(type(control).__name__) if control is not None else ""

        def _is_action_button(control: Any) -> bool:
            if control is None:
                return False
            return _control_name(control) in {
                "ElevatedButton",
                "FilledButton",
                "OutlinedButton",
                "TextButton",
                "IconButton",
            }

        control_state_enum = getattr(ft, "ControlState", None)
        control_state_default = getattr(control_state_enum, "DEFAULT", None)
        control_state_focused = getattr(control_state_enum, "FOCUSED", None)

        def _state_value(default_value: Any, focused_value: Any) -> Any:
            if control_state_default is None or control_state_focused is None:
                return default_value
            return {
                control_state_default: default_value,
                control_state_focused: focused_value,
            }

        def _style_comprobante_button_focus(control: Any, *, primary: bool = False) -> None:
            if control is None or not _is_action_button(control):
                return
            base_style = getattr(control, "style", None)
            if not isinstance(base_style, ft.ButtonStyle):
                base_style = ft.ButtonStyle()

            default_side_color = comprobante_button_border_color if not primary else COLOR_ACCENT
            _maybe_set(
                base_style,
                "side",
                _state_value(
                    ft.BorderSide(width=2, color=default_side_color),
                    ft.BorderSide(width=2, color=comprobante_button_focus_color),
                ),
            )
            _maybe_set(
                base_style,
                "overlay_color",
                _state_value("#00000000", comprobante_button_focus_overlay),
            )
            _maybe_set(
                base_style,
                "shadow_color",
                _state_value("#00000000", comprobante_button_focus_shadow),
            )
            _maybe_set(control, "style", base_style)

        def _is_focus_eligible(control: Any) -> bool:
            if control is None:
                return False
            if hasattr(control, "visible") and getattr(control, "visible") is False:
                return False
            if hasattr(control, "disabled") and bool(getattr(control, "disabled")):
                return False
            if not _is_action_button(control) and hasattr(control, "read_only") and bool(getattr(control, "read_only")):
                return False
            return True

        def _set_focus_tab_index(control: Any, tab_index: int) -> None:
            if control is None:
                return
            if isinstance(control, AsyncSelect) and hasattr(control, "set_tab_index"):
                try:
                    control.set_tab_index(tab_index)
                    return
                except Exception:
                    pass
            _maybe_set(control, "tab_index", tab_index)

        def _focus_control(control: Any) -> bool:
            def _remember_focus_target(target: Any) -> None:
                modal_focus_state["current_control"] = target
                ordered_controls = keyboard_nav_state.get("all", [])
                if target in ordered_controls:
                    modal_focus_state["cursor_index"] = ordered_controls.index(target)

            if control is None:
                return False
            if isinstance(control, AsyncSelect):
                try:
                    control.focus()
                    _remember_focus_target(control)
                    return True
                except Exception:
                    return False
            focus_fn = getattr(control, "focus", None)
            if callable(focus_fn):
                try:
                    focus_fn()
                    _remember_focus_target(control)
                    return True
                except Exception:
                    pass
            if hasattr(page, "set_focus"):
                try:
                    page.set_focus(control)
                    _remember_focus_target(control)
                    return True
                except Exception:
                    pass
            return False

        def _is_modal_keydown_event(event: Any) -> bool:
            event_type = str(
                getattr(event, "event_type", "") or getattr(event, "type", "") or getattr(event, "data", "")
            ).strip().lower()
            if not event_type:
                return True
            if event_type in {"keyup", "up", "key_up"}:
                return False
            if event_type in {"keydown", "down", "key_down"}:
                return True
            return "up" not in event_type

        def _forward_modal_previous_keyboard_handler(event: Any) -> None:
            previous_handler = modal_focus_state.get("prev_keyboard_handler")
            if callable(previous_handler):
                try:
                    previous_handler(event)
                except Exception:
                    logger.debug("Fallo delegando keyboard handler previo del modal", exc_info=True)

        def _is_own_modal_keyboard_handler(handler: Any) -> bool:
            if handler is None:
                return False
            if handler is _on_modal_keyboard_event:
                return True
            return (
                getattr(handler, "__self__", None) is None
                and getattr(handler, "__name__", "") == "_on_modal_keyboard_event"
            )

        def _bind_modal_focus_tracker(control: Any) -> None:
            if control is None:
                return
            tracked_controls = modal_focus_state.get("tracked_controls")
            if not isinstance(tracked_controls, set):
                tracked_controls = set()
                modal_focus_state["tracked_controls"] = tracked_controls
            control_id = id(control)
            if control_id in tracked_controls:
                return
            if not hasattr(control, "on_focus"):
                return
            original_on_focus = getattr(control, "on_focus", None)

            def _tracked_focus(event: Any, ctrl: Any = control, original: Any = original_on_focus) -> None:
                modal_focus_state["current_control"] = ctrl
                ordered_controls = keyboard_nav_state.get("all", [])
                if ctrl in ordered_controls:
                    modal_focus_state["cursor_index"] = ordered_controls.index(ctrl)
                if callable(original):
                    try:
                        original(event)
                    except Exception:
                        logger.debug("Fallo handler original on_focus en modal comprobante", exc_info=True)

            try:
                _maybe_set(control, "on_focus", _tracked_focus)
                tracked_controls.add(control_id)
            except Exception:
                pass

        def _list_line_rows() -> List[Any]:
            try:
                return list(lines_container.controls)
            except Exception:
                return []

        def _any_comprobante_async_select_open() -> bool:
            tracked_selects: List[Any] = [dropdown_entidad, dropdown_lista_global]
            for row in _list_line_rows():
                row_map = getattr(row, "data", None) or {}
                tracked_selects.extend([row_map.get("art_drop"), row_map.get("lista_drop")])

            for control in tracked_selects:
                dialog = getattr(control, "_dialog", None)
                if dialog is not None and bool(getattr(dialog, "visible", False)):
                    return True
            return False

        def _modal_action_buttons_in_order() -> List[Any]:
            return [
                btn_modal_print,
                btn_modal_print_no_prices,
                btn_modal_confirm,
                btn_modal_afip,
                btn_modal_reset,
                btn_modal_close,
                btn_modal_save,
            ]

        def _get_preferred_action_button() -> Optional[Any]:
            if _is_focus_eligible(btn_modal_save):
                return btn_modal_save
            for control in _modal_action_buttons_in_order():
                if _is_focus_eligible(control):
                    return control
            return None

        def _style_comprobante_action_buttons() -> None:
            _style_comprobante_button_focus(btn_add_line)
            _style_comprobante_button_focus(btn_modal_print)
            _style_comprobante_button_focus(btn_modal_print_no_prices)
            _style_comprobante_button_focus(btn_modal_confirm)
            _style_comprobante_button_focus(btn_modal_afip)
            _style_comprobante_button_focus(btn_modal_reset)
            _style_comprobante_button_focus(btn_modal_close)
            _style_comprobante_button_focus(btn_modal_save, primary=True)

            for row in _list_line_rows():
                row_map = getattr(row, "data", None) or {}
                _style_comprobante_button_focus(row_map.get("delete_btn"))

        def _is_placeholder_line_row(row_map: Dict[str, Any]) -> bool:
            art_drop_ctrl = row_map.get("art_drop")
            lista_drop_ctrl = row_map.get("lista_drop")
            cant_field_ctrl = row_map.get("cant_field")
            price_field_ctrl = row_map.get("price_field")
            iva_field_ctrl = row_map.get("iva_field")
            desc_pct_ctrl = row_map.get("desc_pct_field")
            desc_imp_ctrl = row_map.get("desc_imp_field")

            art_val = str(getattr(art_drop_ctrl, "value", "") or "").strip()
            lista_val = str(getattr(lista_drop_ctrl, "value", "") or "").strip()
            cant_val = str(getattr(cant_field_ctrl, "value", "") or "").strip()
            price_val = str(getattr(price_field_ctrl, "value", "") or "").strip()
            iva_val = str(getattr(iva_field_ctrl, "value", "") or "").strip()
            desc_pct_val = str(getattr(desc_pct_ctrl, "value", "") or "").strip()
            desc_imp_val = str(getattr(desc_imp_ctrl, "value", "") or "").strip()

            return (
                art_val == ""
                and lista_val in {"", "0"}
                and cant_val in {"", "0", "0,00", "0.00", "1", "1,00", "1.00"}
                and price_val in {"", "0", "0,00", "0.00"}
                and iva_val in {"", "0", "0,00", "0.00"}
                and desc_pct_val in {"", "0", "0,00", "0.00"}
                and desc_imp_val in {"", "0", "0,00", "0.00"}
            )

        def _resolve_primary_line_article_focus() -> Optional[Any]:
            fallback_control: Optional[Any] = None
            for row in _list_line_rows():
                row_map = getattr(row, "data", None) or {}
                art_control = row_map.get("art_drop")
                if not _is_focus_eligible(art_control):
                    continue
                if fallback_control is None:
                    fallback_control = art_control
                if _is_placeholder_line_row(row_map):
                    return art_control
            return fallback_control

        def _resolve_next_line_article_focus(current_row: Any) -> Optional[Any]:
            rows = _list_line_rows()
            if not rows:
                return None
            try:
                current_index = rows.index(current_row)
            except ValueError:
                return _resolve_primary_line_article_focus()

            for next_row in rows[current_index + 1 :]:
                next_map = getattr(next_row, "data", None) or {}
                next_art_control = next_map.get("art_drop")
                if _is_focus_eligible(next_art_control):
                    return next_art_control
            return _resolve_primary_line_article_focus()

        def _refresh_keyboard_navigation_order() -> None:
            _style_comprobante_action_buttons()
            ordered_controls: List[Any] = []
            placeholder_line_control_ids: set = set()

            def _append(control: Any) -> None:
                if _is_focus_eligible(control):
                    ordered_controls.append(control)

            # Header
            for control in [
                field_fecha,
                field_vto,
                dropdown_tipo,
                dropdown_entidad,
                dropdown_lista_global,
                dropdown_deposito,
                field_numero,
                field_sena,
                field_obs,
                field_direccion,
            ]:
                _append(control)

            # Dynamic lines
            for row in _list_line_rows():
                row_map = getattr(row, "data", None) or {}
                is_placeholder_row = _is_placeholder_line_row(row_map)
                for key in ("art_drop", "lista_drop", "cant_field", "price_field", "iva_field", "desc_pct_field", "desc_imp_field", "delete_btn"):
                    line_control = row_map.get(key)
                    _append(line_control)
                    if is_placeholder_row and line_control is not None:
                        placeholder_line_control_ids.add(id(line_control))

            # Add line button (visually right after the items grid)
            _append(btn_add_line)

            # Footer
            for control in [
                field_descuento_global_pct,
                field_descuento_global_imp,
                manual_mode,
                sum_subtotal,
                sum_iva,
                sum_total,
            ]:
                _append(control)

            # Actions
            for control in [
                btn_modal_print,
                btn_modal_print_no_prices,
                btn_modal_confirm,
                btn_modal_afip,
                btn_modal_reset,
                btn_modal_close,
                btn_modal_save,
            ]:
                _append(control)

            keyboard_nav_state["all"] = ordered_controls
            current_control = modal_focus_state.get("current_control")
            if current_control in ordered_controls:
                modal_focus_state["cursor_index"] = ordered_controls.index(current_control)
            else:
                cursor_index = modal_focus_state.get("cursor_index")
                if isinstance(cursor_index, int):
                    if ordered_controls:
                        modal_focus_state["cursor_index"] = max(0, min(cursor_index, len(ordered_controls) - 1))
                    else:
                        modal_focus_state["cursor_index"] = None
            capture_controls = [
                ctrl
                for ctrl in ordered_controls
                if not _is_action_button(ctrl) and id(ctrl) not in placeholder_line_control_ids
            ]
            if not capture_controls:
                capture_controls = [ctrl for ctrl in ordered_controls if not _is_action_button(ctrl)]
            keyboard_nav_state["capture"] = capture_controls

            for index, control in enumerate(ordered_controls, start=1):
                _set_focus_tab_index(control, index)
                _bind_modal_focus_tracker(control)

        def _focus_next_modal_control(reverse: bool = False) -> bool:
            _refresh_keyboard_navigation_order()
            ordered_controls = keyboard_nav_state.get("all", [])
            if not ordered_controls:
                return False

            current_control = modal_focus_state.get("current_control")
            current_index: Optional[int] = None
            if current_control in ordered_controls:
                current_index = ordered_controls.index(current_control)
            else:
                cursor_index = modal_focus_state.get("cursor_index")
                if isinstance(cursor_index, int) and 0 <= cursor_index < len(ordered_controls):
                    current_index = cursor_index

            delta = -1 if reverse else 1
            if current_index is None:
                # If we don't have a reliable current index, start from the edge
                # and let the circular scan find the first focusable target.
                current_index = 0 if reverse else -1

            controls_count = len(ordered_controls)
            for step in range(1, controls_count + 1):
                target_index = (current_index + (delta * step)) % controls_count
                target = ordered_controls[target_index]
                if _focus_control(target):
                    return True
            return False

        def _focus_next_capture_from(current_control: Any) -> bool:
            _refresh_keyboard_navigation_order()
            capture_controls = keyboard_nav_state.get("capture", [])
            if not capture_controls:
                action_control = _get_preferred_action_button()
                if action_control is not None:
                    return _focus_control(action_control)
                return False
            if current_control in capture_controls:
                current_index = capture_controls.index(current_control)
                is_last_capture = current_index >= len(capture_controls) - 1
                if is_last_capture:
                    action_control = _get_preferred_action_button()
                    if action_control is not None:
                        return _focus_control(action_control)
                next_control = capture_controls[(current_index + 1) % len(capture_controls)]
            else:
                next_control = capture_controls[0]
            return _focus_control(next_control)

        def _on_modal_keyboard_event(event: Any) -> None:
            if not form_dialog.visible:
                _forward_modal_previous_keyboard_handler(event)
                return

            key = str(getattr(event, "key", "") or "").strip().lower().replace("_", " ").replace("-", " ")
            if key in {"f10"} and _is_keyup_event(event):
                _cancel_windows_menu_mode_debounced()
                return
            if key in {"f8", "f9"} and not _is_modal_keydown_event(event):
                if key == "f8":
                    shortcut_state["f8_pressed"] = False
                else:
                    shortcut_state["f9_pressed"] = False
                _forward_modal_previous_keyboard_handler(event)
                return

            if not _is_modal_keydown_event(event):
                _forward_modal_previous_keyboard_handler(event)
                return

            if key in {"tab"}:
                shift_raw = getattr(event, "shift", False)
                if isinstance(shift_raw, str):
                    reverse = shift_raw.strip().lower() in {"true", "1", "yes"}
                else:
                    reverse = bool(shift_raw)
                if _any_comprobante_async_select_open():
                    return
                _focus_next_modal_control(reverse=reverse)
                return

            if key in {"esc", "escape"}:
                _close_comprobante_form(None)
                return

            if key in {"f12"}:
                _save()
                return

            if key in {"f10"}:
                if _confirm_dialog_from_shortcut():
                    shortcut_state["f10_pending_token"] = 0
                    shortcut_state["last_f10_ts"] = 0.0
                    return
                repeat_raw = getattr(event, "repeat", False)
                if isinstance(repeat_raw, str):
                    is_repeat = repeat_raw.strip().lower() in {"true", "1", "yes"}
                else:
                    is_repeat = bool(repeat_raw)
                if is_repeat:
                    return
                # Open confirmation immediately on F10 to avoid the delay window
                # where Windows may capture F10 for the native title/menu bar.
                shortcut_state["last_f10_ts"] = 0.0
                shortcut_state["f10_pending_token"] = 0
                _confirm_current_document(force_direct=False)
                _cancel_windows_menu_mode_debounced()
                return

            if key in {"f11"}:
                _reset_comprobante_form()
                return

            if key in {"f8"}:
                now_ts = time.monotonic()
                last_ts = float(shortcut_state.get("last_f8_ts") or 0.0)
                if shortcut_state.get("f8_pressed") and (now_ts - last_ts) < 0.45:
                    return
                repeat_raw = getattr(event, "repeat", False)
                if isinstance(repeat_raw, str):
                    is_repeat = repeat_raw.strip().lower() in {"true", "1", "yes"}
                else:
                    is_repeat = bool(repeat_raw)
                if is_repeat:
                    return
                shortcut_state["f8_pressed"] = True
                shortcut_state["last_f8_ts"] = now_ts
                _print_current_document_direct(include_prices=False, copies=1)
                return

            if key in {"f9"}:
                now_ts = time.monotonic()
                last_ts = float(shortcut_state.get("last_f9_ts") or 0.0)
                if shortcut_state.get("f9_pressed") and (now_ts - last_ts) < 0.45:
                    return
                repeat_raw = getattr(event, "repeat", False)
                if isinstance(repeat_raw, str):
                    is_repeat = repeat_raw.strip().lower() in {"true", "1", "yes"}
                else:
                    is_repeat = bool(repeat_raw)
                if is_repeat:
                    return
                shortcut_state["f9_pressed"] = True
                shortcut_state["last_f9_ts"] = now_ts
                _print_current_document_direct(include_prices=True, copies=1)
                return

            _forward_modal_previous_keyboard_handler(event)

        def _install_modal_keyboard_handler() -> None:
            current_handler = getattr(page, "on_keyboard_event", None)
            if not _is_own_modal_keyboard_handler(current_handler):
                modal_focus_state["prev_keyboard_handler"] = current_handler
            modal_focus_state["keyboard_page_ref"] = page
            _maybe_set(page, "on_keyboard_event", _on_modal_keyboard_event)

        def _restore_modal_keyboard_handler() -> None:
            keyboard_page = modal_focus_state.get("keyboard_page_ref")
            if keyboard_page is None:
                modal_focus_state["prev_keyboard_handler"] = None
                return
            try:
                current_handler = getattr(keyboard_page, "on_keyboard_event", None)
                if _is_own_modal_keyboard_handler(current_handler):
                    _maybe_set(keyboard_page, "on_keyboard_event", modal_focus_state.get("prev_keyboard_handler"))
            except Exception:
                logger.debug("No se pudo restaurar keyboard handler del modal comprobante", exc_info=True)
            finally:
                modal_focus_state["keyboard_page_ref"] = None
                modal_focus_state["prev_keyboard_handler"] = None

        def _chain_handler_and_focus(handler: Optional[Callable[[Any], Any]], current_control: Any) -> Callable[[Any], None]:
            def _wrapped(event: Any) -> None:
                result: Any = None
                if callable(handler):
                    try:
                        result = handler(event)
                    except Exception as exc:
                        logger.warning(f"Fallo al ejecutar handler de teclado: {exc}")
                        return
                if result is False:
                    return
                _focus_next_capture_from(current_control)

            return _wrapped

        if doc_data:
            sum_subtotal.value = normalize_input_value(doc_data.get("total", 0), decimals=2, use_grouping=True)
            sum_iva.value = normalize_input_value(0, decimals=2, use_grouping=True)
            sum_total.value = normalize_input_value(doc_data.get("total", 0), decimals=2, use_grouping=True)
            sum_desc_global.value = normalize_input_value(doc_data.get("descuento_importe", 0), decimals=2, use_grouping=True)

        def _dec_to_input(value: Any, decimals: int = 2, use_grouping: bool = False) -> str:
            return normalize_input_value(quantize_2(to_decimal(value)), decimals=decimals, use_grouping=use_grouping)

        def _dec_to_input_or_blank(
            value: Any,
            decimals: int = 2,
            use_grouping: bool = False,
        ) -> str:
            normalized_value = quantize_2(to_decimal(value))
            if normalized_value == to_decimal("0"):
                return ""
            return normalize_input_value(normalized_value, decimals=decimals, use_grouping=use_grouping)

        def _normalize_field_numeric(field: Optional[ft.TextField], decimals: int = 2, use_grouping: bool = True) -> None:
            if not field:
                return
            normalized = normalize_input_value(field.value, decimals=decimals, use_grouping=use_grouping)
            field.value = normalized if normalized else normalize_input_value("0", decimals=decimals, use_grouping=use_grouping)
            _safe_update_control(field)

        def _normalize_field_numeric_preserve_blank_zero(
            field: Optional[ft.TextField],
            decimals: int = 2,
            use_grouping: bool = True,
        ) -> None:
            if not field:
                return
            raw_value = str(field.value or "").strip()
            if not raw_value:
                field.value = ""
                _safe_update_control(field)
                return

            normalized = normalize_input_value(raw_value, decimals=decimals, use_grouping=use_grouping)
            if not normalized:
                field.value = ""
                _safe_update_control(field)
                return

            parsed = parse_locale_number(normalized)
            field.value = "" if parsed is not None and parsed == 0 else normalized
            _safe_update_control(field)

        def _normalize_quantity_field(field: Optional[ft.TextField], *, label: str = "Cantidad") -> int:
            if not field:
                raise ValueError(f"El campo '{label}' es obligatorio.")
            qty_int = _parse_int_quantity(field.value, label)
            field.value = normalize_input_value(qty_int, decimals=0, use_grouping=False) or "0"
            _safe_update_control(field)
            return qty_int

        def _base_neto_lineas() -> Any:
            subtotal_neto_lineas = to_decimal("0")
            for row in lines_container.controls:
                row_map = row.data or {}
                try:
                    cantidad_val = to_decimal(_parse_int_quantity(row_map["cant_field"].value, "Cantidad"))
                    precio_val = to_decimal(_parse_float(row_map["price_field"].value, "Precio"))
                    d_pct = row_map.get("desc_pct_field").value
                    d_imp = row_map.get("desc_imp_field").value
                    d_mode = (row_map.get("discount_mode_ref") or {}).get("value", "percentage")
                    base_line = cantidad_val * precio_val
                    _, line_imp = normalize_discount_pair(
                        base_amount=base_line,
                        descuento_porcentaje=d_pct,
                        descuento_importe=d_imp,
                        mode=d_mode if d_mode in ("percentage", "amount") else "percentage",
                    )
                    line_sign = to_decimal("1") if base_line >= to_decimal("0") else to_decimal("-1")
                    subtotal_neto_lineas += base_line - (line_sign * line_imp)
                except Exception:
                    continue
            return subtotal_neto_lineas

        def _discount_limit_message(
            *,
            mode: Any,
            descuento_porcentaje: Any,
            descuento_importe: Any,
            max_importe: Any,
            scope_label: str,
        ) -> Optional[str]:
            mode_norm = mode if mode in ("percentage", "amount") else "percentage"
            pct_val = max(to_decimal("0"), to_decimal(descuento_porcentaje))
            imp_val = max(to_decimal("0"), to_decimal(descuento_importe))
            max_importe_val = max(to_decimal("0"), abs(to_decimal(max_importe)))

            if mode_norm == "percentage" and pct_val > to_decimal("100"):
                return f"{scope_label}: el descuento no puede superar el 100%."

            if mode_norm == "amount" and imp_val > max_importe_val:
                max_text = _dec_to_input(max_importe_val, use_grouping=True)
                return f"{scope_label}: el descuento no puede ser mayor al precio ({max_text})."

            return None

        def _resolve_global_discount_mode_for_commit(preferred_mode: str) -> str:
            current_mode = (
                global_discount_mode["value"]
                if global_discount_mode["value"] in ("percentage", "amount")
                else "percentage"
            )
            if preferred_mode not in ("percentage", "amount"):
                return current_mode
            if current_mode == preferred_mode:
                return current_mode

            base_for_global = _base_neto_lineas()
            pct_current, imp_current = normalize_discount_pair(
                base_amount=base_for_global,
                descuento_porcentaje=field_descuento_global_pct.value,
                descuento_importe=field_descuento_global_imp.value,
                mode="amount" if current_mode == "amount" else "percentage",
            )
            pct_entered = max(to_decimal("0"), to_decimal(field_descuento_global_pct.value))
            imp_entered = max(to_decimal("0"), to_decimal(field_descuento_global_imp.value))
            epsilon = to_decimal("0.0001")

            if preferred_mode == "percentage" and abs(pct_entered - pct_current) > epsilon:
                return "percentage"
            if preferred_mode == "amount" and abs(imp_entered - imp_current) > epsilon:
                return "amount"
            return current_mode

        def _sync_global_discount_pair_from_mode(
            active_field: Optional[ft.TextField] = None,
            normalize_active: bool = False,
        ) -> None:
            base_for_global = _base_neto_lineas()
            mode = global_discount_mode["value"]
            pct_norm, imp_norm = normalize_discount_pair(
                base_amount=base_for_global,
                descuento_porcentaje=field_descuento_global_pct.value,
                descuento_importe=field_descuento_global_imp.value,
                mode="amount" if mode == "amount" else "percentage",
            )
            if active_field is not field_descuento_global_pct or normalize_active:
                field_descuento_global_pct.value = _dec_to_input_or_blank(pct_norm, use_grouping=True)
                _safe_update_control(field_descuento_global_pct)
            if active_field is not field_descuento_global_imp or normalize_active:
                field_descuento_global_imp.value = _dec_to_input_or_blank(imp_norm, use_grouping=True)
                _safe_update_control(field_descuento_global_imp)
        
        def _recalc_total(active_field: Optional[ft.TextField] = None):
            if manual_mode.value: return # Don't overwrite manual edits

            controls_to_update: List[Any] = []

            def _set_value_if_changed(control: Any, new_value: str) -> None:
                if control is None:
                    return
                if str(getattr(control, "value", "")) == str(new_value):
                    return
                control.value = new_value
                controls_to_update.append(control)

            calc_items = []
            row_refs = []
            for row in lines_container.controls:
                row_map = row.data or {}
                try:
                    c_cant = _parse_int_quantity(row_map["cant_field"].value, "Cantidad")
                    c_price = _parse_float(row_map["price_field"].value, "Precio")
                    c_iva = _parse_float(row_map["iva_field"].value, "IVA")
                    d_pct = _parse_float(row_map["desc_pct_field"].value, "Desc. %")
                    d_imp = _parse_float(row_map["desc_imp_field"].value, "Desc. $")
                except Exception:
                    continue
                fiscal_iva_ref = row_map.get("fiscal_iva_rate_ref") or {}
                fiscal_iva_rate = fiscal_iva_ref.get("value", 0)
                calc_items.append({
                    "cantidad": c_cant,
                    "precio_unitario": c_price,
                    "porcentaje_iva": c_iva,
                    "porcentaje_iva_fiscal": fiscal_iva_rate,
                    "descuento_porcentaje": d_pct,
                    "descuento_importe": d_imp,
                    "descuento_mode": (row_map.get("discount_mode_ref") or {}).get("value", "percentage"),
                })
                row_refs.append(row_map)

            try:
                desc_global_pct_val = _parse_float(field_descuento_global_pct.value, "Desc. Global %")
            except Exception:
                desc_global_pct_val = 0.0
            try:
                desc_global_imp_val = _parse_float(field_descuento_global_imp.value, "Desc. Global $")
            except Exception:
                desc_global_imp_val = 0.0
            try:
                sena_val = _parse_float(field_sena.value, "Seña")
            except Exception:
                sena_val = 0.0

            result = calculate_document_totals(
                items=calc_items,
                descuento_global_porcentaje=desc_global_pct_val,
                descuento_global_importe=desc_global_imp_val,
                descuento_global_mode="amount" if global_discount_mode["value"] == "amount" else "percentage",
                sena=sena_val,
                pricing_mode="tax_included",
            )
            has_manual_visible_iva = any(
                to_decimal((item or {}).get("porcentaje_iva", 0)) > to_decimal("0")
                for item in calc_items
            )

            for i, priced_item in enumerate(result["items"]):
                if i >= len(row_refs):
                    break
                _set_value_if_changed(
                    row_refs[i]["total_field"],
                    _dec_to_input(priced_item["total_linea"], use_grouping=True),
                )
            try:
                if active_field is not field_descuento_global_pct:
                    _set_value_if_changed(
                        field_descuento_global_pct,
                        _dec_to_input_or_blank(result["descuento_global_porcentaje"], use_grouping=True),
                    )
                if active_field is not field_descuento_global_imp:
                    _set_value_if_changed(
                        field_descuento_global_imp,
                        _dec_to_input_or_blank(result["descuento_global_importe"], use_grouping=True),
                    )
                _set_value_if_changed(sum_desc_lineas, _dec_to_input(result["descuento_lineas_importe"], use_grouping=True))
                _set_value_if_changed(sum_desc_global, _dec_to_input(result["descuento_global_importe"], use_grouping=True))
                if has_manual_visible_iva:
                    _set_value_if_changed(sum_subtotal, _dec_to_input(result["neto"], use_grouping=True))
                    _set_value_if_changed(sum_iva, _dec_to_input(result["iva_total"], use_grouping=True))
                else:
                    _set_value_if_changed(sum_subtotal, _dec_to_input(result["ui_subtotal"], use_grouping=True))
                    _set_value_if_changed(sum_iva, _dec_to_input(0, use_grouping=True))
                _set_value_if_changed(sum_total, _dec_to_input(result["total"], use_grouping=True))
                _set_value_if_changed(sum_saldo, _dec_to_input(result["saldo"], use_grouping=True))
            except Exception:
                pass
            
            if controls_to_update:
                _safe_update_multiple(*controls_to_update)

        def toggle_manual(e):
             is_manual = manual_mode.value
             sum_subtotal.read_only = not is_manual
             sum_iva.read_only = not is_manual
             sum_total.read_only = not is_manual
             if not is_manual:
                 _recalc_total() # Restore auto values
             else:
                 _safe_update_multiple(sum_subtotal, sum_iva, sum_total, manual_mode)
             _refresh_keyboard_navigation_order()

        def _on_global_desc_pct_change(_):
            global_discount_mode["value"] = "percentage"
            global_discount_last_edited["value"] = "percentage"
            _sync_global_discount_pair_from_mode(active_field=field_descuento_global_pct)
            _recalc_total(active_field=field_descuento_global_pct)

        def _on_global_desc_imp_change(_):
            global_discount_mode["value"] = "amount"
            global_discount_last_edited["value"] = "amount"
            _sync_global_discount_pair_from_mode(active_field=field_descuento_global_imp)
            _recalc_total(active_field=field_descuento_global_imp)

        def _on_global_desc_pct_commit(_):
            preferred_mode = (
                global_discount_last_edited["value"]
                if global_discount_last_edited.get("value") in ("percentage", "amount")
                else "percentage"
            )
            global_discount_mode["value"] = _resolve_global_discount_mode_for_commit(preferred_mode)
            mode = global_discount_mode["value"]
            limit_msg = _discount_limit_message(
                mode=mode,
                descuento_porcentaje=field_descuento_global_pct.value,
                descuento_importe=field_descuento_global_imp.value,
                max_importe=_base_neto_lineas(),
                scope_label="Descuento global",
            )
            if limit_msg:
                show_discount_limit_modal(f"{limit_msg} Se ajustó al máximo permitido.")
            _sync_global_discount_pair_from_mode(active_field=field_descuento_global_pct, normalize_active=True)
            _recalc_total()
            return True

        def _on_global_desc_imp_commit(_):
            preferred_mode = (
                global_discount_last_edited["value"]
                if global_discount_last_edited.get("value") in ("percentage", "amount")
                else "percentage"
            )
            global_discount_mode["value"] = _resolve_global_discount_mode_for_commit(preferred_mode)
            mode = global_discount_mode["value"]
            limit_msg = _discount_limit_message(
                mode=mode,
                descuento_porcentaje=field_descuento_global_pct.value,
                descuento_importe=field_descuento_global_imp.value,
                max_importe=_base_neto_lineas(),
                scope_label="Descuento global",
            )
            if limit_msg:
                show_discount_limit_modal(f"{limit_msg} Se ajustó al máximo permitido.")
            _sync_global_discount_pair_from_mode(active_field=field_descuento_global_imp, normalize_active=True)
            _recalc_total()
            return True

        def _on_sena_commit(_):
            _normalize_field_numeric(field_sena, decimals=2, use_grouping=True)
            _recalc_total()
            return True

        for manual_field in [sum_subtotal, sum_iva, sum_total]:
            manual_field.on_submit = _chain_handler_and_focus(
                lambda _, fld=manual_field: _normalize_field_numeric(fld, decimals=2, use_grouping=True),
                manual_field,
            )
            if hasattr(manual_field, "on_blur"):
                manual_field.on_blur = lambda _, fld=manual_field: _normalize_field_numeric(fld, decimals=2, use_grouping=True)  # type: ignore[attr-defined]

        field_descuento_global_pct.on_change = _on_global_desc_pct_change
        field_descuento_global_imp.on_change = _on_global_desc_imp_change
        field_descuento_global_pct.on_submit = _chain_handler_and_focus(_on_global_desc_pct_commit, field_descuento_global_pct)
        field_descuento_global_imp.on_submit = _chain_handler_and_focus(_on_global_desc_imp_commit, field_descuento_global_imp)
        field_sena.on_submit = _chain_handler_and_focus(_on_sena_commit, field_sena)
        if hasattr(field_descuento_global_pct, "on_blur"):
            field_descuento_global_pct.on_blur = _on_global_desc_pct_commit  # type: ignore[attr-defined]
        if hasattr(field_descuento_global_imp, "on_blur"):
            field_descuento_global_imp.on_blur = _on_global_desc_imp_commit  # type: ignore[attr-defined]
        if hasattr(field_sena, "on_blur"):
            field_sena.on_blur = _on_sena_commit  # type: ignore[attr-defined]

        # Keyboard flow for header fields
        field_fecha.on_submit = _chain_handler_and_focus(field_fecha.on_submit, field_fecha)
        field_vto.on_submit = _chain_handler_and_focus(field_vto.on_submit, field_vto)
        field_obs.on_submit = _chain_handler_and_focus(field_obs.on_submit, field_obs)
        field_direccion.on_submit = _chain_handler_and_focus(field_direccion.on_submit, field_direccion)

        def _on_entidad_change_with_item_focus(_: Any) -> Any:
            _update_entidad_info(None)
            target_control = _resolve_primary_line_article_focus()
            if target_control is not None and _focus_control(target_control):
                return False
            return True

        dropdown_tipo.on_change = _chain_handler_and_focus(dropdown_tipo.on_change, dropdown_tipo)
        dropdown_entidad.on_change = _chain_handler_and_focus(_on_entidad_change_with_item_focus, dropdown_entidad)
        dropdown_deposito.on_change = _chain_handler_and_focus(dropdown_deposito.on_change, dropdown_deposito)
        manual_mode.on_change = _chain_handler_and_focus(toggle_manual, manual_mode)
        
        # Use ListView with internal padding to prevent "first item cut-off" issue
        lines_container = ft.ListView(
            spacing=10,
            padding=ft.padding.only(top=15, left=5, right=10, bottom=5),
            expand=True,
            auto_scroll=True,
        )
        duplicate_item_dialog = ft.AlertDialog(modal=True)
        duplicate_item_dialog_state: Dict[str, Any] = {"close_callback": None, "is_open": False}
        modal_bottom_scroll_done_ref = {"value": False}

        def _scroll_comprobante_modal_to_bottom_once() -> None:
            if modal_bottom_scroll_done_ref["value"]:
                return
            try:
                if hasattr(_form_scroll_column, "scroll_to"):
                    _form_scroll_column.scroll_to(key=_FORM_SCROLL_BOTTOM_KEY, duration=0)
                if _safe_update_control(_form_scroll_column) or _safe_update_control(form_dialog):
                    modal_bottom_scroll_done_ref["value"] = True
            except Exception:
                logger.debug("No se pudo hacer auto-scroll del modal de comprobantes", exc_info=True)

        def _normalize_duplicate_text(value: Any) -> str:
            text = str(value or "").strip().lower()
            if not text:
                return ""
            normalized = unicodedata.normalize("NFKD", text)
            clean = "".join(ch for ch in normalized if not unicodedata.combining(ch))
            return " ".join(clean.split())

        def _find_article_by_value(value: Any) -> Optional[Dict[str, Any]]:
            if value in (None, ""):
                return None
            try:
                art_id = int(value)
            except Exception:
                return None
            return next((a for a in articulos if int(a.get("id")) == art_id), None)

        def _get_row_duplicate_keys(row: Any) -> Dict[str, str]:
            row_map = getattr(row, "data", None) or {}
            art_drop_ctrl = row_map.get("art_drop")
            selected_value = getattr(art_drop_ctrl, "value", None)
            code_key = _normalize_duplicate_text(selected_value)

            selected_art = _find_article_by_value(selected_value)
            article_name = str((selected_art or {}).get("nombre") or row_map.get("article_name") or "").strip()
            name_key = _normalize_duplicate_text(article_name)

            duplicate_desc = row_map.get("duplicate_desc") or article_name
            desc_key = _normalize_duplicate_text(duplicate_desc)
            return {"code": code_key, "name": name_key, "description": desc_key}

        def _find_duplicate_target_row(source_row: Any) -> Optional[Any]:
            source_keys = _get_row_duplicate_keys(source_row)
            if not any(source_keys.values()):
                return None
            for candidate_row in lines_container.controls:
                if candidate_row is source_row:
                    continue
                candidate_keys = _get_row_duplicate_keys(candidate_row)
                if not any(candidate_keys.values()):
                    continue
                if any(
                    source_keys[key] and source_keys[key] == candidate_keys[key]
                    for key in ("code", "name", "description")
                ):
                    return candidate_row
            return None

        def _clear_line_for_duplicate(row_to_clear: Any, *, recalc: bool = True) -> None:
            row_map = getattr(row_to_clear, "data", None) or {}
            art_drop_ctrl = row_map.get("art_drop")
            lista_drop_ctrl = row_map.get("lista_drop")
            cant_field_ctrl = row_map.get("cant_field")
            price_field_ctrl = row_map.get("price_field")
            iva_field_ctrl = row_map.get("iva_field")
            desc_pct_field_ctrl = row_map.get("desc_pct_field")
            desc_imp_field_ctrl = row_map.get("desc_imp_field")
            bultos_field_ctrl = row_map.get("bultos_field")
            total_field_ctrl = row_map.get("total_field")
            stock_text_ctrl = row_map.get("stock_text")

            if art_drop_ctrl is not None:
                art_drop_ctrl.value = None
            if lista_drop_ctrl is not None:
                global_lista_raw = getattr(dropdown_lista_global, "value", None)
                global_lista_value = str(global_lista_raw).strip() if global_lista_raw not in (None, "") else ""
                lista_drop_ctrl.value = global_lista_value if global_lista_value else ""
                try:
                    lista_drop_ctrl.clear_cache()
                except Exception:
                    pass
            if cant_field_ctrl is not None:
                cant_field_ctrl.value = ""
            if price_field_ctrl is not None:
                price_field_ctrl.value = "0,00"
            if iva_field_ctrl is not None:
                iva_field_ctrl.value = ""
            if desc_pct_field_ctrl is not None:
                desc_pct_field_ctrl.value = ""
            if desc_imp_field_ctrl is not None:
                desc_imp_field_ctrl.value = ""
            if bultos_field_ctrl is not None:
                bultos_field_ctrl.value = ""
            if total_field_ctrl is not None:
                total_field_ctrl.value = "0,00"

            fiscal_ref = row_map.get("fiscal_iva_rate_ref")
            if isinstance(fiscal_ref, dict):
                fiscal_ref["value"] = to_decimal("0")
            discount_mode_ref = row_map.get("discount_mode_ref")
            if isinstance(discount_mode_ref, dict):
                discount_mode_ref["value"] = "percentage"

            if stock_text_ctrl is not None:
                stock_text_ctrl.value = "Stock: -"
                stock_text_ctrl.color = COLOR_TEXT_MUTED
                stock_text_ctrl.weight = ft.FontWeight.NORMAL

            row_map["duplicate_desc"] = ""
            row_map["article_name"] = ""

            refresh_labels = row_map.get("refresh_lista_labels")
            if callable(refresh_labels):
                refresh_labels()
            recalc_line = row_map.get("recalculate_line")
            if callable(recalc_line):
                recalc_line()

            _safe_update_multiple(
                art_drop_ctrl,
                lista_drop_ctrl,
                cant_field_ctrl,
                price_field_ctrl,
                iva_field_ctrl,
                desc_pct_field_ctrl,
                desc_imp_field_ctrl,
                bultos_field_ctrl,
                total_field_ctrl,
                stock_text_ctrl,
            )
            if recalc:
                _recalc_total()

        def _ask_duplicate_item_confirmation(source_row: Any, apply_article_change: Callable[[], None]) -> None:
            duplicate_focus_state: Dict[str, str] = {"action": "clear"}
            previous_handler = getattr(page, "on_keyboard_event", None)

            def _set_focused_action(action: str) -> None:
                duplicate_focus_state["action"] = action

            def _focus_action_button(action: str) -> None:
                target = no_btn if action == "clear" else add_btn
                _set_focused_action(action)
                _focus_control(target)

            def _restore_keyboard_handler() -> None:
                if previous_handler:
                    page.on_keyboard_event = previous_handler
                else:
                    page.on_keyboard_event = None

            def _close(_: Any = None) -> None:
                _restore_keyboard_handler()
                duplicate_item_dialog_state["close_callback"] = None
                duplicate_item_dialog_state["is_open"] = False
                _safe_page_close(page, duplicate_item_dialog, "duplicate_item_confirmation")

            def _confirm_clear(_: Any = None) -> None:
                _close(None)
                _clear_line_for_duplicate(source_row)
                source_row_map = getattr(source_row, "data", None) or {}
                _focus_control(source_row_map.get("art_drop"))

            def _confirm_add(_: Any = None) -> None:
                _close(None)
                apply_article_change()
                source_row_map = getattr(source_row, "data", None) or {}
                _focus_control(source_row_map.get("cant_field"))

            def _on_duplicate_dialog_key(e: ft.KeyboardEvent) -> None:
                key = str(getattr(e, "key", "") or "").strip().lower().replace("_", "").replace("-", "").replace(" ", "")
                if not _is_keydown_event(e):
                    return
                if key in {"arrowleft", "left"}:
                    _focus_action_button("clear")
                    return
                if key in {"arrowright", "right"}:
                    _focus_action_button("add")
                    return
                if key in {"enter", "numpadenter", "return"}:
                    if duplicate_focus_state.get("action") == "add":
                        _confirm_add(None)
                    else:
                        _confirm_clear(None)
                    return
                if key in {"esc", "escape"}:
                    _confirm_clear(None)
                    return

            no_btn = _cancel_button("No", on_click=_confirm_clear)
            _maybe_set(no_btn, "on_focus", lambda _: _set_focused_action("clear"))
            _maybe_set(no_btn, "autofocus", True)
            add_btn = ft.ElevatedButton(
                "Agregar",
                bgcolor=COLOR_ACCENT,
                color="#FFFFFF",
                on_click=_confirm_add,
                on_focus=lambda _: _set_focused_action("add"),
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
            )

            page.on_keyboard_event = _on_duplicate_dialog_key

            duplicate_item_dialog.title = ft.Text("Artículo ya ingresado", size=20, weight=ft.FontWeight.BOLD)
            duplicate_item_dialog.content = ft.Container(
                content=ft.Text(
                    "El artículo ya está en el comprobante. ¿Deseás volver a ingresarlo?",
                    size=14,
                    color=COLOR_TEXT_MUTED,
                ),
                padding=ft.padding.symmetric(vertical=10),
            )
            duplicate_item_dialog.shape = ft.RoundedRectangleBorder(radius=16)
            duplicate_item_dialog.actions = [no_btn, add_btn]
            opened = _safe_page_open(page, duplicate_item_dialog, "duplicate_item_confirmation")
            if not opened:
                _restore_keyboard_handler()
                duplicate_item_dialog_state["close_callback"] = None
                duplicate_item_dialog_state["is_open"] = False
                return
            duplicate_item_dialog_state["close_callback"] = _close
            duplicate_item_dialog_state["is_open"] = True

            def _focus_no_button() -> None:
                _focus_action_button("clear")

            def _delayed_focus():
                try:
                    time.sleep(0.08)
                    _run_on_ui(_focus_no_button)
                except Exception:
                    pass

            _run_on_ui(_focus_no_button)
            _run_in_background(_delayed_focus)

        def _add_line(_=None, update_ui=True, initial_data=None):
            if is_read_only_ref["value"]:
                return
            art_initial_items = [{"value": a["id"], "label": f"{a['nombre']} (Cod: {a['id']})"} for a in articulos]
            art_drop = AsyncSelect(
                label="Artículo *", 
                loader=comprobante_article_loader, 
                expand=True,
                initial_items=art_initial_items,
                keyboard_accessible=True,
                **comprobante_async_select_style,
            )
            initial_articulo_id = str(initial_data["id_articulo"]) if initial_data and initial_data.get("id_articulo") else None
            initial_fiscal_iva = to_decimal((initial_data or {}).get("porcentaje_iva"), to_decimal("0"))
            fiscal_iva_rate_ref = {"value": initial_fiscal_iva}
            preserve_initial_fiscal = {"value": bool(initial_data)}
            def item_price_list_loader(query, offset, limit):
                if not db: return [], False
                rows = db.fetch_listas_precio(search=query, offset=offset, limit=limit)
                active_rows = [r for r in rows if r.get("activa", True)]
                
                art_id = art_drop.value
                prices_map = {}
                if art_id:
                    try:
                        prices = db.fetch_article_prices(int(art_id))
                        for p in prices:
                            prices_map[str(p["id_lista_precio"])] = p.get("precio", 0)
                    except Exception as e:
                        logger.warning(f"Operacion fallo: {e}")
                
                items = []
                for r in active_rows:
                    lid = str(r["id"])
                    label = r["nombre"]
                    if art_id and lid in prices_map:
                        p_val = prices_map[lid]
                        p_fmt = _format_money(p_val)
                        label = f"{label} ({p_fmt})"
                    items.append({"value": r["id"], "label": label})
                
                return items, len(rows) >= limit

            lista_drop = AsyncSelect(
                label="Lista",
                loader=item_price_list_loader,
                width=220,
                initial_items=lista_initial_items,
                keyboard_accessible=True,
                **comprobante_async_select_style,
            )
            cant_field = ft.TextField(label="Cant. *", width=80, value=""); _style_input(cant_field); _style_comprobante_control(cant_field)
            price_field = ft.TextField(label="Precio *",width=90, value="0,00"); _style_input(price_field); _style_comprobante_control(price_field)
            iva_field = ft.TextField(label="IVA % *", width=60, value=""); _style_input(iva_field); _style_comprobante_control(iva_field)
            desc_pct_field = ft.TextField(label="Desc. %", width=90, value=""); _style_input(desc_pct_field); _style_comprobante_control(desc_pct_field)
            desc_imp_field = ft.TextField(label="Desc. $", width=100, value=""); _style_input(desc_imp_field); _style_comprobante_control(desc_imp_field)
            bultos_field = ft.TextField(label="Bultos", width=75, value="", read_only=True, text_align=ft.TextAlign.RIGHT); _style_input(bultos_field); _style_comprobante_control(bultos_field)
            total_field = ft.TextField(label="Total", width=100, value="0,00", read_only=True, text_align=ft.TextAlign.RIGHT); _style_input(total_field); _style_comprobante_control(total_field)
            line_discount_mode = {"value": "percentage"}
            line_discount_last_edited = {"value": "percentage"}
            quantity_warning_guard = {"raw": None, "ts": 0.0}
            stock_cache: Dict[str, Any] = {"article_id": None, "available": None}
            
            if initial_data:
                art_drop.value = str(initial_data["id_articulo"])
                lista_drop.value = str(initial_data["id_lista_precio"]) if initial_data.get("id_lista_precio") else ""
                qty_initial_dec = parse_locale_number(initial_data["cantidad"])
                if qty_initial_dec is not None and qty_initial_dec == qty_initial_dec.to_integral_value():
                    cant_field.value = normalize_input_value(initial_data["cantidad"], decimals=0, use_grouping=False)
                else:
                    # Compatibilidad histórica: mantenemos decimales visibles, pero se bloqueará guardado hasta corregir.
                    cant_field.value = normalize_input_value(initial_data["cantidad"], decimals=2, use_grouping=False)
                price_field.value = normalize_input_value(initial_data["precio_unitario"], decimals=2, use_grouping=True)
                iva_field.value = ""
                desc_pct_field.value = _dec_to_input_or_blank(initial_data.get("descuento_porcentaje", 0), use_grouping=True)
                desc_imp_field.value = _dec_to_input_or_blank(initial_data.get("descuento_importe", 0), use_grouping=True)
                if _parse_float(desc_pct_field.value, "Desc. %") > 0:
                    line_discount_mode["value"] = "percentage"
                elif _parse_float(desc_imp_field.value, "Desc. $") > 0:
                    line_discount_mode["value"] = "amount"
                line_discount_last_edited["value"] = line_discount_mode["value"]
            else:
                # Usar lista global si está seleccionada
                if dropdown_lista_global.value and dropdown_lista_global.value != "":
                    lista_drop.value = dropdown_lista_global.value
                else:
                    lista_drop.value = ""
                iva_field.value = ""

            def _get_selected_article() -> Optional[Dict[str, Any]]:
                if not art_drop.value:
                    return None
                try:
                    selected_id = int(art_drop.value)
                except Exception:
                    return None
                return next((a for a in articulos if int(a.get("id")) == selected_id), None)

            def _get_article_default_iva_rate() -> Any:
                art = _get_selected_article()
                if not art and db and art_drop.value:
                    try:
                        art = db.get_article_simple(int(art_drop.value))
                    except Exception:
                        art = None
                if not art:
                    return to_decimal("0")
                iva_default = art.get("porcentaje_iva")
                if iva_default is None:
                    return to_decimal("0")
                return max(to_decimal("0"), to_decimal(iva_default))

            def _get_article_unidades_por_bulto() -> Any:
                art = _get_selected_article()
                if not art and db and art_drop.value:
                    try:
                        art = db.get_article_simple(int(art_drop.value))
                    except Exception:
                        art = None
                if not art:
                    return None
                return art.get("unidades_por_bulto")

            def _update_bultos_field() -> None:
                try:
                    cantidad_value = _parse_int_quantity(cant_field.value, "Cantidad")
                except Exception:
                    cantidad_value = None
                bultos_value = calculate_bultos(
                    cantidad_value,
                    _get_article_unidades_por_bulto(),
                    mode="strict_exact",
                )
                bultos_field.value = str(bultos_value) if bultos_value is not None else ""
                _safe_update_control(bultos_field)

            def _sync_fiscal_iva_from_visible(*, source: str) -> None:
                # source: "auto" (carga inicial), "article_change" (cambia artículo), "user" (edición manual)
                try:
                    visible_iva = to_decimal(_parse_float(iva_field.value, "IVA"))
                except Exception:
                    visible_iva = to_decimal("0")

                if visible_iva > to_decimal("0"):
                    fiscal_iva_rate_ref["value"] = quantize_2(visible_iva)
                    preserve_initial_fiscal["value"] = False
                    return

                if (
                    source == "auto"
                    and preserve_initial_fiscal["value"]
                    and initial_articulo_id
                    and str(art_drop.value or "") == initial_articulo_id
                    and initial_fiscal_iva > to_decimal("0")
                ):
                    fiscal_iva_rate_ref["value"] = max(to_decimal("0"), quantize_2(initial_fiscal_iva))
                    return

                fiscal_iva_rate_ref["value"] = quantize_2(_get_article_default_iva_rate())
                preserve_initial_fiscal["value"] = False
            
            def _update_line_total(active_field: Optional[ft.TextField] = None, normalize_active: bool = False):
                """Actualiza el total de la línea"""
                try:
                    c_cant = _parse_int_quantity(cant_field.value, "Cantidad")
                    c_price = _parse_float(price_field.value, "Precio")
                    base_line = to_decimal(c_cant) * to_decimal(c_price)
                    d_pct, d_imp = normalize_discount_pair(
                        base_amount=base_line,
                        descuento_porcentaje=desc_pct_field.value,
                        descuento_importe=desc_imp_field.value,
                        mode="amount" if line_discount_mode["value"] == "amount" else "percentage",
                    )
                    if active_field is not desc_pct_field or normalize_active:
                        desc_pct_field.value = _dec_to_input_or_blank(d_pct, use_grouping=True)
                        _safe_update_control(desc_pct_field)
                    if active_field is not desc_imp_field or normalize_active:
                        desc_imp_field.value = _dec_to_input_or_blank(d_imp, use_grouping=True)
                        _safe_update_control(desc_imp_field)
                    line_sign = to_decimal("1") if base_line >= to_decimal("0") else to_decimal("-1")
                    line_total = base_line - (line_sign * d_imp)
                    total_field.value = _dec_to_input(line_total, use_grouping=True)
                    _update_bultos_field()
                    if total_field.page:
                        total_field.update()
                except Exception:
                    total_field.value = "0,00"
                    bultos_field.value = ""
            
            def _update_price_from_list(*, recalc: bool = True):
                """Actualiza el precio basado en artículo y lista seleccionados"""
                art_id_val = art_drop.value
                # Primero intentar usar la lista del ítem, si no la lista global
                lid_raw = getattr(lista_drop, "value", None)
                lid = str(lid_raw).strip() if lid_raw not in (None, "") else ""
                if not lid:
                    global_lid_raw = getattr(dropdown_lista_global, "value", None)
                    global_lid = str(global_lid_raw).strip() if global_lid_raw not in (None, "") else ""
                    if global_lid:
                        lid = global_lid
                        lista_drop.value = global_lid
                        _safe_update_control(lista_drop)
                
                if not art_id_val:
                    return

                try:
                    art_id = int(art_id_val)
                except Exception:
                    return

                final_price = 0.0
                try:
                    prices = db.fetch_article_prices(art_id) if db else []
                except Exception as ex:
                    logger.warning(f"No se pudieron obtener precios para el artículo {art_id}: {ex}")
                    prices = []
                
                if prices:
                    if lid and lid != "":
                        # Usar la lista seleccionada
                        p_obj = next((p for p in prices if str(p["id_lista_precio"]) == str(lid)), None)
                        if p_obj:
                            raw_price = p_obj.get("precio")
                            if raw_price not in (None, ""):
                                try:
                                    final_price = float(raw_price)
                                except Exception:
                                    logger.warning(
                                        f"Precio inválido en lista {lid} para artículo {art_id}: {raw_price}"
                                    )
                
                # Removed 'First available' usage and Cost fallback
                # If no list selected, price stays 0.0 unless manually edited
                
                price_field.value = _dec_to_input(final_price, use_grouping=True)
                _update_line_total()
                _safe_update_control(price_field)
                _safe_update_control(total_field)
                if recalc:
                    _recalc_total()
            
            stock_text = ft.Text("Stock: -", size=10, color=COLOR_TEXT_MUTED)

            def _check_stock_warning(*, force_refresh: bool = False):
                if not art_drop.value: return
                try:
                    requested = _parse_int_quantity(cant_field.value, "Cantidad")
                    current_art_id = int(art_drop.value)
                    cached_art_id = stock_cache.get("article_id")
                    cached_available = stock_cache.get("available")
                    if (
                        not force_refresh
                        and cached_available is not None
                        and cached_art_id == current_art_id
                    ):
                        available = int(cached_available)
                    else:
                        available = db.get_article_stock(current_art_id)
                        stock_cache["article_id"] = current_art_id
                        stock_cache["available"] = available
                    stock_text.value = f"Stock: {available}"
                    if requested > available:
                        stock_text.color = COLOR_ERROR
                        stock_text.weight = ft.FontWeight.BOLD
                    else:
                        stock_text.color = ft.Colors.GREEN_600
                        stock_text.weight = ft.FontWeight.NORMAL
                    _safe_update_control(stock_text)
                except Exception as e:
                    logger.warning(f"Falló al actualizar interfaz: {e}")

            def _refresh_lista_labels_with_prices(preferred_list: Any = None) -> str:
                try:
                    items, _ = item_price_list_loader("", 0, 100)
                    opts = [ft.dropdown.Option(str(i["value"]), i["label"]) for i in items]
                    valid_ids = {str(i["value"]) for i in items}
                    global_list_raw = getattr(dropdown_lista_global, "value", None)
                    global_list = str(global_list_raw).strip() if global_list_raw not in (None, "") else ""
                    current_raw = preferred_list if preferred_list is not None else getattr(lista_drop, "value", None)
                    current_value = str(current_raw).strip() if current_raw not in (None, "") else ""
                    if not current_value and global_list:
                        current_value = global_list
                    if current_value and valid_ids and current_value not in valid_ids:
                        if global_list and global_list in valid_ids:
                            current_value = global_list
                        else:
                            current_value = ""
                    lista_drop.options = opts
                    lista_drop.value = current_value
                    _safe_update_control(lista_drop)
                    return current_value
                except Exception as ex:
                    logger.warning(f"No se pudieron refrescar labels de listas con precios: {ex}")
                    fallback_raw = preferred_list if preferred_list is not None else getattr(lista_drop, "value", None)
                    return str(fallback_raw).strip() if fallback_raw not in (None, "") else ""

            def _apply_article_change_effects() -> None:
                _refresh_lista_labels_with_prices()
                _sync_fiscal_iva_from_visible(source="article_change")
                _update_price_from_list()
                _check_stock_warning(force_refresh=True)
                if art_drop.value and lines_container.controls and lines_container.controls[-1] == row:
                    _add_line()
                    _scroll_comprobante_modal_to_bottom_once()

            def _on_art_change(e):
                current_row_map = row.data or {}
                selected_art = _get_selected_article()
                selected_name = str((selected_art or {}).get("nombre") or "").strip()
                current_row_map["article_name"] = selected_name
                current_row_map["duplicate_desc"] = selected_name

                if art_drop.value:
                    duplicate_target = _find_duplicate_target_row(row)
                    if duplicate_target is not None:
                        _ask_duplicate_item_confirmation(row, _apply_article_change_effects)
                        return False
                _apply_article_change_effects()
                return True

            def _on_art_change_and_focus_qty(e):
                result = _on_art_change(e)
                if result is False:
                    return False
                if _focus_control(cant_field):
                    return False
                return True
            
            def _on_value_change(_):
                _update_line_total()
                _recalc_total()

            def _on_iva_change(_):
                _sync_fiscal_iva_from_visible(source="user")
                _update_line_total()
                _recalc_total()

            def _line_discount_limit_message() -> Optional[str]:
                try:
                    c_cant = _parse_int_quantity(cant_field.value, "Cantidad")
                    c_price = _parse_float(price_field.value, "Precio")
                except Exception:
                    return None
                base_line = to_decimal(c_cant) * to_decimal(c_price)
                return _discount_limit_message(
                    mode=line_discount_mode["value"],
                    descuento_porcentaje=desc_pct_field.value,
                    descuento_importe=desc_imp_field.value,
                    max_importe=base_line,
                    scope_label="Descuento de línea",
                )

            def _resolve_line_discount_mode_for_commit(preferred_mode: str) -> str:
                current_mode = (
                    line_discount_mode["value"]
                    if line_discount_mode["value"] in ("percentage", "amount")
                    else "percentage"
                )
                if preferred_mode not in ("percentage", "amount"):
                    return current_mode
                if current_mode == preferred_mode:
                    return current_mode

                try:
                    c_cant = _parse_int_quantity(cant_field.value, "Cantidad")
                    c_price = _parse_float(price_field.value, "Precio")
                except Exception:
                    return current_mode

                base_line = to_decimal(c_cant) * to_decimal(c_price)
                pct_current, imp_current = normalize_discount_pair(
                    base_amount=base_line,
                    descuento_porcentaje=desc_pct_field.value,
                    descuento_importe=desc_imp_field.value,
                    mode="amount" if current_mode == "amount" else "percentage",
                )
                pct_entered = max(to_decimal("0"), to_decimal(desc_pct_field.value))
                imp_entered = max(to_decimal("0"), to_decimal(desc_imp_field.value))
                epsilon = to_decimal("0.0001")

                if preferred_mode == "percentage" and abs(pct_entered - pct_current) > epsilon:
                    return "percentage"
                if preferred_mode == "amount" and abs(imp_entered - imp_current) > epsilon:
                    return "amount"
                return current_mode

            cant_field.on_change = lambda _: (_check_stock_warning(), _update_line_total(), _recalc_total())
            
            def _on_desc_pct_change(_):
                line_discount_mode["value"] = "percentage"
                line_discount_last_edited["value"] = "percentage"
                _update_line_total(active_field=desc_pct_field)
                _recalc_total()

            def _on_desc_imp_change(_):
                line_discount_mode["value"] = "amount"
                line_discount_last_edited["value"] = "amount"
                _update_line_total(active_field=desc_imp_field)
                _recalc_total()

            def _on_desc_pct_commit(_):
                preferred_mode = (
                    line_discount_last_edited["value"]
                    if line_discount_last_edited.get("value") in ("percentage", "amount")
                    else "percentage"
                )
                line_discount_mode["value"] = _resolve_line_discount_mode_for_commit(preferred_mode)
                limit_msg = _line_discount_limit_message()
                if limit_msg:
                    show_discount_limit_modal(f"{limit_msg} Se ajustó al máximo permitido.")
                _update_line_total(active_field=desc_pct_field, normalize_active=True)
                _recalc_total()
                return True

            def _on_desc_imp_commit(_):
                preferred_mode = (
                    line_discount_last_edited["value"]
                    if line_discount_last_edited.get("value") in ("percentage", "amount")
                    else "percentage"
                )
                line_discount_mode["value"] = _resolve_line_discount_mode_for_commit(preferred_mode)
                limit_msg = _line_discount_limit_message()
                if limit_msg:
                    show_discount_limit_modal(f"{limit_msg} Se ajustó al máximo permitido.")
                _update_line_total(active_field=desc_imp_field, normalize_active=True)
                _recalc_total()
                return True

            def _on_price_commit(_):
                _normalize_field_numeric(price_field, decimals=2, use_grouping=True)
                _update_line_total()
                _recalc_total()
                return True

            def _on_iva_commit(_):
                _normalize_field_numeric_preserve_blank_zero(iva_field, decimals=2, use_grouping=True)
                _sync_fiscal_iva_from_visible(source="user")
                _update_line_total()
                _recalc_total()
                return True

            def _on_cantidad_commit(_):
                try:
                    _normalize_quantity_field(cant_field, label="Cantidad")
                    quantity_warning_guard["raw"] = None
                    quantity_warning_guard["ts"] = 0.0
                except ValueError as exc:
                    raw_value = str(cant_field.value or "").strip()
                    now_ts = time.time()
                    if (
                        quantity_warning_guard["raw"] == raw_value
                        and (now_ts - float(quantity_warning_guard["ts"] or 0.0)) < 0.8
                    ):
                        return False
                    quantity_warning_guard["raw"] = raw_value
                    quantity_warning_guard["ts"] = now_ts
                    show_toast(str(exc), kind="warning")
                    return False
                _check_stock_warning()
                _update_line_total()
                _recalc_total()
                return True

            def _on_cantidad_commit_focus_next_article(e):
                result = _on_cantidad_commit(e)
                if result is False:
                    return False
                next_article_control = _resolve_next_line_article_focus(row)
                if next_article_control is not None and _focus_control(next_article_control):
                    return False
                return True

            desc_pct_field.on_change = _on_desc_pct_change
            desc_imp_field.on_change = _on_desc_imp_change
            desc_pct_field.on_submit = _chain_handler_and_focus(_on_desc_pct_commit, desc_pct_field)
            desc_imp_field.on_submit = _chain_handler_and_focus(_on_desc_imp_commit, desc_imp_field)
            price_field.on_submit = _chain_handler_and_focus(_on_price_commit, price_field)
            iva_field.on_submit = _chain_handler_and_focus(_on_iva_commit, iva_field)
            cant_field.on_submit = _chain_handler_and_focus(_on_cantidad_commit_focus_next_article, cant_field)

            price_field.on_change = _on_value_change
            iva_field.on_change = _on_iva_change
            art_drop.on_change = _chain_handler_and_focus(_on_art_change_and_focus_qty, art_drop)
            def _on_lista_change(e):
                _update_price_from_list()
                return True
            lista_drop.on_change = _chain_handler_and_focus(_on_lista_change, lista_drop)
            for field, handler in [
                (desc_pct_field, _on_desc_pct_commit),
                (desc_imp_field, _on_desc_imp_commit),
                (price_field, _on_price_commit),
                (iva_field, _on_iva_commit),
                (cant_field, _on_cantidad_commit),
            ]:
                if hasattr(field, "on_blur"):
                    field.on_blur = handler  # type: ignore[attr-defined]

            cant_container = ft.Column([cant_field, stock_text], spacing=0, width=80)

            # Store callbacks for external updates
            initial_article = _get_selected_article()
            initial_article_name = str((initial_article or {}).get("nombre") or "").strip()
            duplicate_desc_value = str((initial_data or {}).get("descripcion_historica") or "").strip()
            if not duplicate_desc_value:
                duplicate_desc_value = initial_article_name

            row_map = {
                "update_price": _update_price_from_list,
                "lista_drop": lista_drop,
                "art_drop": art_drop,
                "cant_field": cant_field,
                "price_field": price_field,
                "iva_field": iva_field,
                "fiscal_iva_rate_ref": fiscal_iva_rate_ref,
                "sync_fiscal_iva": _sync_fiscal_iva_from_visible,
                "desc_pct_field": desc_pct_field,
                "desc_imp_field": desc_imp_field,
                "bultos_field": bultos_field,
                "total_field": total_field,
                "discount_mode_ref": line_discount_mode,
                "recalculate_line": _update_line_total,
                "check_stock_warning": _check_stock_warning,
                "refresh_lista_labels": _refresh_lista_labels_with_prices,
                "stock_text": stock_text,
                "duplicate_desc": duplicate_desc_value,
                "article_name": initial_article_name,
            }

            delete_btn = ft.IconButton(
                icon=ft.icons.DELETE, 
                icon_color=COLOR_ERROR, 
                tooltip="Eliminar línea",
                on_click=lambda e: _remove_line(e.control.parent)
            )
            _style_comprobante_button_focus(delete_btn)
            row_map["delete_btn"] = delete_btn

            # [Articulo, Lista, Cant, Precio, IVA, Desc %, Desc $, Bultos, Total, Delete]
            row = ft.Row([art_drop, lista_drop, cant_container, price_field, iva_field, desc_pct_field, desc_imp_field, bultos_field, total_field, delete_btn], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.START)
            row.data = row_map # Attack callbacks to row

            _sync_fiscal_iva_from_visible(source="auto")
            
            lines_container.controls.append(row)
            if update_ui:
                lines_container.update()
                _recalc_total()
            _refresh_keyboard_navigation_order()
            
            # Initial Run
            if initial_data:
                _update_line_total()
                # If editing, check stock silently?
                pass
            
            # Trigger initial stock check if article is pre-selected (e.g. from copy)
            # Trigger initial stock check and label refresh if needed
            if art_drop.value:
                _check_stock_warning()
                # Manual trigger of label refresh
                try: 
                    if initial_data:
                        _refresh_lista_labels_with_prices()
                    else:
                        _on_art_change(None)
                except: 
                     lista_drop.clear_cache()
            
            # If global list is set and we have a value (e.g. from user or global default logic), 
            # make sure labels are updated even if no article yet? No, if no article no price.
            # But if article selected, _on_art_change above handles it.
            
            # If we just added a line and have a global list, but no article yet:
            # The loader will return standard labels. That's fine.
            # But if we HAVE an article (e.g. initial_data), _on_art_change should have run above.
            
            if not initial_data and dropdown_lista_global.value and art_drop.value:
                 # Case where we might need to refresh if art was set differently
                 pass
            _refresh_keyboard_navigation_order()
        
        def _resolve_line_focus_target(row_to_remove: Any) -> Optional[Any]:
            rows = _list_line_rows()
            if row_to_remove not in rows:
                if _is_focus_eligible(btn_add_line):
                    return btn_add_line
                return _get_preferred_action_button()

            row_index = rows.index(row_to_remove)

            def _first_focusable_row_control(row: Any) -> Optional[Any]:
                row_map = getattr(row, "data", None) or {}
                for key in ("art_drop", "lista_drop", "cant_field", "price_field", "iva_field", "desc_pct_field", "desc_imp_field", "delete_btn"):
                    candidate = row_map.get(key)
                    if _is_focus_eligible(candidate):
                        return candidate
                return None

            if row_index + 1 < len(rows):
                candidate = _first_focusable_row_control(rows[row_index + 1])
                if candidate is not None:
                    return candidate
            if row_index > 0:
                candidate = _first_focusable_row_control(rows[row_index - 1])
                if candidate is not None:
                    return candidate
            if _is_focus_eligible(btn_add_line):
                return btn_add_line
            return _get_preferred_action_button()

        def _remove_line(row_to_remove):
            if is_read_only_ref["value"]:
                return
            focus_target = _resolve_line_focus_target(row_to_remove)
            if row_to_remove not in lines_container.controls:
                if focus_target is not None:
                    _focus_control(focus_target)
                return
            lines_container.controls.remove(row_to_remove)
            lines_container.update()
            _recalc_total()
            _refresh_keyboard_navigation_order()
            if focus_target is not None:
                _focus_control(focus_target)

        def _on_global_list_change(e):
            """When global price list changes, update all line items that don't have a specific list set."""
            if is_read_only_ref["value"]:
                return False
            new_global_list_id = dropdown_lista_global.value
            if not new_global_list_id:
                return False

            changed_any = False
            for row in lines_container.controls:
                row_map = row.data
                line_lista_drop = row_map["lista_drop"]
                
                # If the line list is empty (Automatic), change it to the new Global list
                # This satisfies the user's request: "automaticamente todos los precios deberían tomar la lista 2"
                # If the line list is empty (Automatic), change it to the new Global list
                # This satisfies the user's request: "automaticamente todos los precios deberían tomar la lista 2"
                if not line_lista_drop.value or line_lista_drop.value == "":
                    line_lista_drop.value = new_global_list_id
                    changed_any = True
                    _safe_update_control(line_lista_drop)
                    update_price_fn = row_map.get("update_price")
                    if callable(update_price_fn):
                        try:
                            update_price_fn(recalc=False)
                        except TypeError:
                            update_price_fn()

            if changed_any:
                _recalc_total()
            _refresh_keyboard_navigation_order()
            return True

        # (Manual wire for AsyncSelect global list is handled in its on_change)
        dropdown_lista_global.on_change = _chain_handler_and_focus(_on_global_list_change, dropdown_lista_global)

        def _set_control_locked(control: Any, locked: bool) -> None:
            if control is None:
                return
            if hasattr(control, "disabled"):
                _maybe_set(control, "disabled", locked)
            if locked and hasattr(control, "read_only"):
                _maybe_set(control, "read_only", True)

        def _refresh_modal_action_buttons() -> None:
            current_doc_id = active_doc_id_ref["value"]
            doc_row = current_doc_row_ref["value"] or {}
            can_manage_docs = CURRENT_USER_ROLE in ["ADMIN", "GERENTE"]

            if btn_modal_print is not None:
                btn_modal_print.visible = bool(current_doc_id)

            if btn_modal_print_no_prices is not None:
                btn_modal_print_no_prices.visible = bool(current_doc_id)

            if btn_modal_confirm is not None:
                btn_modal_confirm.visible = bool(
                    current_doc_id
                    and can_manage_docs
                    and doc_row
                    and doc_row.get("estado") == DocumentoEstado.BORRADOR.value
                )

            if btn_modal_afip is not None:
                btn_modal_afip.visible = bool(
                    current_doc_id
                    and can_manage_docs
                    and doc_row
                    and _can_authorize_afip(doc_row)
                )

            if btn_modal_reset is not None:
                btn_modal_reset.visible = True

            if btn_modal_save is not None:
                btn_modal_save.visible = not is_read_only_ref["value"]
                btn_modal_save.text = "Guardar cambios [F12]" if current_doc_id else ("Crear Comprobante [F12]" if not edit_doc_id else "Guardar [F12]")

            if btn_add_line is not None:
                btn_add_line.visible = not is_read_only_ref["value"]
            _refresh_keyboard_navigation_order()

        def _set_form_read_only(read_only: bool = True) -> None:
            is_read_only_ref["value"] = bool(read_only)
            locked = is_read_only_ref["value"]

            static_controls = [
                field_fecha,
                field_vto,
                dropdown_tipo,
                dropdown_entidad,
                dropdown_lista_global,
                dropdown_deposito,
                field_numero,
                field_sena,
                field_obs,
                field_direccion,
                field_descuento_global_pct,
                field_descuento_global_imp,
                manual_mode,
                sum_subtotal,
                sum_iva,
                sum_total,
            ]
            for ctrl in static_controls:
                _set_control_locked(ctrl, locked)

            for row in lines_container.controls:
                row_map = row.data or {}
                for key in ("art_drop", "lista_drop", "cant_field", "price_field", "iva_field", "desc_pct_field", "desc_imp_field", "bultos_field", "delete_btn"):
                    _set_control_locked(row_map.get(key), locked)

            _set_control_locked(btn_add_line, locked)
            _refresh_modal_action_buttons()
            _safe_update_multiple(
                field_fecha,
                field_vto,
                dropdown_tipo,
                dropdown_entidad,
                dropdown_lista_global,
                dropdown_deposito,
                field_numero,
                field_sena,
                field_obs,
                field_direccion,
                field_descuento_global_pct,
                field_descuento_global_imp,
                manual_mode,
                sum_subtotal,
                sum_iva,
                sum_total,
                btn_add_line,
                btn_modal_print,
                btn_modal_print_no_prices,
                btn_modal_confirm,
                btn_modal_afip,
                btn_modal_reset,
                btn_modal_save,
            )
            _refresh_keyboard_navigation_order()

        def _refresh_modal_doc_state() -> None:
            current_doc_id = active_doc_id_ref["value"]
            current_doc_row_ref["value"] = None
            if current_doc_id:
                try:
                    current_doc_row_ref["value"] = db.fetch_documento_resumen_by_id(int(current_doc_id))
                except Exception as exc:
                    logger.warning(f"No se pudo refrescar estado de comprobante {current_doc_id}: {exc}")

            doc_row = current_doc_row_ref["value"] or {}
            doc_number = str(doc_row.get("numero_serie") or "").strip()
            if doc_number:
                field_numero.value = doc_number
                _safe_update_control(field_numero)

            is_non_draft = bool(doc_row and doc_row.get("estado") != DocumentoEstado.BORRADOR.value)
            if is_non_draft and not is_read_only_ref["value"]:
                _set_form_read_only(True)
            else:
                _refresh_modal_action_buttons()
                _safe_update_multiple(
                    btn_modal_print,
                    btn_modal_print_no_prices,
                    btn_modal_confirm,
                    btn_modal_afip,
                    btn_modal_reset,
                    btn_modal_save,
                    btn_add_line,
                )
            _refresh_keyboard_navigation_order()

        def _save(_=None) -> bool:
            if is_read_only_ref["value"]:
                show_toast("El comprobante está en solo lectura.", kind="warning")
                return False
            if not dropdown_tipo.value or not dropdown_entidad.value or not dropdown_deposito.value:
                show_toast("Faltan campos obligatorios", kind="warning")
                return False

            # Normalización final para persistir formato consistente sin forzar durante tipeo.
            _normalize_field_numeric_preserve_blank_zero(field_descuento_global_pct, decimals=2, use_grouping=True)
            _normalize_field_numeric_preserve_blank_zero(field_descuento_global_imp, decimals=2, use_grouping=True)
            _normalize_field_numeric(field_sena, decimals=2, use_grouping=True)
            if manual_mode.value:
                _normalize_field_numeric(sum_subtotal, decimals=2, use_grouping=True)
                _normalize_field_numeric(sum_iva, decimals=2, use_grouping=True)
                _normalize_field_numeric(sum_total, decimals=2, use_grouping=True)

            for row in lines_container.controls:
                row_map = row.data or {}
                try:
                    _normalize_quantity_field(row_map.get("cant_field"), label="Cantidad")
                except ValueError as exc:
                    show_toast(str(exc), kind="error")
                    return False
                _normalize_field_numeric(row_map.get("price_field"), decimals=2, use_grouping=True)
                _normalize_field_numeric_preserve_blank_zero(row_map.get("iva_field"), decimals=2, use_grouping=True)
                sync_fiscal = row_map.get("sync_fiscal_iva")
                if callable(sync_fiscal):
                    sync_fiscal(source="user")
                _normalize_field_numeric_preserve_blank_zero(row_map.get("desc_pct_field"), decimals=2, use_grouping=True)
                _normalize_field_numeric_preserve_blank_zero(row_map.get("desc_imp_field"), decimals=2, use_grouping=True)

            def _resolve_discount_mode(raw_mode: Any, pct_val: float, imp_val: float) -> str:
                mode = "amount" if str(raw_mode).lower() == "amount" else "percentage"
                # Fallback defensivo: si el estado UI de modo no se actualizó,
                # inferimos por el campo efectivamente usado.
                if mode == "percentage" and pct_val <= 0 and imp_val > 0:
                    return "amount"
                if mode == "amount" and imp_val <= 0 and pct_val > 0:
                    return "percentage"
                return mode
            
            items = []
            for idx, row in enumerate(lines_container.controls, start=1):
                row_map = row.data or {}
                art_id = row_map["art_drop"].value
                if not art_id: continue
                
                # Usar lista del ítem, o la global si no tiene
                item_lista = row_map["lista_drop"].value
                if not item_lista or item_lista == "" or item_lista == "Automático":
                    item_lista = dropdown_lista_global.value
                
                # Ensure global value is also clean
                if item_lista == "Automático": item_lista = ""
                try:
                    fiscal_iva_ref = row_map.get("fiscal_iva_rate_ref") or {}
                    fiscal_iva_rate = float(to_decimal(fiscal_iva_ref.get("value", 0)))
                    cantidad_val = _parse_int_quantity(row_map["cant_field"].value, "Cantidad")
                    precio_unitario_val = _parse_float(row_map["price_field"].value, "Precio Unitario")
                    descuento_pct_val = _parse_float(row_map["desc_pct_field"].value, "Desc. %")
                    descuento_imp_val = _parse_float(row_map["desc_imp_field"].value, "Desc. $")
                    discount_mode_ref = row_map.get("discount_mode_ref") or {}
                    descuento_mode_val = _resolve_discount_mode(
                        discount_mode_ref.get("value", "percentage"),
                        descuento_pct_val,
                        descuento_imp_val,
                    )
                    if isinstance(discount_mode_ref, dict):
                        discount_mode_ref["value"] = descuento_mode_val
                    base_line = to_decimal(cantidad_val) * to_decimal(precio_unitario_val)
                    line_limit_msg = _discount_limit_message(
                        mode=descuento_mode_val,
                        descuento_porcentaje=descuento_pct_val,
                        descuento_importe=descuento_imp_val,
                        max_importe=base_line,
                        scope_label=f"Línea {idx}",
                    )
                    if line_limit_msg:
                        show_discount_limit_modal(
                            f"{line_limit_msg} Se ajustó al máximo permitido. Revisá y guardá nuevamente."
                        )
                        recalculate_line = row_map.get("recalculate_line")
                        if callable(recalculate_line):
                            recalculate_line(normalize_active=True)
                        _recalc_total()
                        return False
                    items.append({
                        "id_articulo": int(art_id),
                        "id_lista_precio": int(item_lista) if item_lista and item_lista != "" else None,
                        "cantidad": cantidad_val,
                        "precio_unitario": precio_unitario_val,
                        # Persistimos siempre la alícuota fiscal real, no el 0 visual del modo IVA incluido.
                        "porcentaje_iva": fiscal_iva_rate,
                        "descuento_porcentaje": descuento_pct_val,
                        "descuento_importe": descuento_imp_val,
                        "descuento_mode": descuento_mode_val,
                    })
                except ValueError as exc:
                    show_toast(str(exc), kind="error")
                    return False
            
            if not items:
                show_toast("El comprobante debe tener al menos una línea", kind="warning")
                return False

            # Determinar id_lista_precio del documento
            gl_val = dropdown_lista_global.value
            doc_lista_precio = int(gl_val) if gl_val and gl_val != "" and gl_val != "Automático" else None
            try:
                desc_global_pct = _parse_float(field_descuento_global_pct.value, "Desc. Global %")
                desc_global_imp = _parse_float(field_descuento_global_imp.value, "Desc. Global $")
            except ValueError as exc:
                show_toast(str(exc), kind="error")
                return False
            global_mode_to_persist = _resolve_discount_mode(
                global_discount_mode["value"],
                desc_global_pct,
                desc_global_imp,
            )
            global_discount_mode["value"] = global_mode_to_persist
            global_limit_msg = _discount_limit_message(
                mode=global_mode_to_persist,
                descuento_porcentaje=desc_global_pct,
                descuento_importe=desc_global_imp,
                max_importe=_base_neto_lineas(),
                scope_label="Descuento global",
            )
            if global_limit_msg:
                show_discount_limit_modal(
                    f"{global_limit_msg} Se ajustó al máximo permitido. Revisá y guardá nuevamente."
                )
                _sync_global_discount_pair_from_mode(normalize_active=True)
                _recalc_total()
                return False

            numero_val = (field_numero.value or "").strip()
            if not numero_val and current_doc_row_ref["value"]:
                numero_val = str((current_doc_row_ref["value"] or {}).get("numero_serie") or "").strip()
            numero_serie_value = numero_val if numero_val else None
            direccion_entrega_value = str(field_direccion.value or "").strip() or None
            fecha_vencimiento_value = str(field_vto.value or "").strip() or None

            current_doc_id = active_doc_id_ref["value"]
            doc_id = None
            action = "UPDATE" if current_doc_id else "INSERT"
            success_message = "Comprobante actualizado con éxito" if current_doc_id else "Comprobante creado con éxito"
            try:
                if current_doc_id:
                    db.update_document(
                        doc_id=int(current_doc_id),
                        id_tipo_documento=int(dropdown_tipo.value),
                        id_entidad_comercial=int(dropdown_entidad.value),
                        id_deposito=int(dropdown_deposito.value),
                        items=items,
                        observacion=field_obs.value,
                        numero_serie=numero_serie_value,
                        descuento_porcentaje=desc_global_pct,
                        descuento_importe=desc_global_imp,
                        descuento_global_mode=global_mode_to_persist,
                        fecha=field_fecha.value, 
                        fecha_vencimiento=fecha_vencimiento_value,
                        direccion_entrega=direccion_entrega_value,
                        id_lista_precio=doc_lista_precio,
                        sena=_parse_float(field_sena.value, "Seña"),
                        manual_values={
                            "subtotal": _parse_float(sum_subtotal.value, "Neto Manual"),
                            "iva_total": _parse_float(sum_iva.value, "IVA Manual"),
                            "total": _parse_float(sum_total.value, "Total Manual"),
                        } if manual_mode.value else None
                    )
                    doc_id = int(current_doc_id)
                else:
                    doc_id = db.create_document(
                        id_tipo_documento=int(dropdown_tipo.value),
                        id_entidad_comercial=int(dropdown_entidad.value),
                        id_deposito=int(dropdown_deposito.value),
                        items=items,
                        observacion=field_obs.value,
                        numero_serie=numero_serie_value,
                        descuento_porcentaje=desc_global_pct,
                        descuento_importe=desc_global_imp,
                        descuento_global_mode=global_mode_to_persist,
                        fecha=field_fecha.value, 
                        fecha_vencimiento=fecha_vencimiento_value,
                        direccion_entrega=direccion_entrega_value,
                        id_lista_precio=doc_lista_precio,
                        sena=_parse_float(field_sena.value, "Seña"),
                        manual_values={
                            "subtotal": _parse_float(sum_subtotal.value, "Neto Manual"),
                            "iva_total": _parse_float(sum_iva.value, "IVA Manual"),
                            "total": _parse_float(sum_total.value, "Total Manual"),
                        } if manual_mode.value else None
                    )
                    if doc_id:
                        active_doc_id_ref["value"] = int(doc_id)
                if db and doc_id:
                    db.log_activity("DOCUMENTO", action, id_entidad=doc_id, detalle={"tipo": dropdown_tipo.value, "items": len(items)})
                show_toast(success_message, kind="success")
                # Refresh tables if they are visible
                documentos_summary_table.refresh()
                refresh_all_stats()
                _refresh_modal_doc_state()
                return True
            except Exception as ex:
                show_toast(f"Error al guardar: {ex}", kind="error")
                return False
        if doc_data:
            copy_global_list_id = str(getattr(dropdown_lista_global, "value", "") or "").strip() if copy_doc_id else ""
            # Add existing items
            for item in doc_data["items"]:
                item_payload = dict(item)
                if copy_doc_id:
                    # En copia, usar la lista global ya resuelta por cliente.
                    if copy_global_list_id:
                        item_payload["id_lista_precio"] = copy_global_list_id
                    else:
                        item_payload.pop("id_lista_precio", None)
                else:
                    # Inject fallback price list (from header) if item doesn't have one
                    # (which it won't until DB supports it)
                    if "id_lista_precio" not in item_payload:
                        item_payload["id_lista_precio"] = doc_data.get("id_lista_precio")
                _add_line(initial_data=item_payload, update_ui=False)
            
            # Always add a blank line at the end for automation to work
            _add_line(update_ui=False)
            
            # Set manual totals if they were different from calculated?
            # Or just set them if the document state says so.
            # Simplified: always load them and if they match, user can just keep going.
            sum_subtotal.value = normalize_input_value(doc_data["total"], decimals=2, use_grouping=True)
            sum_iva.value = normalize_input_value(0, decimals=2, use_grouping=True)
            sum_total.value = normalize_input_value(doc_data["total"], decimals=2, use_grouping=True)
            try:
                total_val = _parse_float(sum_total.value, "Total")
                sena_val = _parse_float(field_sena.value, "Seña")
                sum_saldo.value = normalize_input_value(round(max(0, total_val - sena_val), 2), decimals=2, use_grouping=True)
            except Exception:
                sum_saldo.value = "0,00"
            sum_desc_lineas.value = normalize_input_value(
                round(sum(float(it.get("descuento_importe", 0) or 0) for it in doc_data.get("items", [])), 2),
                decimals=2,
                use_grouping=True,
            )
            sum_desc_global.value = normalize_input_value(doc_data.get("descuento_importe", 0), decimals=2, use_grouping=True)
            # Auto-enable manual mode if there's a discrepancy? 
            # For now, let user enable it if they want to edit.
        else:
            _add_line(update_ui=False) # Add one line by default, no update yet

        _sync_global_discount_pair_from_mode()
        if not doc_data:
            _recalc_total()

        def _print_current_document(_=None):
            current_doc_id = active_doc_id_ref["value"]
            if not current_doc_id:
                show_toast("Primero guardá el comprobante para poder imprimir.", kind="warning")
                return
            request_invoice_print(int(current_doc_id))

        def _print_current_document_direct(*, include_prices: bool = True, copies: int = 1) -> None:
            current_doc_id = active_doc_id_ref["value"]
            if not current_doc_id:
                show_toast("Primero guardá el comprobante para poder imprimir.", kind="warning")
                return
            print_document_external(int(current_doc_id), include_prices=include_prices, copies=copies)

        def _handle_modal_doc_state_change() -> None:
            _refresh_modal_doc_state()
            doc_row = current_doc_row_ref["value"] or {}
            if doc_row and doc_row.get("estado") != DocumentoEstado.BORRADOR.value:
                _set_form_read_only(True)

        def _confirm_document_direct(doc_id: int) -> None:
            if shortcut_state.get("confirming"):
                show_toast("Ya hay una confirmación en curso.", kind="warning")
                return
            shortcut_state["confirming"] = True

            def _finish_ui(success: bool, message: str = "") -> None:
                shortcut_state["confirming"] = False
                if not success:
                    if message:
                        show_toast(message, kind="error")
                    return
                show_toast("Comprobante confirmado", kind="success")
                _handle_modal_doc_state_change()
                if hasattr(documentos_summary_table, "refresh"):
                    documentos_summary_table.refresh()
                refresh_all_stats()

            def _job() -> None:
                try:
                    if not db:
                        _run_on_ui(_finish_ui, False, "No hay conexión a base de datos.")
                        return
                    db.confirm_document(int(doc_id))
                    db.log_activity("DOCUMENTO", "CONFIRM", id_entidad=int(doc_id))
                    _run_on_ui(_finish_ui, True, "")
                except Exception as exc:
                    _run_on_ui(_finish_ui, False, f"Error al confirmar: {exc}")

            _run_in_background(_job)

        def _confirm_after_save(*, force_direct: bool) -> None:
            if not _save():
                return
            current_doc_id_after_save = active_doc_id_ref["value"]
            if not current_doc_id_after_save:
                show_toast("No se pudo guardar el comprobante antes de confirmar.", kind="error")
                return
            if force_direct:
                _confirm_document_direct(int(current_doc_id_after_save))
                return
            _confirm_document(
                int(current_doc_id_after_save),
                close_after=False,
                on_success=_handle_modal_doc_state_change,
            )

        def _confirm_current_document(_=None, *, force_direct: bool = False):
            current_doc_id = active_doc_id_ref["value"]
            if not current_doc_id:
                show_toast("Primero guardá el comprobante para poder confirmarlo.", kind="warning")
                return
            if force_direct:
                _confirm_after_save(force_direct=True)
                return
            ask_confirm(
                "Confirmar Comprobante",
                "¿Está seguro que desea confirmar este comprobante? Esto generará movimientos de stock y afectará la cuenta corriente.",
                "Confirmar [Enter/F10]",
                lambda: _confirm_after_save(force_direct=True),
                button_color=COLOR_SUCCESS,
            )

        def _authorize_current_document(_=None):
            current_doc_id = active_doc_id_ref["value"]
            if not current_doc_id:
                show_toast("Primero guardá el comprobante para poder facturarlo.", kind="warning")
                return
            doc_row = current_doc_row_ref["value"]
            if not doc_row:
                doc_row = db.fetch_documento_resumen_by_id(int(current_doc_id))
            if not doc_row:
                show_toast("No se pudo obtener el estado actual del comprobante.", kind="error")
                return
            _confirm_afip_authorization(doc_row, close_after=False, on_success=_handle_modal_doc_state_change)

        def _close_comprobante_form(e: Any = None) -> None:
            close_duplicate = duplicate_item_dialog_state.get("close_callback")
            if callable(close_duplicate):
                close_duplicate(None)
            elif getattr(duplicate_item_dialog, "open", False):
                _safe_page_close(page, duplicate_item_dialog, "close_comprobante_duplicate_dialog")
            if _is_confirm_dialog_open():
                confirm_dialog_state["on_confirm"] = None
                confirm_dialog_state["is_open"] = False
                _safe_page_close(page, confirm_dialog, "close_comprobante_confirm_dialog")
            if getattr(print_options_dialog, "open", False):
                _safe_page_close(page, print_options_dialog, "close_comprobante_print_options")
            if getattr(discount_limit_dialog, "open", False):
                _safe_page_close(page, discount_limit_dialog, "close_comprobante_discount_limit")
            _restore_modal_keyboard_handler()
            shortcut_state["f8_pressed"] = False
            shortcut_state["f9_pressed"] = False
            shortcut_state["f10_pending_token"] = 0
            shortcut_state["last_f10_ts"] = 0.0
            shortcut_state["confirming"] = False
            _maybe_set(main_app_container, "disabled", False)
            close_form(e)
            _safe_update_control(main_app_container)

        def _reset_comprobante_form(_=None) -> None:
            close_duplicate = duplicate_item_dialog_state.get("close_callback")
            if callable(close_duplicate):
                close_duplicate(None)
            elif getattr(duplicate_item_dialog, "open", False):
                _safe_page_close(page, duplicate_item_dialog, "reset_comprobante_duplicate_dialog")
            if _is_confirm_dialog_open():
                confirm_dialog_state["on_confirm"] = None
                confirm_dialog_state["is_open"] = False
                _safe_page_close(page, confirm_dialog, "reset_comprobante_confirm_dialog")
            if getattr(print_options_dialog, "open", False):
                _safe_page_close(page, print_options_dialog, "reset_comprobante_print_options")
            if getattr(discount_limit_dialog, "open", False):
                _safe_page_close(page, discount_limit_dialog, "reset_comprobante_discount_limit")
            _close_comprobante_form(None)
            open_nuevo_comprobante()

        btn_add_line = ft.ElevatedButton(
            "Agregar Línea",
            icon=ft.icons.ADD,
            on_click=_add_line,
            bgcolor=COLOR_ACCENT,
            color="white",
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
            ),
        )
        btn_modal_print = ft.ElevatedButton(
            "Imprimir [F9]",
            icon=ft.icons.PRINT_ROUNDED,
            on_click=_print_current_document,
            bgcolor="#F1F5F9",
            color=COLOR_TEXT,
            visible=False,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
        )
        btn_modal_print_no_prices = ft.ElevatedButton(
            "Imprimir s/precios [F8]",
            icon=ft.icons.PRINT_ROUNDED,
            on_click=lambda _: _print_current_document_direct(include_prices=False, copies=1),
            bgcolor="#F1F5F9",
            color=COLOR_TEXT,
            visible=False,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
        )
        btn_modal_confirm = ft.ElevatedButton(
            "Confirmar [F10]",
            icon=ft.icons.CHECK_CIRCLE,
            on_click=_confirm_current_document,
            bgcolor=COLOR_SUCCESS,
            color="#FFFFFF",
            visible=False,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
        )
        btn_modal_afip = ft.ElevatedButton(
            "Facturar AFIP",
            icon=ft.icons.SECURITY,
            on_click=_authorize_current_document,
            bgcolor=COLOR_WARNING,
            color="#FFFFFF",
            visible=False,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
        )
        btn_modal_reset = ft.ElevatedButton(
            "Reset [F11]",
            icon=ft.icons.RESTART_ALT_ROUNDED,
            on_click=_reset_comprobante_form,
            bgcolor="#F1F5F9",
            color=COLOR_TEXT,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
        )
        btn_modal_save = ft.ElevatedButton(
            "Guardar cambios [F12]" if active_doc_id_ref["value"] else "Crear Comprobante [F12]",
            icon=ft.icons.CHECK,
            on_click=_save,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
                bgcolor=COLOR_ACCENT,
                color=ft.Colors.WHITE,
            ),
        )
        btn_modal_close = _cancel_button("Cerrar", on_click=_close_comprobante_form)
        _style_comprobante_action_buttons()
        actions_row = ft.Row(
            [
                ft.Row([btn_modal_print, btn_modal_print_no_prices, btn_modal_confirm, btn_modal_afip], spacing=8),
                ft.Row([btn_modal_reset, btn_modal_close, btn_modal_save], spacing=8),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )



        # Custom Dialog Content (replacing generic open_form to control layout fully)
        dialog_content = ft.Container(
            content=ft.Column(
                controls=[
                    ft.Container(height=20), # Header Spacer
                    ft.Row([
                        ft.Text("Nuevo Comprobante", size=20, weight=ft.FontWeight.BOLD),
                        ft.IconButton(ft.icons.CLOSE, on_click=_close_comprobante_form)
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    ft.Row([field_fecha, field_vto, dropdown_tipo], spacing=10),
                    ft.Row([dropdown_entidad, field_saldo], spacing=10),
                    ft.Row([dropdown_lista_global], spacing=10),
                    ft.Row([dropdown_deposito, field_numero, field_sena], spacing=10),
                    ft.Row([ft.Container(expand=True, content=field_obs)], spacing=10),
                    ft.Row([field_direccion], spacing=10),
                    ft.Divider(),
                    ft.Text("Ítems", weight=ft.FontWeight.BOLD),
                    ft.Container(
                        content=lines_container,
                        height=500, # Height for the items list specifically
                        border=ft.border.all(1, "#E2E8F0"),
                        border_radius=8,
                    ),
                    ft.Row([btn_add_line], alignment=ft.MainAxisAlignment.START),
                    ft.Divider(),
                    # Financial Footer
                    ft.Row([
                        ft.Container(expand=True),
                        field_descuento_global_pct,
                        field_descuento_global_imp,
                    ], alignment=ft.MainAxisAlignment.END, spacing=10),
                    ft.Row([
                        manual_mode,
                        ft.Column([sum_desc_lineas], spacing=0),
                        ft.Column([sum_desc_global], spacing=0),
                        ft.Column([sum_subtotal], spacing=0),
                        ft.Column([sum_iva], spacing=0),
                        ft.Column([sum_total], spacing=0),
                        ft.Column([sum_saldo], spacing=0),
                    ], alignment=ft.MainAxisAlignment.END, spacing=15),
                    ft.Container(height=10),
                    actions_row,
                ],
                spacing=15,
            ),
            padding=ft.padding.all(25),
            width=1600,
            bgcolor="white",
            border_radius=12,
        )

        # Directly open dialog bypassing generic wrapper for custom size/layout
        # Directly open dialog bypassing generic wrapper for custom size/layout
        # Using nonlocal/closure from main
        # Using nonlocal/closure from main
        _form_title.value = "Nuevo Comprobante" if not edit_doc_id else "Editar Comprobante"
        _form_content_area.content = dialog_content
        _form_actions_area.controls = [] # No actions, buttons are inside
        _form_header.visible = False # Hide header to use internal one
        _refresh_modal_doc_state()

        # Clear native dialog just in case
        page.dialog = None

        _maybe_set(main_app_container, "disabled", True)
        _install_modal_keyboard_handler()
        form_dialog.visible = True
        if form_dialog in page.overlay:
            page.overlay.remove(form_dialog)
        page.overlay.append(form_dialog)
        page.update()
        _refresh_keyboard_navigation_order()
        if not _focus_control(dropdown_entidad):
            _focus_control(field_fecha)

    def wire_live_search(table: GenericTable):
        for flt in table.advanced_filters:
            ctrl = getattr(flt, "control", None)
            if not ctrl or not hasattr(ctrl, "on_change"):
                continue
            if ctrl.on_change:
                continue
            ctrl.on_change = lambda _, t=table: t.refresh()

    for t in [entidades_table, articulos_table, documentos_summary_table, movimientos_table, pagos_table, remitos_table]:
        wire_live_search(t)

    # Don't auto-navigate - login will call set_view after auth
    # update_nav() and set_view() are called in do_login after successful authentication
    
    # Ensure window settings are applied before shutdown hooks.
    page.window_prevent_close = False
    
    # Initial catalog load after all UI is ready
    if db is not None and not db_error:
        try:
            reload_catalogs()
        except Exception as exc:
            print(f"Error enitial reload: {exc}")

    page.update()

    def _shutdown(reason: str) -> None:
        """Best-effort shutdown: log logout if needed, then close DB pool."""
        nonlocal db, window_is_closing, logout_logged
        if window_is_closing:
            return
        window_is_closing = True

        if db:
            try:
                if current_user and current_user.get("id") and not logout_logged:
                    nombre = current_user.get("nombre")
                    logout_logged = db.log_logout(reason, usuario=nombre, use_pool=False)
            except Exception as ex:
                pass # print(f"DEBUG: Error logging logout: {ex}")
            try:
                db.close()
            except Exception:
                pass
            try:
                scheduler.shutdown()
            except Exception:
                pass

    atexit.register(lambda: _shutdown("salida_programa"))
    # page.on_window_event = None
    # page.on_close = None
    # page.on_disconnect = None



if __name__ == "__main__":
    ft.app(target=main)
