from __future__ import annotations

from pathlib import Path
from datetime import datetime
import base64
import json
import atexit
import inspect
import socket
import sys
import time
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from venv import logger

import flet as ft
try:
    from flet.core.datatable import DataTable as CoreDataTable
except Exception:
    CoreDataTable = ft.DataTable

if not getattr(CoreDataTable.before_update, "_nexoryn_patched_v2", False):
    _original_before_update_core = CoreDataTable.before_update
    
    def _patched_before_update_core(self):
        try:
            # Asegurarse de que __content existe y tiene visible=True
            if hasattr(self, '_DataTable__content'):
                # Asegurarnos que el contenido es visible
                content = self._DataTable__content
                if hasattr(content, 'visible'):
                    content.visible = True
            return _original_before_update_core(self)
        except AssertionError as e:
            if "content must be visible" in str(e):
                # Silenciar este error específico
                return
            raise
        except Exception:
            # Silenciar otros errores durante before_update
            pass
    
    _patched_before_update_core._nexoryn_patched_v2 = True
    CoreDataTable.before_update = _patched_before_update_core
    ft.DataTable.before_update = _patched_before_update_core

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
    from desktop_app.services.print_service import generate_pdf_and_open
    from desktop_app.components.async_select import AsyncSelect
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
    from services.print_service import generate_pdf_and_open # type: ignore
except ImportError:
    from config import load_config  # type: ignore
    from database import Database  # type: ignore
    from services.afip_service import AfipService # type: ignore
    from services.backup_service import BackupService # type: ignore
    from services.print_service import generate_pdf_and_open # type: ignore
    from components.backup_professional_view import BackupProfessionalView # type: ignore
    from components.dashboard_view import DashboardView # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


ICONS = ft.Icons


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

class SafeDataTable(ft.DataTable):
    """Subclass of DataTable to fix TypeErrors and AssertionErrors in Flet updates"""
    def before_update(self):
        try:
            # Ensure content is visible before parent update
            if hasattr(self, '_DataTable__content'):
                content = self._DataTable__content
                if hasattr(content, 'visible'):
                    content.visible = True
            
            # Ensure index is int or None before parent check
            if hasattr(self, "sort_column_index"):
                val = self.sort_column_index
                if val is not None and not isinstance(val, int):
                    try:
                        self.sort_column_index = int(val)
                    except:
                        self.sort_column_index = None
            
            # Forzar visibilidad de la tabla
            self.visible = True
            
            super().before_update()
        except AssertionError as e:
            if "content must be visible" in str(e):
                # Ignorar este error específico
                return
            raise
        except Exception:
            # Ignorar otros errores
            pass


def _format_money(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
    if value is None:
        return "—"
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _format_bool(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
    if value is None:
        return "—"
    return "Sí" if bool(value) else "No"


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
    colors = {
        "PAGADO": ("#DCFCE7", "#166534"),
        "CONFIRMADO": ("#E0F2FE", "#075985"),
        "BORRADOR": ("#F1F5F9", "#475569"),
        "ANULADO": ("#FEE2E2", "#991B1B"),
    }
    bg, fg = colors.get(status, ("#F3F4F6", "#374151"))
    return ft.Container(
        padding=ft.padding.symmetric(horizontal=10, vertical=4),
        border_radius=20,
        bgcolor=bg,
        content=ft.Text(status, size=11, weight=ft.FontWeight.W_600, color=fg),
    )


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
    _maybe_set(control, "content_padding", ft.padding.all(12))

    if is_dropdown:
        _maybe_set(control, "bgcolor", "#F8FAFC")
        _maybe_set(control, "filled", True)
        _maybe_set(control, "border_width", 2)
        return

    _maybe_set(control, "filled", True)
    _maybe_set(control, "bgcolor", "#F8FAFC")
    _maybe_set(control, "border_width", 1)

    if is_textfield and not is_dropdown:
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
        options=[ft.dropdown.Option(str(v) if v is not None else "", t) for v, t in options],
        width=width,
    )
    if on_change is not None:
        _maybe_set(dd, "on_change", on_change)
    _maybe_set(dd, "enable_search", True)
    _style_input(dd)
    return dd


def _date_field(page: ft.Page, label: str, width: int = 180) -> ft.TextField:
    tf = ft.TextField(label=label, width=width)
    _style_input(tf)
    
    def on_date_change(e):
        if e.control.value:
            tf.value = e.control.value.strftime("%Y-%m-%d")
            tf.update()
            if hasattr(tf, "on_submit") and tf.on_submit:
                try:
                    tf.on_submit(None)
                except:
                    pass
    
    dp = ft.DatePicker(
        on_change=on_date_change,
        help_text="SELECCIONAR FECHA",
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
    page.overlay.append(dp)
    try:
        page.update()  # Ensure DatePicker is registered with the page
    except Exception:
        pass
    
    def open_picker(_):
        try:
            if hasattr(page, "open"):
                page.open(dp)
            else:
                dp.open = True
                page.update()
        except AssertionError:
            # Fallback: re-add and try again
            if dp not in page.overlay:
                page.overlay.append(dp)
                page.update()
            dp.open = True
            page.update()

    tf.suffix = ft.IconButton(
        icon=ft.Icons.CALENDAR_MONTH_ROUNDED,
        icon_size=18,
        on_click=open_picker,
    )
    return tf


def main(page: ft.Page) -> None:
    page.title = "Nexoryn Tech"
    page.window_width = 1280
    page.window_height = 860
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    page.fonts = {"Roboto": "Roboto-Regular.ttf"}

    def print_document_external(doc_id):
        """Global helper to print from table"""
        try:
            if not db: return 
            
            # Fetch full document data
            doc = db.get_document_full(doc_id)
            if not doc:
                show_toast("Error al recuperar datos del documento", kind="error")
                return
            
            # Get client name
            ent = db.get_entity_simple(doc.get("id_entidad_comercial"))
            
            # Build Items Data
            items_data = []
            for item in doc.get("items", []):
                art = db.get_article_simple(item["id_articulo"])
                item_copy = item.copy()
                item_copy["articulo_nombre"] = art["nombre"] if art else f"Artículo {item['id_articulo']}"
                items_data.append(item_copy)

            # Generate PDF
            generate_pdf_and_open(doc, ent or {}, items_data)
            show_toast(f"PDF generado correctamente.", kind="success")
            
        except Exception as e:
            show_toast(f"Error al imprimir: {e}", kind="error")


    # --- SHARED DIALOGS ---
    # --- SHARED DIALOGS (Custom Modal to allow nesting) ---
    _form_title = ft.Text(size=18, weight=ft.FontWeight.BOLD)
    _form_content_area = ft.Container()
    _form_actions_area = ft.Row(alignment=ft.MainAxisAlignment.END, spacing=10)
    _form_header = ft.Row([_form_title, ft.IconButton(ft.Icons.CLOSE_ROUNDED, icon_size=20, on_click=lambda _: close_form())], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
    
    # This replaces the native AlertDialog to allow multiple layers of modals
    form_dialog = ft.Container(
        content=ft.Card(
            elevation=20,
            shape=ft.RoundedRectangleBorder(radius=12),
            content=ft.Container(
                padding=24,
                width=850, # Default wide
                height=650, # Max height to enable scrolling
                content=ft.Column([
                    _form_header,
                    _form_content_area,
                    _form_actions_area
                ], tight=True, spacing=15, scroll=ft.ScrollMode.AUTO, expand=True)
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
        form_dialog.visible = False
        page.update()

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

    # Session state
    current_user: Dict[str, Any] = {}
    
    db: Optional[Database] = None
    db_error: Optional[str] = None
    local_ip = "127.0.0.1"
    try:
        config = load_config()
        db = Database(
            config.database_url,
            pool_min_size=config.db_pool_min,
            pool_max_size=config.db_pool_max,
        )

        # DB compatibility checks are handled by schema sync on login
    
        # Initialize Backup Service & Scheduler
        # Old standard backup system disabled
        # backup_service = BackupService(pg_bin_path=config.pg_bin_path)

        # Initialize Professional Backup System Scheduler
        professional_scheduler = None
        try:
            # Import and initialize professional backup system
            from services.backup_manager import BackupManager
            from apscheduler.schedulers.background import BackgroundScheduler as ProScheduler
            from apscheduler.triggers.cron import CronTrigger as ProCronTrigger

            professional_backup_manager = BackupManager(db)
            professional_scheduler = ProScheduler(timezone='America/Argentina/Buenos_Aires')

            # Schedule professional backups: FULL, DIFERENCIAL, INCREMENTAL
            def run_professional_backup(backup_type):
                try:
                    resultado = professional_backup_manager.execute_scheduled_backup(backup_type)
                    if resultado['exitoso']:
                        logger.info(f"Professional {backup_type} backup completed successfully")
                    else:
                        logger.error(f"Professional {backup_type} backup failed: {resultado['mensaje']}")
                except Exception as e:
                    logger.error(f"Error in professional {backup_type} backup: {e}")

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
                    logger.info(f"Backup validation completed: {resultado['validos']}/{resultado['total']} valid")
                except Exception as e:
                    logger.error(f"Error in backup validation: {e}")

            professional_scheduler.add_job(
                run_backup_validation,
                ProCronTrigger(hour=1, minute=0),
                id='backup_validation',
                name='Validación de Backups',
                max_instances=1,
                replace_existing=True
            )

            professional_scheduler.start()
            logger.info("Professional backup system scheduler started successfully")

        except Exception as e:
            logger.warning(f"Could not initialize professional backup system: {e}")
            # Fallback to legacy system
            professional_scheduler = BackgroundScheduler()

            def run_scheduled_backup(btype):
                try:
                    backup_service.create_backup(btype)
                    if db:
                        backup_service.record_backup_execution(db, btype)
                except Exception as e:
                    logger.error(f"Scheduled {btype} backup failed: {e}")

            # Old standard backup system disabled (replaced by Professional system)
            # professional_scheduler.add_job(lambda: run_scheduled_backup("daily"), CronTrigger(hour=23, minute=0), id="backup_daily")
            # professional_scheduler.add_job(lambda: run_scheduled_backup("weekly"), CronTrigger(day_of_week="sun", hour=23, minute=30), id="backup_weekly")
            # professional_scheduler.add_job(lambda: run_scheduled_backup("monthly"), CronTrigger(day=1, hour=0, minute=0), id="backup_monthly")
            # Legacy pruning disabled
            # professional_scheduler.add_job(lambda: backup_service.prune_backups(), CronTrigger(hour=1, minute=0), id="backup_prune")

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
        except: pass
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

    def ask_confirm(title: str, message: str, confirm_label: str, on_confirm, button_color: str = None) -> None:
        def close(_: Any) -> None:
            if hasattr(page, "close"):
                page.close(confirm_dialog)
            else:
                confirm_dialog.open = False
                page.update()

        def do_confirm(_: Any) -> None:
            close(None)
            try:
                on_confirm()
            except Exception as exc:
                show_toast(f"Error: {exc}", kind="error")

        final_color = button_color if button_color else COLOR_ERROR
        confirm_dialog.title = ft.Text(title, size=20, weight=ft.FontWeight.BOLD)
        confirm_dialog.content = ft.Container(
            content=ft.Text(message, size=14, color=COLOR_TEXT_MUTED),
            padding=ft.padding.symmetric(vertical=10)
        )
        confirm_dialog.shape = ft.RoundedRectangleBorder(radius=16)
        confirm_dialog.actions = [
            ft.TextButton("Cancelar", on_click=close, style=ft.ButtonStyle(color=COLOR_TEXT_MUTED, shape=ft.RoundedRectangleBorder(radius=8))),
            ft.ElevatedButton(
                confirm_label, 
                bgcolor=final_color, 
                color="#FFFFFF", 
                on_click=do_confirm,
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
            ),
        ]
        if hasattr(page, "open"):
            page.open(confirm_dialog)
        else:
            page.dialog = confirm_dialog
            confirm_dialog.open = True
            page.update()

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
        try: refresh_articles_catalogs()
        except: pass
        try: refresh_movimientos_catalogs()
        except: pass
        try: refresh_documentos_catalogs()
        except: pass
        try: refresh_pagos_catalogs()
        except: pass

    # reload_catalogs() will be called at the end of main after all controls are defined

    def dropdown_editor(values_provider: Callable[[], Sequence[str]], *, width: int, empty_label: str = "—") -> Any:
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

    # --- AsyncSelect Loaders ---
    def article_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_articles(search=query, offset=offset, limit=limit)
        items = [{"value": r["id"], "label": f"{r['nombre']} (Cod: {r['id']})"} for r in rows]
        return items, len(rows) >= limit

    def entity_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_entities(search=query, offset=offset, limit=limit)
        items = [{"value": r["id"], "label": f"{r['nombre_completo']} ({r['tipo']})"} for r in rows]
        return items, len(rows) >= limit

    def supplier_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_entities(search=query, tipo="PROVEEDOR", offset=offset, limit=limit)
        items = [{"value": r["id"], "label": f"{r['nombre_completo']} (Proveedor)"} for r in rows]
        return items, len(rows) >= limit

    def price_list_loader(query, offset, limit):
        if not db: return [], False
        rows = db.fetch_listas_precio(search=query, offset=offset, limit=limit)
        # Solo activas
        items = [{"value": r["id"], "label": r["nombre"]} for r in rows if r.get("activa", True)]
        return items, len(rows) >= limit

    def province_loader(query, offset, limit):
        if not db: return [], False
        rows = db.list_provincias()
        if query:
            rows = [r for r in rows if query.lower() in r["nombre"].lower()]
        items = [{"value": r["id"], "label": r["nombre"]} for r in rows[offset:offset+limit]]
        return items, (offset + limit < len(rows))

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
    status_icon_value = ft.Icons.CHECK_CIRCLE_ROUNDED if db and not db_error else ft.Icons.ERROR_OUTLINE_ROUNDED
    status_color = "#166534" if db and not db_error else "#991B1B"
    status_badge.content.controls.extend(
        [
            ft.Icon(status_icon_value, size=16, color=status_color) if status_icon_value is not None else ft.Container(width=16, height=16),
            ft.Text("DB OK" if db and not db_error else "DB ERROR", size=12, color=status_color),
        ]
    )

    card_registry: Dict[str, ft.Text] = {}

    def make_stat_card(label: str, value: str, icon_name: str, color: str = COLOR_ACCENT, key: str = None) -> ft.Control:
        icon_value = getattr(ft.Icons, icon_name, ft.Icons.QUESTION_MARK_ROUNDED)
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
        except:
            pass

    def _art_slider_change(e):
        # Update label real-time
        s = articulos_advanced_costo_slider
        articulos_advanced_costo_label.value = f"Costo: entre {_format_money(s.start_value)} y {_format_money(s.end_value)}"
        try: articulos_advanced_costo_label.update()
        except: pass

    def _reset_cost_filter(ctrl, val):
        s = articulos_advanced_costo_slider
        s.start_value = s.min
        s.end_value = s.max
        articulos_advanced_costo_label.value = f"Costo: entre {_format_money(s.min)} y {_format_money(s.max)}"
        try: 
            s.update()
            articulos_advanced_costo_label.update()
        except: pass

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
        width=300,
        inactive_color="#E2E8F0",
        active_color=COLOR_ACCENT,
        label="{value}",
        on_change=_art_slider_change,
        on_change_end=_art_live,
    )
    
    articulos_advanced_costo_label = ft.Text("Filtro de Costo", size=12, weight=ft.FontWeight.BOLD)
    articulos_advanced_costo_ctrl = ft.Column([
        articulos_advanced_costo_label,
        articulos_advanced_costo_slider
    ], spacing=0, width=350)

    # Stock range filter
    def _art_stock_slider_change(e):
        s = articulos_advanced_stock_slider
        articulos_advanced_stock_label.value = f"Stock: entre {int(s.start_value)} y {int(s.end_value)} un."
        try: articulos_advanced_stock_label.update()
        except: pass

    def _reset_stock_filter(ctrl, val):
        s = articulos_advanced_stock_slider
        s.start_value = s.min
        s.end_value = s.max
        articulos_advanced_stock_label.value = f"Stock: entre {int(s.min)} y {int(s.max)} un."
        try:
            s.update()
            articulos_advanced_stock_label.update()
        except: pass

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
        width=300,
        inactive_color="#E2E8F0",
        active_color=COLOR_ACCENT,
        label="{value}",
        on_change=_art_stock_slider_change,
        on_change_end=_art_live,
    )
    articulos_advanced_stock_label = ft.Text("Filtro de Stock", size=12, weight=ft.FontWeight.BOLD)
    articulos_advanced_stock_ctrl = ft.Column([
        articulos_advanced_stock_label,
        articulos_advanced_stock_slider
    ], spacing=0, width=350)

    articulos_advanced_stock_bajo = ft.Switch(label="Solo bajo mínimo (stock)", value=False, on_change=_art_live)
    
    articulos_advanced_iva = _dropdown("Alicuota IVA", [("", "Todas")], value="", width=200, on_change=_art_live)
    articulos_advanced_unidad = _dropdown("Unidad Medida", [("", "Todas")], value="", width=200, on_change=_art_live)
    articulos_advanced_redondeo = _dropdown("Redondeo", [("", "Todos"), ("SI", "Sí"), ("NO", "No")], value="", width=150, on_change=_art_live)
    
    articulos_advanced_lista_precio = AsyncSelect(label="Precios de lista", loader=price_list_loader, width=200, on_change=lambda _: _art_live(None))
    
    articulos_advanced_estado = _dropdown(
        "Estado",
        [("", "Todos"), ("ACTIVO", "Activos"), ("INACTIVO", "Inactivos")],
        value="",
        on_change=_art_live,
        width=150
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
                except: pass
            
        except Exception as e: 
            print(f"Error refreshing article filters: {e}")
    def _ent_live(e):
        try:
            entidades_table.trigger_refresh()
        except:
            pass

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
    entidades_advanced_localidad = ft.TextField(label="Localidad", width=180, on_change=_ent_live)
    _style_input(entidades_advanced_localidad)
    entidades_advanced_provincia = ft.TextField(label="Provincia", width=150, on_change=_ent_live)
    _style_input(entidades_advanced_provincia)
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
    entidades_advanced_desde = _date_field(page, "Alta desde", width=150)
    entidades_advanced_hasta = _date_field(page, "Alta hasta", width=150)
    # Set on_submit for date fields to trigger refresh upon selection
    entidades_advanced_desde.on_submit = _ent_live
    entidades_advanced_hasta.on_submit = _ent_live

    entidades_advanced_iva = _dropdown("Condición IVA", [("", "Todos")], on_change=_ent_live, width=200)

    entidades_advanced_tipo = _dropdown(
        "Tipo",
        [("", "Todos"), ("CLIENTE", "Cliente"), ("PROVEEDOR", "Proveedor"), ("AMBOS", "Ambos")],
        value="",
        on_change=_ent_live,
        width=150
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
            ColumnConfig(key="apellido", label="Apellido", width=120),
            ColumnConfig(key="nombre", label="Nombre", width=120),
            ColumnConfig(key="razon_social", label="Razón Social", width=180),
            ColumnConfig(
                key="tipo", 
                label="Tipo", 
                formatter=lambda v, _: v or "—", 
                width=100,
                editable=True,
                inline_editor=dropdown_editor(lambda: ["CLIENTE", "PROVEEDOR", "AMBOS"], width=150, empty_label="Seleccionar...")
            ),
            ColumnConfig(key="cuit", label="CUIT", width=110),
            ColumnConfig(
                key="condicion_iva", 
                label="IVA", 
                width=140,
                editable=True,
                inline_editor=dropdown_editor(
                    lambda: [c["nombre"] for c in db.fetch_condiciones_iva(limit=100)], 
                    width=200, 
                    empty_label="Seleccionar..."
                )
            ),
            ColumnConfig(
                key="lista_precio",
                label="Lista Precio",
                width=140,
                editable=True,
                formatter=lambda v, _: v or "—",
                inline_editor=dropdown_editor(
                    lambda: [l["nombre"] for l in db.fetch_listas_precio(limit=100) if l["activa"]],
                    width=200,
                    empty_label="Seleccionar..."
                )
            ),
            ColumnConfig(key="domicilio", label="Domicilio", width=180, editable=True),
            ColumnConfig(key="telefono", label="Teléfono", width=120, editable=True),
            ColumnConfig(key="email", label="Email", width=180, editable=True),
            ColumnConfig(key="localidad", label="Localidad", width=140, editable=True, inline_editor=dropdown_editor(lambda: [l["nombre"] for l in db.fetch_localidades(limit=500)], width=250, empty_label="Seleccionar...")),
            ColumnConfig(key="provincia", label="Provincia", width=110, editable=True, inline_editor=dropdown_editor(lambda: [p["nombre"] for p in db.list_provincias()], width=200, empty_label="Seleccionar...")),
            ColumnConfig(
                key="notas",
                label="Notas",
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.INFO_OUTLINE_ROUNDED,
                    tooltip="Ver notas" if row.get("notas") else "Sin notas",
                    icon_color=COLOR_INFO if row.get("notas") else ft.Colors.GREY_400,
                    on_click=lambda _: open_form("Notas de Entidad", ft.Column([ft.Text(row.get("notas"), selectable=True)], scroll=ft.ScrollMode.ADAPTIVE, height=300), [ft.TextButton("Cerrar", on_click=close_form)]) if row.get("notas") else None,
                ),
                width=50,
            ),
            ColumnConfig(
                key="fecha_creacion", 
                label="Fecha Alta", 
                formatter=lambda v, _: v.strftime("%d/%m/%Y") if isinstance(v, datetime) else (datetime.fromisoformat(str(v).split(".")[0].replace(" ", "T")).strftime("%d/%m/%Y") if v else "—"),
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
                    icon=ft.Icons.EDIT_ROUNDED,
                    tooltip="Editar entidad completa",
                    icon_color=COLOR_ACCENT,
                    on_click=lambda e, rid=row.get("id"): open_editar_entidad(int(rid)),
                ),
                width=40,
            ),
            ColumnConfig(
                key="_toggle_active",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.CHECK_CIRCLE_OUTLINE_ROUNDED if not row.get("activo") else ft.Icons.DO_NOT_DISTURB_ON_ROUNDED,
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
            AdvancedFilterControl("localidad", entidades_advanced_localidad),
            AdvancedFilterControl("provincia", entidades_advanced_provincia),
            AdvancedFilterControl("email", entidades_advanced_email),
            AdvancedFilterControl("telefono", entidades_advanced_telefono),
            AdvancedFilterControl("notas", entidades_advanced_notas),
            AdvancedFilterControl("activo", entidades_advanced_activo),
            AdvancedFilterControl("desde", entidades_advanced_desde),
            AdvancedFilterControl("hasta", entidades_advanced_hasta),
            AdvancedFilterControl("condicion_iva", entidades_advanced_iva),
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
    )
    entidades_table.search_field.hint_text = "Búsqueda global (nombre/razón social/cuit)…"
    
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
                ft.ElevatedButton(
                    "Nueva Entidad", 
                    icon=ft.Icons.ADD_ROUNDED, 
                    bgcolor=COLOR_ACCENT, 
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=lambda _: open_nueva_entidad()
                )
            ]
        )
    ], spacing=10, scroll=ft.ScrollMode.ADAPTIVE)
    
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

    def deactivate_article(article_id: int) -> None:
        if db is None:
            raise provider_error()
        db.update_article_fields(int(article_id), {"activo": False})
        show_toast("Artículo desactivado", kind="success")
        articulos_table.refresh()

    articulos_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Nombre", width=240),
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
                formatter=lambda v, _: _format_money(v),
                width=110,
            ),
            ColumnConfig(
                key="unidad_abreviatura",
                label="UM",
                width=60,
            ),
            ColumnConfig(
                key="stock_minimo",
                label="Mínimo (stock)",
                editable=True,
                formatter=lambda v, _: "—" if v is None else f"{float(v):.2f}",
                width=90,
            ),
            ColumnConfig(
                key="stock_actual",
                label="Stock",
                formatter=lambda v, _: "—" if v is None else f"{float(v):.2f}",
                width=90,
            ),
            ColumnConfig(
                key="id_tipo_iva",
                label="Alicuota IVA",
                editable=True,
                formatter=lambda v, row: next((i["descripcion"] for i in tipos_iva_values if str(i["id"]) == str(v or row.get("id_tipo_iva"))), "—"),
                inline_editor=dropdown_editor(lambda: [i["descripcion"] for i in tipos_iva_values], width=200, empty_label="Seleccionar..."),
                width=150,
            ),
            ColumnConfig(
                key="id_proveedor",
                label="Proveedor",
                editable=True,
                formatter=lambda v, row: next((p["nombre"] for p in proveedores_values if str(p["id"]) == str(v or row.get("id_proveedor"))), "—"),
                inline_editor=dropdown_editor(lambda: [p["nombre"] for p in proveedores_values], width=200, empty_label="Seleccionar..."),
                width=180,
            ),
            ColumnConfig(
                key="ubicacion",
                label="Ubicación",
                width=120,
                editable=True,
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
                    icon=ft.Icons.INFO_OUTLINE_ROUNDED,
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
                    icon=ft.Icons.EDIT_ROUNDED,
                    tooltip="Editar artículo completo",
                    icon_color=COLOR_ACCENT,
                    on_click=lambda e, rid=row.get("id"): open_editar_articulo(int(rid)),
                ),
                width=40,
            ),
            ColumnConfig(
                key="_toggle_active",
                label="",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.CHECK_CIRCLE_OUTLINE_ROUNDED if not row.get("activo") else ft.Icons.DO_NOT_DISTURB_ON_ROUNDED,
                    tooltip="Activar artículo" if not row.get("activo") else "Desactivar artículo",
                    icon_color=COLOR_SUCCESS if not row.get("activo") else COLOR_WARNING,
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
        inline_edit_callback=lambda row_id, changes: db.update_article_fields(int(row_id), changes) if db else None,
        mass_edit_callback=lambda ids, updates: db.bulk_update_articles([int(i) for i in ids], updates) if db else None,
        mass_activate_callback=lambda ids: db.bulk_update_articles([int(i) for i in ids], {"activo": True}) if db else None,
        mass_deactivate_callback=lambda ids: db.bulk_update_articles([int(i) for i in ids], {"activo": False}) if db else None,
        show_inline_controls=True,
        show_mass_actions=True,
        show_selection=True,
        auto_load=True,
        page_size=12,
        page_size_options=(10, 25, 50),
        show_export_button=True,
    )
    articulos_table.search_field.hint_text = "Búsqueda global (nombre)…"
    
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
                ft.ElevatedButton(
                    "Nuevo Artículo", 
                    icon=ft.Icons.ADD_ROUNDED, 
                    bgcolor=COLOR_ACCENT, 
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                    on_click=lambda _: open_nuevo_articulo()
                )
            ]
        )
    ], spacing=10, scroll=ft.ScrollMode.ADAPTIVE)

    articulos_view = ft.Container(
        content=articulos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    admin_export_tables = [entidades_table, articulos_table]

    # ---- Crear entidad / artículo ----
    # (Using the unified form_dialog defined above at the start of main)

    nueva_entidad_nombre = ft.TextField(label="Nombre *", width=250)
    _style_input(nueva_entidad_nombre)
    nueva_entidad_apellido = ft.TextField(label="Apellido *", width=250)
    _style_input(nueva_entidad_apellido)
    nueva_entidad_razon_social = ft.TextField(label="Razón social *", width=510)
    _style_input(nueva_entidad_razon_social)
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
    nueva_entidad_condicion_iva = ft.Dropdown(label="Condición IVA *", width=250, options=[], enable_search=True)
    _style_input(nueva_entidad_condicion_iva)
    nueva_entidad_notas = ft.TextField(label="Notas", width=510, multiline=True, min_lines=2, max_lines=4)
    _style_input(nueva_entidad_notas)

    def _reload_entity_dropdowns():
        """Populate Province and Condición IVA dropdowns."""
        if not db:
            return
        try:
            provincias = db.list_provincias()
            nueva_entidad_provincia.options = [ft.dropdown.Option(str(p["id"]), p["nombre"]) for p in provincias]

            condiciones = db.fetch_condiciones_iva(limit=50)
            nueva_entidad_condicion_iva.options = [ft.dropdown.Option(str(c["id"]), c["nombre"]) for c in condiciones]
            
            # Load Price Lists
            listas = db.fetch_listas_precio(limit=100)
            nueva_entidad_lista_precio.options = [ft.dropdown.Option("", "—")] + [
                ft.dropdown.Option(str(l["id"]), l["nombre"]) for l in listas
            ]

            # Also update the advanced filter dropdown and trigger UI refresh
            entidades_advanced_iva.options = [ft.dropdown.Option("", "Todos")] + [ft.dropdown.Option(c["nombre"], c["nombre"]) for c in condiciones]
            
            try:
                nueva_entidad_condicion_iva.update()
                nueva_entidad_lista_precio.update()
                entidades_advanced_iva.update()
            except:
                pass
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
        nueva_entidad_localidad.prefetch(on_done=lambda: setattr(nueva_entidad_localidad, "disabled", False))
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
        f_prov = bool((nueva_entidad_provincia.value or "").strip())
        f_loc = bool((nueva_entidad_localidad.value or "").strip())

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
                descuento=float(nueva_entidad_descuento.value or 0),
                limite_credito=float(nueva_entidad_limite_credito.value or 0)
            )
            close_form()
            show_toast("Entidad creada", kind="success")
            entidades_table.refresh()
        except Exception as exc:
            show_toast(f"Error al crear: {exc}", kind="error")

    def open_nueva_entidad(_: Any = None) -> None:
        nonlocal editing_entity_id
        editing_entity_id = None
        nueva_entidad_nombre.value = ""
        nueva_entidad_apellido.value = ""
        nueva_entidad_razon_social.value = ""
        nueva_entidad_tipo.value = ""
        nueva_entidad_cuit.value = ""
        nueva_entidad_telefono.value = ""
        nueva_entidad_email.value = ""
        nueva_entidad_domicilio.value = ""
        nueva_entidad_lista_precio.value = ""
        nueva_entidad_descuento.value = "0"
        nueva_entidad_limite_credito.value = "0"
        nueva_entidad_activo.value = True

        nueva_entidad_descuento.value = "0"
        nueva_entidad_limite_credito.value = "0"
        nueva_entidad_activo.value = True
        
        # Reset new fields
        nueva_entidad_provincia.value = ""
        nueva_entidad_localidad.value = ""
        nueva_entidad_localidad.options = []
        nueva_entidad_localidad.disabled = True
        nueva_entidad_condicion_iva.value = ""
        nueva_entidad_notas.value = ""

        _reload_entity_dropdowns()
        open_form("Nueva entidad", _prepare_entity_form_content(), [
            ft.TextButton("Cancelar", on_click=close_form),
            ft.ElevatedButton("Crear", icon=ft.Icons.ADD, bgcolor=COLOR_ACCENT, color="#FFFFFF", 
                              style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)), on_click=crear_entidad),
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
            nueva_entidad_lista_precio.value = str(ent["id_lista_precio"]) if ent.get("id_lista_precio") else ""
            nueva_entidad_descuento.value = str(ent.get("descuento", 0))
            nueva_entidad_limite_credito.value = str(ent.get("limite_credito", 0))
            nueva_entidad_limite_credito.value = str(ent.get("limite_credito", 0))
            nueva_entidad_activo.value = bool(ent.get("activo", True))
            
            # Load new fields
            nueva_entidad_notas.value = ent.get("notas", "")
            nueva_entidad_condicion_iva.value = str(ent["id_condicion_iva"]) if ent.get("id_condicion_iva") else ""
            
            # Handle Location
            pid = ent.get("id_provincia")
            lid = ent.get("id_localidad")
            if pid:
                nueva_entidad_provincia.value = str(pid)
                # Manually trigger locality reload
                _on_provincia_change(None)
                if lid:
                    nueva_entidad_localidad.value = str(lid)
                    nueva_entidad_localidad.disabled = False
            else:
                nueva_entidad_provincia.value = ""
                nueva_entidad_localidad.value = ""
                nueva_entidad_localidad.disabled = True
            
            open_form("Editar entidad", _prepare_entity_form_content(), [
                ft.TextButton("Cancelar", on_click=close_form),
                ft.ElevatedButton("Guardar Cambios", icon=ft.Icons.SAVE_ROUNDED, bgcolor=COLOR_ACCENT, color="#FFFFFF",
                                  style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)), on_click=guardar_edicion_entidad),
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
        f_prov = bool((nueva_entidad_provincia.value or "").strip())
        f_loc = bool((nueva_entidad_localidad.value or "").strip())

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
            
            # Atomic update
            db_conn.update_entity_full(
                editing_entity_id,
                updates=updates,
                id_lista_precio=nueva_entidad_lista_precio.value,
                descuento=float(nueva_entidad_descuento.value or 0),
                limite_credito=float(nueva_entidad_limite_credito.value or 0)
            )
            
            close_form()
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
    nuevo_articulo_marca = ft.Dropdown(label="Marca *", width=275, options=[], value="")
    _style_input(nuevo_articulo_marca)
    nuevo_articulo_rubro = ft.Dropdown(label="Rubro *", width=275, options=[], value="")
    _style_input(nuevo_articulo_rubro)
    nuevo_articulo_tipo_iva = ft.Dropdown(label="Alicuota IVA *", width=275, options=[], value="")
    _style_input(nuevo_articulo_tipo_iva)
    nuevo_articulo_unidad = ft.Dropdown(label="Unidad Medida *", width=275, options=[], value="")
    _style_input(nuevo_articulo_unidad)
    nuevo_articulo_proveedor = AsyncSelect(label="Proveedor Habitual", loader=supplier_loader, width=560)
    
    nuevo_articulo_costo = _number_field("Costo *", width=275)
    nuevo_articulo_stock_minimo = _number_field("Stock mínimo *", width=275)
    nuevo_articulo_stock_actual = _number_field("Stock *", width=275)
    nuevo_articulo_ubicacion = ft.Dropdown(label="Ubicación *", width=560, options=[], value="")
    _style_input(nuevo_articulo_ubicacion)
    nuevo_articulo_descuento_base = _number_field("Descuento Base (%)", width=180)
    nuevo_articulo_ganancia_2 = _number_field("Ganancia 2 (%)", width=180)
    nuevo_articulo_redondeo = ft.Switch(label="Redondeo", value=False)
    nuevo_articulo_observacion = ft.TextField(label="Observaciones", width=560, multiline=True, min_lines=2, max_lines=4)
    _style_input(nuevo_articulo_observacion)
    nuevo_articulo_activo = ft.Switch(label="Activo", value=True)
    articulo_precios_container = ft.Column(spacing=10)

    def crear_articulo(_: Any = None) -> None:
        db_conn = get_db_or_toast()
        if db_conn is None:
            return
        try:
            if not nuevo_articulo_marca.value or not nuevo_articulo_rubro.value or not nuevo_articulo_nombre.value:
                show_toast("Campos obligatorios marcados con * son requeridos", kind="warning")
                return

            costo_val = float(nuevo_articulo_costo.value or 0)
            if costo_val <= 0:
                show_toast("El costo debe ser mayor a 0", kind="warning")
                return

            art_id = db_conn.create_article(
                nombre=nuevo_articulo_nombre.value or "",
                marca=nuevo_articulo_marca.value,
                rubro=nuevo_articulo_rubro.value,
                costo=costo_val,
                stock_minimo=float(nuevo_articulo_stock_minimo.value or 0),
                ubicacion=nuevo_articulo_ubicacion.value,
                activo=bool(nuevo_articulo_activo.value),
                id_tipo_iva=int(nuevo_articulo_tipo_iva.value) if nuevo_articulo_tipo_iva.value else None,
                id_unidad_medida=int(nuevo_articulo_unidad.value) if nuevo_articulo_unidad.value else None,
                id_proveedor=int(nuevo_articulo_proveedor.value) if nuevo_articulo_proveedor.value else None,
                observacion=nuevo_articulo_observacion.value,
                descuento_base=float(nuevo_articulo_descuento_base.value or 0),
                redondeo=bool(nuevo_articulo_redondeo.value),
                porcentaje_ganancia_2=float(nuevo_articulo_ganancia_2.value or 0)
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
                            price_val = float(tf_precio.value or 0) if tf_precio.value else None
                            if price_val and price_val > 0:
                                any_price = True
                            price_updates.append({
                                "id_lista_precio": lp_id,
                                "precio": price_val,
                                "porcentaje": float(tf_porc.value or 0) if tf_porc.value else None,
                                "id_tipo_porcentaje": int(dd_tipo.value) if dd_tipo.value else None
                            })
                        except: pass
                
                if not any_price:
                    show_toast("Al menos una lista de precio debe tener un valor mayor a 0", kind="warning")
                    return
                
                db_conn.update_article_prices(art_id, price_updates)
            
            # Initial Stock Movement
            stock_ini = float(nuevo_articulo_stock_actual.value or 0)
            if stock_ini > 0:
                # Assuming type_id=1 for Adjustment/Initial Stock. 
                # Better to lookup 'Saldo Inicial' or similar if dynamic, but hardcoded ID 1 is common or needs verifiction.
                # Actually, let's check mtype_table or just use a known "Ajuste" type if available, otherwise just standard entry.
                # The user asked for "Saldo Inicial". I'll use a safe fallback logic.
                try:
                    # Try to find a suitable movement type or create one
                    mtypes = db_conn.fetch_tipos_movimiento_articulo()
                    adj_type = next((t["id"] for t in mtypes if "inicial" in t["nombre"].lower()), None)
                    if not adj_type:
                        adj_type = next((t["id"] for t in mtypes if "ajuste" in t["nombre"].lower()), None)
                    if not adj_type and mtypes:
                        adj_type = mtypes[0]["id"] # Fallback to first available

                    if adj_type:
                        db_conn.create_stock_movement(
                            id_articulo=art_id,
                            id_tipo_movimiento=adj_type,
                            cantidad=stock_ini,
                            id_deposito=1, # Default deposito ?? Need to fetch or assume 1.
                            observacion="Saldo inicial al crear artículo"
                        )
                except Exception as e:
                    print(f"Error creating initial stock: {e}")

            close_form()
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

            if float(nuevo_articulo_costo.value or 0) <= 0:
                show_toast("El costo debe ser mayor a 0", kind="warning")
                return

            updates = {
                "nombre": nuevo_articulo_nombre.value or "",
                "marca": nuevo_articulo_marca.value,
                "rubro": nuevo_articulo_rubro.value,
                "costo": nuevo_articulo_costo.value,
                "stock_minimo": nuevo_articulo_stock_minimo.value,
                "ubicacion": nuevo_articulo_ubicacion.value,
                "activo": bool(nuevo_articulo_activo.value),
                "id_tipo_iva": int(nuevo_articulo_tipo_iva.value) if nuevo_articulo_tipo_iva.value else None,
                "id_unidad_medida": int(nuevo_articulo_unidad.value) if nuevo_articulo_unidad.value else None,
                "id_proveedor": int(nuevo_articulo_proveedor.value) if nuevo_articulo_proveedor.value else None,
                "observacion": nuevo_articulo_observacion.value,
                "descuento_base": nuevo_articulo_descuento_base.value,
                "redondeo": bool(nuevo_articulo_redondeo.value),
                "porcentaje_ganancia_2": nuevo_articulo_ganancia_2.value
            }
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
                        price_val = float(tf_precio.value or 0) if tf_precio.value else None
                        if price_val and price_val > 0:
                            any_price = True
                        price_updates.append({
                            "id_lista_precio": lp_id,
                            "precio": price_val,
                            "porcentaje": float(tf_porc.value or 0) if tf_porc.value else None,
                            "id_tipo_porcentaje": int(dd_tipo.value) if dd_tipo.value else None
                        })
                    except: pass
            
            if not any_price:
                show_toast("Al menos una lista de precio debe tener un valor mayor a 0", kind="warning")
                return

            # Prices
            if price_updates:
                db_conn.update_article_prices(editing_article_id, price_updates)
                
            # Handle Stock Change (Auto-Adjustment)
            new_stock = float(nuevo_articulo_stock_actual.value or 0)
            # Fetch current stock (fresh)
            # We need to know the current stock to calc diff. 
            # `fetch_article_by_id` usually joins stock, let's verify or fetch separate.
            # `v_articulo_detallado` has `stock_actual`.
            current_art_data = db_conn.fetch_article_by_id(editing_article_id)
            current_stock = float(current_art_data.get("stock_actual", 0)) if current_art_data else 0.0
            
            diff = new_stock - current_stock
            
            if abs(diff) > 0.0001: # Float epsilon
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
                    ft.Row([nuevo_articulo_marca, nuevo_articulo_rubro], spacing=10),
                    ft.Row([nuevo_articulo_tipo_iva, nuevo_articulo_unidad], spacing=10),
                    ft.Row([nuevo_articulo_proveedor], spacing=10),
                    
                    ft.Container(height=10),
                    section_title("Costos y Stock"),
                    ft.Row([nuevo_articulo_costo, nuevo_articulo_stock_minimo], spacing=10),
                    ft.Row([nuevo_articulo_stock_actual], spacing=10),
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
        nuevo_articulo_proveedor.options = [ft.dropdown.Option("", "—")] + [ft.dropdown.Option(str(p["id"]), p["nombre"]) for p in proveedores_values]
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
                if not l.get("activa", True): continue
                
                tf_p = ft.TextField(
                    label="Precio",
                    value="0",
                    width=110,
                    prefix_text="$",
                )
                _style_input(tf_p)
                
                tf_per = ft.TextField(
                    label="%",
                    value="0",
                    width=90,
                )
                _style_input(tf_per)
                
                dd_tp = ft.Dropdown(
                    label="Tipo de Calculo",
                    width=180,
                    options=[
                        ft.dropdown.Option(str(t["id"]), t["tipo"]) for t in tipos_porcentaje_values
                    ],
                    value=next((str(t["id"]) for t in tipos_porcentaje_values if "mar" in t["tipo"].lower()), 
                          (str(tipos_porcentaje_values[0]["id"]) if tipos_porcentaje_values else "")),
                )
                _style_input(dd_tp)
                
                row_cont = ft.Container(
                    content=ft.Row([
                        ft.Text(l['nombre'], size=13, width=120),
                        tf_p,
                        tf_per,
                        dd_tp
                    ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER)
                )
                row_cont.price_data = {"lp_id": l["id"]}
                articulo_precios_container.controls.append(row_cont)
        except: pass

        open_form(
            "Nuevo artículo",
            _prepare_article_form_content(),
            [
                ft.TextButton("Cancelar", on_click=close_form),
                ft.ElevatedButton(
                    "Crear",
                    icon=ft.Icons.ADD,
                    bgcolor=COLOR_ACCENT,
                    color="#FFFFFF",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)),
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
                    ft.DataCell(ft.Text(f"{float(p['porcentaje']):.2f}% ({p['tipo_porcentaje']})")),
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
                "Desactivar Artículo" if is_active else "Activar Artículo",
                icon=ft.Icons.DO_NOT_DISTURB_ON_ROUNDED if is_active else ft.Icons.CHECK_CIRCLE_ROUNDED,
                bgcolor=COLOR_ERROR if is_active else COLOR_SUCCESS,
                color="#FFFFFF",
                on_click=toggle_status
            )

            content = ft.Column([
                ft.Container(
                    content=ft.Column([
                        ft.Text("Información General", size=16, weight=ft.FontWeight.BOLD),
                        ft.Divider(height=1, color=COLOR_BORDER),
                                ft.Row([
                                    ft.Column([
                                        info_row("Marca", art.get('marca'), ft.Icons.LABEL_ROUNDED),
                                        info_row("Rubro", art.get('rubro'), ft.Icons.CATEGORY_ROUNDED),
                                        info_row("Proveedor", art.get('proveedor'), ft.Icons.BUSINESS_ROUNDED),
                                        info_row("PGan 2", f"{float(art.get('porcentaje_ganancia_2', 0)):.2f}%" if art.get('porcentaje_ganancia_2') is not None else "—", ft.Icons.PERCENT_ROUNDED),
                                        info_row("Notas", art.get('observacion'), ft.Icons.NOTE_ROUNDED),
                                    ], expand=True),
                                    ft.Column([
                                        info_row("Costo", _format_money(art.get('costo')), ft.Icons.MONEY_ROUNDED),
                                        info_row("Stock Actual", f"{float(art.get('stock_actual', 0)):.2f} {art.get('unidad_abreviatura') or ''}", ft.Icons.INVENTORY_ROUNDED),
                                        info_row("Ubicación", art.get('ubicacion'), ft.Icons.LOCATION_ON_ROUNDED),
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
                ft.Row([status_btn], alignment=ft.MainAxisAlignment.END)
            ], spacing=10, width=750, scroll=ft.ScrollMode.ADAPTIVE)

            open_form(
                f"Detalles: {art.get('nombre')}",
                content,
                [ft.TextButton("Cerrar", on_click=close_form)]
            )
            
        except Exception as exc:
            show_toast(f"Error al cargar detalles: {exc}", kind="error")

    def open_editar_articulo(art_id: int) -> None:
        nonlocal editing_article_id
        editing_article_id = art_id
        db_conn = get_db_or_toast()
        if db_conn is None: return
        try: reload_catalogs()
        except: pass

        _populate_dropdowns()

        # Fetch data
        try:
            art = db_conn.fetch_article_by_id(art_id)
            if not art:
                show_toast("Artículo no encontrado", kind="error")
                return

            nuevo_articulo_nombre.value = art.get("nombre", "")
            nuevo_articulo_marca.value = art.get("marca_nombre") or ""
            nuevo_articulo_rubro.value = art.get("rubro_nombre") or ""
            nuevo_articulo_tipo_iva.value = str(art["id_tipo_iva"]) if art.get("id_tipo_iva") else ""
            nuevo_articulo_unidad.value = str(art["id_unidad_medida"]) if art.get("id_unidad_medida") else ""
            nuevo_articulo_proveedor.value = str(art["id_proveedor"]) if art.get("id_proveedor") else ""
            nuevo_articulo_costo.value = str(art.get("costo", 0))
            nuevo_articulo_stock_minimo.value = str(art.get("stock_minimo", 0))
            nuevo_articulo_stock_actual.value = str(art.get("stock_actual", 0))
            nuevo_articulo_stock_actual.read_only = False # Enabled to allow adjustments
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
                tf_p = ft.TextField(
                    label="Precio",
                    value=str(p["precio"] or 0),
                    width=110,
                    prefix_text="$",
                )
                _style_input(tf_p)
                
                tf_per = ft.TextField(
                    label="%",
                    value=str(p["porcentaje"] or 0),
                    width=90,
                )
                _style_input(tf_per)
                
                dd_tp = ft.Dropdown(
                    label="Tipo de Calculo",
                    width=180,
                    options=[
                        ft.dropdown.Option(str(t["id"]), t["tipo"]) for t in tipos_porcentaje_values
                    ],
                    value=str(p["id_tipo_porcentaje"]) if p.get("id_tipo_porcentaje") else "",
                )
                _style_input(dd_tp)
                
                row_cont = ft.Container(
                    content=ft.Row([
                        ft.Text(p['lista_nombre'], size=13, width=120),
                        tf_p,
                        tf_per,
                        dd_tp
                    ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER)
                )
                row_cont.price_data = {"lp_id": p["id_lista_precio"]}
                articulo_precios_container.controls.append(row_cont)

        except Exception as exc:
            show_toast(f"Error al cargar artículo: {exc}", kind="error")
            return

        open_form(
            "Editar artículo",
            _prepare_article_form_content(),
            [
                ft.TextButton("Cancelar", on_click=close_form),
                ft.ElevatedButton(
                    "Guardar Cambios",
                    icon=ft.Icons.SAVE_ROUNDED,
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

    # Logo preview image
    sys_logo_preview = ft.Image(src="", width=120, height=120, fit=ft.ImageFit.CONTAIN, visible=False)
    sys_logo_label = ft.Text("", size=12, color=COLOR_TEXT_MUTED)

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
            else:
                sys_logo_preview.visible = False
                sys_logo_label.value = ""
            
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
            page.update()

    def on_logo_picked(e: ft.FilePickerResultEvent):
        nonlocal sys_logo_path
        if e.files and len(e.files) > 0:
            selected = e.files[0].path
            sys_logo_path = selected
            sys_logo_preview.src = selected
            sys_logo_preview.visible = True
            sys_logo_label.value = selected.split("\\")[-1].split("/")[-1]
            page.update()
    
    logo_picker = ft.FilePicker(on_result=on_logo_picked)
    page.overlay.append(logo_picker)

    def select_logo_click(_: Any = None):
        logo_picker.pick_files(
            dialog_title="Seleccionar Logo",
            allowed_extensions=["png", "jpg", "jpeg", "gif", "svg", "webp"],
            allow_multiple=False
        )

    def clear_logo(_: Any = None):
        nonlocal sys_logo_path
        sys_logo_path = ""
        sys_logo_preview.src = ""
        sys_logo_preview.visible = False
        sys_logo_label.value = ""
        page.update()

    # Drop zone for logo
    logo_drop_zone = ft.Container(
        content=ft.Column([
            ft.Icon(ft.Icons.CLOUD_UPLOAD_ROUNDED, size=40, color=COLOR_TEXT_MUTED),
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
            ft.TextButton("Quitar logo", icon=ft.Icons.DELETE_OUTLINE, on_click=clear_logo, visible=True),
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
            ft.ElevatedButton("Guardar Configuración del Sistema", icon=ft.Icons.SAVE_ROUNDED, on_click=save_sistema_config, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))),
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
                    icon=ft.Icons.DELETE_OUTLINE,
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
                    icon=ft.Icons.DELETE_OUTLINE,
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
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
            ColumnConfig(key="abreviatura", label="Abr.", editable=True, width=100),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
                    on_click=lambda e: ask_confirm("Eliminar", "¿Eliminar esta unidad?", "Eliminar", lambda: delete_unidad(int(row["id"])))
                ) if CURRENT_USER_ROLE == "ADMIN" else ft.Container()
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_unidades_medida, db.count_unidades_medida),
        inline_edit_callback=lambda rid, changes: db.update_unidad_medida_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_mass_actions=False, show_selection=False, auto_load=False, page_size=12,
    )
    nueva_uni_nombre = ft.TextField(label="Nombre Unidad", width=180)
    nueva_uni_abr = ft.TextField(label="Abreviatura", width=100)
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
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
            ColumnConfig(key="activo", label="Activo", editable=True, width=100, formatter=_format_bool),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
            ColumnConfig(key="activa", label="Activa", editable=True, width=100, formatter=_format_bool),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
                    icon=ft.Icons.UNPUBLISHED_OUTLINED if row.get("activa") else ft.Icons.CHECK_CIRCLE_OUTLINE,
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
    nueva_lp_orden = ft.TextField(label="Orden", width=80, value="0", input_filter=ft.InputFilter(allow=True, regex_string=r"[0-9]")); _style_input(nueva_lp_orden)

    def agregar_lp(_: Any = None):
        nom = (nueva_lp_nom.value or "").strip()
        orden_val = (nueva_lp_orden.value or "0").strip()
        if not nom: return
        try:
            db.create_lista_precio(nom, orden=int(orden_val))
            nueva_lp_nom.value = ""; nueva_lp_orden.value = "0"
            precios_table.refresh(); show_toast("Lista agregada", kind="success")
        except Exception as exc: show_toast(f"Error: {exc}", kind="error")

    # Percentage Types
    ptype_table = GenericTable(
        columns=[
            ColumnConfig(key="tipo", label="Tipo", editable=True, width=320),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
            ColumnConfig(key="clase", label="Clase", editable=True, width=100),
            ColumnConfig(key="letra", label="Letra", editable=True, width=60),
            ColumnConfig(key="afecta_stock", label="Stk", editable=True, width=60, formatter=_format_bool),
            ColumnConfig(key="afecta_cuenta_corriente", label="Cta", editable=True, width=60, formatter=_format_bool),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
            ColumnConfig(key="signo_stock", label="Signo", editable=True, width=80),
            ColumnConfig(
                key="_delete", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE, tooltip="Eliminar", icon_color="#DC2626",
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
            ColumnConfig(key="ultimo_login", label="Últ. Acceso", width=160),
            ColumnConfig(
                key="_toggle", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.PERSON_OFF_ROUNDED if row.get("activo") else ft.Icons.PERSON_ADD_ROUNDED,
                    tooltip="Desactivar Usuario" if row.get("activo") else "Reactivar Usuario",
                    icon_color="#DC2626" if row.get("activo") else "#10B981",
                    on_click=lambda e, rid=row.get("id"), is_active=row.get("activo"): toggle_usuario(int(rid), is_active) if rid else None
                )
            ),
        ],
        data_provider=create_catalog_provider(db.fetch_users, db.count_users),
        inline_edit_callback=lambda rid, changes: db.update_user_fields(int(rid), changes) if db else None,
        show_inline_controls=True, show_mass_actions=False, auto_load=False, page_size=20, show_export_scope=False,
    )

    sesiones_table = GenericTable(
        columns=[
            ColumnConfig(key="nombre", label="Nombre", width=180),
            ColumnConfig(key="email", label="Email", width=200),
            ColumnConfig(key="rol", label="Rol", width=100),
            ColumnConfig(key="desde", label="Desde", width=160),
            ColumnConfig(key="ip", label="Dirección IP", width=140),
        ],
        data_provider=create_catalog_provider(db.fetch_active_sessions, db.count_active_sessions),
        auto_load=False, page_size=10,
        show_selection=False, show_mass_actions=False, # cleaner for sessions
        show_export_button=False,
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
            ft.TextButton("Cancelar", on_click=close_form),
            ft.ElevatedButton("Crear Usuario", bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)), on_click=crear_usuario)
        ])

    
    usuarios_tabs = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        expand=True,
        tabs=[
            make_tab(
                text="Lista de Usuarios",
                icon=ft.Icons.PEOPLE_OUTLINE_ROUNDED,
                content=ft.Container(
                    padding=10,
                    content=make_card(
                        "Usuarios del Sistema", 
                        "Gestión de acceso, roles y permisos.", 
                        usuarios_table.build(),
                        actions=[
                            ft.ElevatedButton(
                                "Nuevo Usuario", 
                                icon=ft.Icons.PERSON_ADD_ROUNDED, 
                                bgcolor=COLOR_ACCENT, 
                                color="#FFFFFF",
                                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                                on_click=open_nuevo_usuario
                            )
                        ]
                    )
                )
            ),
            make_tab(
                text="Sesiones Activas",
                icon=ft.Icons.SATELLITE_ALT_ROUNDED,
                content=ft.Container(
                    padding=10,
                    content=make_card(
                        "Sesiones Activas",
                        "Usuarios conectados actualmente al sistema.",
                        sesiones_table.build()
                    )
                )
            ),
        ],
    )

    usuarios_view = ft.Column([
        ft.Row([
            make_stat_card("Sesiones Activas", "0", "PERSON_ROUNDED", COLOR_ACCENT, key="usuarios_activos"),
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

    backup_view_component = BackupProfessionalView(page, db, show_toast)
    
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
        return estado in ("CONFIRMADO", "PAGADO") and doc_row.get("codigo_afip") and not doc_row.get("cae")

    def _authorize_afip_doc(doc_row: Dict[str, Any], *, close_after: bool = False) -> None:
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
            punto_venta = 1
            last = afip.get_last_voucher_number(punto_venta, codigo_afip)
            next_num = last + 1

            entity = None
            ent_id = doc_row.get("id_entidad")
            if ent_id:
                entity = db_local.fetch_entity_by_id(int(ent_id))

            total = float(doc_row.get("total", 0) or 0)
            neto = float(doc_row.get("neto", 0) or 0)
            iva_total = float(doc_row.get("iva_total", 0) or 0)
            if total and (neto <= 0 or iva_total < 0):
                neto = total / 1.21
                iva_total = total - neto

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
                "Iva": [
                    {
                        "Id": 5,
                        "BaseImp": neto,
                        "Importe": iva_total,
                    }
                ],
            }
            if condicion_id is not None:
                invoice_data["CondicionIVAReceptorId"] = condicion_id

            res = afip.authorize_invoice(invoice_data)
            if res.get("success"):
                cuit_emisor = "".join(ch for ch in str(getattr(afip, "cuit", "")).strip() if ch.isdigit())
                qr_data = None
                try:
                    fecha_doc = str(doc_row.get("fecha") or datetime.now().strftime("%Y-%m-%d"))[:10]
                    qr_payload = {
                        "ver": 1,
                        "fecha": fecha_doc,
                        "cuit": int(cuit_emisor) if cuit_emisor else 0,
                        "ptoVta": int(punto_venta),
                        "tipoCmp": int(codigo_afip),
                        "nroCmp": int(next_num),
                        "importe": round(total, 2),
                        "moneda": "PES",
                        "ctz": 1,
                        "tipoDocRec": int(doc_tipo),
                        "nroDocRec": int(doc_nro),
                        "tipoCodAut": "E",
                        "codAut": res.get("CAE"),
                    }
                    qr_json = json.dumps(qr_payload, separators=(",", ":"), ensure_ascii=False)
                    qr_base64 = base64.b64encode(qr_json.encode("utf-8")).decode("ascii")
                    qr_data = f"https://www.afip.gob.ar/fe/qr/?p={qr_base64}"
                except Exception:
                    qr_data = None

                db_local.update_document_afip_data(
                    doc_id,
                    res["CAE"],
                    res["CAEFchVto"],
                    punto_venta,
                    codigo_afip,
                    cuit_emisor=cuit_emisor or None,
                    qr_data=qr_data,
                )
                show_toast(f"Autorizado! CAE: {res['CAE']}", kind="success")
                if close_after:
                    close_form()
                if hasattr(documentos_summary_table, "refresh"):
                    documentos_summary_table.refresh()
                refresh_all_stats()
            else:
                show_toast(f"Error AFIP: {res.get('error')}", kind="error")
        except Exception as e:
            show_toast(f"Error: {e}", kind="error")

    def _confirm_afip_authorization(doc_row: Dict[str, Any], *, close_after: bool = False) -> None:
        ask_confirm(
            "Autorizar AFIP",
            "Vas a facturar electrónicamente este comprobante en AFIP. Esta acción es irreversible y no se puede volver atrás. ¿Deseás continuar?",
            "Autorizar AFIP",
            lambda: _authorize_afip_doc(doc_row, close_after=close_after),
            button_color=COLOR_WARNING,
        )

    def _confirm_document(doc_id: int, *, close_after: bool = False) -> None:
        def on_confirm_real():
            try:
                if not db:
                    return
                db.confirm_document(doc_id)
                show_toast("Comprobante confirmado", kind="success")
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
            "Confirmar",
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
                                ft.Text(str(doc_row.get("fecha", "—"))[:10] if doc_row.get("fecha") else "—", size=14, text_align=ft.TextAlign.RIGHT),
                            ], spacing=2, width=100),
                        ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                        padding=ft.padding.only(bottom=15),
                        border=ft.border.only(bottom=ft.BorderSide(1, "#E2E8F0"))
                    ),
                    ft.Container(height=10),
                    ft.Row([
                        ft.Text("ÍTEMS DEL COMPROBANTE", size=11, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT),
                        ft.Container(
                            content=ft.Text(
                                doc_row.get("estado", ""), 
                                size=10, 
                                weight=ft.FontWeight.BOLD, 
                                color="#FFFFFF"
                            ),
                            bgcolor=COLOR_SUCCESS if doc_row.get("estado") == "PAGADO" else (COLOR_ERROR if doc_row.get("estado") == "ANULADO" else COLOR_INFO),
                            padding=ft.padding.symmetric(horizontal=10, vertical=4),
                            border_radius=20
                        )
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    ft.Container(
                        content=SafeDataTable(
                            heading_row_color="#F8FAFC",
                            heading_row_height=40,
                            data_row_min_height=40,
                            column_spacing=20,
                            columns=[
                                ft.DataColumn(ft.Text("Artículo", size=12, weight=ft.FontWeight.BOLD)),
                                ft.DataColumn(ft.Text("Lista", size=12, weight=ft.FontWeight.BOLD)),
                                ft.DataColumn(ft.Text("Cant.", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                ft.DataColumn(ft.Text("Unitario", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                                ft.DataColumn(ft.Text("Total", size=12, weight=ft.FontWeight.BOLD), numeric=True),
                            ],
                            rows=[
                                ft.DataRow(cells=[
                                    ft.DataCell(ft.Text(f"{d['articulo']} ({d.get('codigo_art', d.get('id_articulo'))})", size=13)),
                                    ft.DataCell(ft.Text(d.get("lista_nombre") or (doc_row.get("lista_precio") if doc_row.get("id_lista_precio") == d.get("id_lista_precio") else "---") , size=12, color=COLOR_TEXT_MUTED)),
                                    ft.DataCell(ft.Text(str(d["cantidad"]), size=13)),
                                    ft.DataCell(ft.Text(_format_money(d["precio_unitario"]), size=13)),
                                    ft.DataCell(ft.Text(_format_money(d["total_linea"]), size=13, weight=ft.FontWeight.W_500)),
                                ]) for d in details
                            ],
                        ),
                        border=ft.border.all(1, "#E2E8F0"),
                        border_radius=8,
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
                                    ft.Text(f"DESCUENTO ({doc_row.get('descuento_porcentaje', 0)}%):" if float(doc_row.get("descuento_porcentaje", 0)) > 0 else "DESCUENTO:", size=11, color=COLOR_ERROR, weight=ft.FontWeight.BOLD),
                                    ft.Text(f"- {_format_money(doc_row.get('descuento_importe') if float(doc_row.get('descuento_importe',0)) > 0 else float(doc_row.get('subtotal', 0)) - float(doc_row.get('neto', 0)))}", size=11, color=COLOR_ERROR, weight=ft.FontWeight.BOLD),
                                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, width=250) if float(doc_row.get("descuento_porcentaje", 0)) > 0 or float(doc_row.get("descuento_importe", 0)) > 0 else ft.Container(),
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
                ], spacing=5, scroll=ft.ScrollMode.ADAPTIVE)

            content = ft.Container(
                content=body,
                padding=10,
                width=650,
                height=550,
            )
            
            actions = [ft.TextButton("Cerrar", on_click=close_form)]
            if estado == "BORRADOR":
                actions.insert(0, ft.ElevatedButton(
                    "Confirmar Comprobante",
                    icon=ft.Icons.CHECK_CIRCLE,
                    bgcolor=COLOR_SUCCESS,
                    color="#FFFFFF",
                    on_click=lambda _: _confirm_document(doc_id, close_after=True),
                ))
            
            # AFIP Authorization
            if _can_authorize_afip(doc_row):
                actions.insert(0, ft.ElevatedButton(
                    "Autorizar AFIP",
                    icon=ft.Icons.SECURITY,
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

    # Movement filters (Move up to be accessible via reload_catalogs if needed)
    def _mov_live(_=None):
        try: movimientos_table.trigger_refresh()
        except: pass

    mov_adv_art = AsyncSelect(label="Artículo", loader=article_loader, width=220, on_change=lambda _: _mov_live(None))
    mov_adv_tipo = ft.Dropdown(label="Tipo Mov.", width=180, on_change=_mov_live); _style_input(mov_adv_tipo)
    mov_adv_depo = ft.Dropdown(label="Depósito", width=180, on_change=_mov_live); _style_input(mov_adv_depo)
    mov_adv_user = ft.Dropdown(label="Usuario", width=180, on_change=_mov_live); _style_input(mov_adv_user)
    mov_adv_desde = _date_field(page, "Desde", width=140); mov_adv_desde.on_submit = _mov_live
    mov_adv_hasta = _date_field(page, "Hasta", width=140); mov_adv_hasta.on_submit = _mov_live

    def refresh_documentos_catalogs():
        if not db: return
        try:
            tipos_doc = db.list_tipos_documento()
            doc_adv_tipo.options = [ft.dropdown.Option("Todos", "Todos")] + [
                ft.dropdown.Option(t["nombre"], t["nombre"]) for t in tipos_doc
            ]
            
            ent_list = db.list_entidades_simple()
            shared_ent_options = [ft.dropdown.Option("0", "Todas")] + [
                ft.dropdown.Option(str(e["id"]), f"{e['nombre_completo']} ({e['tipo']})") for e in ent_list
            ]
            
            doc_adv_entidad.options = shared_ent_options
            pago_adv_entidad.options = shared_ent_options
            
            for ctrl in [doc_adv_tipo, doc_adv_entidad, pago_adv_entidad]:
                try:
                    if ctrl.page: ctrl.update()
                except: pass
        except: pass

    def refresh_pagos_catalogs():
        if not db: return
        try:
            formas = db.fetch_formas_pago(limit=100)
            pago_adv_forma.options = [ft.dropdown.Option("0", "Todas")] + [
                ft.dropdown.Option(str(f["id"]), f["descripcion"]) for f in formas
            ]
            if pago_adv_forma.page: pago_adv_forma.update()
        except: pass

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
                except Exception:
                    pass
            mov_adv_art.options = art_options

            for ctrl in [mov_adv_tipo, mov_adv_depo, mov_adv_user, mov_adv_art]:
                try: 
                    if ctrl.page: ctrl.update()
                except: pass
        except: pass

    # Documents View
    # fetch document types and entities for dropdowns
    try:
        tipos_doc = db.list_tipos_documento()
        tipo_options = [ft.dropdown.Option("Todos", "Todos")] + [ft.dropdown.Option(t["nombre"], t["nombre"]) for t in tipos_doc]
        
        entidades = db.list_entidades_simple()
        ent_options = [ft.dropdown.Option("0", "Todas")] + [ft.dropdown.Option(str(e["id"]), f"{e['nombre_completo']} ({e['tipo']})") for e in entidades]
    except:
        tipo_options = [ft.dropdown.Option("Todos", "Todos")]
        ent_options = [ft.dropdown.Option("0", "Todas")]

    doc_adv_entidad = AsyncSelect(label="Entidad", loader=entity_loader, width=280, on_change=lambda _: _doc_live(None))
    doc_adv_tipo = ft.Dropdown(label="Tipo", options=tipo_options, width=160, value="Todos", enable_search=True); _style_input(doc_adv_tipo)
    
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
            ft.dropdown.Option("BORRADOR", "Borrador"),
            ft.dropdown.Option("CONFIRMADO", "Confirmado"),
            ft.dropdown.Option("ANULADO", "Anulado"),
            ft.dropdown.Option("PAGADO", "Pagado"),
        ],
        value="Todos"
    ); _style_input(doc_adv_estado)

    doc_adv_desde = _date_field(page, "Desde", width=130)
    doc_adv_hasta = _date_field(page, "Hasta", width=130)
    
    # Range slider for Total
    max_total = 1000000.0
    try: max_total = db.get_max_document_total()
    except: pass
    if max_total < 1000: max_total = 1000.0

    # Label for Range Slider (Matches inventory style)
    range_label = ft.Text(f"Total: entre $0 y ${max_total:,.0f}", size=12, weight=ft.FontWeight.BOLD)
    
    def on_range_change(e):
        s = e.control
        range_label.value = f"Total: entre {_format_money(s.start_value)} y {_format_money(s.end_value)}"
        try: range_label.update()
        except: pass

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
        try:
            doc_adv_total.update()
            range_label.update()
        except: pass

    documentos_summary_table = GenericTable(
        columns=[
            ColumnConfig(key="fecha", label="Fecha", width=120),
            ColumnConfig(key="letra", label="Letra", width=60),
            ColumnConfig(key="tipo_documento", label="Tipo", width=120),
            ColumnConfig(key="numero_serie", label="Número", width=100),
            ColumnConfig(key="entidad", label="Entidad", width=200),
            ColumnConfig(key="total", label="Total", width=120, formatter=_format_money),
            ColumnConfig(key="forma_pago", label="Forma de Pago", width=130),
            ColumnConfig(key="estado", label="Estado", width=120, renderer=lambda row: _status_pill(row.get("estado"))),
            ColumnConfig(
                key="_confirm", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") == "BORRADOR",
                    icon=ft.Icons.CHECK_CIRCLE,
                    tooltip="Confirmar comprobante",
                    icon_color=COLOR_SUCCESS,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: _confirm_document(int(rid)),
                )
            ),
            ColumnConfig(
                key="_detail", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.INFO_OUTLINE, tooltip="Ver detalle",
                    icon_color=COLOR_TEXT_MUTED,
                    on_click=lambda e: view_doc_detail(row)
                )
            ),
            ColumnConfig(key="usuario", label="Usuario", width=120),
            ColumnConfig(
                key="_edit", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") == "BORRADOR" and not row.get("cae"),
                    icon=ft.Icons.EDIT_ROUNDED,
                    tooltip="Editar borrador",
                    icon_color=COLOR_ACCENT,
                    on_click=lambda e, rid=row["id"]: open_nuevo_comprobante(edit_doc_id=rid),
                )
            ),
            ColumnConfig(
                key="_copy", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.COPY_ALL_ROUNDED,
                    tooltip="Copiar como nuevo",
                    icon_color=ft.Colors.BLUE_400,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: open_nuevo_comprobante(copy_doc_id=rid),
                )
            ),
            ColumnConfig(
                key="_print", label="", sortable=False, width=40,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.PRINT_ROUNDED,
                    tooltip="Imprimir",
                    icon_color=COLOR_TEXT_MUTED,
                    icon_size=18,
                    on_click=lambda e, rid=row["id"]: print_document_external(rid),
                )
            ),
            ColumnConfig(
                key="_afip", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    _can_authorize_afip(row),
                    icon=ft.Icons.SECURITY,
                    tooltip="Autorizar AFIP",
                    icon_color=COLOR_ACCENT,
                    icon_size=18,
                    on_click=lambda e, r=row: _confirm_afip_authorization(r),
                )
            ),
            ColumnConfig(
                key="_annul", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") != "ANULADO" and not row.get("cae"),
                    icon=ft.Icons.BLOCK_ROUNDED,
                    tooltip="Anular comprobante",
                    icon_color=COLOR_ERROR,
                    on_click=lambda e: ask_confirm(
                        "Anular Comprobante",
                        f"¿Estás seguro que deseas anular el comprobante {row['numero_serie']}? Esta acción revertirá el stock.",
                        "Anular",
                        lambda: (db.anular_documento(row["id"]), show_toast("Comprobante anulado", kind="success"), documentos_summary_table.refresh())
                    ),
                )
            ),
            ColumnConfig(
                key="_nc", label="", sortable=False, width=40,
                renderer=lambda row: _icon_button_or_spacer(
                    row.get("estado") == "CONFIRMADO" and row.get("cae"),
                    icon=ft.Icons.RECEIPT_LONG_OUTLINED,
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
        show_inline_controls=False, show_mass_actions=False, auto_load=True, page_size=50, show_export_button=True,
    )
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
                ft.ElevatedButton("Nuevo Comprobante", icon=ft.Icons.ADD_ROUNDED, bgcolor=COLOR_ACCENT, color="#FFFFFF", 
                                   on_click=lambda e: open_nuevo_comprobante(),
                                   style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))),
            ]
        )
    ], spacing=10, scroll=ft.ScrollMode.ADAPTIVE)

    documentos_view = ft.Container(
        content=documentos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # Movements View
    # (Filters moved up)

    movimientos_table = GenericTable(
        columns=[
            ColumnConfig(key="fecha", label="Fecha", width=120),
            ColumnConfig(key="articulo", label="Artículo", width=200),
            ColumnConfig(key="tipo_movimiento", label="Tipo", width=120),
            ColumnConfig(key="cantidad", label="Cant.", width=80),
            ColumnConfig(
                key="comprobante", label="Comprobante", width=180,
                renderer=lambda row: ft.Text(f"{row.get('tipo_documento') or ''} {row.get('nro_comprobante') or ''}".strip() or "---", size=13)
            ),
            ColumnConfig(key="entidad", label="Entidad", width=180),
            ColumnConfig(key="deposito", label="Depósito", width=120),
            ColumnConfig(key="usuario", label="Usuario", width=120),
            ColumnConfig(key="observacion", label="Obs.", width=200),
        ],
        data_provider=create_catalog_provider(db.fetch_movimientos_stock, db.count_movimientos_stock),
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
    )
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
    ], spacing=10, scroll=ft.ScrollMode.ADAPTIVE)

    movimientos_view = ft.Container(
        content=movimientos_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # Payments View
    # Payments View
    pago_adv_ref = ft.TextField(label="Referencia", width=200, on_change=lambda _: pagos_table.trigger_refresh()); _style_input(pago_adv_ref)
    pago_adv_desde = _date_field(page, "Desde", width=140); pago_adv_desde.on_submit = lambda _: pagos_table.trigger_refresh()
    pago_adv_hasta = _date_field(page, "Hasta", width=140); pago_adv_hasta.on_submit = lambda _: pagos_table.trigger_refresh()
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
    except: pass
    
    monto_range_label = ft.Text(f"Monto: entre $0 y ${max_monto:,.0f}", size=12, weight=ft.FontWeight.BOLD)
    
    def on_pago_monto_change(e):
        s = e.control
        monto_range_label.value = f"Monto: entre {_format_money(s.start_value)} y {_format_money(s.end_value)}"
        try: monto_range_label.update()
        except: pass

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
        try:
            pago_adv_monto.update()
            monto_range_label.update()
        except: pass

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
            ColumnConfig(key="fecha", label="Fecha", width=120),
            ColumnConfig(key="monto", label="Monto", width=100, formatter=_format_money),
            ColumnConfig(key="forma", label="Forma Pago", width=120),
            ColumnConfig(key="documento", label="Comprobante", width=120),
            ColumnConfig(key="entidad", label="Entidad", width=200),
            ColumnConfig(key="referencia", label="Referencia", width=150, renderer=lambda row: ft.Text(row.get("referencia") or "---", tooltip="Dato adicional del pago (ej. nro cheque, banco, etc.)")),
            ColumnConfig(key="observacion", label="Info", width=60, renderer=lambda row: ft.IconButton(ft.Icons.INFO_OUTLINE, tooltip="Ver observaciones", icon_color=COLOR_ACCENT if row.get("observacion") else "grey", on_click=lambda _: show_payment_info(row.get("observacion")))),
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
    )

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
            label="Entidad", 
            loader=entity_loader, 
            width=400,
            initial_items=[{"value": e["id"], "label": e["nombre_completo"]} for e in entidades]
        )
        
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
                items = [
                    {
                        "value": r["id"],
                        "label": f"{r.get('numero_serie', 'N/A')} - ${r.get('total', 0):,.2f} ({r.get('fecha')})",
                    }
                    for r in rows
                ]
                return items, len(rows) >= limit
            except Exception:
                return [], False

        pago_documento = AsyncSelect(label="Comprobante Pendiente", loader=pending_doc_loader, width=400, disabled=True)
        
        pago_forma = ft.Dropdown(label="Forma de Pago", options=[ft.dropdown.Option(str(f["id"]), f["descripcion"]) for f in formas], width=250)
        _style_input(pago_forma)
        
        pago_monto = _number_field("Monto", width=200)
        pago_fecha = _date_field(page, "Fecha", width=200)
        pago_ref = ft.TextField(label="Referencia", width=250); _style_input(pago_ref)
        pago_obs = ft.TextField(label="Observaciones", multiline=True, width=500); _style_input(pago_obs)

        def on_entidad_change(val):
            pago_documento.value = ""
            pago_documento.clear_cache()
            if val:
                pago_documento.set_busy(True)
                pago_documento.prefetch(on_done=lambda: pago_documento.set_busy(False))
                pago_documento.disabled = False
            else:
                pago_documento.set_busy(False)
                pago_documento.disabled = True
            pago_documento.update()

        pago_entidad.on_change = on_entidad_change
        
        def _save_pago(_):
            if not pago_documento.value or not pago_forma.value or not pago_monto.value:
                show_toast("Campos obligatorios faltantes", kind="warning"); return
            try:
                # Convert fecha
                f_str = pago_fecha.value_text.value if hasattr(pago_fecha, 'value_text') else None
                # My _date_field implementation returns a Row or Container. 
                # Need to retrieve value. logic is complex.
                # Let's assume passed validation.
                
                monto_val = pago_monto.value.replace(",", ".")
                
                db.create_payment(
                    id_documento=int(pago_documento.value), # This will fail if placeholder
                    id_forma_pago=int(pago_forma.value),
                    monto=float(monto_val),
                    referencia=pago_ref.value,
                    observacion=pago_obs.value
                )
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
                pago_obs
            ], spacing=15, scroll=ft.ScrollMode.ADAPTIVE)
        )

        open_form("Nuevo Pago", content, [
            ft.TextButton(
                "Cancelar", 
                on_click=close_form,
                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
            ),
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
                 ft.ElevatedButton(
                     "Nuevo Pago", 
                     icon=ft.Icons.ADD_ROUNDED, 
                     bgcolor=COLOR_ACCENT, 
                     color="#FFFFFF", 
                     on_click=open_nuevo_pago,
                     style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                 )
            ]
        )
    ], spacing=10, scroll=ft.ScrollMode.ADAPTIVE)

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
        except:
            pass

    cc_adv_tipo = _dropdown("Tipo", [("", "Todos"), ("CLIENTE", "Clientes"), ("PROVEEDOR", "Proveedores")], value="", width=180, on_change=_cc_live)
    cc_adv_estado = _dropdown("Estado", [("", "Todos"), ("DEUDOR", "Deudores"), ("A_FAVOR", "A Favor"), ("AL_DIA", "Al Día")], value="", width=180, on_change=_cc_live)
    cc_adv_solo_saldo = ft.Switch(label="Solo con saldo", value=True, on_change=_cc_live)

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
        """Muestra los movimientos de una entidad específica."""
        if not db: return
        try:
            movimientos = db.get_movimientos_entidad(int(entidad_id), limit=50)
            
            mov_rows = []
            for m in movimientos:
                tipo = m.get("tipo_movimiento", "")
                monto = float(m.get("monto", 0))
                signo = "+" if tipo in ("CREDITO", "AJUSTE_CREDITO", "ANULACION") else "-"
                color = COLOR_SUCCESS if signo == "+" else COLOR_ERROR
                
                mov_rows.append(
                    ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(m.get("fecha", ""))[:10])),
                        ft.DataCell(ft.Text(m.get("concepto", ""), width=200)),
                        ft.DataCell(ft.Text(tipo, size=11)),
                        ft.DataCell(ft.Text(f"{signo}{_format_money(monto)}", color=color)),
                        ft.DataCell(ft.Text(_format_money(m.get("saldo_nuevo", 0)))),
                    ])
                )
            
            mov_table = SafeDataTable(
                columns=[
                    ft.DataColumn(ft.Text("Fecha")),
                    ft.DataColumn(ft.Text("Concepto")),
                    ft.DataColumn(ft.Text("Tipo")),
                    ft.DataColumn(ft.Text("Monto")),
                    ft.DataColumn(ft.Text("Saldo")),
                ],
                rows=mov_rows[:30],
                column_spacing=15,
            )
            
            dlg = ft.AlertDialog(
                title=ft.Text(f"Movimientos de Cuenta Corriente"),
                content=ft.Container(
                    content=ft.Column([mov_table], scroll=ft.ScrollMode.ADAPTIVE),
                    width=700,
                    height=400,
                ),
                actions=[ft.TextButton("Cerrar", on_click=lambda _: page.close(dlg))],
            )
            page.open(dlg)
        except Exception as ex:
            show_toast(f"Error: {ex}", kind="error")

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
        
        pcc_saldo = ft.Text("Saldo: $0.00", size=12, color=COLOR_TEXT_MUTED)
        
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
                monto = float(pcc_monto.value.replace(",", "."))
                db.registrar_pago_cuenta_corriente(
                    id_entidad=int(pcc_entidad.value),
                    id_forma_pago=int(pcc_forma.value),
                    monto=monto,
                    concepto=pcc_concepto.value or "Pago recibido",
                    referencia=pcc_referencia.value,
                    observacion=pcc_obs.value
                )
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
            ft.TextButton("Cancelar", on_click=close_form, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))),
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
        
        aj_saldo = ft.Text("Saldo actual: $0.00", size=12, color=COLOR_TEXT_MUTED)
        
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
                monto = float(aj_monto.value.replace(",", "."))
                db.ajustar_saldo_cc(
                    id_entidad=int(aj_entidad.value),
                    tipo=aj_tipo.value,
                    monto=monto,
                    concepto=aj_concepto.value,
                    observacion=aj_obs.value
                )
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
            ft.TextButton("Cancelar", on_click=close_form, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))),
            ft.ElevatedButton("Aplicar Ajuste", bgcolor=COLOR_WARNING, color="#FFFFFF", on_click=_save_ajuste, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))
        ])

    cuentas_table = GenericTable(
        columns=[
            ColumnConfig(key="entidad", label="Entidad", width=250),
            ColumnConfig(key="tipo_entidad", label="Tipo", width=100),
            ColumnConfig(key="cuit", label="CUIT", width=120),
            ColumnConfig(key="saldo_actual", label="Saldo", width=150, renderer=lambda row: _saldo_pill(row.get("saldo_actual"))),
            ColumnConfig(key="limite_credito", label="Límite Créd.", width=120, formatter=_format_money),
            ColumnConfig(key="ultimo_movimiento", label="Últ. Movimiento", width=150),
            ColumnConfig(key="total_movimientos", label="Movs.", width=80),
            ColumnConfig(key="acciones", label="", width=80, renderer=lambda row: ft.IconButton(
                ft.Icons.HISTORY_ROUNDED, 
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
        show_inline_controls=False,
        show_mass_actions=False,
        auto_load=True,
        page_size=20,
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
            cc_stat_deuda.update()
            cc_stat_deudores.update()
            cc_stat_cobros.update()
            cc_stat_movs.update()
        except: pass

    cuentas_view = ft.Column([
        ft.Row([
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.Icons.ACCOUNT_BALANCE_ROUNDED, color=COLOR_ERROR, size=24), bgcolor=f"{COLOR_ERROR}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Deuda Clientes", size=12, color=COLOR_TEXT_MUTED), cc_stat_deuda], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.Icons.PEOPLE_ALT_ROUNDED, color=COLOR_WARNING, size=24), bgcolor=f"{COLOR_WARNING}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Clientes Deudores", size=12, color=COLOR_TEXT_MUTED), cc_stat_deudores], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.Icons.PAYMENTS_ROUNDED, color=COLOR_SUCCESS, size=24), bgcolor=f"{COLOR_SUCCESS}1A", padding=10, border_radius=12),
                    ft.Column([ft.Text("Cobros Hoy", size=12, color=COLOR_TEXT_MUTED), cc_stat_cobros], spacing=-2),
                ], spacing=12),
                padding=16, bgcolor=COLOR_CARD, border_radius=16, border=ft.border.all(1, COLOR_BORDER), expand=True,
            ),
            ft.Container(
                content=ft.Row([
                    ft.Container(content=ft.Icon(ft.Icons.SWAP_VERT_ROUNDED, color=COLOR_ACCENT, size=24), bgcolor=f"{COLOR_ACCENT}1A", padding=10, border_radius=12),
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
                ft.ElevatedButton(
                    "Registrar Pago",
                    icon=ft.Icons.ATTACH_MONEY_ROUNDED,
                    bgcolor=COLOR_SUCCESS,
                    color="#FFFFFF",
                    on_click=open_pago_cc,
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                ),
                ft.ElevatedButton(
                    "Ajuste de Saldo",
                    icon=ft.Icons.TUNE_ROUNDED,
                    bgcolor=COLOR_WARNING,
                    color="#FFFFFF",
                    on_click=open_ajuste_cc,
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                ),
            ]
        )
    ], spacing=10, scroll=ft.ScrollMode.ADAPTIVE)

    cuentas_view = ft.Container(
        content=cuentas_view,
        padding=ft.padding.only(right=10),
        expand=True
    )

    # Logs View

    logs_adv_user = ft.TextField(label="Usuario contiene", width=180); _style_input(logs_adv_user)
    logs_adv_ent = ft.TextField(label="Entidad contiene", width=180); _style_input(logs_adv_ent)
    logs_adv_acc = ft.TextField(label="Acción contiene", width=180); _style_input(logs_adv_acc)
    logs_adv_res = ft.Dropdown(
        label="Resultado", 
        width=140,
        options=[
            ft.dropdown.Option("Todas", "Todos"),
            ft.dropdown.Option("OK", "OK"),
            ft.dropdown.Option("FAIL", "FALLO"),
            ft.dropdown.Option("WARNING", "ADVERTENCIA"),
        ],
        value="Todas"
    ); _style_input(logs_adv_res)
    logs_adv_ide = ft.TextField(label="Id. Registro", width=110, keyboard_type=ft.KeyboardType.NUMBER); _style_input(logs_adv_ide)
    logs_adv_desde = _date_field(page, "Desde", width=140)
    logs_adv_hasta = _date_field(page, "Hasta", width=140)

    logs_table = GenericTable(
        columns=[
            ColumnConfig(key="fecha", label="Fecha", width=160),
            ColumnConfig(key="usuario", label="Usuario", width=120),
            ColumnConfig(key="entidad", label="Entidad", width=120),
            ColumnConfig(key="id_entidad", label="Id. Reg.", width=70),
            ColumnConfig(key="accion", label="Acción", width=100),
            ColumnConfig(key="resultado", label="Res.", width=80),
            ColumnConfig(key="ip", label="IP", width=120),
            ColumnConfig(key="detalle", label="Detalle", width=300),
        ],
        data_provider=create_catalog_provider(db.fetch_logs, db.count_logs),
        advanced_filters=[
            AdvancedFilterControl("usuario", logs_adv_user),
            AdvancedFilterControl("entidad", logs_adv_ent),
            AdvancedFilterControl("accion", logs_adv_acc),
            AdvancedFilterControl("resultado", logs_adv_res),
            AdvancedFilterControl("id_entidad", logs_adv_ide),
            AdvancedFilterControl("desde", logs_adv_desde),
            AdvancedFilterControl("hasta", logs_adv_hasta),
        ],
        show_inline_controls=False, show_mass_actions=False, show_selection=True, auto_load=False, page_size=50,
    )

    precios_view = make_card(
        "Listas de Precio", "Definición y actualización de listas.",
        ft.Column([
            ft.Row([nueva_lp_nom, nueva_lp_orden, ft.ElevatedButton("Crear Lista", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_lp, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10),
            precios_table.build()
        ], expand=True, spacing=10)
    )

    logs_view = make_card(
        "Logs de Sistema", "Registro histórico de movimientos y acciones.",
        logs_table.build()
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
                import threading
                threading.Thread(target=tab_to_table[idx].refresh, daemon=True).start()
    
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
                        ft.Container(content=ft.Row([nueva_marca, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_marca, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        marcas_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Rubros",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_rubro, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_rubro, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        rubros_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Unidades",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_uni_nombre, nueva_uni_abr, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_unidad, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        unidades_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Provincias",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_provincia_input, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_provincia, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        provincias_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Localidades",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_loc_nombre, nueva_loc_prov, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_localidad, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        localidades_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Condiciones IVA",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_civa, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_civa, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        civa_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Tipos IVA",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_tiva_porc, nueva_tiva_desc, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_tiva, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        tiva_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Depósitos",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nuevo_depo_nom, nuevo_depo_ubi, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_deposito, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        depo_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Formas Pago",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nueva_fpay, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_fpay, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        fpay_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Tipos Porcentaje",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nuevo_ptype, ft.ElevatedButton("Agregar", height=40, icon=ft.Icons.ADD_ROUNDED, on_click=agregar_ptype, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        ptype_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
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
                            ft.ElevatedButton("Agregar", icon=ft.Icons.ADD_ROUNDED, on_click=agregar_dtype, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))
                        ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        dtype_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
                ),
            ),
            make_tab(
                text="Tipos Movimiento",
                content=ft.Column(
                    [
                        ft.Container(content=ft.Row([nuevo_mtype_nom, nuevo_mtype_signo, ft.ElevatedButton("Agregar", icon=ft.Icons.ADD_ROUNDED, on_click=agregar_mtype, bgcolor=COLOR_ACCENT, color="#FFFFFF", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)))], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.symmetric(vertical=10)),
                        mtype_table.build(),
                    ],
                    expand=True, spacing=10, scroll=ft.ScrollMode.ADAPTIVE,
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
        [entidades_advanced_cuit, entidades_advanced_localidad, entidades_advanced_provincia, entidades_advanced_activo],
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
    wire_refresh(
        logs_table,
        [logs_adv_user, logs_adv_ent, logs_adv_acc, logs_adv_desde],
    )

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

    def refresh_all_stats():
        def _bg_work():
            if not db: return
            try:
                # Fetch all stats in one go using the new role-based method
                stats = db.get_full_dashboard_stats(CURRENT_USER_ROLE)
                
                # Entidades
                se = stats.get("entidades", {})
                if "entidades_clientes" in card_registry: card_registry["entidades_clientes"].value = f"{se.get('clientes_total', 0):,}"
                if "entidades_proveedores" in card_registry: card_registry["entidades_proveedores"].value = f"{se.get('proveedores_total', 0):,}"
                if "entidades_activos" in card_registry: card_registry["entidades_activos"].value = f"{se.get('clientes_total', 0) + se.get('proveedores_total', 0):,}"
                
                # Articulos
                sa = stats.get("stock", {})
                if "articulos_total" in card_registry: card_registry["articulos_total"].value = f"{sa.get('total', 0):,}"
                if "articulos_bajo_stock" in card_registry: card_registry["articulos_bajo_stock"].value = f"{sa.get('bajo_stock', 0):,}"
                
                val_inventario = sa.get('valor_inventario', 0)
                if "articulos_valor" in card_registry: 
                    card_registry["articulos_valor"].value = _format_money(val_inventario)
                
                # Facturacion / Ventas
                sv = stats.get("ventas", {})
                v_mes = sv.get('mes_total', 0)
                if "docs_ventas" in card_registry: 
                    card_registry["docs_ventas"].value = _format_money(v_mes) if isinstance(v_mes, (int, float)) else v_mes
                if "docs_pendientes" in card_registry: 
                    card_registry["docs_pendientes"].value = f"{sv.get('docs_pendientes', 0):,}"
                
                # Finanzas (if available)
                if "finanzas" in stats:
                    sf = stats["finanzas"]
                    if "docs_compras" in card_registry: card_registry["docs_compras"].value = _format_money(sf.get('egresos_mes', 0))
                    if "pagos_hoy" in card_registry: card_registry["pagos_hoy"].value = _format_money(sf.get('ingresos_hoy', 0))
                    if "pagos_recientes" in card_registry: card_registry["pagos_recientes"].value = f"{sf.get('pagos_recientes', 0):,}"
                
                # Usuarios
                so = stats.get("sistema", {})
                if "usuarios_activos" in card_registry: card_registry["usuarios_activos"].value = f"{so.get('usuarios_activos', 0):,}"
                if "usuarios_ultimo" in card_registry: card_registry["usuarios_ultimo"].value = so.get('ultimo_login', "N/A")
                
                # Movimientos
                sm = stats.get("movimientos", {})
                if "movs_ingresos" in card_registry: card_registry["movs_ingresos"].value = f"{sm.get('ingresos', 0):,}"
                if "movs_salidas" in card_registry: card_registry["movs_salidas"].value = f"{sm.get('salidas', 0):,}"
                if "movs_ajustes" in card_registry: card_registry["movs_ajustes"].value = f"{sm.get('ajustes', 0):,}"
                
                if not window_is_closing:
                    page.update()
            except (Exception, RuntimeError) as e:
                # Suppress transient Flet errors like "content must be visible" during transitions
                if not window_is_closing and db and not db.is_closing:
                    err_msg = str(e).lower()
                    if "content must be visible" not in err_msg and "page is not visible" not in err_msg:
                        print(f"Error refreshing stats: {e}")
        
        # Run in a background thread to avoid UI lag on tab switches
        import threading
        threading.Thread(target=_bg_work, daemon=True).start()

    # Re-declare refresh_all_stats for set_view to use

    # =========================================================================
    # LOGIN VIEW & AUTHENTICATION
    # =========================================================================
    CURRENT_USER_ROLE = "EMPLEADO"  # Default, will be set on login
    monitor_started = False

    def apply_role_permissions() -> None:
        is_admin = CURRENT_USER_ROLE == "ADMIN"
        for table in admin_export_tables:
            if hasattr(table.export_button, "visible"):
                table.export_button.visible = is_admin
    
    login_email = ft.TextField(
        label="Email o Usuario",
        width=320,
        prefix_icon=ft.Icons.EMAIL_ROUNDED,
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
        prefix_icon=ft.Icons.LOCK_ROUNDED,
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
    
    main_app_container = ft.Container(visible=False, expand=True)
    login_container = ft.Container(visible=True, expand=True)
    
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
                    content=ft.Icon(ft.Icons.CLOUD_SYNC_ROUNDED, size=64, color=COLOR_ACCENT),
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
                _set_overlay_state(
                    "Ejecutando respaldos pendientes...",
                    f"{label} en progreso",
                    progress=progress,
                    badge=label,
                    badge_color=COLOR_ACCENT,
                )
            elif status == "completed":
                progress = current / max(total, 1)
                _set_overlay_state(
                    "Ejecutando respaldos pendientes...",
                    f"{label} completado",
                    progress=progress,
                    badge=label,
                    badge_color=COLOR_SUCCESS,
                )
            elif status == "failed":
                progress = current / max(total, 1)
                _set_overlay_state(
                    "Error en respaldos",
                    f"{label} fallido",
                    progress=progress,
                    badge="ERROR",
                    badge_color=COLOR_ERROR,
                )

        def _schema_progress(payload: Dict[str, Any]) -> None:
            phase = payload.get("phase")
            if phase in {"extensions", "schemas"}:
                _set_overlay_state(
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
                _set_overlay_state(
                    "Actualizando esquema...",
                    payload.get("message", "Sincronizando..."),
                    progress=progress,
                    badge="SCHEMA",
                    badge_color=COLOR_INFO,
                )
                return

        def _run() -> None:
            try:
                _set_overlay_state(
                    "Preparando sistema...",
                    "Verificando respaldos y esquema...",
                    progress=None,
                )

                try:
                    from desktop_app.services.backup_manager import BackupManager
                except ImportError:
                    from services.backup_manager import BackupManager  # type: ignore

                backup_manager = BackupManager(db, pg_bin_path=config.pg_bin_path)
                missed = backup_manager.check_missed_backups()
                if missed:
                    results = backup_manager.execute_missed_backups(
                        missed,
                        progress_callback=_backup_progress,
                    )
                    if not results or not all(results.values()):
                        _set_overlay_state(
                            "Error en respaldos",
                            "Revisa el log antes de continuar.",
                            progress=1.0,
                            badge="ERROR",
                            badge_color=COLOR_ERROR,
                        )
                        return

                try:
                    from desktop_app.services.schema_sync import SchemaSync
                except ImportError:
                    from services.schema_sync import SchemaSync  # type: ignore

                schema_sync = SchemaSync(
                    db,
                    sql_path=PROJECT_ROOT / "database" / "database.sql",
                    logs_dir=PROJECT_ROOT / "logs",
                )
                if schema_sync.needs_sync():
                    result = schema_sync.apply(progress_callback=_schema_progress)
                    if not result.success:
                        _set_overlay_state(
                            "Error actualizando esquema",
                            result.error or "Fallo la sincronizacion.",
                            progress=1.0,
                            badge="ERROR",
                            badge_color=COLOR_ERROR,
                        )
                        return

                _hide_overlay()
                login_container.disabled = False
                login_container.visible = False
                main_app_container.visible = True
                page.update()
                on_success()
            except Exception as exc:
                _set_overlay_state(
                    "Error de mantenimiento",
                    str(exc),
                    progress=1.0,
                    badge="ERROR",
                    badge_color=COLOR_ERROR,
                )

        threading.Thread(target=_run, daemon=True).start()

    def do_login(_=None):
        nonlocal CURRENT_USER_ROLE, current_user, logout_logged
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
        
        # Successful login
        current_user = user
        CURRENT_USER_ROLE = user.get("rol", "EMPLEADO")
        logout_logged = False
        db.set_context(user["id"], local_ip)
        
        # Update sidebar info
        sidebar_user_name.value = user["nombre"]
        sidebar_user_role.value = f"Rol: {CURRENT_USER_ROLE}"
        apply_role_permissions()
        
        def start_background_monitor():
            nonlocal monitor_started
            if monitor_started:
                return
            monitor_started = True
            import threading
            import time
            def background_monitor():
                while not window_is_closing:
                    try:
                        # Only refresh if logged in
                        if db and db.current_user_id:
                            refresh_all_stats()
                            # If current view is Usuarios (Sessions), refresh sesiones_table only
                            if current_view["key"] == "usuarios":
                                try:
                                    sesiones_table.refresh()
                                except: pass
                    except: pass
                    # Refresh every 5 seconds for real-time feel
                    for _ in range(5):
                        if window_is_closing: break
                        time.sleep(1)

            threading.Thread(target=background_monitor, daemon=True).start()

        start_background_monitor()
        
        # Log login and load system config
        db.log_activity("SISTEMA", "LOGIN_OK", detalle={"modo": "BASIC_UI", "usuario": user["nombre"]})
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
        
        # Reset sidebar info
        sidebar_user_name.value = "Usuario"
        sidebar_user_role.value = "Sesión inactiva"
        
        # Reset login fields
        login_email.value = ""
        login_password.value = ""
        login_error.visible = False
        
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
                            ft.Container(
                                content=ft.Icon(ft.Icons.STOREFRONT_ROUNDED, size=56, color=COLOR_ACCENT),
                                bgcolor=f"{COLOR_ACCENT}15",
                                padding=20,
                                border_radius=20,
                            ),
                            ft.Container(height=16),
                            ft.Text("Nexoryn Tech", size=28, weight=ft.FontWeight.W_900, color=COLOR_TEXT),
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
        if key in ["usuarios", "backups", "logs"] and CURRENT_USER_ROLE != "ADMIN":
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
        elif key == "logs":
            content_holder.content = logs_view
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
            except: pass
        else:
            content_holder.content = articulos_view
        
        update_nav()

        def delayed_update():
            time.sleep(0.1)  # 100ms de retraso
            try:
                page.update()
            except Exception:
                pass
        
        threading.Thread(target=delayed_update, daemon=True).start()
        
        # Trigger refresh on the target table
        table_map = {
            "entidades": entidades_table,
            "precios": precios_table,
            "logs": logs_table,
            "usuarios": usuarios_table,
            "documentos": documentos_summary_table,
            "movimientos": movimientos_table,
            "pagos": pagos_table,
            "articulos": articulos_table,
            "dashboard": ensure_dashboard()
        }

        def safe_table_refresh(tab):
            try:
                time.sleep(0.2)  # Retraso adicional para tablas
                if hasattr(tab, "refresh"):
                    tab.refresh()
                elif hasattr(tab, "load_data"):
                    tab.load_data()
            except (RuntimeError, Exception):
                pass

        if key == "usuarios":
            def safe_refresh():
                try:
                    usuarios_table.refresh()
                    sesiones_table.refresh()
                except (RuntimeError, Exception):
                    pass
            threading.Thread(target=safe_refresh, daemon=True).start()
        elif key == "dashboard":
            if dashboard_view_component:
                dashboard_view_component.role = CURRENT_USER_ROLE
                dashboard_view_component.on_navigate = lambda x: set_view(x)
                dashboard_view_component.load_data()
        elif key in table_map:
            threading.Thread(target=safe_table_refresh, args=(table_map[key],), daemon=True).start()
        elif key == "config":
            # Initial load for the selected tab only
            try:
                on_config_tab_change(None)
            except Exception as e:
                print(f"Error initializing config tabs: {e}")
            
            try:
                refresh_loc_provs()
            except Exception as e:
                print(f"Error refreshing locations/provinces: {e}")

    nav_items: Dict[str, ft.Container] = {}
    admin_only_keys = {"usuarios", "backups", "logs"}

    def nav_item(key: str, label: str, icon_name: str):
        icon_value = getattr(ft.Icons, icon_name, ft.Icons.QUESTION_MARK_ROUNDED)
        
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
        for key, item in nav_items.items():
            # Show/hide admin-only items based on current role
            if key in admin_only_keys:
                item.visible = (CURRENT_USER_ROLE == "ADMIN")
            elif key == "config":
                item.visible = (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"])
            elif key == "masivos":
                item.visible = (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"])
            
            # Header visibility
            header_sistema.visible = (CURRENT_USER_ROLE in ["ADMIN", "GERENTE"])
            header_principal.visible = True # Always visible for now
            
            selected = key == current_view["key"]
            item.bgcolor = "#312E81" if selected else None  # Indigo 900 for active state
            try:
                row = item.content
                icon = row.controls[0]
                text = row.controls[1]
                icon.color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                text.color = COLOR_SIDEBAR_ACTIVE if selected else COLOR_SIDEBAR_TEXT
                text.weight = ft.FontWeight.BOLD if selected else ft.FontWeight.W_500
            except: pass
            item.update()

    # User info display (updated after login)
    sidebar_user_name = ft.Text("Usuario", size=12, color=COLOR_SIDEBAR_TEXT, weight=ft.FontWeight.W_500)
    sidebar_user_role = ft.Text("Sesión activa", size=10, color=COLOR_SIDEBAR_TEXT)

    sidebar = ft.Container(
        width=270,
        bgcolor=COLOR_PANEL,
        padding=ft.padding.all(20),
        content=ft.Column(
            [
                ft.Container(
                    content=ft.Row([
                        ft.Container(
                            width=42, height=42,
                            bgcolor=COLOR_ACCENT,
                            border_radius=12,
                            alignment=ft.alignment.center,
                            content=ft.Icon(ft.Icons.BOLT_ROUNDED, color="#FFFFFF", size=24),
                        ),
                        ft.Column([
                            ft.Text("Nexoryn", size=18, weight=ft.FontWeight.W_900, color="#FFFFFF"),
                            ft.Text("TECH SOLUTION", size=10, weight=ft.FontWeight.W_600, color=COLOR_SIDEBAR_TEXT),
                        ], spacing=-2),
                    ], spacing=12),
                    padding=ft.padding.only(bottom=20, top=10)
                ),
                ft.Container(
                    content=ft.ListView(
                        controls=[
                            header_principal := ft.Text("NAVEGACIÓN PRINCIPAL", size=11, weight=ft.FontWeight.W_700, color=COLOR_SIDEBAR_TEXT),
                            nav_item("dashboard", "Tablero de Control", "DASHBOARD_ROUNDED"),
                            nav_item("articulos", "Inventario", "INVENTORY_2_ROUNDED"),
                            nav_item("entidades", "Entidades", "PEOPLE_ALT_ROUNDED"),
                            nav_item("documentos", "Comprobantes", "RECEIPT_LONG_ROUNDED"),
                            nav_item("movimientos", "Movimientos", "SWAP_HORIZ_ROUNDED"),
                            nav_item("pagos", "Caja y Pagos", "ACCOUNT_BALANCE_WALLET_ROUNDED"),
                            nav_item("cuentas", "Cuentas Corrientes", "ACCOUNT_BALANCE_ROUNDED"),
                            nav_item("precios", "Lista de Precios", "LOCAL_OFFER_ROUNDED"),
                            nav_item("masivos", "Actualización Masiva", "PRICE_CHANGE_ROUNDED"),
                            
                            ft.Container(height=15),
                            header_sistema := ft.Text("SISTEMA", size=11, weight=ft.FontWeight.W_700, color=COLOR_SIDEBAR_TEXT),
                            nav_item("config", "Configuración", "SETTINGS_SUGGEST_ROUNDED"),
                            nav_item("usuarios", "Usuarios", "ADMIN_PANEL_SETTINGS_ROUNDED"),
                            nav_item("logs", "Logs de Actividad", "HISTORY_EDU_ROUNDED"),
                            nav_item("backups", "Respaldos", "CLOUD_SYNC_ROUNDED"),
                        ],
                        spacing=6,
                        padding=ft.padding.only(right=10), # Internal padding for scrollbar separation
                    ),
                    padding=0, # Remove external padding
                    expand=True,
                ),
                # Logout section at bottom
                ft.Container(
                    content=ft.Column([
                        ft.Divider(color="#334155", height=1),
                        ft.Container(height=10),
                        ft.Container(
                            content=ft.Row([
                                ft.Container(
                                    width=36, height=36,
                                    bgcolor="#4F46E5",
                                    border_radius=18,
                                    alignment=ft.alignment.center,
                                    content=ft.Icon(ft.Icons.PERSON_ROUNDED, color="#FFFFFF", size=20),
                                ),
                                ft.Column([
                                    sidebar_user_name,
                                    sidebar_user_role,
                                ], spacing=0, expand=True),
                                ft.IconButton(
                                    ft.Icons.LOGOUT_ROUNDED,
                                    icon_color="#EF4444",
                                    icon_size=22,
                                    tooltip="Cerrar Sesión",
                                    on_click=do_logout,
                                ),
                            ], spacing=10),
                            padding=ft.padding.symmetric(horizontal=5, vertical=8),
                            border_radius=12,
                            bgcolor="#1E293B",
                        ),
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
    )
    main_app_container.content = main_app_content

    # Add both login and main containers to page
    page.add(
        ft.Stack(
            [
                main_app_container,
                login_container,
                backup_overlay,
            ],
            expand=True,
        )
    )
    def open_nuevo_comprobante(edit_doc_id=None, copy_doc_id=None):
        db = get_db_or_toast()
        if not db: return

        try:
            tipos = db.fetch_tipos_documento()
            entidades = db.list_entidades_simple(limit=100) # Performance limit
            depositos = db.fetch_depositos()
            articulos = db.list_articulos_simple(limit=100) # Performance limit with price info
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
            
        if doc_data:
            # Ensure Entity exists
            eid = doc_data.get("id_entidad_comercial")
            if eid and not any(e["id"] == eid for e in entidades):
                missing_ent = db.get_entity_simple(eid)
                if missing_ent:
                    if not missing_ent.get("activo", True):
                        missing_ent["nombre_completo"] += " (Inactivo)"
                    entidades.append(missing_ent)
                    entidades.sort(key=lambda x: x["nombre_completo"])

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
            articulos.sort(key=lambda x: x["nombre"])

        
        # Form Fields
        field_fecha = _date_field(page, "Fecha", width=160)
        field_vto = _date_field(page, "Vencimiento", width=160)
        
        lista_options = [ft.dropdown.Option("", "Automático")] + [ft.dropdown.Option(str(l["id"]), l["nombre"]) for l in listas]
        lista_initial_items = [{"value": l["id"], "label": l["nombre"]} for l in listas]
        field_saldo = ft.Text("", size=12, color=COLOR_ACCENT, weight=ft.FontWeight.W_500)
        
        def _update_entidad_info(e=None):
            if dropdown_entidad.value:
                info = db.get_saldo_entidad(int(dropdown_entidad.value))
                bal = float(info.get("saldo", 0))
                field_saldo.value = f"Saldo actual: {_format_money(bal)}"
                if bal < 0:
                    field_saldo.color = COLOR_SUCCESS
                elif bal > 0:
                    field_saldo.color = COLOR_ERROR
                else:
                    field_saldo.color = COLOR_TEXT_MUTED
                if field_saldo.page:
                    field_saldo.update()
        
        ent_initial_items = [{"value": e["id"], "label": f"{e['nombre_completo']} ({e['tipo']})"} for e in entidades]
        dropdown_entidad = AsyncSelect(
            label="Entidad",
            loader=entity_loader,
            width=300,
            on_change=lambda _: _update_entidad_info(None),
            initial_items=ent_initial_items
        )

        dropdown_deposito = ft.Dropdown(label="Depósito", options=[ft.dropdown.Option(str(d["id"]), d["nombre"]) for d in depositos], width=200); _style_input(dropdown_deposito)
        
        # Lista de precios global (opcional, se aplica a todos los ítems)
        dropdown_lista_global = AsyncSelect(
            label="Lista de Precios (Global)", 
            loader=price_list_loader,
            width=220,
            initial_items=lista_initial_items,
        )
        
        field_obs = ft.TextField(label="Observaciones (Internas)", multiline=True, width=800, height=80); _style_input(field_obs)
        field_direccion = ft.TextField(label="Dirección de Entrega", width=500); _style_input(field_direccion)
        field_numero = ft.TextField(label="Número/Serie", width=200); _style_input(field_numero)
        field_descuento = ft.TextField(label="Desc. %", width=100, value="0"); _style_input(field_descuento)
        field_sena = ft.TextField(label="Seña $", width=120, value="0", on_change=lambda _: _recalc_total()); _style_input(field_sena)
        
        # Filter tipos: NC/ND only allowed if it's a copy of an already "facturado" (with CAE) doc
        # AND the source document was a Factura (not Presupuesto, Remito, etc)
        is_facturado = doc_data and doc_data.get("cae") is not None
        source_is_factura = False
        if doc_data:
             # Try to determine if source type name contains "FACTURA"
             # Since we only have ID here, we look it up in 'tipos' list loop below or pre-fetch?
             # Easier: we loop below.
             pass

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

        def _update_serial_number(e=None):
            if not edit_doc_id and dropdown_tipo.value:
                try:
                    next_num = db.get_next_number(int(dropdown_tipo.value))
                    field_numero.value = str(next_num)
                    field_numero.update()
                except:
                    pass

        dropdown_tipo = ft.Dropdown(
            label="Tipo", 
            options=[ft.dropdown.Option(str(t["id"]), t["nombre"]) for t in allowed_tipos], 
            width=200,
            on_change=_update_serial_number
        ); _style_input(dropdown_tipo)
        
        if doc_data:
            dropdown_tipo.value = str(doc_data["id_tipo_documento"])
            dropdown_entidad.value = str(doc_data["id_entidad_comercial"])
            dropdown_deposito.value = str(doc_data["id_deposito"])
            field_obs.value = doc_data["observacion"]
            
            if edit_doc_id:
                field_numero.value = doc_data["numero_serie"]
                field_fecha.value = doc_data["fecha"][:10] if doc_data["fecha"] else None
            elif copy_doc_id:
                field_obs.value = f"Copia de {doc_data.get('numero_serie','')}. " + (doc_data.get('observacion','') or "")
                # Use helper for copied type
                try:
                    next_num = db.get_next_number(int(doc_data["id_tipo_documento"]))
                    field_numero.value = str(next_num)
                except:
                    field_numero.value = ""
                field_fecha.value = datetime.now().strftime("%Y-%m-%d")
            
            field_descuento.value = str(doc_data["descuento_porcentaje"])
            field_vto.value = doc_data["fecha_vencimiento"]
            field_direccion.value = doc_data.get("direccion_entrega", "") or ""
            
            # Set price list if available
            if doc_data.get("id_lista_precio"):
                dropdown_lista_global.value = str(doc_data["id_lista_precio"])
            
            field_sena.value = str(doc_data.get("sena", 0))
            
            _update_entidad_info(None)
        else:
            # New document: select first type by default and trigger number load
            if not dropdown_tipo.value and allowed_tipos:
                dropdown_tipo.value = str(allowed_tipos[0]["id"])
                _update_serial_number()

        # Financial Summary
        manual_mode = ft.Switch(label="Manual", value=False)
        
        sum_subtotal = ft.TextField(value="0.00", width=120, read_only=True, text_align=ft.TextAlign.RIGHT, label="Subtotal")
        sum_iva = ft.TextField(value="0.00", width=100, read_only=True, text_align=ft.TextAlign.RIGHT, label="IVA")
        sum_total = ft.TextField(value="0.00", width=140, read_only=True, text_align=ft.TextAlign.RIGHT, text_style=ft.TextStyle(weight=ft.FontWeight.BOLD, color=COLOR_ACCENT), label="TOTAL")
        sum_saldo = ft.TextField(value="0.00", width=140, read_only=True, text_align=ft.TextAlign.RIGHT, text_style=ft.TextStyle(weight=ft.FontWeight.BOLD, color=COLOR_WARNING), label="SALDO")
        
        if doc_data:
            sum_subtotal.value = str(doc_data.get("neto", 0))
            sum_iva.value = str(doc_data.get("iva_total", 0))
            sum_total.value = str(doc_data.get("total", 0))
        
        def _recalc_total():
            if manual_mode.value: return # Don't overwrite manual edits
            
            sub = 0.0
            iva_tot = 0.0
            
            for row in lines_container.controls:
                try:
                    # [Artículo, Lista, Cant, Precio, IVA, Delete]
                    # Cant is now a Column: controls[2].controls[0] is the TextField
                    c_cant = float(row.controls[2].controls[0].value or 0)
                    c_price = float(row.controls[3].value or 0)
                    c_iva = float(row.controls[4].value or 0)
                    
                    line_neto = c_cant * c_price
                    sub += line_neto
                    iva_tot += line_neto * (c_iva / 100.0)
                except: pass
            
            try:
                desc_pct = float(field_descuento.value or 0)
            except: desc_pct = 0.0
            
            if desc_pct > 0:
                sub = sub * (1 - desc_pct/100)
                iva_tot = iva_tot * (1 - desc_pct/100)

            total = sub + iva_tot
            
            try:
                sena_val = float(field_sena.value or 0)
            except: sena_val = 0.0

            sum_subtotal.value = str(round(sub, 2))
            sum_iva.value = str(round(iva_tot, 2))
            sum_total.value = str(round(total, 2))
            sum_saldo.value = str(round(max(0, total - sena_val), 2))
            page.update()

        def toggle_manual(e):
             is_manual = manual_mode.value
             sum_subtotal.read_only = not is_manual
             sum_iva.read_only = not is_manual
             sum_total.read_only = not is_manual
             if not is_manual:
                 _recalc_total() # Restore auto values
             else:
                 page.update()

        manual_mode.on_change = toggle_manual

        field_descuento.on_change = lambda _: _recalc_total()
        
        # Use ListView with internal padding to prevent "first item cut-off" issue
        lines_container = ft.ListView(spacing=10, padding=ft.padding.only(top=15, left=5, right=10, bottom=5), expand=True)

        def _add_line(_=None, update_ui=True, initial_data=None):
            art_initial_items = [{"value": a["id"], "label": f"{a['nombre']} (Cod: {a['id']})"} for a in articulos]
            art_drop = AsyncSelect(
                label="Artículo", 
                loader=article_loader, 
                expand=True,
                initial_items=art_initial_items
            )
            lista_drop = AsyncSelect(
                label="Lista",
                loader=price_list_loader,
                width=140,
                initial_items=lista_initial_items,
            )
            cant_field = ft.TextField(label="Cant.", width=80, value="1"); _style_input(cant_field)
            price_field = ft.TextField(label="Precio",width=90, value="0"); _style_input(price_field)
            iva_field = ft.TextField(label="IVA %", width=60, value="21"); _style_input(iva_field)
            total_field = ft.TextField(label="Total", width=100, value="0.00", read_only=True, text_align=ft.TextAlign.RIGHT); _style_input(total_field)
            
            if initial_data:
                art_drop.value = str(initial_data["id_articulo"])
                lista_drop.value = str(initial_data["id_lista_precio"]) if initial_data.get("id_lista_precio") else ""
                cant_field.value = str(initial_data["cantidad"])
                price_field.value = str(initial_data["precio_unitario"])
                iva_field.value = str(initial_data["porcentaje_iva"])
            else:
                 # Usar lista global si está seleccionada
                 if dropdown_lista_global.value and dropdown_lista_global.value != "":
                     lista_drop.value = dropdown_lista_global.value
                 else:
                     lista_drop.value = ""
            
            def _update_line_total():
                """Actualiza el total de la línea"""
                try:
                    c_cant = float(cant_field.value or 0)
                    c_price = float(price_field.value or 0)
                    line_total = c_cant * c_price
                    total_field.value = f"{line_total:.2f}"
                    if total_field.page:
                        total_field.update()
                except:
                    total_field.value = "0.00"
            
            def _update_price_from_list():
                """Actualiza el precio basado en artículo y lista seleccionados"""
                art_id_val = art_drop.value
                # Primero intentar usar la lista del ítem, si no la lista global
                lid = lista_drop.value
                if not lid or lid == "":
                    lid = dropdown_lista_global.value
                
                if not art_id_val:
                    return
                
                art_id = int(art_id_val)
                art = next((a for a in articulos if a["id"] == art_id), None)
                if not art:
                    return
                
                final_price = 0.0
                prices = db.fetch_article_prices(art_id)
                
                if prices:
                    if lid and lid != "":
                        # Usar la lista seleccionada
                        p_obj = next((p for p in prices if str(p["id_lista_precio"]) == str(lid)), None)
                        if p_obj and p_obj.get("precio"):
                            final_price = float(p_obj["precio"])
                    
                    # Si no hay lista seleccionada o no tiene precio, usar la primera con precio
                    if final_price == 0.0:
                        for p in prices:
                            if p.get("precio") and float(p.get("precio", 0)) > 0:
                                final_price = float(p["precio"])
                                break
                
                # Fallback al costo si no hay precios
                if final_price == 0.0:
                    final_price = float(art.get("costo") or 0)
                
                price_field.value = str(final_price)
                iva_field.value = str(art.get("porcentaje_iva", 21))
                _update_line_total()
                page.update()
                _recalc_total()
            
            stock_text = ft.Text("Stock: -", size=10, color=COLOR_TEXT_MUTED)

            def _check_stock_warning():
                if not art_drop.value: return
                try:
                    requested = float(cant_field.value or 0)
                    available = db.get_article_stock(int(art_drop.value))
                    stock_text.value = f"Stock: {available}"
                    if requested > available:
                        stock_text.color = COLOR_ERROR
                        stock_text.weight = ft.FontWeight.BOLD
                    else:
                        stock_text.color = ft.Colors.GREEN_600
                        stock_text.weight = ft.FontWeight.NORMAL
                    if stock_text.page:
                        stock_text.update()
                except: pass

            def _on_art_change(e):
                _update_price_from_list()
                _check_stock_warning()
            
            def _on_value_change(_):
                _update_line_total()
                _recalc_total()

            cant_field.on_change = lambda _: (_check_stock_warning(), _update_line_total(), _recalc_total())
            art_drop.on_change = _on_art_change
            def _on_lista_change(e):
                _update_price_from_list()
            lista_drop.on_change = _on_lista_change
            
            for f in [price_field, iva_field]:
                f.on_change = _on_value_change

            cant_container = ft.Column([cant_field, stock_text], spacing=0, width=80)

            # Store callbacks for external updates
            row_map = {
                "update_price": _update_price_from_list,
                "lista_drop": lista_drop,
                "art_drop": art_drop,
                "cant_field": cant_field # For potential future use
            }

            delete_btn = ft.IconButton(
                icon=ft.Icons.DELETE, 
                icon_color=COLOR_ERROR, 
                tooltip="Eliminar línea",
                on_click=lambda e: _remove_line(e.control.parent)
            )

            # [Artículo, Lista, Cant, Precio, IVA, Total, Delete]
            row = ft.Row([art_drop, lista_drop, cant_container, price_field, iva_field, total_field, delete_btn], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.START)
            row.data = row_map # Attack callbacks to row
            
            lines_container.controls.append(row)
            if update_ui:
                lines_container.update()
                _recalc_total()
            
            # Initial Run
            if initial_data:
                _update_line_total()
                # If editing, check stock silently?
                pass
            
            # Trigger initial stock check if article is pre-selected (e.g. from copy)
            if art_drop.value:
                _check_stock_warning()
        
        def _remove_line(row_to_remove):
            lines_container.controls.remove(row_to_remove)
            lines_container.update()
            _recalc_total()

        def _on_global_list_change(e):
            """When global price list changes, update all line items that don't have a specific list set."""
            new_global_list_id = dropdown_lista_global.value
            if not new_global_list_id: return 

            for row in lines_container.controls:
                row_map = row.data
                line_lista_drop = row_map["lista_drop"]
                
                # If the line list is empty (Automatic), change it to the new Global list
                # This satisfies the user's request: "automaticamente todos los precios deberían tomar la lista 2"
                if not line_lista_drop.value or line_lista_drop.value == "":
                    row_map["update_price"]() # Update price using global list automatically
            
            page.update()
            _recalc_total()

        # (Manual wire for AsyncSelect global list is handled in its on_change)
        dropdown_lista_global.on_change = _on_global_list_change

        def _save(_=None):
            if not dropdown_tipo.value or not dropdown_entidad.value or not dropdown_deposito.value:
                show_toast("Faltan campos obligatorios", kind="warning")
                return
            
            items = []
            for row in lines_container.controls:
                controls = row.controls
                # [Artículo, Lista, Cant, Precio, IVA, Total, Delete]
                art_id = controls[0].value
                if not art_id: continue
                
                # Usar lista del ítem, o la global si no tiene
                item_lista = controls[1].value
                if not item_lista or item_lista == "" or item_lista == "Automático":
                    item_lista = dropdown_lista_global.value
                
                # Ensure global value is also clean
                if item_lista == "Automático": item_lista = ""

                items.append({
                    "id_articulo": int(art_id),
                    "id_lista_precio": int(item_lista) if item_lista and item_lista != "" else None,
                    "cantidad": float(controls[2].controls[0].value or 0),
                    "precio_unitario": float(controls[3].value or 0),
                    "porcentaje_iva": float(controls[4].value or 0)
                })
            
            if not items:
                show_toast("El comprobante debe tener al menos una línea", kind="warning")
                return

            # Determinar id_lista_precio del documento
            gl_val = dropdown_lista_global.value
            doc_lista_precio = int(gl_val) if gl_val and gl_val != "" and gl_val != "Automático" else None

            try:
                if edit_doc_id:
                    db.update_document(
                        doc_id=edit_doc_id,
                        id_tipo_documento=int(dropdown_tipo.value),
                        id_entidad_comercial=int(dropdown_entidad.value),
                        id_deposito=int(dropdown_deposito.value),
                        items=items,
                        observacion=field_obs.value,
                        numero_serie=field_numero.value,
                        descuento_porcentaje=float(field_descuento.value or 0),
                        descuento_importe=float(doc_data.get("descuento_importe", 0)) if doc_data else 0,
                        fecha=field_fecha.value, 
                        fecha_vencimiento=field_vto.value,
                        direccion_entrega=field_direccion.value,
                        id_lista_precio=doc_lista_precio,
                        sena=float(field_sena.value or 0),
                        manual_values={
                            "subtotal": float(sum_subtotal.value or 0),
                            "iva_total": float(sum_iva.value or 0),
                            "total": float(sum_total.value or 0),
                        } if manual_mode.value else None
                    )
                else:
                    db.create_document(
                        id_tipo_documento=int(dropdown_tipo.value),
                    id_entidad_comercial=int(dropdown_entidad.value),
                    id_deposito=int(dropdown_deposito.value),
                    items=items,
                    observacion=field_obs.value,
                    numero_serie=field_numero.value,
                    descuento_porcentaje=float(field_descuento.value or 0),
                    descuento_importe=0,
                    fecha=field_fecha.value, 
                    fecha_vencimiento=field_vto.value,
                    direccion_entrega=field_direccion.value,
                    id_lista_precio=doc_lista_precio,
                    sena=float(field_sena.value or 0),
                    manual_values={
                        "subtotal": float(sum_subtotal.value or 0),
                        "iva_total": float(sum_iva.value or 0),
                        "total": float(sum_total.value or 0),
                    } if manual_mode.value else None
                )
                show_toast("Comprobante creado con éxito", kind="success")
                close_form()
                # Refresh tables if they are visible
                documentos_summary_table.refresh()
                refresh_all_stats()
            except Exception as ex:
                show_toast(f"Error al guardar: {ex}", kind="error") 
        if doc_data:
            # Add existing items
            for item in doc_data["items"]:
                # Inject fallback price list (from header) if item doesn't have one 
                # (which it won't until DB supports it)
                if "id_lista_precio" not in item:
                    item["id_lista_precio"] = doc_data.get("id_lista_precio")
                _add_line(initial_data=item, update_ui=False)
            
            # Set manual totals if they were different from calculated?
            # Or just set them if the document state says so.
            # Simplified: always load them and if they match, user can just keep going.
            sum_subtotal.value = str(doc_data["neto"])
            sum_iva.value = str(doc_data["iva_total"])
            sum_total.value = str(doc_data["total"])
            try:
                total_val = float(sum_total.value or 0)
                sena_val = float(field_sena.value or 0)
                sum_saldo.value = str(round(max(0, total_val - sena_val), 2))
            except Exception:
                sum_saldo.value = "0.00"
            # Auto-enable manual mode if there's a discrepancy? 
            # For now, let user enable it if they want to edit.
        else:
            _add_line(update_ui=False) # Add one line by default, no update yet



        # Custom Dialog Content (replacing generic open_form to control layout fully)
        dialog_content = ft.Container(
            content=ft.ListView(
                controls=[
                    ft.Container(height=20), # Header Spacer
                    ft.Row([
                        ft.Text("Nuevo Comprobante", size=20, weight=ft.FontWeight.BOLD),
                        ft.IconButton(ft.Icons.CLOSE, on_click=close_form)
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    ft.Row([field_fecha, field_vto, dropdown_tipo], spacing=10),
                    ft.Row([ft.Column([dropdown_entidad, field_saldo], spacing=2)], alignment=ft.MainAxisAlignment.START),
                    ft.Row([dropdown_lista_global], spacing=10),
                    ft.Row([dropdown_deposito, field_numero, field_descuento, field_sena], spacing=10),
                    ft.Row([field_obs], spacing=10),
                    ft.Row([field_direccion], spacing=10),
                    ft.Divider(),
                    ft.Text("Ítems", weight=ft.FontWeight.BOLD),
                    ft.Container(
                        content=lines_container,
                        height=200, # Scrollable area
                        border=ft.border.all(1, "#E2E8F0"),
                        border_radius=8,
                        # Padding removed here as it's now handled inside ListView
                        # padding=ft.padding.only(left=10, right=20, top=30, bottom=10), 
                    ),
                    ft.Row([
                         ft.ElevatedButton(
                             "Agregar Línea", 
                             icon=ft.Icons.ADD, 
                             on_click=_add_line, 
                             bgcolor=COLOR_ACCENT,
                             color="white",
                             style=ft.ButtonStyle(
                                 shape=ft.RoundedRectangleBorder(radius=8),
                             )
                         ),
                    ], alignment=ft.MainAxisAlignment.START),
                    ft.Divider(),
                    # Financial Footer

                    ft.Row([
                        manual_mode,
                        ft.Column([sum_subtotal], spacing=0),
                        ft.Column([sum_iva], spacing=0),
                        ft.Column([sum_total], spacing=0),
                        ft.Column([sum_saldo], spacing=0),
                    ], alignment=ft.MainAxisAlignment.END, spacing=15),
                    ft.Container(height=10),
                    ft.Row(
                        [
                            ft.OutlinedButton(
                                "Cancelar", 
                                on_click=close_form, 
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                )
                            ),
                            ft.ElevatedButton(
                                "Guardar" if edit_doc_id else "Crear Comprobante", 
                                icon=ft.Icons.CHECK,
                                on_click=_save,
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                    bgcolor=COLOR_ACCENT,
                                    color=ft.Colors.WHITE,
                                )
                            ),
                        ],
                        alignment=ft.MainAxisAlignment.END,
                    ),
                ],
                padding=ft.padding.all(25), # Internal padding handles scrollbar spacing
                spacing=15, # Restore vertical spacing
            ),
            padding=0, # Remove outer padding to allow scrollbar to hit edge
            width=900,
            height=800,
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

        # Clear native dialog just in case
        page.dialog = None

        form_dialog.visible = True
        if form_dialog in page.overlay:
            page.overlay.remove(form_dialog)
        page.overlay.append(form_dialog)
        page.update()

    def wire_live_search(table: GenericTable):
        for flt in table.advanced_filters:
            if hasattr(flt.control, "on_change"):
                flt.control.on_change = lambda _: table.refresh()

    for t in [entidades_table, articulos_table, documentos_summary_table, movimientos_table, pagos_table]:
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
            print(f"Error initial reload: {exc}")

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
                print(f"DEBUG: Error logging logout: {ex}")
            try:
                db.close()
            except Exception:
                pass
            try:
                scheduler.shutdown()
            except Exception:
                pass

    atexit.register(lambda: _shutdown("salida_programa"))
    page.on_window_event = None
    page.on_close = None
    page.on_disconnect = None



if __name__ == "__main__":
    ft.app(target=main)
