from __future__ import annotations

from pathlib import Path
import atexit
import sys
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

import flet as ft

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from desktop_app.config import load_config
    from desktop_app.database import Database
    from desktop_app.components.generic_table import ColumnConfig, GenericTable, SimpleFilterConfig
    from desktop_app.components.async_select import AsyncSelect
except ImportError:
    from config import load_config  # type: ignore
    from database import Database  # type: ignore
    from components.generic_table import ColumnConfig, GenericTable, SimpleFilterConfig  # type: ignore
    from components.async_select import AsyncSelect  # type: ignore

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from desktop_app.services.backup_service import BackupService
from desktop_app.components.backup_view import BackupView


def _format_money(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _build_stock_alert_tile(alert: Dict[str, Any]) -> ft.Control:
    minimum = float(alert.get("stock_minimo") or 0)
    actual = float(alert.get("stock_actual") or 0)
    if minimum > 0:
        fill = min(max(actual / minimum, 0), 1)
    else:
        fill = 1.0 if actual > 0 else 0.0
    diferencia = float(alert.get("diferencia") or 0)

    return ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Text(alert.get("nombre") or "—", weight=ft.FontWeight.BOLD, max_lines=1),
                        ft.Container(
                            padding=ft.padding.symmetric(horizontal=8, vertical=4),
                            border_radius=999,
                            bgcolor="#FEE2E2" if diferencia < 0 else "#DCFCE7",
                            content=ft.Text(
                                f"Δ {diferencia:.2f}",
                                size=11,
                                weight=ft.FontWeight.BOLD,
                                color="#991B1B" if diferencia < 0 else "#166534",
                            ),
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Text(f"Stock: {actual:.2f} · Mínimo: {minimum:.2f}", size=12, color="#64748B"),
                ft.ProgressBar(value=fill, color=ft.Colors.ORANGE_700),
            ],
            spacing=8,
            tight=True,
        ),
        padding=12,
        border_radius=14,
        border=ft.border.all(1, "#E2E8F0"),
        bgcolor="#FFFFFF",
    )


def main(page: ft.Page) -> None:
    config = load_config()
    db = Database(
        config.database_url,
        pool_min_size=config.db_pool_min,
        pool_max_size=config.db_pool_max,
    )
    is_closing = False
    
    # Old standard backup system disabled
    # backup_service = BackupService()
    scheduler = BackgroundScheduler()
    
    # Automated backups disabled
    # scheduler.add_job(
    #     lambda: backup_service.create_backup("daily"), 
    #     CronTrigger(hour=23, minute=0),
    #     id="backup_daily"
    # )
    # scheduler.add_job(
    #     lambda: backup_service.create_backup("weekly"), 
    #     CronTrigger(day_of_week="sun", hour=23, minute=30),
    #     id="backup_weekly"
    # )
    # scheduler.add_job(
    #     lambda: backup_service.create_backup("monthly"), 
    #     CronTrigger(day=1, hour=0, minute=0),
    #     id="backup_monthly"
    # )
    # # Prune old backups daily at 01:00
    # scheduler.add_job(
    #     lambda: backup_service.prune_backups(), 
    #     CronTrigger(hour=1, minute=0),
    #     id="backup_prune"
    # )
    
    scheduler.start()

    def _shutdown(reason: str = "shutdown") -> None:
        nonlocal is_closing
        if is_closing:
            return
        is_closing = True
        try:
            scheduler.shutdown()
            db.close()
        except Exception:
            pass

    atexit.register(lambda: _shutdown("atexit"))

    page.title = "Nexoryn Tech - Control de operaciones"
    page.window_width = 1200
    page.window_height = 820
    page.theme_mode = ft.ThemeMode.LIGHT
    page.vertical_alignment = ft.MainAxisAlignment.START
    page.padding = 0
    page.spacing = 0
    try:
        AsyncSelect.set_default_page(page)
    except Exception:
        pass

    def show_fatal_error(exc: Exception) -> None:
        page.controls.clear()
        page.add(
            ft.Container(
                ft.Column(
                    [
                        ft.Text("No se pudo inicializar la UI", size=18, weight=ft.FontWeight.BOLD),
                        ft.Text(str(exc)),
                        ft.Text("Pegá este error en el chat para ajustarlo a tu versión de Flet.", size=12),
                    ],
                    spacing=8,
                ),
                padding=20,
            )
        )

    try:
        # ---------- Palette ----------
        COLOR_BG = "#F6F7FB"
        COLOR_CARD = "#FFFFFF"
        COLOR_TEXT = "#0F172A"
        COLOR_TEXT_MUTED = "#64748B"
        COLOR_PRIMARY = "#4F46E5"
        COLOR_BORDER = "#E2E8F0"
        COLOR_SURFACE_2 = "#F1F5F9"
        COLOR_INFO = "#3B82F6"

        page.bgcolor = COLOR_BG

        # ---------- Global message bar ----------
        message_text = ft.Text("", size=12, color=COLOR_TEXT)
        message_bar = ft.Container(
            visible=False,
            padding=12,
            border_radius=14,
            border=ft.border.all(1, COLOR_BORDER),
            bgcolor=COLOR_SURFACE_2,
            content=ft.Row(
                [
                    ft.Icon(ft.Icons.INFO_OUTLINE, color=COLOR_TEXT_MUTED),
                    ft.Container(message_text, expand=1),
                    ft.IconButton(
                        icon=ft.Icons.CLOSE,
                        tooltip="Cerrar",
                        on_click=lambda e: hide_message(),
                    ),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            ),
        )

        def show_message(message: str, kind: str = "info") -> None:
            message_text.value = message
            if kind == "error":
                message_bar.bgcolor = "#FEF2F2"
                message_bar.border = ft.border.all(1, "#FECACA")
                message_text.color = "#991B1B"
            elif kind == "success":
                message_bar.bgcolor = "#F0FDF4"
                message_bar.border = ft.border.all(1, "#BBF7D0")
                message_text.color = "#166534"
            else:
                message_bar.bgcolor = COLOR_SURFACE_2
                message_bar.border = ft.border.all(1, COLOR_BORDER)
                message_text.color = COLOR_TEXT
            message_bar.visible = bool(message)
            if hasattr(page, "open"):
                # Si tenemos SnackBar o similar en el futuro, pero aquí es un Container in-page
                pass
            page.update()

        def hide_message() -> None:
            message_bar.visible = False
            message_text.value = ""
            page.update()

        # ---------- Connection badge ----------
        conn_badge = ft.Container(
            padding=ft.padding.symmetric(horizontal=10, vertical=6),
            border_radius=999,
            bgcolor="#E2E8F0",
            content=ft.Row(
                [
                    ft.Container(width=8, height=8, bgcolor="#64748B", border_radius=999),
                    ft.Text("DB", size=12, color=COLOR_TEXT_MUTED),
                ],
                spacing=8,
                tight=True,
            ),
        )

        def set_connection(ok: bool, label: str) -> None:
            dot = "#16A34A" if ok else "#DC2626"
            bg = "#DCFCE7" if ok else "#FEE2E2"
            border = "#BBF7D0" if ok else "#FECACA"
            text = "#166534" if ok else "#991B1B"
            conn_badge.bgcolor = bg
            conn_badge.border = ft.border.all(1, border)
            conn_badge.content.controls[0].bgcolor = dot
            conn_badge.content.controls[1].value = label
            conn_badge.content.controls[1].color = text
            page.update()

        backup_service = BackupService(pg_bin_path=config.pg_bin_path)
        backup_view_component = BackupView(page, backup_service, show_message, set_connection)
        backup_view_control = ft.Container(
            content=backup_view_component.build(),
            padding=12,
            expand=1,
        )

        def _maybe_set(obj: Any, name: str, value: Any) -> None:
            if hasattr(obj, name):
                try:
                    setattr(obj, name, value)
                except Exception:
                    return

        def _style_input(control: Any) -> None:
            name = getattr(control, "__class__", type("x", (), {})).__name__.lower()
            is_dropdown = "dropdown" in name
            is_textfield = "textfield" in name

            # HIGH VISIBILITY STYLE - "Round & Contrast"
            _maybe_set(control, "border_color", "#475569") # Slate 600 (Darker)
            _maybe_set(control, "focused_border_color", COLOR_PRIMARY)
            _maybe_set(control, "border_radius", 12) # More rounded
            _maybe_set(control, "text_size", 14)
            _maybe_set(control, "label_style", ft.TextStyle(color="#1E293B", size=13, weight=ft.FontWeight.BOLD)) # Darker label
            
            if is_dropdown:
                _maybe_set(control, "bgcolor", "#F8FAFC") # Slight grey background
                _maybe_set(control, "filled", True)
                _maybe_set(control, "border_width", 2) # Thicker border
                return

            _maybe_set(control, "filled", True)
            _maybe_set(control, "bgcolor", "#F8FAFC")
            _maybe_set(control, "border_width", 1)
            
            if is_textfield and not is_dropdown:
                _maybe_set(control, "height", 50) # Taller
                _maybe_set(control, "dense", False)

        def _open_dialog(dialog: ft.Control) -> None:
            if hasattr(page, "open"):
                page.open(dialog)
            else:
                overlay = getattr(page, "overlay", None)
                if isinstance(overlay, list) and dialog not in overlay:
                    overlay.append(dialog)
                try:
                    page.dialog = dialog  # type: ignore[attr-defined]
                except Exception:
                    pass
                if hasattr(dialog, "open"):
                    try:
                        dialog.open = True  # type: ignore[attr-defined]
                    except Exception:
                        pass
                page.update()

        def _close_dialog(dialog: ft.Control) -> None:
            if hasattr(page, "close"):
                page.close(dialog)
            else:
                if hasattr(dialog, "open"):
                    try:
                        dialog.open = False  # type: ignore[attr-defined]
                    except Exception:
                        pass
                page.update()

        # ---------- Detail panel ----------
        def kv(label: str, value: Any) -> ft.Control:
            return ft.Row(
                [
                    ft.Text(label, size=12, color=COLOR_TEXT_MUTED),
                    ft.Text(str(value) if value not in (None, "") else "—", size=12, weight=ft.FontWeight.BOLD, color=COLOR_TEXT),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            )

        entity_detail = ft.Container(
            content=ft.Column(
                [
                    ft.Icon(getattr(ft.Icons, "PERSON_SEARCH_OUTLINED", ft.Icons.SEARCH), size=34, color=ft.Colors.BLUE_GREY_300),
                    ft.Text("Detalle de entidad", weight=ft.FontWeight.BOLD),
                    ft.Text("Seleccioná una fila o tocá el ícono de info.", size=12, color=COLOR_TEXT_MUTED),
                ],
                spacing=6,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            padding=12,
            bgcolor=COLOR_CARD,
            border_radius=14,
            border=ft.border.all(1, COLOR_BORDER),
            expand=1,
        )

        def update_entity_detail(entity: Dict[str, Any]) -> None:
            entity_detail.content = ft.Column(
                [
                    ft.Text(entity.get("nombre_completo") or "—", weight=ft.FontWeight.BOLD, size=16, color=COLOR_TEXT),
                    ft.Text(entity.get("razon_social") or "", size=12, color=COLOR_TEXT_MUTED),
                    ft.Divider(height=18),
                    kv("Tipo", entity.get("tipo")),
                    kv("CUIT", entity.get("cuit")),
                    kv("Lista", entity.get("lista_precio")),
                    kv("Descuento", f"{float(entity.get('descuento') or 0):.2f}%"),
                    kv("Saldo", _format_money(entity.get("saldo_cuenta"))),
                    kv("Provincia", entity.get("provincia")),
                    kv("Localidad", entity.get("localidad")),
                ],
                spacing=4,
            )
            page.update()

        # ---------- Data providers ----------
        def entity_provider(
            offset: int,
            limit: int,
            search: Optional[str],
            simple_filter_value: Optional[str],
            advanced: Dict[str, Any],
            sorts: List[Tuple[str, str]],
        ) -> Tuple[List[Dict[str, Any]], int]:
            rows = db.fetch_entities(
                search=search,
                tipo=simple_filter_value,
                sorts=sorts,
                limit=limit,
                offset=offset,
            )
            total = db.count_entities(search=search, tipo=simple_filter_value)
            return rows, total

        def article_provider(
            offset: int,
            limit: int,
            search: Optional[str],
            simple_filter_value: Optional[str],
            advanced: Dict[str, Any],
            sorts: List[Tuple[str, str]],
        ) -> Tuple[List[Dict[str, Any]], int]:
            if simple_filter_value == "ACTIVO":
                activo = True
            elif simple_filter_value == "INACTIVO":
                activo = False
            else:
                activo = None
            rows = db.fetch_articles(
                search=search,
                activo_only=activo,
                sorts=sorts,
                limit=limit,
                offset=offset,
            )
            total = db.count_articles(search=search, activo_only=activo)
            return rows, total

        # ---------- Tables (generic & reusable) ----------
        entity_columns = [
            ColumnConfig(key="nombre_completo", label="Entidad"),
            ColumnConfig(key="tipo", label="Tipo", formatter=lambda v, _: v or "—"),
            ColumnConfig(key="lista_precio", label="Lista"),
            ColumnConfig(key="cuit", label="CUIT"),
            ColumnConfig(key="localidad", label="Localidad"),
            ColumnConfig(
                key="activo",
                label="Activo",
                editable=True,
                renderer=lambda row: ft.Text("Sí" if row.get("activo") else "No", size=12),
                inline_editor=lambda value, row, setter: ft.Switch(
                    value=bool(row.get("activo")),
                    on_change=lambda e: setter(e.control.value),
                ),
            ),
            ColumnConfig(
                key="detalle",
                label="Detalle",
                sortable=False,
                renderer=lambda row: ft.IconButton(
                    icon=ft.Icons.INFO_OUTLINE,
                    tooltip="Ver detalle",
                    on_click=lambda e, ent=row: update_entity_detail(ent),
                ),
            ),
        ]

        entity_table = GenericTable(
            columns=entity_columns,
            data_provider=entity_provider,
            simple_filter=SimpleFilterConfig(
                label="Tipo",
                options=[(None, "Todos"), ("CLIENTE", "Cliente"), ("PROVEEDOR", "Proveedor"), ("AMBOS", "Ambos")],
            ),
            inline_edit_callback=lambda row_id, changes: db.update_entity_fields(int(row_id), changes),
            mass_edit_callback=lambda ids, updates: db.bulk_update_entities([int(i) for i in ids], updates),
            mass_delete_callback=lambda ids: db.delete_entities([int(i) for i in ids]),
            auto_load=True,
            page_size=12,
        )

        article_columns = [
            ColumnConfig(key="nombre", label="Artículo"),
            ColumnConfig(key="marca", label="Marca"),
            ColumnConfig(key="rubro", label="Rubro"),
            ColumnConfig(key="costo", label="Costo", editable=True, formatter=lambda v, _: _format_money(v)),
            ColumnConfig(key="stock_actual", label="Stock", formatter=lambda v, _: "—" if v is None else f"{float(v):.2f}"),
            ColumnConfig(key="porcentaje_iva", label="IVA", formatter=lambda v, _: f"{float(v or 0):.0f}%"),
            ColumnConfig(
                key="activo",
                label="Estado",
                renderer=lambda row: ft.Text("Activo" if row.get("activo") else "Inactivo"),
                editable=True,
                inline_editor=lambda value, row, setter: ft.Switch(
                    value=bool(row.get("activo")),
                    on_change=lambda e: setter(e.control.value),
                ),
            ),
        ]

        article_table = GenericTable(
            columns=article_columns,
            data_provider=article_provider,
            simple_filter=SimpleFilterConfig(
                label="Estado",
                options=[(None, "Todos"), ("ACTIVO", "Activos"), ("INACTIVO", "Inactivos")],
                default="ACTIVO",
            ),
            inline_edit_callback=lambda row_id, changes: db.update_article_fields(int(row_id), changes),
            mass_edit_callback=lambda ids, updates: db.bulk_update_articles([int(i) for i in ids], updates),
            mass_delete_callback=lambda ids: db.delete_articles([int(i) for i in ids]),
            auto_load=True,
            page_size=10,
        )

        entity_table_view = entity_table.build()
        article_table_view = article_table.build()

        stock_list = ft.ListView(expand=1, spacing=10, padding=0)
        stock_empty = ft.Container(
            visible=False,
            alignment=ft.alignment.center,
            content=ft.Column(
                [
                    ft.Icon(getattr(ft.Icons, "CHECK_CIRCLE_OUTLINE", ft.Icons.CHECK_CIRCLE), size=34, color=ft.Colors.GREEN_400),
                    ft.Text("Sin alertas", weight=ft.FontWeight.BOLD, color=COLOR_TEXT),
                    ft.Text("No hay artículos por debajo del mínimo.", size=12, color=COLOR_TEXT_MUTED),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=6,
                tight=True,
            ),
        )
        stock_stack = ft.Stack([stock_list, stock_empty], expand=True)

        def refresh_stock() -> None:
            try:
                alerts = db.fetch_stock_alerts(limit=12)
                stock_list.controls = [_build_stock_alert_tile(alert) for alert in alerts]
                stock_alertas_text.value = str(len(alerts))
                stock_empty.visible = len(alerts) == 0
                set_connection(True, "DB conectado")
                hide_message()
            except Exception as exc:
                set_connection(False, "DB error")
                show_message(f"Error cargando stock: {exc}", kind="error")

        # ---------- Views ----------
        def header(title: str, subtitle: str, on_refresh, actions: Optional[List[ft.Control]] = None) -> ft.Row:
            buttons: List[ft.Control] = []
            if actions:
                buttons.extend(actions)
            buttons.append(
                ft.ElevatedButton(
                    "Actualizar",
                    icon=ft.Icons.REFRESH,
                    bgcolor=COLOR_PRIMARY,
                    color=ft.Colors.WHITE,
                    on_click=lambda e: on_refresh(),
                )
            )
            return ft.Row(
                [
                    ft.Column(
                        [
                            ft.Text(title, size=22, weight=ft.FontWeight.BOLD, color=COLOR_TEXT),
                            ft.Text(subtitle, size=12, color=COLOR_TEXT_MUTED),
                        ],
                        spacing=2,
                    ),
                    ft.Row(buttons, spacing=10),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            )

        def metric_card(title: str, value_text: ft.Text, icon: str) -> ft.Container:
            return ft.Container(
                content=ft.Row(
                    [
                        ft.Container(
                            width=40,
                            height=40,
                            border_radius=12,
                            bgcolor=COLOR_SURFACE_2,
                            alignment=ft.alignment.center,
                            content=ft.Icon(icon, color=COLOR_PRIMARY),
                        ),
                        ft.Column(
                            [
                                ft.Text(title, size=12, color=COLOR_TEXT_MUTED),
                                value_text,
                            ],
                            spacing=2,
                        ),
                    ],
                    spacing=12,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                padding=12,
                bgcolor=COLOR_CARD,
                border=ft.border.all(1, COLOR_BORDER),
                border_radius=14,
                expand=1,
            )

        entities_total_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)
        entities_clientes_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)
        entities_proveedores_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)

        articulos_total_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)
        articulos_activos_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)
        stock_alertas_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)

        def refresh_entity_metrics() -> None:
            try:
                entities_total_text.value = str(db.count_entities(None, None))
                entities_clientes_text.value = str(db.count_entities_by_type("CLIENTE"))
                entities_proveedores_text.value = str(db.count_entities_by_type("PROVEEDOR"))
                set_connection(True, "DB conectado")
            except Exception as exc:
                entities_total_text.value = "—"
                entities_clientes_text.value = "—"
                entities_proveedores_text.value = "—"
                set_connection(False, "DB error")
                show_message(f"Error cargando métricas de entidades: {exc}", kind="error")

        def refresh_article_metrics() -> None:
            try:
                articulos_total_text.value = str(db.count_articles(None, None))
                articulos_activos_text.value = str(db.count_articles(None, True))
                set_connection(True, "DB conectado")
            except Exception as exc:
                articulos_total_text.value = "—"
                articulos_activos_text.value = "—"
                set_connection(False, "DB error")
                show_message(f"Error cargando métricas de artículos: {exc}", kind="error")

        def refresh_stock_metrics() -> None:
            try:
                stock_alertas_text.value = str(len(db.fetch_stock_alerts(limit=12)))
                set_connection(True, "DB conectado")
            except Exception:
                stock_alertas_text.value = "—"

        def refresh_entities() -> None:
            try:
                hide_message()
                entity_table.refresh()
                refresh_entity_metrics()
            except Exception as exc:
                set_connection(False, "DB error")
                show_message(f"Error cargando entidades: {exc}", kind="error")

        def refresh_articles() -> None:
            try:
                hide_message()
                article_table.refresh()
                refresh_article_metrics()
            except Exception as exc:
                set_connection(False, "DB error")
                show_message(f"Error cargando artículos: {exc}", kind="error")

        # ---------- Create dialogs ----------
        form_dialog = ft.AlertDialog(modal=True)

        def close_form(_: Any = None) -> None:
            _close_dialog(form_dialog)

        def open_form(title: str, content: ft.Control, actions: List[ft.Control]) -> None:
            form_dialog.title = ft.Text(title, weight=ft.FontWeight.BOLD)
            form_dialog.content = content
            form_dialog.actions = actions
            _open_dialog(form_dialog)

        entity_nombre = ft.TextField(label="Nombre", width=240)
        _style_input(entity_nombre)
        entity_apellido = ft.TextField(label="Apellido", width=240)
        _style_input(entity_apellido)
        entity_razon_social = ft.TextField(label="Razón social", width=490)
        _style_input(entity_razon_social)
        def tipo_loader(query: str, offset: int, limit: int) -> Tuple[List[Dict], bool]:
            options = [
                {"label": "—", "value": ""},
                {"label": "Cliente", "value": "CLIENTE"},
                {"label": "Proveedor", "value": "PROVEEDOR"},
                {"label": "Ambos", "value": "AMBOS"},
            ]
            filtered = [o for o in options if query.lower() in o["label"].lower()]
            return filtered[offset:offset+limit], offset + limit < len(filtered)

        entity_tipo = AsyncSelect(
            loader=tipo_loader,
            value="",
            placeholder="Seleccionar tipo...",
            width=240,
            label="Tipo",
        )
        entity_cuit = ft.TextField(label="CUIT", width=240)
        _style_input(entity_cuit)
        entity_telefono = ft.TextField(label="Teléfono", width=240)
        _style_input(entity_telefono)
        entity_email = ft.TextField(label="Email", width=490)
        _style_input(entity_email)
        entity_domicilio = ft.TextField(label="Domicilio", width=490)
        _style_input(entity_domicilio)
        entity_activo = ft.Switch(label="Activo", value=True)

        def create_entity(_: Any = None) -> None:
            try:
                db.create_entity(
                    nombre=entity_nombre.value,
                    apellido=entity_apellido.value,
                    razon_social=entity_razon_social.value,
                    tipo=entity_tipo.value if entity_tipo.value else None,
                    cuit=entity_cuit.value,
                    telefono=entity_telefono.value,
                    email=entity_email.value,
                    domicilio=entity_domicilio.value,
                    activo=bool(entity_activo.value),
                )
                close_form()
                entity_table.refresh()
                refresh_entity_metrics()
                show_message("Entidad creada", kind="success")
            except Exception as exc:
                show_message(f"Error creando entidad: {exc}", kind="error")

        def open_new_entity(_: Any = None) -> None:
            entity_nombre.value = ""
            entity_apellido.value = ""
            entity_razon_social.value = ""
            entity_tipo.value = ""
            entity_cuit.value = ""
            entity_telefono.value = ""
            entity_email.value = ""
            entity_domicilio.value = ""
            entity_activo.value = True
            content = ft.Container(
                width=520,
                content=ft.Column(
                    [
                        ft.Row([entity_nombre, entity_apellido], spacing=10),
                        ft.Row([entity_razon_social], spacing=10),
                        ft.Row([entity_tipo, entity_cuit], spacing=10),
                        ft.Row([entity_telefono], spacing=10),
                        ft.Row([entity_email], spacing=10),
                        ft.Row([entity_domicilio], spacing=10),
                        ft.Row([entity_activo], spacing=10),
                    ],
                    spacing=10,
                    tight=True,
                ),
            )
            open_form(
                "Nueva entidad",
                content,
                [
                    ft.TextButton("Cancelar", on_click=close_form),
                    ft.ElevatedButton(
                        "Crear",
                        icon=ft.Icons.ADD,
                        bgcolor=COLOR_PRIMARY,
                        color=ft.Colors.WHITE,
                        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)),
                        on_click=create_entity,
                    ),
                ],
            )

        marcas_values: List[str] = []
        rubros_values: List[str] = []

        def reload_catalogs() -> None:
            nonlocal marcas_values, rubros_values
            try:
                marcas_values = db.list_marcas()
                rubros_values = db.list_rubros()
            except Exception:
                marcas_values = []
                rubros_values = []

        def marcas_loader(query: str, offset: int, limit: int) -> Tuple[List[Dict], bool]:
            options = [{"label": "(Sin marca)", "value": ""}] + [{"label": m, "value": m} for m in marcas_values]
            filtered = [o for o in options if query.lower() in o["label"].lower()]
            return filtered[offset:offset+limit], offset + limit < len(filtered)

        def rubros_loader(query: str, offset: int, limit: int) -> Tuple[List[Dict], bool]:
            options = [{"label": "(Sin rubro)", "value": ""}] + [{"label": r, "value": r} for r in rubros_values]
            filtered = [o for o in options if query.lower() in o["label"].lower()]
            return filtered[offset:offset+limit], offset + limit < len(filtered)

        article_nombre = ft.TextField(label="Nombre", width=490)
        _style_input(article_nombre)
        article_marca = AsyncSelect(
            loader=marcas_loader,
            value="",
            placeholder="Seleccionar marca...",
            width=240,
            label="Marca",
        )
        article_rubro = AsyncSelect(
            loader=rubros_loader,
            value="",
            placeholder="Seleccionar rubro...",
            width=240,
            label="Rubro",
        )
        article_costo = ft.TextField(label="Costo", width=240, value="0")
        _style_input(article_costo)
        article_stock_minimo = ft.TextField(label="Stock mínimo", width=240, value="0")
        _style_input(article_stock_minimo)
        article_ubicacion = ft.TextField(label="Ubicación", width=490)
        _style_input(article_ubicacion)
        article_activo = ft.Switch(label="Activo", value=True)

        def create_article(_: Any = None) -> None:
            try:
                db.create_article(
                    nombre=article_nombre.value or "",
                    marca=article_marca.value or None,
                    rubro=article_rubro.value or None,
                    costo=article_costo.value,
                    stock_minimo=article_stock_minimo.value,
                    ubicacion=article_ubicacion.value,
                    activo=bool(article_activo.value),
                )
                close_form()
                article_table.refresh()
                refresh_article_metrics()
                show_message("Artículo creado", kind="success")
            except Exception as exc:
                show_message(f"Error creando artículo: {exc}", kind="error")

        def open_new_article(_: Any = None) -> None:
            try:
                reload_catalogs()
            except Exception as exc:
                show_message(f"Error cargando catálogos: {exc}", kind="error")
            article_nombre.value = ""
            article_marca.value = ""
            article_rubro.value = ""
            article_costo.value = "0"
            article_stock_minimo.value = "0"
            article_ubicacion.value = ""
            article_activo.value = True
            content = ft.Container(
                width=520,
                content=ft.Column(
                    [
                        ft.Row([article_nombre], spacing=10),
                        ft.Row([article_marca, article_rubro], spacing=10),
                        ft.Row([article_costo, article_stock_minimo], spacing=10),
                        ft.Row([article_ubicacion], spacing=10),
                        ft.Row([article_activo], spacing=10),
                    ],
                    spacing=10,
                    tight=True,
                ),
            )
            open_form(
                "Nuevo artículo",
                content,
                [
                    ft.TextButton("Cancelar", on_click=close_form),
                    ft.ElevatedButton(
                        "Crear",
                        icon=ft.Icons.ADD,
                        bgcolor=COLOR_PRIMARY,
                        color=ft.Colors.WHITE,
                        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)),
                        on_click=create_article,
                    ),
                ],
            )

        entidades_view = ft.Column(
            [
                header(
                    "Entidades",
                    "Buscá, filtrá y editá rápidamente.",
                    refresh_entities,
                    actions=[
                        ft.OutlinedButton(
                            "Nueva",
                            icon=ft.Icons.ADD,
                            on_click=open_new_entity,
                        )
                    ],
                ),
                ft.Row(
                    [
                        metric_card("Total", entities_total_text, getattr(ft.Icons, "GROUP_OUTLINED", ft.Icons.GROUP)),
                        metric_card("Clientes", entities_clientes_text, getattr(ft.Icons, "PERSON_OUTLINED", ft.Icons.PERSON)),
                        metric_card("Proveedores", entities_proveedores_text, getattr(ft.Icons, "LOCAL_SHIPPING_OUTLINED", ft.Icons.LOCAL_SHIPPING)),
                    ],
                    spacing=12,
                ),
                ft.Row(
                    [
                        ft.Container(
                            content=entity_table_view,
                            expand=3,
                            padding=12,
                            bgcolor=COLOR_CARD,
                            border_radius=14,
                            border=ft.border.all(1, COLOR_BORDER),
                        ),
                        entity_detail,
                    ],
                    expand=True,
                    spacing=12,
                ),
            ],
            expand=True,
            spacing=12,
        )

        articulos_view = ft.Column(
            [
                header(
                    "Artículos",
                    "Inventario con edición inline y estado.",
                    refresh_articles,
                    actions=[
                        ft.OutlinedButton(
                            "Nuevo",
                            icon=ft.Icons.ADD,
                            on_click=open_new_article,
                        )
                    ],
                ),
                ft.Row(
                    [
                        metric_card("Total", articulos_total_text, getattr(ft.Icons, "INVENTORY_2_OUTLINED", ft.Icons.INVENTORY_2)),
                        metric_card("Activos", articulos_activos_text, getattr(ft.Icons, "CHECK_CIRCLE_OUTLINE", ft.Icons.CHECK_CIRCLE)),
                        metric_card("Alertas stock", stock_alertas_text, getattr(ft.Icons, "WARNING_AMBER_OUTLINED", ft.Icons.WARNING_AMBER)),
                    ],
                    spacing=12,
                ),
                ft.Container(
                    content=article_table_view,
                    expand=1,
                    padding=12,
                    bgcolor=COLOR_CARD,
                    border_radius=14,
                    border=ft.border.all(1, COLOR_BORDER),
                ),
            ],
            expand=True,
            spacing=12,
        )

        stock_view = ft.Column(
            [
                header("Stock", "Alertas por debajo del mínimo.", refresh_stock),
                ft.Container(
                    content=stock_stack,
                    expand=1,
                    padding=12,
                    bgcolor=COLOR_CARD,
                    border_radius=14,
                    border=ft.border.all(1, COLOR_BORDER),
                ),
            ],
            expand=True,
            spacing=12,
        )

        # ---------- Shell / navigation ----------
        content_holder = ft.Container(expand=1, padding=16, content=entidades_view)
        current_view = {"key": "entidades"}
        current_title = ft.Text("Entidades", size=14, color=COLOR_TEXT_MUTED)

        def set_view(key: str) -> None:
            current_view["key"] = key
            if key == "entidades":
                content_holder.content = entidades_view
                current_title.value = "Entidades"
                page.update()
                refresh_entities()
            elif key == "articulos":
                content_holder.content = articulos_view
                current_title.value = "Artículos"
                page.update()
                refresh_articles()
            elif key == "stock":
                content_holder.content = stock_view
                current_title.value = "Stock"
                page.update()
                refresh_stock()
            elif key == "backups":
                content_holder.content = backup_view_control
                current_title.value = "Backups"
                page.update()
                backup_view_component.load_data()

        ICON_ENTIDADES = getattr(ft.Icons, "GROUP", ft.Icons.DASHBOARD)
        ICON_ARTICULOS = getattr(ft.Icons, "CATEGORY", ft.Icons.DASHBOARD)
        ICON_STOCK = getattr(ft.Icons, "SHOW_CHART", ft.Icons.DASHBOARD)
        ICON_BACKUP = getattr(ft.Icons, "BACKUP", ft.Icons.DASHBOARD)
        view_keys = ["entidades", "articulos", "stock", "backups"]

        navigation = ft.NavigationRail(
            selected_index=0,
            label_type=ft.NavigationRailLabelType.SELECTED,
            destinations=[
                ft.NavigationRailDestination(icon=ICON_ENTIDADES, label="Entidades"),
                ft.NavigationRailDestination(icon=ICON_ARTICULOS, label="Artículos"),
                ft.NavigationRailDestination(icon=ICON_STOCK, label="Stock"),
                ft.NavigationRailDestination(icon=ICON_BACKUP, label="Backups"),
            ],
            on_change=lambda e: set_view(view_keys[e.control.selected_index]),
        )

        def refresh_current() -> None:
            if current_view["key"] == "entidades":
                refresh_entities()
            elif current_view["key"] == "articulos":
                refresh_articles()
            elif current_view["key"] == "stock":
                refresh_stock()
            elif current_view["key"] == "backups":
                backup_view_component.load_data()

        page.appbar = ft.AppBar(
            title=ft.Row(
                [
                    ft.Text("Nexoryn Tech", weight=ft.FontWeight.BOLD, color=COLOR_TEXT),
                    ft.Container(width=12),
                    current_title,
                ],
                spacing=0,
            ),
            bgcolor=COLOR_CARD,
            elevation=0,
            actions=[
                conn_badge,
                ft.IconButton(icon=ft.Icons.REFRESH, tooltip="Actualizar", on_click=lambda e: refresh_current()),
            ],
        )

        sidebar = ft.Container(
            content=ft.Column(
                [
                    ft.Container(
                        content=navigation,
                        expand=1,
                        padding=ft.padding.only(top=12),
                    ),
                ],
                expand=True,
            ),
            width=96,
            padding=12,
            bgcolor=COLOR_CARD,
            border=ft.border.all(1, COLOR_BORDER),
        )

        content_area = ft.Column(
            [
                ft.Container(message_bar, padding=ft.padding.only(left=16, right=16, top=12)),
                content_holder,
            ],
            expand=True,
            spacing=0,
        )

        maintenance_title = ft.Text("Preparando sistema...", size=22, weight=ft.FontWeight.BOLD, color=COLOR_TEXT)
        maintenance_detail = ft.Text("Verificando respaldos y migraciones...", size=13, color=COLOR_TEXT_MUTED)
        maintenance_progress = ft.ProgressBar(width=360, color=COLOR_PRIMARY, bgcolor=COLOR_BORDER, value=None)
        maintenance_badge_text = ft.Text("", size=11, weight=ft.FontWeight.BOLD, color="#FFFFFF")
        maintenance_badge = ft.Container(
            content=maintenance_badge_text,
            bgcolor=COLOR_PRIMARY,
            padding=ft.padding.symmetric(horizontal=12, vertical=6),
            border_radius=8,
            visible=False,
        )
        maintenance_overlay = ft.Container(
            content=ft.Column(
                [
                    ft.Container(
                        content=ft.Icon(ft.Icons.SETTINGS_SUGGEST_ROUNDED, size=56, color=COLOR_PRIMARY),
                        bgcolor=f"{COLOR_PRIMARY}15",
                        padding=22,
                        border_radius=30,
                        margin=ft.margin.only(bottom=16),
                    ),
                    maintenance_title,
                    ft.Container(height=8),
                    maintenance_detail,
                    ft.Container(height=18),
                    maintenance_badge,
                    ft.Container(height=12),
                    maintenance_progress,
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            visible=False,
            expand=True,
            bgcolor="#F1F5F9E6",
            padding=40,
            alignment=ft.alignment.center,
        )

        def _set_maintenance_state(
            title: str,
            detail: str,
            *,
            progress: Optional[float] = None,
            badge: Optional[str] = None,
            badge_color: Optional[str] = None,
        ) -> None:
            maintenance_overlay.visible = True
            maintenance_title.value = title
            maintenance_detail.value = detail
            maintenance_progress.visible = True
            maintenance_progress.value = progress
            if badge:
                maintenance_badge_text.value = badge
                maintenance_badge.bgcolor = badge_color or COLOR_PRIMARY
                maintenance_badge.visible = True
            else:
                maintenance_badge.visible = False
            page.update()

        def _hide_maintenance() -> None:
            maintenance_overlay.visible = False
            maintenance_progress.value = 0
            maintenance_badge.visible = False
            page.update()

        def run_startup_maintenance(on_success: Callable[[], None]) -> None:
            def _backup_progress(backup_type: str, status: str, current: int, total: int) -> None:
                label_map = {
                    "daily": "DIARIO",
                    "weekly": "SEMANAL",
                    "monthly": "MENSUAL",
                }
                label = label_map.get(backup_type, backup_type.upper())
                if status == "running":
                    progress = (current - 1) / max(total, 1)
                    _set_maintenance_state(
                        "Ejecutando respaldos pendientes...",
                        f"{label} en progreso",
                        progress=progress,
                        badge=label,
                        badge_color=COLOR_PRIMARY,
                    )
                elif status == "completed":
                    progress = current / max(total, 1)
                    _set_maintenance_state(
                        "Ejecutando respaldos pendientes...",
                        f"{label} completado",
                        progress=progress,
                        badge=label,
                        badge_color=COLOR_SUCCESS,
                    )
                elif status == "failed":
                    progress = current / max(total, 1)
                    _set_maintenance_state(
                        "Error en respaldos",
                        f"{label} fallido",
                        progress=progress,
                        badge="ERROR",
                        badge_color=COLOR_ERROR,
                    )

            def _schema_progress(payload: Dict[str, Any]) -> None:
                phase = payload.get("phase")
                if phase in {"extensions", "schemas"}:
                    _set_maintenance_state(
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
                    _set_maintenance_state(
                        "Actualizando esquema...",
                        payload.get("message", "Sincronizando..."),
                        progress=progress,
                        badge="SCHEMA",
                        badge_color=COLOR_INFO,
                    )
                    return

            def _run() -> None:
                try:
                    _set_maintenance_state(
                        "Preparando sistema...",
                        "Verificando respaldos y esquema...",
                        progress=None,
                    )

                    missed = backup_service.check_missed_backups(db)
                    if missed:
                        for path in [
                            backup_service.daily_dir,
                            backup_service.weekly_dir,
                            backup_service.monthly_dir,
                            backup_service.manual_dir,
                        ]:
                            try:
                                path.mkdir(parents=True, exist_ok=True)
                            except Exception:
                                pass
                        results = backup_service.execute_missed_backups(
                            db,
                            missed,
                            progress_callback=_backup_progress,
                        )
                        if not results or not all(results.values()):
                            _set_maintenance_state(
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
                            _set_maintenance_state(
                                "Error actualizando esquema",
                                result.error or "Fallo la sincronizacion.",
                                progress=1.0,
                                badge="ERROR",
                                badge_color=COLOR_ERROR,
                            )
                            return

                    _hide_maintenance()
                    on_success()
                except Exception as exc:
                    _set_maintenance_state(
                        "Error de mantenimiento",
                        str(exc),
                        progress=1.0,
                        badge="ERROR",
                        badge_color=COLOR_ERROR,
                    )

            threading.Thread(target=_run, daemon=True).start()

        page.add(
            ft.Stack(
                [
                    ft.Row(
                        [
                            sidebar,
                            ft.Container(content_area, expand=1, bgcolor=COLOR_BG),
                        ],
                        expand=True,
                        spacing=0,
                    ),
                    maintenance_overlay,
                ],
                expand=True,
            )
        )

        def finalize_startup() -> None:
            set_view("entidades")
            refresh_stock_metrics()
            page.on_close = None
            page.on_window_event = None

        run_startup_maintenance(finalize_startup)
    except Exception as exc:
        show_fatal_error(exc)
        _shutdown("fatal_exception")


if __name__ == "__main__":
    ft.app(target=main)
