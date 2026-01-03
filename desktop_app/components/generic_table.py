from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import flet as ft
import csv
import os
import threading
import threading
from datetime import datetime
from desktop_app.services.export_service import ExportService

SortSpec = List[Tuple[str, str]]  # [(key, "asc"|"desc")] in order

DataProvider = Callable[
    [int, int, Optional[str], Optional[str], Dict[str, Any], SortSpec],
    Tuple[List[Dict[str, Any]], int],
]
InlineEditCallback = Callable[[Any, Dict[str, Any]], None]
MassEditCallback = Callable[[List[Any], Dict[str, Any]], None]
MassDeleteCallback = Callable[[List[Any]], None]


def _expander(label: str, content: ft.Control) -> ft.Control:
    is_open = {"value": False}
    chevron = ft.Icon(ft.Icons.KEYBOARD_ARROW_DOWN_ROUNDED, size=20, color="#64748B")
    body = ft.Container(content=content, visible=False, padding=ft.padding.only(top=15, bottom=5))

    def toggle(_: Any) -> None:
        is_open["value"] = not is_open["value"]
        body.visible = is_open["value"]
        chevron.icon = ft.Icons.KEYBOARD_ARROW_UP_ROUNDED if is_open["value"] else ft.Icons.KEYBOARD_ARROW_DOWN_ROUNDED
        header.bgcolor = "#F8FAFC" if not is_open["value"] else "#FFFFFF"
        header.update()
        body.update()

    header = ft.Container(
        on_click=toggle,
        padding=ft.padding.symmetric(horizontal=16, vertical=12),
        border_radius=12,
        bgcolor="#F8FAFC",
        border=ft.border.all(1, "#E2E8F0"),
        animate=ft.Animation(200, ft.AnimationCurve.EASE_OUT),
        content=ft.Row(
            [ft.Text(label, weight=ft.FontWeight.BOLD, size=13, color="#1E293B"), chevron],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        ),
    )
    return ft.Column([header, body], spacing=0)


def _scroll_auto():
    scroll_mode = getattr(ft, "ScrollMode", None)
    if scroll_mode is not None and hasattr(scroll_mode, "AUTO"):
        return scroll_mode.AUTO
    return "auto"

_ALL_VALUE = "__ALL__"
_FILTER_RESET_UNSET = object()


def _dropdown_option_key(option: Any) -> Any:
    key = getattr(option, "key", None)
    if key is None:
        return getattr(option, "text", None)
    return key


class SafeDataTable(ft.DataTable):
    """Subclass of DataTable to fix TypeErrors in Python 3.14 + Flet 0.28.3"""
    def before_update(self):
        try:
            # Ensure index is int or None before parent check
            if hasattr(self, "sort_column_index"):
                val = self.sort_column_index
                if val is not None and not isinstance(val, int):
                    try:
                        self.sort_column_index = int(val)
                    except:
                        self.sort_column_index = None
            super().before_update()
        except TypeError:
            # If native check still fails, force recovery
            self.sort_column_index = None
            try:
                super().before_update()
            except:
                pass
        except Exception:
            pass


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
    _maybe_set(control, "border_color", "#475569") # Slate 600
    _maybe_set(control, "focused_border_color", "#4F46E5")
    _maybe_set(control, "border_radius", 12)
    _maybe_set(control, "text_size", 14)
    _maybe_set(control, "label_style", ft.TextStyle(color="#1E293B", size=13, weight=ft.FontWeight.BOLD))
    _maybe_set(control, "content_padding", ft.padding.all(12))

    if is_dropdown:
        _maybe_set(control, "bgcolor", "#F8FAFC")
        _maybe_set(control, "filled", True)
        _maybe_set(control, "border_width", 2)
        _maybe_set(control, "enable_search", True)
        return

    _maybe_set(control, "filled", True)
    _maybe_set(control, "bgcolor", "#F8FAFC")
    _maybe_set(control, "border_width", 1)

    if is_textfield and not is_dropdown:
        _maybe_set(control, "height", 50)
        _maybe_set(control, "cursor_color", "#4F46E5")
        _maybe_set(control, "selection_color", "#C7D2FE")


def _style_cell_editor(control: Any, *, width: Optional[int] = None) -> None:
    _maybe_set(control, "filled", True)
    _maybe_set(control, "bgcolor", "#FFFFFF")
    _maybe_set(control, "border_color", "#E2E8F0")
    _maybe_set(control, "focused_border_color", "#6366F1")
    _maybe_set(control, "border_radius", 6)
    _maybe_set(control, "text_size", 12)
    _maybe_set(control, "dense", True)
    _maybe_set(control, "height", 38)
    if width is not None:
        _maybe_set(control, "width", width)


@dataclass
class ColumnConfig:
    key: str
    label: str
    editable: bool = False
    sortable: bool = True
    width: Optional[int] = None
    formatter: Optional[Callable[[Any, Dict[str, Any]], str]] = None
    renderer: Optional[Callable[[Dict[str, Any]], ft.Control]] = None
    inline_editor: Optional[
        Callable[[Any, Dict[str, Any], Callable[[Any], None]], ft.Control]
    ] = None


@dataclass
class SimpleFilterConfig:
    label: str
    options: Sequence[Tuple[Optional[str], str]]
    default: Optional[str] = None


@dataclass
class AdvancedFilterControl:
    name: str
    control: ft.Control
    getter: Callable[[ft.Control], Any] = field(
        default_factory=lambda: lambda ctrl: getattr(ctrl, "value", None)
    )
    setter: Optional[Callable[[ft.Control, Any], None]] = None
    initial_value: Any = field(default=_FILTER_RESET_UNSET, repr=False)

    def __post_init__(self) -> None:
        if self.initial_value is _FILTER_RESET_UNSET and hasattr(self.control, "value"):
            self.initial_value = getattr(self.control, "value")
        if isinstance(self.control, ft.TextField) and self.initial_value is None:
            self.initial_value = ""
        if isinstance(self.control, ft.Dropdown):
            options = getattr(self.control, "options", [])
            option_keys = [_dropdown_option_key(opt) for opt in options]
            if option_keys:
                if self.initial_value is None:
                    self.initial_value = option_keys[0]
                else:
                    if self.initial_value not in option_keys:
                        try:
                            candidate = str(self.initial_value)
                        except Exception:
                            candidate = None
                        if candidate is None or candidate not in option_keys:
                            self.initial_value = option_keys[0]


class GenericTable:
    def __init__(
        self,
        columns: Sequence[ColumnConfig],
        data_provider: DataProvider,
        id_field: str = "id",
        simple_filter: Optional[SimpleFilterConfig] = None,
        advanced_filters: Optional[Sequence[AdvancedFilterControl]] = None,
        inline_edit_callback: Optional[InlineEditCallback] = None,
        mass_edit_callback: Optional[MassEditCallback] = None,
        mass_delete_callback: Optional[MassDeleteCallback] = None,
        show_inline_controls: bool = True,
        show_mass_actions: bool = True,
        show_selection: bool = True,
        auto_load: bool = False,
        page_size: int = 10,
        page_size_options: Sequence[int] = (10, 25, 50),
        show_export_button: bool = True,
    ) -> None:
        super().__init__()
        self.columns = list(columns)
        self.data_provider = data_provider
        self.id_field = id_field
        self.simple_filter = simple_filter
        self.advanced_filters = list(advanced_filters or [])
        self.inline_edit_callback = inline_edit_callback
        self.mass_edit_callback = mass_edit_callback
        self.mass_delete_callback = mass_delete_callback
        self.show_inline_controls = show_inline_controls
        self.show_mass_actions = show_mass_actions
        self.show_selection = show_selection
        self.auto_load = auto_load
        self.show_export_button = show_export_button
        self.page = 1
        self.page_size = page_size
        self.page_size_options = list(page_size_options)
        self.sorts: SortSpec = []
        self._last_sort_idx: Optional[int] = None  # Internal tracking
        self.selected_ids: set = set()
        self._row_selection_controls: Dict[Any, ft.Checkbox] = {}
        self._current_page_ids: List[Any] = []
        self.select_all_checkbox: Optional[ft.Checkbox] = None
        self.edit_buffers: Dict[Any, Dict[str, Any]] = {}
        self.current_rows: List[Dict[str, Any]] = []
        self.total_rows = 0
        self.total_pages = 1
        self._last_error: Optional[str] = None
        self._search_timer: Optional[threading.Timer] = None
        self.select_all_global = False
        
        self.selection_bar_text = ft.Text("", size=12, color=ft.Colors.BLUE_700)
        self.selection_bar_btn = ft.TextButton("Seleccionar todo", on_click=lambda _: self._toggle_global_selection(True))
        self.selection_bar = ft.Container(
            content=ft.Row([self.selection_bar_text, self.selection_bar_btn], alignment=ft.MainAxisAlignment.CENTER),
            bgcolor=ft.Colors.BLUE_50,
            padding=5,
            border_radius=4,
            visible=False,
            margin=ft.margin.only(bottom=10)
        )

        self.search_field = ft.TextField(
            expand=1,
            hint_text="Buscar...",
            on_change=lambda e: self.trigger_refresh(),
        )
        _style_input(self.search_field)
        self.simple_filter_dropdown = (
            ft.Dropdown(
                label=self.simple_filter.label,
                options=[
                    ft.dropdown.Option(_ALL_VALUE if value is None else str(value), label)
                    for value, label in self.simple_filter.options
                ],
                value=_ALL_VALUE if self.simple_filter.default is None else str(self.simple_filter.default),
                on_change=lambda e: self._on_simple_filter_change(),
            )
            if self.simple_filter
            else None
        )
        self._simple_default_value = (
            (_ALL_VALUE if self.simple_filter.default is None else str(self.simple_filter.default))
            if self.simple_filter
            else None
        )
        if self.simple_filter_dropdown:
            _style_input(self.simple_filter_dropdown)
        self.export_button = ft.IconButton(
            icon=ft.Icons.FILE_DOWNLOAD_ROUNDED,
            tooltip="Exportar datos",
            on_click=lambda e: self._open_export_dialog(),
        )
        self.reset_button = ft.IconButton(
            icon=ft.Icons.REPLAY,
            tooltip="Reiniciar filtros (DEBUG)",
            on_click=lambda e: self._reset_filters(),
        )
        self.refresh_button = ft.IconButton(
            icon=ft.Icons.REFRESH_ROUNDED,
            tooltip="Actualizar",
            on_click=lambda e: self.refresh(),
        )
        self.advanced_filters_row = ft.Row(
            [flt.control for flt in self.advanced_filters],
            wrap=True,
            spacing=12,
            run_spacing=12,
        )
        self.advanced_expander = (
            _expander(
                label="Filtros avanzados",
                content=self.advanced_filters_row,
            )
            if self.advanced_filters
            else None
        )
        self.status = ft.Text("", size=11, color="#64748B")
        editable_columns = [col for col in self.columns if col.editable]
        self._mass_field_key: Optional[str] = None
        self._mass_value: Any = None
        self.mass_field_dropdown = ft.Dropdown(
            label="Campo",
            width=220,
            options=[ft.dropdown.Option(col.key, col.label) for col in editable_columns],
            on_change=lambda e: self._on_mass_field_change(e.control.value),
        )
        _style_input(self.mass_field_dropdown)
        self.mass_value_container = ft.Container(
            expand=1,
            content=ft.Text("Selecciona un campo", size=12, color="#64748B"),
        )
        self.mass_edit_button = ft.ElevatedButton(
            "Aplicar Cambios",
            icon=ft.Icons.AUTO_FIX_HIGH_ROUNDED,
            bgcolor="#6366F1",
            color="#FFFFFF",
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
            on_click=lambda e: self._apply_mass_edit(),
        )
        self.mass_delete_button = ft.ElevatedButton(
            "Eliminar Seleccionados",
            icon=ft.Icons.DELETE_SWEEP_ROUNDED,
            bgcolor="#EF4444",
            color="#FFFFFF",
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
            on_click=lambda e: self._confirm_mass_delete(),
        )
        if hasattr(self.mass_edit_button, "disabled"):
            self.mass_edit_button.disabled = True
        if hasattr(self.mass_delete_button, "disabled"):
            self.mass_delete_button.disabled = True
        self.status = ft.Text("", size=11, color="#64748B")
        self.results_label = ft.Text("0 resultados", size=11, color="#64748B")
        self.range_label = ft.Text("", size=11, color="#64748B")
        self.sort_label = ft.Text("Orden: —", size=11, color="#64748B")
        self.selected_label = ft.Text("0 seleccionados", size=11, color="#64748B")
        self.page_size_dropdown = ft.Dropdown(
            label="Filas",
            value=str(self.page_size),
            options=[ft.dropdown.Option(str(size), str(size)) for size in self.page_size_options],
            on_change=lambda e: self._on_page_size_change(e.control.value),
        )
        _style_input(self.page_size_dropdown)
        self.first_button = ft.IconButton(
            icon=ft.Icons.FIRST_PAGE,
            tooltip="Primera página",
            on_click=lambda e: self._goto_page(1),
        )
        self.prev_button = ft.IconButton(
            icon=ft.Icons.ARROW_BACK,
            tooltip="Página anterior",
            on_click=lambda e: self._goto_page(self.page - 1),
        )
        self.page_input = ft.TextField(
            value=str(self.page),
            width=72,
            on_submit=lambda e: self._goto_page_from_input(e.control.value),
        )
        _style_input(self.page_input)
        self.next_button = ft.IconButton(
            icon=ft.Icons.ARROW_FORWARD,
            tooltip="Página siguiente",
            on_click=lambda e: self._goto_page(self.page + 1),
        )
        self.last_button = ft.IconButton(
            icon=ft.Icons.LAST_PAGE,
            tooltip="Última página",
            on_click=lambda e: self._goto_page(self.total_pages),
        )
        self.clear_sort_button = ft.IconButton(
            icon=ft.Icons.CLEAR_ALL,
            tooltip="Limpiar orden",
            on_click=lambda e: self._clear_sorts(),
            visible=False,
        )
        self.pagination_label = ft.Text("Página 1 de 1")
        table_columns: List[ft.DataColumn] = []
        if self.show_selection:
            self.select_all_checkbox = ft.Checkbox(
                value=False,
                tooltip="Seleccionar todas (página actual)",
                on_change=lambda e: self._toggle_select_all(bool(e.control.value)),
            )
            table_columns.append(
                ft.DataColumn(
                    ft.Row(
                        [
                            self.select_all_checkbox,
                            ft.Text("Sel", size=12, weight=ft.FontWeight.BOLD),
                        ],
                        spacing=6,
                    )
                )
            )
        else:
            self.select_all_checkbox = None
        sort_offset = 1 if self.show_selection else 0
        for idx, col in enumerate(self.columns):
            col_index = idx + sort_offset
            on_sort = (
                (lambda e, key=col.key, index=col_index: self._toggle_sort(key, index))
                if col.sortable
                else None
            )
            try:
                table_columns.append(ft.DataColumn(ft.Text(col.label), on_sort=on_sort))
            except TypeError:
                # Compatibilidad: algunas versiones no aceptan on_sort en el constructor.
                dc = ft.DataColumn(ft.Text(col.label))
                if on_sort is not None and hasattr(dc, "on_sort"):
                    dc.on_sort = on_sort  # type: ignore[attr-defined]
                table_columns.append(dc)
        self.table = SafeDataTable(
            columns=table_columns, 
            column_spacing=24, 
            show_checkbox_column=False, 
            # divider_thickness=1,
            # heading_row_height=56,
            # data_row_max_height=52,
        )
        if hasattr(self.table, "bgcolor"):
            self.table.bgcolor = "#FFFFFF"  # type: ignore[attr-defined]
        if hasattr(self.table, "heading_row_color"):
            self.table.heading_row_color = "#F1F5F9"  # type: ignore[attr-defined]
        if hasattr(self.table, "data_row_color"):
            self.table.data_row_color = "#FFFFFF"  # type: ignore[attr-defined]
        if hasattr(self.table, "divider_thickness"):
            self.table.divider_thickness = 1  # type: ignore[attr-defined]
        if hasattr(self.table, "heading_text_style"):
            self.table.heading_text_style = ft.TextStyle(  # type: ignore[attr-defined]
                size=12,
                weight=ft.FontWeight.W_700,
                color="#475569",
            )
        if hasattr(self.table, "data_text_style"):
            self.table.data_text_style = ft.TextStyle(size=13, color="#1E293B")  # type: ignore[attr-defined]
        self._empty_title = ft.Text("Sin resultados", weight=ft.FontWeight.BOLD)
        self._empty_message = ft.Text("Ajustá el buscador o filtros.", size=12, color="#64748B")
        self._empty_overlay = ft.Container(
            visible=False,
            alignment=ft.alignment.center,
            padding=40,
            content=ft.Column(
                [
                    self._empty_title,
                    self._empty_message,
                    ft.OutlinedButton(
                        "Reintentar",
                        on_click=lambda e: self.refresh(),
                    ),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=6,
                tight=True,
            ),
        )
        self._loading_overlay = ft.Container(
            visible=False,
            alignment=ft.alignment.center,
            padding=40,
            content=ft.Column(
                [
                    ft.ProgressRing(),
                    ft.Text("Cargando…", size=12, color="#64748B"),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
                tight=True,
            ),
        )
        self._table_viewport = ft.Column(
            [ft.Row([self.table], scroll=ft.ScrollMode.ADAPTIVE)],
            expand=True,
            scroll=_scroll_auto(),
        )
        self._table_root = ft.Column(
            [self._loading_overlay, self._empty_overlay, self._table_viewport],
            expand=True,
            spacing=0,
        )
        self.table_container = ft.Container(
            self._table_root,
            expand=1,
            padding=0,
            bgcolor="#FFFFFF",
            border=ft.border.all(1, "#E2E8F0"),
            border_radius=12,
        )
        self._loaded_once = False
        self.root: Optional[ft.Control] = None
        self._confirm_dialog = ft.AlertDialog(modal=True)
        self._edit_dialog = ft.AlertDialog(modal=True)
        self._snack_text = ft.Text("")
        self._snack = ft.SnackBar(content=self._snack_text, open=False)

    def _open_export_dialog(self) -> None:
        self.export_format = ft.Dropdown(
            label="Formato",
            value="Excel",
            options=[
                ft.dropdown.Option("Excel", "Excel (.xlsx)"),
                ft.dropdown.Option("CSV", "CSV (.csv)"),
                ft.dropdown.Option("PDF", "PDF (.pdf)"),
            ],
            width=200,
        )
        self.export_scope = ft.Dropdown(
            label="Alcance",
            value="All", # Default to all data
            options=[
                ft.dropdown.Option("All", "Todo (filtrado)"),
                ft.dropdown.Option("Page", "Página actual"),
            ],
            width=200,
        )
        _style_input(self.export_format)
        _style_input(self.export_scope)

        def close_dlg(e):
            if hasattr(self.root.page, "close"):
                self.root.page.close(self.export_dialog)
            else:
                self.export_dialog.open = False
                self.root.page.update()

        def confirm_export(e):
            fmt = self.export_format.value
            scope = self.export_scope.value
            if hasattr(self.root.page, "close"):
                self.root.page.close(self.export_dialog)
            else:
                self.export_dialog.open = False
                self.root.page.update()
            
            self._perform_export(fmt, scope)

        self.export_dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Exportar Datos"),
            content=ft.Column([
                ft.Text("Seleccioná el formato y el alcance de la exportación."),
                self.export_format,
                self.export_scope,
            ], tight=True, spacing=20),
            actions=[
                ft.TextButton("Cancelar", on_click=close_dlg),
                ft.ElevatedButton(
                    "Exportar", 
                    on_click=confirm_export, 
                    bgcolor="#4F46E5", 
                    color="white",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12))
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        
        # Robust dialog opening
        if not self.root or not self.root.page:
            print("Error: GenericTable not attached to page")
            return

        page = self.root.page
        if hasattr(page, "open"):
            page.open(self.export_dialog)
        else:
            page.dialog = self.export_dialog
            self.export_dialog.open = True
            page.update()

    def _perform_export(self, fmt: str, scope: str) -> None:
        try:
            self._notify("Generando exportación...", kind="info")
            
            # Fetch data based on scope
            if scope == "Page":
                rows = self.current_rows
            else:
                # Fetch all filtered data
                search = self.search_field.value.strip() if self.search_field.value else None
                simple_value = (self.simple_filter_dropdown.value if self.simple_filter_dropdown else None)
                if simple_value == _ALL_VALUE: simple_value = None
                if isinstance(simple_value, str) and not simple_value.strip(): simple_value = None
                
                advanced_payload = {flt.name: flt.getter(flt.control) for flt in self.advanced_filters}
                
                # Fetch a large amount to simulate "All"
                rows, _ = self.data_provider(0, 1000000, search, simple_value, advanced_payload, list(self.sorts))

            if not rows:
                self._notify("No hay datos para exportar", kind="warning")
                return

            # Apply formatters to raw data for friendly export
            export_data = []
            for row in rows:
                clean_row = {}
                for col in self.columns:
                    # Skip internal/action columns
                    if col.key.startswith("_") or not col.label.strip():
                        continue
                        
                    val = row.get(col.key)
                    if col.formatter:
                        try: val = col.formatter(val, row)
                        except: pass
                    
                    # Store as is, ExportService._format_value will handle the final string representation
                    clean_row[col.label] = val
                export_data.append(clean_row)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}"
            
            downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
            if not os.path.exists(downloads_path):
                downloads_path = os.getcwd()
            
            full_path = ""
            
            if fmt == "Excel":
                filename += ".xlsx"
                content = ExportService.export_to_excel(export_data)
                full_path = os.path.join(downloads_path, filename)
                with open(full_path, "wb") as f:
                    f.write(content)
            elif fmt == "PDF":
                filename += ".pdf"
                content = ExportService.export_to_pdf(export_data, title="Reporte de Datos")
                full_path = os.path.join(downloads_path, filename)
                with open(full_path, "wb") as f:
                    f.write(content)
            else: # CSV
                filename += ".csv"
                content = ExportService.export_to_csv(export_data)
                full_path = os.path.join(downloads_path, filename)
                with open(full_path, "w", encoding='utf-8-sig', newline='') as f:
                    f.write(content)

            self._notify(f"Exportado a Descargas: {filename}", kind="success")
            try:
                os.startfile(downloads_path)
            except:
                pass

        except Exception as exc:
            self._notify(f"Error exportando: {exc}", kind="error")

    def build(self) -> ft.Control:
        if self.auto_load and not self._loaded_once:
            self._refresh_data(update_ui=False)
            self._loaded_once = True

        row_controls = [
            self.search_field,
            ft.IconButton(icon=ft.Icons.CLEAR_ROUNDED, tooltip="Limpiar", on_click=lambda e: self._clear_search()),
            self.reset_button,
            self.refresh_button,
        ]
        if self.simple_filter_dropdown:
            row_controls.append(self.simple_filter_dropdown)
            
        if self.show_export_button:
            row_controls.append(self.export_button)
        controls = [
            ft.Row(row_controls, alignment=ft.MainAxisAlignment.SPACE_BETWEEN, vertical_alignment=ft.CrossAxisAlignment.CENTER),
        ]
        if self.advanced_expander:
            controls.append(self.advanced_expander)
        
        controls.append(self.selection_bar)

        info_row_controls: List[ft.Control] = [self.results_label, self.range_label]
        controls.append(ft.Row(info_row_controls, spacing=12))
        controls.append(ft.Row([self.sort_label, self.clear_sort_button], spacing=8))
        if self.show_mass_actions:
            controls.append(
                _expander(
                    label="Acciones masivas",
                    content=ft.Column(
                        [
                            ft.Row(
                                [self.mass_field_dropdown, self.mass_value_container, self.mass_edit_button],
                                spacing=10,
                            ),
                            ft.Row(
                                [self.selected_label, self.mass_delete_button],
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            ),
                        ],
                        spacing=10,
                    ),
                )
            )
        controls.extend(
            [
                self.status,
                self.table_container,
                ft.Row(
                    [
                        ft.Row(
                            [
                                self.first_button,
                                self.prev_button,
                                ft.Text("Página", size=12, color="#64748B"),
                                self.page_input,
                                self.pagination_label,
                                self.next_button,
                                self.last_button,
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                        ),
                        self.page_size_dropdown,
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
            ]
        )
        self.root = ft.Column(controls, expand=1, spacing=6)
        return self.root

    def refresh(self) -> None:
        if self._search_timer:
            self._search_timer.cancel()
        self.page = 1
        self._refresh_data()
        self._loaded_once = True

    def trigger_refresh(self) -> None:
        if self._search_timer:
            self._search_timer.cancel()
        self._search_timer = threading.Timer(0.4, self.refresh)
        self._search_timer.start()

    def _notify(self, message: str, kind: str = "info") -> None:
        if not self.root or getattr(self.root, "page", None) is None:
            return
        page = self.root.page
        snack = getattr(page, "snack_bar", None)
        if snack is None or not hasattr(snack, "open"):
            snack = self._snack
            page.snack_bar = snack
        content = getattr(snack, "content", None)
        if content is not None and hasattr(content, "value"):
            content.value = message
        else:
            try:
                snack.content = ft.Text(message)  # type: ignore[attr-defined]
            except Exception:
                pass
        if kind == "error":
            snack.bgcolor = "#FEE2E2"
            if hasattr(self, "_snack_text"):
                self._snack_text.color = "#991B1B"
        elif kind == "success":
            snack.bgcolor = "#DCFCE7"
            if hasattr(self, "_snack_text"):
                self._snack_text.color = "#166534"
        else:
            snack.bgcolor = "#E2E8F0"
            if hasattr(self, "_snack_text"):
                self._snack_text.color = "#111827"
        snack.open = True
        page.update()

    def _on_simple_filter_change(self) -> None:
        self.page = 1
        self._refresh_data()

    def _on_page_size_change(self, value: str) -> None:
        self.page_size = int(value)
        self.page = 1
        self._refresh_data()

    def _goto_page_from_input(self, value: str) -> None:
        try:
            target = int(value)
        except Exception:
            self.page_input.value = str(self.page)
            self.update()
            return
        self._goto_page(target)

    def _goto_page(self, target: int) -> None:
        if target < 1 or target > self.total_pages:
            return
        self.page = target
        self._refresh_data()

    def _clear_search(self) -> None:
        self.search_field.value = ""
        self.page = 1
        self._refresh_data()

    def _reset_filter_control(
        self,
        control: Any,
        setter: Optional[Callable[[ft.Control, Any], None]] = None,
        reset_value: Any = _FILTER_RESET_UNSET,
    ) -> None:
        has_reset_value = reset_value is not _FILTER_RESET_UNSET
        
        # 1. Use setter if provided
        if setter:
            try:
                setter(control, reset_value if has_reset_value else None)
            except:
                pass
            return

        # 2. Handle RangeSlider
        if isinstance(control, ft.RangeSlider):
            try:
                control.start_value = control.min
                control.end_value = control.max
            except:
                pass
            return

        if not hasattr(control, "value"):
            return

        # 3. Determine target reset value
        target_val = reset_value if has_reset_value else ""
        
        if not has_reset_value:
            if isinstance(control, ft.Checkbox):
                target_val = False
            elif isinstance(control, ft.Dropdown):
                options = getattr(control, "options", [])
                option_keys = [_dropdown_option_key(opt) for opt in options]
                if "" in option_keys:
                    target_val = ""
                elif option_keys:
                    target_val = option_keys[0]
                else:
                    target_val = None

        # 4. Apply value change
        if isinstance(control, ft.Dropdown):
            options = getattr(control, "options", [])
            option_keys = [_dropdown_option_key(opt) for opt in options]
            
            final_val = target_val
            if final_val is not None and not isinstance(final_val, str):
                final_val = str(final_val)
            
            if final_val not in option_keys:
                if "" in option_keys:
                    final_val = ""
                elif option_keys:
                    final_val = option_keys[0]
                else:
                    final_val = None

            control.value = final_val
        else:
            control.value = target_val

    def _reset_filters(self) -> None:
        if self._search_timer:
            self._search_timer.cancel()
        
        # 1. VISUAL RELOAD: Detach whole section to force Flet to "forget" stuck state
        if self.advanced_expander:
            self.advanced_expander.content = ft.Row([ft.ProgressRing(scale=0.5), ft.Text("Reiniciando filtros...", size=12)], alignment=ft.MainAxisAlignment.CENTER)
            try: self.advanced_expander.update()
            except: pass

        # 2. Reset internal values
        self.search_field.value = ""
        if self.simple_filter_dropdown:
            self.simple_filter_dropdown.value = self._simple_default_value

        for flt in self.advanced_filters:
            self._reset_filter_control(flt.control, flt.setter, flt.initial_value)
        
        # Clear other state
        self.sorts.clear()
        self._last_sort_idx = None
        self._sync_sort_indicator()
        self._update_sort_label()
        
        self.selected_ids.clear()
        self.select_all_global = False
        self.selection_bar.visible = False
        self._update_selected_label()
        
        self.page = 1
        self._set_status("")
        
        # 3. Restore section
        if self.advanced_expander:
            # Rebuild the row to be sure
            self.advanced_filters_row.controls = [flt.control for flt in self.advanced_filters]
            self.advanced_expander.content = self.advanced_filters_row
            try: self.advanced_expander.update()
            except: pass

        # 4. Final refresh
        self._refresh_data()
        
        if not self._last_error:
            self._notify("Filtros reiniciados", kind="success")

    def _toggle_sort(self, key: str, column_index: int) -> None:
        idx_to_cast = column_index
        try:
            if idx_to_cast is not None:
                idx_to_cast = int(idx_to_cast)
        except (ValueError, TypeError):
            idx_to_cast = None

        existing_index: Optional[int] = None
        for idx, (k, _) in enumerate(self.sorts):
            if k == key:
                existing_index = idx
                break

        if existing_index is None:
            self.sorts.append((key, "asc"))
        else:
            _, direction = self.sorts[existing_index]
            if direction == "asc":
                self.sorts.pop(existing_index)
                self.sorts.append((key, "desc"))
            else:
                self.sorts.pop(existing_index)

        self._last_sort_idx = idx_to_cast if self.sorts else None
        self._sync_sort_indicator()
        self._update_sort_label()
        self.page = 1
        self._refresh_data()

    def _sync_sort_indicator(self) -> None:
        if not self.table:
            return
        if not self.sorts:
            try:
                # Use None for clearing
                self.table.sort_column_index = None  # type: ignore
                self._safe_table_update()
            except Exception:
                pass
            return
        last_key, last_dir = self.sorts[-1]
        last_index = self._column_index_for_key(last_key)
        try:
            safe_index: Optional[int] = None
            if last_index is not None:
                try:
                    safe_index = int(last_index)
                except:
                    safe_index = None
            
            self.table.sort_column_index = safe_index  # type: ignore
            self.table.sort_ascending = last_dir != "desc"  # type: ignore
            self._safe_table_update()
        except Exception:
            pass

    def _column_index_for_key(self, key: str) -> Optional[int]:
        sort_offset = 1 if self.show_selection else 0
        for idx, col in enumerate(self.columns):
            if col.key == key:
                return idx + sort_offset
        return None

    def _clear_sorts(self) -> None:
        self._set_status("Reiniciando orden...", kind="info")
        self.sorts.clear()
        self._last_sort_idx = None
        
        if self.table:
            try:
                self.table.sort_column_index = None # type: ignore
                self.table.sort_ascending = True # type: ignore
                self._safe_table_update()
            except Exception:
                pass

        self._update_sort_label()
        self.page = 1
        self._refresh_data()
        self._notify("Orden reiniciado", kind="success")

    def _update_sort_label(self) -> None:
        labels = {col.key: col.label for col in self.columns}
        if not self.sorts:
            self.sort_label.value = "Orden: —"
            self.clear_sort_button.visible = False
            self.update()
            return
        parts = []
        for idx, (key, direction) in enumerate(self.sorts, 1):
            arrow = "↑" if direction == "asc" else "↓"
            label = labels.get(key, key)
            parts.append(f"({idx}) {label} {arrow}")
        self.sort_label.value = "Orden: " + ", ".join(parts)
        self.clear_sort_button.visible = True
        self.update()

    def _set_loading(self, value: bool) -> None:
        self._is_loading = value
        self._loading_overlay.visible = value
        if value:
            self._table_viewport.visible = False
            self._empty_overlay.visible = False

    def _set_status(self, message: str = "", kind: str = "info") -> None:
        self.status.value = message
        if not message:
            self.status.color = "#64748B"
            return
        if kind == "error":
            self.status.color = "#B91C1C"
        elif kind == "success":
            self.status.color = "#166534"
        else:
            self.status.color = "#64748B"

    def _refresh_data(self, update_ui: bool = True) -> None:
        search = self.search_field.value.strip() if self.search_field.value else None
        simple_value = (self.simple_filter_dropdown.value if self.simple_filter_dropdown else None)
        if simple_value == _ALL_VALUE:
            simple_value = None
        if isinstance(simple_value, str) and not simple_value.strip():
            simple_value = None
        advanced_payload = {
            flt.name: flt.getter(flt.control) for flt in self.advanced_filters
        }
        offset = (self.page - 1) * self.page_size
        self._last_error = None
        self._set_loading(True)
        if update_ui:
            self.update()
        
        # Hide selection bar on refresh if no longer needed
        if not self.select_all_global:
            self.selection_bar.visible = False

        try:
            rows, total = self.data_provider(
                offset,
                self.page_size,
                search,
                simple_value,
                advanced_payload,
                list(self.sorts),
            )
        except Exception as exc:
            self._last_error = str(exc)
            self._set_status(f"Error: {exc}", kind="error")
            self._notify(f"Error cargando datos: {exc}", kind="error")
            rows, total = [], 0
        self.current_rows = rows
        self.total_rows = total
        self.total_pages = max(1, ceil(total / self.page_size)) if total else 1
        if self.page > self.total_pages:
            self.page = self.total_pages
            offset = (self.page - 1) * self.page_size
            try:
                rows, total = self.data_provider(
                    offset,
                    self.page_size,
                    search,
                    simple_value,
                    advanced_payload,
                    list(self.sorts),
                )
            except Exception as exc:
                self._last_error = str(exc)
                self._set_status(f"Error: {exc}", kind="error")
                rows, total = [], 0
            self.current_rows = rows
            self.total_rows = total
        self._current_page_ids = [row.get(self.id_field) for row in rows if row.get(self.id_field) is not None]
        # self.selected_ids &= set(self._current_page_ids)  # Persist selection across pages
        self._update_selected_label()
        self.current_rows_by_id = {
            row.get(self.id_field): row for row in rows if row.get(self.id_field) is not None
        }
        try:
            new_rows = self._build_rows(rows)
            self.table.rows.clear()
            self.table.rows.extend(new_rows)
            self._safe_table_update()
        except Exception:
            self.table.rows = self._build_rows(rows)
            self._safe_table_update()
        self.page_input.value = str(self.page)
        self.pagination_label.value = f"de {self.total_pages}"
        start = offset + 1 if total else 0
        end = offset + len(rows) if total else 0
        self.range_label.value = f"Mostrando {start}-{end} de {total}"
        self.first_button.disabled = self.page <= 1
        self.prev_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages
        self.last_button.disabled = self.page >= self.total_pages
        if self._last_error:
            self.results_label.value = "Sin datos"
            self._empty_title.value = "No se pudo cargar"
            self._empty_message.value = "Revisá la conexión a PostgreSQL y reintentá."
            self._empty_overlay.visible = True
            self._table_viewport.visible = False
        else:
            self.results_label.value = f"{total} resultados"
            self._empty_title.value = "Sin resultados"
            self._empty_message.value = "Ajustá el buscador o filtros."
            self._empty_overlay.visible = total == 0
            self._table_viewport.visible = total > 0
            if total and self.status.value.startswith("Error:"):
                self._set_status("")
        self._sync_select_all_checkbox()
        self._set_loading(False)
        if update_ui:
            self.update()

    def update(self) -> None:
        if not self.root:
            return
        try:
            if self.root.page:
                self.root.page.update()
            else:
                self.root.update()
        except Exception:
            pass

    def _safe_table_update(self) -> None:
        """Defensive update for the DataTable to prevent TypeErrors in Python 3.14"""
        if not self.table:
            return
        try:
            # Ensure sort_column_index is NOT a string before updating
            if hasattr(self.table, "sort_column_index"):
                val = self.table.sort_column_index
                if isinstance(val, str):
                    try:
                        self.table.sort_column_index = int(val)
                    except:
                        self.table.sort_column_index = None
                
            self.table.update()
        except Exception:
            # If it still fails, try one last time with a nuked sort index
            try:
                self.table.sort_column_index = None
                self.table.update()
            except:
                pass

    def _build_rows(self, rows: List[Dict[str, Any]]) -> List[ft.DataRow]:
        result: List[ft.DataRow] = []
        self._row_selection_controls = {}
        for row in rows:
            row_id = row.get(self.id_field)
            if row_id is None:
                continue
            row_cells: List[ft.DataCell] = []
            if self.show_selection:
                cb = ft.Checkbox(
                    value=row_id in self.selected_ids,
                    on_change=lambda e, rid=row_id: self._toggle_selection(rid, bool(e.control.value)),
                )
                self._row_selection_controls[row_id] = cb
                row_cells.append(
                    ft.DataCell(
                        cb
                    )
                )
            for col in self.columns:
                content = self._render_cell(row_id, row, col)
                # Enable edit logic if column is editable and we have a callback
                can_edit = col.editable and self.show_inline_controls and (self.inline_edit_callback is not None)
                
                on_tap = None
                if can_edit:
                    on_tap = lambda e, r=row, c=col: self._open_inline_edit_dialog(r, c)
                
                row_cells.append(ft.DataCell(content, on_tap=on_tap, show_edit_icon=False))
            result.append(ft.DataRow(cells=row_cells))
        return result

    def _render_cell(self, row_id: Any, row: Dict[str, Any], col: ColumnConfig) -> ft.Control:
        if col.renderer:
            return col.renderer(row)
        value = row.get(col.key)
        text = col.formatter(value, row) if col.formatter else ("" if value is None else str(value))
        return ft.Text(
            text,
            size=12,
            overflow=ft.TextOverflow.ELLIPSIS,
        )

    def _toggle_selection(self, row_id: Any, value: bool) -> None:
        if value:
            self.selected_ids.add(row_id)
        else:
            self.selected_ids.discard(row_id)
            # If we uncheck anything, we are no longer in "global" mode
            if self.select_all_global:
                self.select_all_global = False
                self.selection_bar.visible = False

        self._update_selected_label()
        self._set_status("")
        self._sync_select_all_checkbox()
        self.update()

    def _update_selected_label(self) -> None:
        count = len(self.selected_ids)
        self.selected_label.value = f"{count} seleccionado(s)"
        has_targets = count > 0
        can_edit = (self.mass_edit_callback is not None) or (self.inline_edit_callback is not None)
        ready = bool(self._mass_field_key) and (self._mass_value is not None)
        if hasattr(self.mass_edit_button, "disabled"):
            self.mass_edit_button.disabled = not (has_targets and can_edit and ready)
        if hasattr(self.mass_delete_button, "disabled"):
            self.mass_delete_button.disabled = not (has_targets and self.mass_delete_callback is not None)

    def _sync_select_all_checkbox(self) -> None:
        if not self.show_selection or self.select_all_checkbox is None:
            return
        if not self._current_page_ids:
            self.select_all_checkbox.value = False
            return
        self.select_all_checkbox.value = all(rid in self.selected_ids for rid in self._current_page_ids)
        try:
            self.select_all_checkbox.update()
        except Exception:
            pass

    def _toggle_select_all(self, checked: bool) -> None:
        if not self._current_page_ids:
            return
        if checked:
            self.selected_ids.update(self._current_page_ids)
            # Show the global selection bar if we have more results than this page
            if (self.total_rows > self.page_size) and not self.select_all_global:
                self.selection_bar_text.value = f"Has seleccionado los {len(self._current_page_ids)} elementos de esta página."
                self.selection_bar_btn.text = f"Seleccionar los {self.total_rows} resultados"
                self.selection_bar_btn.on_click = lambda _: self._toggle_global_selection(True)
                self.selection_bar.visible = True
        else:
            for rid in self._current_page_ids:
                self.selected_ids.discard(rid)
            self.select_all_global = False
            self.selection_bar.visible = False
            
        for rid, cb in self._row_selection_controls.items():
            cb.value = rid in self.selected_ids or self.select_all_global
        self._update_selected_label()
        self._set_status("")
        self._sync_select_all_checkbox()
        self.update()

    def _toggle_global_selection(self, value: bool) -> None:
        if value:
            self._set_status("Seleccionando todos los resultados...", kind="info")
            try:
                # Reuse data provider to fetch all IDs
                # Note: this might be slow for massive datasets, but for typical ERP views it's fine.
                search = self.search_field.value.strip() if self.search_field.value else None
                simple_value = self.simple_filter_dropdown.value if self.simple_filter_dropdown else None
                if simple_value == _ALL_VALUE: simple_value = None
                advanced_payload = {flt.name: flt.getter(flt.control) for flt in self.advanced_filters}
                
                rows, total = self.data_provider(0, self.total_rows, search, simple_value, advanced_payload, self.sorts)
                all_ids = [r.get(self.id_field) for r in rows if r.get(self.id_field) is not None]
                self.selected_ids.update(all_ids)
                
                self.select_all_global = True
                self.selection_bar_text.value = f"¡Todos los {self.total_rows} resultados están seleccionados!"
                self.selection_bar_btn.text = "Deshacer selección total"
                self.selection_bar_btn.on_click = lambda _: self._toggle_global_selection(False)
                self._set_status("")
            except Exception as e:
                self._set_status(f"Error al seleccionar todo: {e}", kind="error")
                self.select_all_global = False
                self.selection_bar.visible = False
        else:
            self.selected_ids.clear()
            self.select_all_global = False
            self.selection_bar.visible = False
            self._set_status("Selección reiniciada", kind="info")
        
        self._sync_select_all_checkbox()
        for rid, cb in self._row_selection_controls.items():
            cb.value = rid in self.selected_ids
            
        self._update_selected_label()
        self.update()

    def _rebuild_from_current(self) -> None:
        # Not used anymore as we don't have inline mode
        pass

    def _on_mass_field_change(self, key: Any) -> None:
        self._mass_field_key = str(key).strip() if isinstance(key, str) and key.strip() else None
        self._mass_value = None
        if not self._mass_field_key:
            self.mass_value_container.content = ft.Text("Selecciona un campo", size=12, color="#64748B")
            self._update_selected_label()
            self.update()
            return
        self.mass_value_container.content = self._build_mass_value_control(self._mass_field_key)
        self._update_selected_label()
        self.update()

    def _build_mass_value_control(self, key: str) -> ft.Control:
        col = next((c for c in self.columns if c.key == key), None)
        if col is None:
            return ft.Text("Campo inválido", size=12, color="#B91C1C")

        def setter(val: Any) -> None:
            self._mass_value = val
            self._update_selected_label()
            self.update()

        if key.lower() == "activo":
            dd = ft.Dropdown(
                label="Valor",
                width=220,
                options=[
                    ft.dropdown.Option("true", "Activar"),
                    ft.dropdown.Option("false", "Desactivar"),
                ],
                on_change=lambda e: (
                    setter(True)
                    if e.control.value == "true"
                    else setter(False)
                    if e.control.value == "false"
                    else setter(None)
                ),
            )
            _style_input(dd)
            return dd

        if col.inline_editor is not None:
            try:
                return col.inline_editor(None, {}, setter)
            except Exception:
                pass

        control = ft.TextField(
            label="Valor",
            hint_text="Escribe un valor…",
            on_change=lambda e: setter(e.control.value),
        )
        _style_input(control)
        return control

    def _apply_mass_edit(self) -> None:
        if not self._mass_field_key:
            self._set_status("Selecciona un campo para editar", kind="error")
            self.update()
            return
        if self._mass_value is None:
            self._set_status("Define un valor para aplicar", kind="error")
            self.update()
            return
        if not self.mass_edit_callback:
            self._set_status("No hay callback para edición masiva", kind="error")
            self._notify("No hay callback para edición masiva", kind="error")
            self.update()
            return
        targets = [rid for rid in self.selected_ids]
        if not targets:
            self._set_status("Selecciona filas para aplicar edición", kind="error")
            self.update()
            return
        updates = {self._mass_field_key: self._mass_value}
        try:
            self.mass_edit_callback(targets, updates)
        except Exception as exc:
            self._set_status(f"Error: {exc}", kind="error")
            self._notify(f"Error aplicando edición masiva: {exc}", kind="error")
            self.update()
            return
        self._set_status("Edición masiva aplicada", kind="success")
        self._notify("Edición masiva aplicada", kind="success")
        self._refresh_data()

    def _confirm_mass_delete(self) -> None:
        targets = [rid for rid in self.selected_ids]
        if not targets:
            self._set_status("Selecciona filas para eliminar", kind="error")
            self.update()
            return

        def do_delete(_: Any) -> None:
            self._confirm_dialog.open = False
            self.update()
            self._mass_delete()

        self._confirm_dialog.title = ft.Text("Confirmar eliminación")
        self._confirm_dialog.content = ft.Text(
            f"¿Estás seguro que deseas eliminar {len(targets)} registro(s)? Esta acción no se puede deshacer."
        )
        self._confirm_dialog.actions = [
            ft.TextButton("Cancelar", on_click=lambda e: self._close_dialog()),
            ft.ElevatedButton("Eliminar", bgcolor="#DC2626", color="#FFFFFF", on_click=do_delete),
        ]
        self._open_dialog()

    def _open_dialog(self) -> None:
        if not self.root or getattr(self.root, "page", None) is None:
            return
        
        if hasattr(self.root.page, "open"):
            self.root.page.open(self._confirm_dialog)
        else:
            self._confirm_dialog.open = True
            self.root.page.dialog = self._confirm_dialog
            self.root.page.update()

    def _close_dialog(self) -> None:
        if not self.root or getattr(self.root, "page", None) is None:
            return
            
        if hasattr(self.root.page, "close"):
            self.root.page.close(self._confirm_dialog)
        else:
            self._confirm_dialog.open = False
            self.root.page.update()

    def _mass_delete(self) -> None:
        targets = [rid for rid in self.selected_ids]
        if not targets:
            self._set_status("Selecciona filas para eliminar", kind="error")
            self.update()
            return
        if not self.mass_delete_callback:
            self._set_status("No hay callback para eliminar", kind="error")
            self._notify("No hay callback para eliminar", kind="error")
            self.update()
            return
        if self.mass_delete_callback:
            try:
                self.mass_delete_callback(targets)
            except Exception as exc:
                self._set_status(f"Error: {exc}", kind="error")
                self._notify(f"Error eliminando: {exc}", kind="error")
                self.update()
                return
        self.selected_ids.clear()
        self._update_selected_label()
        self._set_status("Eliminación masiva procesada", kind="success")
        self._notify("Registros eliminados", kind="success")
        self._refresh_data()

        self._refresh_data()

    def _open_inline_edit_dialog(self, row: Dict[str, Any], col: ColumnConfig) -> None:
        """Opens a simple dialog to edit a single cell value."""
        row_id = row.get(self.id_field)
        if not row_id: return

        current_val = row.get(col.key)
        
        # Determine input type
        # Ideally we check col type, or based on current val
        input_control: ft.Control
        
        def save(e):
            new_val = input_control.value
            if col.key == "activa" or col.key == "activo" or col.key == "afecta_stock" or col.key == "afecta_cuenta_corriente":
                 # Convert specific known boolean columns
                 # (Hack: usually we should rely on Col Config type, but simple check works for now)
                 pass # Dropdown handles boolean value conversion usually
            
            # Simple bool check from dropdown
            if isinstance(input_control, ft.Dropdown):
                if new_val == "true": new_val = True
                elif new_val == "false": new_val = False
                elif new_val == "": new_val = None
            
            # Call update
            try:
                if self.inline_edit_callback:
                    self.inline_edit_callback(row_id, {col.key: new_val})
                    self._notify("Actualizado", kind="success")
                    self._edit_dialog.open = False
                    self.update()
                    self._refresh_data()
            except Exception as ex:
                self._notify(f"Error: {ex}", kind="error")

        def close(e):
            if self.root and self.root.page and hasattr(self.root.page, "close"):
                self.root.page.close(self._edit_dialog)
            else:
                self._edit_dialog.open = False
                self.update()

        # Build Input
        if isinstance(current_val, bool) or col.key in ["activa", "activo", "afecta_stock", "afecta_cuenta_corriente", "redondeo"]:
             input_control = ft.Dropdown(
                label=col.label,
                value=str(current_val).lower(),
                options=[
                    ft.dropdown.Option("true", "Sí"),
                    ft.dropdown.Option("false", "No")
                ],
                filled=True,
                bgcolor="#F8FAFC",
                border_color="#475569",
                border_radius=12
            )
        else:
            input_control = ft.TextField(
                label=col.label,
                value=str(current_val) if current_val is not None else "",
                autofocus=True,
                on_submit=save,
                filled=True,
                bgcolor="#F8FAFC",
                border_color="#475569",
                border_radius=12
            )

        self._edit_dialog.title = ft.Text(f"Editar {col.label}")
        self._edit_dialog.content = ft.Column([
            ft.Text(f"Valor actual: {current_val}"),
            input_control
        ], tight=True, width=300)
        
        self._edit_dialog.actions = [
            ft.TextButton("Cancelar", on_click=close),
            ft.ElevatedButton("Guardar", on_click=save, bgcolor="#4F46E5", color="white")
        ]
        
        if self.root and self.root.page:
            if hasattr(self.root.page, "open"):
                self.root.page.open(self._edit_dialog)
            else:
                self.root.page.dialog = self._edit_dialog
                self._edit_dialog.open = True
                self.root.page.update()
