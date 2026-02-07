from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional

import flet as ft
from desktop_app.database import Database
from desktop_app.components.async_select import AsyncSelect
from desktop_app.components.button_styles import cancel_button
from desktop_app.services.number_locale import format_currency, format_percent


# Styling helpers
def _maybe_set(obj: Any, name: str, value: Any) -> None:
    if hasattr(obj, name):
        try:
            setattr(obj, name, value)
        except Exception:
            pass


def _style_input(control: Any) -> None:
    _maybe_set(control, "border_color", "#475569")
    _maybe_set(control, "focused_border_color", "#4F46E5")
    _maybe_set(control, "border_radius", 4)
    _maybe_set(control, "text_size", 14)
    _maybe_set(control, "label_style", ft.TextStyle(color="#1E293B", size=13, weight=ft.FontWeight.BOLD))
    _maybe_set(control, "content_padding", ft.padding.symmetric(horizontal=12))

    name = getattr(control, "__class__", type("x", (), {})).__name__.lower()
    if "dropdown" in name:
        _maybe_set(control, "bgcolor", "#F8FAFC")
        _maybe_set(control, "filled", True)
        _maybe_set(control, "border_width", 2)
        _maybe_set(control, "enable_search", True)
        _maybe_set(control, "height", 50)
    else:
        _maybe_set(control, "filled", True)
        _maybe_set(control, "bgcolor", "#F8FAFC")
        _maybe_set(control, "border_width", 1)
        if "textfield" in name:
            _maybe_set(control, "height", 50)
            _maybe_set(control, "cursor_color", "#4F46E5")


def _dropdown(label: str, width: int = 200) -> ft.Dropdown:
    dd = ft.Dropdown(label=label, width=width)
    _style_input(dd)
    return dd


class SafeDataTable(ft.DataTable):
    """Subclass of DataTable to fix TypeErrors and AssertionErrors in Flet updates"""
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

class MassUpdateView(ft.Container):
    def __init__(self, db: Database, on_show_toast: Callable[[str, str], None], supplier_loader: Optional[Callable] = None, price_list_loader: Optional[Callable] = None):
        super().__init__(expand=True)
        self.db = db
        self.show_toast = on_show_toast
        self.supplier_loader = supplier_loader
        self.price_list_loader = price_list_loader

        # State
        self.filters: Dict[str, Any] = {}
        self.preview_data: List[Dict[str, Any]] = []
        self.selected_ids: set[int] = set()
        self._loading: bool = False
        
        # Lazy loading state
        self._batch_size: int = 50
        self._current_index: int = 0
        self._all_preview_data: List[Dict[str, Any]] = []
        self._is_loading_batch: bool = False
        self._preview_mode: str = "COSTO"
        self._preview_active_lists: List[Dict[str, Any]] = []
        self._preview_selected_list_label: str = "Lista Seleccionada"
        self._preview_skipped_invalid_factor: int = 0

        # Loader
        self.loader = ft.ProgressBar(width=None, color="#6366F1", visible=False)

        # UI Components - Filters
        self.filter_nombre = ft.TextField(
            label="Nombre contiene",
            width=220,
            on_submit=self._update_count,
        )
        _style_input(self.filter_nombre)

        # Ojo: esto dispara MUCHAS queries mientras tipeás. Lo dejo igual porque ya lo tenías.
        self.filter_nombre.on_change = lambda e: self._update_count(None)

        self.filter_marca = _dropdown("Marca")
        self.filter_rubro = _dropdown("Rubro")
        self.filter_proveedor = AsyncSelect(label="Proveedor", loader=self.supplier_loader, width=250, on_change=lambda _: self._update_count(None))
        self.filter_lista = AsyncSelect(label="En Lista Precios", loader=self.price_list_loader, width=300, on_change=lambda _: self._update_count(None))
        self.filter_iva = _dropdown("Alicuota IVA")
        self.filter_activo = _dropdown("Estado")
        self.filter_activo.options = [
            ft.dropdown.Option("", "Todos"),
            ft.dropdown.Option("True", "Activos"),
            ft.dropdown.Option("False", "Inactivos"),
        ]

        # Action Components
        self.target_selector = AsyncSelect(
            label="Actualizar Precio en",
            loader=self._target_loader,
            width=300,
            value="COSTO",
            initial_items=[{"value": "COSTO", "label": "COSTO BASE"}],
            on_change=self._on_target_change,
        )

        self.op_selector = _dropdown("Tipo de Ajuste", width=250)
        self.op_selector.options = [
            ft.dropdown.Option("PCT_ADD", "Aumentar Porcentaje (%)"),
            ft.dropdown.Option("PCT_SUB", "Descontar Porcentaje (%)"),
            ft.dropdown.Option("AMT_ADD", "Aumentar Monto Fijo ($)"),
            ft.dropdown.Option("AMT_SUB", "Descontar Monto Fijo ($)"),
            ft.dropdown.Option("SET_VAL", "Fijar Valor Exacto ($)"),
        ]
        self.op_selector.value = "PCT_ADD"

        self.value_input = ft.TextField(label="Valor", width=150, value="0")
        _style_input(self.value_input)
        self.value_input.keyboard_type = ft.KeyboardType.NUMBER

        # Warning Text
        self.warning_text = ft.Text(
            "⚠️ Modificar el Costo recalculará automáticamente todas las listas desde el costo.",
            color="#EA580C",
            size=12,
            visible=True,
        )

        # Count Label
        self.count_label = ft.Text("0 artículos seleccionados", weight=ft.FontWeight.BOLD, color="#64748B")

        # Preview Table
        self.preview_table = SafeDataTable(
            columns=[],
            width=None,
            heading_row_color="#F1F5F9",
            data_row_color={"hovered": "#F8FAFC"},
            bgcolor="#FFFFFF",
        )
        self._set_preview_columns()

        # Buttons
        self.btn_preview = ft.ElevatedButton(
            "Generar Vista Previa",
            icon=ft.icons.PREVIEW_ROUNDED,
            on_click=self._run_preview,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=4),
                padding=20,
            ),
        )

        self.btn_apply = ft.ElevatedButton(
            "APLICAR CAMBIOS SELECCIONADOS",
            icon=ft.icons.SAVE_ROUNDED,
            bgcolor="#4F46E5",
            color="#FFFFFF",
            on_click=self._confirm_apply,
            disabled=True,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=4),
                padding=20,
            ),
        )
        
        self.btn_load_more = ft.TextButton(
            "Cargar más resultados...",
            icon=ft.icons.DOWNLOAD_ROUNDED,
            on_click=lambda _: self._load_next_batch(),
            visible=False,
        )

        self.preview_table_scroll = ft.Row(
            [self.preview_table],
            scroll=ft.ScrollMode.AUTO,
        )

        # Preview Results Container (replaces ListView to avoid ghost space)
        self.scroll_container = ft.Column(
            [self.preview_table_scroll, self.btn_load_more],
            tight=True,
            visible=False,
        )

        self.preview_empty_title = ft.Text("Sin vista previa", weight=ft.FontWeight.BOLD, color="#1E293B")
        self.preview_empty_message = ft.Text("Genera la vista previa para ver articulos.", size=12, color="#64748B")
        self.preview_empty = ft.Container(
            visible=True,
            expand=False,
            alignment=ft.Alignment(0, 0),
            padding=40,
            content=ft.Column(
                [
                    self.preview_empty_title,
                    self.preview_empty_message,
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=6,
                tight=True,
            ),
        )
        
        # Preview Section Container (created here to allow dynamic expand control)
        self.preview_section_container = ft.Container(
            padding=20,
            border=ft.border.all(1, "#E2E8F0"),
            border_radius=8,
            bgcolor="#FFFFFF",
            expand=False,  # Start collapsed, expand only when preview is shown
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Text("3. Vista Previa y Selección", weight=ft.FontWeight.W_600, color="#1E293B", size=16),
                            ft.Text("Seleccione los ítems a actualizar (carga progresiva)", size=12, color="grey"),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    self.scroll_container,
                    self.preview_empty,
                    ft.Row([self.btn_apply], alignment=ft.MainAxisAlignment.END),
                ],
                spacing=15,
                expand=False,  # Start collapsed
                tight=True,    # Force tight layout to avoid gray space
            ),
        )

        # Layout
        self.content = ft.Column(
            [
                ft.Row(
                    [
                        ft.Text(
                            "Actualización Masiva de Precios",
                            size=24,
                            weight=ft.FontWeight.BOLD,
                            color="#1E293B",
                        ),
                        self.loader,
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Text(
                    "Herramienta avanzada para administradores. Use con precaución.",
                    size=14,
                    color="#64748B",
                ),
                ft.Divider(),
                # Filters Section
                ft.Container(
                    padding=20,
                    border=ft.border.all(1, "#E2E8F0"),
                    border_radius=8,
                    bgcolor="#FFFFFF",
                    content=ft.Column(
                        [
                            ft.Row(
                                [
                                    ft.Row([
                                        ft.Text("1. Filtrar Artículos", weight=ft.FontWeight.W_600, color="#1E293B", size=16),
                                        ft.IconButton(
                                            icon=ft.icons.FILTER_ALT_OFF_ROUNDED,
                                            icon_color="#64748B",
                                            icon_size=20,
                                            tooltip="Resetear filtros",
                                            on_click=self._reset_filters,
                                        ),
                                    ], spacing=10),
                                    self.count_label,
                                ],
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            ),
                            ft.Row([self.filter_nombre, self.filter_marca, self.filter_rubro, self.filter_proveedor], wrap=True),
                            ft.Row([self.filter_lista, self.filter_iva, self.filter_activo], wrap=True),
                        ],
                        spacing=15,
                    ),
                ),
                # Actions Section
                ft.Container(
                    padding=20,
                    border=ft.border.all(1, "#E2E8F0"),
                    border_radius=8,
                    bgcolor="#FFFFFF",
                    content=ft.Column(
                        [
                            ft.Text("2. Configurar Ajuste", weight=ft.FontWeight.W_600, color="#1E293B", size=16),
                            ft.Row(
                                [self.target_selector, self.op_selector, self.value_input, self.btn_preview],
                                vertical_alignment=ft.CrossAxisAlignment.START,
                                wrap=True,
                            ),
                            self.warning_text,
                        ],
                        spacing=15,
                    ),
                ),
                # Preview Section
                self.preview_section_container,
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=20,
            expand=False,
            tight=True,
        )
        
        # Initial Load
        self.load_catalogs()
        
        # Trigger initial count update
        self._schedule_initial_count()

        # Bind events
        for ctrl in [
            self.filter_marca,
            self.filter_rubro,
            # self.filter_proveedor, # AsyncSelect has its own on_change
            # self.filter_lista, # AsyncSelect has its own on_change
            self.filter_iva,
            self.filter_activo,
        ]:
            ctrl.on_change = lambda e: self._update_count(None)

    # -----------------------------
    # Helpers
    # -----------------------------
    def _ui_update(self) -> None:
        try:
            if self.page:
                self.page.update()
            else:
                self.update()
        except Exception:
            pass
    
    def _schedule_initial_count(self) -> None:
        """Schedule an async update of the article count after the page is mounted."""
        async def _do_count():
            await asyncio.sleep(0.1)  # Let the UI mount first
            self._update_count(None)
        if self.page:
            self.page.run_task(_do_count)
        else:
            # Will be called again when mounted via did_mount if needed
            pass
    
    def did_mount(self):
        """Called when the component is mounted to the page."""
        super().did_mount()
        # Reset visual state on mount to avoid stale data
        self._clear_preview_state()
        # Trigger initial count now that page is available
        self._schedule_initial_count()
    
    def _clear_preview_state(self):
        """Reset all preview-related state."""
        self._preview_mode = "COSTO"
        self._preview_active_lists = []
        self._preview_selected_list_label = "Lista Seleccionada"
        self._preview_skipped_invalid_factor = 0
        self._set_preview_columns()
        self.preview_table.rows.clear()
        self.selected_ids.clear()
        self._all_preview_data = []
        self._current_index = 0
        self._is_loading_batch = False
        
        self.btn_apply.disabled = True
        self.btn_apply.text = "APLICAR CAMBIOS SELECCIONADOS"
        self.btn_load_more.visible = False

        try:
            self.preview_table.update()
            self.btn_apply.update()
            self.btn_load_more.update()
        except Exception:
            pass
        self._show_preview_placeholder("Sin vista previa", "Genera la vista previa para ver articulos.")

        # Try reset scroll
        try:
            if hasattr(self, 'scroll_container'):
                self.scroll_container.scroll_to(0, duration=0)
        except:
            pass

    def _show_preview_placeholder(self, title: str, message: str) -> None:
        self.preview_empty_title.value = title
        self.preview_empty_message.value = message
        self.preview_empty.visible = True
        self.scroll_container.visible = False
        
        # Ensure section is not expanded
        self.preview_section_container.expand = False
        if isinstance(self.preview_section_container.content, ft.Column):
            self.preview_section_container.content.expand = False
            self.preview_section_container.content.tight = True
        
        self.preview_empty.update()
        self.scroll_container.update()
        self.preview_section_container.update()

    def _show_preview_table(self) -> None:
        self.preview_empty.visible = False
        self.scroll_container.visible = True
        
        # Ensure section is not expanded to only take necessary space
        self.preview_section_container.expand = False
        if isinstance(self.preview_section_container.content, ft.Column):
            self.preview_section_container.content.expand = False
            self.preview_section_container.content.tight = True
        
        self.preview_empty.update()
        self.scroll_container.update()
        self.preview_section_container.update()

    def _format_variation_text(self, diff_pct: Any) -> str:
        try:
            pct = float(diff_pct or 0)
        except Exception:
            pct = 0.0
        formatted = format_percent(pct, decimals=2)
        return f"+{formatted}" if pct > 0 else formatted

    def _variation_color(self, diff_pct: Any) -> str:
        try:
            pct = float(diff_pct or 0)
        except Exception:
            pct = 0.0
        if pct > 0:
            return "#EA580C"
        if pct < 0:
            return "#0F766E"
        return "#64748B"

    def _set_preview_columns(self) -> None:
        select_label = "Sel"
        self.preview_table.columns = [
            ft.DataColumn(ft.Checkbox(label=select_label, on_change=self._toggle_all)),
            ft.DataColumn(ft.Text("ID")),
            ft.DataColumn(ft.Text("Artículo")),
            ft.DataColumn(ft.Text("Costo Actual")),
            ft.DataColumn(ft.Text("Costo Nuevo")),
            ft.DataColumn(ft.Text("Var. Costo")),
        ]

        if self._preview_mode == "COSTO":
            for lp in self._preview_active_lists:
                self.preview_table.columns.append(
                    ft.DataColumn(ft.Text(lp.get("nombre") or "Lista"))
                )
        else:
            label = self._preview_selected_list_label or "Lista Seleccionada"
            self.preview_table.columns.extend(
                [
                    ft.DataColumn(ft.Text(f"{label} Actual")),
                    ft.DataColumn(ft.Text(f"{label} Nuevo")),
                    ft.DataColumn(ft.Text(f"Var. {label}")),
                ]
            )
        try:
            self.preview_table.update()
        except Exception:
            pass
    
    
    def _load_next_batch(self) -> None:
        """Load the next batch of rows into the preview table."""
        if self._is_loading_batch:
            return
        if self._current_index >= len(self._all_preview_data):
            return
        
        self._is_loading_batch = True
        
        try:
            end_index = min(self._current_index + self._batch_size, len(self._all_preview_data))
            batch = self._all_preview_data[self._current_index:end_index]
            
            for r in batch:
                rid = int(r["id"])
                # Don't force add to selection here, respect current state
                is_selected = rid in self.selected_ids
                cells: List[ft.DataCell] = [
                    ft.DataCell(
                        ft.Checkbox(
                            value=is_selected,
                            on_change=lambda e, rid=rid: self._toggle_one(rid, e.control.value),
                        )
                    ),
                    ft.DataCell(ft.Text(str(rid) if rid is not None else "—")),
                    ft.DataCell(ft.Text(r.get("nombre", ""))),
                    ft.DataCell(ft.Text(format_currency(r.get("costo_current", 0.0)))),
                    ft.DataCell(
                        ft.Text(
                            format_currency(r.get("costo_new", 0.0)),
                            weight=ft.FontWeight.BOLD,
                            color="#166534",
                        )
                    ),
                    ft.DataCell(
                        ft.Text(
                            self._format_variation_text(r.get("costo_diff_pct", 0.0)),
                            color=self._variation_color(r.get("costo_diff_pct", 0.0)),
                        )
                    ),
                ]

                if self._preview_mode == "COSTO":
                    list_changes = r.get("list_changes", {}) or {}
                    for lp in self._preview_active_lists:
                        lp_id = int(lp.get("id"))
                        change = list_changes.get(lp_id)
                        if change is None:
                            change = list_changes.get(str(lp_id))
                        if not change:
                            cells.append(ft.DataCell(ft.Text("—", color="#64748B")))
                            continue
                        list_text = (
                            f"{format_currency(change.get('current', 0.0))} -> "
                            f"{format_currency(change.get('new', 0.0))} "
                            f"({self._format_variation_text(change.get('diff_pct', 0.0))})"
                        )
                        cells.append(
                            ft.DataCell(
                                ft.Text(
                                    list_text,
                                    color=self._variation_color(change.get("diff_pct", 0.0)),
                                )
                            )
                        )
                else:
                    cells.extend(
                        [
                            ft.DataCell(ft.Text(format_currency(r.get("selected_current", 0.0)))),
                            ft.DataCell(
                                ft.Text(
                                    format_currency(r.get("selected_new", 0.0)),
                                    weight=ft.FontWeight.BOLD,
                                    color="#166534",
                                )
                            ),
                            ft.DataCell(
                                ft.Text(
                                    self._format_variation_text(r.get("selected_diff_pct", 0.0)),
                                    color=self._variation_color(r.get("selected_diff_pct", 0.0)),
                                )
                            ),
                        ]
                    )

                self.preview_table.rows.append(ft.DataRow(cells=cells))
            
            self._current_index = end_index
            self.preview_table.update()
            
            # Update button text
            self.btn_apply.text = f"APLICAR A {len(self.selected_ids)} ARTÍCULOS"
            self.btn_apply.disabled = len(self.selected_ids) == 0
            self.btn_apply.update()
            
            # Show loading indicator if more to load
            remaining = len(self._all_preview_data) - self._current_index
            if remaining > 0:
                self.btn_load_more.visible = True
                self.btn_load_more.text = f"Cargar {min(self._batch_size, remaining)} más... ({remaining} restantes)"
                self.btn_load_more.update()
                # self.show_toast(f"Cargados {self._current_index} de {len(self._all_preview_data)} artículos...", "info")
            else:
                self.btn_load_more.visible = False
                self.btn_load_more.update()
                
        except Exception as e:
            print(f"Error loading batch: {e}")
        finally:
            self._is_loading_batch = False

    def _set_loading(self, loading: bool) -> None:
        self._loading = loading
        self.loader.visible = loading
        self.btn_preview.disabled = loading
        # btn_apply depende también de selected_ids
        self.btn_apply.disabled = loading or (len(self.selected_ids) == 0)
        self._ui_update()

    def _parse_target(self) -> tuple[str, Optional[int]]:
        target_raw = self.target_selector.value or "COSTO"
        if target_raw.startswith("LIST:"):
            return "LISTA_PRECIO", int(target_raw.split(":")[1])
        return "COSTO", None

    def _parse_value(self) -> float:
        try:
            return float(self.value_input.value or 0)
        except Exception:
            return 0.0

    def _reset_filters(self, _):
        self.filter_nombre.value = ""
        self.filter_marca.value = ""
        self.filter_rubro.value = ""
        self.filter_proveedor.value = None
        self.filter_lista.value = None
        self.filter_iva.value = ""
        self.filter_activo.value = ""

        # Update controls
        self.filter_nombre.update()
        self.filter_marca.update()
        self.filter_rubro.update()
        self.filter_iva.update()
        self.filter_activo.update()

        # Update count and clear preview
        self._update_count(None)

    # -----------------------------
    # Catalogs / Filters
    # -----------------------------
    async def _target_loader(self, query: str, offset: int, limit: int):
        items = []
        has_more = False
        
        # Virtual item "COSTO BASE"
        if offset == 0:
            if not query or any(x in query.lower() for x in ["costo", "base"]):
                items.append({"value": "COSTO", "label": "COSTO BASE"})
        
        if self.price_list_loader:
            try:
                res = self.price_list_loader(query, offset, limit)
                # Check if it's a coroutine (async)
                if asyncio.iscoroutine(res):
                    list_items, has_more = await res
                else:
                    # Sync loader
                    list_items, has_more = await asyncio.to_thread(self.price_list_loader, query, offset, limit)
                
                for r in list_items:
                    items.append({"value": f"LIST:{r['value']}", "label": f"Lista: {r['label']}"})
            except Exception as e:
                print(f"Error in _target_loader: {e}")
                
        return items, has_more

    def load_catalogs(self):
        try:
            marcas = self.db.list_marcas_full()
            self.filter_marca.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(m["id"]), m["nombre"]) for m in marcas
            ]

            rubros = self.db.list_rubros_full()
            self.filter_rubro.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(str(r["id"]), r["nombre"]) for r in rubros
            ]

            # provs = self.db.list_proveedores() # AsyncSelect handles it

            # Fetch IVA types
            # AsyncSelect handles price lists via loader
            if hasattr(self.target_selector, "clear_cache"):
                self.target_selector.clear_cache()
            
            ivas = self.db.fetch_tipos_iva()
            self.filter_iva.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(i["id"]), f"{i['descripcion']} ({i['porcentaje']}%)") for i in ivas
            ]

            self._ui_update()
        except Exception as e:
            print(f"Error loading catalogs: {e}")

    def _get_filters(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if self.filter_nombre.value:
            params["nombre"] = self.filter_nombre.value
        if self.filter_marca.value:
            params["id_marca"] = self.filter_marca.value
        if self.filter_rubro.value:
            params["id_rubro"] = self.filter_rubro.value
        if self.filter_proveedor.value:
            params["id_proveedor"] = self.filter_proveedor.value
        if self.filter_lista.value:
            params["id_lista_precio"] = self.filter_lista.value
        if self.filter_iva.value:
            params["id_tipo_iva"] = self.filter_iva.value

        act = self.filter_activo.value
        if act == "True":
            params["activo_only"] = True
        elif act == "False":
            params["activo_only"] = False

        return params

    def _update_count(self, _):
        # OJO: esto es sync y puede bloquear si la query tarda, pero lo dejo para no romper tu flujo.
        try:
            self.count_label.value = "Calculando..."
            self.count_label.update()

            count = self.db.count_articles(advanced=self._get_filters())
            self.count_label.value = f"{count} artículos coincidentes"
            self.count_label.update()

            # Always clear preview when filters change (invalidates current preview)
            self._clear_preview_state()

        except Exception as e:
            print(f"Error count: {e}")

    def _on_target_change(self, e):
        val = self.target_selector.value
        if val == "COSTO":
            self.warning_text.value = "⚠️ Modificar el Costo recalculará automáticamente todas las listas desde el costo."
            self.warning_text.color = "#EA580C"
        else:
            self.warning_text.value = "ℹ️ Modificar una lista derivará un nuevo costo y recalculará todas las listas."
            self.warning_text.color = "#3B82F6"
        self.warning_text.update()

    # -----------------------------
    # Preview
    # -----------------------------
    def _run_preview(self, _):
        if not self.page:
            self.show_toast("La vista todavía no está montada (no hay page).", "error")
            return
        if self._loading:
            return
        self.page.run_task(self._run_preview_async)

    async def _run_preview_async(self):
        try:
            target, list_id = self._parse_target()
            val = self._parse_value()

            self._set_loading(True)
            self.show_toast("Generando vista previa…", "info")

            preview_payload = await asyncio.to_thread(
                self.db.preview_mass_update,
                filters=self._get_filters(),
                target=target,
                operation=self.op_selector.value,
                value=val,
                list_id=list_id,
                limit=None,  # full list (ojo si son miles)
            )
            if not isinstance(preview_payload, dict):
                raise ValueError("Formato de vista previa inválido.")
            rows = preview_payload.get("rows", []) or []
            meta = preview_payload.get("meta", {}) or {}
            self._preview_mode = str(meta.get("target_mode") or target)
            self._preview_active_lists = meta.get("active_lists", []) or []
            selected_list = meta.get("selected_list") or {}
            self._preview_selected_list_label = str(selected_list.get("nombre") or "Lista Seleccionada")
            self._preview_skipped_invalid_factor = int(meta.get("skipped_invalid_factor") or 0)
            self._set_preview_columns()

            # Clear state
            self.preview_table.rows.clear()
            self.selected_ids.clear()
            self._all_preview_data = rows
            self._current_index = 0
            
            # Reset scroll to top
            try:
                self.scroll_container.scroll_to(0, duration=0)
            except:
                pass

            # Select all by default when generating new preview
            for r in rows:
                self.selected_ids.add(int(r["id"]))

            if not rows:
                self.preview_table.update()
                if self._preview_skipped_invalid_factor > 0:
                    self._show_preview_placeholder(
                        "Sin resultados válidos",
                        "Los artículos encontrados fueron omitidos por factor inválido (DESCUENTO >= 100%).",
                    )
                else:
                    self._show_preview_placeholder(
                        "Sin resultados",
                        "No se encontraron articulos con los filtros actuales.",
                    )
                self.btn_apply.text = "APLICAR CAMBIOS SELECCIONADOS"
                self.btn_apply.disabled = True
                self.btn_apply.update()
                self.btn_load_more.visible = False
                self.btn_load_more.update()
                if self._preview_skipped_invalid_factor > 0:
                    self.show_toast(
                        f"Se omitieron {self._preview_skipped_invalid_factor} artículos por factor inválido (DESCUENTO >= 100%).",
                        "warning",
                    )
                else:
                    self.show_toast("No se encontraron artículos con los filtros actuales.", "info")
            else:
                self._show_preview_table()
                # Load first batch only (lazy loading)
                self._load_next_batch()

                total = len(rows)
                loaded = min(self._batch_size, total)
                if total > self._batch_size:
                    self.show_toast(f"Vista previa lista: {total} artículos. Mostrando {loaded}, scrollea para cargar más.", "success")
                else:
                    self.show_toast(f"Vista previa lista: {total} artículos.", "success")
                if self._preview_skipped_invalid_factor > 0:
                    self.show_toast(
                        f"Se omitieron {self._preview_skipped_invalid_factor} artículos por factor inválido (DESCUENTO >= 100%).",
                        "warning",
                    )

            self._ui_update()

        except Exception as e:
            print(f"Error preview: {e}")
            self.show_toast(f"Error en vista previa: {e}", "error")
        finally:
            self._set_loading(False)

    # -----------------------------
    # Selection
    # -----------------------------
    def _toggle_all(self, e):
        checked = bool(e.control.value)
        self.selected_ids.clear()

        # Update visual checkboxes in loaded rows
        for row in self.preview_table.rows:
            # Checkbox visual
            cb = row.cells[0].content
            if hasattr(cb, "value"):
                cb.value = checked

        # If selecting all, include ALL items (even those not loaded yet)
        if checked:
            for r in self._all_preview_data:
                self.selected_ids.add(int(r["id"]))

        self.preview_table.update()
        self._update_apply_btn()

    def _toggle_one(self, art_id: int, checked: bool):
        if checked:
            self.selected_ids.add(art_id)
        else:
            self.selected_ids.discard(art_id)
        try:
            if self.preview_table.columns:
                header = self.preview_table.columns[0].label
                if isinstance(header, ft.Checkbox):
                    header.value = len(self._all_preview_data) > 0 and len(self.selected_ids) == len(self._all_preview_data)
                    header.update()
        except Exception:
            pass
        self._update_apply_btn()

    def _update_apply_btn(self):
        count = len(self.selected_ids)
        self.btn_apply.text = f"APLICAR A {count} ARTÍCULOS"
        self.btn_apply.disabled = self._loading or (count == 0)
        self.btn_apply.update()
        try:
            if self.preview_table.columns:
                header = self.preview_table.columns[0].label
                if isinstance(header, ft.Checkbox):
                    header.value = len(self._all_preview_data) > 0 and count == len(self._all_preview_data)
                    header.update()
        except Exception:
            pass

    # -----------------------------
    # Apply
    # -----------------------------
    def _confirm_apply(self, _):
        if not self.page or not self.selected_ids or self._loading:
            return

        count = len(self.selected_ids)

        dlg: Optional[ft.AlertDialog] = None

        def close_dlg(_e=None):
            if dlg is None:
                return
            dlg.open = False
            self.page.update()

        def do_apply(_e=None):
            close_dlg()
            self._apply_changes()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar Actualización Masiva"),
            content=ft.Text(
                f"¿Estás seguro de que quieres aplicar estos cambios a {count} ítems seleccionados?\n"
                f"Esta acción no se puede deshacer."
            ),
            actions=[
                cancel_button("Cancelar", on_click=close_dlg),
                ft.ElevatedButton("Confirmar y Aplicar", on_click=do_apply, bgcolor="#16A34A", color="white", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=4))),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )

        # Forma recomendada de abrir dialog en Flet:
        self.page.open(dlg)

    def _apply_changes(self):
        if not self.page or self._loading or not self.selected_ids:
            return
        self.page.run_task(self._apply_changes_async)

    async def _apply_changes_async(self):
        # Snapshot de IDs para que no cambien mientras aplicamos
        ids_snapshot = list(self.selected_ids)

        try:
            target, list_id = self._parse_target()
            val = self._parse_value()

            self._set_loading(True)
            self.show_toast(f"Aplicando cambios a {len(ids_snapshot)} artículos…", "info")

            result = await asyncio.to_thread(
                self.db.mass_update_articles,
                filters={},  # ignorado si ids viene cargado
                target=target,
                operation=self.op_selector.value,
                value=val,
                list_id=list_id,
                ids=ids_snapshot,
            )
            if isinstance(result, dict):
                affected = int(result.get("updated_count") or 0)
                skipped_invalid = int(result.get("skipped_invalid_factor") or 0)
            else:
                affected = int(result or 0)
                skipped_invalid = 0

            # UI cleanup
            self.preview_table.rows.clear()
            self.preview_table.update()

            self.selected_ids.clear()
            self.btn_apply.disabled = True
            self.btn_apply.text = "APLICAR CAMBIOS SELECCIONADOS"
            self.btn_apply.update()
            self._show_preview_placeholder("Sin vista previa", "Genera la vista previa para ver articulos.")

            if affected > 0 and skipped_invalid > 0:
                self.show_toast(
                    f"Actualización completada. {affected} artículos modificados. Omitidos por factor inválido: {skipped_invalid}.",
                    "success",
                )
            elif affected > 0:
                self.show_toast(f"Actualización completada. {affected} artículos modificados.", "success")
            elif skipped_invalid > 0:
                self.show_toast(
                    f"No se modificaron artículos. Omitidos por factor inválido: {skipped_invalid}.",
                    "warning",
                )
            else:
                self.show_toast("No se modificaron artículos.", "info")

            # refrescar count sin bloquear UI
            try:
                self.count_label.value = "Calculando..."
                self.count_label.update()
                count = await asyncio.to_thread(self.db.count_articles, advanced=self._get_filters())
                self.count_label.value = f"{count} artículos coincidentes"
                self.count_label.update()
            except Exception as e:
                print(f"Error refresh count: {e}")

            self._ui_update()

        except Exception as e:
            print(f"Error applying: {e}")
            self.show_toast(f"Error aplicando cambios: {e}", "error")
        finally:
            self._set_loading(False)
