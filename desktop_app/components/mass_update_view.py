from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional

import flet as ft
from desktop_app.database import Database


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
    _maybe_set(control, "content_padding", ft.padding.all(12))

    name = getattr(control, "__class__", type("x", (), {})).__name__.lower()
    if "dropdown" in name:
        _maybe_set(control, "bgcolor", "#F8FAFC")
        _maybe_set(control, "filled", True)
        _maybe_set(control, "border_width", 2)
        _maybe_set(control, "enable_search", True)
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
    def __init__(self, db: Database, on_show_toast: Callable[[str, str], None]):
        super().__init__(expand=True)
        self.db = db
        self.show_toast = on_show_toast

        # State
        self.filters: Dict[str, Any] = {}
        self.preview_data: List[Dict[str, Any]] = []
        self.selected_ids: set[int] = set()
        self._loading: bool = False

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
        self.filter_proveedor = _dropdown("Proveedor")

        self.filter_lista = _dropdown("En Lista Precios")
        self.filter_iva = _dropdown("Alicuota IVA")
        self.filter_activo = _dropdown("Estado")
        self.filter_activo.options = [
            ft.dropdown.Option("", "Todos"),
            ft.dropdown.Option("True", "Activos"),
            ft.dropdown.Option("False", "Inactivos"),
        ]

        # Action Components
        self.target_selector = _dropdown("Actualizar Precio en", width=250)
        self.target_selector.options = [
            ft.dropdown.Option("COSTO", "COSTO BASE"),
        ]
        self.target_selector.value = "COSTO"
        self.target_selector.on_change = self._on_target_change

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
            "⚠️ Modificar el Costo recalculará automáticamente los precios basados en Margen.",
            color="#EA580C",
            size=12,
            visible=True,
        )

        # Count Label
        self.count_label = ft.Text("0 artículos seleccionados", weight=ft.FontWeight.BOLD, color="#64748B")

        # Preview Table
        self.preview_table = SafeDataTable(
            columns=[
                ft.DataColumn(ft.Checkbox(label="Sel", on_change=self._toggle_all)),
                ft.DataColumn(ft.Text("ID")),
                ft.DataColumn(ft.Text("Artículo")),
                ft.DataColumn(ft.Text("Valor Actual")),
                ft.DataColumn(ft.Text("Nuevo Valor")),
                ft.DataColumn(ft.Text("Variación")),
            ],
            width=None,
            expand=True,
            heading_row_color="#F1F5F9",
            data_row_color={"hovered": "#F8FAFC"},
        )

        # Buttons
        self.btn_preview = ft.ElevatedButton(
            "Generar Vista Previa",
            icon=ft.Icons.PREVIEW_ROUNDED,
            on_click=self._run_preview,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=4),
                padding=20,
            ),
        )

        self.btn_apply = ft.ElevatedButton(
            "APLICAR CAMBIOS SELECCIONADOS",
            icon=ft.Icons.SAVE_ROUNDED,
            bgcolor="#EF4444",
            color="#FFFFFF",
            on_click=self._confirm_apply,
            disabled=True,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=4),
                padding=20,
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
                                    ft.Text("1. Filtrar Artículos", weight=ft.FontWeight.W_600, color="#1E293B", size=16),
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
                ft.Container(
                    padding=20,
                    border=ft.border.all(1, "#E2E8F0"),
                    border_radius=8,
                    bgcolor="#F8FAFC",
                    expand=True,
                    content=ft.Column(
                        [
                            ft.Row(
                                [
                                    ft.Text("3. Vista Previa y Selección", weight=ft.FontWeight.W_600, color="#1E293B", size=16),
                                    ft.Text("Seleccione los ítems a actualizar", size=12, color="grey"),
                                ],
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            ),
                            ft.Column([self.preview_table], scroll=ft.ScrollMode.AUTO, expand=True),
                            ft.Row([self.btn_apply], alignment=ft.MainAxisAlignment.END),
                        ],
                        spacing=15,
                        expand=True,
                    ),
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=20,
            expand=True,
        )

        # Initial Load
        self.load_catalogs()

        # Bind events
        for ctrl in [
            self.filter_marca,
            self.filter_rubro,
            self.filter_proveedor,
            self.filter_lista,
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

    # -----------------------------
    # Catalogs / Filters
    # -----------------------------
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

            provs = self.db.list_proveedores()
            self.filter_proveedor.options = [ft.dropdown.Option("", "Todos")] + [
                ft.dropdown.Option(str(p["id"]), p["nombre"]) for p in provs
            ]

            lists = self.db.fetch_listas_precio()
            self.filter_lista.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(l["id"]), l["nombre"]) for l in lists
            ]

            ivas = self.db.fetch_tipos_iva()
            self.filter_iva.options = [ft.dropdown.Option("", "Todas")] + [
                ft.dropdown.Option(str(i["id"]), f"{i['descripcion']} ({i['porcentaje']}%)") for i in ivas
            ]

            # Target Selector - specific lists
            current_target = self.target_selector.value
            self.target_selector.options = [ft.dropdown.Option("COSTO", "COSTO BASE")] + [
                ft.dropdown.Option(f"LIST:{l['id']}", f"Lista: {l['nombre']}") for l in lists
            ]
            self.target_selector.value = current_target or "COSTO"
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

            # Clear preview when filters change as it is invalid
            if len(self.preview_table.rows) > 0:
                self.preview_table.rows.clear()
                self.preview_table.update()
                self.selected_ids.clear()
                self.btn_apply.disabled = True
                self.btn_apply.text = "APLICAR CAMBIOS SELECCIONADOS"
                self.btn_apply.update()

        except Exception as e:
            print(f"Error count: {e}")

    def _on_target_change(self, e):
        val = self.target_selector.value
        if val == "COSTO":
            self.warning_text.value = "⚠️ Modificar el Costo recalculará automáticamente los precios basados en Margen."
            self.warning_text.color = "#EA580C"
        else:
            self.warning_text.value = "ℹ️ Modificar una lista ajustará el margen automáticamente con respecto al costo."
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

            rows = await asyncio.to_thread(
                self.db.preview_mass_update,
                filters=self._get_filters(),
                target=target,
                operation=self.op_selector.value,
                value=val,
                list_id=list_id,
                limit=None,  # full list (ojo si son miles)
            )

            self.preview_table.rows.clear()
            self.selected_ids.clear()

            for r in rows:
                rid = int(r["id"])
                self.selected_ids.add(rid)  # select all by default

                self.preview_table.rows.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(
                                ft.Checkbox(
                                    value=True,
                                    on_change=lambda e, rid=rid: self._toggle_one(rid, e.control.value),
                                )
                            ),
                            ft.DataCell(ft.Text(str(rid))),
                            ft.DataCell(ft.Text(r.get("nombre", ""))),
                            ft.DataCell(ft.Text(f"${float(r.get('current', 0.0)):,.2f}")),
                            ft.DataCell(
                                ft.Text(
                                    f"${float(r.get('new', 0.0)):,.2f}",
                                    weight=ft.FontWeight.BOLD,
                                    color="#166534",
                                )
                            ),
                            ft.DataCell(
                                ft.Text(
                                    f"{float(r.get('diff_pct', 0.0)):+.2f}%",
                                    color="#EA580C" if float(r.get("diff_pct", 0.0)) != 0 else "#64748B",
                                )
                            ),
                        ]
                    )
                )

            self.preview_table.update()

            self.btn_apply.text = f"APLICAR A {len(self.selected_ids)} ARTÍCULOS"
            self.btn_apply.disabled = len(self.selected_ids) == 0
            self.btn_apply.update()

            if not rows:
                self.show_toast("No se encontraron artículos con los filtros actuales.", "info")
            else:
                self.show_toast(f"Vista previa lista: {len(rows)} artículos.", "success")

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

        for row in self.preview_table.rows:
            # Checkbox visual
            cb = row.cells[0].content
            if hasattr(cb, "value"):
                cb.value = checked
            # ID
            art_id_cell = row.cells[1].content
            if hasattr(art_id_cell, "value"):
                art_id = int(art_id_cell.value)
                if checked:
                    self.selected_ids.add(art_id)

        self.preview_table.update()
        self._update_apply_btn()

    def _toggle_one(self, art_id: int, checked: bool):
        if checked:
            self.selected_ids.add(art_id)
        else:
            self.selected_ids.discard(art_id)
        self._update_apply_btn()

    def _update_apply_btn(self):
        count = len(self.selected_ids)
        self.btn_apply.text = f"APLICAR A {count} ARTÍCULOS"
        self.btn_apply.disabled = self._loading or (count == 0)
        self.btn_apply.update()

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
                ft.TextButton("Cancelar", on_click=close_dlg),
                ft.ElevatedButton("Confirmar y Aplicar", on_click=do_apply, bgcolor="#EF4444", color="white", style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=4))),
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

            affected = await asyncio.to_thread(
                self.db.mass_update_articles,
                filters={},  # ignorado si ids viene cargado
                target=target,
                operation=self.op_selector.value,
                value=val,
                list_id=list_id,
                ids=ids_snapshot,
            )

            # UI cleanup
            self.preview_table.rows.clear()
            self.preview_table.update()

            self.selected_ids.clear()
            self.btn_apply.disabled = True
            self.btn_apply.text = "APLICAR CAMBIOS SELECCIONADOS"
            self.btn_apply.update()

            self.show_toast(f"Actualización completada. {affected} artículos modificados.", "success")

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
