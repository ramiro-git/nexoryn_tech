import flet as ft
import datetime
import threading
import time
from typing import Dict, Any, List, Optional, Callable, Tuple
from desktop_app.database import Database

class DashboardView(ft.Container):
    def __init__(self, database: Database, user_role: str = "EMPLEADO"):
        super().__init__()
        self.db = database
        self.role = user_role.upper()
        self.role = user_role.upper()
        self.stats: Dict[str, Any] = {}
        self.on_navigate = None
        self.is_loading = True
        
        # Colors & Theme
        self.COLOR_BG = "#F1F5F9"
        self.COLOR_CARD = "#FFFFFF"
        self.COLOR_PRIMARY = "#4F46E5"  # Indigo
        self.COLOR_SUCCESS = "#10B981"  # Green
        self.COLOR_WARNING = "#F59E0B"  # Amber
        self.COLOR_ERROR = "#EF4444"    # Red
        self.COLOR_INFO = "#3B82F6"     # Blue
        self.COLOR_TEXT = "#1E293B"     # Slate 800
        self.COLOR_TEXT_MUTED = "#64748B" # Slate 500
        self.COLOR_BORDER = "#E2E8F0"   # Slate 200
        
        # UI Elements
        self.kpi_row = ft.Row(spacing=20, wrap=True)
        self.sections_column = ft.Column(spacing=15, expand=True)
        self.last_updated_text = ft.Text("Actualizando...", size=12, color=self.COLOR_TEXT_MUTED)
        self.refresh_button = ft.IconButton(
            ft.icons.REFRESH_ROUNDED, 
            tooltip="Actualizar ahora",
            on_click=lambda _: self.load_data()
        )
        
        # Container properties
        self.padding = 25
        self.bgcolor = self.COLOR_BG
        self.expand = True
        
        # Auto-refresh thread control
        self._stop_event = threading.Event()
        self._refresh_thread = None
        
        # Set lifecycle hooks
        self.on_mount = self._handle_mount
        self.on_unmount = self._handle_unmount

    def _handle_mount(self, e):
        self.load_data()
        self._start_refresh_thread()

    def _handle_unmount(self, e):
        self._stop_event.set()

    def _start_refresh_thread(self):
        def refresh_loop():
            while not self._stop_event.is_set():
                time.sleep(300) # 5 minutes
                if not self._stop_event.is_set():
                    try:
                        self.load_data()
                    except:
                        pass
        
        self._refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        self._refresh_thread.start()

    def load_data(self):
        # 1. Show Loader immediately
        self.is_loading = True
        self.content = ft.Container(
            content=ft.ProgressRing(),
            alignment=ft.Alignment(0, 0),
            expand=True
        )
        try:
            if self.page: self.update()
        except: pass
        
        # 2. Fetch Data (in main thread for simplicity, or bg thread if refactored)
        try:
            # Fetch 100+ stats in one batch filtered by role
            self.stats = self.db.get_full_dashboard_stats(self.role)
            self.last_updated_text.value = f"Última actualización: {datetime.datetime.now().strftime('%H:%M:%S')}"
        except Exception as e:
            err_msg = str(e).lower()
            if "content must be visible" not in err_msg and "page is not visible" not in err_msg:
                print(f"Error loading dashboard stats: {e}")
            self.stats = {} # Fallback
            
        # 3. Show Content
        self.is_loading = False
        self._build_dashboard_content()
        self.content = self._get_main_content()
        try:
            if self.page: self.update()
        except: pass

    def _get_main_content(self):
        return ft.Column([
            # Header
            ft.Row([
                ft.Column([
                    ft.Text("Tablero de Control", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Row([
                        ft.Icon(ft.icons.PERSON_ROUNDED, size=14, color=self.COLOR_TEXT_MUTED),
                        ft.Text(f"Rol: {self.role}", size=13, color=self.COLOR_TEXT_MUTED, weight=ft.FontWeight.W_500),
                    ], spacing=5),
                ], spacing=2),
                ft.Row([
                    self.last_updated_text,
                    self.refresh_button,
                ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER)
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            
            ft.Divider(height=10, color="transparent"),
            
            # Main Scrollable Content
            ft.Column([
                self.kpi_row,
                ft.Divider(height=10, color="transparent"),
                self.sections_column,
            ], scroll=ft.ScrollMode.AUTO, expand=True, spacing=10)
        ], spacing=15, expand=True)

    def _build_dashboard_content(self):
        # 1. KPIs (Top Cards)
        v_hoy = self.stats.get("ventas", {}).get("hoy_total", "—")
        v_cant = self.stats.get("ventas", {}).get("hoy_cant", 0)
        s_bajo = self.stats.get("stock", {}).get("bajo_stock", 0)
        r_pend = self.stats.get("operativas", {}).get("remitos_pend", 0)
        trend_v = self.stats.get("ventas", {}).get("tendencia_mes_pct", 0)
        charts = self.stats.get("charts", {})
        
        self.kpi_row = ft.ResponsiveRow(spacing=20)
        
        if self.role in ("ADMIN", "GERENTE"):
            self.kpi_row.controls.append(
                self._kpi_card("Ventas Hoy", self._format_number(v_hoy, 2, "$"), ft.icons.ATTACH_MONEY_ROUNDED, self.COLOR_SUCCESS, f"{v_cant} oper.", trend=trend_v)
            )
        else:
            self.kpi_row.controls.append(
                self._kpi_card("Mis Ventas Hoy", str(v_cant), ft.icons.SHOPPING_BAG_ROUNDED, self.COLOR_SUCCESS, "Operaciones registradas")
            )
        
        self.kpi_row.controls.extend([
            self._kpi_card("Stock Crítico", str(s_bajo), ft.icons.INVENTORY_2_ROUNDED, self.COLOR_WARNING if s_bajo > 0 else self.COLOR_INFO, "Requiere acción" if s_bajo > 0 else "Al día"),
            self._kpi_card("Remitos Pend.", str(r_pend), ft.icons.LOCAL_SHIPPING_ROUNDED, self.COLOR_PRIMARY, "Por entregar"),
            self._kpi_card("Docs Hoy", str(self.stats.get("operativas", {}).get("mis_operaciones_hoy", 0)), ft.icons.DESCRIPTION_ROUNDED, self.COLOR_INFO, "Mis registros")
        ])


        # 2. Category Sections
        self.sections_column.controls.clear()
        
        # Ventas Section
        self.sections_column.controls.append(
            self._section_container("VENTAS", ft.icons.SHOPPING_CART_ROUNDED, self._build_ventas_section(), "documentos")
        )

        # Operativa Section
        chart_icon = getattr(ft.icons, "SHOW_CHART_ROUNDED", None)
        if not chart_icon:
            chart_icon = getattr(ft.icons, "SHOW_CHART", None)
        if not chart_icon:
            chart_icon = ft.icons.DESCRIPTION_ROUNDED
        self.sections_column.controls.append(
            self._section_container("OPERATIVA Y MOVIMIENTOS", chart_icon, self._build_operativa_section(), "movimientos")
        )
        
        # Stock Section
        self.sections_column.controls.append(
            self._section_container("STOCK e INVENTARIO", ft.icons.INVENTORY_ROUNDED, self._build_stock_section(), "articulos")
        )
        
        # Analítica de Productos (ADMIN/GERENTE)
        # Analítica de Productos (ADMIN/GERENTE)
        if self.role in ("ADMIN", "GERENTE"):
            analitica_content = self._build_analitica_section()
            if analitica_content:
                self.sections_column.controls.append(
                    self._section_container("ANALÍTICA DE PRODUCTOS", ft.icons.ANALYTICS_ROUNDED, analitica_content, "articulos")
                )
        
        # Entidades Section
        self.sections_column.controls.append(
            self._section_container("CLIENTES y PROVEEDORES", ft.icons.PEOPLE_ROUNDED, self._build_entidades_section(), "entidades")
        )
        
        # Financial Section (Gerente/Admin)
        if self.role in ("ADMIN", "GERENTE") and "finanzas" in self.stats:
            self.sections_column.controls.append(
                self._section_container("FINANZAS Y CAJA", ft.icons.ACCOUNT_BALANCE_WALLET_ROUNDED, self._build_finanzas_section(), "pagos")
            )
            
        # System Section (Admin only)
        if self.role == "ADMIN" and "sistema" in self.stats:
            self.sections_column.controls.append(
                self._section_container("SISTEMA y SEGURIDAD", ft.icons.SECURITY_ROUNDED, self._build_sistema_section(), "config")
            )

    def _kpi_card(self, title: str, value: str, icon: str, color: str, subtitle: str, trend: float = None) -> ft.Container:
        # Dynamic font size for long numbers
        font_size = 28
        if len(str(value)) > 11:
            font_size = 20
        elif len(str(value)) > 9:
            font_size = 24

        trend_indicator = ft.Row()
        if trend is not None and trend != 0:
            trend_color = self.COLOR_SUCCESS if trend > 0 else self.COLOR_ERROR
            trend_icon = ft.icons.TRENDING_UP_ROUNDED if trend > 0 else ft.icons.TRENDING_DOWN_ROUNDED 
            
            trend_indicator = ft.Row([
                ft.Icon(trend_icon, color=trend_color, size=14),
                ft.Text(f"{'+' if trend > 0 else ''}{trend}%", size=11, color=trend_color, weight=ft.FontWeight.BOLD),
            ], spacing=2)

        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(icon, color=color, size=24),
                    ft.Text(title, size=14, color=self.COLOR_TEXT_MUTED, weight=ft.FontWeight.W_500),
                    ft.Container(expand=True),
                    trend_indicator
                ], alignment=ft.MainAxisAlignment.START, spacing=10),
                ft.Text(value, size=font_size, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                ft.Text(subtitle, size=13, color=color, weight=ft.FontWeight.W_600),
            ], spacing=5, alignment=ft.MainAxisAlignment.CENTER),
            col={"xs": 12, "sm": 6, "md": 3},
            height=150,
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER),
            shadow=ft.BoxShadow(blur_radius=10, color="#0000000D", offset=ft.Offset(0, 4))
        )


    def _section_container(self, title: str, icon: str, content: ft.Control, view_key: str = None) -> ft.Container:
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(icon, color=self.COLOR_PRIMARY, size=20),
                    ft.Text(title, size=16, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Container(expand=True),
                    ft.TextButton(
                        "Ver detalles",
                        icon=ft.icons.ARROW_FORWARD_ROUNDED,
                        icon_color=self.COLOR_PRIMARY,
                        style=ft.ButtonStyle(color=self.COLOR_PRIMARY),
                        on_click=lambda e: self.on_navigate(view_key) if self.on_navigate and view_key else None
                    )
                ], alignment=ft.MainAxisAlignment.START, spacing=10),
                ft.Divider(height=1, color=self.COLOR_BORDER),
                ft.Container(content=content, padding=ft.padding.only(top=10))
            ], spacing=5),
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER)
        )

    def _stat_item(self, label: str, value: Any, color: str = None, trend: float = None) -> ft.Control:
        if color is None: color = self.COLOR_TEXT
        # Auto-format number if it's a float/int and not already formatted
        display_val = str(value)
        if isinstance(value, (int, float)):
             display_val = self._format_number(value, 2 if isinstance(value, float) else 0)
        
        trend_badge = ft.Container()
        if trend is not None and trend != 0:
            t_color = self.COLOR_SUCCESS if trend > 0 else self.COLOR_ERROR
            t_icon = ft.icons.ARROW_UPWARD_ROUNDED if trend > 0 else ft.icons.ARROW_DOWNWARD_ROUNDED
            trend_badge = ft.Container(
                content=ft.Row([
                    ft.Icon(t_icon, size=10, color=t_color),
                    ft.Text(f"{abs(trend)}%", size=10, color=t_color, weight=ft.FontWeight.BOLD)
                ], spacing=2),
                padding=ft.padding.symmetric(horizontal=6, vertical=2),
                bgcolor=ft.Colors.with_opacity(0.1, t_color),
                border_radius=10
            )

        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text(label, size=12, color=self.COLOR_TEXT_MUTED),
                    trend_badge
                ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Text(display_val, size=16, weight=ft.FontWeight.BOLD, color=color),
            ], spacing=2),
            col={"xs": 6, "sm": 4, "md": 2},
            padding=ft.padding.symmetric(vertical=10)
        )


    def _format_number(self, value: Any, decimals: int = 0, prefix: str = "") -> str:
        if value is None:
            return "—"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        return f"{prefix}{number:,.{decimals}f}"

    def _as_number(self, value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _format_compact(self, n: float) -> str:
        if n >= 1_000_000:
            return f"{n/1_000_000:,.1f}M"
        if n >= 1_000:
            return f"{n/1_000:,.1f}K"
        return f"{n:,.0f}"

    def _chart_panel(self, title: str, content: ft.Control, width: int = 360) -> ft.Container:
        return ft.Container(
            content=ft.Column([
                ft.Text(title, size=12, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT_MUTED),
                content,
            ], spacing=15, tight=True),
            padding=20,
            bgcolor="#F8FAFC",
            border_radius=12,
            border=ft.border.all(1, self.COLOR_BORDER),
            width=width,
        )

    def _bar_chart(
        self,
        items: List[Tuple[str, Any]],
        *,
        color: str,
        value_formatter: Callable[[Any], str],
        empty_text: str = "Sin datos",
    ) -> ft.Control:
        if not items:
            return ft.Text(empty_text, size=12, color=self.COLOR_TEXT_MUTED)

        values = []
        max_val = 0.0
        for label, raw in items:
            num = self._as_number(raw)
            values.append((label, raw, num))
            if num > max_val:
                max_val = num

        if max_val <= 0:
            max_val = 1.0

        bars = []
        for label, raw, num in values:
            ratio = max(min(num / max_val, 1.0), 0.0)
            bars.append(ft.Column([
                ft.Row([
                    ft.Text(label, size=11, color=self.COLOR_TEXT_MUTED, expand=True),
                    ft.Text(value_formatter(raw), size=11, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.ProgressBar(value=ratio, color=color, bgcolor=self.COLOR_BORDER),
            ], spacing=4))

        return ft.Column(bars, spacing=8)

    def _pie_chart(self, data: Dict[str, float], colors: List[str] = None, value_formatter: Callable[[float], str] = str) -> ft.Control:
        if not data:
            return ft.Text("Sin datos", size=12, color=self.COLOR_TEXT_MUTED)
        
        if not colors:
            colors = [self.COLOR_PRIMARY, self.COLOR_INFO, self.COLOR_SUCCESS, self.COLOR_WARNING, self.COLOR_ERROR, "#8B5CF6", "#EC4899"]
        
        sections = []
        for i, (label, val) in enumerate(data.items()):
            if val <= 0: continue
            sections.append(
                ft.PieChartSection(
                    val,
                    title="",
                    color=colors[i % len(colors)],
                    radius=30,
                )
            )
        
        if not sections:
            return ft.Text("Sin datos significativos", size=12, color=self.COLOR_TEXT_MUTED)

        return ft.Row([
            ft.PieChart(
                sections=sections,
                sections_space=2,
                center_space_radius=20,
                expand=True,
                height=120,
            ),
            ft.Column([
                ft.Row([
                    ft.Container(width=10, height=10, bgcolor=colors[i % len(colors)], border_radius=2),
                    ft.Text(f"{label} ({value_formatter(val)})", size=10, color=self.COLOR_TEXT_MUTED)
                ], spacing=5) for i, (label, val) in enumerate(data.items()) if val > 0
            ], spacing=3, scroll=ft.ScrollMode.AUTO, height=120)
        ], spacing=10, alignment=ft.MainAxisAlignment.CENTER)

    def _line_chart(self, history: List[Dict[str, Any]]) -> ft.Control:
        if not history:
            return ft.Text("Sin historial disponible", size=12, color=self.COLOR_TEXT_MUTED)
        
        # Sort history by date ascending if it's descending
        history = sorted(history, key=lambda x: x['mes'])
        
        data_points = []
        max_val = 0
        for i, h in enumerate(history):
            val = float(h.get('total_ventas') or 0)
            if val > max_val: max_val = val
            data_points.append(ft.LineChartDataPoint(i, val))
        
        if max_val == 0: max_val = 1000

        month_map = {
             1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
             7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
        }

        def _get_month_label(m):
            if hasattr(m, 'month'):
                return month_map.get(m.month, str(m.month))
            try:
                 # Check if string date YYYY-MM...
                 dt = datetime.datetime.fromisoformat(str(m))
                 return month_map.get(dt.month, str(m))
            except:
                 return str(m)[:3]

        # Calculate dynamic width and explicit labels for Y-axis
        y_labels = []
        steps = 5
        for i in range(steps + 1):
            val = (max_val / steps) * i
            y_labels.append(
                ft.ChartAxisLabel(
                    value=val,
                    label=ft.Text(self._format_compact(val), size=9, color=self.COLOR_TEXT_MUTED)
                )
            )
        
        y_axis_width = 50 # Compact notation needs less horizontal space but more padding

        return ft.LineChart(
            data_series=[
                ft.LineChartData(
                    data_points=data_points,
                    stroke_width=3,
                    color=self.COLOR_PRIMARY,
                    curved=True,
                    below_line_bgcolor=ft.Colors.with_opacity(0.1, self.COLOR_PRIMARY),
                    below_line_gradient=ft.LinearGradient(
                        begin=ft.alignment.top_center,
                        end=ft.alignment.bottom_center,
                        colors=[ft.Colors.with_opacity(0.2, self.COLOR_PRIMARY), ft.Colors.with_opacity(0, self.COLOR_PRIMARY)]
                    )
                )
            ],
            bottom_axis=ft.ChartAxis(
                labels=[
                    ft.ChartAxisLabel(
                        value=i,
                        label=ft.Text(_get_month_label(h['mes']), size=10, color=self.COLOR_TEXT_MUTED)
                    ) for i, h in enumerate(history)
                ],
                labels_size=25,
            ),
            left_axis=ft.ChartAxis(
                labels=y_labels,
                labels_size=y_axis_width,
            ),
            max_y=max_val * 1.15,
            tooltip_bgcolor=ft.Colors.TRANSPARENT,
            border=ft.Border(bottom=ft.BorderSide(1, self.COLOR_BORDER)),
            horizontal_grid_lines=ft.ChartGridLines(interval=max_val/steps, color=self.COLOR_BORDER, width=0.5),
            expand=True,
            height=300,
        )

    def _real_bar_chart(self, items: List[Dict[str, Any]], color: str = None) -> ft.Control:
        if not items:
            return ft.Text("Sin datos", size=12, color=self.COLOR_TEXT_MUTED)
        
        if not color: color = self.COLOR_PRIMARY
        
        groups = []
        max_val = 0
        for i, item in enumerate(items):
            val = float(item.get('total_facturado') or item.get('cantidad_vendida') or 0)
            if val > max_val: max_val = val
            groups.append(
                ft.BarChartGroup(
                    x=i,
                    bar_rods=[
                        ft.BarChartRod(
                            from_y=0,
                            to_y=val,
                            width=15,
                            color=color,
                            border_radius=3,
                        )
                    ]
                )
            )
        
        if max_val == 0: max_val = 1
        
        # Calculate dynamic width and explicit labels for Y-axis
        y_labels = []
        steps = 4
        for i in range(steps + 1):
            val = (max_val / steps) * i
            y_labels.append(
                ft.ChartAxisLabel(
                    value=val,
                    label=ft.Text(self._format_compact(val), size=9, color=self.COLOR_TEXT_MUTED)
                )
            )

        return ft.BarChart(
            bar_groups=groups,
            bottom_axis=ft.ChartAxis(
                labels=[
                    ft.ChartAxisLabel(
                        value=i,
                        label=ft.Text(item.get('nombre', '')[:8], size=9, color=self.COLOR_TEXT_MUTED, text_align=ft.TextAlign.CENTER)
                    ) for i, item in enumerate(items)
                ],
                labels_size=25,
            ),
            left_axis=ft.ChartAxis(
                labels=y_labels,
                labels_size=45,
            ),
            max_y=max_val * 1.15,
            tooltip_bgcolor=ft.Colors.TRANSPARENT,
            border=ft.Border(bottom=ft.BorderSide(1, self.COLOR_BORDER)),
            horizontal_grid_lines=ft.ChartGridLines(interval=max_val/steps, color=self.COLOR_BORDER, width=0.5),
            expand=True,
            height=250,
        )

    def _build_ventas_section(self) -> ft.Control:
        v = self.stats.get("ventas", {})
        charts = self.stats.get("charts", {})
        trend = v.get("tendencia_mes_pct", 0)
        
        items = [
            self._stat_item("Presup. Pénd.", v.get("presupuestos_pend", 0)),
            self._stat_item("Comprobantes Hoy", v.get("hoy_cant", 0), self.COLOR_INFO),
        ]
        
        if self.role in ("ADMIN", "GERENTE"):
            items.insert(0, self._stat_item("Ventas Semana", self._format_number(v.get('semana_total', 0), 2, "$")))
            items.insert(1, self._stat_item("Ventas Mes", self._format_number(v.get('mes_total', 0), 2, "$"), self.COLOR_PRIMARY, trend=trend))
            items.insert(2, self._stat_item("Ventas Año", self._format_number(v.get('anio_total', 0), 2, "$")))
            items.append(self._stat_item("Anulados Mes", v.get("anulados_mes", 0), self.COLOR_ERROR))

        chart_row = ft.Row(wrap=True, spacing=20)
        if self.role in ("ADMIN", "GERENTE"):
            chart_row.controls.extend([
                self._chart_panel("Tendencia de Ventas (Mensual)", self._line_chart(charts.get("ventas_mensuales", [])), width=650),
                self._chart_panel("Mix de Documentos", self._pie_chart(v.get("por_tipo", {}), value_formatter=lambda x: f"{int(x)}"), width=300),
                self._chart_panel("Participación Formas de Pago", self._pie_chart(v.get("por_forma_pago", {}), value_formatter=lambda x: self._format_number(x, 2, "$")), width=300),
            ])
        else:
            chart_row.controls.append(
                self._chart_panel("Mix de Documentos", self._pie_chart(v.get("por_tipo", {}), value_formatter=lambda x: f"{int(x)}"), width=400)
            )

        return ft.Column([
            ft.ResponsiveRow(items, spacing=10),
            ft.Divider(height=20, color="transparent"),
            chart_row
        ])

    def _build_stock_section(self) -> ft.Control:
        s = self.stats.get("stock", {})
        charts = self.stats.get("charts", {})
        alertas = charts.get("alertas_stock", [])
        
        critical_list = ft.Column([
            ft.Text("Artículos en nivel crítico:", size=11, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT_MUTED),
        ], spacing=5)
        
        if alertas:
            for a in alertas[:5]: # Top 5
                critical_list.controls.append(
                    ft.Row([
                        ft.Icon(ft.icons.WARNING_AMBER_ROUNDED, size=12, color=self.COLOR_WARNING),
                        ft.Text(f"{a['nombre']}", size=11, color=self.COLOR_TEXT, expand=True),
                        ft.Text(f"Stock: {a['stock_actual']} / Min: {a['stock_minimo']}", size=10, weight=ft.FontWeight.W_500, color=self.COLOR_ERROR),
                    ], spacing=5)
                )
        else:
            critical_list.controls.append(ft.Text("No hay alertas de stock", size=11, italic=True, color=self.COLOR_TEXT_MUTED))

        return ft.Column([
            ft.ResponsiveRow([
                self._stat_item("Total Artículos", s.get("total", 0)),
                self._stat_item("Activos", s.get("activos", 0), self.COLOR_SUCCESS),
                self._stat_item("Stock Crítico", s.get("bajo_stock", 0), self.COLOR_WARNING),
                self._stat_item("Sin Stock", s.get("sin_stock", 0), self.COLOR_ERROR),
                self._stat_item("Valor Inventario", self._format_number(s.get('valor_inventario', 0), 2, "$"), self.COLOR_INFO),
                self._stat_item("Ingresos Mes", s.get("entradas_mes", 0)),
                self._stat_item("Salidas Mes", s.get("salidas_mes", 0)),
            ], spacing=10),
            ft.Divider(height=20, color="transparent"),
            ft.Row([
                self._chart_panel(
                    "Alertas de Reposición (Artículos con mayor faltante)",
                    ft.Column([
                        self._real_bar_chart([
                            {"nombre": a['nombre'], "total_facturado": a['faltante']} 
                            for a in alertas
                        ], color=self.COLOR_ERROR),
                        ft.Divider(height=10, color="transparent"),
                        critical_list
                    ], spacing=0),
                    width=650
                ),
                self._chart_panel(
                    "Stock por Rubro",
                    self._pie_chart(
                        {r["nombre"]: r["cantidad"] for r in charts.get("stock_por_rubro", [])},
                        value_formatter=lambda x: f"{int(x)} art."
                    ),
                    width=400
                )
            ], wrap=True, spacing=20)
        ])


    def _build_analitica_section(self) -> ft.Control:
        charts = self.stats.get("charts", {})
        if not charts.get("top_articulos") and not charts.get("bottom_articulos"):
            return None

        return ft.Column([
            ft.Row([
                self._chart_panel(
                    "TOP 5: Lo que más sale (Facturación)",
                    self._real_bar_chart(charts.get("top_articulos", [])),
                    width=630
                ) if charts.get("top_articulos") else ft.Container(),
                self._chart_panel(
                    "BOTTOM 5: Lo que menos sale (Ventas Mes)",
                    self._real_bar_chart(charts.get("bottom_articulos", []), color=self.COLOR_WARNING),
                    width=630
                ) if charts.get("bottom_articulos") else ft.Container(),
            ], wrap=True, spacing=20),
        ])

    def _build_entidades_section(self) -> ft.Control:
        e = self.stats.get("entidades", {})
        charts = self.stats.get("charts", {})
        return ft.Column([
            ft.ResponsiveRow([
                self._stat_item("Clientes Totales", e.get("clientes_total", 0)),
                self._stat_item("Proveedores", e.get("proveedores_total", 0)),
                self._stat_item("Nuevos (Mes)", e.get("nuevos_mes", 0), self.COLOR_SUCCESS),
                self._stat_item("Deuda Clientes", self._format_number(e.get('deuda_clientes', 0), 2, "$"), self.COLOR_ERROR),
                self._stat_item("Cant. Deudores", e.get("deudores_cant", 0)),
            ], spacing=10),
            ft.Divider(height=20, color="transparent"),
            ft.Row([
                self._chart_panel(
                    "Composición de Base",
                    self._pie_chart(
                        {r["nombre"]: r["cantidad"] for r in charts.get("entidades_por_tipo", [])},
                        colors=[self.COLOR_INFO, self.COLOR_PRIMARY, self.COLOR_WARNING],
                        value_formatter=lambda x: f"{int(x)}"
                    ),
                    width=400
                ),
                self._chart_panel(
                    "Estado de Deuda",
                    self._pie_chart({
                        "Deudores": e.get("deudores_cant", 0),
                        "Al día": (self._as_number(e.get("clientes_total", 0)) - self._as_number(e.get("deudores_cant", 0)))
                    }, colors=[self.COLOR_ERROR, self.COLOR_SUCCESS], value_formatter=lambda x: f"{int(x)}"),
                    width=400
                )
            ], spacing=20)
        ])

    def _build_operativa_section(self) -> ft.Control:
        o = self.stats.get("operativas", {})
        m = self.stats.get("movimientos", {})
        
        stat_items = [
            self._stat_item("Entregas Hoy", o.get("entregas_hoy", 0), self.COLOR_SUCCESS),
        ]
        if self.role == "ADMIN":
             stat_items.append(self._stat_item("Actividad Sistema", o.get("actividad_sistema", 0), self.COLOR_INFO))
             
        stat_items.extend([
            self._stat_item("Mov. Ingresos", m.get("ingresos", 0), self.COLOR_SUCCESS),
            self._stat_item("Mov. Salidas", m.get("salidas", 0), self.COLOR_WARNING),
            self._stat_item("Ajustes", m.get("ajustes", 0), self.COLOR_ERROR),
        ])

        chart_items = [
            ("Remitos pend.", o.get("remitos_pend", 0)),
            ("Entregas hoy", o.get("entregas_hoy", 0)),
            ("Docs hoy", o.get("mis_operaciones_hoy", 0)),
        ]
        if self.role == "ADMIN":
            chart_items.append(("Logs hoy", o.get("actividad_sistema", 0)))

        return ft.Column([
            ft.ResponsiveRow(stat_items, spacing=10),
            ft.Container(height=10),
            ft.Row([
                self._chart_panel(
                    "Movimientos de Stock (Hoy)",
                    self._bar_chart(
                        [
                            ("Ingresos", m.get("ingresos", 0)),
                            ("Salidas", m.get("salidas", 0)),
                            ("Ajustes", m.get("ajustes", 0)),
                        ],
                        color=self.COLOR_PRIMARY,
                        value_formatter=lambda v: self._format_number(v),
                        empty_text="Sin movimientos hoy",
                    ),
                ),
                self._chart_panel(
                    "Actividad Operativa (Hoy)",
                    self._bar_chart(
                        chart_items,
                        color=self.COLOR_INFO,
                        value_formatter=lambda v: self._format_number(v),
                        empty_text="Sin actividad hoy",
                    ),
                ),
            ], wrap=True, spacing=20),
        ])

    def _build_finanzas_section(self) -> ft.Control:
        f = self.stats.get("finanzas", {})
        return ft.Column([
            ft.ResponsiveRow([
                self._stat_item("Ingresos Hoy", self._format_number(f.get('ingresos_hoy', 0), 2, "$"), self.COLOR_SUCCESS),
                self._stat_item("Ingresos Mes", self._format_number(f.get('ingresos_mes', 0), 2, "$")),
                self._stat_item("Egresos Mes", self._format_number(f.get('egresos_mes', 0), 2, "$"), self.COLOR_ERROR),
                self._stat_item("Balance Mes", self._format_number(f.get('balance_mes', 0), 2, "$"), self.COLOR_PRIMARY),
                self._stat_item("IVA Est. Mes", self._format_number(f.get('iva_estimado', 0), 2, "$"), self.COLOR_INFO),
            ], spacing=10),
            ft.Divider(height=20, color="transparent"),
            ft.Row([
                self._chart_panel(
                    "Ingresos vs Egresos (Mes)",
                    self._pie_chart({
                        "Ingresos": f.get("ingresos_mes", 0),
                        "Egresos": f.get("egresos_mes", 0)
                    }, colors=[self.COLOR_SUCCESS, self.COLOR_ERROR], value_formatter=lambda x: self._format_number(x, 2, "$")),
                    width=300
                ),
                self._chart_panel(
                    "Distribución Financiera",
                    self._pie_chart(
                        {
                            "Ingresos": f.get("ingresos_mes", 0),
                            "Egresos": f.get("egresos_mes", 0),
                            "IVA Est.": f.get("iva_estimado", 0)
                        },
                        colors=[self.COLOR_SUCCESS, self.COLOR_ERROR, self.COLOR_INFO],
                        value_formatter=lambda v: self._format_number(v, 2, "$")
                    ),
                    width=400
                )
            ], spacing=20)
        ])

    def _build_sistema_section(self) -> ft.Control:
        sis = self.stats.get("sistema", {})
        return ft.ResponsiveRow([
            self._stat_item("Usuarios Activos", sis.get("usuarios_activos", 0)),
            self._stat_item("Errores Mes", sis.get("errores_mes", 0), self.COLOR_ERROR),
            self._stat_item("Backups Ok", sis.get("backups_mes", 0), self.COLOR_SUCCESS),
            self._stat_item("Última Actividad", self.stats.get("operativas", {}).get("actividad_sistema", 0), self.COLOR_INFO),
        ], spacing=10)

    def _badge(self, label: str, value: str) -> ft.Container:
        return ft.Container(
            content=ft.Row([
                ft.Text(label, size=10, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT_MUTED),
                ft.Text(value, size=11, weight=ft.FontWeight.BOLD, color=self.COLOR_PRIMARY),
            ], spacing=5, tight=True),
            padding=ft.padding.symmetric(horizontal=10, vertical=4),
            bgcolor="#F1F5F9",
            border_radius=20,
            border=ft.border.all(1, self.COLOR_BORDER)
        )
