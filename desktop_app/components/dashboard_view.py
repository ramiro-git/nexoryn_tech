import flet as ft
import datetime
import logging
import threading
import time
from typing import Dict, Any, List, Optional, Callable, Tuple
from desktop_app.database import Database
from desktop_app.services.number_locale import format_currency, format_decimal

logger = logging.getLogger(__name__)

class DashboardView(ft.Container):
    # -------------------------------------------------------------------------
    # Theme Configuration (reusable color palette)
    # -------------------------------------------------------------------------
    THEME = {
        "bg": "#F1F5F9",
        "card": "#FFFFFF",
        "primary": "#4F46E5",      # Indigo
        "success": "#10B981",      # Green
        "warning": "#F59E0B",      # Amber
        "error": "#EF4444",        # Red
        "info": "#3B82F6",         # Blue
        "text": "#1E293B",         # Slate 800
        "text_muted": "#64748B",   # Slate 500
        "border": "#E2E8F0",       # Slate 200
    }
    
    # Empty state messages by section
    EMPTY_STATES = {
        "ventas": ("Sin ventas registradas", ft.icons.POINT_OF_SALE_ROUNDED, "No hay ventas en el período seleccionado"),
        "stock": ("Inventario vacío", ft.icons.INVENTORY_2_ROUNDED, "No hay artículos registrados"),
        "entidades": ("Sin clientes/proveedores", ft.icons.PEOPLE_ROUNDED, "Aún no hay entidades comerciales"),
        "operativas": ("Sin actividad", ft.icons.PENDING_ACTIONS_ROUNDED, "No hay operaciones registradas hoy"),
        "finanzas": ("Sin movimientos financieros", ft.icons.ACCOUNT_BALANCE_WALLET_ROUNDED, "No hay pagos ni cobros"),
        "sistema": ("Sin datos del sistema", ft.icons.SETTINGS_ROUNDED, "Información del sistema no disponible"),
        "charts": ("Sin datos para graficar", ft.icons.BAR_CHART_ROUNDED, "No hay suficientes datos"),
    }
    
    # Refresh interval options (label -> seconds, 0 = disabled)
    REFRESH_INTERVALS = {
        "30 seg": 30,
        "1 min": 60,
        "5 min": 300,
        "10 min": 600,
        "Desactivado": 0,
    }
    
    # Period filter options (for future filtering - currently visual only)
    PERIOD_FILTERS = ["Hoy", "Semana", "Mes", "Año"]
    
    VALID_ROLES = {"ADMIN", "GERENTE", "EMPLEADO"}
    
    def __init__(self, database: Database, user_role: str = "EMPLEADO"):
        super().__init__()
        self.db = database
        self.role = self._normalize_role(user_role)
        self.stats: Dict[str, Any] = {}
        self.on_navigate = None
        self.is_loading = True
        self._refresh_retries = 0
        self._max_refresh_retries = 3
        
        # Concurrency control
        self._current_request_id = 0
        self._request_lock = threading.Lock()
        
        # Refresh interval (default 60s)
        self.current_interval = 60
        
        # Period filter (default "Mes" - currently visual, backend filtering TODO)
        self.current_period = "Mes"
        
        # Colors & Theme (derived from class THEME for easy customization)
        self.COLOR_BG = self.THEME["bg"]
        self.COLOR_CARD = self.THEME["card"]
        self.COLOR_PRIMARY = self.THEME["primary"]
        self.COLOR_SUCCESS = self.THEME["success"]
        self.COLOR_WARNING = self.THEME["warning"]
        self.COLOR_ERROR = self.THEME["error"]
        self.COLOR_INFO = self.THEME["info"]
        self.COLOR_TEXT = self.THEME["text"]
        self.COLOR_TEXT_MUTED = self.THEME["text_muted"]
        self.COLOR_BORDER = self.THEME["border"]
        
        # UI Elements
        self.kpi_row = ft.Row(spacing=20, wrap=True)
        self.sections_column = ft.Column(spacing=15, expand=True)
        self.last_updated_text = ft.Text("Actualizando...", size=12, color=self.COLOR_TEXT_MUTED)
        self.refresh_button = ft.IconButton(
            ft.icons.REFRESH_ROUNDED, 
            tooltip="Actualizar ahora",
            on_click=lambda _: self.load_data()
        )
        
        # Interval selector dropdown
        self.interval_dropdown = ft.Dropdown(
            width=130,
            height=35,
            content_padding=ft.padding.symmetric(horizontal=10, vertical=5),
            text_size=12,
            options=[ft.dropdown.Option(k) for k in self.REFRESH_INTERVALS.keys()],
            value="1 min",
            on_change=self._on_interval_change,
            border_color=self.COLOR_BORDER,
            focused_border_color=self.COLOR_PRIMARY,
        )
        
        # Container properties
        self.padding = 25
        self.bgcolor = self.COLOR_BG
        self.expand = True
        
        # Auto-refresh thread control
        self._stop_event = threading.Event()
        self._refresh_thread = None
        self._last_refresh_ts = 0.0
        
        # Set lifecycle hooks
        self.on_mount = self._handle_mount
        self.on_unmount = self._handle_unmount
    
    def _normalize_role(self, role: str) -> str:
        """Normalize and validate user role."""
        normalized = (role or "EMPLEADO").upper().strip()
        return normalized if normalized in self.VALID_ROLES else "EMPLEADO"
    
    def _safe_update(self):
        """Safely call update, handling cases where page is not visible."""
        try:
            if self.page:
                self.update()
        except Exception as e:
            err_msg = str(e).lower()
            if "content must be visible" not in err_msg and "page is not visible" not in err_msg:
                logger.debug(f"Dashboard update skipped: {e}")
    
    def _on_period_change(self, period: str):
        """Handle period filter change."""
        self.current_period = period
        logger.debug(f"Dashboard period changed to {period}")
        # In a real implementation, we would pass this period to the load_data method
        # For now, we just reload to simulate the interaction
        self.load_data()

    def _on_interval_change(self, e):
        """Handle refresh interval change from dropdown."""
        new_interval = self.REFRESH_INTERVALS.get(e.control.value, 60)
        self.current_interval = new_interval
        logger.debug(f"Dashboard refresh interval changed to {new_interval}s")
        
        # Restart refresh thread with new interval
        self._stop_event.set()
        if self._refresh_thread and self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=1)
        self._stop_event.clear()
        self._start_refresh_thread()

    def _handle_mount(self, e):
        self._stop_event.clear()
        self.load_data()
        if not self._refresh_thread or not self._refresh_thread.is_alive():
            self._start_refresh_thread()

    def _handle_unmount(self, e):
        self._stop_event.set()

    def _start_refresh_thread(self):
        """Start the auto-refresh thread with configurable interval."""
        if self.current_interval <= 0:
            logger.debug("Auto-refresh disabled")
            return
            
        def refresh_loop():
            self._refresh_retries = 0
            while not self._stop_event.is_set():
                # Use current_interval which can change dynamically
                interval = self.current_interval
                if interval <= 0:
                    break
                    
                # Sleep in small increments to respond to stop_event quickly
                for _ in range(interval):
                    if self._stop_event.is_set():
                        return
                    time.sleep(1)
                
                if not self._stop_event.is_set():
                    try:
                        self.load_data(silent=True)
                        self._refresh_retries = 0  # Reset on success
                    except Exception as e:
                        self._refresh_retries += 1
                        logger.warning(f"Dashboard refresh failed (attempt {self._refresh_retries}): {e}")
                        if self._refresh_retries >= self._max_refresh_retries:
                            logger.error("Dashboard refresh max retries reached, stopping auto-refresh")
                            break
        
        # Reset retries on new start
        self._refresh_retries = 0
        self._refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        self._refresh_thread.start()

    def load_data(self, silent: bool = False):
        """Load dashboard data. Uses background thread to avoid blocking UI."""
        
        with self._request_lock:
            self._current_request_id += 1
            request_id = self._current_request_id
        
        self.is_loading = True
        
        # Show skeleton/loader only if not silent
        if not silent:
            self._show_skeleton()
        
        def fetch_in_background(req_id, period):
            try:
                # Pass current period to backend
                stats = self.db.get_full_dashboard_stats(self.role, period=period)
                
                # Check if this request is still valid
                with self._request_lock:
                    if self._current_request_id != req_id:
                        logger.debug(f"Ignoring stale dashboard data (Req {req_id} vs {self._current_request_id})")
                        return

                # Update UI in main thread context - verify active state
                if not self._stop_event.is_set():
                    self._on_data_loaded(stats, req_id)
            except Exception as e:
                if not self._stop_event.is_set():
                    self._on_load_error(str(e), req_id)
        
        # Run fetch in background thread
        threading.Thread(
            target=fetch_in_background, 
            args=(request_id, self.current_period), 
            daemon=True
        ).start()
    
    def _show_skeleton(self):
        """Show skeleton placeholder while data is loading."""
        skeleton_cards = ft.ResponsiveRow([
            self._skeleton_card() for _ in range(4)
        ], spacing=20)
        
        skeleton_sections = ft.Column([
            self._skeleton_section() for _ in range(2)
        ], spacing=15)
        
        self.content = ft.Column([
            self._get_header(),
            ft.Divider(height=10, color="transparent"),
            skeleton_cards,
            ft.Divider(height=15, color="transparent"),
            skeleton_sections,
        ], spacing=10, expand=True)
        self._safe_update()
    
    def _skeleton_card(self) -> ft.Container:
        """Create a skeleton placeholder card."""
        return ft.Container(
            content=ft.Column([
                ft.Container(width=100, height=14, bgcolor=self.COLOR_BORDER, border_radius=4),
                ft.Container(width=80, height=28, bgcolor=self.COLOR_BORDER, border_radius=4),
                ft.Container(width=60, height=12, bgcolor=self.COLOR_BORDER, border_radius=4),
            ], spacing=8),
            col={"xs": 12, "sm": 6, "md": 3},
            height=130,
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER),
        )
    
    def _skeleton_section(self) -> ft.Container:
        """Create a skeleton placeholder section."""
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Container(width=150, height=16, bgcolor=self.COLOR_BORDER, border_radius=4),
                ]),
                ft.Container(height=80, bgcolor=self.COLOR_BORDER, border_radius=8, expand=True),
            ], spacing=10),
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER),
            height=140,
        )
    
    def _on_data_loaded(self, stats: Dict[str, Any], request_id: int):
        """Handle successful data load with race condition check."""
        # Race condition check (Step 1)
        with self._request_lock:
            if self._current_request_id != request_id:
                logger.debug(f"Discarding stale data load (Req {request_id})")
                return

        # Thread Safety (Step 2): Delegate UI update to main thread if possible
        if self.page and hasattr(self.page, "run_thread_safe"):
            self.page.run_thread_safe(self._apply_data_to_ui, stats, request_id)
        else:
            # Fallback for older Flet versions or if not attached
            self._apply_data_to_ui(stats, request_id)

    def _apply_data_to_ui(self, stats: Dict[str, Any], request_id: int):
        """Build and update UI elements (must run on main thread preferably)."""
        # Final race condition check on main thread
        with self._request_lock:
             if self._current_request_id != request_id:
                 return

        self._last_refresh_ts = time.monotonic()
        self.stats = stats
        self.last_updated_text.value = f"Última actualización: {datetime.datetime.now().strftime('%H:%M:%S')}"
        self.is_loading = False
        self._build_dashboard_content()
        self.content = self._get_main_content()
        self._safe_update()
    
    def _on_load_error(self, error: str, request_id: int):
        """Handle load error with race condition check."""
        with self._request_lock:
            if self._current_request_id != request_id:
               return

        # Thread Safety (Step 2): Delegate UI update to main thread if possible
        if self.page and hasattr(self.page, "run_thread_safe"):
            self.page.run_thread_safe(self._apply_error_to_ui, error, request_id)
        else:
            self._apply_error_to_ui(error, request_id)

    def _apply_error_to_ui(self, error: str, request_id: int):
        """Update UI on error (must run on main thread preferably)."""
        # Final race condition check on main thread
        with self._request_lock:
             if self._current_request_id != request_id:
                 return

        self._last_refresh_ts = time.monotonic()
        self.is_loading = False
        self.stats = {}  # Fallback
        
        # Log the error
        err_lower = error.lower()
        if "content must be visible" not in err_lower and "page is not visible" not in err_lower:
            logger.error(f"Dashboard load error: {error}")
        
        # Show snackbar to user
        if self.page:
            try:
                self.page.snack_bar = ft.SnackBar(
                    content=ft.Text(f"Error al cargar dashboard: {error[:80]}"),
                    bgcolor=self.COLOR_ERROR,
                    action="Reintentar",
                    on_action=lambda _: self.load_data(),
                )
                self.page.snack_bar.open = True
            except Exception:
                pass
        
        # Still show the dashboard with empty/fallback data
        self._build_dashboard_content()
        self.content = self._get_main_content()
        self._safe_update()

    def request_auto_refresh(self, silent: bool = True) -> None:
        """Auto-refresh gate for external polling triggers (respects interval)."""
        if self.current_interval <= 0:
            return
        if self.is_loading:
            return
        now = time.monotonic()
        if now - self._last_refresh_ts < self.current_interval:
            return
        self.load_data(silent=silent)



    def _get_header(self) -> ft.Control:
        """Build the dashboard header with controls."""
        
        # Period filter chips
        period_chips = ft.Row(
            controls=[
                ft.Container(
                    content=ft.Text(p, size=12, color=self.COLOR_CARD if self.current_period == p else self.COLOR_TEXT_MUTED),
                    padding=ft.padding.symmetric(horizontal=12, vertical=6),
                    bgcolor=self.COLOR_PRIMARY if self.current_period == p else ft.Colors.TRANSPARENT,
                    border=ft.border.all(1, self.COLOR_PRIMARY if self.current_period == p else self.COLOR_BORDER),
                    border_radius=20,
                    on_click=lambda e, p=p: self._on_period_change(p),
                    animate=ft.animation.Animation(200, ft.AnimationCurve.EASE_OUT),
                ) for p in self.PERIOD_FILTERS
            ],
            spacing=8
        )

        return ft.Column([
            ft.Row([
                ft.Column([
                    ft.Text("Tablero de Control", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Row([
                        ft.Icon(ft.icons.PERSON_ROUNDED, size=14, color=self.COLOR_TEXT_MUTED),
                        ft.Text(f"Rol: {self.role}", size=13, color=self.COLOR_TEXT_MUTED, weight=ft.FontWeight.W_500),
                    ], spacing=5),
                ], spacing=2),
                ft.Row([
                    ft.Text("Actualización cada:", size=11, color=self.COLOR_TEXT_MUTED),
                    self.interval_dropdown,
                    ft.Container(width=10),
                    self.last_updated_text,
                    self.refresh_button,
                ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER)
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, wrap=True),
            
            ft.Container(height=5),
            
            ft.Row([
                period_chips,
            ])
        ], spacing=10)

    def _get_main_content(self):
        return ft.Column([
            # Header
            self._get_header(),
            
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
            self._kpi_card("Alerta Inventario", str(s_bajo), ft.icons.INVENTORY_2_ROUNDED, self.COLOR_WARNING if s_bajo > 0 else self.COLOR_INFO, "Requiere acción" if s_bajo > 0 else "Al día"),
            self._kpi_card("Remitos Pend.", str(r_pend), ft.icons.LOCAL_SHIPPING_ROUNDED, self.COLOR_PRIMARY, "Por entregar"),
            self._kpi_card("Comprobantes Hoy", str(self.stats.get("operativas", {}).get("mis_operaciones_hoy", 0)), ft.icons.DESCRIPTION_ROUNDED, self.COLOR_INFO, "Mis registros")
        ])


        # 2. Category Sections
        self.sections_column.controls.clear()
        
        # Ventas Section
        self.sections_column.controls.append(
            self._section_container("VENTAS Y COMPROBANTES", ft.icons.SHOPPING_CART_ROUNDED, self._build_ventas_section(), "documentos")
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
            self._section_container("INVENTARIO Y STOCK", ft.icons.INVENTORY_ROUNDED, self._build_stock_section(), "articulos")
        )
        
        # Analítica de Productos (ADMIN/GERENTE)
        if self.role in ("ADMIN", "GERENTE"):
            analitica_content = self._build_analitica_section()
            if analitica_content:
                self.sections_column.controls.append(
                    self._section_container("ANALÍTICA DE INVENTARIO", ft.icons.ANALYTICS_ROUNDED, analitica_content, "articulos")
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


    # Mapping of view_key to display labels for "Ver detalles" buttons
    VIEW_LABELS = {
        "documentos": ("Ver Comprobantes", "Gestiona facturas, presupuestos y comprobantes"),
        "movimientos": ("Ver Movimientos", "Consulta ingresos, egresos y ajustes de stock"),
        "articulos": ("Ver Inventario", "Administra productos e inventario"),
        "entidades": ("Ver Entidades", "Gestiona clientes y proveedores"),
        "pagos": ("Ver Caja", "Consulta cobros y gestión de caja"),
        "config": ("Configuración", "Ajustes del sistema y usuarios"),
    }

    def _section_container(self, title: str, icon: str, content: ft.Control, view_key: str = None) -> ft.Container:
        # Get contextual button label and tooltip
        btn_label, btn_tooltip = self.VIEW_LABELS.get(view_key, ("Ver detalles", ""))
        
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(icon, color=self.COLOR_PRIMARY, size=20),
                    ft.Text(title, size=16, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Container(expand=True),
                    ft.ElevatedButton(
                        content=ft.Row([
                            ft.Text(btn_label, size=12, weight=ft.FontWeight.W_600),
                            ft.Icon(ft.icons.CHEVRON_RIGHT_ROUNDED, size=16),
                        ], spacing=4),
                        style=ft.ButtonStyle(
                            color=ft.colors.WHITE,
                            bgcolor=self.COLOR_PRIMARY,
                            padding=ft.padding.symmetric(horizontal=12, vertical=8),
                            shape=ft.RoundedRectangleBorder(radius=8),
                            elevation={"hovered": 2, "": 0},
                        ),
                        tooltip=btn_tooltip if btn_tooltip else None,
                        on_click=lambda e, vk=view_key: self.on_navigate(vk) if self.on_navigate and vk else None
                    ) if view_key else ft.Container()
                ], alignment=ft.MainAxisAlignment.START, spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Divider(height=1, color=self.COLOR_BORDER),
                ft.Container(content=content, padding=ft.padding.only(top=10))
            ], spacing=5),
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER),
            shadow=ft.BoxShadow(blur_radius=5, color="#00000005", offset=ft.Offset(0, 2))
        )

    def _stat_item(self, label: str, value: Any, color: str = None, trend: float = None, icon: str = None) -> ft.Control:
        if color is None: color = self.COLOR_TEXT
        # Auto-format number if it's a float/int and not already formatted
        display_val = str(value) if value is not None else "—"
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
                    ft.Icon(icon, color=self.COLOR_TEXT_MUTED, size=14) if icon else ft.Container(),
                    ft.Text(label, size=12, color=self.COLOR_TEXT_MUTED),
                    trend_badge
                ], spacing=5, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Text(display_val, size=16, weight=ft.FontWeight.BOLD, color=color),
            ], spacing=2),
            col={"xs": 6, "sm": 4, "md": 2},
            padding=ft.padding.symmetric(vertical=10)
        )


    def _format_number(self, value: Any, decimals: int = 0, prefix: str = "") -> str:
        if value is None:
            return "—"
        if prefix == "$":
            return format_currency(value)
        return format_decimal(value, decimals=decimals)

    def _as_number(self, value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _format_compact(self, n: float) -> str:
        if n >= 1_000_000:
            return f"{format_decimal(n / 1_000_000, decimals=1)}M"
        if n >= 1_000:
            return f"{format_decimal(n / 1_000, decimals=1)}K"
        return format_decimal(n, decimals=0)

    def _empty_state(self, section_key: str = "charts", custom_message: str = None) -> ft.Control:
        """Create a visually rich empty state component."""
        title, icon, subtitle = self.EMPTY_STATES.get(section_key, ("Sin datos", ft.icons.INFO_ROUNDED, ""))
        
        if custom_message:
            subtitle = custom_message
        
        return ft.Container(
            content=ft.Column([
                ft.Icon(icon, size=40, color=self.COLOR_TEXT_MUTED, opacity=0.5),
                ft.Text(title, size=14, weight=ft.FontWeight.W_500, color=self.COLOR_TEXT_MUTED),
                ft.Text(subtitle, size=11, color=self.COLOR_TEXT_MUTED, opacity=0.7) if subtitle else ft.Container(),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=8),
            alignment=ft.Alignment(0, 0),
            padding=30,
        )

    def _chart_panel(self, title: str, content: ft.Control, width: int = None, col: Dict[str, int] = None) -> ft.Container:
        """Create a chart panel container. Use col for responsive layouts."""
        container = ft.Container(
            content=ft.Column([
                ft.Text(title, size=12, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT_MUTED),
                content,
            ], spacing=15, tight=True),
            padding=20,
            bgcolor="#F8FAFC",
            border_radius=12,
            border=ft.border.all(1, self.COLOR_BORDER),
        )
        # Apply responsive col if provided, otherwise use width for backwards compat
        if col:
            container.col = col
        elif width:
            container.width = width
        else:
            container.col = {"xs": 12, "md": 6}  # Default responsive
        return container

    def _bar_chart(
        self,
        items: List[Tuple[str, Any]],
        *,
        color: str,
        value_formatter: Callable[[Any], str],
        empty_text: str = None,
    ) -> ft.Control:
        if not items:
            return self._empty_state("charts", empty_text)

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
            return self._empty_state("charts")
        
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
            return self._empty_state("charts", "Valores insuficientes para graficar")

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
            return self._empty_state("ventas", "Sin historial de ventas para este periodo")
        
        # Preparar datos
        values = []
        max_val = 0
        for h in history:
            val = float(h.get('total_ventas') or 0)
            if val > max_val: max_val = val
            values.append((h.get('mes'), val))
        
        if max_val == 0: max_val = 1000
        
        month_map = {
             1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
             7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
        }
        
        weekday_map = {
            0: "Lun", 1: "Mar", 2: "Mié", 3: "Jue", 4: "Vie", 5: "Sáb", 6: "Dom"
        }
        
        # Mapeo de días en inglés a español
        weekday_en_map = {
            "mon": "Lun", "tue": "Mar", "wed": "Mié", "thu": "Jue", 
            "fri": "Vie", "sat": "Sáb", "sun": "Dom"
        }
        
        # Mapeo de meses en inglés a español
        month_en_map = {
            "jan": "Ene", "feb": "Feb", "mar": "Mar", "apr": "Abr", "may": "May", "jun": "Jun",
            "jul": "Jul", "aug": "Ago", "sep": "Sep", "oct": "Oct", "nov": "Nov", "dec": "Dic"
        }

        def _get_date_label(m):
            # Si es un objeto date/datetime
            if hasattr(m, 'day') and hasattr(m, 'month'):
                # Para período Hoy: mostrar hora si tiene atributo hour
                if self.current_period == "Hoy" and hasattr(m, 'hour'):
                    return f"{m.hour}h"
                # Para período semanal/mensual: mostrar día del mes
                if self.current_period in ("Semana", "Mes"):
                    return str(m.day)
                # Para período anual: mostrar mes
                return month_map.get(m.month, str(m.month))
            
            # Si es string, intentar parsear
            m_str = str(m).strip() if m is not None else ""
            
            # Si tiene ":" probablemente es hora (formato 00:, 12:, etc)
            if ":" in m_str:
                hour_part = m_str.split(":")[0]
                try:
                    hour = int(hour_part)
                    return f"{hour}h"
                except:
                    pass
            
            # Detectar si es día de semana en inglés (Wed, Thu, etc)
            m_lower = m_str.lower()[:3]
            if m_lower in weekday_en_map:
                return weekday_en_map[m_lower]
            
            # Detectar si es mes en inglés (Jan, Feb, etc)
            if m_lower in month_en_map:
                return month_en_map[m_lower]
            
            # Intentar parsear como fecha ISO
            try:
                dt = datetime.datetime.fromisoformat(m_str.replace("/", "-"))
                if self.current_period == "Hoy" and hasattr(dt, 'hour'):
                    return f"{dt.hour}h"
                if self.current_period in ("Semana", "Mes"):
                    return str(dt.day)
                return month_map.get(dt.month, str(dt.month))
            except:
                pass
            
            # Si tiene "/" es probablemente un formato de fecha
            if "/" in m_str:
                parts = m_str.split("/")
                # Tomar solo la primera parte (día)
                return parts[0]
            
            # Devolver los primeros 3 caracteres limpios
            return m_str[:3].replace("/", "").replace(":", "")
        
        chart_height = 250
        bar_width = max(30, min(80, 600 // len(values)))  # Ancho dinámico
        
        # Crear barras con gradiente que simulan un área chart
        bars = []
        for i, (mes, val) in enumerate(values):
            ratio = val / max_val if max_val > 0 else 0
            bar_height = max(5, chart_height * ratio)
            tooltip_text = f"{_get_date_label(mes)}: {self._format_number(val, 2, '$')}"
            
            bars.append(
                ft.Container(
                    width=bar_width,
                    height=bar_height,
                    bgcolor=self.COLOR_PRIMARY,
                    border_radius=ft.border_radius.only(top_left=4, top_right=4),
                    gradient=ft.LinearGradient(
                        begin=ft.alignment.top_center,
                        end=ft.alignment.bottom_center,
                        colors=[self.COLOR_PRIMARY, ft.Colors.with_opacity(0.3, self.COLOR_PRIMARY)]
                    ),
                    tooltip=tooltip_text,
                    animate=ft.animation.Animation(300, ft.AnimationCurve.EASE_OUT),
                )
            )
        
        # Etiquetas del eje X
        x_labels = ft.Row(
            controls=[
                ft.Container(
                    width=bar_width,
                    content=ft.Text(_get_date_label(mes), size=9, color=self.COLOR_TEXT_MUTED, text_align=ft.TextAlign.CENTER),
                ) for mes, _ in values
            ],
            spacing=2,
            alignment=ft.MainAxisAlignment.CENTER,
        )
        
        # Etiquetas del eje Y
        y_labels = []
        steps = 4
        for i in range(steps + 1):
            val = (max_val / steps) * (steps - i)
            y_labels.append(
                ft.Text(self._format_compact(val), size=9, color=self.COLOR_TEXT_MUTED)
            )
        
        return ft.Row([
            # Eje Y
            ft.Column(
                controls=y_labels,
                spacing=(chart_height - 40) // steps,
                alignment=ft.MainAxisAlignment.START,
            ),
            # Gráfico
            ft.Column([
                ft.Container(
                    height=chart_height,
                    content=ft.Row(
                        controls=bars,
                        spacing=2,
                        alignment=ft.MainAxisAlignment.CENTER,
                        vertical_alignment=ft.CrossAxisAlignment.END,
                    ),
                    border=ft.Border(bottom=ft.BorderSide(1, self.COLOR_BORDER)),
                ),
                x_labels,
            ], spacing=5, expand=True),
        ], spacing=10, expand=True)

    def _real_bar_chart(self, items: List[Dict[str, Any]], color: str = None) -> ft.Control:
        if not items:
            return self._empty_state("charts")
        
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
                            tooltip=self._format_number(val, 2, "$"),
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
            self._stat_item("Presup. Pénd.", v.get("presupuestos_pend", 0), icon=ft.icons.FORMAT_LIST_BULLETED_ROUNDED),
            self._stat_item("Comprobantes Hoy", v.get("hoy_cant", 0), self.COLOR_INFO, icon=ft.icons.RECEIPT_LONG_ROUNDED),
        ]
        
        if self.role in ("ADMIN", "GERENTE"):
            items.insert(0, self._stat_item("Ventas Semana", self._format_number(v.get('semana_total', 0), 2, "$"), icon=ft.icons.CALENDAR_VIEW_WEEK_ROUNDED))
            items.insert(1, self._stat_item("Ventas Mes", self._format_number(v.get('mes_total', 0), 2, "$"), self.COLOR_PRIMARY, trend=trend, icon=ft.icons.CALENDAR_MONTH_ROUNDED))
            items.insert(2, self._stat_item("Ventas Año", self._format_number(v.get('anio_total', 0), 2, "$"), icon=ft.icons.EQUALIZER_ROUNDED))
            items.append(self._stat_item("Anulados Mes", v.get("anulados_mes", 0), self.COLOR_ERROR, icon=ft.icons.BLOCK_ROUNDED))

        chart_row = ft.ResponsiveRow(spacing=20)
        if self.role in ("ADMIN", "GERENTE"):
            chart_title = "Tendencia de Ventas"
            if self.current_period == "Hoy": chart_title = "Ventas de Hoy (Por Hora)"
            elif self.current_period == "Semana": chart_title = "Tendencia Semanal (Diaria)"
            elif self.current_period == "Mes": chart_title = "Tendencia Mensual (Diaria)"
            elif self.current_period == "Año": chart_title = "Tendencia Anual (Mensual)"

            chart_row.controls.extend([
                self._chart_panel(chart_title, self._line_chart(charts.get("ventas_mensuales", [])), col={"xs": 12, "lg": 12}),
                self._chart_panel("Mix de Documentos", self._pie_chart(v.get("por_tipo", {}), value_formatter=lambda x: f"{int(x)}"), col={"xs": 12, "sm": 6, "lg": 6}),
                self._chart_panel("Participación Formas de Pago", self._pie_chart(v.get("por_forma_pago", {}), value_formatter=lambda x: self._format_number(x, 2, "$")), col={"xs": 12, "sm": 6, "lg": 6}),
            ])
        else:
            chart_row.controls.append(
                self._chart_panel("Mix de Documentos", self._pie_chart(v.get("por_tipo", {}), value_formatter=lambda x: f"{int(x)}"), col={"xs": 12, "md": 6})
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
                self._stat_item("Total Artículos", s.get("total", 0), icon=ft.icons.INVENTORY_ROUNDED),
                self._stat_item("Activos", s.get("activos", 0), self.COLOR_SUCCESS, icon=ft.icons.CHECK_CIRCLE_OUTLINE_ROUNDED),
                self._stat_item("Stock Crítico", s.get("bajo_stock", 0), self.COLOR_WARNING, icon=ft.icons.WARNING_AMBER_ROUNDED),
                self._stat_item("Sin Stock", s.get("sin_stock", 0), self.COLOR_ERROR, icon=ft.icons.DND_FORWARDSLASH_ROUNDED),
                self._stat_item("Valor Inventario", self._format_number(s.get('valor_inventario', 0), 2, "$"), self.COLOR_INFO, icon=ft.icons.MONETIZATION_ON_ROUNDED),
                self._stat_item("Artículos Ingresados", s.get("entradas_mes", 0), icon=ft.icons.ARROW_DOWNWARD_ROUNDED),
                self._stat_item("Artículos Egresados", s.get("salidas_mes", 0), icon=ft.icons.ARROW_UPWARD_ROUNDED),
            ], spacing=10),
            ft.Divider(height=20, color="transparent"),
            ft.ResponsiveRow([
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
                    col={"xs": 12, "lg": 7}
                ),
                self._chart_panel(
                    "Stock por Rubro",
                    self._pie_chart(
                        {r["nombre"]: r["cantidad"] for r in charts.get("stock_por_rubro", [])},
                        value_formatter=lambda x: f"{int(x)} art."
                    ),
                    col={"xs": 12, "lg": 5}
                )
            ], spacing=20)
        ])


    def _build_analitica_section(self) -> ft.Control:
        charts = self.stats.get("charts", {})
        if not charts.get("top_articulos") and not charts.get("bottom_articulos"):
            return None

        return ft.Column([
            ft.ResponsiveRow([
                self._chart_panel(
                    "TOP 5: Lo que más sale (Facturación)",
                    self._real_bar_chart(charts.get("top_articulos", [])),
                    col={"xs": 12, "lg": 6}
                ) if charts.get("top_articulos") else ft.Container(),
                self._chart_panel(
                    "BOTTOM 5: Lo que menos sale (Ventas Mes)",
                    self._real_bar_chart(charts.get("bottom_articulos", []), color=self.COLOR_WARNING),
                    col={"xs": 12, "lg": 6}
                ) if charts.get("bottom_articulos") else ft.Container(),
            ], spacing=20),
        ])

    def _build_entidades_section(self) -> ft.Control:
        e = self.stats.get("entidades", {})
        charts = self.stats.get("charts", {})
        return ft.Column([
            ft.ResponsiveRow([
                self._stat_item("Clientes Totales", e.get("clientes_total", 0), icon=ft.icons.PEOPLE_ROUNDED),
                self._stat_item("Proveedores", e.get("proveedores_total", 0), icon=ft.icons.BUSINESS_CENTER_ROUNDED),
                self._stat_item("Nuevos (Mes)", e.get("nuevos_mes", 0), self.COLOR_SUCCESS, icon=ft.icons.PERSON_ADD_ROUNDED),
                self._stat_item("Deuda Clientes", self._format_number(e.get('deuda_clientes', 0), 2, "$"), self.COLOR_ERROR, icon=ft.icons.ACCOUNT_BALANCE_WALLET_ROUNDED),
                self._stat_item("Cant. Deudores", e.get("deudores_cant", 0), icon=ft.icons.PERSON_SEARCH_ROUNDED),
            ], spacing=10),
            ft.Divider(height=20, color="transparent"),
            ft.ResponsiveRow([
                self._chart_panel(
                    "Composición de Base",
                    self._pie_chart(
                        {r["nombre"]: r["cantidad"] for r in charts.get("entidades_por_tipo", [])},
                        colors=[self.COLOR_INFO, self.COLOR_PRIMARY, self.COLOR_WARNING],
                        value_formatter=lambda x: f"{int(x)}"
                    ),
                    col={"xs": 12, "md": 6}
                ),
                self._chart_panel(
                    "Estado de Deuda",
                    self._pie_chart({
                        "Deudores": e.get("deudores_cant", 0),
                        "Al día": (self._as_number(e.get("clientes_total", 0)) - self._as_number(e.get("deudores_cant", 0)))
                    }, colors=[self.COLOR_ERROR, self.COLOR_SUCCESS], value_formatter=lambda x: f"{int(x)}"),
                    col={"xs": 12, "md": 6}
                )
            ], spacing=20)
        ])

    def _build_operativa_section(self) -> ft.Control:
        o = self.stats.get("operativas", {})
        m = self.stats.get("movimientos", {})
        
        stat_items = [
            self._stat_item("Entregas Hoy", o.get("entregas_hoy", 0), self.COLOR_SUCCESS, icon=ft.icons.LOCAL_SHIPPING_ROUNDED),
        ]
        if self.role == "ADMIN":
             stat_items.append(self._stat_item("Actividad Sistema", o.get("actividad_sistema", 0), self.COLOR_INFO, icon=ft.icons.HISTORY_ROUNDED))
             
        stat_items.extend([
            self._stat_item("Mov. Ingresos", m.get("ingresos", 0), self.COLOR_SUCCESS, icon=ft.icons.ADD_BOX_ROUNDED),
            self._stat_item("Mov. Salidas", m.get("salidas", 0), self.COLOR_WARNING, icon=ft.icons.INDETERMINATE_CHECK_BOX_ROUNDED),
            self._stat_item("Ajustes", m.get("ajustes", 0), self.COLOR_ERROR, icon=ft.icons.EDIT_NOTE_ROUNDED),
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
            ft.ResponsiveRow([
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
                    col={"xs": 12, "md": 6}
                ),
                self._chart_panel(
                    "Actividad Operativa (Hoy)",
                    self._bar_chart(
                        chart_items,
                        color=self.COLOR_INFO,
                        value_formatter=lambda v: self._format_number(v),
                        empty_text="Sin actividad hoy",
                    ),
                    col={"xs": 12, "md": 6}
                ),
            ], spacing=20),
        ])

    def _build_finanzas_section(self) -> ft.Control:
        f = self.stats.get("finanzas", {})
        return ft.Column([
            ft.ResponsiveRow([
                self._stat_item("Ingresos Hoy", self._format_number(f.get('ingresos_hoy', 0), 2, "$"), self.COLOR_SUCCESS, icon=ft.icons.ACCOUNT_BALANCE_ROUNDED),
                self._stat_item("Ingresos Mes", self._format_number(f.get('ingresos_mes', 0), 2, "$"), icon=ft.icons.TRENDING_UP_ROUNDED),
                self._stat_item("Egresos Mes", self._format_number(f.get('egresos_mes', 0), 2, "$"), self.COLOR_ERROR, icon=ft.icons.TRENDING_DOWN_ROUNDED),
                self._stat_item("Balance Mes", self._format_number(f.get('balance_mes', 0), 2, "$"), self.COLOR_PRIMARY, icon=ft.icons.CURRENCY_EXCHANGE_ROUNDED),
                self._stat_item("IVA Est. Mes", self._format_number(f.get('iva_estimado', 0), 2, "$"), self.COLOR_INFO, icon=ft.icons.ASSURED_WORKLOAD_ROUNDED),
            ], spacing=10),
            ft.Divider(height=20, color="transparent"),
            ft.ResponsiveRow([
                self._chart_panel(
                    "Ingresos vs Egresos (Mes)",
                    self._pie_chart({
                        "Ingresos": f.get("ingresos_mes", 0),
                        "Egresos": f.get("egresos_mes", 0)
                    }, colors=[self.COLOR_SUCCESS, self.COLOR_ERROR], value_formatter=lambda x: self._format_number(x, 2, "$")),
                    col={"xs": 12, "md": 5}
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
                    col={"xs": 12, "md": 7}
                )
            ], spacing=20)
        ])

    def _build_sistema_section(self) -> ft.Control:
        sis = self.stats.get("sistema", {})
        return ft.ResponsiveRow([
            self._stat_item("Usuarios Activos", sis.get("usuarios_activos", 0), icon=ft.icons.PEOPLE_OUTLINE_ROUNDED),
            self._stat_item("Errores Mes", sis.get("errores_mes", 0), self.COLOR_ERROR, icon=ft.icons.BUG_REPORT_ROUNDED),
            self._stat_item("Backups Ok", sis.get("backups_mes", 0), self.COLOR_SUCCESS, icon=ft.icons.BACKUP_ROUNDED),
            self._stat_item("Última Actividad", self.stats.get("operativas", {}).get("actividad_sistema", 0), self.COLOR_INFO, icon=ft.icons.MONITOR_HEART_ROUNDED),
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
