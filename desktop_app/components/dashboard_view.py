import flet as ft
import datetime
import threading
import time
from typing import Dict, Any, List, Optional
from desktop_app.database import Database

class DashboardView(ft.Container):
    def __init__(self, database: Database, user_role: str = "EMPLEADO"):
        super().__init__()
        self.db = database
        self.role = user_role.upper()
        self.stats: Dict[str, Any] = {}
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
            ft.Icons.REFRESH_ROUNDED, 
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
        self.is_loading = True
        try:
            if self.page: self.update()
        except: pass
        
        try:
            # Fetch 100+ stats in one batch filtered by role
            self.stats = self.db.get_full_dashboard_stats(self.role)
            self.last_updated_text.value = f"Última actualización: {datetime.datetime.now().strftime('%H:%M:%S')}"
        except Exception as e:
            print(f"Error loading dashboard stats: {e}")
            
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
                        ft.Icon(ft.Icons.PERSON_ROUNDED, size=14, color=self.COLOR_TEXT_MUTED),
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
        self.kpi_row.controls.clear()
        
        # Common KPIs
        v_hoy = self.stats.get("ventas", {}).get("hoy_total", "—")
        v_cant = self.stats.get("ventas", {}).get("hoy_cant", 0)
        s_bajo = self.stats.get("stock", {}).get("bajo_stock", 0)
        r_pend = self.stats.get("operativas", {}).get("remitos_pend", 0)
        
        self.kpi_row.controls.extend([
            self._kpi_card("Ventas Hoy", f"${v_hoy}" if isinstance(v_hoy, (float, int)) else v_hoy, ft.Icons.ATTACH_MONEY_ROUNDED, self.COLOR_SUCCESS, f"{v_cant} oper."),
            self._kpi_card("Stock Bajo", str(s_bajo), ft.Icons.INVENTORY_2_ROUNDED, self.COLOR_WARNING if s_bajo > 0 else self.COLOR_INFO, "Requiere acción" if s_bajo > 0 else "Al día"),
            self._kpi_card("Remitos Pend.", str(r_pend), ft.Icons.LOCAL_SHIPPING_ROUNDED, self.COLOR_PRIMARY, "Por entregar"),
            self._kpi_card("Docs Hoy", str(self.stats.get("operativas", {}).get("mis_operaciones_hoy", 0)), ft.Icons.DESCRIPTION_ROUNDED, self.COLOR_INFO, "Mis registros")
        ])

        # 2. Category Sections
        self.sections_column.controls.clear()
        
        # Ventas Section
        self.sections_column.controls.append(
            self._section_container("VENTAS", ft.Icons.SHOPPING_CART_ROUNDED, self._build_ventas_section())
        )
        
        # Stock Section
        self.sections_column.controls.append(
            self._section_container("STOCK e INVENTARIO", ft.Icons.INVENTORY_ROUNDED, self._build_stock_section())
        )
        
        # Entidades Section
        self.sections_column.controls.append(
            self._section_container("CLIENTES y PROVEEDORES", ft.Icons.PEOPLE_ROUNDED, self._build_entidades_section())
        )
        
        # Financial Section (Gerente/Admin)
        if self.role in ("ADMIN", "GERENTE") and "finanzas" in self.stats:
            self.sections_column.controls.append(
                self._section_container("FINANZAS Y CAJA", ft.Icons.ACCOUNT_BALANCE_WALLET_ROUNDED, self._build_finanzas_section())
            )
            
        # System Section (Admin only)
        if self.role == "ADMIN" and "sistema" in self.stats:
            self.sections_column.controls.append(
                self._section_container("SISTEMA y SEGURIDAD", ft.Icons.SECURITY_ROUNDED, self._build_sistema_section())
            )

    def _kpi_card(self, title: str, value: str, icon: str, color: str, subtitle: str) -> ft.Container:
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(icon, color=color, size=24),
                    ft.Text(title, size=14, color=self.COLOR_TEXT_MUTED, weight=ft.FontWeight.W_500),
                ], alignment=ft.MainAxisAlignment.START, spacing=10),
                ft.Text(value, size=28, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                ft.Text(subtitle, size=13, color=color, weight=ft.FontWeight.W_600),
            ], spacing=5, alignment=ft.MainAxisAlignment.CENTER),
            width=230,
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER),
            shadow=ft.BoxShadow(blur_radius=10, color="#0000000D", offset=ft.Offset(0, 4))
        )

    def _section_container(self, title: str, icon: str, content: ft.Control) -> ft.Container:
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(icon, color=self.COLOR_PRIMARY, size=20),
                    ft.Text(title, size=16, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Container(expand=True),
                    ft.Text("Ver detalles", size=12, color=self.COLOR_PRIMARY, weight=ft.FontWeight.W_600),
                ], alignment=ft.MainAxisAlignment.START, spacing=10),
                ft.Divider(height=1, color=self.COLOR_BORDER),
                ft.Container(content=content, padding=ft.padding.only(top=10))
            ], spacing=5),
            padding=20,
            bgcolor=self.COLOR_CARD,
            border_radius=15,
            border=ft.border.all(1, self.COLOR_BORDER)
        )

    def _stat_item(self, label: str, value: Any, color: str = None) -> ft.Control:
        if color is None: color = self.COLOR_TEXT
        return ft.Column([
            ft.Text(label, size=12, color=self.COLOR_TEXT_MUTED),
            ft.Text(str(value), size=16, weight=ft.FontWeight.BOLD, color=color),
        ], spacing=2)

    def _build_ventas_section(self) -> ft.Control:
        v = self.stats.get("ventas", {})
        return ft.Column([
            ft.Row([
                self._stat_item("Ventas Semana", f"${v.get('semana_total', 0)}"),
                self._stat_item("Ventas Mes", f"${v.get('mes_total', 0)}", self.COLOR_PRIMARY),
                self._stat_item("Ventas Año", f"${v.get('anio_total', 0)}"),
                self._stat_item("Presup. Pénd.", v.get("presupuestos_pend", 0)),
                self._stat_item("Anulados Mes", v.get("anulados_mes", 0), self.COLOR_ERROR),
            ], wrap=True, spacing=40),
            ft.Container(height=10),
            ft.Row([
                ft.Column([
                    ft.Text("Ventas por Tipo (Mes)", size=12, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT_MUTED),
                    ft.Row([
                        self._badge(k, str(v)) for k, v in v.get("por_tipo", {}).items()
                    ], wrap=True)
                ], expand=True),
                ft.Column([
                    ft.Text("Por Forma de Pago", size=12, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT_MUTED),
                    ft.Row([
                        self._badge(k, f"${v}") for k, v in v.get("por_forma_pago", {}).items()
                    ], wrap=True)
                ], expand=True)
            ], spacing=20)
        ])

    def _build_stock_section(self) -> ft.Control:
        s = self.stats.get("stock", {})
        return ft.Row([
            self._stat_item("Total Artículos", s.get("total", 0)),
            self._stat_item("Activos", s.get("activos", 0), self.COLOR_SUCCESS),
            self._stat_item("Bajo Stock", s.get("bajo_stock", 0), self.COLOR_WARNING),
            self._stat_item("Sin Stock", s.get("sin_stock", 0), self.COLOR_ERROR),
            self._stat_item("Valor Inventario", f"${s.get('valor_costo', 0)}", self.COLOR_INFO),
            self._stat_item("Ingresos Mes", s.get("entradas_mes", 0)),
            self._stat_item("Salidas Mes", s.get("salidas_mes", 0)),
        ], wrap=True, spacing=40)

    def _build_entidades_section(self) -> ft.Control:
        e = self.stats.get("entidades", {})
        return ft.Row([
            self._stat_item("Clientes Totales", e.get("clientes_total", 0)),
            self._stat_item("Proveedores", e.get("proveedores_total", 0)),
            self._stat_item("Nuevos (Mes)", e.get("nuevos_mes", 0), self.COLOR_SUCCESS),
            self._stat_item("Deuda Clientes", f"${e.get('deuda_clientes', 0)}", self.COLOR_ERROR),
            self._stat_item("Cant. Deudores", e.get("deudores_cant", 0)),
        ], wrap=True, spacing=40)

    def _build_finanzas_section(self) -> ft.Control:
        f = self.stats.get("finanzas", {})
        return ft.Row([
            self._stat_item("Ingresos Hoy", f"${f.get('ingresos_hoy', 0)}", self.COLOR_SUCCESS),
            self._stat_item("Ingresos Mes", f"${f.get('ingresos_mes', 0)}"),
            self._stat_item("Egresos Mes", f"${f.get('egresos_mes', 0)}", self.COLOR_ERROR),
            self._stat_item("Balance Mes", f"${f.get('balance_mes', 0)}", self.COLOR_PRIMARY),
            self._stat_item("IVA Est. Mes", f"${f.get('iva_estimado', 0)}", self.COLOR_INFO),
        ], wrap=True, spacing=40)

    def _build_sistema_section(self) -> ft.Control:
        sis = self.stats.get("sistema", {})
        return ft.Row([
            self._stat_item("Usuarios Activos", sis.get("usuarios_activos", 0)),
            self._stat_item("Errores Mes", sis.get("errores_mes", 0), self.COLOR_ERROR),
            self._stat_item("Backups Ok", sis.get("backups_mes", 0), self.COLOR_SUCCESS),
            self._stat_item("Última Actividad", self.stats.get("operativas", {}).get("actividad_sistema", 0), self.COLOR_INFO),
        ], wrap=True, spacing=40)

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
