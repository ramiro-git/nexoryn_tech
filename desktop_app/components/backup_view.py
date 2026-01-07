import os
import sys
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime, timedelta
import flet as ft
from pathlib import Path

# Agregar parent directory al path para importar servicios nuevos
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class BackupView:
    def __init__(self, page: ft.Page, backup_service, show_message_callback: Callable, set_connection_callback: Callable):
        self.page = page
        self.backup_service = backup_service
        self.show_message = show_message_callback
        self.set_connection = set_connection_callback
        self.data: List[Dict[str, Any]] = []

        # Colores
        self.COLOR_CARD = "#FFFFFF"
        self.COLOR_BORDER = "#E2E8F0"
        self.COLOR_PRIMARY = "#4F46E5"
        self.COLOR_TEXT = "#0F172A"
        self.COLOR_TEXT_MUTED = "#64748B"
        self.COLOR_SUCCESS = "#10B981"
        self.COLOR_WARNING = "#F59E0B"
        self.COLOR_ERROR = "#EF4444"
        self.COLOR_INFO = "#3B82F6"

        # Colores por tipo de backup profesional
        self.TYPE_COLORS = {
            "FULL": "#10B981",        # Green
            "DIFERENCIAL": "#3B82F6", # Blue
            "INCREMENTAL": "#F59E0B", # Amber
            "MANUAL": "#8B5CF6",      # Purple
            "daily": "#3B82F6",       # Blue (legacy)
            "weekly": "#8B5CF6",      # Purple (legacy)
            "monthly": "#0D9488",     # Teal (legacy)
            "manual": "#10B981",      # Green (legacy)
        }

        # Estado
        self.use_professional_mode = True
        self.data_loaded = False

        # Métricas principales
        self.total_backups_text = ft.Text("—", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT)
        self.last_backup_text = ft.Text("—", size=18, color=self.COLOR_TEXT_MUTED)
        self.next_backup_text = ft.Text("—", size=18, color=self.COLOR_INFO, weight=ft.FontWeight.BOLD)

        # File pickers
        self.folder_picker = ft.FilePicker(on_result=self._on_folder_selected)
        self.sync_folder_picker = ft.FilePicker(on_result=self._on_sync_folder_selected)
        page.overlay.append(self.folder_picker)
        page.overlay.append(self.sync_folder_picker)

        self._setup_view()

    def _format_size(self, size_bytes: float) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"

    def _format_datetime(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _format_time_until(self, dt: datetime) -> str:
        now = datetime.now()
        diff = dt - now

        if diff.days > 0:
            return f"en {diff.days} días"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"en {hours} horas"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"en {minutes} minutos"
        else:
            return "próximamente"

    def _setup_view(self):
        # Tarjetas de programación
        self.schedule_cards_container = ft.Column([], spacing=12)

        # Tabla de backups
        self.backups_table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Tipo", size=12, weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Archivo", size=12, weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Tamaño", size=12, weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Fecha", size=12, weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Estado", size=12, weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Acciones", size=12, weight=ft.FontWeight.BOLD)),
            ],
            rows=[],
            border_radius=10,
            border=ft.border.all(1, self.COLOR_BORDER),
            heading_row_color=ft.Colors.with_opacity(0.05, self.COLOR_PRIMARY),
            data_row_max_height=60,
        )

        # Tabs para diferentes secciones
        self.tabs = ft.Tabs(
            selected_index=0,
            animation_duration=300,
            tabs=[
                ft.Tab(
                    text="Dashboard",
                    icon=ft.Icons.DASHBOARD_ROUNDED,
                    content=self._build_dashboard_tab()
                ),
                ft.Tab(
                    text="Configuración",
                    icon=ft.Icons.SETTINGS_ROUNDED,
                    content=self._build_config_tab()
                ),
                ft.Tab(
                    text="Historial",
                    icon=ft.Icons.HISTORY_ROUNDED,
                    content=self._build_history_tab()
                ),
            ],
            expand=1
        )

    def _build_dashboard_tab(self) -> ft.Control:
        return ft.Column([
            # Header
            ft.Row([
                ft.Column([
                    ft.Text("Sistema de Backups Profesionales", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Text("FULL + DIFERENCIAL + INCREMENTAL - Restauración concatenable", size=12, color=self.COLOR_TEXT_MUTED),
                ], spacing=2),
                ft.Row([
                    ft.ElevatedButton(
                        "Crear Backup",
                        icon=ft.Icons.ADD_ROUNDED,
                        bgcolor=self.COLOR_PRIMARY,
                        color=ft.Colors.WHITE,
                        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                        on_click=self._trigger_backup
                    ),
                ], spacing=8)
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),

            # Métricas
            ft.Row([
                self._metric_card("Total Backups", self.total_backups_text, ft.Icons.FOLDER_SPECIAL_ROUNDED, self.COLOR_PRIMARY),
                self._metric_card("Último Backup", self.last_backup_text, ft.Icons.ACCESS_TIME_ROUNDED, self.COLOR_SUCCESS),
                self._metric_card("Próximo Backup", self.next_backup_text, ft.Icons.SCHEDULE_ROUNDED, self.COLOR_INFO),
            ], spacing=12),

            # Acciones rápidas
            ft.Container(
                content=ft.Column([
                    ft.Text("Ejecutar Backup Ahora", size=16, weight=ft.FontWeight.BOLD),
                    ft.Row([
                        self._action_button("FULL", ft.Icons.CALENDAR_MONTH_ROUNDED, self.COLOR_SUCCESS, lambda e: self._execute_backup('FULL')),
                        self._action_button("DIFERENCIAL", ft.Icons.DATE_RANGE_ROUNDED, self.COLOR_INFO, lambda e: self._execute_backup('DIFERENCIAL')),
                        self._action_button("INCREMENTAL", ft.Icons.TODAY_ROUNDED, self.COLOR_WARNING, lambda e: self._execute_backup('INCREMENTAL')),
                    ], spacing=8),
                ], spacing=12),
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=12,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),

            # Programación automática
            ft.Container(
                content=ft.Column([
                    ft.Row([
                        ft.Icon(ft.Icons.EVENT_ROUNDED, color=self.COLOR_PRIMARY, size=20),
                        ft.Text("Programación Automática", size=16, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ], spacing=8),
                    ft.Container(height=8),
                    self.schedule_cards_container,
                ]),
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=12,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),

            # Tabla de backups recientes
            ft.Container(
                content=ft.Column([
                    ft.Text("Backups Recientes", size=16, weight=ft.FontWeight.BOLD),
                    self.backups_table,
                ], spacing=12),
                expand=True,
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=12,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),
        ], expand=True, spacing=12)

    def _build_config_tab(self) -> ft.Control:
        return ft.Column([
            ft.Text("Configuración de Backups", size=18, weight=ft.FontWeight.BOLD),
            ft.Divider(),

            ft.Container(
                content=ft.Column([
                    ft.Text("Sistema de Backups Profesionales Activado", size=16, weight=ft.FontWeight.BOLD, color=self.COLOR_SUCCESS),
                    ft.Text("• Backup FULL: Día 1 de cada mes a las 00:00", size=12),
                    ft.Text("• Backup DIFERENCIAL: Domingos a las 23:30", size=12),
                    ft.Text("• Backup INCREMENTAL: Diariamente a las 23:00", size=12),
                    ft.Text("• Validación automática: Diariamente a las 01:00", size=12),
                ], spacing=8),
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=12,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),

            ft.Container(
                content=ft.Column([
                    ft.Text("Configuración de Nube", size=16, weight=ft.FontWeight.BOLD),
                    ft.Text("Para configurar sincronización con Google Drive o S3, contactar al administrador.", size=12, color=self.COLOR_TEXT_MUTED),
                ], spacing=8),
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=12,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),
        ], expand=True, spacing=12)

    def _build_history_tab(self) -> ft.Control:
        return ft.Column([
            ft.Text("Historial Completo de Backups", size=18, weight=ft.FontWeight.BOLD),
            ft.Divider(),

            ft.Container(
                content=self.backups_table,
                expand=True,
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=12,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),
        ], expand=True, spacing=12)

    def load_data(self):
        try:
            self.data_loaded = False

            # Cargar datos del sistema existente
            self.data = self.backup_service.list_backups()

            if self.data:
                self.total_backups_text.value = str(len(self.data))
                latest = self.data[0]['created'].strftime("%Y-%m-%d %H:%M")
                self.last_backup_text.value = latest
            else:
                self.total_backups_text.value = "0"
                self.last_backup_text.value = "N/A"

            # Próximo backup (simulado)
            self.next_backup_text.value = "Sistema automático"

            # Cargar tabla
            self._load_backups_table()

            # Cargar tarjetas de programación
            self._update_schedule_cards()

            self.data_loaded = True

        except Exception as e:
            self.show_message(f"Error cargando datos: {str(e)}", "error")
        finally:
            self.page.update()

    def _load_backups_table(self):
        rows = []
        for backup in self.data[:50]:
            tipo_badge = self._get_backup_type_badge(backup.get('type', 'manual'))
            estado_badge = self._get_status_badge('COMPLETADO')

            row = ft.DataRow(
                cells=[
                    ft.DataCell(tipo_badge),
                    ft.DataCell(ft.Text(backup['name'], size=12)),
                    ft.DataCell(ft.Text(self._format_size(backup['size']), size=12)),
                    ft.DataCell(ft.Text(self._format_datetime(backup['created']), size=12)),
                    ft.DataCell(estado_badge),
                    ft.DataCell(
                        ft.Row([
                            ft.IconButton(
                                ft.Icons.RESTORE,
                                icon_color=self.COLOR_WARNING,
                                tooltip="Restaurar backup",
                                on_click=lambda e, p=backup['path']: self._confirm_restore(p)
                            ),
                            ft.IconButton(
                                ft.Icons.DELETE,
                                icon_color=self.COLOR_ERROR,
                                tooltip="Eliminar backup",
                                on_click=lambda e, p=backup['path']: self._delete_backup(p)
                            ),
                        ], spacing=4)
                    ),
                ]
            )
            rows.append(row)

        self.backups_table.rows = rows

    def _update_schedule_cards(self):
        # Tarjetas simuladas para el sistema profesional
        cards = []
        for backup_type, info in [("FULL", {"schedule": "Dia 1 a las 00:00", "next_run": datetime.now() + timedelta(days=1)}),
                                  ("DIFERENCIAL", {"schedule": "Domingos a las 23:30", "next_run": datetime.now() + timedelta(hours=2)}),
                                  ("INCREMENTAL", {"schedule": "Diario a las 23:00", "next_run": datetime.now() + timedelta(hours=1)})]:
            color = self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY)
            labels = {"FULL": "FULL", "DIFERENCIAL": "DIFERENCIAL", "INCREMENTAL": "INCREMENTAL"}
            icons = {"FULL": ft.Icons.CALENDAR_MONTH_ROUNDED, "DIFERENCIAL": ft.Icons.DATE_RANGE_ROUNDED, "INCREMENTAL": ft.Icons.TODAY_ROUNDED}

            time_until = self._format_time_until(info["next_run"])

            card = ft.Container(
                content=ft.Row([
                    ft.Container(
                        content=ft.Icon(icons.get(backup_type, ft.Icons.BACKUP_ROUNDED), color=color, size=22),
                        bgcolor=f"{color}1A",
                        padding=10,
                        border_radius=10,
                    ),
                    ft.Column([
                        ft.Text(labels.get(backup_type, backup_type), size=14, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                        ft.Text(info["schedule"], size=11, color=self.COLOR_TEXT_MUTED),
                    ], spacing=0, expand=True),
                    ft.Container(
                        content=ft.Text(time_until, size=12, weight=ft.FontWeight.BOLD, color=color),
                        bgcolor=f"{color}1A",
                        padding=ft.padding.symmetric(horizontal=12, vertical=6),
                        border_radius=16,
                    ),
                ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                padding=12,
                bgcolor=self.COLOR_CARD,
                border=ft.border.all(1, self.COLOR_BORDER),
                border_radius=12,
            )
            cards.append(card)

        self.schedule_cards_container.controls = cards

    def _get_backup_type_badge(self, tipo: str) -> ft.Container:
        colors = self.TYPE_COLORS

        labels = {
            'FULL': 'FULL',
            'DIFERENCIAL': 'DIF',
            'INCREMENTAL': 'INC',
            'MANUAL': 'Manual',
            "daily": "Diario",
            "weekly": "Semanal",
            "monthly": "Mensual",
            "manual": "Manual"
        }

        color = colors.get(tipo, self.COLOR_TEXT_MUTED)
        label = labels.get(tipo, tipo)

        return ft.Container(
            content=ft.Text(label, size=11, weight=ft.FontWeight.BOLD, color=color),
            padding=ft.padding.symmetric(horizontal=12, vertical=4),
            border_radius=10,
            bgcolor=f"{color}1A"
        )

    def _get_status_badge(self, estado: str) -> ft.Container:
        colors = {
            'COMPLETADO': self.COLOR_SUCCESS,
            'EN_PROGRESO': self.COLOR_INFO,
            'FALLIDO': self.COLOR_ERROR,
            'PENDIENTE': self.COLOR_WARNING
        }

        color = colors.get(estado, self.COLOR_TEXT_MUTED)

        return ft.Container(
            content=ft.Text(estado, size=11, weight=ft.FontWeight.BOLD, color=color),
            padding=ft.padding.symmetric(horizontal=8, vertical=4),
            border_radius=8,
            bgcolor=f"{color}1A"
        )

    def _execute_backup(self, backup_type: str):
        def run_backup():
            try:
                # Mapear tipos profesionales a tipos legacy
                type_mapping = {
                    'FULL': 'monthly',
                    'DIFERENCIAL': 'weekly',
                    'INCREMENTAL': 'daily'
                }

                legacy_type = type_mapping.get(backup_type, 'manual')
                self.backup_service.create_backup(legacy_type)

                self.show_message(f"Backup {backup_type} creado exitosamente.", "success")
                self.load_data()

            except Exception as e:
                self.show_message(f"Error creando backup: {str(e)}", "error")

        import threading
        threading.Thread(target=run_backup, daemon=True).start()

    def _confirm_restore(self, path):
        def on_confirm(e):
            self.page.close(dlg)
            self._perform_restore(path)

        def on_cancel(e):
            self.page.close(dlg)

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar Restauración"),
            content=ft.Text("¿Estás seguro? Esto SOBREESCRIBIRÁ la base de datos actual.\nEsta acción no se puede deshacer."),
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton("Restaurar", on_click=on_confirm, bgcolor=self.COLOR_WARNING, color=ft.Colors.WHITE),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)

    def _perform_restore(self, path):
        try:
            self.show_message("Restaurando base de datos... Por favor espera.", "info")
            self.page.update()

            self.backup_service.restore_backup(path)
            self.show_message("Restauración completada. Reiniciá la aplicación si es necesario.", "success")
        except Exception as exc:
            self.show_message(f"Falló la restauración: {exc}", "error")

    def _delete_backup(self, path):
        def on_confirm(e):
            self.page.close(dlg)
            try:
                import pathlib
                p = pathlib.Path(path)
                if p.exists():
                    p.unlink()
                    self.show_message("Backup eliminado.", "success")
                    self.load_data()
            except Exception as exc:
                self.show_message(f"Error eliminando: {exc}", "error")

        def on_cancel(e):
            self.page.close(dlg)

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Eliminar backup"),
            content=ft.Text("¿Estás seguro que deseas eliminar este backup? Esta acción no se puede deshacer."),
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton("Eliminar", on_click=on_confirm, bgcolor=self.COLOR_ERROR, color=ft.Colors.WHITE),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)

    def _trigger_backup(self, e):
        def create_backup_option(backup_type: str, label: str, icon: str):
            color = self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY)
            return ft.ListTile(
                leading=ft.Icon(icon, color=color),
                title=ft.Text(label),
                on_click=lambda e, t=backup_type: (self.page.close(bottom_sheet), self._execute_backup(t))
            )

        bottom_sheet = ft.BottomSheet(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Seleccionar tipo de backup", size=16, weight=ft.FontWeight.BOLD),
                    ft.Divider(),
                    create_backup_option("FULL", "Backup FULL (Completo)", ft.Icons.CALENDAR_MONTH_ROUNDED),
                    create_backup_option("DIFERENCIAL", "Backup DIFERENCIAL (Semanal)", ft.Icons.DATE_RANGE_ROUNDED),
                    create_backup_option("INCREMENTAL", "Backup INCREMENTAL (Diario)", ft.Icons.TODAY_ROUNDED),
                    create_backup_option("MANUAL", "Backup Manual", ft.Icons.SAVE_ROUNDED),
                ], tight=True),
                padding=20,
            ),
        )
        self.page.open(bottom_sheet)

    def _on_folder_selected(self, e: ft.FilePickerResultEvent):
        if e.path:
            success = self.backup_service.set_backup_dir(e.path)
            if success:
                self.show_message(f"Carpeta de backups cambiada a: {e.path}", "success")
                self.load_data()
            else:
                self.show_message("No se pudo cambiar la carpeta de backups", "error")

    def _on_sync_folder_selected(self, e: ft.FilePickerResultEvent):
        if e.path:
            success = self.backup_service.set_sync_dir(e.path)
            if success:
                self.show_message(f"Sincronización automática activada: {e.path}", "success")
            else:
                self.show_message("No se pudo configurar la carpeta de sincronización", "error")

    def build(self) -> ft.Control:
        return ft.Column([
            self.tabs,
        ], expand=True, spacing=0)

    def _metric_card(self, title: str, value_text: ft.Text, icon: str, color: Optional[str] = None) -> ft.Container:
        if color is None:
            color = self.COLOR_PRIMARY
        return ft.Container(
            content=ft.Row([
                ft.Container(
                    width=48,
                    height=48,
                    border_radius=12,
                    bgcolor=f"{color}1A",
                    alignment=ft.alignment.center,
                    content=ft.Icon(icon, color=color, size=24),
                ),
                ft.Column([
                    ft.Text(title, size=12, color=self.COLOR_TEXT_MUTED),
                    value_text,
                ], spacing=2),
            ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            padding=16,
            bgcolor=self.COLOR_CARD,
            border=ft.border.all(1, self.COLOR_BORDER),
            border_radius=12,
            expand=True,
        )

    def _action_button(self, label: str, icon, color: str, on_click) -> ft.Container:
        return ft.Container(
            content=ft.Column([
                ft.Icon(icon, color=color, size=32),
                ft.Text(label, size=11, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                ft.Text("One-click", size=10, color=self.COLOR_TEXT_MUTED),
            ], spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            padding=ft.padding.symmetric(horizontal=20, vertical=16),
            border_radius=12,
            bgcolor=f"{color}1A",
            border=ft.border.all(2, color),
            on_click=on_click,
            expand=True,
        )