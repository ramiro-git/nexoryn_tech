import os
import sys
from typing import List, Optional, Dict, Any, Callable, Tuple
from datetime import datetime, timedelta
import flet as ft
from pathlib import Path

# Agregar parent directory al path para importar servicios nuevos
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from desktop_app.components.generic_table import GenericTable, ColumnConfig, SimpleFilterConfig, AdvancedFilterControl
except ImportError:
    # Adjust import based on environment
    from desktop_app.components.generic_table import GenericTable, ColumnConfig, SimpleFilterConfig, AdvancedFilterControl

try:
    from desktop_app.components.button_styles import cancel_button
except ImportError:
    from components.button_styles import cancel_button

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
        self.folder_picker = ft.FilePicker()
        self.folder_picker.on_result = self._on_folder_selected
        self.sync_folder_picker = ft.FilePicker()
        self.sync_folder_picker.on_result = self._on_sync_folder_selected
        page.overlay.append(self.folder_picker)
        page.overlay.append(self.sync_folder_picker)

        self._setup_view()

    def _format_size(self, size_bytes: float) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"

    def _format_datetime(self, dt: Any) -> str:
        if isinstance(dt, str): return dt
        if not dt: return ""
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

    def _backup_provider(self, offset: int, limit: int, search: Optional[str], simple: Optional[str], advanced: Dict[str, Any], sorts: List[Tuple[str, str]]) -> Tuple[List[Dict[str, Any]], int]:
        # Always fetch fresh data to ensure we are up to date with FS
        all_data = self.backup_service.list_backups()
        self.data = all_data # Keep a reference for metrics

        filtered = all_data

        # Filter by search (Name)
        if search:
            s = search.lower()
            filtered = [b for b in filtered if s in b['name'].lower()]

        # Filter by simple (Type)
        if simple and simple != "Todos" and simple is not None:
             filtered = [b for b in filtered if b.get('type') == simple]

        # Filter by date range
        if advanced:
            date_from = advanced.get("date_from")
            date_to = advanced.get("date_to")
            
            def parse_date(d_str):
                try:
                    return datetime.strptime(d_str, "%Y-%m-%d")
                except:
                    return None

            if date_from or date_to:
                d_from = parse_date(date_from) if date_from else None
                d_to = parse_date(date_to) if date_to else None
                
                new_filtered = []
                for b in filtered:
                    created = b.get('created')
                    if not isinstance(created, datetime):
                        new_filtered.append(b)
                        continue
                        
                    # Normalize to date for comparison
                    b_date = created.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    if d_from and b_date < d_from:
                        continue
                    if d_to and b_date > d_to:
                        continue
                    new_filtered.append(b)
                filtered = new_filtered

        # Sort
        if sorts:
            key, direction = sorts[0]
            reverse = (direction == "desc")
            # Handle specific keys if needed, assuming keys match data dict
            filtered.sort(key=lambda x: x.get(key, ""), reverse=reverse)
        else:
            # Default sort desc by date
            filtered.sort(key=lambda x: x.get('created', datetime.min), reverse=True)

        # Paginate
        total = len(filtered)
        paged = filtered[offset : offset + limit]
        
        return paged, total

    def _create_date_input(self, label: str) -> ft.TextField:
        tf = ft.TextField(
            label=label, 
            width=180, 
            dense=True,
            filled=True,
            bgcolor="#F8FAFC",
            border_color="#475569",
            text_size=14,
            border_radius=12,
            content_padding=ft.padding.all(12)
        )
        
        def on_date_change(e):
            if e.control.value:
                tf.value = e.control.value.strftime("%Y-%m-%d")
                tf.update()
                # Trigger table refresh
                if hasattr(self, "backups_table"):
                    self.backups_table.refresh()

        dp = ft.DatePicker(
            on_change=on_date_change,
            cancel_text="CANCELAR",
            confirm_text="ACEPTAR",
            error_format_text="Formato inválido",
            error_invalid_text="Fecha fuera de rango",
            help_text="SELECCIONAR FECHA... *"
        )
        def _maybe_set(obj: Any, name: str, value: Any) -> None:
            if hasattr(obj, name):
                try:
                    setattr(obj, name, value)
                except Exception:
                    pass

        safe_min = datetime(1970, 1, 1)
        safe_max = datetime(2100, 12, 31)
        _maybe_set(dp, "first_date", safe_min)
        _maybe_set(dp, "last_date", safe_max)
        _maybe_set(dp, "current_date", datetime.now())
        
        # Add to overlay safely
        if self.page:
            self.page.overlay.append(dp)

        def open_picker(_):
            if hasattr(self.page, "open"):
                self.page.open(dp)
            else:
                dp.open = True
                self.page.update()

        tf.suffix = ft.IconButton(
            ft.icons.CALENDAR_MONTH_ROUNDED,
            on_click=open_picker,
            icon_size=18,
            tooltip="Seleccionar fecha"
        )
        return tf

    def _setup_view(self):
        # Tarjetas de programación
        self.schedule_cards_container = ft.Column([], spacing=12)

        # Filtros avanzados
        self.date_from_input = self._create_date_input("Desde")
        self.date_to_input = self._create_date_input("Hasta")
        
        # Tabla de backups con GenericTable
        self.backups_table = GenericTable(
            columns=[
                ColumnConfig(
                    key="type", 
                    label="Tipo", 
                    width=120,
                    renderer=lambda row: self._get_backup_type_badge(row.get('type', 'manual'))
                ),
                ColumnConfig(key="name", label="Archivo", width=300),
                ColumnConfig(
                    key="size", 
                    label="Tamaño", 
                    width=100,
                    formatter=lambda v, _: self._format_size(v)
                ),
                ColumnConfig(
                    key="created", 
                    label="Fecha", 
                    width=180,
                    formatter=lambda v, _: self._format_datetime(v)
                ),
                ColumnConfig(
                    key="status",
                    label="Estado",
                    width=100,
                    renderer=lambda row: self._get_status_badge('COMPLETADO') # Assuming listed are complete
                ),
                ColumnConfig(
                    key="_actions",
                    label="Acciones",
                    width=120,
                    sortable=False,
                    renderer=lambda row: ft.Row([
                        ft.IconButton(
                            ft.icons.RESTORE,
                            icon_color=self.COLOR_WARNING,
                            tooltip="Restaurar backup",
                            on_click=lambda e: self._confirm_restore(row['path'])
                        ),
                        ft.IconButton(
                            ft.icons.DELETE,
                            icon_color=self.COLOR_ERROR,
                            tooltip="Eliminar backup",
                            on_click=lambda e: self._delete_backup(row['path'])
                        ),
                    ], spacing=0)
                )
            ],
            data_provider=self._backup_provider,
            simple_filter=SimpleFilterConfig(
                label="Tipo",
                options=[
                    (None, "Todos"),
                    ("FULL", "FULL"),
                    ("DIFERENCIAL", "DIFERENCIAL"),
                    ("INCREMENTAL", "INCREMENTAL"),
                    ("MANUAL", "Manual"),
                ]
            ),
            advanced_filters=[
                AdvancedFilterControl(name="date_from", control=self.date_from_input),
                AdvancedFilterControl(name="date_to", control=self.date_to_input),
            ],
            show_mass_actions=False, # Mass actions disabled as per request
            auto_load=True,
            page_size=10,
            id_field="path"
        )
        self.backups_table.search_field.hint_text = "Buscar por nombre..."


        # Contenido principal
        self.main_content = ft.ListView([
            # Dashboard content
            *self._build_dashboard_content(),
            ft.Divider(height=20),
            # History content
            *self._build_history_content(),
        ], spacing=12)

    def _build_dashboard_content(self):
        return [
            # Header
            ft.Row([
                ft.Column([
                    ft.Text("Sistema de Backups Profesionales", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Text("FULL + DIFERENCIAL + INCREMENTAL - Restauración concatenable", size=12, color=self.COLOR_TEXT_MUTED),
                ], spacing=2),
                ft.Row([
                    ft.ElevatedButton(
                        "Crear Backup",
                        icon=ft.icons.ADD_ROUNDED,
                        bgcolor=self.COLOR_PRIMARY,
                        color=ft.Colors.WHITE,
                        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                        on_click=self._trigger_backup
                    ),
                ], spacing=8)
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),

            # Métricas
            ft.Row([
                self._metric_card("Total Backups", self.total_backups_text, ft.icons.FOLDER_SPECIAL_ROUNDED, self.COLOR_PRIMARY),
                self._metric_card("Último Backup", self.last_backup_text, ft.icons.ACCESS_TIME_ROUNDED, self.COLOR_SUCCESS),
                self._metric_card("Próximo Backup", self.next_backup_text, ft.icons.SCHEDULE_ROUNDED, self.COLOR_INFO),
            ], spacing=12),

            # Acciones rápidas
            ft.Container(
                content=ft.Column([
                    ft.Text("Ejecutar Backup Ahora", size=16, weight=ft.FontWeight.BOLD),
                    ft.Row([
                        self._action_button("FULL", ft.icons.CALENDAR_MONTH_ROUNDED, self.COLOR_SUCCESS, lambda e: self._execute_backup('FULL')),
                        self._action_button("DIFERENCIAL", ft.icons.DATE_RANGE_ROUNDED, self.COLOR_INFO, lambda e: self._execute_backup('DIFERENCIAL')),
                        self._action_button("INCREMENTAL", ft.icons.TODAY_ROUNDED, self.COLOR_WARNING, lambda e: self._execute_backup('INCREMENTAL')),
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
                        ft.Icon(ft.icons.EVENT_ROUNDED, color=self.COLOR_PRIMARY, size=20),
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
        ]



    def _build_history_content(self):
        return [
            ft.Text("Historial Completo de Backups", size=18, weight=ft.FontWeight.BOLD),
            ft.Divider(),
            # Generic Table component wrapped in limited height
            ft.Container(
                content=self.backups_table.build(),
                height=500,
                # border=ft.border.all(1, self.COLOR_BORDER), # Removed border
                border_radius=12,
            )
        ]

    def load_data(self):
        try:
            self.data_loaded = False
            
            # The provider fetches data, but we can also fetch here for metrics
            self.data = self.backup_service.list_backups()
            
            if self.data:
                self.total_backups_text.value = str(len(self.data))
                latest = self.data[0]['created'].strftime("%Y-%m-%d %H:%M")
                self.last_backup_text.value = latest
            else:
                self.total_backups_text.value = "0"
                self.last_backup_text.value = "N/A"

            # Próximo backup
            next_times = self._get_next_backup_times()
            if next_times:
                closest_type = min(next_times.keys(), key=lambda k: next_times[k]['next_run'])
                closest = next_times[closest_type]
                self.next_backup_text.value = f"{closest_type} {self._format_time_until(closest['next_run'])}"

            # Refresh table
            self.backups_table.refresh()

            # Cargar tarjetas de programación
            self._update_schedule_cards()

            self.data_loaded = True

        except Exception as e:
            self.show_message(f"Error cargando datos: {str(e)}", "error")
        finally:
            self.page.update()

    def _get_next_backup_times(self):
        now = datetime.now()
        next_times = {}

        # FULL: Día 1 de cada mes a las 00:00
        if now.day == 1 and now.hour == 0 and now.minute < 1:
            next_full = now.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # Próximo mes
            if now.month == 12:
                next_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                next_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            next_full = next_month

        next_times['FULL'] = {'next_run': next_full, 'schedule': 'Dia 1 a las 00:00'}

        # DIFERENCIAL: Domingos a las 23:30
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0 and (now.hour < 23 or (now.hour == 23 and now.minute < 30)):
            next_dif = now.replace(hour=23, minute=30, second=0, microsecond=0)
        else:
            if days_until_sunday == 0:
                days_until_sunday = 7
            next_dif = (now + timedelta(days=days_until_sunday)).replace(hour=23, minute=30, second=0, microsecond=0)

        next_times['DIFERENCIAL'] = {'next_run': next_dif, 'schedule': 'Domingos a las 23:30'}

        # INCREMENTAL: Diario a las 23:00
        if now.hour < 23 or (now.hour == 23 and now.minute < 1):
            next_inc = now.replace(hour=23, minute=0, second=0, microsecond=0)
        else:
            next_inc = (now + timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)

        next_times['INCREMENTAL'] = {'next_run': next_inc, 'schedule': 'Diario a las 23:00'}

        return next_times

    def _update_schedule_cards(self):
        next_times = self._get_next_backup_times()
        cards = []
        for backup_type, info in next_times.items():
            color = self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY)
            labels = {"FULL": "FULL", "DIFERENCIAL": "DIFERENCIAL", "INCREMENTAL": "INCREMENTAL"}
            icons = {"FULL": ft.icons.CALENDAR_MONTH_ROUNDED, "DIFERENCIAL": ft.icons.DATE_RANGE_ROUNDED, "INCREMENTAL": ft.icons.TODAY_ROUNDED}

            time_until = self._format_time_until(info["next_run"])

            card = ft.Container(
                content=ft.Row([
                    ft.Container(
                        content=ft.Icon(icons.get(backup_type, ft.icons.BACKUP_ROUNDED), color=color, size=22),
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

        # Obtener info del backup
        backup = next((b for b in self.data if b['path'] == path), None)
        if backup:
            content = ft.Column([
                ft.Text("Resumen de Restauración", size=16, weight=ft.FontWeight.BOLD),
                ft.Text(f"Archivo: {backup['name']}", size=12),
                ft.Text(f"Tipo: {backup.get('type', 'manual').upper()}", size=12),
                ft.Text(f"Tamaño: {self._format_size(backup['size'])}", size=12),
                ft.Text(f"Fecha: {self._format_datetime(backup['created'])}", size=12),
                ft.Text("¿Estás seguro? Esto SOBREESCRIBIRÁ la base de datos actual.\nEsta acción no se puede deshacer.", size=12, color=self.COLOR_WARNING),
            ], spacing=8)
        else:
            content = ft.Text("¿Estás seguro? Esto SOBREESCRIBIRÁ la base de datos actual.\nEsta acción no se puede deshacer.")

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar Restauración"),
            content=content,
            actions=[
                cancel_button("Cancelar", on_click=on_cancel),
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
                cancel_button("Cancelar", on_click=on_cancel),
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
                    create_backup_option("FULL", "Backup FULL (Completo)", ft.icons.CALENDAR_MONTH_ROUNDED),
                    create_backup_option("DIFERENCIAL", "Backup DIFERENCIAL (Semanal)", ft.icons.DATE_RANGE_ROUNDED),
                    create_backup_option("INCREMENTAL", "Backup INCREMENTAL (Diario)", ft.icons.TODAY_ROUNDED),
                    create_backup_option("MANUAL", "Backup Manual", ft.icons.SAVE_ROUNDED),
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
        return ft.Container(
            content=self.main_content,
            expand=True
        )

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
                    alignment=ft.Alignment(0, 0),
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
