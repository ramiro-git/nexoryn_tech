from typing import List, Optional, Dict, Any, Callable
import flet as ft
from desktop_app.components.generic_table import ColumnConfig, GenericTable, SimpleFilterConfig
from desktop_app.services.backup_service import BackupService
import datetime

class BackupView:
    def __init__(self, page: ft.Page, backup_service: BackupService, show_message_callback: Callable, set_connection_callback: Callable):
        self.page = page
        self.backup_service = backup_service
        self.show_message = show_message_callback
        self.set_connection = set_connection_callback
        self.data: List[Dict[str, Any]] = []
        
        # Colors from ui_advanced.py
        self.COLOR_CARD = "#FFFFFF"
        self.COLOR_BORDER = "#E2E8F0"
        self.COLOR_PRIMARY = "#4F46E5"
        self.COLOR_TEXT = "#0F172A"
        self.COLOR_TEXT_MUTED = "#64748B"
        self.COLOR_SUCCESS = "#10B981"
        self.COLOR_WARNING = "#F59E0B"
        self.COLOR_INFO = "#3B82F6"
        
        # Backup type colors
        self.TYPE_COLORS = {
            "daily": "#3B82F6",    # Blue
            "weekly": "#8B5CF6",   # Purple
            "monthly": "#0D9488",  # Teal 600
            "manual": "#10B981",   # Green
        }
        
        # File pickers for folder selection
        self.folder_picker = ft.FilePicker(on_result=self._on_folder_selected)
        self.sync_folder_picker = ft.FilePicker(on_result=self._on_sync_folder_selected)
        page.overlay.append(self.folder_picker)
        page.overlay.append(self.sync_folder_picker)
        
        # Track which picker is active
        self._picker_mode = "backup"  # "backup" or "sync"
        
        self._setup_view()

    def _format_size(self, size_bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"

    def _setup_view(self):
        # Table Configuration
        self.backup_columns = [
            ColumnConfig(key="name", label="Archivo", sortable=True),
            ColumnConfig(
                key="type", 
                label="Tipo", 
                sortable=True,
                renderer=lambda row: self._type_badge(row.get("type", ""))
            ),
            ColumnConfig(key="size", label="Tamaño", formatter=lambda v, _: self._format_size(v)),
            ColumnConfig(
                key="created", 
                label="Fecha de creación", 
                formatter=lambda v, _: v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime.datetime) else str(v)
            ),
            ColumnConfig(
                key="actions",
                label="Acciones",
                sortable=False,
                renderer=lambda row: ft.Row([
                    ft.IconButton(
                        ft.Icons.RESTORE, 
                        tooltip="Restaurar backup", 
                        on_click=lambda e, p=row['path']: self._confirm_restore(p)
                    ),
                    ft.IconButton(
                        ft.Icons.DELETE, 
                        tooltip="Eliminar", 
                        icon_color=ft.Colors.RED_400,
                        on_click=lambda e, p=row['path']: self._delete_backup(p)
                    )
                ], spacing=0)
            )
        ]

        # Date filter controls with consistent styling
        self.date_from_picker = ft.DatePicker(
            on_change=lambda e: self._on_date_filter_change(e, "from"),
            help_text="SELECCIONAR FECHA",
            cancel_text="CANCELAR",
            confirm_text="ACEPTAR",
        )
        self.date_to_picker = ft.DatePicker(
            on_change=lambda e: self._on_date_filter_change(e, "to"),
            help_text="SELECCIONAR FECHA",
            cancel_text="CANCELAR",
            confirm_text="ACEPTAR",
        )
        self.page.overlay.append(self.date_from_picker)
        self.page.overlay.append(self.date_to_picker)
        
        # Style constants (matching ui_basic.py)
        COLOR_ACCENT = "#6366F1"
        
        def _style_input(control):
            control.border_color = "#475569"
            control.focused_border_color = COLOR_ACCENT
            control.border_radius = 12
            control.text_size = 14
            control.label_style = ft.TextStyle(color="#1E293B", size=13, weight=ft.FontWeight.BOLD)
            control.content_padding = ft.padding.all(12)
            control.bgcolor = "#F8FAFC"
            control.filled = True
            if hasattr(control, "border_width"):
                control.border_width = 2 if isinstance(control, ft.Dropdown) else 1
        
        self.date_from_field = ft.TextField(
            label="Desde", width=150,
            suffix=ft.IconButton(ft.Icons.CALENDAR_MONTH_ROUNDED, icon_size=18, on_click=lambda e: self.page.open(self.date_from_picker))
        )
        _style_input(self.date_from_field)
        
        self.date_to_field = ft.TextField(
            label="Hasta", width=150,
            suffix=ft.IconButton(ft.Icons.CALENDAR_MONTH_ROUNDED, icon_size=18, on_click=lambda e: self.page.open(self.date_to_picker))
        )
        _style_input(self.date_to_field)
        
        self.type_filter_dropdown = ft.Dropdown(
            label="Tipo", width=160,
            options=[ft.dropdown.Option("Todos", "Todos"), ft.dropdown.Option("manual", "Manual"), ft.dropdown.Option("daily", "Diario"), ft.dropdown.Option("weekly", "Semanal"), ft.dropdown.Option("monthly", "Mensual")],
            value="Todos",
            on_change=lambda e: self.table.refresh()
        )
        _style_input(self.type_filter_dropdown)
        
        from desktop_app.components.generic_table import AdvancedFilterControl
        
        self.table = GenericTable(
            columns=self.backup_columns,
            data_provider=self._data_provider,
            id_field="path",
            advanced_filters=[
                AdvancedFilterControl("type", self.type_filter_dropdown),
                AdvancedFilterControl("date_from", self.date_from_field),
                AdvancedFilterControl("date_to", self.date_to_field),
            ],
            show_mass_actions=False,
            auto_load=True,
            page_size=10
        )
        
        self.table_view = self.table.build()

        # Stats Cards - will be updated in load_data
        self.last_backup_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD)
        self.total_backups_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD)
        self.next_backup_text = ft.Text("—", size=18, weight=ft.FontWeight.BOLD)
        
        # Scheduled backups cards - will be populated with times
        self.schedule_cards_container = ft.Column([], spacing=8)
        
        # Current directory display
        self.current_dir_text = ft.Text(
            self.backup_service.get_backup_dir(),
            size=12,
            color=self.COLOR_TEXT_MUTED,
            max_lines=1,
            overflow=ft.TextOverflow.ELLIPSIS,
        )
        
        # Cloud sync directory display
        sync_dir = self.backup_service.get_sync_dir()
        self.sync_dir_text = ft.Text(
            sync_dir if sync_dir else "No configurada",
            size=12,
            color=self.COLOR_TEXT_MUTED if sync_dir else "#94A3B8",
            max_lines=1,
            overflow=ft.TextOverflow.ELLIPSIS,
            italic=not sync_dir,
        )
        
        # Sync status indicator
        self.sync_status_icon = ft.Icon(
            ft.Icons.CLOUD_OFF_ROUNDED if not self.backup_service.is_sync_enabled() else ft.Icons.CLOUD_DONE_ROUNDED,
            color="#94A3B8" if not self.backup_service.is_sync_enabled() else self.COLOR_SUCCESS,
            size=18,
        )
        
    def _type_badge(self, backup_type: str) -> ft.Container:
        """Create a colored badge for backup type."""
        color = self.TYPE_COLORS.get(backup_type, self.COLOR_TEXT_MUTED)
        labels = {
            "daily": "Diario",
            "weekly": "Semanal", 
            "monthly": "Mensual",
            "manual": "Manual"
        }
        return ft.Container(
            content=ft.Text(
                labels.get(backup_type, backup_type),
                size=11,
                weight=ft.FontWeight.BOLD,
                color=color,
            ),
            padding=ft.padding.symmetric(horizontal=10, vertical=4),
            border_radius=12,
            bgcolor=f"{color}1A",  # 10% opacity
        )
        
    def _on_date_filter_change(self, e, which: str):
        """Handle date picker changes."""
        if e.control.value:
            date_str = e.control.value.strftime("%Y-%m-%d")
            if which == "from":
                self.date_from_field.value = date_str
            else:
                self.date_to_field.value = date_str
            self.page.update()
            self.table.refresh()

    def _data_provider(self, offset: int, limit: int, search: Optional[str], simple_filter_value: Optional[str], advanced: Dict[str, Any], sorts: List[tuple]) -> tuple[List[Dict[str, Any]], int]:
        # Filter logic
        filtered = self.data
        
        # Type filter
        type_val = advanced.get("type")
        if type_val and type_val != "Todos":
            filtered = [b for b in filtered if b['type'] == type_val]
        
        # Date filters
        date_from = advanced.get("date_from")
        if date_from:
            from datetime import datetime
            try:
                from_dt = datetime.strptime(date_from, "%Y-%m-%d")
                filtered = [b for b in filtered if b['created'] >= from_dt]
            except: pass
        
        date_to = advanced.get("date_to")
        if date_to:
            from datetime import datetime
            try:
                to_dt = datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                filtered = [b for b in filtered if b['created'] <= to_dt]
            except: pass
        
        if search:
            s = search.lower()
            filtered = [b for b in filtered if s in b['name'].lower()]
            
        # Sort logic
        if sorts:
            key, direction = sorts[0]
            reverse = direction == "desc"
            filtered.sort(key=lambda x: x.get(key, ""), reverse=reverse)
            
        # Pagination
        total = len(filtered)
        paginated = filtered[offset:offset+limit]
        return paginated, total

    def load_data(self):
        try:
            self.data = self.backup_service.list_backups()
            self.table.refresh()
            self._update_metrics()
            self._update_schedule_cards()
            self.set_connection(True, "DB conectado")
        except Exception as e:
            self.show_message(f"Error cargando backups: {e}", "error")
            self.set_connection(False, "Error de backup")

    def _update_metrics(self):
        self.total_backups_text.value = str(len(self.data))
        if self.data:
            # Assumes data is sorted by creation desc by service
            latest = self.data[0]['created'].strftime("%Y-%m-%d %H:%M")
            self.last_backup_text.value = latest
        else:
            self.last_backup_text.value = "N/A"
        
        # Next backup
        next_times = self.backup_service.get_next_backup_times()
        closest = min(next_times.values(), key=lambda x: x["next_run"])
        self.next_backup_text.value = BackupService.format_time_until(closest["next_run"])
        
        self.current_dir_text.value = self.backup_service.get_backup_dir()
        self.page.update()
    
    def _update_schedule_cards(self):
        """Update the scheduled backups panel."""
        next_times = self.backup_service.get_next_backup_times()
        
        cards = []
        for backup_type, info in [("daily", next_times["daily"]), 
                                   ("weekly", next_times["weekly"]), 
                                   ("monthly", next_times["monthly"])]:
            color = self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY)
            labels = {"daily": "Diario", "weekly": "Semanal", "monthly": "Mensual"}
            icons = {"daily": ft.Icons.TODAY_ROUNDED, "weekly": ft.Icons.DATE_RANGE_ROUNDED, "monthly": ft.Icons.CALENDAR_MONTH_ROUNDED}
            
            time_until = BackupService.format_time_until(info["next_run"])
            
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
                    ft.ElevatedButton(
                        "Ejecutar",
                        icon=ft.Icons.PLAY_ARROW_ROUNDED,
                        bgcolor=color,
                        color="#FFFFFF",
                        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                        on_click=lambda e, t=backup_type: self._confirm_backup(t)
                    ),
                ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                padding=12,
                bgcolor=self.COLOR_CARD,
                border=ft.border.all(1, self.COLOR_BORDER),
                border_radius=12,
            )
            cards.append(card)
        
        self.schedule_cards_container.controls = cards
        
    def _confirm_backup(self, backup_type: str):
        """Show confirmation dialog before creating backup."""
        labels = {"daily": "Diario", "weekly": "Semanal", "monthly": "Mensual", "manual": "Manual"}
        type_label = labels.get(backup_type, backup_type)
        
        def on_confirm(e):
            self.page.close(dlg)
            self._execute_backup(backup_type)
            
        def on_cancel(e):
            self.page.close(dlg)

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Crear Backup {type_label}"),
            content=ft.Column([
                ft.Icon(ft.Icons.BACKUP_ROUNDED, size=48, color=self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY)),
                ft.Text(
                    f"¿Deseas crear un backup de tipo '{type_label}' ahora?",
                    size=14,
                    text_align=ft.TextAlign.CENTER,
                ),
                ft.Text(
                    f"El backup se guardará en:\n{self.backup_service.get_backup_dir()}",
                    size=12,
                    color=self.COLOR_TEXT_MUTED,
                    text_align=ft.TextAlign.CENTER,
                ),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=12, tight=True),
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton(
                    "Aceptar", 
                    on_click=on_confirm, 
                    bgcolor=self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY), 
                    color=ft.Colors.WHITE
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)
    
    def _execute_backup(self, backup_type: str):
        """Execute backup of specified type."""
        labels = {"daily": "Diario", "weekly": "Semanal", "monthly": "Mensual", "manual": "Manual"}
        try:
            self.show_message(f"Creando backup {labels.get(backup_type, backup_type)}...", "info")
            self.backup_service.create_backup(backup_type)
            self.load_data()
            
            # Check sync status
            if self.backup_service.is_sync_enabled():
                sync_status = self.backup_service.get_last_sync_status()
                if sync_status and sync_status.get("success"):
                    self.show_message(f"Backup {labels.get(backup_type, backup_type)} creado y sincronizado a la nube.", "success")
                else:
                    self.show_message(f"Backup {labels.get(backup_type, backup_type)} creado. ⚠️ Falló sincronización.", "warning")
            else:
                self.show_message(f"Backup {labels.get(backup_type, backup_type)} creado exitosamente.", "success")
        except Exception as exc:
            self.show_message(f"Falló el backup: {exc}", "error")

    def _trigger_backup(self, e):
        """Manual backup button click handler (opens type selection)."""
        self._show_backup_type_menu()
    
    def _show_backup_type_menu(self):
        """Show a menu to select backup type."""
        def create_backup_option(backup_type: str, label: str, icon: str):
            color = self.TYPE_COLORS.get(backup_type, self.COLOR_PRIMARY)
            return ft.ListTile(
                leading=ft.Icon(icon, color=color),
                title=ft.Text(label),
                on_click=lambda e, t=backup_type: (self.page.close(bottom_sheet), self._confirm_backup(t))
            )
        
        bottom_sheet = ft.BottomSheet(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Seleccionar tipo de backup", size=16, weight=ft.FontWeight.BOLD),
                    ft.Divider(),
                    create_backup_option("manual", "Manual", ft.Icons.SAVE_ROUNDED),
                    create_backup_option("daily", "Diario", ft.Icons.TODAY_ROUNDED),
                    create_backup_option("weekly", "Semanal", ft.Icons.DATE_RANGE_ROUNDED),
                    create_backup_option("monthly", "Mensual", ft.Icons.CALENDAR_MONTH_ROUNDED),
                ], tight=True),
                padding=20,
            ),
        )
        self.page.open(bottom_sheet)

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
                ft.ElevatedButton("Eliminar", on_click=on_confirm, bgcolor=ft.Colors.RED_600, color=ft.Colors.WHITE),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)

    def _confirm_restore(self, path):
        def on_confirm(e):
            self.page.close(dlg)
            self._perform_restore(path)
            
        def on_cancel(e):
            self.page.close(dlg)

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar restauración"),
            content=ft.Text("¿Estás seguro? Esto SOBREESCRIBIRÁ la base de datos actual con este backup.\nEsta acción no se puede deshacer."),
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton("Restaurar", on_click=on_confirm, bgcolor=ft.Colors.RED_600, color=ft.Colors.WHITE),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)

    def _perform_restore(self, path):
        try:
            self.show_message("Restaurando base de datos... Por favor espera.", "info")
            # Force UI update before potentially blocking operation
            self.page.update()
            
            self.backup_service.restore_backup(path)
            self.show_message("Restauración completada. Reiniciá la aplicación si es necesario.", "success")
        except Exception as exc:
            self.show_message(f"Falló la restauración: {exc}", "error")
    
    def _open_folder_picker(self, e):
        """Open folder picker dialog."""
        self.folder_picker.get_directory_path(
            dialog_title="Seleccionar carpeta de backups"
        )
    
    def _on_folder_selected(self, e: ft.FilePickerResultEvent):
        """Handle folder selection result."""
        if e.path:
            success = self.backup_service.set_backup_dir(e.path)
            if success:
                self.current_dir_text.value = e.path
                self.show_message(f"Carpeta de backups cambiada a: {e.path}", "success")
                self.load_data()  # Reload to show backups from new location
            else:
                self.show_message("No se pudo cambiar la carpeta de backups", "error")
            self.page.update()
    
    def _open_sync_folder_picker(self, e):
        """Open folder picker for cloud sync directory."""
        self.sync_folder_picker.get_directory_path(
            dialog_title="Seleccionar carpeta de sincronización (Google Drive, OneDrive, etc.)"
        )
    
    def _on_sync_folder_selected(self, e: ft.FilePickerResultEvent):
        """Handle sync folder selection result."""
        if e.path:
            success = self.backup_service.set_sync_dir(e.path)
            if success:
                self.sync_dir_text.value = e.path
                self.sync_dir_text.italic = False
                self.sync_dir_text.color = self.COLOR_TEXT_MUTED
                self._update_sync_status_icon()
                self.show_message(f"Sincronización automática activada: {e.path}", "success")
            else:
                self.show_message("No se pudo configurar la carpeta de sincronización", "error")
            self.page.update()
    
    def _disable_sync(self, e):
        """Disable cloud sync."""
        self.backup_service.set_sync_dir(None)
        self.sync_dir_text.value = "No configurada"
        self.sync_dir_text.italic = True
        self.sync_dir_text.color = "#94A3B8"
        self._update_sync_status_icon()
        self.show_message("Sincronización automática desactivada", "info")
        self.page.update()
    
    def _update_sync_status_icon(self):
        """Update the sync status icon based on current state."""
        if self.backup_service.is_sync_enabled():
            self.sync_status_icon.name = ft.Icons.CLOUD_DONE_ROUNDED
            self.sync_status_icon.color = self.COLOR_SUCCESS
        else:
            self.sync_status_icon.name = ft.Icons.CLOUD_OFF_ROUNDED
            self.sync_status_icon.color = "#94A3B8"

    def build(self) -> ft.Control:
        return ft.Column([
            # Header row with title and actions
            ft.Row([
                ft.Column([
                    ft.Text("Backups & Restauración", size=22, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Text("Gestioná copias de seguridad de los datos.", size=12, color=self.COLOR_TEXT_MUTED),
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
            
            # Stats row
            ft.Row([
                self._metric_card("Total Backups", self.total_backups_text, ft.Icons.FOLDER_SPECIAL_ROUNDED),
                self._metric_card("Último Backup", self.last_backup_text, ft.Icons.ACCESS_TIME_ROUNDED),
                self._metric_card("Próximo Backup", self.next_backup_text, ft.Icons.SCHEDULE_ROUNDED, self.COLOR_INFO),
            ], spacing=12),
            
            # Folder selection row
            ft.Container(
                content=ft.Row([
                    ft.Icon(ft.Icons.FOLDER_ROUNDED, color=self.COLOR_TEXT_MUTED, size=18),
                    ft.Text("Carpeta de backups:", size=12, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Container(content=self.current_dir_text, expand=True),
                    ft.IconButton(
                        icon=ft.Icons.FOLDER_OPEN_ROUNDED,
                        tooltip="Cambiar carpeta de backups",
                        icon_color=self.COLOR_PRIMARY,
                        on_click=self._open_folder_picker,
                    ),
                ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                padding=ft.padding.symmetric(horizontal=12, vertical=8),
                bgcolor="#F8FAFC",
                border_radius=10,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),
            
            # Cloud sync folder row
            ft.Container(
                content=ft.Row([
                    self.sync_status_icon,
                    ft.Text("Sincronización en la nube:", size=12, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ft.Container(content=self.sync_dir_text, expand=True),
                    ft.IconButton(
                        icon=ft.Icons.CLOUD_OFF_ROUNDED,
                        tooltip="Desactivar sincronización",
                        icon_color="#94A3B8",
                        on_click=self._disable_sync,
                    ),
                    ft.IconButton(
                        icon=ft.Icons.CREATE_NEW_FOLDER_ROUNDED,
                        tooltip="Configurar carpeta de sincronización",
                        icon_color=self.COLOR_INFO,
                        on_click=self._open_sync_folder_picker,
                    ),
                ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                padding=ft.padding.symmetric(horizontal=12, vertical=8),
                bgcolor="#F0F9FF",  # Light blue bg
                border_radius=10,
                border=ft.border.all(1, "#BAE6FD"),
            ),
            
            # Scheduled backups panel
            ft.Container(
                content=ft.Column([
                    ft.Row([
                        ft.Icon(ft.Icons.EVENT_ROUNDED, color=self.COLOR_PRIMARY, size=20),
                        ft.Text("Programación de Backups Automáticos", size=14, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                    ], spacing=8),
                    ft.Container(height=8),
                    self.schedule_cards_container,
                ]),
                padding=16,
                bgcolor=self.COLOR_CARD,
                border_radius=14,
                border=ft.border.all(1, self.COLOR_BORDER),
            ),
            
            # Backups table
            ft.Container(
                content=self.table_view,
                expand=True,
                padding=12,
                bgcolor=self.COLOR_CARD,
                border_radius=14,
                border=ft.border.all(1, self.COLOR_BORDER),
            )
        ], expand=True, spacing=12, scroll=ft.ScrollMode.AUTO)

    def _metric_card(self, title: str, value_text: ft.Text, icon: str, color: str = None) -> ft.Container:
        if color is None:
            color = self.COLOR_PRIMARY
        return ft.Container(
            content=ft.Row(
                [
                    ft.Container(
                        width=40,
                        height=40,
                        border_radius=12,
                        bgcolor=f"{color}1A",
                        alignment=ft.alignment.center,
                        content=ft.Icon(icon, color=color),
                    ),
                    ft.Column(
                        [
                            ft.Text(title, size=12, color=self.COLOR_TEXT_MUTED),
                            value_text,
                        ],
                        spacing=2,
                    ),
                ],
                spacing=12,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            padding=12,
            bgcolor=self.COLOR_CARD,
            border=ft.border.all(1, self.COLOR_BORDER),
            border_radius=14,
            expand=1,
        )
