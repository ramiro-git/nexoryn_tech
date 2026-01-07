import flet as ft
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta


class BackupProfessionalView:
    def __init__(self, page: ft.Page, db, show_message: Callable):
        self.page = page
        self.db = db
        self.show_message = show_message
        
        # Colores
        self.COLOR_PRIMARY = "#4F46E5"
        self.COLOR_SUCCESS = "#10B981"
        self.COLOR_WARNING = "#F59E0B"
        self.COLOR_ERROR = "#EF4444"
        self.COLOR_INFO = "#3B82F6"
        self.COLOR_CARD = "#FFFFFF"
        self.COLOR_BORDER = "#E2E8F0"
        self.COLOR_TEXT = "#0F172A"
        self.COLOR_TEXT_MUTED = "#64748B"
        
        # Importar servicios con lazy loading
        self._backup_manager = None
        self._cloud_service = None
        
        # Estado
        self.loading = False
        self.data_loaded = False
        
        self._setup_view()
    
    @property
    def backup_manager(self):
        if self._backup_manager is None:
            from desktop_app.services.backup_manager import BackupManager
            self._backup_manager = BackupManager(self.db)
        return self._backup_manager
    
    @property
    def cloud_service(self):
        if self._cloud_service is None:
            from desktop_app.services.cloud_storage_service import CloudStorageService
            self._cloud_service = CloudStorageService(self.db, provider='LOCAL')
        return self._cloud_service
    
    def _format_size(self, size_bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"
    
    def _format_datetime(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    def _format_time_ago(self, dt: datetime) -> str:
        now = datetime.now()
        diff = now - dt
        
        if diff.days > 0:
            return f"hace {diff.days} días"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"hace {hours} horas"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"hace {minutes} minutos"
        else:
            return "hace segundos"
    
    def _setup_view(self):
        # Metricas
        self.total_backups_text = ft.Text("—", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT)
        self.last_backup_text = ft.Text("—", size=18, color=self.COLOR_TEXT_MUTED)
        self.next_backup_text = ft.Text("—", size=18, color=self.COLOR_INFO, weight=ft.FontWeight.BOLD)
        
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
        
        # Configuración de horarios
        self.full_schedule_day = ft.Dropdown(
            label="Día del mes",
            options=[ft.dropdown.Option(str(i), f"Día {i}") for i in range(1, 32)],
            width=150,
            value="1"
        )
        
        self.full_schedule_hour = ft.Dropdown(
            label="Hora",
            options=[ft.dropdown.Option(str(i), f"{i:02d}:00") for i in range(24)],
            width=100,
            value="0"
        )
        
        self.dif_schedule_weekday = ft.Dropdown(
            label="Día de semana",
            options=[
                ft.dropdown.Option("0", "Lunes"),
                ft.dropdown.Option("1", "Martes"),
                ft.dropdown.Option("2", "Miércoles"),
                ft.dropdown.Option("3", "Jueves"),
                ft.dropdown.Option("4", "Viernes"),
                ft.dropdown.Option("5", "Sábado"),
                ft.dropdown.Option("6", "Domingo"),
            ],
            width=150,
            value="6"
        )
        
        self.dif_schedule_hour = ft.Dropdown(
            label="Hora",
            options=[ft.dropdown.Option(str(i), f"{i:02d}:00") for i in range(24)],
            width=100,
            value="23"
        )
        
        self.inc_schedule_hour = ft.Dropdown(
            label="Hora",
            options=[ft.dropdown.Option(str(i), f"{i:02d}:00") for i in range(24)],
            width=100,
            value="23"
        )
        
        # Configuración de retención
        self.retention_full = ft.TextField(
            label="Retención FULL (meses)",
            value="12",
            width=120,
            keyboard_type=ft.KeyboardType.NUMBER
        )
        
        self.retention_dif = ft.TextField(
            label="Retención DIF (semanas)",
            value="8",
            width=120,
            keyboard_type=ft.KeyboardType.NUMBER
        )
        
        self.retention_inc = ft.TextField(
            label="Retención INC (días)",
            value="7",
            width=120,
            keyboard_type=ft.KeyboardType.NUMBER
        )
        
        # Configuración de nube
        self.cloud_provider = ft.Dropdown(
            label="Proveedor",
            options=[
                ft.dropdown.Option("LOCAL", "Carpeta Local"),
                ft.dropdown.Option("GOOGLE_DRIVE", "Google Drive"),
                ft.dropdown.Option("S3", "AWS S3"),
            ],
            width=200,
            value="LOCAL"
        )
        
        self.sync_dir = ft.TextField(
            label="Carpeta de sincronización",
            width=300,
            hint="Ruta a carpeta para sincronizar backups"
        )
        
        self.enable_sync = ft.Switch(
            label="Habilitar sincronización en la nube",
            value=False
        )
        
        # Logs
        self.logs_table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Fecha", size=11)),
                ft.DataColumn(ft.Text("Tipo", size=11)),
                ft.DataColumn(ft.Text("Mensaje", size=11)),
                ft.DataColumn(ft.Text("Estado", size=11)),
            ],
            rows=[],
            border_radius=10,
            border=ft.border.all(1, self.COLOR_BORDER),
            heading_row_color=ft.Colors.with_opacity(0.05, self.COLOR_PRIMARY),
        )
    
    def load_data(self):
        try:
            self.loading = True
            self.page.update()
            
            # Cargar estadísticas
            stats = self.backup_manager.get_backup_stats()
            
            if '_total' in stats:
                self.total_backups_text.value = str(stats['_total']['cantidad'])
            
            # Último backup
            backups = self.backup_manager.backup_incremental_service.list_backups(limit=1)
            if backups:
                last_backup = backups[0]
                self.last_backup_text.value = self._format_datetime(last_backup['fecha_inicio'])
            
            # Próximo backup
            next_times = self.backup_manager.get_next_backup_times()
            closest_type = min(next_times.keys(), 
                             key=lambda k: next_times[k]['next_run'])
            closest = next_times[closest_type]
            self.next_backup_text.value = f"{closest_type} en {self._format_time_ago(closest['next_run'])}"
            
            # Cargar tabla de backups
            self._load_backups_table()
            
            # Cargar logs
            self._load_logs()
            
            self.data_loaded = True
            
        except Exception as e:
            self.show_message(f"Error cargando datos: {str(e)}", "error")
        finally:
            self.loading = False
            self.page.update()
    
    def _load_backups_table(self):
        try:
            backups = self.backup_manager.backup_incremental_service.list_backups(limit=50)
            
            rows = []
            for backup in backups:
                tipo_badge = self._get_backup_type_badge(backup['tipo'])
                estado_badge = self._get_status_badge(backup['estado'])
                
                row = ft.DataRow(
                    cells=[
                        ft.DataCell(tipo_badge),
                        ft.DataCell(ft.Text(backup['archivo'], size=12)),
                        ft.DataCell(ft.Text(self._format_size(backup['tamano'] or 0), size=12)),
                        ft.DataCell(ft.Text(self._format_datetime(backup['fecha_inicio']), size=12)),
                        ft.DataCell(estado_badge),
                        ft.DataCell(
                            ft.Row([
                                ft.IconButton(
                                    ft.Icons.CHECK_CIRCLE,
                                    icon_color=self.COLOR_SUCCESS,
                                    tooltip="Validar backup",
                                    on_click=lambda e, b=backup: self._validate_backup(b)
                                ),
                                ft.IconButton(
                                    ft.Icons.CLOUD_UPLOAD,
                                    icon_color=self.COLOR_INFO,
                                    tooltip="Subir a la nube",
                                    on_click=lambda e, b=backup: self._upload_to_cloud(b)
                                ),
                                ft.IconButton(
                                    ft.Icons.RESTORE,
                                    icon_color=self.COLOR_WARNING,
                                    tooltip="Restaurar backup",
                                    on_click=lambda e, b=backup: self._confirm_restore(b)
                                ),
                                ft.IconButton(
                                    ft.Icons.DELETE,
                                    icon_color=self.COLOR_ERROR,
                                    tooltip="Eliminar backup",
                                    on_click=lambda e, b=backup: self._delete_backup(b)
                                ),
                            ], spacing=4)
                        ),
                    ]
                )
                rows.append(row)
            
            self.backups_table.rows = rows
            
        except Exception as e:
            self.show_message(f"Error cargando tabla de backups: {str(e)}", "error")
    
    def _load_logs(self):
        try:
            query = """
            SELECT fecha_hora, tipo_evento, detalle, nivel_log
            FROM seguridad.backup_event
            ORDER BY fecha_hora DESC
            LIMIT 20
            """
            
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    
                    table_rows = []
                    for row in rows:
                        fecha = row[0]
                        tipo = row[1] or "SISTEMA"
                        detalle = row[2] or "{}"
                        nivel = row[3] or "INFO"
                        
                        if isinstance(detalle, str):
                            detalle_text = detalle
                        else:
                            import json
                            detalle_text = json.dumps(detalle, ensure_ascii=False)
                        
                        nivel_color = {
                            "ERROR": self.COLOR_ERROR,
                            "WARNING": self.COLOR_WARNING,
                            "INFO": self.COLOR_INFO,
                            "DEBUG": self.COLOR_TEXT_MUTED
                        }.get(nivel, self.COLOR_TEXT_MUTED)
                        
                        row_cell = ft.DataRow(
                            cells=[
                                ft.DataCell(ft.Text(self._format_datetime(fecha), size=11)),
                                ft.DataCell(ft.Text(tipo, size=11)),
                                ft.DataCell(ft.Text(detalle_text[:50], size=11)),
                                ft.DataCell(
                                    ft.Container(
                                        content=ft.Text(nivel, size=11, color="#FFFFFF"),
                                        bgcolor=nivel_color,
                                        padding=ft.padding.symmetric(horizontal=8, vertical=4),
                                        border_radius=12
                                    )
                                ),
                            ]
                        )
                        table_rows.append(row_cell)
                    
                    self.logs_table.rows = table_rows
                    
        except Exception as e:
            print(f"Error cargando logs: {e}")
    
    def _get_backup_type_badge(self, tipo: str) -> ft.Container:
        colors = {
            'FULL': self.COLOR_SUCCESS,
            'DIFERENCIAL': self.COLOR_INFO,
            'INCREMENTAL': self.COLOR_WARNING,
            'MANUAL': self.COLOR_PRIMARY
        }
        
        labels = {
            'FULL': 'FULL',
            'DIFERENCIAL': 'DIF',
            'INCREMENTAL': 'INC',
            'MANUAL': 'Manual'
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
                resultado = self.backup_manager.execute_scheduled_backup(backup_type)
                
                if resultado['exitoso']:
                    self.show_message(
                        f"Backup {backup_type} creado exitosamente en {resultado['duracion_segundos']:.2f}s",
                        "success"
                    )
                    self._load_backups_table()
                else:
                    self.show_message(f"Error en backup {backup_type}: {resultado['mensaje']}", "error")
                    
            except Exception as e:
                self.show_message(f"Error creando backup: {str(e)}", "error")
        
        # Ejecutar en thread para no bloquear UI
        import threading
        threading.Thread(target=run_backup, daemon=True).start()
    
    def _validate_backup(self, backup: Dict):
        def run_validation():
            try:
                result = self.backup_manager.restore_service.validate_backup_chain(backup['id'])
                
                if result['valido']:
                    self.show_message("Backup validado correctamente", "success")
                else:
                    self.show_message(f"Backup inválido: {result['mensaje']}", "warning")
                    
            except Exception as e:
                self.show_message(f"Error validando backup: {str(e)}", "error")
        
        import threading
        threading.Thread(target=run_validation, daemon=True).start()
    
    def _upload_to_cloud(self, backup: Dict):
        def run_upload():
            try:
                from pathlib import Path
                backup_file = Path(backup['archivo'])
                
                result = self.cloud_service.upload_backup(
                    backup_file, 
                    backup['id'], 
                    backup['tipo']
                )
                
                if result.exitoso:
                    self.show_message("Backup subido a la nube exitosamente", "success")
                else:
                    self.show_message(f"Error subiendo a la nube: {result.mensaje}", "error")
                    
            except Exception as e:
                self.show_message(f"Error subiendo a la nube: {str(e)}", "error")
        
        import threading
        threading.Thread(target=run_upload, daemon=True).start()
    
    def _confirm_restore(self, backup: Dict):
        def on_confirm(e):
            self.page.close(dlg)
            self._run_restore_wizard(backup)
        
        def on_cancel(e):
            self.page.close(dlg)
        
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar Restauración"),
            content=ft.Column([
                ft.Icon(ft.Icons.WARNING_ROUNDED, size=48, color=self.COLOR_WARNING),
                ft.Text(
                    "Esta acción SOBREESCRIBIRÁ la base de datos actual.",
                    size=14,
                    text_align=ft.TextAlign.CENTER,
                    weight=ft.FontWeight.BOLD
                ),
                ft.Text(
                    f"Backup: {backup['archivo']}\nTipo: {backup['tipo']}\nFecha: {self._format_datetime(backup['fecha_inicio'])}",
                    size=12,
                    text_align=ft.TextAlign.CENTER,
                    color=self.COLOR_TEXT_MUTED
                ),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=12, tight=True),
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton(
                    "Continuar",
                    on_click=on_confirm,
                    bgcolor=self.COLOR_WARNING,
                    color=ft.Colors.WHITE
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)
    
    def _run_restore_wizard(self, backup: Dict):
        preview = self.backup_manager.restore_service.preview_restore(backup['fecha_inicio'])
        
        def on_confirm_restore(e):
            self.page.close(wizard_dlg)
            self._perform_restore(backup)
        
        def on_cancel(e):
            self.page.close(wizard_dlg)
        
        content = ft.Column([
            ft.Text("Resumen de Restauración", size=18, weight=ft.FontWeight.BOLD),
            ft.Divider(),
            
            ft.Column([
                ft.Row([
                    ft.Icon(ft.Icons.FOLDER_SPECIAL_ROUNDED, size=20, color=self.COLOR_INFO),
                    ft.Column([
                        ft.Text("Backup Base", size=12, weight=ft.FontWeight.BOLD),
                        ft.Text(preview['backups'][0]['archivo'] if preview['backups'] else "N/A", size=11),
                    ], spacing=2)
                ], spacing=10),
                
                ft.Row([
                    ft.Icon(ft.Icons.LAYERS_ROUNDED, size=20, color=self.COLOR_INFO),
                    ft.Column([
                        ft.Text(f"Backups a aplicar: {preview['cantidad_backups']}", size=12, weight=ft.FontWeight.BOLD),
                        ft.Text(f"Tamaño total: {preview['tamano_total_mb']:.2f} MB", size=11),
                    ], spacing=2)
                ], spacing=10),
                
                ft.Column([
                    ft.Text("Backups en la cadena:", size=12, weight=ft.FontWeight.BOLD),
                    *[
                        ft.Text(f"  • {b['tipo']}: {b['archivo']}", size=11, color=self.COLOR_TEXT_MUTED)
                        for b in preview['backups']
                    ]
                ], spacing=4),
            ], spacing=12),
            
            ft.Container(
                content=ft.Text(
                    "⚠️ Esta acción es irreversible. Se recomienda hacer un backup antes de continuar.",
                    size=12,
                    color=self.COLOR_WARNING
                ),
                bgcolor=f"{self.COLOR_WARNING}1A",
                padding=ft.padding.all(12),
                border_radius=10,
                border=ft.border.all(1, self.COLOR_WARNING),
            )
        ], spacing=12, tight=True)
        
        wizard_dlg = ft.AlertDialog(
            modal=True,
            title=ft.Icon(ft.Icons.RESTORE_ROUNDED, size=32, color=self.COLOR_WARNING),
            content=content,
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton(
                    "Restaurar Ahora",
                    on_click=on_confirm_restore,
                    bgcolor=self.COLOR_WARNING,
                    color=ft.Colors.WHITE,
                    icon=ft.Icons.RESTORE_ROUNDED
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(wizard_dlg)
    
    def _perform_restore(self, backup: Dict):
        def run_restore():
            try:
                self.show_message("Iniciando restauración... Esto puede tomar varios minutos.", "info")
                
                result = self.backup_manager.restore_from_backup_id(backup['id'])
                
                if result['exitoso']:
                    self.show_message(
                        f"Restauración completada exitosamente en {result['tiempo_segundos']:.2f}s",
                        "success"
                    )
                else:
                    self.show_message(f"Error en restauración: {result['mensaje']}", "error")
                    
            except Exception as e:
                self.show_message(f"Error en restauración: {str(e)}", "error")
        
        import threading
        threading.Thread(target=run_restore, daemon=True).start()
    
    def _delete_backup(self, backup: Dict):
        def on_confirm(e):
            self.page.close(dlg)
            
            def run_delete():
                try:
                    from pathlib import Path
                    backup_file = Path(backup['archivo'])
                    
                    if backup_file.exists():
                        backup_file.unlink()
                        self.show_message("Backup eliminado exitosamente", "success")
                        self._load_backups_table()
                    else:
                        self.show_message("El archivo de backup no existe", "warning")
                        
                except Exception as e:
                    self.show_message(f"Error eliminando backup: {str(e)}", "error")
            
            import threading
            threading.Thread(target=run_delete, daemon=True).start()
        
        def on_cancel(e):
            self.page.close(dlg)
        
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Eliminar Backup"),
            content=ft.Column([
                ft.Icon(ft.Icons.DELETE_ROUNDED, size=48, color=self.COLOR_ERROR),
                ft.Text(
                    "¿Estás seguro que deseas eliminar este backup?",
                    size=14,
                    text_align=ft.TextAlign.CENTER
                ),
                ft.Text(
                    f"Archivo: {backup['archivo']}",
                    size=12,
                    text_align=ft.TextAlign.CENTER,
                    color=self.COLOR_TEXT_MUTED
                ),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=12, tight=True),
            actions=[
                ft.TextButton("Cancelar", on_click=on_cancel),
                ft.ElevatedButton(
                    "Eliminar",
                    on_click=on_confirm,
                    bgcolor=self.COLOR_ERROR,
                    color=ft.Colors.WHITE
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(dlg)
    
    def _save_schedule(self):
        try:
            self.backup_manager.set_schedule('FULL', 
                day=int(self.full_schedule_day.value),
                hour=int(self.full_schedule_hour.value)
            )
            
            self.backup_manager.set_schedule('DIFERENCIAL',
                weekday=int(self.dif_schedule_weekday.value),
                hour=int(self.dif_schedule_hour.value)
            )
            
            self.backup_manager.set_schedule('INCREMENTAL',
                hour=int(self.inc_schedule_hour.value)
            )
            
            self.show_message("Horarios de backups actualizados correctamente", "success")
            
        except Exception as e:
            self.show_message(f"Error guardando horarios: {str(e)}", "error")
    
    def _save_cloud_config(self):
        try:
            if self.enable_sync.value:
                if not self.sync_dir.value:
                    self.show_message("Especifica la carpeta de sincronización", "warning")
                    return
                
                # Actualizar configuración de nube
                cloud_config = {
                    'sync_dir': self.sync_dir.value,
                    'provider': self.cloud_provider.value
                }
                
                self.show_message(f"Configuración de nube guardada: {self.cloud_provider.value}", "success")
            else:
                self.show_message("Sincronización en la nube desactivada", "info")
                
        except Exception as e:
            self.show_message(f"Error guardando configuración de nube: {str(e)}", "error")
    
    def build(self) -> ft.Control:
        tabs = ft.Tabs(
            selected_index=0,
            animation_duration=300,
            tabs=[
                # Tab 1: Dashboard y Acciones
                ft.Tab(
                    text="Dashboard",
                    icon=ft.Icons.DASHBOARD_ROUNDED,
                    content=ft.Column([
                        # Métricas
                        ft.Row([
                            self._metric_card(
                                "Total Backups",
                                self.total_backups_text,
                                ft.Icons.FOLDER_SPECIAL_ROUNDED,
                                self.COLOR_PRIMARY
                            ),
                            self._metric_card(
                                "Último Backup",
                                self.last_backup_text,
                                ft.Icons.ACCESS_TIME_ROUNDED,
                                self.COLOR_SUCCESS
                            ),
                            self._metric_card(
                                "Próximo Backup",
                                self.next_backup_text,
                                ft.Icons.SCHEDULE_ROUNDED,
                                self.COLOR_INFO
                            ),
                        ], spacing=12),
                        
                        ft.Divider(height=20),
                        
                        # Acciones rápidas
                        ft.Container(
                            content=ft.Column([
                                ft.Text("Ejecutar Backup Ahora", size=16, weight=ft.FontWeight.BOLD),
                                ft.Row([
                                    self._action_button(
                                        "FULL",
                                        ft.Icons.CALENDAR_MONTH_ROUNDED,
                                        self.COLOR_SUCCESS,
                                        lambda e: self._execute_backup('FULL')
                                    ),
                                    self._action_button(
                                        "DIFERENCIAL",
                                        ft.Icons.DATE_RANGE_ROUNDED,
                                        self.COLOR_INFO,
                                        lambda e: self._execute_backup('DIFERENCIAL')
                                    ),
                                    self._action_button(
                                        "INCREMENTAL",
                                        ft.Icons.TODAY_ROUNDED,
                                        self.COLOR_WARNING,
                                        lambda e: self._execute_backup('INCREMENTAL')
                                    ),
                                ], spacing=8),
                            ], spacing=12),
                            padding=16,
                            bgcolor=self.COLOR_CARD,
                            border_radius=12,
                            border=ft.border.all(1, self.COLOR_BORDER),
                        ),
                        
                        ft.Divider(height=20),
                        
                        # Tabla de backups
                        ft.Text("Historial de Backups", size=16, weight=ft.FontWeight.BOLD),
                        ft.Container(
                            content=ft.Column([
                                self.backups_table,
                            ], scroll=ft.ScrollMode.AUTO),
                            expand=True,
                            bgcolor=self.COLOR_CARD,
                            border_radius=12,
                            border=ft.border.all(1, self.COLOR_BORDER),
                        ),
                    ], expand=True, spacing=12)
                ),
                
                # Tab 2: Configuración
                ft.Tab(
                    text="Configuración",
                    icon=ft.Icons.SETTINGS_ROUNDED,
                    content=ft.Column([
                        ft.Text("Horarios de Backups Automáticos", size=16, weight=ft.FontWeight.BOLD),
                        ft.Divider(),
                        
                        # Configuración FULL
                        ft.Container(
                            content=ft.Column([
                                ft.Row([
                                    ft.Icon(ft.Icons.CALENDAR_MONTH_ROUNDED, color=self.COLOR_SUCCESS, size=24),
                                    ft.Column([
                                        ft.Text("Backup FULL (Mensual)", size=14, weight=ft.FontWeight.BOLD),
                                        ft.Text("Backup completo mensual - base de todos los backups", size=12, color=self.COLOR_TEXT_MUTED),
                                    ], spacing=2)
                                ], spacing=12),
                                ft.Row([
                                    self.full_schedule_day,
                                    self.full_schedule_hour,
                                    ft.Text("a las", size=12),
                                ], spacing=8),
                            ], spacing=12),
                            padding=16,
                            bgcolor=self.COLOR_CARD,
                            border_radius=12,
                            border=ft.border.all(1, self.COLOR_BORDER),
                        ),
                        
                        ft.Container(height=12),
                        
                        # Configuración DIFERENCIAL
                        ft.Container(
                            content=ft.Column([
                                ft.Row([
                                    ft.Icon(ft.Icons.DATE_RANGE_ROUNDED, color=self.COLOR_INFO, size=24),
                                    ft.Column([
                                        ft.Text("Backup DIFERENCIAL (Semanal)", size=14, weight=ft.FontWeight.BOLD),
                                        ft.Text("Cambios desde el último backup FULL", size=12, color=self.COLOR_TEXT_MUTED),
                                    ], spacing=2)
                                ], spacing=12),
                                ft.Row([
                                    self.dif_schedule_weekday,
                                    self.dif_schedule_hour,
                                    ft.Text("a las", size=12),
                                ], spacing=8),
                            ], spacing=12),
                            padding=16,
                            bgcolor=self.COLOR_CARD,
                            border_radius=12,
                            border=ft.border.all(1, self.COLOR_BORDER),
                        ),
                        
                        ft.Container(height=12),
                        
                        # Configuración INCREMENTAL
                        ft.Container(
                            content=ft.Column([
                                ft.Row([
                                    ft.Icon(ft.Icons.TODAY_ROUNDED, color=self.COLOR_WARNING, size=24),
                                    ft.Column([
                                        ft.Text("Backup INCREMENTAL (Diario)", size=14, weight=ft.FontWeight.BOLD),
                                        ft.Text("Cambios desde el último backup", size=12, color=self.COLOR_TEXT_MUTED),
                                    ], spacing=2)
                                ], spacing=12),
                                ft.Row([
                                    self.inc_schedule_hour,
                                    ft.Text("todos los días", size=12),
                                ], spacing=8),
                            ], spacing=12),
                            padding=16,
                            bgcolor=self.COLOR_CARD,
                            border_radius=12,
                            border=ft.border.all(1, self.COLOR_BORDER),
                        ),
                        
                        ft.Container(height=20),
                        
                        ft.ElevatedButton(
                            "Guardar Horarios",
                            icon=ft.Icons.SAVE_ROUNDED,
                            bgcolor=self.COLOR_PRIMARY,
                            color=ft.Colors.WHITE,
                            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                            on_click=lambda e: self._save_schedule()
                        ),
                        
                        ft.Divider(height=20),
                        
                        # Configuración de retención
                        ft.Text("Política de Retención", size=16, weight=ft.FontWeight.BOLD),
                        ft.Divider(),
                        
                        ft.Row([
                            self.retention_full,
                            self.retention_dif,
                            self.retention_inc,
                        ], spacing=12),
                        
                        ft.Divider(height=20),
                        
                        # Configuración de nube
                        ft.Text("Sincronización en la Nube", size=16, weight=ft.FontWeight.BOLD),
                        ft.Divider(),
                        
                        ft.Column([
                            self.enable_sync,
                            self.cloud_provider,
                            self.sync_dir,
                        ], spacing=12),
                        
                        ft.Container(height=12),
                        
                        ft.ElevatedButton(
                            "Guardar Configuración de Nube",
                            icon=ft.Icons.CLOUD_UPLOAD_ROUNDED,
                            bgcolor=self.COLOR_INFO,
                            color=ft.Colors.WHITE,
                            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                            on_click=lambda e: self._save_cloud_config()
                        ),
                    ], expand=True, spacing=12, scroll=ft.ScrollMode.AUTO)
                ),
                
                # Tab 3: Logs
                ft.Tab(
                    text="Logs",
                    icon=ft.Icons.HISTORY_ROUNDED,
                    content=ft.Column([
                        ft.Text("Eventos del Sistema de Backups", size=16, weight=ft.FontWeight.BOLD),
                        ft.Divider(),
                        ft.Container(
                            content=self.logs_table,
                            expand=True,
                            scroll=ft.ScrollMode.AUTO,
                        ),
                    ], expand=True, spacing=12)
                ),
            ],
            expand=1
        )
        
        return ft.Column([
            tabs,
        ], expand=True, spacing=0)
    
    def _metric_card(self, title: str, value_text: ft.Text, icon: str, color: str) -> ft.Container:
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
            border_radius=12,
            border=ft.border.all(1, self.COLOR_BORDER),
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
