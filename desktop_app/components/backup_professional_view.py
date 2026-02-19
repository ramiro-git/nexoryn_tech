import flet as ft
from typing import List, Dict, Any, Optional, Callable, Tuple
from datetime import datetime, timedelta
import json
import logging

try:
    from desktop_app.components.generic_table import GenericTable, ColumnConfig, SimpleFilterConfig, AdvancedFilterControl
    from desktop_app.components.button_styles import cancel_button
    from desktop_app.enums import BackupEstado
except ImportError:
    from components.generic_table import GenericTable, ColumnConfig, SimpleFilterConfig, AdvancedFilterControl
    from components.button_styles import cancel_button
    from enums import BackupEstado


class BackupProfessionalView:
    def __init__(
        self,
        page: ft.Page,
        db,
        show_message: Callable,
        ask_confirm: Optional[Callable] = None,
        pg_bin_path: Optional[str] = None,
    ):
        self.page = page
        self.db = db
        self.show_message = show_message
        self.ask_confirm = ask_confirm
        self.pg_bin_path = pg_bin_path
        self.logger = logging.getLogger(__name__)
        
        # Colores
        self.COLOR_PRIMARY = "#4F46E5"
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
        }
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

    def _log_suppressed(self, context: str, exc: Exception) -> None:
        self.logger.debug("%s: %s", context, exc)

    def _track_backup_event(self, *args, **kwargs) -> None:
        """No-op: backup audit logging was intentionally decoupled from DB logs."""
        _ = (args, kwargs)
    
    @property
    def backup_manager(self):
        if self._backup_manager is None:
            from desktop_app.services.backup_manager import BackupManager
            self._backup_manager = BackupManager(self.db, pg_bin_path=self.pg_bin_path)
            # Purgar registros huérfanos (archivos que ya no existen)
            # DESACTIVADO: Evitar borrado accidental por latencia de disco/red
            # try:
            #     purged = self._backup_manager.purge_invalid_backups()
            #     if purged > 0:
            #         print(f"Purgados {purged} registros de backups huérfanos")
            # except Exception as e:
            #     print(f"Error purgando backups inválidos: {e}")
        return self._backup_manager
    
    def _load_cloud_config(self) -> Dict:
        """Carga y retorna la configuración de nube desde la DB."""
        cloud_config = {}
        try:
            stored_cloud = self.db.get_config("backup_cloud_config")
            if stored_cloud:
                if isinstance(stored_cloud, str):
                    cloud_config = json.loads(stored_cloud)
                elif isinstance(stored_cloud, dict):
                    cloud_config = stored_cloud
        except Exception as e:
            print(f"Error cargando config de nube: {e}")
        return cloud_config

    @property
    def cloud_service(self):
        if self._cloud_service is None:
            from desktop_app.services.cloud_storage_service import CloudStorageService
            
            # Cargar configuración desde la DB
            cloud_config = self._load_cloud_config()
            provider = cloud_config.get('provider', 'LOCAL')
            
            self._cloud_service = CloudStorageService(self.db, provider=provider, config=cloud_config)
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
    
    def _load_cloud_config_initial(self):
        """Carga la configuración de nube en la UI al inicializar."""
        try:
            cloud_config = self._load_cloud_config()
            if cloud_config:
                self.cloud_provider.value = cloud_config.get('provider', 'LOCAL')
                self.sync_dir.value = cloud_config.get('sync_dir', '')
                self.enable_sync.value = cloud_config.get('enabled', False)
        except Exception as e:
            print(f"Error cargando configuración de nube al inicializar: {e}")

    def _setup_view(self):
        # Metricas
        self.total_backups_text = ft.Text("—", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT)
        self.last_backup_text = ft.Text("—", size=18, color=self.COLOR_TEXT_MUTED)
        self.next_backup_text = ft.Text("—", size=18, color=self.COLOR_INFO, weight=ft.FontWeight.BOLD)
        
        # Tarjetas de programación
        self.schedule_cards_container = ft.Column([], spacing=12)
        
        # Filtros avanzados
        self.date_from_input = self._create_date_input("Desde")
        self.date_to_input = self._create_date_input("Hasta")
        def _style_filter(control: Any):
            # Same parameters as _style_input in ui_basic.py
            if hasattr(control, "border_color"): control.border_color = "#475569"
            if hasattr(control, "focused_border_color"): control.focused_border_color = self.COLOR_PRIMARY
            if hasattr(control, "border_radius"): control.border_radius = 12
            if hasattr(control, "text_size"): control.text_size = 14
            if hasattr(control, "label_style"): control.label_style = ft.TextStyle(color="#1E293B", size=13, weight=ft.FontWeight.BOLD)
            if hasattr(control, "content_padding"): control.content_padding = ft.padding.all(12)
            if hasattr(control, "bgcolor"): control.bgcolor = "#F8FAFC"
            if hasattr(control, "filled"): control.filled = True
            if hasattr(control, "height"): control.height = 45

        self.type_filter_input = ft.Dropdown(
            label="Tipo de Backup",
            width=180,
            options=[
                ft.dropdown.Option("Todas", "Todos los tipos"),
                ft.dropdown.Option("FULL", "FULL (Completo)"),
                ft.dropdown.Option("DIFERENCIAL", "DIFERENCIAL"),
                ft.dropdown.Option("INCREMENTAL", "INCREMENTAL"),
                ft.dropdown.Option("MANUAL", "Manual"),
            ],
            value="Todas",
            on_change=lambda _: self.backups_table.trigger_refresh() if hasattr(self, "backups_table") else None
        )
        _style_filter(self.type_filter_input)

        # Tabla de backups con GenericTable
        self.backups_table = GenericTable(
            columns=[
                ColumnConfig(
                    key="tipo", 
                    label="Tipo", 
                    width=100,
                    renderer=lambda row: self._get_backup_type_badge(row.get('tipo', 'MANUAL'))
                ),
                ColumnConfig(key="archivo", label="Archivo", width=350),
                ColumnConfig(
                    key="tamano", 
                    label="Tamaño", 
                    width=100,
                    formatter=lambda v, _: self._format_size(v) if v else "0 B"
                ),
                ColumnConfig(
                    key="fecha_inicio", 
                    label="Fecha", 
                    width=160,
                    formatter=lambda v, _: self._format_datetime(v)
                ),
                ColumnConfig(
                    key="estado",
                    label="Estado",
                    width=100,
                    renderer=lambda row: self._get_status_badge(row.get('estado', BackupEstado.PENDIENTE.value))
                ),
                ColumnConfig(
                    key="_actions",
                    label="Acciones",
                    width=180,
                    sortable=False,
                    renderer=lambda row: ft.Row([
                        ft.IconButton(
                            ft.icons.CHECK_CIRCLE,
                            icon_color=self.COLOR_SUCCESS,
                            tooltip="Validar backup",
                            on_click=lambda e: self._validate_backup(row)
                        ),
                        ft.IconButton(
                            ft.icons.CLOUD_UPLOAD,
                            icon_color=self.COLOR_INFO,
                            tooltip="Subir a la nube",
                            on_click=lambda e: self._upload_to_cloud(row)
                        ),
                        ft.IconButton(
                            ft.icons.RESTORE,
                            icon_color=self.COLOR_WARNING,
                            tooltip="Restaurar backup",
                            on_click=lambda e: self._confirm_restore(row)
                        ),
                        ft.IconButton(
                            ft.icons.DELETE,
                            icon_color=self.COLOR_ERROR,
                            tooltip="Eliminar backup",
                            on_click=lambda e: self._delete_backup(row)
                        ),
                    ], spacing=0)
                )
            ],
            data_provider=self._backups_provider,
            advanced_filters=[
                AdvancedFilterControl(name="tipo", control=self.type_filter_input),
                AdvancedFilterControl(name="date_from", control=self.date_from_input),
                AdvancedFilterControl(name="date_to", control=self.date_to_input),
            ],
            show_mass_actions=False,
            auto_load=True,
            page_size=25,
            id_field="id"
        )
        self.backups_table.search_field.hint_text = "Buscar por nombre..."
        
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
        # RETIRADO POR SOLICITUD DEL USUARIO
        # self.retention_full = ...
        # self.retention_dif = ...
        # self.retention_inc = ...
        
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
            hint_text="Ruta a carpeta para sincronizar backups"
        )
        
        self.enable_sync = ft.Switch(
            label="Habilitar sincronización en la nube",
            value=False
        )
        
        # Cargar configuración de nube guardada
        self._load_cloud_config_initial()
        
    def _update_metrics(self):
        try:
            # Cargar estadísticas
            stats = self.backup_manager.get_backup_stats()
            
            if '_total' in stats:
                self.total_backups_text.value = str(stats['_total']['cantidad'])
            
            # Último backup
            backups = self.backup_manager.backup_incremental_service.list_backups(limit=1)
            if backups:
                last_backup = backups[0]
                self.last_backup_text.value = self._format_datetime(last_backup['fecha_inicio']) or "N/A"
            else:
                self.last_backup_text.value = "N/A"
            
            # Próximo backup
            next_times = self.backup_manager.get_next_backup_times()
            if next_times:
                closest_type = min(next_times.keys(),
                                key=lambda k: next_times[k]['next_run'])
                closest = next_times[closest_type]
                self.next_backup_text.value = f"{closest_type} en {self._format_time_until(closest['next_run'])}"
            else:
                self.next_backup_text.value = "No programado"
                
        except Exception as e:
            print(f"Error actualizando métricas: {e}")

    
    def load_data(self):
        try:
            self.loading = True
            self.page.update()
            
            # Actualizar indicadores
            self._update_metrics()
            
            # Cargar tabla de backups
            self._load_backups_table()
            
            # Cargar configuraciones de la DB
            self._load_configs()
            
            self.data_loaded = True
            
        except Exception as e:
            self.show_message(f"Error cargando datos: {str(e)}", "error")
        finally:
            self.loading = False
            self.page.update()

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
                    self.backups_table.trigger_refresh()

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

    def _backups_provider(self, offset: int, limit: int, search: Optional[str], simple: Optional[str], advanced: Dict[str, Any], sorts: List[Tuple[str, str]]) -> Tuple[List[Dict[str, Any]], int]:
        # Fetch fresh data (using a high limit to allow in-memory filtering for now, or updating service)
        # Using 500 limit as a reasonable cap for in-memory filtering of recent backups
        all_data = self.backup_manager.backup_incremental_service.list_backups(limit=500)
        
        filtered = all_data

        # Filter by search (Name)
        if search:
            s = search.lower()
            filtered = [b for b in filtered if s in str(b.get('archivo', '')).lower()]

        # Filter by type (Now in advanced)
        tipo = advanced.get("tipo") if advanced else None
        if tipo and tipo != "Todas":
             filtered = [b for b in filtered if b.get('tipo') == tipo]

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
                    created = b.get('fecha_inicio')
                    if not isinstance(created, datetime):
                        new_filtered.append(b)
                        continue
                        
                    # Normalize to date for comparison, ensure both are naive or aware
                    b_date = created.replace(hour=0, minute=0, second=0, microsecond=0)
                    if b_date.tzinfo is not None:
                        b_date = b_date.replace(tzinfo=None)
                    
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
            filtered.sort(key=lambda x: x.get(key, ""), reverse=reverse)
        else:
            # Default sort desc by date
            filtered.sort(key=lambda x: x.get('fecha_inicio', datetime.min), reverse=True)

        # Paginate
        total = len(filtered)
        paged = filtered[offset : offset + limit]
        
        return paged, total
    
    def _load_backups_table(self):
         # Logic moved to generic table provider
        try:
            if self.db:
                self._track_backup_event("BACKUP", "VIEW")
        except Exception as e:
            self._log_suppressed("log_activity BACKUP VIEW", e)
        self.backups_table.refresh()
        # Ensure metrics are updated whenever table is refreshed
        import threading
        threading.Thread(target=self._update_metrics, daemon=True).start()

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
            BackupEstado.COMPLETADO.value: self.COLOR_SUCCESS,
            BackupEstado.EN_PROGRESO.value: self.COLOR_INFO,
            BackupEstado.FALLIDO.value: self.COLOR_ERROR,
            BackupEstado.PENDIENTE.value: self.COLOR_WARNING
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
                # Log intent
                try:
                    self._track_backup_event("BACKUP", f"EXEC_INIT_{backup_type}", detalle={"tipo": backup_type})
                except Exception as e:
                    self._log_suppressed(f"log_activity EXEC_INIT_{backup_type}", e)

                resultado = self.backup_manager.execute_scheduled_backup(backup_type)
                
                if resultado['exitoso']:
                    # Trazabilidad desacoplada: no se persiste en DB.
                    try:
                        self._track_backup_event(
                            entidad="SISTEMA",
                            accion=f"BACKUP_{backup_type}",
                            resultado="OK",
                            detalle={"mensaje": f"Backup {backup_type} completado", "duracion": resultado.get('duracion_segundos')}
                        )
                    except Exception as e:
                        self._log_suppressed(f"log_activity BACKUP_{backup_type} OK", e)

                    self.show_message(
                        f"Backup {backup_type} creado exitosamente en {resultado['duracion_segundos']:.2f}s",
                        "success"
                    )
                    self.backups_table.refresh()
                else:
                    try:
                        self._track_backup_event(
                            entidad="SISTEMA",
                            accion=f"BACKUP_{backup_type}",
                            resultado="FAIL",
                            detalle={"error": resultado.get('mensaje')}
                        )
                    except Exception as e:
                        self._log_suppressed(f"log_activity BACKUP_{backup_type} FAIL", e)
                    self.show_message(f"Error en backup {backup_type}: {resultado['mensaje']}", "error")
                    
            except Exception as e:
                self.show_message(f"Error creando backup: {str(e)}", "error")
        
        # Ejecutar en thread para no bloquear UI
        import threading
        threading.Thread(target=run_backup, daemon=True).start()
    
    def _validate_backup(self, backup: Dict):
        def run_validation():
            try:
                # Log intent
                try:
                    self._track_backup_event("BACKUP", "VALIDATE_INIT", id_entidad=backup['id'])
                except Exception as e:
                    self._log_suppressed("log_activity VALIDATE_INIT", e)

                result = self.backup_manager.restore_service.validate_backup_chain(backup['id'])
                
                if result['valido']:
                    self.show_message("Backup validado correctamente", "success")
                    try:
                        self._track_backup_event("BACKUP", "VALIDATE_OK", id_entidad=backup['id'])
                    except Exception as e:
                        self._log_suppressed("log_activity VALIDATE_OK", e)
                else:
                    self.show_message(f"Backup inválido: {result['mensaje']}", "warning")
                    try:
                        self._track_backup_event("BACKUP", "VALIDATE_FAIL", id_entidad=backup['id'], resultado="FAIL", detalle={"error": result['mensaje']})
                    except Exception as e:
                        self._log_suppressed("log_activity VALIDATE_FAIL", e)
                    
            except Exception as e:
                self.show_message(f"Error validando backup: {str(e)}", "error")
                try:
                    self._track_backup_event("BACKUP", "VALIDATE_ERROR", id_entidad=backup['id'], resultado="ERROR", detalle={"error": str(e)})
                except Exception as exc:
                    self._log_suppressed("log_activity VALIDATE_ERROR", exc)
        
        import threading
        threading.Thread(target=run_validation, daemon=True).start()
    
    def _upload_to_cloud(self, backup: Dict):
        provider = self.cloud_provider.value
        sync_dir_value = self.sync_dir.value
        enable_sync_value = self.enable_sync.value
        
        def run_upload():
            try:
                try:
                    self._track_backup_event("BACKUP", "UPLOAD_INIT", id_entidad=backup['id'], detalle={"provider": provider})
                except Exception as e:
                    self._log_suppressed("log_activity UPLOAD_INIT", e)

                from pathlib import Path
                backup_file = Path(backup['archivo'])
                
                cloud_config = {
                    'enabled': enable_sync_value,
                    'sync_dir': sync_dir_value,
                    'provider': provider
                }
                
                if provider == "LOCAL":
                    if not sync_dir_value or not sync_dir_value.strip():
                        self.show_message("Error: Debes especificar la carpeta de sincronización para LOCAL", "error")
                        try:
                            self._track_backup_event("BACKUP", "UPLOAD_CONFIG_MISSING", id_entidad=backup['id'], resultado="FAIL", detalle={"error": "sync_dir not configured"})
                        except Exception as e:
                            self._log_suppressed("log_activity UPLOAD_CONFIG_MISSING", e)
                        return
                
                self.db.set_config("backup_cloud_config", json.dumps(cloud_config), tipo='TEXT', descripcion='Configuración de sincronización en la nube')
                
                self._cloud_service = None
                
                result = self.cloud_service.upload_backup(
                    backup_file, 
                    backup['id'], 
                    backup['tipo']
                )
                
                if result.exitoso:
                    self.show_message("Backup subido a la nube exitosamente", "success")
                    try:
                        self._track_backup_event("BACKUP", "UPLOAD_OK", id_entidad=backup['id'], detalle={"url": result.url, "tiempo_segundos": result.tiempo_segundos})
                    except Exception as e:
                        self._log_suppressed("log_activity UPLOAD_OK", e)
                else:
                    self.show_message(f"Error subiendo a la nube: {result.mensaje}", "error")
                    try:
                        self._track_backup_event("BACKUP", "UPLOAD_FAIL", id_entidad=backup['id'], resultado="FAIL", detalle={"error": result.mensaje})
                    except Exception as e:
                        self._log_suppressed("log_activity UPLOAD_FAIL", e)
                    
            except Exception as e:
                self.show_message(f"Error subiendo a la nube: {str(e)}", "error")
                try:
                    self._track_backup_event("BACKUP", "UPLOAD_ERROR", id_entidad=backup['id'], resultado="ERROR", detalle={"error": str(e)})
                except Exception as exc:
                    self._log_suppressed("log_activity UPLOAD_ERROR", exc)
        
        import threading
        threading.Thread(target=run_upload, daemon=True).start()
    
    def _confirm_restore(self, backup: Dict):
        if not self.db: return

        # Log intent
        try:
            self._track_backup_event("BACKUP", "RESTORE_INIT", id_entidad=backup['id'], detalle={"tipo": backup['tipo'], "fecha": str(backup['fecha_inicio'])})
        except Exception as e:
            self._log_suppressed("log_activity RESTORE_INIT", e)

        def on_confirm():
            def run_restore():
                try:
                    # Log execution
                    try:
                        self._track_backup_event("BACKUP", "RESTORE_EXEC", id_entidad=backup['id'])
                    except Exception as e:
                        self._log_suppressed("log_activity RESTORE_EXEC", e)

                    self.show_message("Iniciando restauración... Esto puede tomar varios minutos.", "info")
                    result = self.backup_manager.restore_from_backup_id(backup['id'])
                    if result['exitoso']:
                        self.show_message(f"Restauración completada: {result['mensaje']}", "success")
                        try:
                            self._track_backup_event("BACKUP", "RESTORE_OK", id_entidad=backup['id'])
                        except Exception as e:
                            self._log_suppressed("log_activity RESTORE_OK", e)
                    else:
                        self.show_message(f"Error en restauración: {result['mensaje']}", "error")
                        try:
                            self._track_backup_event("BACKUP", "RESTORE_FAIL", id_entidad=backup['id'], resultado="FAIL", detalle={"error": result['mensaje']})
                        except Exception as e:
                            self._log_suppressed("log_activity RESTORE_FAIL", e)
                except Exception as e:
                    self.show_message(f"Error crítico en restauración: {str(e)}", "error")
                    try:
                        self._track_backup_event("BACKUP", "RESTORE_ERROR", id_entidad=backup['id'], resultado="ERROR", detalle={"error": str(e)})
                    except Exception as exc:
                        self._log_suppressed("log_activity RESTORE_ERROR", exc)
            
            import threading
            threading.Thread(target=run_restore, daemon=True).start()

        if self.ask_confirm:
            # En lugar de confirmación directa, abrimos el wizard que muestra la cadena
            self._run_restore_wizard(backup)
        else:
            # Fallback
            self._manual_ask_confirm(
                 "Confirmar Restauración",
                f"¿Está seguro que desea restaurar el backup del {self._format_datetime(backup['fecha_inicio'])}?\n\n¡ATENCION! Los datos actuales serán reemplazados por los de esta copia.",
                lambda: self._perform_restore(backup),
                self.COLOR_SUCCESS
            )

    def _manual_ask_confirm(self, title, message, on_confirm, color):
         dlg = ft.AlertDialog(
            title=ft.Text(title, weight=ft.FontWeight.BOLD),
            content=ft.Text(message, size=14),
            actions=[
                cancel_button("Cancelar", on_click=lambda e: self.page.close(dlg)),
                ft.ElevatedButton(
                    "Confirmar",
                    on_click=lambda e: (self.page.close(dlg), on_confirm()),
                    bgcolor=color,
                    color=ft.Colors.WHITE,
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8))
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
         self.page.open(dlg)
    
    def _run_restore_wizard(self, backup: Dict):
        preview = self.backup_manager.restore_service.preview_restore(backup['fecha_inicio'])
        
        # Verificar si hay una cadena de backups válida
        if not preview.get('existe', False) or not preview.get('backups'):
            self.show_message(
                f"No se encontró una cadena de backups válida para restaurar desde {self._format_datetime(backup['fecha_inicio'])}. "
                "Asegúrese de que existe un backup FULL previo.",
                "error"
            )
            return
        
        def on_confirm_restore(e):
            self.page.close(wizard_dlg)
            self._perform_restore(backup)
        
        def on_cancel(e):
            self.page.close(wizard_dlg)
        
        cantidad_backups = preview.get('cantidad_backups', len(preview['backups']))
        tamano_total_mb = preview.get('tamano_total_mb', 0)
        
        content = ft.Column([
            ft.Text("Resumen de Restauración", size=18, weight=ft.FontWeight.BOLD),
            ft.Divider(),
            
            ft.Column([
                ft.Row([
                    ft.Icon(ft.icons.FOLDER_SPECIAL_ROUNDED, size=20, color=self.COLOR_INFO),
                    ft.Column([
                        ft.Text("Backup Base", size=12, weight=ft.FontWeight.BOLD),
                        ft.Text(preview['backups'][0]['archivo'] if preview['backups'] else "N/A", size=11),
                    ], spacing=2)
                ], spacing=10),
                
                ft.Row([
                    ft.Icon(ft.icons.LAYERS_ROUNDED, size=20, color=self.COLOR_INFO),
                    ft.Column([
                        ft.Text(f"Backups a aplicar: {cantidad_backups}", size=12, weight=ft.FontWeight.BOLD),
                        ft.Text(f"Tamaño total: {tamano_total_mb:.2f} MB", size=11),
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
            title=ft.Icon(ft.icons.RESTORE_ROUNDED, size=32, color=self.COLOR_WARNING),
            content=content,
            actions=[
                cancel_button("Cancelar", on_click=on_cancel),
                ft.ElevatedButton(
                    "Restaurar Ahora",
                    on_click=on_confirm_restore,
                    bgcolor=self.COLOR_WARNING,
                    color=ft.Colors.WHITE,
                    icon=ft.icons.RESTORE_ROUNDED
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.open(wizard_dlg)
    
    def _perform_restore(self, backup: Dict):
        # This function is now integrated into _confirm_restore's on_confirm
        # Keeping it for now, but it's effectively unused if _confirm_restore calls the new logic directly.
        # If _run_restore_wizard still calls this, it needs to be updated.
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
        if not self.db: return
        import os
        from pathlib import Path

        def on_confirm():
            try:
                # Log intent
                try:
                    self._track_backup_event("BACKUP", "DELETE_EXEC", id_entidad=backup['id'], detalle={"archivo": backup['archivo']})
                except Exception as e:
                    self._log_suppressed("log_activity DELETE_EXEC", e)

                # 1. Eliminar archivo físico
                path = Path(backup['archivo'])
                if path.exists():
                    os.remove(path)
                else:
                    # Intento con path relativo si el absoluto falla (fallback)
                    self.show_message(f"El archivo no se encontró físicamente: {backup['archivo']}", "warning")
                
                # 2. Eliminar de la base de datos (marcar como eliminado o borrar manifest)
                with self.db.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM seguridad.backup_manifest WHERE id = %s", (backup['id'],))
                        conn.commit()
                
                self.show_message("Backup eliminado correctamente", "success")
                self.backups_table.refresh()
                try:
                    self._track_backup_event("BACKUP", "DELETE_OK", id_entidad=backup['id'])
                except Exception as e:
                    self._log_suppressed("log_activity DELETE_OK", e)
            except Exception as e:
                self.show_message(f"Error eliminando backup: {str(e)}", "error")
                try:
                    self._track_backup_event("BACKUP", "DELETE_ERROR", id_entidad=backup['id'], resultado="ERROR", detalle={"error": str(e)})
                except Exception as exc:
                    self._log_suppressed("log_activity DELETE_ERROR", exc)

        if self.ask_confirm:
            self.ask_confirm(
                "Eliminar Backup",
                f"¿Está seguro que desea eliminar permanentemente este backup?\nArchivo: {backup['archivo']}",
                "Eliminar permanentemente",
                on_confirm,
                button_color=self.COLOR_ERROR
            )
        else:
            self._manual_ask_confirm("Eliminar Backup", f"¿Está seguro que desea eliminar este backup?", on_confirm, self.COLOR_ERROR)
    
    def _load_configs(self):
        """Carga todas las configuraciones de la base de datos a los controles de la UI."""
        try:
            # Horarios (ya cargados en backup_manager)
            schedules = self.backup_manager.schedules
            
            self.full_schedule_day.value = str(schedules['FULL'].get('day', 1))
            self.full_schedule_hour.value = str(schedules['FULL'].get('hour', 0))
            
            self.dif_schedule_weekday.value = str(schedules['DIFERENCIAL'].get('weekday', 6))
            self.dif_schedule_hour.value = str(schedules['DIFERENCIAL'].get('hour', 23))
            
            self.inc_schedule_hour.value = str(schedules['INCREMENTAL'].get('hour', 23))
            
            # Retención (ELIMINADO)
            # retention = self.db.get_config("backup_retention")
            # if retention:
            #     if isinstance(retention, str): retention = json.loads(retention)
            #     self.retention_full.value = str(retention.get('full_months', 12))
            #     self.retention_dif.value = str(retention.get('dif_weeks', 8))
            #     self.retention_inc.value = str(retention.get('inc_days', 7))
            
            # Nube (volver a cargar para asegurar que está al día)
            try:
                cloud_config = self._load_cloud_config()
                if cloud_config:
                    self.enable_sync.value = cloud_config.get('enabled', False)
                    self.cloud_provider.value = cloud_config.get('provider', 'LOCAL')
                    self.sync_dir.value = cloud_config.get('sync_dir', '')
            except Exception as e:
                self.logger.error(f"Error cargando config de nube en _load_configs: {e}")
            
            self.page.update()
        except Exception as e:
            self.logger.error(f"Error cargando configuraciones en la vista: {e}")

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
            try:
                self._track_backup_event("BACKUP", "UPDATE_SCHEDULE", detalle={"FULL": self.full_schedule_day.value, "DIF": self.dif_schedule_weekday.value})
            except Exception as e:
                self._log_suppressed("log_activity UPDATE_SCHEDULE", e)
            
        except Exception as e:
            self.show_message(f"Error guardando horarios: {str(e)}", "error")
    
    # def _save_retention(self):
    #     METODO ELIMINADO POR SOLICITUD DEL USUARIO
    #     pass

    def _save_cloud_config(self):
        try:
            cloud_config = {
                'enabled': self.enable_sync.value,
                'sync_dir': self.sync_dir.value,
                'provider': self.cloud_provider.value
            }
            
            # Validar que se especifique carpeta de sync si está habilitado
            if self.enable_sync.value and not self.sync_dir.value:
                self.show_message("Especifica la carpeta de sincronización", "warning")
                return

            # Guardar configuración persistentemente en la DB
            self.db.set_config(
                "backup_cloud_config", 
                json.dumps(cloud_config), 
                tipo='TEXT', 
                descripcion='Configuración de sincronización en la nube'
            )
            
            # Resetear el servicio para que tome la nueva configuración en el próximo acceso
            self._cloud_service = None
            
            self.show_message(f"Configuración de nube guardada con éxito", "success")
            
            try:
                self._track_backup_event("BACKUP", "UPDATE_CLOUD_CONFIG", detalle=cloud_config)
            except Exception as e:
                self._log_suppressed("log_activity UPDATE_CLOUD_CONFIG", e)

        except Exception as e:
            self.show_message(f"Error guardando configuración de nube: {str(e)}", "error")

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
    
    def build(self) -> ft.Control:
        return ft.Container(
            content=ft.Column([
                # Header
                ft.Row([
                    ft.Column([
                        ft.Text("Sistema de Backups Profesionales", size=24, weight=ft.FontWeight.BOLD, color=self.COLOR_TEXT),
                        ft.Text("FULL + DIFERENCIAL + INCREMENTAL - Restauración concatenable", size=12, color=self.COLOR_TEXT_MUTED),
                    ], spacing=2),
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),

                # Métricas
                ft.Row([
                    self._metric_card(
                        "Total Backups",
                        self.total_backups_text,
                        ft.icons.FOLDER_SPECIAL_ROUNDED,
                        self.COLOR_PRIMARY
                    ),
                    self._metric_card(
                        "Último Backup",
                        self.last_backup_text,
                        ft.icons.ACCESS_TIME_ROUNDED,
                        self.COLOR_SUCCESS
                    ),
                    self._metric_card(
                        "Próximo Backup",
                        self.next_backup_text,
                        ft.icons.SCHEDULE_ROUNDED,
                        self.COLOR_INFO
                    ),
                ], spacing=12),

                # Acciones rápidas
                ft.Container(
                    content=ft.Column([
                        ft.Text("Ejecutar Backup Ahora", size=16, weight=ft.FontWeight.BOLD),
                        ft.Row([
                            self._action_button(
                                "FULL",
                                ft.icons.CALENDAR_MONTH_ROUNDED,
                                self.COLOR_SUCCESS,
                                lambda e: self._execute_backup('FULL')
                            ),
                            self._action_button(
                                "DIFERENCIAL",
                                ft.icons.DATE_RANGE_ROUNDED,
                                self.COLOR_INFO,
                                lambda e: self._execute_backup('DIFERENCIAL')
                            ),
                            self._action_button(
                                "INCREMENTAL",
                                ft.icons.TODAY_ROUNDED,
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

                ft.Container(height=4),

                # Configuración FULL
                ft.Container(
                    content=ft.Column([
                        ft.Row([
                            ft.Icon(ft.icons.CALENDAR_MONTH_ROUNDED, color=self.COLOR_SUCCESS, size=24),
                            ft.Column([
                                ft.Text("Backup FULL (Mensual)", size=14, weight=ft.FontWeight.BOLD),
                                ft.Text("Backup completo mensual - base de todos los backups", size=12, color=self.COLOR_TEXT_MUTED),
                            ], spacing=2)
                        ], spacing=12),
                        ft.Row([
                            self.full_schedule_day,
                            ft.Text("a las", size=14, weight=ft.FontWeight.W_500),
                            self.full_schedule_hour,
                        ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
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
                            ft.Icon(ft.icons.DATE_RANGE_ROUNDED, color=self.COLOR_INFO, size=24),
                            ft.Column([
                                ft.Text("Backup DIFERENCIAL (Semanal)", size=14, weight=ft.FontWeight.BOLD),
                                ft.Text("Cambios desde el último backup FULL", size=12, color=self.COLOR_TEXT_MUTED),
                            ], spacing=2)
                        ], spacing=12),
                        ft.Row([
                            self.dif_schedule_weekday,
                            ft.Text("a las", size=14, weight=ft.FontWeight.W_500),
                            self.dif_schedule_hour,
                        ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
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
                            ft.Icon(ft.icons.TODAY_ROUNDED, color=self.COLOR_WARNING, size=24),
                            ft.Column([
                                ft.Text("Backup INCREMENTAL (Diario)", size=14, weight=ft.FontWeight.BOLD),
                                ft.Text("Cambios desde el último backup", size=12, color=self.COLOR_TEXT_MUTED),
                            ], spacing=2)
                        ], spacing=12),
                        ft.Row([
                            ft.Text("Todos los días a las", size=14, weight=ft.FontWeight.W_500),
                            self.inc_schedule_hour,
                        ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                    ], spacing=12),
                    padding=16,
                    bgcolor=self.COLOR_CARD,
                    border_radius=12,
                    border=ft.border.all(1, self.COLOR_BORDER),
                ),

                ft.Container(height=20),

                ft.ElevatedButton(
                    "Guardar Horarios",
                    icon=ft.icons.SAVE_ROUNDED,
                    bgcolor=self.COLOR_PRIMARY,
                    color=ft.Colors.WHITE,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=8),
                        padding=ft.padding.symmetric(horizontal=20, vertical=12)
                    ),
                    on_click=lambda e: self._save_schedule()
                ),

                ft.Divider(height=20),

                # Configuración de retención
                # RETIRADO POR SOLICITUD DEL USUARIO
                # ft.Text("Política de Retención", ...),
                # ...

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
                    icon=ft.icons.CLOUD_UPLOAD_ROUNDED,
                    bgcolor=self.COLOR_INFO,
                    color=ft.Colors.WHITE,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=8),
                        padding=ft.padding.symmetric(horizontal=20, vertical=12)
                    ),
                    on_click=lambda e: self._save_cloud_config()
                ),

                ft.Divider(height=20),

                # Historial de backups
                ft.Text("Historial de Backups", size=16, weight=ft.FontWeight.BOLD),
                ft.Container(
                    content=self.backups_table.build(),
                    height=600,
                    padding=16,
                    bgcolor=self.COLOR_CARD,
                    border_radius=12,
                ),
            ], spacing=12, scroll=ft.ScrollMode.AUTO),
            expand=True
        )
    
    def _metric_card(self, title: str, value_text: ft.Text, icon: str, color: str) -> ft.Container:
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
