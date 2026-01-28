import os
import logging
import calendar
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .backup_incremental_service import BackupIncrementalService, BackupInfo
from .restore_service import RestoreService

logger = logging.getLogger(__name__)


class BackupManager:
    def __init__(self, db, backup_dir: str = "backups_incrementales", pg_bin_path: Optional[str] = None):
        self.db = db
        self.backup_incremental_service = BackupIncrementalService(db, backup_dir, pg_bin_path)
        self.restore_service = RestoreService(db, self.backup_incremental_service, pg_bin_path)
        
        self.schedules = {
            'FULL': {'day': 1, 'hour': 0, 'minute': 0},
            'DIFERENCIAL': {'weekday': 6, 'hour': 23, 'minute': 30},
            'INCREMENTAL': {'hour': 23, 'minute': 0}
        }
        
        self.logger = logger
    
    def get_schedule_for_backup_type(self, backup_type: str) -> Dict:
        return self.schedules.get(backup_type, {})
    
    def set_schedule(self, backup_type: str, **kwargs) -> bool:
        if backup_type not in self.schedules:
            self.logger.error(f"Tipo de backup inválido: {backup_type}")
            return False
        
        for key, value in kwargs.items():
            if key in self.schedules[backup_type]:
                self.schedules[backup_type][key] = value
        
        self.logger.info(f"Schedule actualizado para {backup_type}: {self.schedules[backup_type]}")
        return True
    
    def determine_backup_type(self, current_time: Optional[datetime] = None) -> str:
        if current_time is None:
            current_time = datetime.now()
        
        full_schedule = self.schedules['FULL']
        if current_time.day == full_schedule['day'] and \
           current_time.hour == full_schedule['hour'] and \
           current_time.minute == full_schedule['minute']:
            return 'FULL'
        
        dif_schedule = self.schedules['DIFERENCIAL']
        if current_time.weekday() == dif_schedule['weekday'] and \
           current_time.hour == dif_schedule['hour'] and \
           current_time.minute == dif_schedule['minute']:
            return 'DIFERENCIAL'
        
        inc_schedule = self.schedules['INCREMENTAL']
        if current_time.hour == inc_schedule['hour'] and \
           current_time.minute == inc_schedule['minute']:
            return 'INCREMENTAL'
        
        return 'INCREMENTAL'

    def _build_scheduled_datetime(
        self,
        *,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
    ) -> datetime:
        last_day = calendar.monthrange(year, month)[1]
        safe_day = min(max(day, 1), last_day)
        return datetime(year, month, safe_day, hour, minute, 0, 0)

    def get_last_required_run_time(
        self,
        backup_type: str,
        *,
        current_time: Optional[datetime] = None,
    ) -> datetime:
        now = current_time or datetime.now()
        schedule = self.schedules.get(backup_type, {})

        if backup_type == "FULL":
            day = int(schedule.get("day", 1))
            hour = int(schedule.get("hour", 0))
            minute = int(schedule.get("minute", 0))
            this_month = self._build_scheduled_datetime(
                year=now.year,
                month=now.month,
                day=day,
                hour=hour,
                minute=minute,
            )
            if now >= this_month:
                return this_month
            if now.month == 1:
                return self._build_scheduled_datetime(
                    year=now.year - 1,
                    month=12,
                    day=day,
                    hour=hour,
                    minute=minute,
                )
            return self._build_scheduled_datetime(
                year=now.year,
                month=now.month - 1,
                day=day,
                hour=hour,
                minute=minute,
            )

        if backup_type == "DIFERENCIAL":
            weekday = int(schedule.get("weekday", 6))
            hour = int(schedule.get("hour", 23))
            minute = int(schedule.get("minute", 30))
            days_since = (now.weekday() - weekday) % 7
            candidate = (now - timedelta(days=days_since)).replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )
            if candidate > now:
                candidate = candidate - timedelta(days=7)
            return candidate

        if backup_type == "INCREMENTAL":
            hour = int(schedule.get("hour", 23))
            minute = int(schedule.get("minute", 0))
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate > now:
                candidate = candidate - timedelta(days=1)
            return candidate

        return now

    def get_last_backup_time(self, backup_type: str) -> Optional[datetime]:
        query = """
        SELECT fecha_inicio
        FROM seguridad.backup_manifest
        WHERE tipo_backup = %s AND estado = 'COMPLETADO'
        ORDER BY fecha_inicio DESC
        LIMIT 1
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (backup_type,))
                row = cur.fetchone()
                if not row:
                    return None
                return row[0]

    def check_missed_backups(self, current_time: Optional[datetime] = None) -> List[str]:
        now = current_time or datetime.now()
        missed: List[str] = []
        for backup_type in ("FULL", "DIFERENCIAL", "INCREMENTAL"):
            required_time = self.get_last_required_run_time(backup_type, current_time=now)
            last_run = self.get_last_backup_time(backup_type)
            if last_run is None:
                missed.append(backup_type)
                continue
            if hasattr(last_run, "tzinfo") and last_run.tzinfo is not None:
                last_run = last_run.replace(tzinfo=None)
            if last_run < required_time:
                missed.append(backup_type)
        return missed

    def execute_missed_backups(
        self,
        missed_types: List[str],
        *,
        progress_callback=None,
    ) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        ordered = [t for t in ("FULL", "DIFERENCIAL", "INCREMENTAL") if t in missed_types]
        total = len(ordered)
        for index, backup_type in enumerate(ordered, start=1):
            try:
                if progress_callback:
                    progress_callback(backup_type, "running", index, total)
                result = self.execute_scheduled_backup(backup_type)
                ok = bool(result.get("exitoso"))
                results[backup_type] = ok
                if progress_callback:
                    progress_callback(backup_type, "completed" if ok else "failed", index, total)
            except Exception as exc:
                self.logger.error("Error ejecutando backup %s: %s", backup_type, exc)
                results[backup_type] = False
                if progress_callback:
                    progress_callback(backup_type, "failed", index, total)
        return results
    
    def execute_scheduled_backup(self, backup_type: Optional[str] = None) -> Dict:
        if backup_type is None:
            backup_type = self.determine_backup_type()
        
        self.logger.info(f"=== Iniciando backup {backup_type} ===")
        inicio = datetime.now()
        
        try:
            backup_file = self.backup_incremental_service.create_backup(backup_type)
            fin = datetime.now()
            duracion = (fin - inicio).total_seconds()
            
            return {
                'exitoso': True,
                'tipo': backup_type,
                'archivo': backup_file,
                'inicio': inicio,
                'fin': fin,
                'duracion_segundos': duracion,
                'mensaje': f'Backup {backup_type} completado exitosamente en {duracion:.2f}s'
            }
            
        except Exception as e:
            fin = datetime.now()
            duracion = (fin - inicio).total_seconds()
            
            self.logger.error(f"Error en backup {backup_type}: {e}")
            
            return {
                'exitoso': False,
                'tipo': backup_type,
                'inicio': inicio,
                'fin': fin,
                'duracion_segundos': duracion,
                'mensaje': f'Backup {backup_type} falló: {str(e)}',
                'error': str(e)
            }
    
    def get_backup_stats(self) -> Dict:
        query = """
        SELECT
            tipo_backup,
            COUNT(*) as cantidad,
            SUM(tamano_bytes) as total_bytes,
            ROUND(SUM(tamano_bytes) / 1024.0 / 1024.0, 2) as total_mb,
            MIN(fecha_inicio) as primero,
            MAX(fecha_inicio) as ultimo
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO'
        GROUP BY tipo_backup
        ORDER BY tipo_backup
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                stats = {}
                total_tamano = 0
                total_cantidad = 0
                
                for row in rows:
                    tipo = row[0]
                    stats[tipo] = {
                        'cantidad': row[1],
                        'tamano_bytes': row[2],
                        'tamano_mb': row[3],
                        'primero': row[4],
                        'ultimo': row[5]
                    }
                    total_tamano += row[2] or 0
                    total_cantidad += row[1] or 0
                
                stats['_total'] = {
                    'cantidad': total_cantidad,
                    'tamano_bytes': total_tamano,
                    'tamano_mb': round(total_tamano / 1024 / 1024, 2),
                    'tamano_gb': round(total_tamano / 1024 / 1024 / 1024, 2)
                }
                
                return stats
    
    def get_available_restore_points(self, limit: int = 100) -> List[Dict]:
        query = """
        SELECT DISTINCT
            DATE_TRUNC('day', fecha_inicio) as fecha,
            COUNT(*) as cantidad_backups,
            MIN(fecha_inicio) as primero,
            MAX(fecha_inicio) as ultimo
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO'
        GROUP BY DATE_TRUNC('day', fecha_inicio)
        ORDER BY fecha DESC
        LIMIT %s
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall()
                
                return [{
                    'fecha': row[0].date() if row[0] else None,
                    'cantidad_backups': row[1],
                    'primero': row[2],
                    'ultimo': row[3]
                } for row in rows]
    
    def get_backup_chain_info(self, target_date: datetime) -> Dict:
        preview = self.restore_service.preview_restore(target_date)
        
        if not preview['existe']:
            return {
                'existe': False,
                'mensaje': f'No se encontraron backups para la fecha {target_date}'
            }
        
        return preview
    
    def validate_all_backups(self) -> Dict:
        query = """
        SELECT id, tipo_backup, archivo_nombre, checksum_sha256, estado
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO'
        ORDER BY fecha_inicio DESC
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                validaciones = []
                validos = 0
                invalidos = 0
                
                for row in rows:
                    backup_id = row[0]
                    validacion = self.restore_service.validate_backup_chain(backup_id)
                    validaciones.append({
                        'id': backup_id,
                        'tipo': row[1],
                        'archivo': row[2],
                        'valido': validacion['valido'],
                        'mensaje': validacion['mensaje']
                    })
                    
                    if validacion['valido']:
                        validos += 1
                    else:
                        invalidos += 1
                
                return {
                    'total': len(validaciones),
                    'validos': validos,
                    'invalidos': invalidos,
                    'validaciones': validaciones
                }

    def purge_invalid_backups(self) -> int:
        """
        Check all COMPLETED backups in the database.
        If the physical file does not exist, remove the record from the database.
        Returns the number of deleted records.
        """
        query = "SELECT id, archivo_ruta FROM seguridad.backup_manifest WHERE estado = 'COMPLETADO'"
        deleted_count = 0
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                ids_to_delete = []
                for row in rows:
                    backup_id = row[0]
                    ruta = row[1]
                    
                    if not ruta:
                        ids_to_delete.append(backup_id)
                        continue
                        
                    p = Path(ruta)
                    if not p.exists():
                        ids_to_delete.append(backup_id)
                
                if ids_to_delete:
                    # Delete in batch
                    placeholders = ",".join(["%s"] * len(ids_to_delete))
                    del_query = f"DELETE FROM seguridad.backup_manifest WHERE id IN ({placeholders})"
                    cur.execute(del_query, tuple(ids_to_delete))
                    conn.commit()
                    deleted_count = len(ids_to_delete)
                    self.logger.debug(f"Purged {deleted_count} ghost backups from database.")
                    
        return deleted_count
    
    def get_space_usage(self) -> Dict:
        query = """
        SELECT
            tipo_backup,
            SUM(tamano_bytes) as total_bytes,
            COUNT(*) as cantidad
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO'
        GROUP BY tipo_backup
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                tipos = {}
                total_bytes = 0
                
                for row in rows:
                    tipo = row[0]
                    bytes_val = row[1] or 0
                    tipos[tipo] = {
                        'bytes': bytes_val,
                        'mb': round(bytes_val / 1024 / 1024, 2),
                        'gb': round(bytes_val / 1024 / 1024 / 1024, 2),
                        'cantidad': row[2]
                    }
                    total_bytes += bytes_val
                
                return {
                    'por_tipo': tipos,
                    'total': {
                        'bytes': total_bytes,
                        'mb': round(total_bytes / 1024 / 1024, 2),
                        'gb': round(total_bytes / 1024 / 1024 / 1024, 2)
                    }
                }
    
    def get_next_backup_times(self) -> Dict[str, Dict]:
        now = datetime.now()
        result = {}
        
        full_schedule = self.schedules['FULL']
        full_hour = int(full_schedule.get('hour', 0))
        full_minute = int(full_schedule.get('minute', 0))
        full_day = int(full_schedule.get('day', 1))
        next_full = self._build_scheduled_datetime(
            year=now.year,
            month=now.month,
            day=full_day,
            hour=full_hour,
            minute=full_minute,
        )
        if now >= next_full:
            next_month = now.month + 1
            next_year = now.year
            if next_month > 12:
                next_month = 1
                next_year += 1
            next_full = self._build_scheduled_datetime(
                year=next_year,
                month=next_month,
                day=full_day,
                hour=full_hour,
                minute=full_minute,
            )
        result['FULL'] = {
            'next_run': next_full,
            'schedule': f"Día {full_day} a {full_hour:02d}:{full_minute:02d}"
        }

        dif_schedule = self.schedules['DIFERENCIAL']
        dif_weekday = int(dif_schedule.get('weekday', 6))
        dif_hour = int(dif_schedule.get('hour', 23))
        dif_minute = int(dif_schedule.get('minute', 30))
        dif_days_ahead = (dif_weekday - now.weekday()) % 7
        next_dif = now + timedelta(days=dif_days_ahead)
        next_dif = next_dif.replace(hour=dif_hour, minute=dif_minute, second=0, microsecond=0)
        if next_dif <= now:
            next_dif = next_dif + timedelta(days=7)
        weekday_names = {
            0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
            4: "Viernes", 5: "Sábado", 6: "Domingo"
        }
        weekday_label = weekday_names.get(dif_weekday, f"Día {dif_weekday}")
        result['DIFERENCIAL'] = {
            'next_run': next_dif,
            'schedule': f"{weekday_label} a las {dif_hour:02d}:{dif_minute:02d}"
        }

        inc_schedule = self.schedules['INCREMENTAL']
        inc_hour = int(inc_schedule.get('hour', 23))
        inc_minute = int(inc_schedule.get('minute', 0))
        next_inc = now.replace(hour=inc_hour, minute=inc_minute, second=0, microsecond=0)
        if now >= next_inc:
            next_inc = next_inc + timedelta(days=1)
        result['INCREMENTAL'] = {
            'next_run': next_inc,
            'schedule': f"Diario a las {inc_hour:02d}:{inc_minute:02d}"
        }
        
        return result
    
    def restore_to_date(self, target_date: datetime, target_db: Optional[str] = None) -> Dict:
        self.logger.info(f"=== Iniciando restauración a {target_date} ===")
        
        result = self.restore_service.restore_to_date(target_date, target_db)
        
        if result.exitoso:
            self.logger.info(f"Restauración exitosa: {result.mensaje}")
        else:
            self.logger.error(f"Restauración fallida: {result.mensaje}")
        
        return {
            'exitoso': result.exitoso,
            'mensaje': result.mensaje,
            'backups_aplicados': result.backups_aplicados,
            'tiempo_segundos': result.tiempo_segundos,
            'lsn_final': result.lsn_final
        }
    
    def restore_from_backup_id(self, backup_id: int, target_db: Optional[str] = None) -> Dict:
        self.logger.info(f"=== Iniciando restauración desde backup ID {backup_id} ===")
        
        result = self.restore_service.restore_from_backup_id(backup_id, target_db)
        
        return {
            'exitoso': result.exitoso,
            'mensaje': result.mensaje,
            'backups_aplicados': result.backups_aplicados,
            'tiempo_segundos': result.tiempo_segundos,
            'lsn_final': result.lsn_final
        }
    
    def get_status_summary(self) -> Dict:
        stats = self.get_backup_stats()
        space = self.get_space_usage()
        restore_points = self.get_available_restore_points(10)
        next_times = self.get_next_backup_times()
        
        return {
            'estadisticas': stats,
            'espacio_uso': space,
            'puntos_restauracion': restore_points,
            'proximos_backups': next_times,
            'fecha_actual': datetime.now()
        }
