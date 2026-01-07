import os
import logging
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
        next_full = now.replace(day=1, hour=full_schedule['hour'], minute=full_schedule['minute'], second=0, microsecond=0)
        if now >= next_full:
            if now.month == 12:
                next_full = next_full.replace(year=now.year + 1, month=1)
            else:
                next_full = next_full.replace(month=now.month + 1)
        result['FULL'] = {'next_run': next_full, 'schedule': f"Día {full_schedule['day']} a {full_schedule['hour']:02d}:{full_schedule['minute']:02d}"}
        
        dif_schedule = self.schedules['DIFERENCIAL']
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0:
            next_dif = now.replace(hour=dif_schedule['hour'], minute=dif_schedule['minute'], second=0, microsecond=0)
            if now >= next_dif:
                days_until_sunday = 7
                next_dif = next_dif + timedelta(days=days_until_sunday)
                next_dif = next_dif.replace(hour=dif_schedule['hour'], minute=dif_schedule['minute'], second=0, microsecond=0)
        else:
            next_dif = now + timedelta(days=days_until_sunday)
            next_dif = next_dif.replace(hour=dif_schedule['hour'], minute=dif_schedule['minute'], second=0, microsecond=0)
        result['DIFERENCIAL'] = {'next_run': next_dif, 'schedule': f"Domingos a {dif_schedule['hour']:02d}:{dif_schedule['minute']:02d}"}
        
        inc_schedule = self.schedules['INCREMENTAL']
        next_inc = now.replace(hour=inc_schedule['hour'], minute=inc_schedule['minute'], second=0, microsecond=0)
        if now >= next_inc:
            next_inc = next_inc + timedelta(days=1)
        result['INCREMENTAL'] = {'next_run': next_inc, 'schedule': f"Diario a las {inc_schedule['hour']:02d}:{inc_schedule['minute']:02d}"}
        
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
