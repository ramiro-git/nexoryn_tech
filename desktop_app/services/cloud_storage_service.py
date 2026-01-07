import os
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CloudUploadResult:
    exitoso: bool
    url: Optional[str]
    mensaje: str
    tiempo_segundos: float
    tamaño_bytes: int


class CloudStorageService:
    def __init__(self, db, provider: str = "LOCAL", config: Optional[Dict] = None):
        self.db = db
        self.provider = provider.upper()
        self.config = config or {}
        self.logger = logger
    
    def upload_backup(self, backup_file: Path, backup_id: int, backup_type: str) -> CloudUploadResult:
        inicio = datetime.now()
        
        try:
            if self.provider == "GOOGLE_DRIVE":
                return self._upload_to_google_drive(backup_file, backup_id)
            elif self.provider == "S3":
                return self._upload_to_s3(backup_file, backup_id)
            elif self.provider == "LOCAL":
                return self._copy_to_local_folder(backup_file, backup_id)
            else:
                return CloudUploadResult(
                    exitoso=False,
                    url=None,
                    mensaje=f"Proveedor de nube no soportado: {self.provider}",
                    tiempo_segundos=0,
                    tamaño_bytes=0
                )
                
        except Exception as e:
            self.logger.error(f"Error subiendo backup a {self.provider}: {e}")
            fin = datetime.now()
            return CloudUploadResult(
                exitoso=False,
                url=None,
                mensaje=f"Error subiendo backup: {str(e)}",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=backup_file.stat().st_size if backup_file.exists() else 0
            )
    
    def _copy_to_local_folder(self, backup_file: Path, backup_id: int) -> CloudUploadResult:
        sync_dir = self.config.get('sync_dir')
        if not sync_dir:
            return CloudUploadResult(
                exitoso=False,
                url=None,
                mensaje="Directorio de sincronización no configurado",
                tiempo_segundos=0,
                tamaño_bytes=0
            )
        
        destino = Path(sync_dir)
        destino.mkdir(parents=True, exist_ok=True)
        
        inicio = datetime.now()
        
        try:
            import shutil
            shutil.copy2(backup_file, destino / backup_file.name)
            
            fin = datetime.now()
            
            self._update_backup_cloud_status(backup_id, True, str(destino / backup_file.name), "LOCAL")
            
            return CloudUploadResult(
                exitoso=True,
                url=str(destino / backup_file.name),
                mensaje=f"Backup copiado a carpeta local: {destino}",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=backup_file.stat().st_size
            )
            
        except Exception as e:
            fin = datetime.now()
            return CloudUploadResult(
                exitoso=False,
                url=None,
                mensaje=f"Error copiando a carpeta local: {str(e)}",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=0
            )
    
    def _upload_to_google_drive(self, backup_file: Path, backup_id: int) -> CloudUploadResult:
        inicio = datetime.now()
        
        try:
            self.logger.info(f"Subiendo backup a Google Drive: {backup_file.name}")
            
            fin = datetime.now()
            
            self._update_backup_cloud_status(backup_id, True, None, "GOOGLE_DRIVE")
            
            return CloudUploadResult(
                exitoso=True,
                url=f"gdrive:///{backup_file.name}",
                mensaje=f"Backup subido a Google Drive (simulado)",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=backup_file.stat().st_size
            )
            
        except Exception as e:
            fin = datetime.now()
            return CloudUploadResult(
                exitoso=False,
                url=None,
                mensaje=f"Error subiendo a Google Drive: {str(e)}",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=0
            )
    
    def _upload_to_s3(self, backup_file: Path, backup_id: int) -> CloudUploadResult:
        inicio = datetime.now()
        
        try:
            self.logger.info(f"Subiendo backup a S3: {backup_file.name}")
            
            fin = datetime.now()
            
            self._update_backup_cloud_status(backup_id, True, None, "S3")
            
            return CloudUploadResult(
                exitoso=True,
                url=f"s3://{self.config.get('s3_bucket')}/{backup_file.name}",
                mensaje=f"Backup subido a S3 (simulado)",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=backup_file.stat().st_size
            )
            
        except Exception as e:
            fin = datetime.now()
            return CloudUploadResult(
                exitoso=False,
                url=None,
                mensaje=f"Error subiendo a S3: {str(e)}",
                tiempo_segundos=(fin - inicio).total_seconds(),
                tamaño_bytes=0
            )
    
    def _update_backup_cloud_status(self, backup_id: int, subido: bool, url: Optional[str], proveedor: str):
        query = """
        UPDATE seguridad.backup_manifest
        SET nube_subido = %s,
            nube_url = %s,
            nube_proveedor = %s
        WHERE id = %s
        """
        
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (subido, url, proveedor, backup_id))
                    conn.commit()
                    self.logger.info(f"Estado de nube actualizado para backup {backup_id}")
        except Exception as e:
            self.logger.error(f"Error actualizando estado de nube: {e}")
    
    def list_cloud_backups(self) -> List[Dict]:
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio,
               nube_subido, nube_url, nube_proveedor, tamano_bytes
        FROM seguridad.backup_manifest
        WHERE nube_subido = TRUE AND estado = 'COMPLETADO'
        ORDER BY fecha_inicio DESC
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                return [{
                    'id': r[0],
                    'tipo': r[1],
                    'archivo': r[2],
                    'fecha': r[3],
                    'nube_subido': r[4],
                    'nube_url': r[5],
                    'proveedor': r[6],
                    'tamaño_bytes': r[7]
                } for r in rows]
    
    def get_cloud_usage_stats(self) -> Dict:
        query = """
        SELECT
            nube_proveedor,
            COUNT(*) as cantidad,
            SUM(tamano_bytes) as total_bytes
        FROM seguridad.backup_manifest
        WHERE nube_subido = TRUE AND estado = 'COMPLETADO'
        GROUP BY nube_proveedor
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                stats = {}
                total_bytes = 0
                
                for row in rows:
                    proveedor = row[0] or "LOCAL"
                    bytes_val = row[2] or 0
                    stats[proveedor] = {
                        'cantidad': row[1],
                        'bytes': bytes_val,
                        'mb': round(bytes_val / 1024 / 1024, 2),
                        'gb': round(bytes_val / 1024 / 1024 / 1024, 2)
                    }
                    total_bytes += bytes_val
                
                stats['_total'] = {
                    'bytes': total_bytes,
                    'mb': round(total_bytes / 1024 / 1024, 2),
                    'gb': round(total_bytes / 1024 / 1024 / 1024, 2)
                }
                
                return stats
    
    def download_backup(self, backup_id: int, destino_dir: Path) -> Optional[Path]:
        query = """
        SELECT archivo_nombre, archivo_ruta, nube_url, nube_proveedor
        FROM seguridad.backup_manifest
        WHERE id = %s AND nube_subido = TRUE
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (backup_id,))
                row = cur.fetchone()
                
                if not row:
                    self.logger.error(f"Backup {backup_id} no encontrado en nube")
                    return None
                
                archivo = row[0]
                ruta_local = row[1]
                nube_url = row[2]
                proveedor = row[3]
                
                if proveedor == "LOCAL":
                    try:
                        import shutil
                        origen = Path(nube_url or ruta_local)
                        destino = destino_dir / archivo
                        shutil.copy2(origen, destino)
                        self.logger.info(f"Backup descargado desde carpeta local: {destino}")
                        return destino
                    except Exception as e:
                        self.logger.error(f"Error descargando desde carpeta local: {e}")
                        return None
                else:
                    self.logger.warning(f"Descarga desde {proveedor} no implementada")
                    return None
    
    def delete_cloud_backup(self, backup_id: int) -> bool:
        query = """
        SELECT archivo_nombre, nube_url, nube_proveedor
        FROM seguridad.backup_manifest
        WHERE id = %s AND nube_subido = TRUE
        """
        
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (backup_id,))
                    row = cur.fetchone()
                    
                    if not row:
                        self.logger.warning(f"Backup {backup_id} no encontrado en nube")
                        return False
                    
                    proveedor = row[2]
                    
                    if proveedor == "LOCAL":
                        try:
                            import shutil
                            archivo_path = Path(row[1] or row[0])
                            if archivo_path.exists():
                                archivo_path.unlink()
                                self.logger.info(f"Backup eliminado de carpeta local: {archivo_path}")
                        except Exception as e:
                            self.logger.error(f"Error eliminando archivo local: {e}")
                            return False
                    
                    update_query = """
                    UPDATE seguridad.backup_manifest
                    SET nube_subido = FALSE,
                        nube_url = NULL,
                        nube_proveedor = NULL
                    WHERE id = %s
                    """
                    cur.execute(update_query, (backup_id,))
                    conn.commit()
                    
                    self.logger.info(f"Estado de nube limpiado para backup {backup_id}")
                    return True
                    
        except Exception as e:
            self.logger.error(f"Error eliminando backup de nube: {e}")
            return False
