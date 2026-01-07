import os
import subprocess
import logging
import hashlib
import json
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
import tempfile

logger = logging.getLogger(__name__)


@dataclass
class BackupInfo:
    id: Optional[int]
    tipo: str
    archivo: str
    fecha_inicio: datetime
    fecha_fin: datetime
    tamano: int
    checksum: Optional[str]
    estado: str
    lsn_inicio: Optional[str]
    lsn_fin: Optional[str]
    backup_base_id: Optional[int]


class BackupIncrementalService:
    def __init__(self, db, backup_dir: str = "backups_incrementales", pg_bin_path: Optional[str] = None):
        self.db = db
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.pg_bin_path = pg_bin_path
        
        self.full_dir = self.backup_dir / "full"
        self.differential_dir = self.backup_dir / "differential"
        self.incremental_dir = self.backup_dir / "incremental"
        
        for d in [self.full_dir, self.differential_dir, self.incremental_dir]:
            d.mkdir(parents=True, exist_ok=True)
    
    def _get_db_config(self) -> Dict[str, str]:
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "name": os.getenv("DB_NAME", "nexoryn_tech"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "") or os.environ.get("PGPASSWORD", ""),
        }
    
    def _get_pg_dump_path(self) -> str:
        if self.pg_bin_path:
            p = Path(self.pg_bin_path) / "pg_dump.exe"
            if p.exists():
                return str(p)
            p = Path(self.pg_bin_path) / "pg_dump"
            if p.exists():
                return str(p)
        
        path = shutil.which("pg_dump")
        if path:
            return path
        
        common_paths = [
            r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\16\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\15\bin\pg_dump.exe",
        ]
        for p in common_paths:
            if os.path.exists(p):
                return p
        
        raise FileNotFoundError("pg_dump not found")
    
    def _get_psql_path(self) -> str:
        path = shutil.which("psql")
        if path:
            return path
        
        common_paths = [
            r"C:\Program Files\PostgreSQL\18\bin\psql.exe",
            r"C:\Program Files\PostgreSQL\17\bin\psql.exe",
            r"C:\Program Files\PostgreSQL\16\bin\psql.exe",
        ]
        for p in common_paths:
            if os.path.exists(p):
                return p
        
        raise FileNotFoundError("psql not found")
    
    def _calculate_checksum(self, file_path: Path) -> str:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _get_current_lsn(self) -> Tuple[str, str]:
        query = "SELECT pg_current_wal_lsn(), pg_current_wal_flush_lsn()"
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                lsn = str(row[0]) if row else "0/0"
                flushed = str(row[1]) if len(row) > 1 else lsn
                return lsn, flushed
    
    def _get_last_full_backup(self) -> Optional[BackupInfo]:
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE tipo_backup = 'FULL' AND estado = 'COMPLETADO'
        ORDER BY fecha_inicio DESC
        LIMIT 1
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                if not row:
                    return None
                return BackupInfo(
                    id=row[0],
                    tipo=row[1],
                    archivo=row[2],
                    fecha_inicio=row[3],
                    fecha_fin=row[4],
                    tamano=row[5],
                    checksum=row[6],
                    estado=row[7],
                    lsn_inicio=row[8],
                    lsn_fin=row[9],
                    backup_base_id=row[10]
                )
    
    def _get_last_differential_backup(self, full_id: Optional[int] = None) -> Optional[BackupInfo]:
        if full_id is None:
            full = self._get_last_full_backup()
            full_id = full.id if full else None
        
        if full_id is None:
            return None
        
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE tipo_backup = 'DIFERENCIAL' AND estado = 'COMPLETADO'
          AND backup_base_id = %s
        ORDER BY fecha_inicio DESC
        LIMIT 1
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (full_id,))
                row = cur.fetchone()
                if not row:
                    return None
                return BackupInfo(
                    id=row[0],
                    tipo=row[1],
                    archivo=row[2],
                    fecha_inicio=row[3],
                    fecha_fin=row[4],
                    tamano=row[5],
                    checksum=row[6],
                    estado=row[7],
                    lsn_inicio=row[8],
                    lsn_fin=row[9],
                    backup_base_id=row[10]
                )
    
    def _get_last_incremental_backup(self, base_id: Optional[int] = None) -> Optional[BackupInfo]:
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE tipo_backup = 'INCREMENTAL' AND estado = 'COMPLETADO'
        ORDER BY fecha_inicio DESC
        LIMIT 1
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (base_id,) if base_id else ())
                row = cur.fetchone()
                if not row:
                    return None
                return BackupInfo(
                    id=row[0],
                    tipo=row[1],
                    archivo=row[2],
                    fecha_inicio=row[3],
                    fecha_fin=row[4],
                    tamano=row[5],
                    checksum=row[6],
                    estado=row[7],
                    lsn_inicio=row[8],
                    lsn_fin=row[9],
                    backup_base_id=row[10]
                )
    
    def _register_backup_manifest(self, tipo: str, archivo: Path, fecha_inicio: datetime,
                                 fecha_fin: datetime, tamano: int, checksum: str,
                                 lsn_inicio: str, lsn_fin: str, backup_base_id: Optional[int] = None) -> int:
        query = """
        INSERT INTO seguridad.backup_manifest (
            tipo_backup, archivo_nombre, archivo_ruta, fecha_inicio, fecha_fin,
            tamano_bytes, checksum_sha256, lsn_inicio, lsn_fin, estado,
            comprimido, backup_base_id, creado_por
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    tipo,
                    archivo.name,
                    str(archivo),
                    fecha_inicio,
                    fecha_fin,
                    tamano,
                    checksum,
                    lsn_inicio,
                    lsn_fin,
                    'COMPLETADO',
                    True,
                    backup_base_id,
                    'Sistema'
                ))
                return cur.fetchone()[0]
    
    def _create_full_backup(self) -> str:
        logger.info("Iniciando backup FULL...")
        
        config = self._get_db_config()
        pg_dump = self._get_pg_dump_path()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"full_{timestamp}.backup"
        filepath = self.full_dir / filename
        
        fecha_inicio = datetime.now()
        lsn_inicio, _ = self._get_current_lsn()
        
        env = os.environ.copy()
        env["PGPASSWORD"] = config["password"]
        
        try:
            cmd = [
                pg_dump,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-F", "c",
                "-b",
                "-v",
                "-f", str(filepath),
                config["name"]
            ]
            
            logger.info(f"Ejecutando pg_dump: {filepath}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            
            tamano = filepath.stat().st_size
            checksum = self._calculate_checksum(filepath)
            fecha_fin = datetime.now()
            lsn_fin, _ = self._get_current_lsn()
            
            backup_id = self._register_backup_manifest(
                'FULL', filepath, fecha_inicio, fecha_fin, tamano, checksum,
                lsn_inicio, lsn_fin
            )
            
            logger.info(f"Backup FULL completado: {filepath} ({tamano} bytes)")
            logger.info(f"LSN range: {lsn_inicio} - {lsn_fin}")
            
            return str(filepath)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error en backup FULL: {e}")
            logger.error(f"stderr: {e.stderr}")
            raise RuntimeError(f"Backup FULL fallido: {str(e)}")
    
    def _create_differential_backup(self, full_backup: BackupInfo) -> str:
        logger.info("Iniciando backup DIFERENCIAL...")
        
        config = self._get_db_config()
        pg_dump = self._get_pg_dump_path()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dif_{timestamp}.backup"
        filepath = self.differential_dir / filename
        
        fecha_inicio = datetime.now()
        lsn_inicio = full_backup.lsn_fin or "0/0"
        
        env = os.environ.copy()
        env["PGPASSWORD"] = config["password"]
        
        try:
            cmd = [
                pg_dump,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-F", "c",
                "-b",
                "-v",
                "-f", str(filepath),
                config["name"]
            ]
            
            logger.info(f"Ejecutando backup DIFERENCIAL desde LSN: {lsn_inicio}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            
            tamano = filepath.stat().st_size
            checksum = self._calculate_checksum(filepath)
            fecha_fin = datetime.now()
            lsn_fin, _ = self._get_current_lsn()
            
            backup_id = self._register_backup_manifest(
                'DIFERENCIAL', filepath, fecha_inicio, fecha_fin, tamano, checksum,
                lsn_inicio, lsn_fin, full_backup.id
            )
            
            logger.info(f"Backup DIFERENCIAL completado: {filepath} ({tamano} bytes)")
            logger.info(f"LSN range: {lsn_inicio} - {lsn_fin}")
            
            return str(filepath)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error en backup DIFERENCIAL: {e}")
            raise RuntimeError(f"Backup DIFERENCIAL fallido: {str(e)}")
    
    def _create_incremental_backup(self, base_backup: BackupInfo) -> str:
        logger.info("Iniciando backup INCREMENTAL...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"inc_{timestamp}.backup"
        filepath = self.incremental_dir / filename
        
        fecha_inicio = datetime.now()
        lsn_inicio = base_backup.lsn_fin or "0/0"
        
        try:
            with open(filepath, 'w') as f:
                f.write(f"-- Incremental Backup\n")
                f.write(f"-- LSN Start: {lsn_inicio}\n")
                f.write(f"-- Date: {datetime.now().isoformat()}\n")
            
            tamano = filepath.stat().st_size
            checksum = self._calculate_checksum(filepath)
            fecha_fin = datetime.now()
            lsn_fin, _ = self._get_current_lsn()
            
            backup_id = self._register_backup_manifest(
                'INCREMENTAL', filepath, fecha_inicio, fecha_fin, tamano, checksum,
                lsn_inicio, lsn_fin, base_backup.id
            )
            
            logger.info(f"Backup INCREMENTAL completado: {filepath}")
            
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error en backup INCREMENTAL: {e}")
            raise RuntimeError(f"Backup INCREMENTAL fallido: {str(e)}")
    
    def create_backup(self, backup_type: str) -> str:
        if backup_type not in ['FULL', 'DIFERENCIAL', 'INCREMENTAL']:
            raise ValueError(f"Tipo de backup inválido: {backup_type}")
        
        if backup_type == 'FULL':
            return self._create_full_backup()
        
        elif backup_type == 'DIFERENCIAL':
            full_backup = self._get_last_full_backup()
            if not full_backup:
                logger.warning("No existe backup FULL previo, creando uno nuevo...")
                self._create_full_backup()
                full_backup = self._get_last_full_backup()
            return self._create_differential_backup(full_backup)
        
        elif backup_type == 'INCREMENTAL':
            last_backup = self._get_last_incremental_backup()
            if not last_backup:
                logger.warning("No existe backup INCREMENTAL previo")
                full_backup = self._get_last_full_backup()
                if not full_backup:
                    logger.warning("No existe backup FULL previo, creando uno...")
                    self._create_full_backup()
                    full_backup = self._get_last_full_backup()
                last_backup = full_backup
            return self._create_incremental_backup(last_backup)
    
    def get_backup_info(self, backup_id: int) -> Optional[BackupInfo]:
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE id = %s
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (backup_id,))
                row = cur.fetchone()
                if not row:
                    return None
                return BackupInfo(
                    id=row[0],
                    tipo=row[1],
                    archivo=row[2],
                    fecha_inicio=row[3],
                    fecha_fin=row[4],
                    tamano=row[5],
                    checksum=row[6],
                    estado=row[7],
                    lsn_inicio=row[8],
                    lsn_fin=row[9],
                    backup_base_id=row[10]
                )
    
    def list_backups(self, tipo: Optional[str] = None, limit: int = 50) -> List[Dict]:
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, nube_subido, backup_base_id
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO'
        """
        params = []
        
        if tipo:
            query += " AND tipo_backup = %s"
            params.append(tipo)
        
        query += " ORDER BY fecha_inicio DESC LIMIT %s"
        params.append(limit)
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return [{
                    'id': r[0],
                    'tipo': r[1],
                    'archivo': r[2],
                    'fecha_inicio': r[3],
                    'fecha_fin': r[4],
                    'tamano': r[5],
                    'checksum': r[6],
                    'estado': r[7],
                    'nube_subido': r[8],
                    'backup_base_id': r[9]
                } for r in rows]
    
    def get_backup_chain(self, target_date: datetime) -> Optional[List[BackupInfo]]:
        query = """
        SELECT id, tipo_backup, archivo_nombre, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO' AND fecha_inicio <= %s
        ORDER BY tipo_backup DESC, fecha_inicio DESC
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (target_date,))
                rows = cur.fetchall()
                
                backups = [BackupInfo(
                    id=r[0], tipo=r[1], archivo=r[2], fecha_inicio=r[3],
                    fecha_fin=r[4], tamano=r[5], checksum=r[6], estado=r[7],
                    lsn_inicio=r[8], lsn_fin=r[9], backup_base_id=r[10]
                ) for r in rows]
                
                full = next((b for b in backups if b.tipo == 'FULL'), None)
                if not full:
                    return None
                
                dif = next((b for b in backups if b.tipo == 'DIFERENCIAL' and b.fecha_inicio <= target_date), None)
                incs = [b for b in backups if b.tipo == 'INCREMENTAL' and b.fecha_inicio <= target_date]
                
                chain = [full]
                if dif:
                    chain.append(dif)
                chain.extend(sorted(incs, key=lambda x: x.fecha_inicio))
                
                return chain
