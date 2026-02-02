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
        
        # Usar ruta absoluta para evitar problemas con directorio de trabajo
        backup_path = Path(backup_dir)
        if not backup_path.is_absolute():
            # Si es relativa, hacerla relativa al directorio del proyecto
            # Buscar el directorio raíz del proyecto (donde está desktop_app)
            current_file = Path(__file__).resolve()
            project_root = current_file.parent.parent.parent  # services -> desktop_app -> project
            backup_path = project_root / backup_dir
        
        self.backup_dir = backup_path
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.pg_bin_path = pg_bin_path
        
        self.full_dir = self.backup_dir / "full"
        self.differential_dir = self.backup_dir / "differential"
        self.incremental_dir = self.backup_dir / "incremental"
        
        for d in [self.full_dir, self.differential_dir, self.incremental_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self.stats_dir = self.backup_dir / "stats"
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Backup directory inicializado: {self.backup_dir}")
    
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
            r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
        ]
        for p in common_paths:
            if os.path.exists(p):
                return p
        
        raise FileNotFoundError("pg_dump not found")
    
    def _calculate_checksum(self, file_path: Path) -> str:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _get_current_table_stats(self) -> Dict[str, int]:
        """
        Returns a dictionary {schema.table: total_modifications}
        total_modifications = n_tup_ins + n_tup_upd + n_tup_del
        """
        query = """
            SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del
            FROM pg_stat_user_tables
            WHERE schemaname != 'seguridad'
        """
        stats = {}
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                for row in cur.fetchall():
                    schema, table = row[0], row[1]
                    total_ops = (row[2] or 0) + (row[3] or 0) + (row[4] or 0)
                    key = f"{schema}.{table}"
                    stats[key] = total_ops
        return stats

    def _save_stats(self, backup_filename: str, stats: Dict[str, int]):
        stats_file = self.stats_dir / f"{backup_filename}.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f)

    def _load_stats(self, backup_filename: str) -> Dict[str, int]:
        stats_file = self.stats_dir / f"{backup_filename}.json"
        if not stats_file.exists():
            return {}
        with open(stats_file, 'r') as f:
            return json.load(f)

    def _get_changed_tables(self, base_stats: Dict[str, int], current_stats: Dict[str, int]) -> List[str]:
        changed = []
        for key, curr_val in current_stats.items():
            prev_val = base_stats.get(key, -1)
            # If table is new or ops count increased, it changed.
            # Note: if postgres restarted stats, curr_val might be smaller, count as changed to be safe.
            if prev_val == -1 or curr_val != prev_val:
                changed.append(key)
        return changed

    def _get_last_full_backup(self) -> Optional[BackupInfo]:
        query = """
        SELECT id, tipo_backup, archivo_ruta, fecha_inicio, fecha_fin,
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
                if not row: return None
                return BackupInfo(*row)

    def _get_last_backup(self) -> Optional[BackupInfo]:
        query = """
        SELECT id, tipo_backup, archivo_ruta, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE estado = 'COMPLETADO'
        ORDER BY fecha_inicio DESC
        LIMIT 1
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                if not row: return None
                return BackupInfo(*row)
    
    def _sync_manifest_sequence(self):
        """Asegura que la secuencia de IDs esté sincronizada con el máximo ID actual."""
        # Al usar setval con el valor actual del ID, el próximo nextval() devolverá el siguiente operando
        query = "SELECT setval('seguridad.backup_manifest_id_seq', COALESCE((SELECT MAX(id) FROM seguridad.backup_manifest), 1))"
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()
        except Exception as e:
            logger.warning(f"No se pudo sincronizar la secuencia de backup_manifest: {e}")

    def _register_backup_manifest(self, tipo: str, archivo: Path, fecha_inicio: datetime,
                                 fecha_fin: datetime, tamano: int, checksum: str,
                                 lsn_inicio: str = "0/0", lsn_fin: str = "0/0", backup_base_id: Optional[int] = None) -> int:
        
        # Sincronizar secuencia antes de insertar para evitar "llave duplicada"
        self._sync_manifest_sequence()
        
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
                    tipo, archivo.name, str(archivo), fecha_inicio, fecha_fin,
                    tamano, checksum, lsn_inicio, lsn_fin, 'COMPLETADO', True,
                    backup_base_id, 'Sistema'
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
        
        env = os.environ.copy()
        env["PGPASSWORD"] = config["password"]
        
        try:
            # Capture stats BEFORE dump (or after? usually better to capture close to dump)
            current_stats = self._get_current_table_stats()
            
            cmd = [
                pg_dump,
                "-h", config["host"], "-p", config["port"], "-U", config["user"],
                "-F", "c", "-b", "-v",
                # Excluir tablas de sistema de backup para evitar sobrescritura circular
                "--exclude-table=seguridad.backup_manifest",
                "--exclude-table=seguridad.backup_event",
                "-f", str(filepath),
                config["name"]
            ]
            
            logger.info(f"Ejecutando pg_dump FULL: {filepath}")
            subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            
            tamano = filepath.stat().st_size
            checksum = self._calculate_checksum(filepath)
            fecha_fin = datetime.now()
            
            # Save stats associated with this backup
            self._save_stats(filename, current_stats)
            
            self._register_backup_manifest('FULL', filepath, fecha_inicio, fecha_fin, tamano, checksum)
            return str(filepath)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error en backup FULL: {e.stderr}")
            raise RuntimeError(f"Backup FULL fallido: {e.stderr}")

    def _dump_subset(self, filepath: Path, tables: List[str], config: Dict[str, str], pg_dump: str, env: Dict):
        if not tables:
            # Create an empty dummy file or a minimal dump
            # pg_dump of a non-existent table? Or just metadata?
            # Better: create a valid custom archive with no tables?
            # pg_dump -F c --schema-only --exclude-schema=* ? No that's metadata.
            # Logical incremental for "Usage" usually implies we just skip if empty?
            # But the user expects a file.
            pass

        cmd = [
            pg_dump,
            "-h", config["host"], "-p", config["port"], "-U", config["user"],
            "-F", "c", "-v", "-f", str(filepath)
        ]

        if not tables:
             logger.debug("No detected changes. Creating empty valid archive (metadata only).")
             cmd.append("--schema-only")
        else:
            cmd.append("--data-only")
            for t in tables:
                cmd.extend(["-t", t])
        
        # Database name must be the last argument
        cmd.append(config["name"])

        subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)

    def _create_partial_backup(self, tipo: str, base_backup: BackupInfo) -> str:
        logger.info(f"Iniciando backup {tipo}...")
        config = self._get_db_config()
        pg_dump = self._get_pg_dump_path()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = "dif" if tipo == 'DIFERENCIAL' else "inc"
        subdir = self.differential_dir if tipo == 'DIFERENCIAL' else self.incremental_dir
        filename = f"{prefix}_{timestamp}.backup"
        filepath = subdir / filename
        
        fecha_inicio = datetime.now()
        env = os.environ.copy()
        env["PGPASSWORD"] = config["password"]
        
        try:
            current_stats = self._get_current_table_stats()
            base_stats = self._load_stats(Path(base_backup.archivo).name)
            
            changed_tables = self._get_changed_tables(base_stats, current_stats)
            logger.info(f"Tablas modificadas: {len(changed_tables)}")
            
            # Logic: If tables changed, dump them.
            # If no tables changed, we still produce an archive (maybe tiny) to maintain chain?
            # Yes, pg_dump without tables dumps nothing if using -t?
            # Actually, we need to construct the command carefully.
            
            cmd = [
                pg_dump,
                "-h", config["host"], "-p", config["port"], "-U", config["user"],
                "-F", "c", "-v", "-f", str(filepath)
            ]
            
            if not changed_tables:
                 logger.debug("No detection changes. Force dumping nothing (empty archive).")
                 cmd.append("--schema-only")
                 cmd.append("--exclude-schema=*")
            else:
                cmd.append("--data-only")
                for t in changed_tables:
                    cmd.extend(["-t", t])
            
            # Database name must be the last argument
            cmd.append(config["name"])
            
            subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            
            tamano = filepath.stat().st_size
            checksum = self._calculate_checksum(filepath)
            fecha_fin = datetime.now()
            
            self._save_stats(filename, current_stats)
            self._register_backup_manifest(tipo, filepath, fecha_inicio, fecha_fin, tamano, checksum, backup_base_id=base_backup.id)
            return str(filepath)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error en backup {tipo}: {e.stderr}")
            raise RuntimeError(f"Backup {tipo} fallido: {e.stderr}")

    def create_backup(self, backup_type: str) -> str:
        if backup_type in ['FULL', 'MANUAL']:
            return self._create_full_backup()
        
        elif backup_type == 'DIFERENCIAL':
            full_backup = self._get_last_full_backup()
            if not full_backup:
                return self._create_full_backup()
            return self._create_partial_backup('DIFERENCIAL', full_backup)
        
        elif backup_type == 'INCREMENTAL':
             # Base is the LAST backup (any type)
            last = self._get_last_backup()
            if not last:
                return self._create_full_backup()
            return self._create_partial_backup('INCREMENTAL', last)
        
        raise ValueError("Invalid backup type")

    # Keep helpers for UI
    def get_backup_info(self, backup_id: int) -> Optional[BackupInfo]:
        query = """
        SELECT id, tipo_backup, archivo_ruta, fecha_inicio, fecha_fin,
               tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
        FROM seguridad.backup_manifest
        WHERE id = %s
        """
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (backup_id,))
                row = cur.fetchone()
                if not row: return None
                return BackupInfo(*row)

    def list_backups(self, tipo: Optional[str] = None, limit: int = 50) -> List[Dict]:
        query = """
        SELECT id, tipo_backup, archivo_ruta, fecha_inicio, fecha_fin,
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
                    'id': r[0], 'tipo': r[1], 'archivo': r[2], 'fecha_inicio': r[3],
                    'fecha_fin': r[4], 'tamano': r[5], 'checksum': r[6],
                    'estado': r[7], 'nube_subido': r[8], 'backup_base_id': r[9]
                } for r in rows]

    def get_backup_chain(self, target_date: datetime) -> Optional[List[BackupInfo]]:
        # Simplified chain logic: Find last FULL before target_date, then detected DIF/INCs.
        # Note: If we use Inc-on-Inc, we need the whole chain from Full.
        
        # 1. Get closest FULL before target
        query_full = """
            SELECT id, tipo_backup, archivo_ruta, fecha_inicio, fecha_fin,
                   tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
            FROM seguridad.backup_manifest
            WHERE tipo_backup = 'FULL' AND estado = 'COMPLETADO' AND fecha_inicio <= %s
            ORDER BY fecha_inicio DESC LIMIT 1
        """
        
        with self.db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_full, (target_date,))
                row = cur.fetchone()
                if not row: return None
                full = BackupInfo(*row)
                
                # 2. Get all subsequent backups until target date that link back to this full (transitively)
                # But since we do strict chaining layout (Inc depends on Last), we just grab range.
                # Actually, standard logic: Full + Last Dif + Subsequent Incs.
                # My implementation allows Inc on Inc.
                # So we just get ALL backups between Full and Target.
                
                query_rest = """
                    SELECT id, tipo_backup, archivo_ruta, fecha_inicio, fecha_fin,
                           tamano_bytes, checksum_sha256, estado, lsn_inicio, lsn_fin, backup_base_id
                    FROM seguridad.backup_manifest
                    WHERE estado = 'COMPLETADO' 
                      AND fecha_inicio > %s 
                      AND fecha_inicio <= %s
                    ORDER BY fecha_inicio ASC
                """
                cur.execute(query_rest, (full.fecha_inicio, target_date))
                rows = cur.fetchall()
                chain = [full]
                chain.extend([BackupInfo(*r) for r in rows])
                return chain
