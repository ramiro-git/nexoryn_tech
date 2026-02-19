import os
import subprocess
import logging
import hashlib
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Set
from dataclasses import dataclass

import psycopg
from psycopg import sql

try:
    from desktop_app.services.backup_incremental_service import BackupInfo, BackupIncrementalService
    from desktop_app.config import get_db_config
except ImportError:
    from backup_incremental_service import BackupInfo, BackupIncrementalService
    from config import get_db_config

logger = logging.getLogger(__name__)


@dataclass
class RestoreResult:
    exitoso: bool
    mensaje: str
    backups_aplicados: List[str]
    tiempo_segundos: float
    lsn_final: Optional[str]
    checksum: Optional[str]


class RestoreService:
    def __init__(self, db, backup_incremental_service: BackupIncrementalService, pg_bin_path: Optional[str] = None):
        self.db = db
        self.backup_service = backup_incremental_service
        self.pg_bin_path = pg_bin_path
    
    def _get_db_config(self) -> Dict[str, str]:
        """
        Returns database configuration from environment variables or DATABASE_URL.
        Delegates to config.get_db_config() for consistency across all services.
        """
        return get_db_config()
    
    def _get_pg_restore_path(self) -> str:
        if self.pg_bin_path:
            p = Path(self.pg_bin_path) / "pg_restore.exe"
            if p.exists():
                return str(p)
            p = Path(self.pg_bin_path) / "pg_restore"
            if p.exists():
                return str(p)
        
        path = shutil.which("pg_restore")
        if path:
            return path
        
        common_paths = [
            r"C:\Program Files\PostgreSQL\18\bin\pg_restore.exe",
            r"C:\Program Files\PostgreSQL\17\bin\pg_restore.exe",
            r"C:\Program Files\PostgreSQL\16\bin\pg_restore.exe",
            r"C:\Program Files\PostgreSQL\15\bin\pg_restore.exe",
        ]
        for p in common_paths:
            if os.path.exists(p):
                return p
        
        raise FileNotFoundError("pg_restore not found")
    
    def _get_psql_path(self) -> str:
        if self.pg_bin_path:
            p = Path(self.pg_bin_path) / "psql.exe"
            if p.exists():
                return str(p)
            p = Path(self.pg_bin_path) / "psql"
            if p.exists():
                return str(p)
        
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

    def _get_maintenance_user_id(self) -> Optional[int]:
        raw = os.getenv("DB_MAINTENANCE_USER_ID", "").strip()
        if raw:
            try:
                value = int(raw)
                if value > 0:
                    return value
                logger.warning("DB_MAINTENANCE_USER_ID inválido (debe ser > 0).")
            except ValueError:
                logger.warning("DB_MAINTENANCE_USER_ID inválido (no es un entero).")

        current_user_id = getattr(self.db, "current_user_id", None)
        if current_user_id:
            try:
                value = int(current_user_id)
                if value > 0:
                    return value
            except (TypeError, ValueError):
                pass

        return None

    def _build_pg_env(self, password: str) -> Dict[str, str]:
        env = os.environ.copy()
        env["PGPASSWORD"] = password

        maintenance_user_id = self._get_maintenance_user_id()
        if maintenance_user_id:
            opt_piece = f"-c app.user_id={maintenance_user_id}"
            existing = env.get("PGOPTIONS", "").strip()
            if existing:
                if opt_piece not in existing:
                    env["PGOPTIONS"] = f"{existing} {opt_piece}"
            else:
                env["PGOPTIONS"] = opt_piece
        else:
            logger.warning(
                "DB_MAINTENANCE_USER_ID no definido y sin current_user_id; RLS puede bloquear restores."
            )

        return env

    def _open_restore_connection(self) -> psycopg.Connection:
        config = self._get_db_config()
        try:
            conn = psycopg.connect(
                host=config["host"],
                port=config["port"],
                dbname=config["name"],
                user=config["user"],
                password=config["password"],
            )
        except Exception as e:
            logger.error(f"No se pudo abrir conexión para restore: {e}")
            raise

        conn.autocommit = True
        maintenance_user_id = self._get_maintenance_user_id()
        try:
            with conn.cursor() as cur:
                if maintenance_user_id:
                    cur.execute("SELECT set_config('app.user_id', %s, true)", (str(maintenance_user_id),))
                else:
                    logger.warning(
                        "DB_MAINTENANCE_USER_ID no definido y sin current_user_id; RLS puede bloquear restores."
                    )
        except Exception as e:
            logger.warning(f"No se pudo configurar app.user_id en la sesión de restore: {e}")

        return conn

    def _resolve_tables(
        self,
        tables: List[str],
        *,
        conn: Optional[psycopg.Connection] = None,
    ) -> List[Tuple[str, str]]:
        if not tables:
            return []

        def _strip_quotes(value: str) -> str:
            value = value.strip()
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                return value[1:-1].replace('""', '"')
            return value

        close_conn = False
        if conn is None:
            conn = self._open_restore_connection()
            close_conn = True

        resolved: List[Tuple[str, str]] = []
        seen: Set[Tuple[str, str]] = set()

        try:
            with conn.cursor() as cur:
                for raw in tables:
                    if raw is None:
                        logger.warning("Nombre de tabla inválido en backup: %r", raw)
                        continue
                    raw_str = str(raw).strip()
                    if not raw_str or "." not in raw_str:
                        logger.warning("Nombre de tabla inválido en backup: %r", raw)
                        continue
                    parts = raw_str.split(".")
                    if len(parts) != 2:
                        logger.warning("Nombre de tabla inválido en backup: %r", raw)
                        continue

                    schema = _strip_quotes(parts[0])
                    table = _strip_quotes(parts[1])
                    if not schema or not table:
                        logger.warning("Nombre de tabla inválido en backup: %r", raw)
                        continue

                    if schema.startswith("pg_") or schema == "information_schema":
                        continue

                    key = (schema, table)
                    if key in seen:
                        continue

                    cur.execute(
                        """
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = %s
                          AND c.relname = %s
                          AND c.relkind IN ('r', 'p')
                        """,
                        (schema, table),
                    )
                    if cur.fetchone():
                        resolved.append(key)
                        seen.add(key)
                    else:
                        logger.warning("Tabla no encontrada o no es tabla/partición: %s.%s", schema, table)
        finally:
            if close_conn:
                try:
                    conn.close()
                except Exception:
                    pass

        return resolved
    
    def _verify_checksum(self, file_path: Path, expected_checksum: str) -> bool:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        calculated = sha256_hash.hexdigest()
        return calculated.lower() == expected_checksum.lower()
    
    def _restore_full_backup(self, backup_file: Path, target_db: Optional[str] = None) -> RestoreResult:
        logger.info(f"Restaurando backup FULL: {backup_file}")
        
        # Validar que el archivo existe y tiene contenido
        if not backup_file.exists():
            return RestoreResult(
                exitoso=False,
                mensaje=f"Archivo de backup no encontrado: {backup_file}",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
        
        if backup_file.stat().st_size == 0:
            return RestoreResult(
                exitoso=False,
                mensaje=f"Archivo de backup vacío: {backup_file}",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
        
        config = self._get_db_config()
        pg_restore = self._get_pg_restore_path()
        db_name = target_db or config["name"]
        
        inicio = datetime.now()
        env = self._build_pg_env(config["password"])
        
        try:
            # backup_manifest ya fue excluida del pg_dump, por lo que no
            # es necesario excluirla aquí.
            cmd = [
                pg_restore,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-d", db_name,
                "-c",
                "--if-exists",
                "-v",
                str(backup_file)
            ]
            
            logger.info(f"Ejecutando pg_restore: {' '.join(cmd[:8])}...")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
            
            fin = datetime.now()
            tiempo = (fin - inicio).total_seconds()
            
            # pg_restore puede retornar exit code 1 por warnings menores
            # Analizamos stderr para determinar si es un error crítico
            stderr_lower = (result.stderr or "").lower()
            
            # Errores críticos que indican falla real
            critical_errors = [
                "fatal:",
                "could not connect",
                "authentication failed",
                "permission denied",
                "database does not exist",
                "no such file",
                "invalid input syntax",
                "out of memory"
            ]
            
            has_critical_error = any(err in stderr_lower for err in critical_errors)
            
            if result.returncode != 0 and has_critical_error:
                logger.error(f"Error crítico restaurando backup FULL: {result.stderr}")
                return RestoreResult(
                    exitoso=False,
                    mensaje=f"Error restaurando backup FULL: {result.stderr[:500] if result.stderr else 'Error desconocido'}",
                    backups_aplicados=[],
                    tiempo_segundos=tiempo,
                    lsn_final=None,
                    checksum=None
                )
            
            # Si hay warnings pero no errores críticos, consideramos éxito
            if result.returncode != 0:
                logger.warning(f"pg_restore completó con warnings (exit code {result.returncode})")
                logger.warning(f"stderr: {result.stderr[:1000] if result.stderr else 'N/A'}")
            
            return RestoreResult(
                exitoso=True,
                mensaje=f"Backup FULL restaurado exitosamente" + (f" (con warnings)" if result.returncode != 0 else ""),
                backups_aplicados=[str(backup_file.name)],
                tiempo_segundos=tiempo,
                lsn_final=None,
                checksum=None
            )
            
        except Exception as e:
            logger.error(f"Error restaurando backup FULL: {e}")
            return RestoreResult(
                exitoso=False,
                mensaje=f"Error restaurando backup FULL: {str(e)}",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
    
    def _get_backup_tables(self, backup_file: Path) -> List[str]:
        """
        Lee el TOC del backup usando pg_restore -l y extrae los nombres de las tablas
        que contienen datos (TABLE DATA).
        """
        pg_restore = self._get_pg_restore_path()
        cmd = [pg_restore, "-l", str(backup_file)]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            tables = []
            for line in result.stdout.splitlines():
                # Buscamos líneas como: "5459; 0 607288 TABLE DATA seguridad backup_manifest postgres"
                if "TABLE DATA" in line:
                    parts = line.split()
                    # El formato suele ser: ID; 0 ADDR TABLE DATA SCHEMA TABLE OWNER
                    try:
                        idx = parts.index("DATA")
                        schema = parts[idx + 1]
                        table = parts[idx + 2]
                        if schema != 'pg_catalog' and schema != 'information_schema':
                            tables.append(f"{schema}.{table}")
                    except (ValueError, IndexError):
                        continue
            return list(set(tables))
        except Exception as e:
            logger.error(f"Error leyendo TOC del backup: {e}")
            return []

    def _truncate_tables(self, tables: List[str]) -> bool:
        """
        Ejecuta TRUNCATE de las tablas especificadas usando psql.
        """
        if not tables:
            return True

        conn: Optional[psycopg.Connection] = None
        try:
            conn = self._open_restore_connection()
            resolved = self._resolve_tables(tables, conn=conn)
            if not resolved:
                logger.warning("No se encontraron tablas válidas para truncar.")
                return False

            truncate_sql = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                sql.SQL(", ").join(sql.Identifier(schema, table) for schema, table in resolved)
            )

            logger.info(f"Truncando {len(resolved)} tablas antes de restauración parcial...")
            with conn.cursor() as cur:
                cur.execute(truncate_sql)
            return True
        except Exception as e:
            logger.error(f"Error truncando tablas: {e}")
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _sync_sequences(self, tables: List[str]) -> bool:
        """
        Resincroniza las secuencias de las tablas especificadas después de restaurar datos.
        Calcula el máximo ID en cada tabla y ajusta el next_value de la secuencia correspondiente.
        
        Esto previene errores de "duplicate key" cuando TRUNCATE RESTART IDENTITY deja
        las secuencias desincronizadas respecto a los IDs restaurados.
        """
        if not tables:
            return True

        conn: Optional[psycopg.Connection] = None
        try:
            conn = self._open_restore_connection()
            resolved = self._resolve_tables(tables, conn=conn)
            if not resolved:
                logger.warning("No se encontraron tablas válidas para resincronizar secuencias.")
                return False

            logger.info(f"Resincronizando secuencias de {len(resolved)} tablas...")
            total_sequences = 0
            seen_sequences: Set[str] = set()

            with conn.cursor() as cur:
                for schema, table in resolved:
                    table_qual = f"{schema}.{table}"
                    cur.execute(
                        """
                        SELECT a.attname,
                               pg_get_serial_sequence(%s, a.attname) AS seq_name
                        FROM pg_attribute a
                        JOIN pg_class c ON c.oid = a.attrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = %s
                          AND c.relname = %s
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                        """,
                        (table_qual, schema, table),
                    )
                    rows = cur.fetchall()
                    sequences = []
                    for row in rows:
                        col_name = row[0]
                        seq_name = row[1]
                        if seq_name:
                            sequences.append((col_name, seq_name))

                    if not sequences:
                        logger.warning("No se encontraron secuencias owned para %s.%s", schema, table)
                        continue

                    for col_name, seq_name in sequences:
                        if seq_name in seen_sequences:
                            continue
                        seen_sequences.add(seq_name)

                        cur.execute(
                            sql.SQL("SELECT MAX({col}) FROM {schema}.{table}").format(
                                col=sql.Identifier(col_name),
                                schema=sql.Identifier(schema),
                                table=sql.Identifier(table),
                            )
                        )
                        max_val = cur.fetchone()[0]

                        if max_val is None:
                            cur.execute(
                                "SELECT seqstart FROM pg_sequence WHERE seqrelid = %s::regclass",
                                (seq_name,),
                            )
                            seq_row = cur.fetchone()
                            if seq_row:
                                seq_start = seq_row[0]
                            else:
                                seq_start = 1
                                logger.warning("No se pudo encontrar seqstart para %s, usando 1", seq_name)

                            cur.execute("SELECT setval(%s, %s, false)", (seq_name, seq_start))
                            logger.info(
                                "Secuencia %s ajustada a %s (tabla vacía)", seq_name, seq_start
                            )
                        else:
                            cur.execute("SELECT setval(%s, %s, true)", (seq_name, max_val))
                            logger.info("Secuencia %s ajustada a %s", seq_name, max_val)
                        total_sequences += 1

            if total_sequences == 0:
                logger.warning("No se encontraron secuencias owned para resincronizar.")

            return True
        except Exception as e:
            logger.error(f"Error resincronizando secuencias: {e}")
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _apply_differential_backup(self, backup_file: Path, lsn_inicio: str) -> RestoreResult:
        logger.info(f"Aplicando backup DIFERENCIAL (Data Only): {backup_file}")
        
        # Validar que el archivo existe y tiene contenido
        if not backup_file.exists():
            return RestoreResult(
                exitoso=False,
                mensaje=f"Archivo de backup diferencial no encontrado: {backup_file}",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
        
        if backup_file.stat().st_size == 0:
            # Un archivo vacío puede ser válido si no hubo cambios
            logger.warning(f"Archivo de backup diferencial vacío: {backup_file}")
        
        config = self._get_db_config()
        pg_restore = self._get_pg_restore_path()
        
        inicio = datetime.now()
        env = self._build_pg_env(config["password"])
        
        try:
            # --- Fase de Truncado Inteligente ---
            # Dado que pg_restore --clean es incompatible con --data-only en algunas versiones,
            # vaciamos las tablas manualmente antes de restaurar los datos.
            tables_to_truncate = self._get_backup_tables(backup_file)
            if tables_to_truncate:
                if not self._truncate_tables(tables_to_truncate):
                    logger.warning("No se pudieron truncar todas las tablas, la restauración podría tener duplicados.")
            
            # --- Fase de Restauración de Datos ---
            # Differential/Incremental contains data only.
            # Nota: En algunas versiones de pg_restore, --clean y --data-only son incompatibles.
            # Como los backups parciales son solo de datos, intentamos restaurar directamente.
            cmd = [
                pg_restore,
                "-h", config["host"], "-p", config["port"], "-U", config["user"],
                "-d", config["name"],
                "--data-only",
                "--disable-triggers",  # Importante para evitar errores de FK
                "-v",
                str(backup_file)
            ]
            
            logger.info(f"Ejecutando pg_restore DIFERENCIAL (Data Only): {backup_file.name}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
            
            fin = datetime.now()
            tiempo = (fin - inicio).total_seconds()
            
            # pg_restore puede retornar exit code 1 por warnings menores
            stderr_lower = (result.stderr or "").lower()
            
            critical_errors = [
                "fatal:",
                "could not connect",
                "authentication failed",
                "permission denied",
                "database does not exist",
                "no such file",
                "invalid input syntax",
                "out of memory"
            ]
            
            has_critical_error = any(err in stderr_lower for err in critical_errors)
            
            if result.returncode != 0 and has_critical_error:
                logger.error(f"Error crítico aplicando backup DIFERENCIAL: {result.stderr}")
                return RestoreResult(
                    exitoso=False,
                    mensaje=f"Error aplicando backup DIFERENCIAL: {result.stderr[:500] if result.stderr else 'Error desconocido'}",
                    backups_aplicados=[],
                    tiempo_segundos=tiempo,
                    lsn_final=None,
                    checksum=None
                )
            
            if result.returncode != 0:
                logger.warning(f"pg_restore DIFERENCIAL completó con warnings (exit code {result.returncode})")
                logger.warning(f"stderr: {result.stderr[:1000] if result.stderr else 'N/A'}")
            
            # --- Fase de Resincronización de Secuencias ---
            # CRUCIAL: Después de restaurar datos con TRUNCATE RESTART IDENTITY,
            # las secuencias pueden estar desincronizadas. Esto causa errores de "duplicate key"
            # en inserciones posteriores. Recalculamos y ajustamos cada secuencia al máximo ID restaurado.
            if tables_to_truncate:
                if not self._sync_sequences(tables_to_truncate):
                    logger.warning("No se pudieron resincronizar todas las secuencias, podría haber problemas en inserciones posteriores.")
            
            return RestoreResult(
                exitoso=True,
                mensaje=f"Backup DIFERENCIAL aplicado exitosamente" + (f" (con warnings)" if result.returncode != 0 else ""),
                backups_aplicados=[str(backup_file.name)],
                tiempo_segundos=tiempo,
                lsn_final=None,
                checksum=None
            )
            
        except Exception as e:
            logger.error(f"Error aplicando backup DIFERENCIAL: {e}")
            return RestoreResult(
                exitoso=False,
                mensaje=f"Error aplicando backup DIFERENCIAL: {str(e)}",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
    
    def _apply_incremental_backup(self, backup_file: Path) -> RestoreResult:
        logger.info(f"Aplicando backup INCREMENTAL (Data Only): {backup_file}")
        # Logic is identical to Differential for Restore side (just applying a patch)
        return self._apply_differential_backup(backup_file, "0/0")
    
    def restore_to_date(self, target_date: datetime, target_db: Optional[str] = None) -> RestoreResult:
        logger.info(f"Restaurando a fecha: {target_date}")
        
        chain = self.backup_service.get_backup_chain(target_date)
        
        if not chain:
            return RestoreResult(
                exitoso=False,
                mensaje=f"No se encontró cadena de backups para la fecha {target_date}",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
        
        logger.info(f"Cadena de backups encontrada: {len(chain)} archivos")
        for backup in chain:
            logger.info(f"  - {backup.tipo}: {backup.archivo} ({backup.fecha_inicio})")
        
        backups_aplicados = []
        inicio_total = datetime.now()
        
        try:
            for i, backup in enumerate(chain):
                backup_file = Path(backup.archivo)
                
                if not backup_file.exists():
                    logger.error(f"Archivo no encontrado: {backup_file}")
                    return RestoreResult(
                        exitoso=False,
                        mensaje=f"Archivo de backup no encontrado: {backup_file}",
                        backups_aplicados=backups_aplicados,
                        tiempo_segundos=0,
                        lsn_final=None,
                        checksum=None
                    )
                
                if backup.tipo == 'FULL':
                    logger.info(f"Paso {i+1}/{len(chain)}: Restaurando backup FULL...")
                    result = self._restore_full_backup(backup_file, target_db)
                    if not result.exitoso:
                        return result
                    backups_aplicados.extend(result.backups_aplicados)
                    lsn_actual = result.lsn_final
                
                elif backup.tipo == 'DIFERENCIAL':
                    logger.info(f"Paso {i+1}/{len(chain)}: Aplicando backup DIFERENCIAL...")
                    lsn_inicio = backup.lsn_inicio
                    result = self._apply_differential_backup(backup_file, lsn_inicio)
                    if not result.exitoso:
                        return result
                    backups_aplicados.extend(result.backups_aplicados)
                    lsn_actual = result.lsn_final
                
                elif backup.tipo == 'INCREMENTAL':
                    logger.info(f"Paso {i+1}/{len(chain)}: Aplicando backup INCREMENTAL...")
                    result = self._apply_incremental_backup(backup_file)
                    if not result.exitoso:
                        return result
                    backups_aplicados.extend(result.backups_aplicados)
                    lsn_actual = result.lsn_final
            
            fin_total = datetime.now()
            tiempo_total = (fin_total - inicio_total).total_seconds()
            
            logger.info(f"Restauración completada en {tiempo_total:.2f} segundos")
            logger.info(f"Backups aplicados: {len(backups_aplicados)}")
            
            return RestoreResult(
                exitoso=True,
                mensaje=f"Restauración completada exitosamente ({len(chain)} backups aplicados)",
                backups_aplicados=backups_aplicados,
                tiempo_segundos=tiempo_total,
                lsn_final=lsn_actual,
                checksum=None
            )
            
        except Exception as e:
            logger.error(f"Error en restauración: {e}")
            return RestoreResult(
                exitoso=False,
                mensaje=f"Error en restauración: {str(e)}",
                backups_aplicados=backups_aplicados,
                tiempo_segundos=(datetime.now() - inicio_total).total_seconds(),
                lsn_final=None,
                checksum=None
            )
    
    def restore_from_backup_id(self, backup_id: int, target_db: Optional[str] = None) -> RestoreResult:
        logger.info(f"Restaurando desde backup ID: {backup_id}")
        
        backup_info = self.backup_service.get_backup_info(backup_id)
        
        if not backup_info:
            return RestoreResult(
                exitoso=False,
                mensaje=f"Backup ID {backup_id} no encontrado",
                backups_aplicados=[],
                tiempo_segundos=0,
                lsn_final=None,
                checksum=None
            )
        
        return self.restore_to_date(backup_info.fecha_inicio, target_db)
    
    def preview_restore(self, target_date: datetime) -> Dict:
        chain = self.backup_service.get_backup_chain(target_date)
        
        if not chain:
            return {
                'existe': False,
                'fecha': target_date,
                'backups': []
            }
        
        total_tamano = sum(b.tamano for b in chain if b.tamano)
        
        return {
            'existe': True,
            'fecha': target_date,
            'backups': [
                {
                    'tipo': b.tipo,
                    'archivo': b.archivo,
                    'fecha': b.fecha_inicio,
                    'tamano': b.tamano,
                    'checksum': b.checksum,
                    'lsn_inicio': b.lsn_inicio,
                    'lsn_fin': b.lsn_fin
                }
                for b in chain
            ],
            'cantidad_backups': len(chain),
            'tamano_total_bytes': total_tamano,
            'tamano_total_mb': round(total_tamano / 1024 / 1024, 2),
            'fecha_inicial': chain[0].fecha_inicio if chain else None,
            'fecha_final': chain[-1].fecha_inicio if chain else None
        }
    
    def validate_backup_chain(self, backup_id: int) -> Dict:
        backup = self.backup_service.get_backup_info(backup_id)
        
        if not backup:
            return {
                'valido': False,
                'mensaje': f"Backup ID {backup_id} no encontrado",
                'validaciones': []
            }
        
        validaciones = []
        backup_file = Path(backup.archivo)
        
        if not backup_file.exists():
            validaciones.append({
                'tipo': 'existencia_archivo',
                'exito': False,
                'mensaje': f'Archivo no encontrado: {backup_file}'
            })
        else:
            validaciones.append({
                'tipo': 'existencia_archivo',
                'exito': True,
                'mensaje': f'Archivo existe: {backup_file}'
            })
            
            if backup.checksum:
                checksum_valido = self._verify_checksum(backup_file, backup.checksum)
                validaciones.append({
                    'tipo': 'checksum',
                    'exito': checksum_valido,
                    'mensaje': f'Checksum {"válido" if checksum_valido else "inválido"}'
                })
        
        todas_validas = all(v['exito'] for v in validaciones)
        
        return {
            'valido': todas_validas,
            'mensaje': 'Cadena de backup válida' if todas_validas else 'Cadena de backup inválida',
            'validaciones': validaciones,
            'backup': {
                'id': backup.id,
                'tipo': backup.tipo,
                'archivo': backup.archivo,
                'fecha': backup.fecha_inicio
            }
        }
