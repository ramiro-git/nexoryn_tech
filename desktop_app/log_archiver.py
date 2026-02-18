"""
Log Archiver Service
====================

Automatically archives old logs to compressed JSON files and purges them from the database.

Features:
- Exports logs older than retention period to .jsonl.gz files
- Automatically creates archive directory
- Runs in background thread at application startup
- Safe transaction-based deletion
"""

import gzip
import json
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

# Default values
DEFAULT_RETENTION_DAYS = 90
DEFAULT_ARCHIVE_DIR = "logs_archive"
BATCH_SIZE = 5000  # Process in batches to avoid memory issues


class LogArchiver:
    """Service for archiving and purging old log entries."""
    
    def __init__(self, db, app_data_dir: Optional[str] = None):
        """
        Initialize the log archiver.
        
        Args:
            db: Database connection instance
            app_data_dir: Base directory for archive files. If None, uses current directory.
        """
        self.db = db
        self.app_data_dir = Path(app_data_dir) if app_data_dir else Path.cwd()
        self._running = False
        self._lock = threading.Lock()
    
    def get_archive_dir(self) -> Path:
        """Get the archive directory path, creating it if necessary."""
        archive_subdir = self._get_config("log_directorio_archivo", DEFAULT_ARCHIVE_DIR)
        archive_path = self.app_data_dir / archive_subdir
        archive_path.mkdir(parents=True, exist_ok=True)
        return archive_path
    
    def get_retention_days(self) -> int:
        """Get the configured retention period in days."""
        try:
            return int(self._get_config("log_retencion_dias", str(DEFAULT_RETENTION_DAYS)))
        except (ValueError, TypeError):
            return DEFAULT_RETENTION_DAYS
    
    def _get_config(self, key: str, default: str) -> str:
        """Read a config value from the database."""
        try:
            if not self.db or not hasattr(self.db, 'pool') or self.db.pool is None:
                return default
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT valor FROM seguridad.config_sistema WHERE clave = %s",
                        (key,)
                    )
                    row = cur.fetchone()
                    if row:
                        return row.get("valor", default) if isinstance(row, dict) else row[0]
                    return default
        except Exception:
            return default
    
    def _fetch_old_logs(self, cutoff_date: datetime, batch_size: int = BATCH_SIZE, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch a batch of logs older than the cutoff date."""
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT l.id, l.fecha_hora, l.id_usuario, u.nombre as usuario,
                               l.id_tipo_evento_log, l.entidad, l.id_entidad, l.accion,
                               l.resultado, l.ip::text as ip, l.user_agent, l.session_id, l.detalle
                        FROM seguridad.log_actividad l
                        LEFT JOIN seguridad.usuario u ON l.id_usuario = u.id
                        WHERE l.fecha_hora < %s
                        ORDER BY l.fecha_hora ASC
                        LIMIT %s OFFSET %s
                    """, (cutoff_date, batch_size, offset))
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = []
                    for row in cur.fetchall():
                        if isinstance(row, dict):
                            rows.append(row)
                        else:
                            rows.append(dict(zip(columns, row)))
                    return rows
        except Exception as e:
            print(f"[LogArchiver] Error fetching old logs: {e}")
            return []
    
    def _delete_logs_by_ids(self, ids: List[int]) -> int:
        """Delete logs by their IDs. Returns number deleted."""
        if not ids:
            return 0
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM seguridad.log_actividad WHERE id = ANY(%s)",
                        (ids,)
                    )
                    deleted = cur.rowcount
                    conn.commit()
                    return deleted
        except Exception as e:
            print(f"[LogArchiver] Error deleting logs: {e}")
            return 0
    
    def _serialize_for_json(self, obj: Any) -> Any:
        """Convert objects to JSON-serializable format."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode('utf-8', errors='replace')
        if hasattr(obj, '__str__'):
            return str(obj)
        return obj
    
    def archive_old_logs(self, progress_callback=None) -> Dict[str, Any]:
        """
        Archive and purge old logs using streaming to minimize memory usage.
        
        Args:
            progress_callback: Optional function(current, total, status_text) call for progress updates.
        
        Returns dict with:
            - archived: number of logs archived
            - deleted: number of logs deleted
            - file: path to archive file (if any)
            - error: error message (if any)
        """
        result = {"archived": 0, "deleted": 0, "file": None, "error": None}
        
        # Validate DB connection
        if not self.db or not hasattr(self.db, 'pool') or self.db.pool is None:
            # print("[LogArchiver] No database connection active. Skipping archive run.") # Debug info
            return result

        # Prevent concurrent runs
        if not self._lock.acquire(blocking=False):
            result["error"] = "Another archive operation is in progress"
            return result
        
        try:
            self._running = True
            
            retention_days = self.get_retention_days()
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # --- 1. Count total logs to process for progress bar ---
            total_logs = 0
            if progress_callback:
                progress_callback(0, 0, "Calculando registros...")
                try:
                    with self.db.pool.connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT count(*) FROM seguridad.log_actividad l WHERE l.fecha_hora < %s", (cutoff_date,))
                            row = cur.fetchone()
                            total_logs = row[0] if row else 0 if isinstance(row, tuple) else row.get('count', 0)
                except Exception: pass
            
            if total_logs == 0:
                if progress_callback: progress_callback(1, 1, "Sin logs antiguos para archivar.")
                return result
            
            # --- 2. Determine date range with first batch ---
            if progress_callback:
                progress_callback(0, total_logs, "Recuperando rango de fechas...")
            
            first_batch = self._fetch_old_logs(cutoff_date, batch_size=1, offset=0)
            if not first_batch:
                return result
            
            min_date = datetime.now()
            max_date = datetime.now()
            
            try:
                first_log = first_batch[0]
                date_val = first_log.get("fecha_hora")
                if isinstance(date_val, str):
                    date_val = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
                min_date = date_val
            except:
                pass
            
            archive_dir = self.get_archive_dir()
            filename = f"logs_{min_date.strftime('%Y-%m-%d')}_to_{max_date.strftime('%Y-%m-%d')}.jsonl.gz"
            filepath = archive_dir / filename
            
            # --- 3. Stream logs to compressed file in batches (minimize memory) ---
            if progress_callback:
                progress_callback(0, total_logs, "Comprimiendo registros en archivo...")
            
            fetched_count = 0
            ids_to_delete = []
            
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                while True:
                    # Fetch batch
                    batch = self._fetch_old_logs(cutoff_date, batch_size=BATCH_SIZE, offset=fetched_count)
                    if not batch:
                        break
                    
                    # Write batch immediately to file
                    for log in batch:
                        clean_log = {}
                        for k, v in log.items():
                            clean_log[k] = self._serialize_for_json(v)
                        f.write(json.dumps(clean_log, ensure_ascii=False) + '\n')
                        result["archived"] += 1
                        
                        # Collect IDs for deletion (keep small, batch delete soon)
                        if log.get("id"):
                            ids_to_delete.append(log["id"])
                    
                    fetched_count += len(batch)
                    
                    if progress_callback:
                        progress_callback(fetched_count, total_logs, f"Comprimiendo... ({fetched_count}/{total_logs})")
            
            result["file"] = str(filepath)
            
            # Update max_date from last batch
            try:
                if fetched_count > 0:
                    last_batch = self._fetch_old_logs(cutoff_date, batch_size=1, offset=fetched_count - 1)
                    if last_batch:
                        date_val = last_batch[0].get("fecha_hora")
                        if isinstance(date_val, str):
                            date_val = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
                        max_date = date_val
            except:
                pass
            
            # --- 4. Delete archived logs in batches ---
            if ids_to_delete:
                if progress_callback:
                    progress_callback(total_logs, total_logs, "Limpiando BD...")
                
                total_to_delete = len(ids_to_delete)
                
                for i in range(0, total_to_delete, 1000):
                    batch_ids = ids_to_delete[i:i+1000]
                    deleted = self._delete_logs_by_ids(batch_ids)
                    result["deleted"] += deleted
                    
                    if progress_callback:
                        deleted_so_far = min(i + 1000, total_to_delete)
                        progress_callback(total_logs + deleted_so_far, total_logs + total_to_delete, 
                                        f"Limpiando BD... ({deleted_so_far}/{total_to_delete})")
            
            print(f"[LogArchiver] Archived {result['archived']} logs to {filepath}, deleted {result['deleted']}")
            
            if progress_callback: progress_callback(100, 100, "Finalizado")
            
            # --- AUTOMATIC PARTITION MAINTENANCE ---
            self._maintain_partitions()
            
        except Exception as e:
            result["error"] = str(e)
            print(f"[LogArchiver] Error during archival: {e}")
        finally:
            self._running = False
            self._lock.release()
        
        return result
    def _maintain_partitions(self) -> None:
        """Call the database function to create future partitions."""
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    # Check if function exists first to avoid errors if SQL script wasn't run
                    cur.execute("SELECT 1 FROM pg_proc WHERE proname = 'mantener_particiones_log'")
                    if cur.fetchone():
                        try:
                            cur.execute("SELECT seguridad.mantener_particiones_log()")
                            conn.commit()
                        except Exception as sql_err:
                            # Attempt repair if function definition is broken (e.g. old table name)
                            conn.rollback()
                            # print(f"[LogArchiver] Maintenance SQL error: {sql_err}. Attempting repair...")
                            
                            try:
                                from desktop_app.db_migration import update_partition_functions
                                update_partition_functions(conn, cur)
                                conn.commit()
                                
                                # Retry maintenance
                                cur.execute("SELECT seguridad.mantener_particiones_log()")
                                conn.commit()
                                print("[LogArchiver] Partition functions repaired and maintenance completed.")
                            except Exception as repair_err:
                                print(f"[LogArchiver] Repair failed: {repair_err}")
                                raise sql_err

        except Exception as e:
            print(f"[LogArchiver] Partition maintenance warning: {e}")
    
    def run_async(self) -> None:
        """Run the archive operation in a background thread."""
        thread = threading.Thread(target=self.archive_old_logs, daemon=True)
        thread.start()
    
    @property
    def is_running(self) -> bool:
        """Check if an archive operation is currently running."""
        return self._running


def start_log_archiver(db, app_data_dir: Optional[str] = None, delay_seconds: int = 30) -> LogArchiver:
    """
    Start the log archiver service.
    
    Args:
        db: Database connection instance
        app_data_dir: Base directory for archive files
        delay_seconds: Delay before starting the first archive run (to let app initialize)
    
    Returns:
        LogArchiver instance
    """
    archiver = LogArchiver(db, app_data_dir)
    
    def delayed_start():
        import time
        time.sleep(delay_seconds)
        archiver.run_async()
    
    threading.Thread(target=delayed_start, daemon=True).start()
    return archiver
