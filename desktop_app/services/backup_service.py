import os
import subprocess
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import shutil

# Configure logging
logger = logging.getLogger(__name__)

# Settings file path
SETTINGS_FILE = Path.home() / ".nexoryn" / "backup_settings.json"

def _load_settings() -> Dict:
    """Load settings from JSON file."""
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load backup settings: {e}")
    return {}

def _save_settings(settings: Dict) -> bool:
    """Save settings to JSON file."""
    try:
        SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save backup settings: {e}")
        return False

class BackupService:
    def __init__(self, backup_dir: str = "backups", pg_bin_path: Optional[str] = None, sync_dir: Optional[str] = None):
        # Load persisted settings
        settings = _load_settings()
        
        # Use persisted backup_dir if not overridden
        saved_backup_dir = settings.get("backup_dir")
        if saved_backup_dir and backup_dir == "backups":
            backup_dir = saved_backup_dir
        
        self.backup_dir = Path(backup_dir)
        self.pg_bin_path = pg_bin_path
        
        # Use persisted sync_dir if not overridden
        saved_sync_dir = settings.get("sync_dir")
        if saved_sync_dir and sync_dir is None:
            sync_dir = saved_sync_dir
        
        self.sync_dir: Optional[Path] = Path(sync_dir) if sync_dir else None
        self.sync_enabled: bool = sync_dir is not None
        self.last_sync_status: Optional[Dict[str, any]] = None
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Standard backup directory creation is disabled to favor incremental system."""
        # Definitions are kept for compatibility with listing methods, but creation is disabled.
        self.daily_dir = self.backup_dir / "daily"
        self.weekly_dir = self.backup_dir / "weekly"
        self.monthly_dir = self.backup_dir / "monthly"
        self.manual_dir = self.backup_dir / "manual"
    
    def set_backup_dir(self, new_path: str) -> bool:
        """Change the backup directory to a new location."""
        try:
            new_dir = Path(new_path)
            new_dir.mkdir(parents=True, exist_ok=True)
            self.backup_dir = new_dir
            self._ensure_directories()
            
            # Persist to settings file
            settings = _load_settings()
            settings["backup_dir"] = str(new_dir.absolute())
            _save_settings(settings)
            
            logger.info(f"Backup directory changed to: {new_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to set backup directory: {e}")
            return False
    
    def get_backup_dir(self) -> str:
        """Returns current backup directory path."""
        return str(self.backup_dir.absolute())
    
    def set_sync_dir(self, new_path: Optional[str]) -> bool:
        """Set the cloud sync directory. Pass None to disable sync."""
        try:
            settings = _load_settings()
            
            if new_path is None or new_path.strip() == "":
                self.sync_dir = None
                self.sync_enabled = False
                settings.pop("sync_dir", None)
                _save_settings(settings)
                logger.info("Cloud sync disabled")
                return True
            
            sync_path = Path(new_path)
            sync_path.mkdir(parents=True, exist_ok=True)
            self.sync_dir = sync_path
            self.sync_enabled = True
            
            # Persist to settings file
            settings["sync_dir"] = str(sync_path.absolute())
            _save_settings(settings)
            
            logger.info(f"Cloud sync directory set to: {new_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to set sync directory: {e}")
            return False
    
    def get_sync_dir(self) -> Optional[str]:
        """Returns current sync directory path or None if disabled."""
        return str(self.sync_dir.absolute()) if self.sync_dir else None
    
    def is_sync_enabled(self) -> bool:
        """Returns whether cloud sync is enabled."""
        return self.sync_enabled and self.sync_dir is not None
    
    def copy_to_sync(self, file_path: str) -> bool:
        """
        Copy a backup file to the sync directory.
        Returns True on success, False on failure.
        """
        if not self.is_sync_enabled():
            logger.debug("Sync not enabled, skipping copy")
            return False
        
        try:
            src = Path(file_path)
            if not src.exists():
                raise FileNotFoundError(f"Source file not found: {file_path}")
            
            dest = self.sync_dir / src.name
            shutil.copy2(src, dest)
            
            self.last_sync_status = {
                "success": True,
                "file": src.name,
                "destination": str(dest),
                "timestamp": datetime.now(),
                "size": src.stat().st_size
            }
            logger.info(f"Backup synced to cloud folder: {dest}")
            return True
            
        except Exception as e:
            self.last_sync_status = {
                "success": False,
                "file": Path(file_path).name if file_path else None,
                "error": str(e),
                "timestamp": datetime.now()
            }
            logger.error(f"Failed to sync backup: {e}")
            return False
    
    def get_last_sync_status(self) -> Optional[Dict[str, any]]:
        """Returns the status of the last sync operation."""
        return self.last_sync_status
    
    def get_next_backup_times(self) -> Dict[str, Dict[str, any]]:
        """
        Calculate the next scheduled backup times.
        Returns dict with type -> {next_run: datetime, schedule: str}
        """
        now = datetime.now()
        result = {}
        
        # Daily: every day at 23:00
        next_daily = now.replace(hour=23, minute=0, second=0, microsecond=0)
        if now >= next_daily:
            next_daily += timedelta(days=1)
        result["daily"] = {
            "next_run": next_daily,
            "schedule": "Todos los días a las 23:00"
        }
        
        # Weekly: Sunday at 23:30
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0:
            next_weekly = now.replace(hour=23, minute=30, second=0, microsecond=0)
            if now >= next_weekly:
                days_until_sunday = 7
                next_weekly = now + timedelta(days=days_until_sunday)
                next_weekly = next_weekly.replace(hour=23, minute=30, second=0, microsecond=0)
        else:
            next_weekly = now + timedelta(days=days_until_sunday)
            next_weekly = next_weekly.replace(hour=23, minute=30, second=0, microsecond=0)
        result["weekly"] = {
            "next_run": next_weekly,
            "schedule": "Domingos a las 23:30"
        }
        
        # Monthly: Day 1 at 00:00
        if now.day == 1 and now.hour == 0 and now.minute == 0:
            next_monthly = now.replace(second=0, microsecond=0)
        else:
            if now.month == 12:
                next_monthly = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                next_monthly = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        result["monthly"] = {
            "next_run": next_monthly,
            "schedule": "Día 1 de cada mes a las 00:00"
        }
        
        return result
    
    @staticmethod
    def format_time_until(target: datetime) -> str:
        """Format time remaining until target datetime in human-readable format."""
        now = datetime.now()
        delta = target - now
        
        if delta.total_seconds() <= 0:
            return "Ahora"
        
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes = remainder // 60
        
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0 and days == 0:
            parts.append(f"{minutes}min")
        
        return "En " + " ".join(parts) if parts else "En <1min"

    def _get_db_config(self) -> Dict[str, str]:
        """Extracts DB config from environment variables."""
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "name": os.getenv("DB_NAME", "nexoryn_tech"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "") or os.environ.get("PGPASSWORD", ""),
        }
    
    def _get_pg_dump_path(self) -> str:
        """Attempts to find pg_dump executable."""
        # Try configured path first
        if self.pg_bin_path:
            p = Path(self.pg_bin_path) / "pg_dump.exe"
            if p.exists():
                return str(p)
            p = Path(self.pg_bin_path) / "pg_dump"
            if p.exists():
                return str(p)

        # Try system path
        path = shutil.which("pg_dump")
        if path:
            return path
            
        # Common Windows paths (expanded list)
        common_paths = [
            r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\16\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\15\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\13\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\12\bin\pg_dump.exe",
        ]
        for p in common_paths:
            if os.path.exists(p):
                return p
                
        raise FileNotFoundError("pg_dump not found. Please install PostgreSQL tools or add bin to PATH.")

    def create_backup(self, backup_type: str = "manual") -> str:
        """
        Creates a database backup.
        backup_type options: 'manual', 'daily', 'weekly', 'monthly'
        """
        try:
            config = self._get_db_config()
            pg_dump = self._get_pg_dump_path()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backup_{backup_type}_{timestamp}.sql"
            
            target_dir = getattr(self, f"{backup_type}_dir", self.manual_dir)
            file_path = target_dir / filename
            
            # Set PGPASSWORD env var for the subprocess
            env = os.environ.copy()
            env["PGPASSWORD"] = config["password"]
            
            cmd = [
                pg_dump,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-F", "c", # Custom format (compressed)
                "-b",      # Include large objects
                "-v",      # Verbose
                "-f", str(file_path),
                config["name"]
            ]
            
            logger.info(f"Starting backup: {file_path}")
            subprocess.run(cmd, env=env, check=True)
            logger.info("Backup completed successfully.")
            
            # Auto-sync to cloud folder if enabled
            if self.is_sync_enabled():
                self.copy_to_sync(str(file_path))
            
            return str(file_path)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Backup failed: {e}")
            raise RuntimeError(f"Backup failed: {str(e)}")
        except Exception as e:
            logger.error(f"An error occurred during backup: {e}")
            raise

    def restore_backup(self, file_path: str) -> None:
        """Restores a database from a backup file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Backup file not found: {file_path}")
            
        try:
            config = self._get_db_config()
            # We typically use pg_restore for custom format
            pg_restore = shutil.which("pg_restore") or self._get_pg_dump_path().replace("pg_dump", "pg_restore")
            
            env = os.environ.copy()
            env["PGPASSWORD"] = config["password"]
            
            # Note: --clean requires the user to have permission to drop objects
            cmd = [
                pg_restore,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-d", config["name"],
                "-c", # Clean (drop) database objects before recreating
                "--if-exists",
                "-v",
                str(file_path)
            ]
            
            logger.info(f"Starting restore from: {file_path}")
            subprocess.run(cmd, env=env, check=True)
            logger.info("Restore completed successfully.")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Restore failed: {e}")
            raise RuntimeError(f"Restore failed: {str(e)}")

    def list_backups(self) -> List[Dict]:
        """Lists all available backups."""
        backups = []
        for d in [self.daily_dir, self.weekly_dir, self.monthly_dir, self.manual_dir]:
            logger.info(f"Checking directory: {d}, exists: {d.exists()}")
            if d.exists():
                files = list(d.glob("*.sql"))
                logger.info(f"Found {len(files)} .sql files in {d}")
                for f in files:
                    stats = f.stat()
                    backups.append({
                        "name": f.name,
                        "path": str(f),
                        "type": d.name,
                        "size": stats.st_size,
                        "created": datetime.fromtimestamp(stats.st_ctime)
                    })
        
        logger.info(f"Total backups found: {len(backups)}")
        # Sort by creation time desc
        return sorted(backups, key=lambda x: x["created"], reverse=True)

    def prune_backups(self, retention_policy: Optional[Dict[str, int]] = None):
        """
        Deletes old backups based on retention policy.
        Default policy: 7 dailies, 4 weeklies, 6 monthlies.
        """
        if retention_policy is None:
            retention_policy = {
                "daily": 7,
                "weekly": 4,
                "monthly": 6,
                # manual backups are typically not pruned automatically or have a much higher limit
                "manual": 100 
            }
            
        for type_name, limit in retention_policy.items():
            target_dir = getattr(self, f"{type_name}_dir", None)
            if not target_dir or not target_dir.exists():
                continue
                
            files = sorted(
                target_dir.glob("*.sql"), 
                key=lambda f: f.stat().st_ctime, 
                reverse=True
            )
            
            if len(files) > limit:
                for f in files[limit:]:
                    try:
                        # Use missing_ok=True to handle cases where another instance 
                        # might have already deleted the file (race condition at 1:00 AM)
                        if f.exists():
                            logger.info(f"Pruning old backup: {f}")
                            f.unlink(missing_ok=True)
                        else:
                            logger.debug(f"Backup already pruned by another instance: {f}")
                    except Exception as e:
                        # Only log as error if it's not a 'File not found' error
                        if not isinstance(e, FileNotFoundError):
                            logger.error(f"Failed to delete {f}: {e}")

    # =========================================================================
    # MISSED BACKUP DETECTION SYSTEM
    # =========================================================================

    def get_last_required_run_date(self, backup_type: str) -> datetime:
        """
        Calculate when the last backup of a given type SHOULD have been executed.
        This is used to detect missed backups.
        """
        now = datetime.now()
        
        if backup_type == "daily":
            # Daily backup runs at 23:00. If it's before 23:00 today, the last required
            # was yesterday at 23:00. If it's after, it was today at 23:00.
            today_run = now.replace(hour=23, minute=0, second=0, microsecond=0)
            if now >= today_run:
                return today_run
            else:
                return today_run - timedelta(days=1)
        
        elif backup_type == "weekly":
            # Weekly backup runs on Sunday at 23:30
            days_since_sunday = (now.weekday() + 1) % 7  # Sunday = 0
            last_sunday = now - timedelta(days=days_since_sunday)
            last_sunday_run = last_sunday.replace(hour=23, minute=30, second=0, microsecond=0)
            
            # If today is Sunday and we're before 23:30, last required was previous Sunday
            if days_since_sunday == 0 and now < last_sunday_run:
                return last_sunday_run - timedelta(days=7)
            return last_sunday_run
        
        elif backup_type == "monthly":
            # Monthly backup runs on day 1 at 00:00
            this_month_run = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            # If we're on day 1 and it's midnight, this is the run time
            # Otherwise, the last required was the first of this month (if we're past day 1)
            # or the first of last month (if we're still on day 1 before the run)
            if now.day == 1 and now.hour == 0 and now.minute == 0:
                return this_month_run
            elif now >= this_month_run:
                return this_month_run
            else:
                # We're before day 1 somehow - shouldn't happen, but handle it
                if now.month == 1:
                    return now.replace(year=now.year - 1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    return now.replace(month=now.month - 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        return now  # Fallback

    def check_missed_backups(self, db) -> List[str]:
        """
        Check for missed backups by comparing last execution dates with when they should have run.
        
        Args:
            db: Database connection object with fetch_backup_config() method
            
        Returns:
            List of backup types that are overdue (e.g., ['daily', 'weekly'])
        """
        missed = []
        
        try:
            config = db.fetch_backup_config()
            if not config:
                logger.warning("No backup config found in database")
                return []
            
            for backup_type in ["daily", "weekly", "monthly"]:
                column_name = f"ultimo_{backup_type}"
                last_run = config.get(column_name)
                
                # Calculate when this backup should have last run
                required_date = self.get_last_required_run_date(backup_type)
                
                if last_run is None:
                    # Never run before - definitely missed
                    logger.info(f"Backup '{backup_type}' has never been executed - marked as missed")
                    missed.append(backup_type)
                else:
                    # Convert to naive datetime if needed for comparison
                    if hasattr(last_run, 'tzinfo') and last_run.tzinfo is not None:
                        last_run = last_run.replace(tzinfo=None)
                    
                    # If last run is before the required date, it's missed
                    if last_run < required_date:
                        logger.info(f"Backup '{backup_type}' is overdue. Last run: {last_run}, Required: {required_date}")
                        missed.append(backup_type)
                    else:
                        logger.debug(f"Backup '{backup_type}' is up to date. Last run: {last_run}")
        
        except Exception as e:
            logger.error(f"Error checking for missed backups: {e}")
        
        return missed

    def record_backup_execution(self, db, backup_type: str) -> bool:
        """
        Record that a backup of the given type was executed.
        Updates the corresponding ultimo_<type> column in backup_config.
        
        Args:
            db: Database connection object with update_backup_config() method
            backup_type: One of 'daily', 'weekly', 'monthly'
            
        Returns:
            True if successful, False otherwise
        """
        if backup_type not in ["daily", "weekly", "monthly"]:
            logger.warning(f"Cannot record backup execution for type: {backup_type}")
            return False
        
        try:
            column_name = f"ultimo_{backup_type}"
            db.update_backup_config({column_name: datetime.now()})
            logger.info(f"Recorded backup execution for '{backup_type}'")
            return True
        except Exception as e:
            logger.error(f"Failed to record backup execution for '{backup_type}': {e}")
            return False

    def execute_missed_backups(self, db, missed_types: List[str], progress_callback=None) -> Dict[str, bool]:
        """
        Execute all missed backups and record their execution.
        
        Args:
            db: Database connection object
            missed_types: List of backup types to execute (e.g., ['daily', 'weekly'])
            progress_callback: Optional callback function(backup_type, status) for UI updates
            
        Returns:
            Dict mapping backup_type -> success (True/False)
        """
        results = {}
        
        for i, backup_type in enumerate(missed_types):
            try:
                if progress_callback:
                    progress_callback(backup_type, "running", i + 1, len(missed_types))
                
                logger.info(f"Executing missed backup: {backup_type}")
                file_path = self.create_backup(backup_type)
                
                # Record the execution in the database
                self.record_backup_execution(db, backup_type)
                
                results[backup_type] = True
                logger.info(f"Missed backup '{backup_type}' completed: {file_path}")
                
                if progress_callback:
                    progress_callback(backup_type, "completed", i + 1, len(missed_types))
                    
            except Exception as e:
                logger.error(f"Failed to execute missed backup '{backup_type}': {e}")
                results[backup_type] = False
                
                if progress_callback:
                    progress_callback(backup_type, "failed", i + 1, len(missed_types))
        
        return results


