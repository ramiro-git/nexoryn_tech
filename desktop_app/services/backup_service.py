import os
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import shutil

# Configure logging
logger = logging.getLogger(__name__)

class BackupService:
    def __init__(self, backup_dir: str = "backups", pg_bin_path: Optional[str] = None, sync_dir: Optional[str] = None):
        self.backup_dir = Path(backup_dir)
        self.pg_bin_path = pg_bin_path
        self.sync_dir: Optional[Path] = Path(sync_dir) if sync_dir else None
        self.sync_enabled: bool = sync_dir is not None
        self.last_sync_status: Optional[Dict[str, any]] = None
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create backup subdirectories if they don't exist."""
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Subdirectories for organized backups
        self.daily_dir = self.backup_dir / "daily"
        self.weekly_dir = self.backup_dir / "weekly"
        self.monthly_dir = self.backup_dir / "monthly"
        self.manual_dir = self.backup_dir / "manual"
        
        for d in [self.daily_dir, self.weekly_dir, self.monthly_dir, self.manual_dir]:
            d.mkdir(parents=True, exist_ok=True)
    
    def set_backup_dir(self, new_path: str) -> bool:
        """Change the backup directory to a new location."""
        try:
            new_dir = Path(new_path)
            new_dir.mkdir(parents=True, exist_ok=True)
            self.backup_dir = new_dir
            self._ensure_directories()
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
            if new_path is None or new_path.strip() == "":
                self.sync_dir = None
                self.sync_enabled = False
                logger.info("Cloud sync disabled")
                return True
            
            sync_path = Path(new_path)
            sync_path.mkdir(parents=True, exist_ok=True)
            self.sync_dir = sync_path
            self.sync_enabled = True
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
                        logger.info(f"Pruning old backup: {f}")
                        f.unlink()
                    except Exception as e:
                        logger.error(f"Failed to delete {f}: {e}")

