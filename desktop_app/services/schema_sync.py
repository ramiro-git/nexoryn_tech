
import os
import subprocess
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Configure logger
logger = logging.getLogger("schema_sync")
logger.setLevel(logging.INFO)

@dataclass
class SyncResult:
    success: bool
    error: Optional[str] = None

class SchemaSync:
    def __init__(
        self,
        db, # Not used in psql mode but kept for signature compatibility
        *,
        sql_path: Path,
        pg_bin_path: Optional[str] = None, # Optional path to psql
        **kwargs
    ) -> None:
        self.sql_path = sql_path
        self.pg_bin_path = pg_bin_path

    def needs_sync(self) -> bool:
        return True

    def _get_db_config(self) -> dict:
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "name": os.getenv("DB_NAME", "nexoryn_tech"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "") or os.environ.get("PGPASSWORD", ""),
        }
        
    def _get_psql_path(self) -> str:
        # Check explicit path
        if self.pg_bin_path:
            p = Path(self.pg_bin_path) / "psql.exe"
            if p.exists(): return str(p)
            p = Path(self.pg_bin_path) / "psql"
            if p.exists(): return str(p)
            
        # Check PATH
        path = subprocess.run(["where", "psql"], capture_output=True, text=True).stdout.strip().split('\n')[0]
        if path and os.path.exists(path.strip()):
            return path.strip()
            
        # Check common locations
        common_paths = [
            r"C:\Program Files\PostgreSQL\18\bin\psql.exe",
            r"C:\Program Files\PostgreSQL\17\bin\psql.exe",
            r"C:\Program Files\PostgreSQL\16\bin\psql.exe",
            r"C:\Program Files\PostgreSQL\15\bin\psql.exe",
            r"C:\Program Files\PostgreSQL\14\bin\psql.exe",
        ]
        for p in common_paths:
            if os.path.exists(p):
                return p
        
        raise FileNotFoundError("psql not found. Please ensure PostgreSQL bin directory is in PATH.")

    def apply(self, **kwargs) -> SyncResult:
        if not self.sql_path.exists():
            return SyncResult(success=False, error=f"File not found: {self.sql_path}")
            
        logger.info(f"Syncing schema from {self.sql_path} using psql...")
        
        try:
            config = self._get_db_config()
            psql = self._get_psql_path()
            
            env = os.environ.copy()
            env["PGPASSWORD"] = config["password"]
            
            # Use psql to execute the file
            # -f file, -d dbname, -h host, -p port, -U user
            # -v ON_ERROR_STOP=1 to fail on error
            cmd = [
                psql,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-d", config["name"],
                "-v", "ON_ERROR_STOP=1",
                "-w",
                "-f", str(self.sql_path)
            ]
            # print(f"DEBUG: Running psql command: {cmd}", flush=True)
            
            result = subprocess.run(
                cmd, 
                env=env, 
                capture_output=True, 
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=15
            )
            
            if result.returncode != 0:
                logger.error(f"Schema sync stderr: {result.stderr}")
                return SyncResult(success=False, error=f"psql failed: {result.stderr}")
                
            logger.info("Schema sync complete.")
            return SyncResult(success=True)
            
        except Exception as e:
            logger.exception("Schema sync failed with exception")
            return SyncResult(success=False, error=str(e))
