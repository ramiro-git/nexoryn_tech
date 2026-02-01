
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
        """
        Check if schema sync is needed by comparing file version with DB version.
        """
        try:
            # 1. Parse version from file header
            file_version = None
            with open(self.sql_path, "r", encoding="utf-8") as f:
                for line in f:
                    if "Version:" in line:
                         # Expected format: "-- Version: 2.2 - ..."
                        parts = line.split("Version:")
                        if len(parts) > 1:
                            file_version = parts[1].split("-")[0].strip()
                            break
                    if not line.startswith("--"): # Stop if header ends
                        break
            
            if not file_version:
                logger.warning("Could not parse version from database.sql header. Forcing sync.")
                return True

            # 2. Check DB version
            # We need a connection. If this is invoked from ui_basic, 'db' might be closed or not passed properly?
            # Creating a lightweight connection here just for the check is tricky without credentials.
            # However, looking at __init__, 'db' is passed but unused in current code.
            
            # Use subprocess to check version quickly? 
            # Or use the passed 'db' object if available?
            # 'db' object in ui_basic IS available.
            # But SchemaSync receives 'db'.
            
            # Let's inspect __init__ again to be sure.
            # Line 21: db, # Not used in psql mode but kept for signature compatibility
            
            # We can use the 'psql' way to check version to avoid python driver dependency if needed,
            # BUT since we are in the app, we likely have python db access.
            # Actually, reusing the 'psql' subprocess approach is safer for credential consistency if environment vars are used.
            
            config = self._get_db_config()
            psql = self._get_psql_path()
            env = os.environ.copy()
            env["PGPASSWORD"] = config["password"]
            
            cmd = [
                psql,
                "-h", config["host"],
                "-p", config["port"],
                "-U", config["user"],
                "-d", config["name"],
                "-t", # Tuple only (no headers)
                "-c", "SELECT valor FROM seguridad.config_sistema WHERE clave = 'db_version';"
            ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=5)
            
            if result.returncode != 0:
                # Table might not exist yet
                logger.debug("DB version check failed (likely new DB). Sync needed.")
                return True
                
            db_version = result.stdout.strip()
            
            logger.info(f"Schema Version Check: File={file_version}, DB={db_version}")
            
            if db_version == file_version:
                logger.info("Database is up to date. Skipping sync.")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking schema version: {e}. Forcing sync.")
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
                timeout=120
            )
            
            if result.returncode != 0:
                logger.error(f"Schema sync stderr: {result.stderr}")
                return SyncResult(success=False, error=f"psql failed: {result.stderr}")
                
            logger.info("Schema sync complete.")
            return SyncResult(success=True)
            
        except Exception as e:
            logger.exception("Schema sync failed with exception")
            return SyncResult(success=False, error=str(e))
