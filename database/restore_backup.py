#!/usr/bin/env python3
"""
Nexoryn Tech - Database Restoration Script
Resets the database and restores it from a selected backup file.
"""

import os
import sys
import subprocess
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Try to import dependencies
try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("ERROR: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKUPS_DIR = PROJECT_ROOT / "backups_incrementales" / "full"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_environment():
    """Load environment variables from .env file."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists() and load_dotenv:
        load_dotenv(dotenv_path=env_path)
    else:
        logger.warning(f".env file not found at {env_path} or python-dotenv not installed.")

def get_db_config(args):
    """Get database configuration from arguments or environment."""
    return {
        "host": args.host or os.getenv("DB_HOST", "localhost"),
        "port": args.port or int(os.getenv("DB_PORT", 5432)),
        "name": args.db_name or os.getenv("DB_NAME", "nexoryn_tech"),
        "user": args.user or os.getenv("DB_USER", "postgres"),
        "password": args.password or os.getenv("DB_PASSWORD", "") or os.getenv("PGPASSWORD", ""),
        "pg_bin": os.getenv("PG_BIN_PATH", "")
    }

def kill_sessions(config):
    """Terminates all sessions for the target database."""
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database="postgres",
            user=config["user"],
            password=config["password"]
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            logger.info(f"Terminating active sessions for database: {config['name']}")
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                AND pid <> pg_backend_pid()
                """,
                (config["name"],)
            )
            count = cur.rowcount
            logger.info(f"Terminated {count} sessions.")
        conn.close()
    except Exception as e:
        logger.error(f"Failed to kill sessions: {e}")
        # We continue anyway as the DB might not have active sessions

def reset_database(config):
    """Drops and recreates the target database."""
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database="postgres",
            user=config["user"],
            password=config["password"]
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            logger.info(f"Dropping database '{config['name']}' if exists...")
            cur.execute(f'DROP DATABASE IF EXISTS "{config["name"]}"')
            
            logger.info(f"Creating fresh database '{config['name']}'...")
            cur.execute(f'CREATE DATABASE "{config["name"]}" ENCODING "UTF8"')
            
        conn.close()
        logger.info("Database reset successfully.")
    except Exception as e:
        logger.error(f"Failed to reset database: {e}")
        sys.exit(1)

def list_backups():
    """Returns a list of available backups in the full backups directory."""
    if not BACKUPS_DIR.exists():
        logger.error(f"Backups directory not found: {BACKUPS_DIR}")
        return []
    
    backups = sorted(list(BACKUPS_DIR.glob("*.backup")), key=os.path.getmtime, reverse=True)
    return backups

def restore_backup(config, backup_path):
    """Executes pg_restore to restore the database from a backup file."""
    logger.info(f"Starting restoration from: {backup_path.name}")
    
    # Construct path to pg_restore
    pg_restore_exe = "pg_restore"
    if config["pg_bin"]:
        pg_restore_exe = str(Path(config["pg_bin"]) / "pg_restore")

    # Set password in environment for pg_restore
    env = os.environ.copy()
    if config["password"]:
        env["PGPASSWORD"] = config["password"]

    cmd = [
        pg_restore_exe,
        "--host", config["host"],
        "--port", str(config["port"]),
        "--username", config["user"],
        "--dbname", config["name"],
        "--verbose",
        "--no-owner",  # Often safer during restoration to avoid role issues
        "--no-privileges",
        str(backup_path)
    ]

    try:
        # Using subprocess.run to execute the command
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Restoration completed successfully.")
        else:
            # pg_restore often returns non-zero for minor warnings, check output
            if "errors ignored on restore" in result.stderr:
                logger.warning("Restoration completed with some warnings/ignored errors.")
            else:
                logger.error(f"Restoration failed with return code {result.returncode}")
                logger.error(result.stderr)
                sys.exit(1)
                
    except FileNotFoundError:
        logger.error(f"Could not find pg_restore. Ensure it is in your PATH or PG_BIN_PATH is correct in .env.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred during restoration: {e}")
        sys.exit(1)

def ensure_schema_completeness(config):
    """Executes database.sql to ensure all tables (like backup_manifest) exist."""
    schema_file = SCRIPT_DIR / "database.sql"
    if not schema_file.exists():
        logger.warning(f"Schema file not found at {schema_file}. Skipping schema completeness check.")
        return

    logger.info("Ensuring schema completeness (recreating missing tables/indexes)...")
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["name"],
            user=config["user"],
            password=config["password"]
        )
        conn.autocommit = True
        
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_sql = f.read()
            
        with conn.cursor() as cur:
            # We execute the whole file. CREATE TABLE IF NOT EXISTS will skip existing ones.
            cur.execute(schema_sql)
            
        conn.close()
        logger.info("Schema completeness verified.")
    except Exception as e:
        logger.error(f"Failed to ensure schema completeness: {e}")
        # We don't exit here as the main restoration might have been enough for some use cases

def main():
    load_environment()
    
    parser = argparse.ArgumentParser(description="Reset and restore Nexoryn Tech database from a full backup.")
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--db-name", help="Database name")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--backup", help="Specific backup file name (in backups_incrementales/full)")
    parser.add_argument("--list", action="store_true", help="List available backups and exit")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    # List backups if requested
    backups = list_backups()
    if args.list:
        print("\nAvailable backups (most recent first):")
        for i, b in enumerate(backups):
            size_mb = os.path.getsize(b) / (1024 * 1024)
            mtime = datetime.fromtimestamp(os.path.getmtime(b)).strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{i}] {b.name} - {size_mb:.2f} MB - {mtime}")
        return

    if not backups:
        logger.error("No backup files found in backups_incrementales/full")
        sys.exit(1)

    # Select backup
    selected_backup = None
    if args.backup:
        potential_path = BACKUPS_DIR / args.backup
        if potential_path.exists():
            selected_backup = potential_path
        else:
            logger.error(f"Backup file not found: {args.backup}")
            sys.exit(1)
    else:
        selected_backup = backups[0] # Most recent
        logger.info(f"Using most recent backup: {selected_backup.name}")

    config = get_db_config(args)

    # Confirmation
    if not args.yes:
        print(f"\nWARNING: This will DROP the database '{config['name']}' and restore it from '{selected_backup.name}'.")
        print(f"Server: {config['host']}:{config['port']}")
        confirm = input("Are you sure? (yes/no): ").lower()
        if confirm != 'yes':
            print("Operation cancelled.")
            return

    # Execution
    kill_sessions(config)
    reset_database(config)
    restore_backup(config, selected_backup)
    ensure_schema_completeness(config)

if __name__ == "__main__":
    main()
