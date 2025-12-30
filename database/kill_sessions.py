#!/usr/bin/env python3
"""
Nexoryn Tech - Session Terminator
Utility to kill all active connections to the database.
Useful for manual resets or when processes get hung.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

try:
    import psycopg
    from psycopg import sql
except ImportError:
    print("ERROR: psycopg is required. Install with: pip install psycopg")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def kill_sessions(db_name: str, host: str, port: int, user: str, password: str):
    """Terminates all sessions for the target database except current one."""
    try:
        # Connect to 'postgres' system database to perform management
        conn = psycopg.connect(
            host=host,
            port=port,
            dbname="postgres",
            user=user,
            password=password,
            autocommit=True
        )
        
        with conn.cursor() as cur:
            logger.info(f"Terminating all sessions for database: {db_name}")
            
            # Count active sessions first
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,)
            )
            count = cur.fetchone()[0]
            
            if count == 0:
                logger.info("No other active sessions found.")
                return

            # Kill them
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                AND pid <> pg_backend_pid()
                """,
                (db_name,)
            )
            
            logger.info(f"Successfully sent termination signal to {count} sessions.")
            
        conn.close()
    except Exception as e:
        logger.error(f"Failed to kill sessions: {e}")
        sys.exit(1)

def main():
    # Load .env if exists in parent or current dir
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    parser = argparse.ArgumentParser(description="Kill all active DB sessions")
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "nexoryn"), help="Target database name")
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"), help="Database host")
    parser.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", 5432)), help="Database port")
    parser.add_argument("--user", default=os.getenv("DB_USER", "postgres"), help="Database user")
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", ""), help="Database password")
    
    args = parser.parse_args()
    
    kill_sessions(args.db_name, args.host, args.port, args.user, args.password)

if __name__ == "__main__":
    main()
