import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")
if not dsn:
    dsn = f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} dbname={os.getenv('DB_NAME', 'nexoryn')} user={os.getenv('DB_USER', 'postgres')} password={os.getenv('DB_PASSWORD', '')}"

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pid, now() - query_start as duration, query, state 
            FROM pg_stat_activity 
            WHERE state != 'idle' AND pid != pg_backend_pid()
        """)
        rows = cur.fetchall()
        for row in rows:
            print(f"PID: {row[0]}, Duration: {row[1]}, Query: {row[2][:100]}, State: {row[3]}")
