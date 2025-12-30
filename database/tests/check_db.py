import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")
if not dsn:
    dsn = f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} dbname={os.getenv('DB_NAME', 'nexoryn')} user={os.getenv('DB_USER', 'postgres')} password={os.getenv('DB_PASSWORD', '')}"

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('app', 'ref', 'seguridad') AND table_type = 'BASE TABLE'")
        tables = cur.fetchall()
        for schema, table in tables:
            cur.execute(f"SELECT count(*) FROM {schema}.{table}")
            count = cur.fetchone()[0]
            print(f"{schema}.{table}: {count}")
