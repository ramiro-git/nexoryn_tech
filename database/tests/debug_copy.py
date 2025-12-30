import os
import psycopg
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")
if not dsn:
    dsn = f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} dbname={os.getenv('DB_NAME', 'nexoryn')} user={os.getenv('DB_USER', 'postgres')} password={os.getenv('DB_PASSWORD', '')}"

csv_file = Path("backups/backup_20251227_225551/ref.condicion_iva.csv")
print(f"File size: {csv_file.stat().st_size}")

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE ref.condicion_iva CASCADE")
        with open(csv_file, "rb") as f:
            with cur.copy("COPY ref.condicion_iva FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                data = f.read()
                print(f"Data read: {len(data)} bytes")
                copy.write(data)
        conn.commit()
        
        cur.execute("SELECT count(*) FROM ref.condicion_iva")
        print(f"Count after: {cur.fetchone()[0]}")
