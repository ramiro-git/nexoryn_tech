import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")
if not dsn:
    dsn = f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} dbname={os.getenv('DB_NAME', 'nexoryn')} user={os.getenv('DB_USER', 'postgres')} password={os.getenv('DB_PASSWORD', '')}"

with psycopg.connect(dsn) as conn:
    print(f"Connected to {dsn.split('password=')[0]}...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                schemaname, 
                sequencename 
            FROM pg_sequences 
            WHERE schemaname IN ('app', 'ref', 'seguridad')
        """)
        sequences = cur.fetchall()
        print(f"Found {len(sequences)} sequences.")
        for schema, seq in sequences:
            # Try to find the table/column associated with the sequence
            # This is hard to do generically without complex queries
            # Let's just print the current value
            cur.execute(f"SELECT last_value FROM {schema}.{seq}")
            val = cur.fetchone()[0]
            print(f"Sequence: {schema}.{seq}, Last Value: {val}")
