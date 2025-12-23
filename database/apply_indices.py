
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_PORT = os.getenv("DB_PORT", "5432")

def apply_indices():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        # Set autocommit to True because CREATE INDEX CONCURRENTLY cannot run in a transaction block
        # Although we are running simple CREATE INDEX IF NOT EXISTS, autocommit is safer for DDL
        conn.autocommit = True
        cur = conn.cursor()

        print(f"Connected to database '{DB_NAME}'. Applying indices...")

        with open("database/extra_indices.sql", "r", encoding="utf-8") as f:
            sql_script = f.read()

        # Split statements by semicolon to execute individually nicely
        statements = sql_script.split(';')
        
        for statement in statements:
            stmt = statement.strip()
            if stmt:
                try:
                    cur.execute(stmt)
                    print(f"Executed: {stmt[:50]}...")
                except Exception as e:
                    print(f"Error executing statement: {stmt[:50]}... -> {e}")

        print("Indices optimization applied successfully.")
        
    except Exception as e:
        print(f"Failed to connect or apply indices: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    apply_indices()
