import os

import psycopg2

try:
    from dotenv import load_dotenv
except ImportError:  # Optional dependency for DB scripts
    load_dotenv = None


def get_connection(*, default_db: str = "nexoryn_tech") -> psycopg2.extensions.connection:
    if load_dotenv is not None:
        load_dotenv()

    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", default_db),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "") or os.environ.get("PGPASSWORD", ""),
    )
