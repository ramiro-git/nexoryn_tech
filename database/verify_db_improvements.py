import os
import sys
import psycopg2
from psycopg2 import sql

def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "nexoryn"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "") or os.environ.get("PGPASSWORD", "")
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def check_views(cur):
    views = [
        "v_reporte_ventas_mensual",
        "v_top_articulos_mes",
        "v_deudores"
    ]
    print("\n--- Checking Views ---")
    all_ok = True
    for v in views:
        cur.execute("SELECT 1 FROM information_schema.views WHERE table_schema = 'app' AND table_name = %s", (v,))
        if cur.fetchone():
            print(f"[OK] View 'app.{v}' exists.")
        else:
            print(f"[FAIL] View 'app.{v}' MISSING.")
            all_ok = False
    return all_ok

def check_rls(cur):
    tables = ["documento", "entidad_comercial", "movimiento_articulo"]
    print("\n--- Checking RLS ---")
    all_ok = True
    for t in tables:
        cur.execute("""
            SELECT relrowsecurity 
            FROM pg_class c 
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = 'app' AND c.relname = %s
        """, (t,))
        res = cur.fetchone()
        if res and res[0]:
            print(f"[OK] RLS enabled on 'app.{t}'.")
        else:
            print(f"[FAIL] RLS NOT enabled on 'app.{t}'.")
            all_ok = False
    return all_ok

def check_indexes(cur):
    indexes = [
        "idx_entidad_email_lower",
        "idx_articulo_lookup_covering",
        "idx_documento_fecha_estado"
    ]
    print("\n--- Checking Indexes ---")
    all_ok = True
    for idx in indexes:
        cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (idx,))
        if cur.fetchone():
            print(f"[OK] Index '{idx}' exists.")
        else:
            print(f"[FAIL] Index '{idx}' MISSING.")
            all_ok = False
    return all_ok

def main():
    print("Verifying Database Improvements...")
    conn = get_connection()
    all_passed = True
    with conn.cursor() as cur:
        if not check_views(cur): all_passed = False
        if not check_rls(cur): all_passed = False
        if not check_indexes(cur): all_passed = False
    
    conn.close()
    
    if all_passed:
        print("\nSUCCESS: All database improvements verified!")
    else:
        print("\nWARNING: Some improvements are missing. Please run database.sql.")

if __name__ == "__main__":
    main()
