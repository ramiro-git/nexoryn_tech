import psycopg2

conn_params = {
    "host": "localhost",
    "port": 5432,
    "database": "nexoryn_tech",
    "user": "postgres",
    "password": "postgres"
}

try:
    conn = psycopg2.connect(**conn_params)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'app' AND table_name = 'v_stock_total'
        """)
        cols = cur.fetchall()
        print("Columns in v_stock_total:")
        for c in cols:
            print(c[0])
            
    conn.close()
except Exception as e:
    print(f"Error: {e}")
