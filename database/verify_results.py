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
        # Check stock for top 10 articles
        cur.execute("SELECT id_articulo, articulo, stock_total FROM app.v_stock_total LIMIT 10")
        stocks = cur.fetchall()
        print("Stock Results (Top 10):")
        for s in stocks:
            print(f"ID: {s[0]}, Name: {s[1]}, Stock: {s[2]}")
        
        # Check document statuses
        cur.execute("SELECT estado, COUNT(*) FROM app.documento GROUP BY estado")
        stats = cur.fetchall()
        print("\nDocument States:")
        for s in stats:
            print(f"State: {s[0]}, Count: {s[1]}")
            
    conn.close()
except Exception as e:
    print(f"Error: {e}")
