
import psycopg2
from pathlib import Path
import sys

def check_counts():
    try:
        conn = psycopg2.connect(
            dbname="nexoryn_db",
            user="postgres",
            password="admin", # Default password seen in logs
            host="localhost",
            port="5432"
        )
        with conn.cursor() as cur:
            tables = [
                'app.entidad_comercial',
                'app.articulo',
                'app.documento',
                'app.documento_detalle',
                'app.movimiento_articulo',
                'app.pago',
                'ref.tipo_documento',
                'ref.tipo_movimiento_articulo'
            ]
            print(f"{'Table':<30} | {'Count':<10}")
            print("-" * 45)
            for t in tables:
                cur.execute(f"SELECT count(*) FROM {t}")
                count = cur.fetchone()[0]
                print(f"{t:<30} | {count:<10}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_counts()
