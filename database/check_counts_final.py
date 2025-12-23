
import psycopg2
import sys

def check_counts():
    try:
        conn = psycopg2.connect(
                dbname="nexoryn_tech",
                user="postgres",
                password="postgres",
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
                'app.pago'
            ]
            for t in tables:
                cur.execute(f"SELECT count(*) FROM {t}")
                count = cur.fetchone()[0]
                print(f"{t}: {count}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_counts()
