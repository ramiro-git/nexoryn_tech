from desktop_app.config import load_config
from desktop_app.database import Database
import os

def test_sorting():
    print("Iniciando prueba de ordenamiento...")
    config = load_config()
    db = Database(config.database_url)
    
    try:
        # Get a valid price list ID to test with
        with db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ref.lista_precio LIMIT 1")
                res = cur.fetchone()
                lp_id = res[0] if res else None

        if not lp_id:
            print("No hay listas de precio para probar.")
            return

        print(f"Probando con lista de precio ID: {lp_id}")

        # Test DESC sort (Previous issue: NULLs were at top)
        print("\n--- Test 1: Orden DESC (Los precios más altos primero, nulos al final) ---")
        articles_desc = db.fetch_articles(
            sorts=[('precio_lista', 'desc')],
            advanced={'id_lista_precio': lp_id},
            limit=10
        )
        
        for art in articles_desc:
            price = art.get('precio_lista')
            print(f"Art: {art['nombre'][:20]:<20} | Precio: {price}")
        
        # Check first item (should be non-null if data exists)
        if articles_desc and articles_desc[0].get('precio_lista') is None:
            # Check if ALL are null (edge case) or if sort failed
            all_null = all(a.get('precio_lista') is None for a in articles_desc)
            if not all_null:
                print("FAIL: El primer elemento es NULL pero hay elementos con precio.")
            else:
                print("INFO: Todos los resultados son NULL.")
        else:
            print("PASS: El primer elemento tiene precio (o la lista está vacía).")

        # Test ASC sort
        print("\n--- Test 2: Orden ASC (Los precios más bajos primero, nulos al final) ---")
        articles_asc = db.fetch_articles(
            sorts=[('precio_lista', 'asc')],
            advanced={'id_lista_precio': lp_id},
            limit=10
        )
        
        for art in articles_asc:
            price = art.get('precio_lista')
            print(f"Art: {art['nombre'][:20]:<20} | Precio: {price}")

        # Check last items generally (harder to assert blindly without full dataset, but visual check helps)
        
    except Exception as e:
        print(f"Error durante la prueba: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    test_sorting()
