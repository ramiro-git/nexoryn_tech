from desktop_app.database import Database
from desktop_app.config import load_config
import logging
from datetime import datetime

# Setup
logging.basicConfig(level=logging.ERROR)

def check_backups():
    print("\n--- Estado Actual de Backups (Base de Datos) ---\n")
    try:
        config = load_config()
        db = Database(config.database_url)
        
        query = """
            SELECT tipo_backup, fecha_inicio, estado, tamano_bytes, archivo_nombre 
            FROM seguridad.backup_manifest 
            ORDER BY fecha_inicio DESC 
            LIMIT 5
        """
        
        with db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                if not rows:
                    print("No se encontraron registros de backups en la base de datos.")
                    return

                print(f"{'TIPO':<15} | {'FECHA':<20} | {'ESTADO':<10} | {'TAMAÑO':<15} | {'ARCHIVO'}")
                print("-" * 80)
                
                for row in rows:
                    tipo, fecha, estado, bytes_size, nombre = row
                    
                    # Convert bytes to human readable
                    if bytes_size < 1024:
                        size_str = f"{bytes_size} B"
                    elif bytes_size < 1024*1024:
                        size_str = f"{bytes_size/1024:.2f} KB"
                    else:
                        size_str = f"{bytes_size/(1024*1024):.2f} MB"
                        
                    print(f"{tipo:<15} | {fecha.strftime('%Y-%m-%d %H:%M:%S'):<20} | {estado:<10} | {size_str:<15} | {nombre}")
                    
    except Exception as e:
        print(f"Error consultando la base de datos: {e}")

if __name__ == "__main__":
    check_backups()
