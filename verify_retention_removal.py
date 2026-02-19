import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from desktop_app.services.backup_service import BackupService
from desktop_app.database import Database
from desktop_app.config import load_config
import logging

# Configure minimal logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def verify_retention_removal():
    print("\n--- Verificación de Eliminación de Política de Retención ---\n")
    
    config = load_config()
    db = Database(config.database_url)

    # 1. Verificar Configuración en Base de Datos
    print("1. Verificando configuración en Base de Datos...")
    retention_db = db.get_config("backup_retention")
    if retention_db is None:
        print("   ✅ CORRECTO: La clave 'backup_retention' NO existe en seguridad.config_sistema.")
    else:
        print(f"   ❌ ERROR: La clave 'backup_retention' AÚN EXISTE: {retention_db}")

    # 2. Verificar Lógica de Borrado (Simulación)
    print("\n2. Verificando Lógica de Borrado (BackupService.prune_backups)...")
    
    # Crear un directorio temporal de backups
    with tempfile.TemporaryDirectory() as temp_dir:
        backup_service = BackupService(pg_bin_path=config.pg_bin_path, db=db)
        # Hack para usar el directorio temporal sin afectar la config real permanentemente
        original_backup_dir = backup_service.backup_dir
        backup_service.backup_dir = Path(temp_dir)
        
        # Crear estructura de carpetas
        (Path(temp_dir) / "daily").mkdir(exist_ok=True)
        
        # Crear un archivo "viejo" (simulado con timestamp antiguo)
        old_file = Path(temp_dir) / "daily" / "backup_old_test.backup"
        old_file.touch()
        
        # Modificar fecha de modificación a hace 2000 días
        old_time = time.time() - (2000 * 86400)
        os.utime(old_file, (old_time, old_time))
        
        print(f"   Archivo creado: {old_file.name}")
        print(f"   Fecha modificada: {datetime.fromtimestamp(old_file.stat().st_mtime)}")
        
        # Intentar ejecutar prune_backups con política agresiva (ej. borrar todo lo > 1 día)
        print("   Ejecutando prune_backups(retention_policy={'daily': 1})...")
        try:
            backup_service.prune_backups(retention_policy={'daily': 1})
        except Exception as e:
            print(f"   ❌ ERROR al ejecutar prune_backups: {e}")
            
        # Verificar si el archivo sigue existiendo
        file_still_exists = old_file.exists()
        if file_still_exists:
            print("   ✅ CORRECTO: El archivo NO FUE BORRADO. La función prune_backups está desactivada.")
        else:
            print("   ❌ FALLO: El archivo FUE BORRADO. La política de retención sigue activa.")
            
        # Restaurar directorio original (aunque es instancia local)
        backup_service.backup_dir = original_backup_dir

    print("\n--- Conclusión ---")
    if retention_db is None and file_still_exists:
        print("RESULTADO FINAL: ✅ EXITOSO. La política de retención ha sido eliminada correctamente.")
    else:
        print("RESULTADO FINAL: ❌ FALLIDO. Revisar los errores anteriores.")

if __name__ == "__main__":
    verify_retention_removal()
