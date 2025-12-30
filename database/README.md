# Gestión de Base de Datos - Nexoryn Tech

Este directorio contiene herramientas críticas para la inicialización, mantenimiento y gestión de la base de datos PostgreSQL del sistema.

## Scripts Principales

### 1. `init_db.py` (Inicializador)
Este es el script principal para configurar la base de datos desde cero.
- **Acciones**: Crea el esquema (`database.sql`), importa datos desde la carpeta `csvs/` y puede resetear la base de datos.
- **Dependencias**: `pandas`, `psycopg2-binary`.
- **Ejecución**:
  ```bash
  # Inicialización normal (Esquema + Importación)
  python init_db.py

  # Reset completo (Borra esquemas existentes y recrea todo)
  python init_db.py --reset

  # Solo esquema (Sin importar CSVs)
  python init_db.py --skip-csv
  ```

### 2. `master_script.py` (Herramienta de Mantenimiento)
Script "navaja suiza" para operaciones comunes de administración.
- **Acciones**: Backup, Clear (vaciar datos), Seed (datos de prueba), Restore (restaurar backup) y Reset de secuencias.
- **Ejecución**:
  ```bash
  # Realizar un backup de todas las tablas en /backups
  python master_script.py --backup

  # Generar datos de prueba masivos
  python master_script.py --seed --articles 500 --entities 100

  # Restaurar desde un directorio de backup específico
  python master_script.py --restore ../backups/backup_YYYYMMDD_HHMMSS

  # Vaciar todos los datos de las tablas (mantiene el esquema)
  python master_script.py --clear
  ```

### 3. `kill_sessions.py` (Terminador de Sesiones)
Utilidad para forzar el cierre de todas las conexiones activas a la base de datos.
- **Uso**: Útil cuando PostgreSQL bloquea operaciones de mantenimiento (como `DROP DATABASE`) porque hay procesos conectados.
- **Ejecución**:
  ```bash
  python kill_sessions.py
  ```

### 4. `db_conn.py`
Módulo de utilidad que centraliza la lógica de conexión para los scripts de este directorio. Raramente se ejecuta directamente.

---

## Archivos de Soporte

- `database.sql`: Definición completa del esquema SQL (Tablas, Vistas, Funciones, Triggers).
- `csvs/`: Carpeta que debe contener los archivos legacy (`ARTICULOS.csv`, `CLIPROV.csv`, etc.) para la migración inicial.
- `init_db.log`: Registro detallado del último proceso de inicialización.
