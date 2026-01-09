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

### 2. `kill_sessions.py` (Terminador de Sesiones)
Utilidad para forzar el cierre de todas las conexiones activas a la base de datos.
- **Uso**: Útil cuando PostgreSQL bloquea operaciones de mantenimiento (como `DROP DATABASE`) porque hay procesos conectados.
- **Ejecución**:
  ```bash
  python kill_sessions.py
  ```

### 3. `db_conn.py`
Módulo de utilidad que centraliza la lógica de conexión para los scripts de este directorio. Raramente se ejecuta directamente.

## Sincronizacion automatica desde `database.sql`

El esquema se sincroniza leyendo `database/database.sql` y aplicando solo cambios seguros.
No se re-ejecuta el archivo completo ni se borra informacion existente.

- **Fuente**: `database/database.sql`.
- **Control de cambios**: hash guardado en `seguridad.config_sistema` (clave `schema_hash`).
- **Aplicacion**: automatico al iniciar el ejecutable, sin comandos manuales.
- **Operaciones permitidas**:
  - `CREATE EXTENSION IF NOT EXISTS`
  - `CREATE SCHEMA IF NOT EXISTS`
  - `CREATE TABLE` si la tabla no existe
  - `ADD COLUMN` solo si es compatible (nullable o con default, sin PK/UNIQUE/CHECK)
  - `CREATE INDEX` si no existe
- **Operaciones ignoradas**:
  - cambios destructivos (drop, truncate, alter incompatible)
  - DML masivo (insert/update/delete) para evitar duplicados

> Nota: si se requiere un cambio riesgoso, debe hacerse en un flujo aparte con backup y confirmacion.
