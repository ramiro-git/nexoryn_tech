# Sistema de Backups Profesionales (FULL/DIFERENCIAL/INCREMENTAL)

## Resumen

El sistema profesional de backups está implementado en `BackupManager` + `BackupIncrementalService`. Genera backups concatenables FULL + DIFERENCIAL + INCREMENTAL y los registra en base de datos.

**Cómo detecta cambios:**
- Usa `pg_stat_user_tables` para detectar tablas con actividad desde el último backup.
- Guarda estadísticas en `backups_incrementales/stats/*.json`.
- Si PostgreSQL reinicia y las estadísticas se reinician, el sistema considera las tablas como “cambiadas” para estar del lado seguro.

> Esto es un incremental lógico (por tablas) y **no** basado en WAL.

## Características Principales

- Backups FULL (mensuales) con `pg_dump -F c`
- Backups DIFERENCIALES (semanales) por tablas con cambios desde el último FULL
- Backups INCREMENTALES (diarios) por tablas con cambios desde el último backup (full/dif/inc)
- Cadena de restauración: FULL + DIFERENCIAL + INCREMENTALES
- Validación por existencia de archivos y checksum SHA-256
- Scheduler integrado vía APScheduler al iniciar la app
- No modifica el sistema de backups legacy (`backups/`)

## Arquitectura

### Directorios

```
backups_incrementales/
├── full/
├── differential/
├── incremental/
└── stats/              # estadísticas por backup para detectar cambios
```

### Base de Datos

Tablas principales:
- `seguridad.backup_manifest`
- `seguridad.backup_chain`
- `seguridad.backup_validation`
- `seguridad.backup_event` (estructura disponible, no siempre poblada)

Configuración en `seguridad.config_sistema`:
- `backup_schedules` (JSON)
- `backup_retention` (JSON)
- `backup_cloud_config` (JSON)

## Scheduler Integrado

**Horarios por defecto (hora local):**
- **FULL**: Día 1 de cada mes a las 00:00
- **DIFERENCIAL**: Domingos a las 23:30
- **INCREMENTAL**: Diariamente a las 23:00
- **Validación**: Diariamente a las 01:00

**Edición desde UI:**
- El panel **Respaldos** permite cambiar días y horas.
- Los minutos se mantienen según el default (FULL 00, DIF 30, INC 00).
- Los cambios se guardan en `backup_schedules`.

**Backups omitidos:**
Al iniciar la app, se detectan backups faltantes y se ejecutan en orden `FULL → DIFERENCIAL → INCREMENTAL`.

## Uso desde la UI

En el panel **Respaldos** puedes:
- Ejecutar backups manuales (FULL/DIF/INC).
- Ver historial y métricas.
- Validar backups existentes.
- Configurar horarios y retención.

## Restauración

### API Programática

```python
from desktop_app.database import Database
from desktop_app.config import load_config
from desktop_app.services.backup_incremental_service import BackupIncrementalService
from desktop_app.services.restore_service import RestoreService

config = load_config()
db = Database(config.database_url)

backup_service = BackupIncrementalService(db)
restore_service = RestoreService(db, backup_service)

# Restaurar a una fecha específica
from datetime import datetime
target_date = datetime(2025, 12, 15, 12, 0, 0)
result = restore_service.restore_to_date(target_date)

if result.exitoso:
    print(f"Restauración exitosa: {result.mensaje}")
    print(f"Backups aplicados: {result.backups_aplicados}")
    print(f"Tiempo: {result.tiempo_segundos}s")

# Previsualizar restauración
preview = restore_service.preview_restore(target_date)
print(f"Cantidad de backups: {preview['cantidad_backups']}")
print(f"Tamaño total: {preview['tamano_total_mb']} MB")
print(f"Backups a aplicar: {[b['archivo'] for b in preview['backups']]}")
```

### Línea de Comandos (Bootstrap rápido)

```bash
python -c "
from desktop_app.database import Database
from desktop_app.config import load_config
from desktop_app.services.backup_manager import BackupManager

db = Database(load_config().database_url)
manager = BackupManager(db)
result = manager.restore_from_backup_id(123, 'nexoryn_tech_restaurado')
print(result)
"
```

> Nota: la base de datos destino debe existir antes de restaurar.

## Validaciones

`validate_backup_chain(backup_id)` verifica:
- existencia del archivo
- checksum SHA-256 (si está registrado)

No valida contenido lógico ni dependencias externas.

## Nube / Sync

El sistema soporta **sincronización a carpeta local** mediante `CloudStorageService`:
- `provider=LOCAL` copia el archivo a `sync_dir`.
- `provider=GOOGLE_DRIVE` y `provider=S3` están **reservados**; hoy hacen fallback a carpeta local si `sync_dir` existe.

La configuración se guarda en `backup_cloud_config`.

## Retención

- La UI permite definir retención (FULL meses / DIF semanas / INC días).
- La configuración se guarda en `backup_retention`.
- **No hay purga automática** en el sistema incremental actual.
- Existe `purge_invalid_backups()` para limpiar registros huérfanos (archivos faltantes).

## Configuración

### Variables de Entorno

```bash
# Conexión a PostgreSQL
export DATABASE_URL=postgresql://postgres:password@localhost:5432/nexoryn_tech
# o DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD

# Ruta a binarios de PostgreSQL
export PG_BIN_PATH="C:\Program Files\PostgreSQL\16\bin"
```

## Solución de Problemas

### `pg_dump` / `pg_restore` no encontrados
1. Instalar PostgreSQL con herramientas de línea de comandos.
2. Configurar `PG_BIN_PATH` o agregar `bin` al `PATH`.

### Permisos / archivos en uso
- Ejecutar la app con permisos suficientes para crear archivos en el directorio de backups.

## Compatibilidad con Sistema Antiguo

El sistema legacy (`backups/` con daily/weekly/monthly/manual) **no es modificado**.
Si el sistema profesional falla al iniciar, la app intenta usar el servicio legacy como fallback para no detener el flujo de respaldos.
