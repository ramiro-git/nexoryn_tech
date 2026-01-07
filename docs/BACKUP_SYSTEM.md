# Sistema de Backups Profesionales (Incremental/Diferencial)

## Resumen

Este sistema implementa un esquema de backups profesional con capacidad de concatenación FULL + DIFERENCIAL + INCREMENTAL, permitiendo reconstruir cualquier punto en el tiempo sin pérdida de datos.

## Características Principales

- Backups FULL (mensuales): Base completa de datos
- Backups DIFERENCIALES (semanales): Cambios desde último FULL
- Backups INCREMENTALES (diarios): Cambios desde último backup (incremental o diferencial)
- Restauración concatenable: FULL + DIFERENCIAL + INCREMENTALES
- Validaciones con checksum SHA-256
- Integración con nube (Google Drive/S3/Carpeta Local)
- Automatización completa vía scheduler
- No modifica el sistema de backups existente

## Arquitectura

### Directorios

```
backups_incrementales/
├── full/               # Backups completos (mensuales)
├── differential/         # Backups diferenciales (semanales)
└── incremental/          # Backups incrementales (diarios)
```

### Base de Datos

- `seguridad.backup_manifest`: Registro de todos los backups
- `seguridad.backup_chain`: Relaciones entre backups
- `seguridad.backup_validation`: Historial de validaciones
- `seguridad.backup_retention_policy`: Políticas de retención
- `seguridad.backup_event`: Eventos del sistema de backups

### Vistas Útiles

- `seguridad.v_backup_resumen`: Resumen de backups
- `seguridad.v_backup_cadenas`: Cadenas concatenables

## Instalación

### 1. Ejecutar migración de base de datos

```bash
cd database
psql -U postgres -d nexoryn_tech -f migrations/002_add_incremental_backup_system.sql
```

### 2. Verificar instalación

```bash
python scripts/backup_scheduler.py status
```

## Uso

### Scheduler Automatizado

Iniciar el scheduler que ejecutará backups automáticamente:

```bash
python scripts/backup_scheduler.py start
```

**Horarios por defecto:**
- **FULL**: Día 1 de cada mes a las 00:00
- **DIFERENCIAL**: Domingos a las 23:30
- **INCREMENTAL**: Diariamente a las 23:00
- **Validación**: Diariamente a las 01:00
- **Limpieza**: Lunes a las 02:00

### Ejecuciones Manuales

```bash
# Backup FULL inmediato
python scripts/backup_scheduler.py full

# Backup DIFERENCIAL inmediato
python scripts/backup_scheduler.py diferencial

# Backup INCREMENTAL inmediato
python scripts/backup_scheduler.py incremental

# Mostrar estado del sistema
python scripts/backup_scheduler.py status

# Validar todos los backups
python scripts/backup_scheduler.py validate

# Limpiar backups antiguos
python scripts/backup_scheduler.py cleanup
```

## Restauración

### API Programática

```python
from database.db_conn import Database
from config import load_config
from services.backup_incremental_service import BackupIncrementalService
from services.restore_service import RestoreService

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

### Línea de Comandos

```bash
# Restaurar desde backup ID específico
python -c "
from database.db_conn import Database
from config import load_config
from services.backup_manager import BackupManager

db = Database(load_config().database_url)
manager = BackupManager(db)
result = manager.restore_from_backup_id(123, 'nexoryn_tech_restaurado')
print(result)
"
```

## Monitoreo

### Estadísticas del Sistema

```python
from services.backup_manager import BackupManager

manager = BackupManager(db)
status = manager.get_status_summary()

# Estadísticas de backups
stats = status['estadisticas']
for tipo, info in stats.items():
    if tipo != '_total':
        print(f"{tipo}: {info['cantidad']} archivos, {info['tamano_mb']} MB")

print(f"Total: {stats['_total']['cantidad']} archivos, {stats['_total']['tamano_gb']} GB")

# Espacio utilizado
espacio = status['espacio_uso']
print(f"Total: {espacio['total']['gb']} GB")

# Puntos de restauración disponibles
puntos = status['puntos_restauracion']
for punto in puntos:
    print(f"{punto['fecha']}: {punto['cantidad_backups']} backups")
```

### Consultas SQL Útiles

```sql
-- Resumen de backups
SELECT * FROM seguridad.v_backup_resumen ORDER BY fecha_inicio DESC LIMIT 20;

-- Cadenas de backup concatenables
SELECT * FROM seguridad.v_backup_cadenas ORDER BY full_fecha DESC;

-- Espacio utilizado por tipo
SELECT * FROM seguridad.get_backup_space_usage();

-- Últimos backups de cada tipo
SELECT DISTINCT ON (tipo_backup) *
FROM seguridad.backup_manifest
WHERE estado = 'COMPLETADO'
ORDER BY tipo_backup, fecha_inicio DESC;

-- Backups que no están en la nube
SELECT tipo_backup, archivo_nombre, fecha_inicio, tamano_bytes
FROM seguridad.backup_manifest
WHERE nube_subido = FALSE AND estado = 'COMPLETADO'
ORDER BY fecha_inicio DESC;

-- Validaciones recientes
SELECT v.*, bm.tipo_backup, bm.archivo_nombre
FROM seguridad.backup_validation v
JOIN seguridad.backup_manifest bm ON v.backup_id = bm.id
ORDER BY v.fecha_validacion DESC
LIMIT 20;
```

## Integración con Nube

### Configurar Sync con Carpeta Local

```python
from services.cloud_storage_service import CloudStorageService

cloud_config = {
    'sync_dir': r'C:\Users\TuUsuario\Google Drive\Nexoryn Backups'
}

cloud_service = CloudStorageService(db, provider='LOCAL', config=cloud_config)

# Subir backup a nube
from pathlib import Path
backup_file = Path('backups_incrementales/full/full_20250106_000000.backup')
result = cloud_service.upload_backup(backup_file, backup_id=1, backup_type='FULL')

if result.exitoso:
    print(f"Backup subido: {result.url}")
```

### Configurar Google Drive

```python
cloud_config = {
    'gdrive_credentials': 'path/to/credentials.json',
    'gdrive_folder_id': 'folder_id'
}

cloud_service = CloudStorageService(db, provider='GOOGLE_DRIVE', config=cloud_config)
```

### Configurar AWS S3

```python
cloud_config = {
    's3_bucket': 'nexoryn-backups',
    's3_region': 'us-east-1',
    's3_access_key': 'your_access_key',
    's3_secret_key': 'your_secret_key'
}

cloud_service = CloudStorageService(db, provider='S3', config=cloud_config)
```

## Políticas de Retención

### Política Estándar

- **Backups FULL**: Retener 12 meses
- **Backups DIFERENCIALES**: Retener 8 semanas
- **Backups INCREMENTALES**: Retener 7 días

### Personalizar Política

```sql
INSERT INTO seguridad.backup_retention_policy (nombre, descripcion, 
    retencion_full_meses, retencion_diferencial_semanas, retencion_incremental_dias)
VALUES ('PERSONALIZADA', 'Política personalizada', 
    24, 16, 14);
```

## Validaciones

### Verificar Integridad de Backups

```python
from services.restore_service import RestoreService

restore_service = RestoreService(db, backup_service)

# Validar backup específico
result = restore_service.validate_backup_chain(backup_id=123)

if result['valido']:
    print("Backup válido")
else:
    print("Backup inválido:")
    for validacion in result['validaciones']:
        print(f"  {validacion['tipo']}: {validacion['mensaje']}")
```

### Verificar Checksum SHA-256

```bash
# Calcular checksum local
sha256sum archivo.backup

# Comparar con base de datos
psql -U postgres -d nexoryn_tech -c "
SELECT archivo_nombre, checksum_sha256 
FROM seguridad.backup_manifest 
WHERE id = 123;
"
```

## Configuración

### Variables de Entorno

```bash
# Conexión a PostgreSQL
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=nexoryn_tech
export DB_USER=postgres
export DB_PASSWORD=tu_contraseña

# Ruta a binarios de PostgreSQL (opcional)
export PG_BIN_PATH="C:\Program Files\PostgreSQL\16\bin"

# Directorio de backups (opcional)
export BACKUP_DIR="C:\backups"
```

### Modificar Horarios

```python
from services.backup_manager import BackupManager

manager = BackupManager(db)

# Modificar horario de backup FULL (día 1 del mes)
manager.set_schedule('FULL', day=1, hour=0, minute=0)

# Modificar horario de backup DIFERENCIAL (domingo)
manager.set_schedule('DIFERENCIAL', weekday=6, hour=23, minute=30)

# Modificar horario de backup INCREMENTAL (diario)
manager.set_schedule('INCREMENTAL', hour=23, minute=0)
```

## Solución de Problemas

### Scheduler no se inicia

1. Verificar que APScheduler esté instalado:
```bash
pip install APScheduler
```

2. Verificar logs:
```bash
tail -f backup_scheduler.log
```

### Backup falla con "pg_dump not found"

1. Instalar PostgreSQL con herramientas de línea de comandos
2. Configurar `PG_BIN_PATH` en variables de entorno
3. Agregar `bin` de PostgreSQL al PATH del sistema

### Error de permisos

```bash
# En Linux/Unix
chmod +x scripts/backup_scheduler.py

# En Windows, ejecutar como administrador
```

### Backups no se suben a la nube

1. Verificar configuración de nube
2. Verificar credenciales (Google Drive/S3)
3. Verificar conectividad de red
4. Revisar logs del scheduler

## Rendimiento

### Optimizaciones Implementadas

- **Backups Full**: Usan formato comprimido de pg_dump (-F c)
- **Backups Incrementales**: Solo cambios desde último backup
- **Paralelización**: Varios jobs pueden ejecutarse en paralelo
- **Hot Backups**: Mínimos locks en la base de datos
- **Validación**: Checksums calculados durante backup

### Métricas Esperadas

- **Backup FULL**: ~5-10 minutos (dependiendo del tamaño)
- **Backup DIFERENCIAL**: ~2-5 minutos
- **Backup INCREMENTAL**: ~30 segundos - 2 minutos
- **Restauración**: Variable según cantidad de backups en cadena

## Comparación con Sistema Antiguo

| Característica | Sistema Antiguo | Sistema Nuevo |
|--------------|------------------|---------------|
| Tipos de backup | Diario, Semanal, Mensual | FULL, DIFERENCIAL, INCREMENTAL |
| Concatenación | No | Sí (FULL + DIF + INC) |
| Validaciones | No | SHA-256 checksum |
| Tracking | Parcial | Completo con metadata |
| Nube | Carpeta local | S3, Google Drive, Local |
| Retención | Configurable | Configurable por política |
| Restauración | Último backup | Cualquier fecha |

## Compatibilidad

### Sistema Antiguo NO Modificado

El sistema de backups existente (en `backups/`) **NO es modificado**. Puedes seguir usándolo normalmente.

### Coexistencia

Ambos sistemas pueden coexistir simultáneamente:
- `backups/` - Sistema antiguo (diario/semanal/mensual/manual)
- `backups_incrementales/` - Sistema nuevo (full/differential/incremental)

## Seguridad

### Checksums SHA-256

Todos los backups tienen un checksum SHA-256 calculado automáticamente:
- Garantiza integridad del archivo
- Permite detectar corrupción
- Requerido para restauración

### Validaciones

- Verificación de existencia de archivo
- Verificación de checksum
- Validación de cadena de backup
- Logs de todos los eventos

### Logs

El sistema mantiene logs detallados en:
- `backup_scheduler.log` - Logs del scheduler
- Logs de aplicación (`seguridad.backup_event` en DB)

## Soporte

### Logs

```bash
# Ver logs en tiempo real
tail -f backup_scheduler.log

# Ver logs de errores
grep ERROR backup_scheduler.log

# Ver últimos 50 eventos en DB
psql -U postgres -d nexoryn_tech -c "
SELECT * FROM seguridad.backup_event 
ORDER BY fecha_hora DESC 
LIMIT 50;
"
```

### Consultas de Diagnóstico

```sql
-- Backups fallidos recientes
SELECT * FROM seguridad.backup_manifest
WHERE estado = 'FALLIDO'
ORDER BY fecha_inicio DESC
LIMIT 10;

-- Backups sin validar recientes
SELECT * FROM seguridad.backup_manifest
WHERE estado = 'COMPLETADO'
  AND id NOT IN (SELECT DISTINCT backup_id FROM seguridad.backup_validation)
ORDER BY fecha_inicio DESC
LIMIT 10;

-- Cadenas rotas (backups sin base)
SELECT bm.* 
FROM seguridad.backup_manifest bm
LEFT JOIN seguridad.backup_manifest bm_base ON bm.backup_base_id = bm_base.id
WHERE bm.backup_base_id IS NOT NULL AND bm_base.id IS NULL;
```

## Actualización

### Actualizar desde versión anterior

1. Hacer backup FULL del sistema actual
2. Ejecutar migración de DB
3. Iniciar scheduler nuevo
4. Verificar que ambos sistemas funcionan
5. Probar restauración

## Resumen de Comandos

| Comando | Descripción |
|---------|-------------|
| `python scripts/backup_scheduler.py start` | Iniciar scheduler |
| `python scripts/backup_scheduler.py full` | Backup FULL inmediato |
| `python scripts/backup_scheduler.py diferencial` | Backup DIFERENCIAL inmediato |
| `python scripts/backup_scheduler.py incremental` | Backup INCREMENTAL inmediato |
| `python scripts/backup_scheduler.py status` | Mostrar estado |
| `python scripts/backup_scheduler.py validate` | Validar backups |
| `python scripts/backup_scheduler.py cleanup` | Limpiar backups antiguos |

---

**Nota**: Este sistema es completamente compatible con el sistema de backups existente. No requiere cambios en el flujo de trabajo actual y ambos pueden funcionar simultáneamente.
