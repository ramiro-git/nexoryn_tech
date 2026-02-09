# Gestión de Base de Datos - Nexoryn Tech

Este documento describe los scripts de base de datos, el flujo de sincronización automática del esquema y el mantenimiento de logs en el sistema actual.

## Scripts Principales

### 1. `init_db.py` (Inicializador)
Script para crear el esquema y opcionalmente importar CSVs.
- **Acciones**: Crea el esquema desde `database.sql`, importa datos desde `database/csvs/` y puede resetear esquemas.
- **Dependencias**: `pandas`, `psycopg2-binary`.
- **Ejecución**:
  ```bash
  # Inicialización normal (Esquema + Importación)
  python database/init_db.py

  # Reset completo (Borra esquemas existentes y recrea todo)
  python database/init_db.py --reset

  # Solo esquema (Sin importar CSVs)
  python database/init_db.py --skip-csv

  # Usar otra base de datos
  python database/init_db.py --db-name nexoryn_tech

  # Modo simulación (no ejecuta cambios)
  python database/init_db.py --dry-run
  ```

> Nota: el script usa por defecto `--db-name nexoryn_tech` si no se especifica.

### 2. `kill_sessions.py` (Terminador de Sesiones)
Utilidad para cerrar conexiones activas cuando PostgreSQL bloquea operaciones de mantenimiento.
- **Dependencias**: `psycopg`.
- **Ejecución**:
  ```bash
  python database/kill_sessions.py --db-name nexoryn_tech
  ```

### 3. `db_conn.py`
Módulo de utilidad que centraliza la conexión para scripts. Usa `.env` si existe y soporta `DATABASE_URL` o variables `DB_*`.

## Sincronización Automática del Esquema (SchemaSync)

La app ejecuta la verificación de esquema en ambos modos de UI, pero en distinto momento de arranque:
- **UI básica (`desktop_app/ui_basic.py`)**: corre un sync temprano antes de crear `Database`.
- **UI avanzada (`desktop_app/ui_advanced.py`)**: corre dentro del flujo de mantenimiento inicial.

**Cómo funciona:**
- Lee la versión del encabezado de `database/database.sql` (`-- Version: X.X`).
- Consulta `seguridad.config_sistema` (clave `db_version`) mediante `psql`.
- Si la versión no coincide, ejecuta `psql -f database.sql` con `ON_ERROR_STOP=1`.

**Requisitos:**
- `psql` en el `PATH` o definido en `PG_BIN_PATH`.

**Importante:**
- No aplica un diff por hashes. Re-ejecuta el archivo completo.
- El SQL está diseñado para ser idempotente (`CREATE IF NOT EXISTS`, `CREATE OR REPLACE`), pero **cambios destructivos deben manejarse en un flujo separado**.
- El esquema usa `pg_advisory_lock` al inicio del archivo para evitar concurrencia entre instancias.

## Migraciones en Runtime (Database._run_migrations)
Al inicializar `Database`, se aplican migraciones idempotentes en caliente:
- `app.pago.id_documento` se vuelve nullable (para pagos de cuenta corriente).
- `app.movimiento_articulo.stock_resultante` se agrega si falta.
- `app.documento_detalle.descuento_importe` se agrega si falta y se normaliza `NULL -> 0`.
- `app.articulo.codigo` se agrega si falta, se normaliza (`NULLIF(TRIM(codigo), '')`) y se completa con `id::text` cuando falta.
- Se crean índices sobre `app.articulo.codigo`:
  - `idx_articulo_codigo`
  - `idx_articulo_codigo_lower_trgm` (GIN sobre `lower(codigo)`).
- Se refresca la vista `app.v_articulo_detallado` para incluir `codigo` y estructura vigente.
- Se actualiza el trigger `app.fn_sync_stock_resumen` para persistir `stock_resultante`.

## Descuentos en comprobantes
- `app.documento` mantiene el descuento global:
  - `descuento_porcentaje`
  - `descuento_importe`
- `app.documento_detalle` almacena descuento por línea:
  - `descuento_porcentaje`
  - `descuento_importe`
  - `total_linea` persiste el neto de la línea luego del descuento de línea (sin descuento global).
- Regla de cálculo vigente:
  - Primero se aplica descuento por línea.
  - Luego el descuento global en importe se prorratea proporcionalmente sobre el neto de líneas antes de IVA.
  - Modo global actual: **precio con IVA incluido** (`pricing_mode = tax_included`).
  - El total operativo visible en UI no suma IVA adicional.
  - El desglose fiscal (neto/IVA) se calcula internamente sobre la base neta resultante.
- UX actual en comprobantes:
  - El campo de IVA visible por línea inicia en `0,00` (editable).
  - Si el usuario ingresa IVA visible `> 0`, ese valor actúa como override fiscal de la línea.
  - Si el usuario deja IVA visible en `0,00`, la alícuota fiscal interna usa fallback del artículo.
  - En persistencia (`app.documento_detalle.porcentaje_iva`) se guarda siempre la alícuota fiscal real.
  - Para AFIP se arma `Iva` por alícuota real, sin hardcodear 21%.

## RLS y contexto de sesión
El esquema habilita RLS en tablas núcleo:
- `app.documento`
- `app.entidad_comercial`
- `app.movimiento_articulo`

**Regla operativa:** las operaciones de escritura requieren que la sesión tenga `app.user_id` seteado (policies con `WITH CHECK (current_setting('app.user_id', true) IS NOT NULL)`).  
Si no está definido, las escrituras pueden fallar o dejar auditoría sin usuario. Las lecturas no filtran datos en este escenario (policies con `USING (true)`).

**Cómo lo gestiona la app:** en conexiones normales la app setea el contexto con `set_config('app.user_id', ...)` al abrir cada transacción.

### Restore y mantenimiento
Para tareas manuales o herramientas de PostgreSQL (`psql`, `pg_dump`, `pg_restore`) se debe setear `app.user_id` por sesión. Ejemplos:

```bash
PGOPTIONS="-c app.user_id=1" psql -h localhost -U postgres -d nexoryn_tech
PGOPTIONS="-c app.user_id=1" pg_restore -h localhost -U postgres -d nexoryn_tech backup.dump
```

Dentro de una sesión `psql`:
```sql
SET app.user_id = 1;
```

Los servicios de backup/restore toman `DB_MAINTENANCE_USER_ID` y lo convierten en `PGOPTIONS` automáticamente para evitar bloqueos por RLS.

## Logs de Auditoría (particionado y archivado)

### Particionado automático
En el arranque de la UI básica se ejecuta `migrate_to_partitioned_logs(db)` para:
- Convertir `seguridad.log_actividad` en tabla particionada por semana.
- Migrar datos históricos si existía una tabla no particionada.

### Archivado automático
Se inicia un `LogArchiver` en segundo plano:
- **Retención**: `log_retencion_dias` en `seguridad.config_sistema` (default 90).
- **Directorio**: `log_directorio_archivo` (default `logs_archive`).
- **Ruta final**: `<PROJECT_ROOT>/data/<log_directorio_archivo>`.
- Archiva a `.jsonl.gz` y luego elimina los registros antiguos.
- Ejecuta `seguridad.mantener_particiones_log()` si la función existe.

## Actualizaciones en Tiempo Real (UI básica y avanzada)
La UI básica y la UI avanzada usan polling cada 5 segundos sobre `seguridad.log_actividad` mediante `Database.check_recent_activity()` para refrescar la vista activa.

## Variables de Entorno Relevantes
- `DATABASE_URL` o `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.
- `DB_MAINTENANCE_USER_ID` debe ser un `seguridad.usuario.id` existente; se usa para setear `app.user_id` en restores/mantenimiento.
- `PG_BIN_PATH` para localizar `psql`, `pg_dump`, `pg_restore`.
- `DB_POOL_MIN` y `DB_POOL_MAX` para el pool de conexiones.

> Nota: si se requieren cambios riesgosos, ejecutar un flujo de migración controlado con backup previo.

## Formato numérico AR (UI/PDF)
- Se centralizó formato/parseo en `desktop_app/services/number_locale.py`.
- Convención visual:
  - Moneda: `$1.000.000,00`
  - Porcentaje: `12,50%`
  - Decimal genérico: `1.000.000,00`
- Política de inputs:
  - Durante `on_change` no se fuerza autoformato sobre el campo activo.
  - La normalización se aplica en `on_blur`, `on_submit` y en `save`.
- Compatibilidad de entrada:
  - Se aceptan notaciones con coma o punto (`1,5`, `1000.5`, `1.000,50`, `1,000.50`).
- Exclusión explícita:
  - Campos de stock no se tratan como moneda/porcentaje por este criterio.
