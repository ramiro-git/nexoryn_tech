# Gestión de Base de Datos - Nexoryn Tech

Este documento describe los scripts de base de datos, el flujo de sincronización automática del esquema y la trazabilidad operativa vigente.

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
- `app.documento_detalle.unidades_por_bulto_historico` se agrega si falta (nullable), se normalizan valores inválidos (`<= 0 -> NULL`) y se aplica `CHECK (> 0 cuando no es NULL)`.
- `app.articulo.codigo` se agrega si falta, se normaliza (`NULLIF(TRIM(codigo), '')`) y se completa con `id::text` cuando falta.
- `app.articulo.unidades_por_bulto` se agrega si falta (nullable), se normalizan valores historicos invalidos (`<= 0 -> NULL`) y se aplica `CHECK (> 0 cuando no es NULL)`.
- Se crean índices sobre `app.articulo.codigo`:
  - `idx_articulo_codigo`
  - `idx_articulo_codigo_lower_trgm` (GIN sobre `lower(codigo)`).
- Se refresca la vista `app.v_articulo_detallado` para incluir `codigo`, `unidades_por_bulto` y estructura vigente.
- Se actualiza el trigger `app.fn_sync_stock_resumen` para persistir `stock_resultante`.

Compatibilidad:
- `unidades_por_bulto` queda en `NULL` por defecto para articulos existentes y nuevos sin dato cargado, sin romper historicos.
- En comprobantes, `unidades_por_bulto_historico` se guarda por línea al crear/editar para mantener consistencia en reimpresiones futuras.
- El snapshot histórico cubre la lógica de bultos; otros metadatos de presentación (por ejemplo nombre/código/unidad del artículo) pueden seguir resolviéndose desde maestros vigentes según el flujo de impresión.

## Validación de detalle de remitos desde comprobantes
- `Database._insert_remito_detalle_from_document` acepta líneas legacy (tupla/lista) y líneas en formato dict (`id_articulo`, `cantidad`, `observacion`).
- El artículo se normaliza a ID entero válido por línea; si no se puede resolver, la operación falla con error explícito de línea.
- La cantidad se normaliza con `Decimal` y debe cumplir:
  - número válido,
  - entero exacto (sin fracción),
  - mayor a `0`.
- `observacion` se trimmea y se persiste como `NULL` cuando queda vacía.
- Objetivo: evitar registros inválidos en `app.remito_detalle` y mejorar trazabilidad de errores operativos.

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
  - El campo de IVA visible por línea inicia vacío (editable).
  - Si el usuario ingresa IVA visible `> 0`, ese valor actúa como override fiscal de la línea.
  - Si el usuario deja IVA visible vacío (o en `0`), la alícuota fiscal interna usa fallback del artículo.
  - Los campos de descuento por línea (`descuento_porcentaje` y `descuento_importe`) pueden verse vacíos en UI y se interpretan internamente como `0`.
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

## Logs y Trazabilidad (TXT diario)

La auditoría dejó de persistirse en tablas de DB y ahora se guarda en archivos diarios:
- Carpeta: `<PROJECT_ROOT>/logs/`
- Archivo por día: `activity_YYYY-MM-DD.txt`
- Formato: CSV delimitado por `,` con cabecera fija (`id`, `fecha_hora`, `id_usuario`, `entidad`, `id_entidad`, `accion`, `resultado`, `ip`, `user_agent`, `session_id`, `detalle`)
- `detalle` se serializa como JSON string.

Notas operativas:
- `Database.log_activity`, `Database.log_logout` y `Database._log_login_attempt` escriben solo en archivo.
- El logging es no bloqueante: si falla la escritura no rompe la operación principal.
- No existe política automática de retención/archivado de logs.

## Sesiones activas y refresco UI

- `Database.fetch_active_sessions` y `Database.count_active_sessions` quedaron deshabilitadas (devuelven vacío/cero por compatibilidad).
- `Database.check_recent_activity` usa marcas de actividad en memoria del proceso (sin consultar tablas de logs).
- La UI básica ya no expone el módulo "Logs de Actividad" ni la vista de sesiones activas.

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
