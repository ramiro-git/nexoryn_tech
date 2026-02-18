---
description: aplicar cambios en base de datos de forma **segura, reversible y medible**, minimizando riesgos (locks, downtime, pérdida de datos).
---

---

## 1) Intake (inputs mínimos obligatorios)
Pedir/confirmar:

1) Motor + versión:
- PostgreSQL / SQL Server / MySQL + versión exacta

2) Entorno:
- prod/staging/dev
- tamaño aproximado (filas por tabla clave)
- ventana de mantenimiento disponible (si existe)
- tolerancia a downtime (0, bajo, medio)

3) Cambio requerido (exacto):
- qué objeto: tabla/columna/index/constraint/view/proc
- DDL deseado o descripción precisa
- motivación (bug, feature, performance, compliance)

4) Dependencias:
- qué servicios/ETLs/reportes/queries lo usan
- ORM/migrator (Alembic/Flyway/Liquibase/Prisma/etc.)
- versionado de schema (si hay)

> Si falta info crítica: no ejecutar diseño final; proponer plan y pedir lo mínimo.

---

## 2) Clasificación de riesgo (obligatorio)
Etiquetar el cambio:

- **LOW**: índice nuevo en tabla chica, columna nullable sin backfill, view nueva, etc.
- **MEDIUM**: índice en tabla grande, constraint sin validar previamente, cambios con backfill pequeño.
- **HIGH**: cambio de tipo de columna, rename de columnas usadas, drop de columnas, NOT NULL sin backfill, PK/FK en tablas grandes, cambios que bloquean escrituras, reescrituras masivas.

Indicar por qué cae en esa categoría.

---

## 3) Principios obligatorios (guardrails)
- No cambios irreversibles sin **rollback** o **plan de reversión**.
- No “big-bang” si el cambio toca producción y hay alternativa gradual.
- Evitar locks largos y reescrituras masivas en horario pico.
- Medir antes/después (latencia, CPU/IO, locks, plan, errores app).

---

## 4) Estrategias seguras por tipo de cambio (playbook)

### 4.1 Agregar columna (con o sin backfill)
- Preferir `NULL` inicialmente.
- Si requiere valor por defecto:
  - evaluar impacto (algunos motores reescriben tabla / toman locks)
  - alternativa: agregar nullable → backfill por batches → set default → set NOT NULL (si aplica)

**Backfill (batch)**
- definir batch size y criterio (PK range o timestamps)
- idempotencia: que pueda re-ejecutarse sin romper
- pausas/ratelimit si hay carga

### 4.2 Cambiar tipo de columna
- Preferir estrategia de “expand/contract”:
  1) agregar columna nueva (nuevo tipo)
  2) dual-write desde app (si aplica) o triggers temporales (si se acepta)
  3) backfill batch
  4) switch reads
  5) deprecate columna vieja
  6) drop (en release posterior)

### 4.3 Renombrar columna / tabla
- Evitar si no hay control total de consumidores.
- Preferir alias/compat:
  - view/synonym (si motor lo permite)
  - columna nueva + app dual-read/dual-write
- Planificar deprecación.

### 4.4 Agregar índice
- Validar:
  - patrón real de queries (WHERE/JOIN/ORDER)
  - costo en writes
- Si la tabla es grande:
  - usar método “online/concurrent” si existe (según motor) o planificar ventana
- Verificar que no sea redundante (prefijos/dobles).

### 4.5 Agregar constraint (FK/UNIQUE/CHECK/NOT NULL)
- Pre-chequeo:
  - query para detectar violaciones (antes de aplicar)
- Preferir aplicar de forma que reduzca lock/impacto:
  - si el motor soporta “validate later”, separar creación vs validación
- En NOT NULL:
  - backfill antes
  - luego enforce

### 4.6 Drop de columna/tabla
- No borrar “directo” si hay riesgo:
  - primero: dejar de usar (feature flag / deploy app)
  - luego: archival/backup
  - finalmente: drop en release posterior

---

## 5) Plan de ejecución (obligatorio)
Entregar un plan con:

1) **Pre-checks**
- backups / snapshot si aplica
- estado replicación (si existe)
- locks actuales (si se sabe medir)
- espacio en disco (sobre todo para índices)

2) **Pasos exactos**
- orden de DDL y scripts
- si hay backfill: pseudo-código y batch strategy

3) **Rollback**
- DDL para revertir (DROP/RENAME revert)
- en cambios no reversibles: plan alternativo (restore, dual schema, feature flag)

4) **Validación**
- checks funcionales (app)
- checks DB:
  - planes antes/después (EXPLAIN/Execution Plan)
  - métricas (tiempo, locks, CPU/IO)
- smoke tests

5) **Monitoreo post-deploy**
- qué métricas mirar
- umbrales y acción (rollback / pause backfill)

---

## 6) Formato de salida (obligatorio)
- Riesgo: LOW/MEDIUM/HIGH + justificación
- Qué se cambia (exacto)
- Qué NO se toca
- Script/DDL propuesto (si corresponde)
- Plan paso a paso
- Rollback / reversión
- Validación + monitoreo

---

## 7) Reglas de comunicación
- Si detectás que el request es peligroso (“drop prod table”, “change type en caliente”), decirlo directo y proponer alternativa gradual.
- No asumir disponibilidad de “online DDL”: confirmar por motor/versión.