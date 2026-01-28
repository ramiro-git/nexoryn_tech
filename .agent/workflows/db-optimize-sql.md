---
description: mejorar performance de una consulta/caso real SIN adivinar. Requiere evidencia (planes/medición).
---

---

## 1) Intake (pedir lo mínimo indispensable)
Solicitar/confirmar:

1) Motor + versión:
- PostgreSQL / SQL Server / MySQL (y versión exacta)

2) Caso real:
- Query exacta (con placeholders/params)
- Ejemplo de parámetros reales (sin PII)
- Frecuencia (QPS o “cada cuánto se ejecuta”)
- SLA esperado (ej. p95 < 200ms)

3) Datos e índices:
- Tablas involucradas (filas aproximadas)
- Índices existentes (y columnas incluidas)
- Distribución/cardinalidad aproximada de columnas filtradas

4) Evidencia (OBLIGATORIO):
- Postgres: EXPLAIN (ANALYZE, BUFFERS)
- SQL Server: Actual Execution Plan + (si existe) Query Store
- MySQL: EXPLAIN / EXPLAIN ANALYZE (según versión)

> Si falta algo, frenar y pedirlo. No optimizar por intuición.

---

## 2) Lectura del plan (qué buscar)
Identificar en el plan:

- Scan dominante: Seq Scan / Index Scan / Index Only / Bitmap / Table Scan
- Operadores caros: Sort / Hash Aggregate / Nested Loop con high rows / Key Lookup
- Join order y join type
- Estimaciones vs reales (mis-estimates)
- I/O: buffers, reads, spills, temp, memory grants (según motor)

---

## 3) Orden de intervención (no saltarse pasos)
Proponer cambios en este orden:

1) **Query rewrite**
- Sargabilidad: evitar funciones sobre columnas filtradas cuando sea posible
- Predicados: mover filtros al lugar correcto; evitar OR innecesarios
- Joins: evitar join explosivo; validar cardinalidades
- Paginación/orden: revisar ORDER BY + LIMIT/OFFSET y alternativas

2) **Índices**
- Diseñar índice alineado a predicados/join/order-by
- Evitar índices “por las dudas”
- Explicar costo en writes + mantenimiento

3) **Estadísticas / mantenimiento**
- Analizar/actualizar estadísticas si el motor lo requiere
- Reindex/Vacuum/Autoanalyze (según motor y alcance)

4) **Configuración**
- Solo si se pide o si es indispensable (y con riesgo explícito)

---

## 4) Principios obligatorios
- Evitar cambios de performance sin evidencia (planes/medición).
- No agregar índices “por las dudas”:
  - justificar por predicados/join/order-by y por frecuencia de consulta
  - considerar costo en writes y mantenimiento
- Respetar seguridad:
  - evitar exponer PII en logs
  - parametrizar queries (no string concatenation)
- Migraciones:
  - ser idempotentes cuando sea posible
  - incluir down/rollback o plan alternativo
- Evitar hints salvo caso documentado y medido (y declarar riesgo de regresión).

---

## 5) Fuentes a consultar (mínimo)
- Use The Index, Luke! — https://use-the-index-luke.com/
- PostgreSQL Wiki — https://wiki.postgresql.org/
- Microsoft Learn SQL Server — https://learn.microsoft.com/sql/
- MySQL Reference Manual — https://dev.mysql.com/doc/

Si hay conflicto, priorizar docs oficiales y explicar tradeoffs.

---

## 6) Formato de salida (obligatorio)
Entregar:

1) Diagnóstico (qué está lento y por qué) + evidencia del plan
2) Propuesta A (mínima) con impacto esperado
3) Propuesta B (más fuerte) con tradeoffs
4) Qué NO se toca
5) Riesgos + rollback
6) Validación:
   - cómo medir antes/después
   - comandos/pasos reproducibles