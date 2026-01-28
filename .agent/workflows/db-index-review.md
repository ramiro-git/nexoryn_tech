---
description: proponer índices correctos y justificables, evitando over-indexing.
---

---

## 1) Inputs requeridos
- Motor + versión
- Query exacta + parámetros típicos
- Columnas usadas en:
  - WHERE
  - JOIN (ON)
  - ORDER BY / GROUP BY
- Índices actuales (definiciones)
- Volumen de tablas y patrón de escrituras (INSERT/UPDATE/DELETE)

---

## 2) Reglas de decisión
- Un índice debe responder a un patrón real (WHERE/JOIN/ORDER BY) + frecuencia.
- Si el workload es write-heavy, ser conservador: cada índice cuesta.
- Evitar duplicados:
  - índices con mismo prefijo
  - índices redundantes por orden de columnas
- Considerar “include/covering” solo cuando aplique al motor y al caso real (y con evidencia).

---

## 3) Checklist de justificación por índice
Para cada índice propuesto, documentar:

- Query objetivo
- Predicado/join/order-by que cubre
- Por qué el índice actual no alcanza
- Riesgo (write amplification, bloat, lock, migración)
- Rollback (DROP INDEX / revert migration)
- Validación (plan antes/después + métricas)

---

## 4) Fuentes a consultar
- Use The Index, Luke! — https://use-the-index-luke.com/
- Docs oficiales del motor (según corresponda)

---

## 5) Formato de salida
- Índices propuestos (DDL)
- Índices a revisar/remover (si corresponde, sin ejecutar sin confirmación)
- Plan de despliegue seguro (migración, ventana, monitoreo)
- Medición antes/después