# 1. Conceptos Básicos

Nexoryn Tech es un sistema de gestión en tiempo real para operaciones comerciales.

- Cada acción confirmada impacta en stock, cuentas corrientes y reportes.
- El foco operativo diario está en: **Inventario**, **Entidades**, **Comprobantes**, **Remitos**, **Movimientos**, **Caja y Pagos**, **Cuentas Corrientes** y **Lista de Precios**.

La consistencia del sistema depende de registrar correctamente cada operación, en lugar de editar saldos sin trazabilidad.

## Diferencia clave

- **Comprobantes**: registran el hecho comercial (venta, compra, presupuesto, etc.).
- **Movimientos**: registran el impacto físico de stock (entradas, salidas y ajustes).


# 2. Navegación, Filtros y Atajos

La barra lateral agrupa las vistas principales operativas:

- Tablero de Control
- Inventario
- Entidades
- Comprobantes
- Remitos
- Movimientos
- Caja y Pagos
- Cuentas Corrientes
- Lista de Precios
- Actualización Masiva

## Búsqueda y filtros

- Usá la búsqueda global para encontrar registros por texto.
- Combiná filtros avanzados por fecha, estado, montos o categorías para acotar resultados.
- En tablas grandes, aplicá filtros antes de exportar.

## Atajos útiles

- `Rueda del mouse`: desplazamiento vertical.
- `Shift + Rueda`: desplazamiento horizontal en tablas.
- `Doble clic`: edición inline en columnas habilitadas.
- `Enter`: confirma campo o acción enfocada.
- `Tab / Shift + Tab`: navegación por campos en formularios.
- En `AsyncSelect`: `ArrowDown / ArrowUp` navega resultados.
- En `AsyncSelect`: `Enter` selecciona el resultado activo.
- `Shift + Enter`: salto de línea en observaciones.
- `Esc`: cerrar modal o cancelar acción activa.
- En confirmación de artículo duplicado (comprobantes): `Enter` o `Esc` ejecuta **Limpiar línea**.
- En modal de comprobantes: `F9` imprime directo (sin descargar PDF).
- En modal de comprobantes: `F10` confirma comprobante y con segundo `F10` confirma el diálogo.
- En modal de comprobantes: `F11` resetea el formulario a estado inicial de venta rápida.
- En modal de comprobantes: `F12` guarda/crea el comprobante automáticamente.

## Nota de sesión

Actualmente la UI básica no aplica cierre automático por inactividad.  
El cierre de sesión se realiza manualmente desde el botón de logout.


# 3. Tablero de Control

El **Tablero de Control** concentra indicadores del negocio:

- Ventas del período.
- Alertas de stock.
- Actividad operativa reciente.
- Resumen financiero.

## Controles

- Selector de período: Hoy / Semana / Mes / Año.
- Selector de refresco: 30 seg / 1 min / 5 min / 10 min / Desactivado.
- Botón "Actualizar ahora" para forzar recarga.

> Recomendación: trabajar con período "Mes" para seguimiento comercial y pasar a "Hoy" para control operativo de cierre.


# 4. Entidades

La vista **Entidades** centraliza clientes y proveedores.

## Alta y edición

- Campos principales: nombre/apellido o razón social, CUIT/DNI, condición IVA y contacto.
- Se puede editar de forma rápida en tabla o en formulario completo.

## Buenas prácticas

- Mantener CUIT/DNI correcto para evitar rechazos en comprobantes y AFIP.
- Usar estado activo/inactivo en lugar de borrar históricos.
- Completar domicilio cuando el cliente opera con entrega.


# 5. Inventario (Artículos y Stock)

La gestión de stock está integrada en **Inventario** (no hay módulo separado de stock).

## Artículos

- Alta con datos base: nombre, código, marca, rubro, unidad, IVA, costos y precios.
- Campo logístico opcional: **Unid./Bulto** (`unidades_por_bulto`).
  - Si se deja vacío, queda en `NULL`.
  - Si se informa, debe ser entero positivo (`> 0`).
- Soporta edición inline para cambios rápidos.
- Permite activar/desactivar artículos sin perder historial.

## Stock y alertas

- Definir stock mínimo por artículo para alertas de reposición.
- El tablero refleja stock crítico para seguimiento diario.

## Ajustes de stock

No modificar stock sin trazabilidad.

1. Registrar ajustes mediante movimientos de stock.
2. Dejar observación clara del motivo.
3. Validar depósito y cantidad antes de confirmar.


# 6. Comprobantes y Facturación

La vista **Comprobantes** concentra ventas, compras y presupuestos.

## Flujo típico

1. Crear comprobante.
2. Cargar entidad, depósito, ítems, descuentos y observaciones.
3. Guardar borrador o confirmar.
4. Imprimir o continuar con autorización AFIP si aplica.

## Estados operativos

- `BORRADOR`: editable, sin impacto final.
- `CONFIRMADO`: impacta operación (stock/cuenta).
- `PAGADO`: confirmado con cancelación.
- `ANULADO`: deja sin efecto operativo.

## Comportamientos relevantes

- El depósito se inicializa automáticamente con el primero disponible.
- La lista de precios global puede autocompletarse según la entidad (si tiene lista asignada).
- Al confirmar la entidad con `Enter`, el foco pasa al primer ítem y el modal puede desplazarse automáticamente a la sección **Ítems**.
- En el modal de comprobantes, `Tab / Shift + Tab` quedan confinados al modal mientras está abierto.
- En el modal de comprobantes, `Esc` cierra el modal (y cancela acciones/modales auxiliares abiertos).
- En cada nueva línea de ítem, **Cantidad** inicia vacía (carga obligatoria por el operador).
- Si se selecciona un artículo en la última línea, el sistema agrega una nueva línea automáticamente y desplaza la lista al final.
- Si se repite un artículo, se abre una confirmación para fusionar cantidades o limpiar la línea actual.
- En impresión de comprobantes, `Incluir precios e importes` queda activo por defecto; desmarcar sólo en excepciones.
- En comprobantes, la impresión usa la impresora predeterminada de Windows en forma directa.

## Bultos (Logística)

- En cada línea del comprobante se muestra el campo read-only **Bultos**.
- Regla vigente (modo estricto):
  - Se calcula `cantidad / unidades_por_bulto`.
  - Si no da entero exacto, se muestra vacío.
  - Si `unidades_por_bulto` no existe o es inválido, se muestra vacío.
- El valor logístico se guarda por línea como snapshot histórico (`unidades_por_bulto_historico`) al crear/editar el comprobante.
- En reimpresiones, **Presupuesto** y **Remito** usan ese snapshot histórico para la columna **Bultos**.
- **Factura** no muestra columna de bultos.


# 7. Facturación Electrónica (AFIP/ARCA)

La autorización fiscal se realiza desde el botón **Autorizar AFIP** en comprobantes aptos.

## Requisitos previos

- Certificados `.crt` y `.key` vigentes.
- CUIT y punto de venta correctamente configurados.
- OpenSSL accesible.
- Comprobante confirmado y sin CAE.

## Validaciones frecuentes

- Para letra A: CUIT válido del receptor + condición IVA informada.
- Para letra B/C: CUIT o DNI válido según reglas del tipo.

## Resultado esperado

- AFIP devuelve CAE y vencimiento.
- El comprobante guarda datos fiscales y QR.

```text
Si AFIP devuelve error, validar primero conectividad, certificados y entorno (homologación/producción)
antes de reintentar autorización.
```


# 8. Remitos

La vista **Remitos** permite seguir despachos y entregas.

## Uso operativo

- Consultar estado: Pendiente, Despachado, Entregado o Anulado.
- Filtrar por entidad, depósito, documento o rango de fechas.
- Revisar valor declarado y unidades despachadas.

## Recomendación

Actualizar estado del remito en el momento operativo real para mantener trazabilidad logística.

## Validaciones al generar detalle

- Al generar detalle de remito desde un comprobante, cada línea debe tener artículo válido y cantidad entera positiva.
- Cantidades decimales, cero, negativas o no numéricas se rechazan con detalle de la línea para corrección operativa.
- La observación de cada línea se normaliza (sin espacios sobrantes) para evitar registros inconsistentes.


# 9. Movimientos de Stock

La vista **Movimientos** es el historial de entradas, salidas y ajustes.

## Qué muestra cada registro

- Fecha y usuario.
- Artículo y stock resultante.
- Tipo de movimiento.
- Cantidad.
- Depósito.
- Comprobante vinculado (si existe).
- Observación.

## Uso recomendado

- Auditar diferencias físicas vs sistema.
- Verificar impacto de confirmaciones y anulaciones.
- Controlar ajustes manuales y sus motivos.


# 10. Caja y Pagos

La vista **Caja y Pagos** registra cobros/pagos y su referencia operativa.

## Flujo de carga

1. Seleccionar entidad.
2. Seleccionar comprobante pendiente.
3. Elegir forma de pago.
4. Informar monto, fecha y referencia.
5. Confirmar registro.

## Recomendaciones

- Verificar monto antes de guardar.
- Completar referencia para conciliación (cheque, transferencia, etc.).
- Usar observaciones solo para contexto adicional útil.


# 11. Cuentas Corrientes

La vista **Cuentas Corrientes** permite control de saldos por entidad.

## Funcionalidades operativas

- Ver saldo actual por cliente/proveedor.
- Filtrar por tipo y estado de saldo.
- Abrir historial de movimientos contables.
- Registrar pago o ajuste de saldo cuando corresponda.

## Estados habituales

- Deudor.
- A favor.
- Al día.


# 12. Lista de Precios

La vista **Lista de Precios** organiza precios por segmento comercial.

## Operación

- Crear listas con nombre y orden.
- Activar/desactivar listas según estrategia comercial.
- Usar listas activas en comprobantes e inventario.

## Buen criterio

Mantener pocas listas bien definidas evita errores de selección en ventas.


# 13. Actualización Masiva

**Actualización Masiva** es una herramienta de alto impacto.

> Disponible para roles con permisos de gestión (por ejemplo ADMIN/GERENTE en la UI básica).

## Flujo recomendado

1. Filtrar el conjunto de artículos.
2. Elegir objetivo (Costo Base o lista de precio).
3. Definir tipo de ajuste (porcentaje, monto fijo o valor exacto).
4. Generar vista previa.
5. Seleccionar filas y aplicar cambios.

## Precaución operativa

Siempre revisar la vista previa antes de aplicar cambios masivos.


# 14. Troubleshooting Operativo

## La app no inicia o falla conexión a DB

- Verificar `DATABASE_URL` o variables `DB_*`.
- Confirmar que PostgreSQL esté activo.
- Revisar conectividad y firewall si es red LAN.

## Error AFIP / OpenSSL

- Confirmar rutas de certificados.
- Verificar entorno homologación/producción.
- Validar disponibilidad de `openssl.exe`.

## Diferencia entre stock físico y sistema

- No corregir fuera de flujo.
- Registrar movimiento de ajuste con observación.
- Reconciliar depósito y comprobantes vinculados.

## Impresión de comprobantes no sale

- Revisar que exista impresora predeterminada en Windows.
- Confirmar permisos de carpeta temporal.
- Si falla la impresión directa, el sistema abre el PDF para impresión manual.

## Error al generar remito por cantidad inválida

- Revisar línea por línea del comprobante origen.
- Corregir cantidades no enteras o menores/iguales a cero.
- Reintentar la generación del remito luego de guardar los cambios.

## Error al generar ejecutable (.exe)

- No usar `python -m flet` ni `py -m flet`.
- Si `flet` no aparece en `PATH`, usar la ruta completa a `flet.exe`.
- Para el paso a paso actualizado de empaquetado, seguir `docs/GUIA_EMPAQUETADO.md`.


# 15. Resumen de Atajos

- `Enter`: avanzar/confirmar campo.
- `Tab / Shift + Tab`: moverse entre controles.
- En comprobantes, `Tab / Shift + Tab` no sale del modal mientras esté abierto.
- En comprobantes, `F9` imprime directo.
- En comprobantes, `F10` confirma y segundo `F10` confirma el diálogo.
- En comprobantes, `F11` resetea a formulario nuevo.
- En comprobantes, `F12` guarda/crea comprobante.
- En comprobantes, `Esc` cierra el modal.
- En confirmación de artículo duplicado (comprobantes), `Enter` y `Esc` aplican **Limpiar línea**.
- En `AsyncSelect`, `ArrowDown / ArrowUp` mueve el resultado activo.
- En `AsyncSelect`, `Enter` selecciona el resultado activo.
- `Shift + Enter`: salto de línea en observaciones.
- `Esc`: cerrar modal o cancelar.
- `Shift + Rueda`: scroll horizontal en tablas.
- `Doble clic`: edición rápida de celdas habilitadas.

Este manual se mantiene desde `docs/MANUAL_OPERATIVO.md` y se publica en PDF con `scripts/generate_manual_pdf.py` (usar `python` o `python3` según el entorno).
