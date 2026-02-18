# Integración AFIP / ARCA (Implementada)

Este documento describe la integración técnica actual con AFIP/ARCA para facturación electrónica en Nexoryn Tech.

## Estado Actual

La integración está implementada en `desktop_app/services/afip_service.py` y se usa desde la UI básica.
- WSAA: obtención de Token/Sign
- WSFEv1: autorización de comprobantes y obtención de CAE
- Firma CMS con OpenSSL
- Caché del Token en `logs/afip_ta_wsfe_{homo|prod}.xml`

## Requisitos

- Clave Fiscal nivel 3
- Certificados `.crt` y `.key` válidos
- OpenSSL instalado o incluido junto al ejecutable

## Configuración (.env)

La app soporta homologación y producción. El flag `AFIP_PRODUCCION` (o `AFIP_PRODUCTION`) define qué credenciales usar.

```env
# Flag de entorno (acepta AFIP_PRODUCCION o AFIP_PRODUCTION)
AFIP_PRODUCCION=False

# Credenciales Homologación (default si AFIP_PRODUCCION=False)
# También soporta *_HOMOLOGACION
AFIP_CUIT=20XXXXXXXX9
AFIP_CERT_PATH=certs/empresa_homo.crt
AFIP_KEY_PATH=certs/empresa_homo.key
# Alternativas homologación:
# AFIP_CUIT_HOMOLOGACION=20XXXXXXXX9
# AFIP_CERT_PATH_HOMOLOGACION=certs/empresa_homo.crt
# AFIP_KEY_PATH_HOMOLOGACION=certs/empresa_homo.key

# Credenciales Producción (se usan si AFIP_PRODUCCION=True)
# También soporta *_PRODUCTION
AFIP_CUIT_PRODUCCION=20YYYYYYYY9
AFIP_CERT_PATH_PRODUCCION=certs/empresa_prod.crt
AFIP_KEY_PATH_PRODUCCION=certs/empresa_prod.key
# Alternativas producción:
# AFIP_CUIT_PRODUCTION=20YYYYYYYY9
# AFIP_CERT_PATH_PRODUCTION=certs/empresa_prod.crt
# AFIP_KEY_PATH_PRODUCTION=certs/empresa_prod.key

# Punto de venta
AFIP_PUNTO_VENTA=3
```

**Resolución de credenciales:**
- Homologación: usa `AFIP_CUIT/AFIP_CERT_PATH/AFIP_KEY_PATH` y, si existen, prioriza `*_HOMOLOGACION`.
- Producción: usa `*_PRODUCCION` y, si no existen, toma `*_PRODUCTION`.

**Resolución de rutas:**
- Si las rutas son relativas, se resuelven respecto al directorio de configuración.
- Directorio de configuración: `%APPDATA%\Nexoryn_Tech\` o la carpeta del ejecutable (modo portable).

## Flujo en la UI

- El botón **Autorizar AFIP** se habilita cuando:
  - el documento está en estado `CONFIRMADO` o `PAGADO`
  - tiene `codigo_afip`
  - no tiene `cae`
- Al autorizar, se actualizan:
  - `cae`, `cae_vencimiento`, `punto_venta`, `tipo_comprobante_afip`, `cuit_emisor`, `qr_data`
  - solo se actualizan datos AFIP del comprobante (no crea remitos automáticamente)

> La autorización es irreversible desde la UI. Verifica los datos antes de autorizar.

## Impresión de Facturas (Formato AFIP Clásico)

- Las **facturas** (`FACTURA A/B/C`) se imprimen con layout AFIP clásico:
  - Encabezado con rótulo `ORIGINAL`
  - Datos fiscales de emisor/receptor
  - Tabla de ítems con columnas AFIP (código, unidad, bonificación, alícuota, subtotales)
  - Matriz de importes e IVA por alícuota
  - Bloque fiscal inferior con QR, estado, CAE y vencimiento
- El layout aplica solo a facturas. `PRESUPUESTO` y `REMITO` mantienen su formato actual.
- Si la factura no tiene CAE, se imprime igual con estado **Comprobante no autorizado**.
- En numeración `Pto. Vta. / Comp. Nro` se prioriza:
  1. payload del `qr_data` de AFIP (`ptoVta`, `nroCmp`)
  2. datos locales del documento (`punto_venta`, `numero_serie`)
- La opción de impresión con importes ocultos se mantiene:
  - por defecto se imprimen precios e importes (`Incluir precios e importes` activo)
  - para excepciones se puede desmarcar y ocultar montos
  - montos en `---`
  - datos fiscales no monetarios visibles

## Troubleshooting

### `openssl no encontrado`
- Instalar OpenSSL o Git for Windows
- O copiar `openssl.exe`, `libcrypto-*.dll`, `libssl-*.dll` en `bin/` junto al `.exe`

### `Certificado no encontrado` / `Clave privada no encontrada`
- Revisar rutas en `.env`
- Si son relativas, confirmar que estén junto a la configuración

### `WSAA/WSFE error`
- Verificar que el certificado corresponda al CUIT configurado
- Confirmar `AFIP_PRODUCCION`/`AFIP_PRODUCTION` vs homologación

## Seguridad

- No subir certificados al repositorio.
- Guardar `.key` en una ubicación segura.
- Evitar loguear contenido de certificados o tokens.

## Portal AFIP

Para el alta inicial de servicios y generación de certificados, ver `docs/GUIA_AFIP_PORTAL.md`.
