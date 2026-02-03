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

La app soporta homologación y producción. El flag `AFIP_PRODUCCION` define qué credenciales usar.

```env
# Flag de entorno (acepta AFIP_PRODUCCION o AFIP_PRODUCTION)
AFIP_PRODUCCION=False

# Credenciales Homologación (default si AFIP_PRODUCCION=False)
AFIP_CUIT=20XXXXXXXX9
AFIP_CERT_PATH=certs/empresa_homo.crt
AFIP_KEY_PATH=certs/empresa_homo.key

# Credenciales Producción (se usan si AFIP_PRODUCCION=True)
AFIP_CUIT_PRODUCCION=20YYYYYYYY9
AFIP_CERT_PATH_PRODUCCION=certs/empresa_prod.crt
AFIP_KEY_PATH_PRODUCCION=certs/empresa_prod.key

# Punto de venta
AFIP_PUNTO_VENTA=3
```

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
  - se asegura la creación del remito asociado si aplica

> La autorización es irreversible desde la UI. Verifica los datos antes de autorizar.

## Troubleshooting

### `openssl no encontrado`
- Instalar OpenSSL o Git for Windows
- O copiar `openssl.exe`, `libcrypto-*.dll`, `libssl-*.dll` en `bin/` junto al `.exe`

### `Certificado no encontrado` / `Clave privada no encontrada`
- Revisar rutas en `.env`
- Si son relativas, confirmar que estén junto a la configuración

### `WSAA/WSFE error`
- Verificar que el certificado corresponda al CUIT configurado
- Confirmar `AFIP_PRODUCCION` vs homologación

## Seguridad

- No subir certificados al repositorio.
- Guardar `.key` en una ubicación segura.
- Evitar loguear contenido de certificados o tokens.

## Portal AFIP

Para el alta inicial de servicios y generación de certificados, ver `docs/GUIA_AFIP_PORTAL.md`.
