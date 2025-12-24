# Integración AFIP/ARCA - Facturación Electrónica

Guía para implementar facturación electrónica con ARCA (ex-AFIP) en Nexoryn Tech.

## Campos Disponibles en la BD

La tabla `app.documento` ya tiene los campos necesarios:

```sql
punto_venta             INTEGER       -- Ej: 1, 2, 3...
tipo_comprobante_afip   INTEGER       -- Código AFIP del comprobante
cae                     VARCHAR(14)   -- Código de Autorización Electrónico
cae_vencimiento         DATE          -- Fecha límite de validez del CAE
cuit_emisor             VARCHAR(11)   -- CUIT de quien emite
qr_data                 TEXT          -- Datos para generar el QR (obligatorio)
```

## Códigos de Comprobante AFIP

| Tipo | Código |
|------|--------|
| Factura A | 1 |
| Nota Débito A | 2 |
| Nota Crédito A | 3 |
| Factura B | 6 |
| Nota Débito B | 7 |
| Nota Crédito B | 8 |
| Factura C | 11 |
| Nota Crédito C | 13 |

## Configuración Requerida

Agregar en `seguridad.config_sistema`:

| Clave | Descripción |
|-------|-------------|
| `cuit_empresa` | CUIT del emisor (ya existe) |
| `punto_venta_default` | Punto de venta habilitado (ej: 1) |
| `afip_certificado_path` | Ruta al archivo .crt |
| `afip_clave_path` | Ruta al archivo .key |
| `afip_modo` | `homologacion` o `produccion` |

## Pasos para Habilitar

### 1. Obtener Certificado Digital

1. Ir a [AFIP con clave fiscal](https://auth.afip.gob.ar/)
2. Administración de Certificados Digitales
3. Generar CSR y descargar certificado

### 2. Instalar Dependencia

```bash
pip install pyafipws
```

### 3. Flujo de Facturación

```python
from pyafipws.wsfev1 import WSFEv1
from pyafipws.wsaa import WSAA

# 1. Autenticación
wsaa = WSAA()
wsaa.Conectar()
wsaa.Autenticar("wsfe", "certificado.crt", "clave.key")

# 2. Conexión al webservice
wsfev1 = WSFEv1()
wsfev1.Cuit = "20123456789"
wsfev1.Token = wsaa.Token
wsfev1.Sign = wsaa.Sign
wsfev1.Conectar()

# 3. Obtener último comprobante
ultimo = wsfev1.CompUltimoAutorizado(tipo_cbte=6, punto_vta=1)

# 4. Crear factura
wsfev1.CrearFactura(
    concepto=1,  # Productos
    tipo_doc=80, # CUIT
    nro_doc="20123456789",
    tipo_cbte=6, # Factura B
    punto_vta=1,
    nro_cbte=ultimo + 1,
    imp_total=1210.00,
    imp_neto=1000.00,
    imp_iva=210.00,
    fecha_cbte="20241221",
)

# 5. Obtener CAE
wsfev1.AgregarIva(id=5, base_imp=1000, importe=210)  # IVA 21%
wsfev1.CAESolicitar()

cae = wsfev1.CAE
vencimiento = wsfev1.Vencimiento
```

### 4. Generar QR (Obligatorio desde 2021)

```python
import json
import base64

qr_data = {
    "ver": 1,
    "fecha": "2024-12-21",
    "cuit": 20123456789,
    "ptoVta": 1,
    "tipoCmp": 6,
    "nroCmp": 12345,
    "importe": 1210.00,
    "moneda": "PES",
    "ctz": 1,
    "tipoCodAut": "E",
    "codAut": int(cae)
}
qr_base64 = base64.b64encode(json.dumps(qr_data).encode()).decode()
qr_url = f"https://www.afip.gob.ar/fe/qr/?p={qr_base64}"
```

## Recursos

- [Documentación AFIP WSFEv1](https://www.afip.gob.ar/fe/documentos/manual_desarrollador.pdf)
- [PyAfipWs GitHub](https://github.com/reingart/pyafipws)
- [Entorno de Homologación](https://wswhomo.afip.gov.ar/)
