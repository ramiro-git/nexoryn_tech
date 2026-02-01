# Plan Maestro: Integración Facturación Electrónica ARCA (AFIP)

Este documento detalla el plan integral paso a paso para habilitar y automatizar la facturación electrónica en **Nexoryn Tech**.

---

## Fase 1: Trámites Administrativos (Portal AFIP)

Antes de tocar una línea de código, se debe configurar el entorno en el portal de AFIP (ARCA) con Clave Fiscal Nivel 3.

### 1.1. Habilitación de Servicios
Es necesario habilitar los siguientes servicios en "Administrador de Relaciones de Clave Fiscal":
1.  **Administración de Certificados Digitales**: Para subir el archivo `.csr` y obtener el `.crt`.
2.  **Gestión de Puntos de Venta y Comprobantes**: Para crear un punto de venta tipo "FactuWS" (Web Services).

### 1.2. Crear el Punto de Venta
1.  Ingresar a "Gestión de Puntos de Venta y Comprobantes".
2.  Agregar nuevo Punto de Venta.
3.  **Importante**: El sistema debe ser **"RECE para aplicativo y web services"**.
4.  Anotar el número (Ej: 0002).

### 1.3. Generación del Certificado (Homologación/Producción)
1.  **Generar Clave Privada (`.key`)**: Se hace localmente (Nexoryn lo hará con OpenSSL).
2.  **Generar Pedido de Certificado (`.csr`)**: Documento que vincula el CUIT con la clave.
3.  **Obtener Certificado (`.crt`)**: Subir el `.csr` a AFIP y descargar el certificado firmado.

---

## Fase 2: Configuración del Sistema (Nexoryn Tech)

### 2.1. Gestión de Credenciales
Los certificados NO deben subirse al repositorio. Se guardarán en una carpeta segura (ej: `C:\Nexoryn\Certs\`).

### 2.2. Variables de Entorno (`.env`)
```powershell
AFIP_CUIT="20XXXXXXXX9"
AFIP_CERT_PATH="C:/Nexoryn/Certs/empresa.crt"
AFIP_KEY_PATH="C:/Nexoryn/Certs/empresa.key"
AFIP_PUNTO_VENTA=3
AFIP_MODO="homologacion" # o "produccion"
```

---

## Fase 3: Implementación Técnica (Backend)

### 3.1. Servicio de AFIP (`AfipService`)
Crearemos una clase robusta para manejar:
- **WSAA**: Obtención del Token de Acceso (válido por 12 horas).
- **WSFE**: Envío de factura y recepción de CAE.
- **Validación de CUIT**: Consulta de datos del cliente ante AFIP.

### 3.2. Estructura de Datos (SQL)
La tabla `app.documento` ya cuenta con los campos clave. Aseguraremos que se guarden:
- `cae`: El número devuelto por AFIP.
- `cae_vencimiento`: Fecha de caducidad.
- `qr_data`: El JSON base64 para el QR legal.

---

## Fase 4: Integración UI (Flet)

### 4.1. Flujo del Comprobante
1.  **Estado "Borrador"**: Se carga el comprobante localmente.
2.  **Botón "Autorizar en AFIP"**: Solo visible si tiene tipo de comprobante AFIP.
3.  **Loading State**: Spinner mientras se comunica con los servidores de ARCA.
4.  **Estado "Autorizado"**: Se bloquea la edición y se muestra el CAE.

### 4.2. Generación de PDF
Actualizar el motor de reportes para:
- Incluir el **Código de Barras** o **QR** obligatorio.
- Mostrar la leyenda "Comprobante Autorizado".
- Formato según Resoluciones Generales de AFIP.

---

## Fase 5: Pruebas y Salida a Producción

### 5.1. Homologación
- Usar el CUIT de prueba de AFIP.
- Verificar que el Token se renueve correctamente.
- Validar tipos de IVA (0%, 10.5%, 21%, 27%).

### 5.2. Paso a Producción
- Cambiar rutas de certificados por los reales.
- Cambiar `AFIP_MODO=produccion`.
- Realizar la primera factura real (generalmente de $1) para verificar impacto.

---

> [!TIP]
> **Recomendación**: Empezar con Factura C (Monotributo) si el cliente es pequeño, o Factura B (Responsable Inscripto a Consumidor Final) por ser las más comunes.
