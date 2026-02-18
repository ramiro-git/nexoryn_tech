# Guía Detallada: Trámites en el Portal de AFIP/ARCA

Esta guía describe paso a paso qué botones tocar y qué opciones elegir dentro del portal de AFIP/ARCA. No es necesario saber programar para esta parte.

> Nota: los nombres de botones pueden variar con el tiempo. Si algún texto no coincide, buscá la opción con el mismo nombre funcional (ej: “Administrador de Relaciones”, “Adherir Servicio”, “Web Services”).

---

## 1. Habilitar los Servicios Necesarios
Si estos servicios no te aparecen en tu pantalla principal de AFIP, debes "agregarlos" así:

1.  Entra a [afip.gob.ar](https://www.afip.gob.ar) con tu **CUIT** y **Clave Fiscal (Nivel 3)**.
2.  Busca **"Administrador de Relaciones de Clave Fiscal"** (a veces figura como **"Administrador de Relaciones"**).
3.  Haz clic en **"Adherir Servicio"**.
4.  Haz clic sobre el logo de **"AFIP"** y luego en **"Servicios Interactivos"**.
5.  Busca en la lista (está por orden alfabético) y haz clic en:
    - **"Administración de Certificados Digitales"** (Obligatorio para subir tu llave).
    - **"Gestión de Puntos de Venta y Comprobantes"** (Obligatorio para crear el punto de venta de Nexoryn).
6.  Haz clic en **"Confirmar"**.
7.  **Importante**: Cierra la sesión y vuelve a entrar para que aparezcan en el menú principal.

---

## 2. Crear el Punto de Venta para el Sistema
El sistema no puede usar el mismo punto de venta que usas para "Facturador en Línea" o la App del celular.

1.  Entra al servicio **"Gestión de Puntos de Venta y Comprobantes"**.
2.  Selecciona tu nombre/empresa.
3.  Haz clic en **"A/B/M de Puntos de Venta"** (puede figurar como **"Administrar Puntos de Venta"**).
4.  Haz clic en **"Agregar"**.
5.  Completa los campos:
    - **Número**: El que siga (ejemplo: `0002` o `0005`). Anótalo.
    - **Nombre Fantasía**: "Nexoryn Tech" o el nombre de tu negocio.
    - **Sistema**: Selecciona **"RECE para aplicativo y web services"** (a veces figura como “Web Services” o “FactuWS”). **Este es el paso más importante**.
    - **Domicilio**: Selecciona tu dirección fiscal.
6.  Haz clic en **"Aceptar"** y luego **"Confirmar"**.

---

## 3. Cargar el Certificado Digital (Alias)
Primero debes haber generado el archivo `.csr`. Una vez que lo tengas:

1.  Entra al servicio **"Administración de Certificados Digitales"**.
2.  Haz clic en **"Agregar Alias"** o **"Agregar Certificado"**.
3.  En **Alias**, ponle un nombre fácil, ej: `Nexoryn_Factura`.
4.  Donde dice **"Archivo"**, haz clic en "Examinar" y sube el archivo `.csr`.
5.  Haz clic en **"Agregar"** / **"Confirmar"**.
6.  En la siguiente pantalla, verás un link **"Ver"** o una flecha de descarga. Descarga el certificado oficial (archivo **.crt**).

> Tip: los certificados vencen (normalmente 1 año). Guardá la fecha de vencimiento.

---

## 4. El "Paso Final" en el Administrador de Relaciones
Ahora tienes que decirle a AFIP: *"Este certificado que subí tiene permiso para hacer Facturas"*.

1.  Vuelve al **"Administrador de Relaciones de Clave Fiscal"**.
2.  Haz clic en **"Nueva Relación"**.
3.  Haz clic en **"Buscar"**.
4.  Haz clic en el logo de **"AFIP"** y luego en **"WebServices"**.
5.  Busca **"Facturación Electrónica"** (WSFEv1) y selecciónalo.
6.  En la parte de **"Representante"**, haz clic en **"Buscar"**.
7.  **¡OJO AQUÍ!**: No pongas tu CUIT. En el desplegable selecciona el **Alias** que creaste en el paso 3 (ej: `Nexoryn_Factura`).
8.  Haz clic en **"Confirmar"**.

---

## Resumen de lo que debes guardar:
Al terminar esto, deberías tener:
1.  El número del **Punto de Venta** nuevo (para AFIP).
2.  Tu clave privada (archivo **.key**).
3.  Tu certificado firmado (archivo **.crt**).

Con estos 3 datos, el sistema ya puede hablar legalmente con AFIP.
