# Guía Detallada: Trámites en el Portal de AFIP (ARCA)

Esta guía describe paso a paso qué botones tocar y qué opciones elegir dentro del portal de AFIP. No es necesario saber programar para esta parte.

---

## 1. Habilitar los Servicios Necesarios
Si estos servicios no te aparecen en tu pantalla principal de AFIP, debes "agregarlos" así:

1.  Entra a [afip.gob.ar](https://www.afip.gob.ar) con tu **CUIT** y **Clave Fiscal (Nivel 3)**.
2.  Busca el botón **"Administrador de Relaciones de Clave Fiscal"**.
3.  Haz clic en el botón **"Adherir Servicio"**.
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
3.  Haz clic en **"A/B/M de Puntos de Venta"**.
4.  Haz clic en **"Agregar"**.
5.  Completa los campos:
    - **Número**: El que siga (ejemplo: `0002` o `0005`). Anótalo.
    - **Nombre Fantasía**: "Nexoryn Tech" o el nombre de tu negocio.
    - **Sistema**: Selecciona **"RECE para aplicativo y web services"**. (Este es el paso más importante).
    - **Domicilio**: Selecciona tu dirección fiscal.
6.  Haz clic en **"Aceptar"** y luego **"Confirmar"**.

---

## 3. Cargar el Certificado Digital (Alias)
Primero debes haber generado el archivo `.csr` (yo te ayudaré con eso después). Una vez que lo tengas:

1.  Entra al servicio **"Administración de Certificados Digitales"**.
2.  Haz clic en **"Agregar Alias"**.
3.  En **Alias**, ponle un nombre fácil, ej: `Nexoryn_Factura`.
4.  Donde dice **"Archivo"**, haz clic en "Examinar" y sube el archivo `.csr`.
5.  Haz clic en **"Agregar Alias"**.
6.  En la siguiente pantalla, verás que aparece un link que dice **"Ver"** o una flecha de descarga. Haz clic ahí para bajar tu certificado oficial (archivo **.crt**).

---

## 4. El "Paso Final" en el Administrador de Relaciones
Ahora tienes que decirle a AFIP: *"Este certificado que subí tiene permiso para hacer Facturas"*.

1.  Vuelve al **"Administrador de Relaciones de Clave Fiscal"**.
2.  Haz clic en **"Nueva Relación"**.
3.  Haz clic en **"Buscar"**.
4.  Haz clic en el logo de **"AFIP"** y luego en **"WebServices"**.
5.  Busca el que dice **"Facturación Electrónica"** y selecciónalo.
6.  En la parte de **"Representante"**, haz clic en **"Buscar"**.
7.  **¡OJO AQUÍ!**: No pongas tu CUIT. Haz clic en el desplegable y selecciona el **Alias** que creaste en el paso 3 (ej: `Nexoryn_Factura`).
8.  Haz clic en **"Confirmar"**.

---

## Resumen de lo que debes guardar:
Al terminar esto, deberías tener:
1.  El número del **Punto de Venta** nuevo.
2.  Tu clave privada (el archivo **.key** que generamos primero).
3.  Tu certificado firmado (el archivo **.crt** que bajaste de AFIP).

Con estos 3 datos, el sistema ya puede hablar legalmente con AFIP.
