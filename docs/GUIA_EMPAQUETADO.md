# Guía de Generación de Ejecutable y Configuración - Nexoryn Tech

Esta guía explica cómo generar el archivo ejecutable (`.exe`) de la aplicación y dónde ubicar los archivos de configuración en el entorno de producción.

## 1. Generación del Ejecutable

La aplicación está construida con **Flet**. Para generar el ejecutable, utilizaremos el comando `flet pack`.

### Requisitos previos
Asegúrese de tener instaladas las dependencias necesarias:
```powershell
pip install flet pyinstaller
```

### Comandos de Empaquetado
Para que el ejecutable incluya todos los archivos necesarios (como el esquema de base de datos), use este comando:

```powershell
flet pack desktop_app/main.py --name "NexorynTech" --add-data "database;database"
```

> [!IMPORTANT]
> **Archivos de Base de Datos:**
> Es fundamental incluir `--add-data "database;database"` para que el programa encuentre `database.sql` al iniciar. He corregido el código interno para que detecte correctamente estos archivos dentro del paquete.

> [!TIP]
> **Compatibilidad con AFIP (OpenSSL):**
> He actualizado el código para que la aplicación sea "Bash-Aware". Ahora maneja automáticamente la conversión de rutas y variables de entorno (`MSYS_NO_PATHCONV`) necesarias para que OpenSSL funcione correctamente desde el ejecutable sin necesidad de abrir una consola de Git Bash manualmente.

---

## 2. Ubicación de Archivos (.env, Certificados, etc.)

La aplicación busca la configuración en dos lugares, con el siguiente orden de prioridad:

### Opción A: Instalación Estándar (Recomendado para PC fija)
Ideal para instalaciones donde no se moverá la carpeta de la aplicación.
- **Ruta**: `%APPDATA%\Nexoryn_Tech\`
- **Archivos**:
  - `.env`: Debe estar dentro de esa carpeta.
  - **Certificados**: Cree una carpeta `certs/` dentro de esa misma ruta.

### Opción B: Modo Portable (Recomendado para Pendrives o carpetas locales)
Ideal si quieres copiar la carpeta a cualquier lado y que funcione.
- **Ruta**: En el mismo directorio donde se encuentra el `NexorynTech.exe`.
- **Archivos**:
  - `.env`: Junto al ejecutable.
  - **Certificados**: En una carpeta `certs/` junto al ejecutable.

---

## 3. Ejemplo de archivo `.env`

Para que la aplicación funcione correctamente, el archivo `.env` debe configurarse así (ejemplo para modo portable):

```env
# Conexión a Base de Datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tu_base_de_datos
DB_USER=tu_usuario
DB_PASSWORD=tu_password

# Configuración AFIP (Certificados)
# Si están en la carpeta 'certs' junto al ejecutable:
AFIP_CERT_PATH=certs/mi_certificado.crt
AFIP_KEY_PATH=certs/mi_llave.key
AFIP_CUIT=20XXXXXXXXX
AFIP_PRODUCCION=False
```

---

## ¿Es necesario instalar Git?

No es estrictamente obligatorio instalar Git en la computadora del cliente, pero **OpenSSL sí lo es** para la facturación de AFIP. Tienes dos opciones:

1.  **Opción A (Recomendada): Instalar Git para Windows.**
    - Es la forma más fácil. Al instalarlo, `openssl` queda disponible automáticamente en el sistema.
2.  **Opción B (Sin Git): OpenSSL Independiente.**
    - Puedes descargar el instalador de OpenSSL (Win64 OpenSSL v3.x Light) desde [slproweb.com](https://slproweb.com/products/Win32OpenSSL.html).
    - **Modo Pro**: Si quieres que el ejecutable sea 100% independiente, copia los archivos `openssl.exe`, `libcrypto-*.dll` y `libssl-*.dll` de una instalación de OpenSSL a una carpeta llamada `bin` junto a tu `NexorynTech.exe`. El programa los buscará allí primero.

---

## Estructura de archivos para Modo Portable

Si eliges el **Modo Portable**, así debe verse tu carpeta de trabajo:

```text
Carpeta_App/
│
├── NexorynTech.exe    (El generado en 'dist/')
├── .env               (Tu configuración)
└── certs/             (Carpeta con certificados AFIP)
    ├── mi_certificado.crt
    └── mi_llave.key
```

> [!TIP]
> Si ejecutas la aplicación y falta algo, el programa te dará un error indicándote las rutas exactas donde está buscando los archivos.
