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

---

## Implementación en Red LAN (Ejecutable Compartido)

Si planeas dejar el ejecutable en una carpeta compartida de la red para que varios usuarios lo abran (ej: `\\SERVIDOR\Sistema\NexorynTech.exe`), esta es la mejor estrategia:

### Ubicación Centralizada (Recomendada)
Coloca el archivo `.env` y la carpeta `certs/` **en la misma carpeta de red** donde está el ejecutable.
- **Ventaja**: "Cero Instalación". Si cambias la contraseña de la base de datos o renuevas un certificado, lo haces una sola vez en el servidor y todos los usuarios se actualizan automáticamente al abrir el programa.
- **Estructura en el Servidor**:
  ```text
  \\SERVIDOR\Nexoryn\
  ├── NexorynTech.exe
  ├── .env
  └── certs/
      ├── mi_empresa.crt
      └── mi_empresa.key
  ```

### Consideraciones de Seguridad
Al estar el `.env` en la red, cualquier usuario con acceso a esa carpeta puede ver los datos de conexión. Si la red es privada y de confianza, esto es lo más práctico.

### Requisito en cada PC
Aunque el programa esté en la red, cada computadora que lo ejecute **debe tener instalado OpenSSL** (o Git para Windows) para que la facturación electrónica funcione, ya que el ejecutable llama a las herramientas criptográficas locales del sistema.

---

## Seguridad de Archivos en Red

Si te preocupa que los usuarios puedan entrar a la carpeta y ver el `.env` o los certificados, tienes estas opciones (de menor a mayor seguridad):

### 1. Atributos de Windows (Básico)
Puedes marcar la carpeta `certs` y el archivo `.env` como **Ocultos** y **de Sistema**.
- Command: `attrib +h +s .env` / `attrib +h +s certs`
- *Efectividad*: Muy baja. Solo oculta los archivos a usuarios casuales.

### 2. Permisos de Carpeta NTFS (Recomendado)
Es la forma profesional. En el servidor:
1. Haz clic derecho en la carpeta `Sistema_Nexoryn` > Propiedades > Seguridad.
2. Quita el acceso de "Lectura" a los usuarios estándar para los archivos específicos `.env` y la carpeta `certs`.
3. **Ojo**: Para que el sistema funcione, el usuario que abre el programa *debe* tener permiso de lectura. Si bloqueas al usuario, bloqueas al sistema (porque el sistema corre "como" el usuario). 
4. *Mejor práctica*: Crear un usuario de red específico para la aplicación (Service Account), pero suele ser complejo para redes hogareñas/pymes.

### 3. Almacenamiento en Base de Datos (Avanzado)
Lo más seguro es no tener archivos físicos en la red.
- **Configuración**: El sistema ya puede leer configuraciones (como el CUIT) desde la tabla `seguridad.config_sistema` en lugar del `.env`.
- **Certificados**: Podrías guardar el contenido de los `.crt` y `.key` dentro de la base de datos (como texto) y que el sistema los descargue a una carpeta temporal solo al momento de facturar.

> [!TIP]
> Si buscas una solución rápida, usa los **Permisos de NTFS** restringiendo quién puede entrar a esa carpeta compartida a nivel de Windows, y usa el atributo **Oculto** para evitar errores accidentales de los usuarios.

---

## Estructura de archivos para Modo Portable (Local)

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
