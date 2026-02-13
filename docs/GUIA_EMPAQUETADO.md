# Guía de Generación de Ejecutable y Configuración - Nexoryn Tech

Esta guía explica cómo generar el ejecutable (`.exe`) y cómo ubicar la configuración en producción.

## 1. Generación del Ejecutable

La aplicación está construida con **Flet**. Para generar el ejecutable, utilizamos `flet pack`.

### Requisitos previos
```powershell
python -m pip install -r requirements.txt
```

Si tu entorno no expone `python`, usa:

```powershell
python3 -m pip install -r requirements.txt
```

En Windows, recomendado:

```powershell
py -3.12 -m pip install -r requirements.txt
```

### Comandos de Empaquetado
Caso 1: si `flet` está en `PATH`:

```powershell
flet pack desktop_app/main.py --name "NexorynTech" --add-data "database;database"
```

```powershell
flet pack desktop_app/main.py --name "NexorynTech" --icon "exe_nexoryn_tech.png" --add-data "database;database"
```

Caso 2 (Windows): si aparece `flet: The term 'flet' is not recognized...`, usar el ejecutable por ruta:

```powershell
$fletScripts = "$env:LOCALAPPDATA\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\Scripts"
& "$fletScripts\flet.exe" --version
& "$fletScripts\flet.exe" pack desktop_app/main.py --name "NexorynTech" --icon "exe_nexoryn_tech.png" --add-data "database;database"
```

Caso 3 (Windows, sesión actual): agregar `Scripts` al `PATH` temporal y usar `flet` normal:

```powershell
$fletScripts = "$env:LOCALAPPDATA\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\Scripts"
$env:Path += ";$fletScripts"
flet --version
flet pack desktop_app/main.py --name "NexorynTech" --icon "exe_nexoryn_tech.png" --add-data "database;database"
```

Si `flet.exe` no existe en esa carpeta, instalar CLI explícitamente:

```powershell
py -3.12 -m pip install "flet-cli==0.25.2"
```

> **Importante**: incluir `--add-data "database;database"` para que el `database.sql` esté disponible en el ejecutable.

### Diagnóstico rápido (Windows)
```powershell
py -0p
py -3.12 -m pip show flet
where.exe flet
```

- `where.exe flet` vacío: el paquete puede estar instalado, pero `Scripts` no está en `PATH`.
- `pip show flet` sin resultados: falta instalar dependencias en ese Python.

> Nota: el proyecto ya incluye `pyinstaller` en `requirements.txt`. Si cambias versiones de dependencias, valida el empaquetado en una máquina limpia antes de distribuir.

### Impresión directa de comprobantes (Windows)

- La impresión directa del sistema ya no depende de la asociación de `.pdf` en Windows.
- El backend interno de impresión usa `pypdfium2` + `pywin32` para enviar páginas al spooler.
- Si ejecutas desde código fuente, instalar `requirements.txt` es obligatorio para disponer de ese backend.
- En caso de falla real de impresora/spooler, la app mantiene fallback abriendo el PDF para impresión manual.

---

## 2. Ubicación de Archivos (.env, Certificados, etc.)

La aplicación busca configuración en dos lugares, en este orden:

### Opción A: Instalación estándar (recomendado para PC fija)
- **Ruta**: `%APPDATA%\Nexoryn_Tech\`
- **Archivos**:
  - `.env`
  - `certs/` con certificados AFIP

### Opción B: Modo portable (pendrives o carpeta local)
- **Ruta**: mismo directorio del `NexorynTech.exe`
- **Archivos**:
  - `.env`
  - `certs/`

> Las rutas relativas de `AFIP_CERT_PATH`/`AFIP_KEY_PATH` se resuelven respecto al directorio de configuración.

---

## 3. Ejemplo de archivo `.env`

```env
# Conexión a Base de Datos
DATABASE_URL=postgresql://postgres:password@localhost:5432/nexoryn_tech

# Binarios PostgreSQL (si no están en PATH)
PG_BIN_PATH="C:\Program Files\PostgreSQL\16\bin"

# AFIP (homologación por defecto)
AFIP_PRODUCCION=False  # también soporta AFIP_PRODUCTION
AFIP_PUNTO_VENTA=3
AFIP_CUIT=20XXXXXXXX9
AFIP_CERT_PATH=certs/mi_certificado.crt
AFIP_KEY_PATH=certs/mi_llave.key
# Variantes soportadas:
# - Homologación: AFIP_CUIT_HOMOLOGACION / AFIP_CERT_PATH_HOMOLOGACION / AFIP_KEY_PATH_HOMOLOGACION
# - Producción: AFIP_CUIT_PRODUCCION o AFIP_CUIT_PRODUCTION (y equivalentes para CERT/KEY)

# UI (opcional)
NEXORYN_UI=basic  # o advanced
```

---

## 4. OpenSSL (AFIP)

No es obligatorio instalar Git, pero **OpenSSL sí** para AFIP. Opciones:

1. **Instalar Git para Windows** (incluye OpenSSL).
2. **Instalar OpenSSL independiente** (Win64 OpenSSL v3.x Light).
3. **Modo portable**: copiar `openssl.exe`, `libcrypto-*.dll` y `libssl-*.dll` en una carpeta `bin/` junto al `NexorynTech.exe`.

La app ya maneja `MSYS_NO_PATHCONV` automáticamente para evitar problemas de rutas en Windows.

> Nota importante: AFIP funciona en el `.exe` sin Bash. Lo único necesario es que `openssl.exe` esté accesible (instalado y en PATH, o incluido junto al ejecutable).

---

## 5. Implementación en Red LAN (Ejecutable Compartido)

Si planeas dejar el ejecutable en una carpeta compartida (ej: `\\SERVIDOR\Sistema\NexorynTech.exe`):

### Ubicación centralizada (recomendada)
Coloca `.env` y `certs/` en la misma carpeta de red donde está el ejecutable.

```text
\\SERVIDOR\Nexoryn\
├── NexorynTech.exe
├── .env
└── certs/
    ├── mi_empresa.crt
    └── mi_empresa.key
```

### Requisito en cada PC
Cada computadora que ejecute la app debe tener OpenSSL (o Git para Windows) instalado.

---

## Seguridad de Archivos en Red

Opciones (de menor a mayor seguridad):

1. **Atributos de Windows** (básico): `attrib +h +s .env` / `attrib +h +s certs`
2. **Permisos NTFS** (recomendado): restringir acceso a `.env` y `certs/`
3. **Almacenamiento en DB** (avanzado): guardar datos sensibles en `seguridad.config_sistema`

---

## Estructura para Modo Portable

```text
Carpeta_App/
├── NexorynTech.exe
├── .env
└── certs/
    ├── mi_certificado.crt
    └── mi_llave.key
```

---

## 6. Generación del Manual PDF

El Manual Maestro ahora se genera desde una fuente única en Markdown:

- Fuente: `docs/MANUAL_OPERATIVO.md`
- Generador: `scripts/generate_manual_pdf.py`
- Salida por defecto: `MANUAL_MAESTRO_NEXORYN_TECH.pdf`

### Comando estándar

```powershell
python scripts/generate_manual_pdf.py
```

Alternativas válidas según entorno:

```powershell
python3 scripts/generate_manual_pdf.py
py -3 scripts/generate_manual_pdf.py
```

### Comando con rutas explícitas

```powershell
python scripts/generate_manual_pdf.py --source docs/MANUAL_OPERATIVO.md --output MANUAL_MAESTRO_NEXORYN_TECH.pdf
```

### Regla operativa de mantenimiento

Cuando cambie un flujo operativo del sistema:

1. Actualizar primero `docs/MANUAL_OPERATIVO.md`.
2. Regenerar luego `MANUAL_MAESTRO_NEXORYN_TECH.pdf` con el script.
