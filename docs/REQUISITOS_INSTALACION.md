# Guía de Requisitos: Componentes "En Mano"

Este sistema utiliza archivos y configuraciones que, por seguridad o peso, **no están incluidos en el repositorio de Git**. Siempre que reinstales el sistema o lo muevas a otra PC, debes asegurarte de tener estos elementos.

---

## 1. Entorno de Software
Antes de empezar, la máquina debe tener:
- **Python 3.12+** (recomendado) agregado al PATH.
- **PostgreSQL 16+** (con `psql`, `pg_dump` y `pg_restore`).
- **OpenSSL** (para firmar solicitudes AFIP).
  - AFIP funciona en el `.exe` sin Bash, pero requiere `openssl.exe` accesible (instalado o junto al ejecutable).

### Instalación de dependencias
Ejecutar en la carpeta raíz:
```bash
pip install -r requirements.txt
```

---

## 2. Variables de Entorno (.env)
El archivo `.env` **no se sube a GitHub**.

**Ubicaciones soportadas (prioridad):**
1. `%APPDATA%\Nexoryn_Tech\.env` (instalación estándar)
2. `.env` junto al ejecutable o en la raíz del proyecto (modo portable/dev)

**Plantilla de `.env`:**
```ini
# Base de Datos
DATABASE_URL=postgresql://postgres:password@localhost:5432/nexoryn_tech
# o componentes individuales:
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nexoryn_tech
DB_USER=postgres
DB_PASSWORD=tu_password
DB_MAINTENANCE_USER_ID=1

# Pool (opcional)
DB_POOL_MIN=1
DB_POOL_MAX=4

# PostgreSQL bin (necesario si psql/pg_dump no están en PATH)
PG_BIN_PATH="C:\Program Files\PostgreSQL\16\bin"

# AFIP
AFIP_PRODUCCION=False  # también soporta AFIP_PRODUCTION
AFIP_PUNTO_VENTA=3

# Homologación (default si AFIP_PRODUCCION=False)
# También soporta *_HOMOLOGACION
AFIP_CUIT=20XXXXXXXX9
AFIP_CERT_PATH="C:/Nexoryn/Certs/empresa_homo.crt"
AFIP_KEY_PATH="C:/Nexoryn/Certs/empresa_homo.key"
# Alternativas homologación:
# AFIP_CUIT_HOMOLOGACION=20XXXXXXXX9
# AFIP_CERT_PATH_HOMOLOGACION="C:/Nexoryn/Certs/empresa_homo.crt"
# AFIP_KEY_PATH_HOMOLOGACION="C:/Nexoryn/Certs/empresa_homo.key"

# Producción (si AFIP_PRODUCCION=True)
# También soporta *_PRODUCTION
AFIP_CUIT_PRODUCCION=20YYYYYYYY9
AFIP_CERT_PATH_PRODUCCION="C:/Nexoryn/Certs/empresa_prod.crt"
AFIP_KEY_PATH_PRODUCCION="C:/Nexoryn/Certs/empresa_prod.key"
# Alternativas producción:
# AFIP_CUIT_PRODUCTION=20YYYYYYYY9
# AFIP_CERT_PATH_PRODUCTION="C:/Nexoryn/Certs/empresa_prod.crt"
# AFIP_KEY_PATH_PRODUCTION="C:/Nexoryn/Certs/empresa_prod.key"

# UI (opcional)
NEXORYN_UI=basic  # o advanced
```

Nota: `DB_MAINTENANCE_USER_ID` debe ser un `seguridad.usuario.id` existente (por defecto 1 para el admin inicial). Se usa en restores/psql para setear `app.user_id` y evitar bloqueos por RLS.

---

## 3. Certificados AFIP
Para que la facturación electrónica funcione, necesitas:
1. **Clave Privada (`.key`)**: Generada en tu PC. **No la compartas**.
2. **Certificado (`.crt`)**: Descargado de AFIP tras subir el CSR.
3. **Punto de Venta**: Número configurado en AFIP (ej: `0002`).

> Consulta `docs/GUIA_AFIP_PORTAL.md` para el paso a paso en AFIP.

---

## 4. Datos Iniciales (CSVs)
El script `database/init_db.py` busca archivos en:
`database/csvs/*.csv`

Estos archivos contienen la información histórica o bases de datos externas que el sistema importa al primer uso.

---

## 5. Resumen de lo que debes llevar
- [ ] Tu archivo `.env` configurado.
- [ ] Tu certificado AFIP (`.crt`).
- [ ] Tu clave privada AFIP (`.key`).
- [ ] La carpeta `database/csvs/` (si tienes datos para importar).
- [ ] La carpeta `backups/` o `backups_incrementales/` (si quieres conservar copias previas).
