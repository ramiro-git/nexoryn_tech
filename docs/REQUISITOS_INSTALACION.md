# Guía de Requisitos: Componentes "En Mano"

Este sistema utiliza archivos y configuraciones que, por seguridad o peso, **no están incluidos en el repositorio de Git**. Siempre que reinstales el sistema o lo muevas a otra PC, debes asegurarte de tener estos elementos.

---

## 1. Entorno de Software
Antes de empezar, la máquina debe tener:
*   **Python 3.12** o superior: Instalado y agregado al PATH.
*   **PostgreSQL 16** o superior: Instalado y corriendo.
*   **OpenSSL**: Necesario para la generación de pedidos de certificados (CSR).

### Instalación de dependencias:
Ejecutar en la carpeta raíz:
```bash
pip install -r requirements.txt
```

---

## 2. Variables de Entorno (.env)
El archivo `.env` **no se sube a GitHub**. Debes crearlo manualmente en la raíz del proyecto.

**Plantilla de `.env`:**
```ini
# Base de Datos
# Puedes usar la URL completa:
DATABASE_URL=postgresql://postgres:password@localhost:5432/nexoryn_tech

# O componentes individuales (la aplicación prioriza DATABASE_URL si existe):
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nexoryn_tech
DB_USER=postgres
DB_PASSWORD=tu_password

# Rutas de PostgreSQL (Necesario para backups/restauración)
PG_BIN_PATH="C:\Program Files\PostgreSQL\16\bin"

# AFIP
AFIP_CUIT=20XXXXXXXX9
AFIP_CERT_PATH="C:/Nexoryn/Certs/empresa.crt"
AFIP_KEY_PATH="C:/Nexoryn/Certs/empresa.key"
AFIP_PUNTO_VENTA=3
AFIP_PRODUCTION=False
```

---

## 3. Certificados AFIP
Para que la facturación electrónica funcione, necesitas:
1.  **Clave Privada (`.key`)**: Generada en tu PC. **No la compartas**.
2.  **Certificado (`.crt`)**: El archivo que descargas de la web de AFIP tras subir el CSR.
3.  **Punto de Venta**: El número de punto de venta configurado en AFIP (ej: `0002`).

> [!IMPORTANT]
> Consulta la [Guía de AFIP](GUIA_AFIP_PORTAL.md) para saber cómo obtener estos archivos.

---

## 4. Datos Iniciales (CSVs)
El script de inicialización de base de datos (`database/init_db.py`) busca archivos en:
`database/csvs/*.csv`

Estos archivos contienen la información histórica o bases de datos externas que el sistema importa al primer uso. Asegúrate de copiar la carpeta `csvs` completa si vas a realizar una migración de datos.

---

## 5. Resumen de lo que debes llevar en un Pendrive/Cloud:
- [ ] Tu archivo `.env` configurado.
- [ ] Tu certificado AFIP (`.crt`).
- [ ] Tu clave privada AFIP (`.key`).
- [ ] La carpeta `database/csvs/` (si tienes datos para importar).
- [ ] La carpeta `backups/` (opcional, si quieres conservar copias de seguridad anteriores).
