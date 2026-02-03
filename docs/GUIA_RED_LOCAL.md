# Guía de Configuración en Red Local (LAN)

Esta guía explica cómo configurar el sistema para que funcione con múltiples computadoras en la misma red. Una computadora actuará como **Servidor** (donde está la base de datos) y las demás como **Clientes**.

## 1. Preparar el Servidor (Computadora con la Base de Datos)

### A. Obtener la IP Local
1. Abre una terminal (CMD o PowerShell).
2. Escribe `ipconfig` y presiona Enter.
3. Busca la sección de tu adaptador (Wi‑Fi o Ethernet).
4. Anota la **Dirección IPv4** (ejemplo: `192.168.1.15`).

### B. Configurar PostgreSQL
PostgreSQL debe aceptar conexiones remotas:
1. Ubica `postgresql.conf` (ej: `C:\Program Files\PostgreSQL\[VERSION]\data`).
2. Cambia `listen_addresses` a:
   ```text
   listen_addresses = '*'
   ```
3. En `pg_hba.conf`, agrega:
   ```text
   host    all             all             192.168.1.0/24          md5
   ```
   Ajusta el rango si tu red es distinta (ej: `10.0.0.0/24`).
4. Reinicia el servicio de PostgreSQL.

### C. Habilitar el Firewall
Permitir tráfico al puerto 5432:
1. Firewall de Windows Defender > Configuración avanzada.
2. Regla de entrada > Puerto > TCP > `5432`.
3. Permitir la conexión.

---

## 2. Configurar los Clientes (Demás Computadoras)

En cada PC cliente:
1. Asegúrate de tener el ejecutable o el proyecto.
2. Edita el `.env` usado por la app.
3. Cambia `DB_HOST` por la IP del servidor.

```env
DB_HOST=192.168.1.15
```

También puedes usar `DATABASE_URL`:
```env
DATABASE_URL=postgresql://postgres:password@192.168.1.15:5432/nexoryn_tech
```

**Ubicación del `.env`:**
- `%APPDATA%\Nexoryn_Tech\.env` (instalación estándar)
- `.env` junto al ejecutable (modo portable)

---

## Resumen de cambios
- **Servidor**: habilitar `listen_addresses`, agregar regla en `pg_hba.conf`, abrir puerto `5432`.
- **Clientes**: apuntar `DB_HOST`/`DATABASE_URL` a la IP del servidor.
