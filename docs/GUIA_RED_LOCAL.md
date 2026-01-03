# Guía de Configuración en Red Local (LAN)

Esta guía explica cómo configurar el sistema para que funcione con múltiples computadoras (Notebooks/PCs) conectadas al mismo router. Una computadora actuará como **Servidor** (donde está la base de datos) y las demás como **Clientes**.

## 1. Preparar el Servidor (Computadora con la Base de Datos)

### A. Obtener la IP Local
1. Abre una terminal (CMD o PowerShell).
2. Escribe `ipconfig` y presiona Enter.
3. Busca la sección "Adaptador de LAN inalámbrica Wi-Fi" (o Ethernet si usas cable).
4. Anota la **Dirección IPv4** (ejemplo: `192.168.1.15`). Los demás equipos usarán esta IP para conectarse.

### B. Configurar PostgreSQL
PostgreSQL debe estar configurado para "escuchar" conexiones de otros equipos:
1. Localiza los archivos de configuración (usualmente en `C:\Program Files\PostgreSQL\[VERSÍON]\data`).
2. **`postgresql.conf`**: Busca la línea `listen_addresses` y cámbiala a:
   ```text
   listen_addresses = '*'
   ```
3. **`pg_hba.conf`**: Añade esta línea al final para permitir el acceso desde tu red local:
   ```text
   host    all             all             192.168.1.0/24          md5
   ```
   *(Nota: Ajusta `192.168.1.0` si tu red usa otro rango, ej. `10.0.0.0`)*.
4. **Reiniciar el servicio**: Abre "Servicios" en Windows, busca `postgresql-x64-[VERSION]` y dale a "Reiniciar".

### C. Habilitar el Firewall
Debes permitir que el tráfico llegue al puerto de la base de datos:
1. Ve a "Panel de Control" > "Sistema y Seguridad" > "Firewall de Windows Defender" > "Configuración avanzada".
2. En **Reglas de Entrada**, crea una **Nueva regla**.
3. Elige **Puerto** > **TCP** > Escribe `5432` en puertos locales específicos.
4. Elige **Permitir la conexión** y asígnale un nombre (ej. "PostgreSQL LAN").

---

## 2. Configurar los Clientes (Demás Computadoras)

En cada notebook que vaya a usar el sistema pero **no** tenga la base de datos:

1. Asegúrate de tener el código del proyecto o el ejecutable.
2. Abre el archivo de configuración de entorno (archivo `.env`).
3. Cambia el valor de `DB_HOST` por el de la IP del Servidor que anotaste en el paso 1-A:
   ```env
   DB_HOST=192.168.1.15
   ```
4. Asegúrate de que `DB_USER`, `DB_PASSWORD` y `DB_NAME` coincidan con los configurados en el Servidor.

---

## Resumen de cambios:
- **Servidor**: Cambia `listen_addresses`, añade permiso en `pg_hba.conf` y abre el puerto `5432` en el Firewall.
- **Clientes**: Cambia el `DB_HOST` en el archivo `.env` por la IP del servidor.
