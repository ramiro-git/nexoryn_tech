# Plan de Recuperación ante Desastres (DRP) - RPO & RTO

Este documento define los objetivos y estrategias para garantizar la durabilidad y disponibilidad de los datos en **Nexoryn Tech**.

---

## Definiciones

| Término | Definición |
|---------|------------|
| **RPO** (Recovery Point Objective) | Tiempo máximo aceptable de pérdida de datos ante un incidente. "¿Cuántos datos estoy dispuesto a perder?" |
| **RTO** (Recovery Time Objective) | Tiempo máximo aceptable que el sistema puede estar "caído" antes de restaurar el servicio. |

---

## Objetivos Definidos

### 1. RPO (Objetivo de Punto de Recuperación)

| Escenario | Objetivo RPO | Estrategia |
|-----------|--------------|------------|
| **Base** | < 24 horas | Backups diarios automáticos a las 23:00 |
| **Ideal** | < 1 hora | Backups manuales antes de operaciones críticas |
| **Óptimo** | Minutos | Backup manual inmediato pre-cambios masivos |

**Estrategias implementadas:**

1. **Backups Automáticos Programados**
   - **Diarios**: 23:00 HS - Garantiza máximo 24h de pérdida en el peor caso
   - **Semanales**: Domingos 23:30 - Puntos de restauración históricos
   - **Mensuales**: Día 1 del mes 00:00 - Auditoría y largo plazo

2. **Backups Manuales a Demanda**
   - Disponibles desde la UI para administradores
   - Se pueden crear backups de cualquier tipo (diario/semanal/mensual/manual)
   - **Recomendación**: Ejecutar antes de importaciones masivas o actualizaciones

### 2. RTO (Objetivo de Tiempo de Recuperación)

| Escenario | Objetivo RTO | Descripción |
|-----------|--------------|-------------|
| **Restauración local** | < 5 minutos | Backup en disco local |
| **Restauración externa** | < 15 minutos | Backup desde nube/disco externo |

**Estrategias implementadas:**

1. **Herramientas Locales**: `pg_dump` y `pg_restore` en formato comprimido
2. **Interfaz Gráfica**: Restauración con 2 clicks (seleccionar backup → confirmar)
3. **Sin comandos**: No requiere conocimiento técnico del administrador

---

## Panel de Backups en la Aplicación

### Funcionalidades Disponibles (Solo Administradores)

| Función | Descripción | Ubicación |
|---------|-------------|-----------|
| **Ver próximos backups** | Panel con cuenta regresiva para cada tipo programado | Sección superior |
| **Crear backup por tipo** | Ejecutar backup diario/semanal/mensual/manual a demanda | Botón "Crear Backup" o "Ejecutar" |
| **Cambiar carpeta destino** | Selector de carpeta con explorador de Windows | Icono de carpeta |
| **Sincronización en la nube** | Copia automática a carpeta sincronizada (Google Drive, OneDrive, etc.) | Icono de nube |
| **Restaurar backup** | Restaurar cualquier backup de la lista | Icono de restaurar |
| **Eliminar backup** | Eliminar backups obsoletos manualmente | Icono de eliminar |
| **Filtrar por tipo** | Ver solo backups de un tipo específico | Dropdown "Tipo" |

### Sincronización Automática en la Nube ☁️

La aplicación permite configurar una **carpeta de sincronización** que copia automáticamente cada backup a una ubicación secundaria. Esto es ideal para:

- **Google Drive**: `C:\Users\Usuario\Google Drive\Backups_Nexoryn`
- **OneDrive**: `C:\Users\Usuario\OneDrive\Backups_Nexoryn`
- **Dropbox**: `C:\Users\Usuario\Dropbox\Backups_Nexoryn`
- **Carpeta de red**: `\\servidor\backups\`

**Configuración:**
1. Ir a "Respaldos"
2. Click en icono ☁️ "Configurar carpeta de sincronización"
3. Seleccionar carpeta de tu servicio de nube
4. ¡Listo! Cada backup se copiará automáticamente

**Nota:** El cliente de escritorio del servicio de nube (Google Drive, OneDrive, etc.) debe estar instalado y configurado previamente.

### Visualización de Programación

El panel muestra:
- **Diario**: "Todos los días a las 23:00" → "En Xh Xmin"
- **Semanal**: "Domingos a las 23:30" → "En Xd"
- **Mensual**: "Día 1 de cada mes a las 00:00" → "En Xd"

---

## Plan de Ejecución de Backups

| Tipo | Frecuencia | Hora | Retención | Color en UI |
|:-----|:-----------|:-----|:----------|:------------|
| **Diario** | Todos los días | 23:00 | 7 copias | 🔵 Azul |
| **Semanal** | Domingos | 23:30 | 4 copias | 🟣 Violeta |
| **Mensual** | Día 1 del mes | 00:00 | 6 copias | 🩷 Rosa |
| **Manual** | A demanda | - | 100 copias | 🟢 Verde |

### Estructura de Carpetas

```
backups/
├── daily/          # Backups diarios (últimos 7)
├── weekly/         # Backups semanales (últimos 4)
├── monthly/        # Backups mensuales (últimos 6)
└── manual/         # Backups manuales (hasta 100)
```

### Limpieza Automática (Pruning)

Se ejecuta diariamente a la 01:00 para eliminar backups que excedan la política de retención.

---

## Procedimiento de Recuperación

### Paso a Paso

1. **Identificar el incidente**
   - Corrupción de datos
   - Borrado accidental
   - Falla de hardware/disco

2. **Acceder a Nexoryn Tech**
   - Ingresar con credenciales de **Administrador**

3. **Navegar a "Respaldos"**
   - Menú lateral → Sección "Respaldos" (solo visible para admins)

4. **Seleccionar Punto de Restauración**
   | Situación | Backup Recomendado |
   |-----------|-------------------|
   | Error reciente (hoy) | Último "Manual" o "Diario" |
   | Error de ayer/anteayer | Backup "Diario" correspondiente |
   | Error de hace semanas | Backup "Semanal" correspondiente |
   | Auditoría/histórico | Backup "Mensual" correspondiente |

5. **Ejecutar Restauración**
   - Click en icono 🔄 (Restaurar)
   - Confirmar en el diálogo de advertencia
   - Esperar mensaje de éxito

6. **Verificar Integridad**
   - Revisar: Clientes, Proveedores, Stock, Ventas
   - Confirmar que los datos son consistentes

7. **Reiniciar aplicación** (si es necesario)

---

## Recomendaciones de Seguridad

### Regla 3-2-1 📦

> Mantener **3** copias de los datos, en **2** tipos de almacenamiento diferentes, con **1** copia fuera del sitio.

**Implementación sugerida:**

1. **Copia 1**: Backups automáticos en carpeta local (`backups/`)
2. **Copia 2**: Sincronizar carpeta con nube (Google Drive, OneDrive, Dropbox)
3. **Copia 3**: Copia semanal a disco externo/USB

### Cambiar Ubicación de Backups

Para mayor seguridad, se puede cambiar la carpeta destino:
1. Ir a "Respaldos"
2. Click en icono 📁 junto a la ruta actual
3. Seleccionar carpeta en el explorador (ej: disco externo, NAS, carpeta sincronizada con nube)

### Mejores Prácticas

| Práctica | Frecuencia | Responsable |
|----------|------------|-------------|
| Verificar backups automáticos funcionan | Semanal | Admin |
| Backup manual antes de cambios masivos | Antes de cada operación | Admin |
| Copia a disco externo | Semanal | Admin |
| Probar restauración en entorno de prueba | Mensual | Admin |
| Revisar espacio en disco | Semanal | Admin |

---

## Troubleshooting

### Errores Comunes

| Error | Causa Probable | Solución |
|-------|----------------|----------|
| "pg_dump not found" | PostgreSQL no instalado o no en PATH | Instalar PostgreSQL o configurar `pg_bin_path` en config |
| "Permission denied" | Sin permisos de escritura en carpeta | Cambiar carpeta destino o ajustar permisos |
| "Backup failed" | BD no accesible o credenciales incorrectas | Verificar conexión a PostgreSQL |
| Restauración falla | Archivo corrupto o versión incompatible | Intentar con backup anterior |

### Contacto de Soporte

En caso de problemas graves de recuperación, contactar al equipo técnico con:
- Logs de la aplicación
- Mensaje de error exacto
- Fecha aproximada de los datos a recuperar
