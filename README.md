# Nexoryn Tech Desktop

Cliente de escritorio basado en `flet` que consume la base de datos PostgreSQL ya definida en `database/database.sql`.

## Pasos rápidos
1. Instalar dependencias:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Configurar la conexión a PostgreSQL creando un archivo `.env` (puedes copiar `.env.example`).
   * Puedes definir una URL completa:
     ```bash
     DATABASE_URL=postgresql://usuario:clave@host:puerto/base
     ```
   * O proveer los componentes individuales y dejar que la app los combine:
     ```bash
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=nexoryn_tech
     DB_USER=postgres
     DB_PASSWORD=postgres
     ```
3. Ejecutar la aplicación:
   ```bash
   python desktop_app/main.py
   ```

## Qué incluye
- `desktop_app/config.py`: carga las variables de entorno.
- `desktop_app/database.py`: conexión con pooling y consultas sobre las vistas `app.v_entidad_detallada` y `app.v_articulo_detallado`.
- `desktop_app/main.py`: UI en `flet` con pestañas para entidades, artículos y alertas de stock.
- `desktop_app/components/generic_table.py`: componente reutilizable que opera sobre cualquier consulta, con buscador, filtros, edición en línea/masiva, eliminaciones y paginación responsiva.

## Tabla genérica
Utilizá `GenericTable` cuando necesites mostrar cualquier vista/consulta. Se configura con columnas (pueden tener formateo, botones o editores personalizados), un proveedor de datos (retorna filas y total), parámetros de filtro/simple/avanzado y callbacks para edición y eliminación. Es el componente que reemplaza a `app.v_entidad_detallada` en el ejemplo y se puede reutilizar para artículos, movimientos u otros listados.

## Interfaz modernizada
- La pantalla central se re-construyó completamente con hero cards, métricas clave y secciones contenedoras que muestran las `GenericTable` para entidades y artículos junto a los paneles de stock y detalle.
- Todo el layout usa contenedores blancos con bordes redondeados sobre un fondo gris suave, botones con íconos y tarjetas de color para dar contraste y foco sobre la información.
- Añadir nuevas métricas o vistas solo implica crear nuevos contenedores `ft.Container` dentro del `main_column` y usar el mismo `GenericTable` con diferentes `data_provider`.

## Buenas prácticas
- Mantener la variable `DATABASE_URL` protegida y usar conexiones seguras.
- Correr `flet` en modo escritorio (ya incluido) y explorar filtros desde la interfaz.
