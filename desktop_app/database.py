import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg_pool import ConnectionPool


def _rows_to_dicts(cursor) -> List[Dict[str, Any]]:
    columns = [
        col.name if hasattr(col, "name") else col[0]
        for col in cursor.description
    ]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = ConnectionPool(conninfo=dsn)
        self.current_user_id: Optional[int] = None
        self.current_ip: Optional[str] = None
        self.is_closing = False

    def set_context(self, user_id: Optional[int], ip: Optional[str] = None) -> None:
        self.current_user_id = user_id
        self.current_ip = ip

    def _setup_session(self, cur: Any) -> None:
        if self.current_user_id:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(self.current_user_id),))
        if self.current_ip:
            cur.execute("SELECT set_config('app.ip', %s, true)", (self.current_ip,))

    def log_activity(self, entidad: str, accion: str, id_entidad: Optional[int] = None, resultado: str = "OK", detalle: Optional[Dict[str, Any]] = None) -> None:
        if self.is_closing:
            return # Silent skip on shutdown
            
        query = """
            INSERT INTO seguridad.log_actividad (id_usuario, id_tipo_evento_log, entidad, id_entidad, accion, resultado, ip, detalle)
            VALUES (
                %s, 
                COALESCE(
                    (SELECT id FROM seguridad.tipo_evento_log WHERE codigo = %s),
                    (SELECT id FROM seguridad.tipo_evento_log WHERE codigo = 'ERROR'),
                    (SELECT id FROM seguridad.tipo_evento_log LIMIT 1)
                ), 
                %s, %s, %s, %s, %s, %s
            )
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (
                        self.current_user_id,
                        accion.upper() if accion else "ERROR",
                        entidad, id_entidad, accion, resultado, self.current_ip,
                        json.dumps(detalle) if detalle else None
                    ))
                    conn.commit()
        except Exception:
            pass # Silent failure for logs

    # =========================================================================
    # In-Memory Catalog Cache (reduces DB hits for frequently accessed data)
    # =========================================================================
    _catalog_cache: Dict[str, Tuple[float, List[str]]] = {}  # {key: (timestamp, data)}
    _CACHE_TTL = 300  # 5 minutes

    def _get_cached_catalog(self, cache_key: str, query: str) -> List[str]:
        """Get catalog from cache or fetch from DB if expired."""
        import time
        now = time.time()
        if cache_key in self._catalog_cache:
            cached_time, cached_data = self._catalog_cache[cache_key]
            if now - cached_time < self._CACHE_TTL:
                return cached_data
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                result = [row[0] if isinstance(row, (list, tuple)) else row.get("nombre", "") for row in rows]
                self._catalog_cache[cache_key] = (now, result)
                return result

    def invalidate_catalog_cache(self, cache_key: Optional[str] = None) -> None:
        """Invalidate catalog cache. Call after modifying catalogs."""
        if cache_key:
            self._catalog_cache.pop(cache_key, None)
        else:
            self._catalog_cache.clear()

    # =========================================================================
    # Batch Dashboard Statistics (single connection, fewer round-trips)
    # =========================================================================
    def get_all_dashboard_stats(self) -> Dict[str, Any]:
        """Fetch all dashboard statistics in a single database connection."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Combined query for efficiency
                cur.execute("""
                    SELECT 
                        -- Entidades
                        (SELECT COUNT(*) FROM app.entidad_comercial WHERE tipo = 'CLIENTE' OR tipo = 'AMBOS') as clientes,
                        (SELECT COUNT(*) FROM app.entidad_comercial WHERE tipo = 'PROVEEDOR' OR tipo = 'AMBOS') as proveedores,
                        (SELECT COUNT(*) FROM app.entidad_comercial WHERE activo = true) as entidades_activas,
                        -- Articulos
                        (SELECT COUNT(*) FROM app.articulo) as articulos_total,
                        (SELECT COUNT(*) FROM app.v_articulo_detallado WHERE stock_actual <= stock_minimo) as bajo_stock,
                        (SELECT COALESCE(SUM(costo * stock_actual), 0) FROM app.v_articulo_detallado) as valorizacion,
                        -- Facturación
                        (SELECT COALESCE(SUM(total), 0) FROM app.v_documento_resumen WHERE clase = 'VENTA' AND fecha >= date_trunc('month', now())) as ventas_mes,
                        (SELECT COALESCE(SUM(total), 0) FROM app.v_documento_resumen WHERE clase = 'COMPRA' AND fecha >= date_trunc('month', now())) as compras_mes,
                        (SELECT COUNT(*) FROM app.documento WHERE estado IN ('BORRADOR', 'CONFIRMADO')) as pendientes,
                        -- Movimientos
                        (SELECT COUNT(*) FROM app.v_movimientos_full WHERE fecha >= current_date AND signo_stock > 0) as mov_ingresos,
                        (SELECT COUNT(*) FROM app.v_movimientos_full WHERE fecha >= current_date AND signo_stock < 0) as mov_salidas,
                        (SELECT COUNT(*) FROM app.movimiento_articulo WHERE id_documento IS NULL AND fecha >= current_date) as ajustes,
                        -- Pagos
                        (SELECT COALESCE(SUM(monto), 0) FROM app.pago WHERE fecha >= current_date) as pagos_hoy,
                        (SELECT COUNT(*) FROM app.pago WHERE fecha >= now() - interval '7 days') as pagos_recientes,
                        -- Usuarios (Real connected sessions)
                (
                    WITH last_states AS (
                        SELECT id_usuario, accion, 
                               ROW_NUMBER() OVER (PARTITION BY id_usuario ORDER BY fecha_hora DESC) as rn
                        FROM seguridad.log_actividad
                        WHERE id_usuario IS NOT NULL
                          AND accion IN ('LOGIN_OK', 'LOGOUT')
                          AND fecha_hora > now() - interval '24 hours'
                    )
                    SELECT COUNT(*) 
                    FROM last_states l
                    JOIN seguridad.usuario u ON l.id_usuario = u.id
                    JOIN seguridad.rol r ON u.id_rol = r.id
                    WHERE l.rn = 1 AND l.accion = 'LOGIN_OK'
                ) as usuarios_conectados
            """)
                row = cur.fetchone()
                
                return {
                    "entidades": {
                        "clientes": row[0] or 0,
                        "proveedores": row[1] or 0,
                        "activos": row[2] or 0
                    },
                    "articulos": {
                        "total": row[3] or 0,
                        "bajo_stock": row[4] or 0,
                        "valorizacion": float(row[5] or 0)
                    },
                    "facturacion": {
                        "ventas_mes": float(row[6] or 0),
                        "compras_mes": float(row[7] or 0),
                        "pendientes": row[8] or 0
                    },
                    "movimientos": {
                        "ingresos": row[9] or 0,
                        "salidas": row[10] or 0,
                        "ajustes": row[11] or 0
                    },
                    "pagos": {
                        "hoy": float(row[12] or 0),
                        "recientes": row[13] or 0
                    },
                    "usuarios": {
                        "activos": row[14] or 0
                    }
                }

    # Dashboard Statistics (individual methods kept for backwards compatibility)
    def get_stats_entidades(self) -> Dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tipo, COUNT(*) FROM app.entidad_comercial GROUP BY tipo")
                rows = cur.fetchall()
                counts = {row[0]: row[1] for row in rows} if rows else {}
                cur.execute("SELECT COUNT(*) FROM app.entidad_comercial WHERE activo = true")
                active = cur.fetchone()[0]
                return {
                    "clientes": (counts.get("CLIENTE", 0) or 0) + (counts.get("AMBOS", 0) or 0),
                    "proveedores": (counts.get("PROVEEDOR", 0) or 0) + (counts.get("AMBOS", 0) or 0),
                    "activos": active
                }

    def get_stats_articulos(self) -> Dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM app.articulo")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM app.v_articulo_detallado WHERE stock_actual <= stock_minimo")
                low_stock = cur.fetchone()[0]
                cur.execute("SELECT SUM(costo * stock_actual) FROM app.v_articulo_detallado")
                val = cur.fetchone()[0] or 0
                return {"total": total, "bajo_stock": low_stock, "valorizacion": float(val)}

    def get_stats_facturacion(self) -> Dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT SUM(total) FROM app.v_documento_resumen 
                    WHERE clase = 'VENTA' AND fecha >= date_trunc('month', now())
                """)
                ventas = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT SUM(total) FROM app.v_documento_resumen 
                    WHERE clase = 'COMPRA' AND fecha >= date_trunc('month', now())
                """)
                compras = cur.fetchone()[0] or 0
                cur.execute("SELECT COUNT(*) FROM app.documento WHERE estado IN ('BORRADOR', 'CONFIRMADO')")
                pend = cur.fetchone()[0]
                return {"ventas_mes": float(ventas), "compras_mes": float(compras), "pendientes": pend}

    def get_stats_movimientos(self) -> Dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        SUM(CASE WHEN signo_stock > 0 THEN 1 ELSE 0 END) as ingresos,
                        SUM(CASE WHEN signo_stock < 0 THEN 1 ELSE 0 END) as salidas
                    FROM app.v_movimientos_full 
                    WHERE fecha >= current_date
                """)
                res = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM app.movimiento_articulo WHERE id_documento IS NULL AND fecha >= current_date")
                ajustes = cur.fetchone()[0]
                return {"ingresos": res[0] or 0, "salidas": res[1] or 0, "ajustes": ajustes}

    def get_stats_pagos(self) -> Dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SUM(monto) FROM app.pago WHERE fecha >= current_date")
                cobrado = cur.fetchone()[0] or 0
                cur.execute("SELECT COUNT(*) FROM app.pago WHERE fecha >= now() - interval '7 days'")
                recientes = cur.fetchone()[0]
                return {"hoy": float(cobrado), "recientes": recientes}
    
    def get_stats_usuarios(self) -> Dict[str, Any]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM seguridad.usuario WHERE activo = true")
                active = cur.fetchone()[0]
                cur.execute("SELECT MAX(ultimo_login) FROM seguridad.usuario")
                last = cur.fetchone()[0]
                return {"activos": active, "ultimo_login": last.strftime("%H:%M") if last else "N/A"}

    # =========================================================================
    # Authentication
    # =========================================================================
    def authenticate_user(self, email_or_username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user by email and password.
        Returns user dict with role if successful, None if authentication fails.
        Uses bcrypt via PostgreSQL's pgcrypto extension.
        """
        if not email_or_username or not password:
            return None
        
        query = """
            SELECT 
                u.id, 
                u.nombre, 
                u.email, 
                u.activo,
                r.nombre AS rol,
                u.contrasena_hash
            FROM seguridad.usuario u
            JOIN seguridad.rol r ON r.id = u.id_rol
            WHERE lower(u.email) = lower(%s) OR lower(u.nombre) = lower(%s)
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (email_or_username.strip(), email_or_username.strip()))
                    row = cur.fetchone()
                    
                    if not row:
                        self._log_login_attempt(None, email_or_username, False)
                        return None
                    
                    # Extract user data - handle both dict and tuple
                    if isinstance(row, dict):
                        user_id = row.get("id")
                        nombre = row.get("nombre")
                        email = row.get("email")
                        activo = row.get("activo")
                        rol = row.get("rol")
                        stored_hash = row.get("contrasena_hash")
                    else:
                        user_id, nombre, email, activo, rol, stored_hash = row
                    
                    # Check if user is active
                    if not activo:
                        self._log_login_attempt(user_id, email_or_username, False, "Usuario inactivo")
                        return None
                    
                    # Verify password using bcrypt via PostgreSQL
                    cur.execute(
                        "SELECT crypt(%s, %s) = %s AS valid",
                        (password, stored_hash, stored_hash)
                    )
                    result = cur.fetchone()
                    is_valid = result.get("valid") if isinstance(result, dict) else result[0]
                    
                    if not is_valid:
                        self._log_login_attempt(user_id, email_or_username, False, "Contraseña incorrecta")
                        return None
                    
                    # Update ultimo_login
                    cur.execute(
                        "UPDATE seguridad.usuario SET ultimo_login = now() WHERE id = %s",
                        (user_id,)
                    )
                    conn.commit()
                    
                    # Log successful login
                    self._log_login_attempt(user_id, email_or_username, True)
                    
                    return {
                        "id": user_id,
                        "nombre": nombre,
                        "email": email,
                        "rol": rol
                    }
        except Exception as e:
            print(f"Error during authentication: {e}")
            return None
    
    def _log_login_attempt(self, user_id: Optional[int], identifier: str, success: bool, detail: str = None) -> None:
        """Log login attempt to activity log."""
        try:
            event_code = "LOGIN_OK" if success else "LOGIN_FAIL"
            query = """
                INSERT INTO seguridad.log_actividad (id_usuario, id_tipo_evento_log, entidad, accion, resultado, ip, detalle)
                VALUES (
                    %s, 
                    (SELECT id FROM seguridad.tipo_evento_log WHERE codigo = %s),
                    'seguridad.usuario',
                    %s,
                    %s,
                    %s,
                    %s
                )
            """
            detalle = {"identifier": identifier}
            if detail:
                detalle["motivo"] = detail
            
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (
                        user_id,
                        event_code,
                        event_code,
                        "OK" if success else "FAIL",
                        self.current_ip,
                        json.dumps(detalle)
                    ))
                    conn.commit()
        except Exception as e:
            print(f"Error logging login attempt: {e}")


    def get_reporte_ventas(self, limit: int = 12) -> List[Dict[str, Any]]:
        query = "SELECT * FROM app.v_reporte_ventas_mensual LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_top_articulos(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = "SELECT * FROM app.v_top_articulos_mes LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_deudores(self, limit: int = 50) -> List[Dict[str, Any]]:
        query = "SELECT * FROM app.v_deudores LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def close(self) -> None:
        """Gracefully close the connection pool and join worker threads."""
        self.is_closing = True
        if hasattr(self, 'pool') and self.pool:
            try:
                # Explicitly close the pool to join worker threads and avoid PythonFinalizationError
                self.pool.close()
            except Exception:
                pass
            finally:
                self.pool = None

    def _build_order_by(
        self,
        sorts: Optional[Sequence[Tuple[str, str]]],
        mapping: Dict[str, str],
        default: str,
        tiebreaker: Optional[str] = None,
    ) -> str:
        clauses: List[str] = []
        if sorts:
            for key, direction in sorts:
                column = mapping.get((key or "").strip())
                if not column:
                    continue
                dir_sql = "DESC" if (direction or "").lower() == "desc" else "ASC"
                clauses.append(f"{column} {dir_sql}")
        if not clauses:
            clauses.append(default)
        if tiebreaker:
            clauses.append(tiebreaker)
        seen: set = set()
        deduped: List[str] = []
        for clause in clauses:
            if clause in seen:
                continue
            seen.add(clause)
            deduped.append(clause)
        return ", ".join(deduped)

    def _build_entity_filters(
        self,
        search: Optional[str],
        tipo: Optional[str],
        advanced: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, List[Any]]:
        filters: List[str] = ["1=1"]
        params: List[Any] = []

        advanced = advanced or {}

        if tipo:
            tipo_upper = tipo.upper()
            if tipo_upper == "AMBOS":
                filters.append("tipo IN ('CLIENTE', 'PROVEEDOR', 'AMBOS')")
            else:
                filters.append("tipo = %s")
                params.append(tipo_upper)

        if search:
            search_pattern = f"%{search.strip().lower()}%"
            filters.append(
                "(lower(nombre_completo) LIKE %s OR lower(razon_social) LIKE %s OR lower(cuit) LIKE %s)"
            )
            params.extend([search_pattern] * 3)

        cuit = advanced.get("cuit")
        if isinstance(cuit, str) and cuit.strip():
            filters.append("lower(cuit) LIKE %s")
            params.append(f"%{cuit.strip().lower()}%")

        localidad = advanced.get("localidad")
        if isinstance(localidad, str) and localidad.strip():
            filters.append("lower(localidad) LIKE %s")
            params.append(f"%{localidad.strip().lower()}%")

        provincia = advanced.get("provincia")
        if isinstance(provincia, str) and provincia.strip():
            filters.append("lower(provincia) LIKE %s")
            params.append(f"%{provincia.strip().lower()}%")

        activo = advanced.get("activo")
        if isinstance(activo, str):
            activo = activo.strip().upper()
            if activo in {"ACTIVO", "TRUE", "SI", "SÍ", "1"}:
                activo = True
            elif activo in {"INACTIVO", "FALSE", "NO", "0"}:
                activo = False
            else:
                activo = None
        if activo is True or activo is False:
            filters.append("activo = %s")
            params.append(bool(activo))

        return " AND ".join(filters), params

    def fetch_entities(
        self,
        search: Optional[str] = None,
        tipo: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 60,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where_clause, params = self._build_entity_filters(search, tipo, advanced)

        sort_columns = {
            "id": "id",
            "tipo": "tipo",
            "nombre_completo": "nombre_completo",
            "razon_social": "razon_social",
            "cuit": "cuit",
            "localidad": "localidad",
            "provincia": "provincia",
            "lista_precio": "lista_precio",
            "descuento": "descuento",
            "saldo_cuenta": "saldo_cuenta",
            "activo": "activo",
        }
        order_by = self._build_order_by(sorts, sort_columns, default="nombre_completo ASC", tiebreaker="id ASC")

        query = f"""
            SELECT
                id,
                tipo,
                nombre_completo,
                razon_social,
                cuit,
                localidad,
                provincia,
                lista_precio,
                descuento,
                saldo_cuenta,
                activo
            FROM app.v_entidad_detallada
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s
            OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def fetch_entity_by_id(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Fetch full details for an entity by ID from the detailed view."""
        query = "SELECT * FROM app.v_entidad_detallada WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(entity_id),))
                rows = _rows_to_dicts(cur)
                return rows[0] if rows else None

    def count_entities(
        self,
        search: Optional[str] = None,
        tipo: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
    ) -> int:
        where_clause, params = self._build_entity_filters(search, tipo, advanced)
        query = f"SELECT COUNT(*) AS total FROM app.v_entidad_detallada WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def count_entities_active(self, activo: bool = True) -> int:
        query = "SELECT COUNT(*) AS total FROM app.entidad_comercial WHERE activo = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (activo,))
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def count_entities_by_type(self, tipo: str) -> int:
        query = "SELECT COUNT(*) AS total FROM app.entidad_comercial WHERE tipo = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (tipo,))
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def _build_article_filters(
        self,
        search: Optional[str],
        activo_only: Optional[bool],
        advanced: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, List[Any]]:
        filters: List[str] = ["1=1"]
        params: List[Any] = []

        advanced = advanced or {}

        if activo_only is True:
            filters.append("activo = TRUE")
        elif activo_only is False:
            filters.append("activo = FALSE")

        if search:
            pattern = f"%{search.strip().lower()}%"
            filters.append("lower(nombre) LIKE %s")
            params.append(pattern)

        def add_like(field: str, value: Any) -> None:
            if isinstance(value, str) and value.strip():
                filters.append(f"lower({field}) LIKE %s")
                params.append(f"%{value.strip().lower()}%")

        add_like("nombre", advanced.get("nombre"))
        add_like("marca", advanced.get("marca"))
        add_like("rubro", advanced.get("rubro"))
        add_like("proveedor", advanced.get("proveedor"))
        add_like("ubicacion", advanced.get("ubicacion"))

        costo_min = advanced.get("costo_min")
        costo_max = advanced.get("costo_max")
        try:
            if costo_min not in (None, ""):
                filters.append("costo >= %s")
                params.append(float(costo_min))
        except Exception:
            pass
        try:
            if costo_max not in (None, ""):
                filters.append("costo <= %s")
                params.append(float(costo_max))
        except Exception:
            pass

        stock_bajo = advanced.get("stock_bajo_minimo")
        if stock_bajo is True:
            filters.append("COALESCE(stock_actual, 0) < COALESCE(stock_minimo, 0)")

        # List price filter (if provided, we might want to filter only articles with that list price)
        # But usually we just want to see the value. Let's assume filter means "must have price"
        lp_id = advanced.get("id_lista_precio")
        if lp_id not in (None, ""):
            filters.append("id IN (SELECT id_articulo FROM app.articulo_precio WHERE id_lista_precio = %s)")
            params.append(int(lp_id))

        return " AND ".join(filters), params

    def fetch_articles(
        self,
        search: Optional[str] = None,
        activo_only: Optional[bool] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 40,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where_clause, params = self._build_article_filters(search, activo_only, advanced)

        sort_columns = {
            "id": "id",
            "nombre": "nombre",
            "marca": "marca",
            "rubro": "rubro",
            "costo": "costo",
            "porcentaje_iva": "porcentaje_iva",
            "proveedor": "proveedor",
            "stock_minimo": "stock_minimo",
            "stock_actual": "stock_actual",
            "ubicacion": "ubicacion",
            "activo": "activo",
        }
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")

        lp_id = (advanced or {}).get("id_lista_precio")
        if lp_id not in (None, ""):
            query = f"""
                SELECT ad.*, ap.precio as precio_lista
                FROM app.v_articulo_detallado ad
                LEFT JOIN app.articulo_precio ap ON ad.id = ap.id_articulo AND ap.id_lista_precio = %s
            """
            params.insert(0, int(lp_id))
        else:
            query = f"""
                SELECT ad.*, NULL::numeric as precio_lista
                FROM app.v_articulo_detallado ad
            """

        query += f" WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_articles(
        self,
        search: Optional[str] = None,
        activo_only: Optional[bool] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
    ) -> int:
        where_clause, params = self._build_article_filters(search, activo_only, advanced)
        query = f"SELECT COUNT(*) AS total FROM app.v_articulo_detallado WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def get_article_details(self, article_id: int) -> Dict[str, Any]:
        """Fetch full details for an article including all price lists."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # 1. Basic Info
                cur.execute("SELECT * FROM app.v_articulo_detallado WHERE id = %s", (article_id,))
                basic_info = _rows_to_dicts(cur)
                if not basic_info:
                    return {}
                
                result = basic_info[0]
                
                # 2. Price Lists
                cur.execute("""
                    SELECT 
                        lp.nombre as lista_nombre,
                        ap.precio,
                        ap.porcentaje,
                        tp.tipo as tipo_porcentaje,
                        ap.fecha_actualizacion
                    FROM app.articulo_precio ap
                    JOIN ref.lista_precio lp ON ap.id_lista_precio = lp.id
                    JOIN ref.tipo_porcentaje tp ON ap.id_tipo_porcentaje = tp.id
                    WHERE ap.id_articulo = %s
                    ORDER BY lp.id ASC
                """, (article_id,))
                result['precios'] = _rows_to_dicts(cur)
                
                return result

    def set_article_active(self, article_id: int, active: bool) -> bool:
        """Toggle article active status."""
        query = "UPDATE app.articulo SET activo = %s WHERE id = %s"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (active, article_id))
                    conn.commit()
                    self.log_activity("app.articulo", "UPDATE", article_id, detalle={"activo": active})
                    return True
        except Exception as e:
            print(f"Error updating article status: {e}")
            return False

    def fetch_stock_alerts(
        self,
        search: Optional[str] = None,
        limit: int = 5,
        offset: int = 0,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        filters: List[str] = ["COALESCE(stock_actual, 0) < COALESCE(stock_minimo, 0)"]
        params: List[Any] = []
        if search:
            filters.append("lower(nombre) LIKE %s")
            params.append(f"%{search.strip().lower()}%")
        where_clause = " AND ".join(filters)

        sort_columns = {
            "nombre": "nombre",
            "stock_actual": "stock_actual",
            "stock_minimo": "stock_minimo",
            "diferencia": "diferencia",
        }
        order_by = self._build_order_by(sorts, sort_columns, default="diferencia ASC", tiebreaker="nombre ASC")

        query = f"""
            SELECT
                nombre,
                COALESCE(stock_actual, 0) AS stock_actual,
                stock_minimo,
                COALESCE(stock_actual, 0) - COALESCE(stock_minimo, 0) AS diferencia
            FROM app.v_articulo_detallado
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s
            OFFSET %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                params.extend([limit, offset])
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def list_marcas(self) -> List[str]:
        return self._get_cached_catalog("marcas", "SELECT nombre FROM ref.marca ORDER BY nombre")

    def list_rubros(self) -> List[str]:
        return self._get_cached_catalog("rubros", "SELECT nombre FROM ref.rubro ORDER BY nombre")

    def list_proveedores(self) -> List[Dict[str, Any]]:
        """Special case: list providers for dropdowns (id + name)."""
        # We don't use _get_cached_catalog here because it expects List[str], but we want List[Dict]
        import time
        cache_key = "proveedores"
        now = time.time()
        if cache_key in self._catalog_cache:
            cached_time, cached_data = self._catalog_cache[cache_key]
            if now - cached_time < self._CACHE_TTL:
                return cached_data # type: ignore
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, nombre_completo as nombre FROM app.v_entidad_detallada WHERE tipo IN ('PROVEEDOR', 'AMBOS') ORDER BY nombre")
                result = _rows_to_dicts(cur)
                self._catalog_cache[cache_key] = (now, result) # type: ignore
                return result

    def list_unidades_medida(self) -> List[Dict[str, Any]]:
        import time
        cache_key = "unidades"
        now = time.time()
        if cache_key in self._catalog_cache:
            cached_time, cached_data = self._catalog_cache[cache_key]
            if now - cached_time < self._CACHE_TTL:
                return cached_data # type: ignore
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, nombre, abreviatura FROM ref.unidad_medida ORDER BY nombre")
                result = _rows_to_dicts(cur)
                self._catalog_cache[cache_key] = (now, result) # type: ignore
                return result


    def _build_catalog_filters(self, search: Optional[str]) -> Tuple[str, List[Any]]:
        filters: List[str] = ["1=1"]
        params: List[Any] = []
        if isinstance(search, str) and search.strip():
            filters.append("lower(nombre) LIKE %s")
            params.append(f"%{search.strip().lower()}%")
        return " AND ".join(filters), params

    def fetch_marcas(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 80,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"""
            SELECT id, nombre
            FROM ref.marca
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s
            OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_marcas(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.marca WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_marca(self, nombre: str) -> int:
        if not isinstance(nombre, str) or not nombre.strip():
            raise ValueError("El nombre no puede estar vacío.")
        query = "INSERT INTO ref.marca (nombre) VALUES (%s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(),))
                res = cur.fetchone()
                conn.commit()
                self.invalidate_catalog_cache("marcas")
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_marca_fields(self, marca_id: int, updates: Dict[str, Any]) -> None:
        nombre = updates.get("nombre")
        if nombre is None:
            return
        if not isinstance(nombre, str) or not nombre.strip():
            raise ValueError("El nombre no puede estar vacío.")
        query = "UPDATE ref.marca SET nombre = %s WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), marca_id))
                conn.commit()
                self.invalidate_catalog_cache("marcas")

    def delete_marcas(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        query = "DELETE FROM ref.marca WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()
                self.invalidate_catalog_cache("marcas")

    def fetch_rubros(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 80,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"""
            SELECT id, nombre
            FROM ref.rubro
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s
            OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_rubros(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.rubro WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_rubro(self, nombre: str) -> int:
        if not isinstance(nombre, str) or not nombre.strip():
            raise ValueError("El nombre no puede estar vacío.")
        query = "INSERT INTO ref.rubro (nombre) VALUES (%s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(),))
                res = cur.fetchone()
                conn.commit()
                self.invalidate_catalog_cache("rubros")
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_rubro_fields(self, rubro_id: int, updates: Dict[str, Any]) -> None:
        nombre = updates.get("nombre")
        if nombre is None:
            return
        if not isinstance(nombre, str) or not nombre.strip():
            raise ValueError("El nombre no puede estar vacío.")
        query = "UPDATE ref.rubro SET nombre = %s WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), rubro_id))
                conn.commit()
                self.invalidate_catalog_cache("rubros")

    def delete_rubros(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        query = "DELETE FROM ref.rubro WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()
                self.invalidate_catalog_cache("rubros")

    def count_stock_alerts(self, search: Optional[str] = None) -> int:
        filters: List[str] = ["COALESCE(stock_actual, 0) < COALESCE(stock_minimo, 0)"]
        params: List[Any] = []
        if search:
            filters.append("lower(nombre) LIKE %s")
            params.append(f"%{search.strip().lower()}%")
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM app.v_articulo_detallado WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_entity(
        self,
        *,
        nombre: Optional[str] = None,
        apellido: Optional[str] = None,
        razon_social: Optional[str] = None,
        cuit: Optional[str] = None,
        telefono: Optional[str] = None,
        email: Optional[str] = None,
        domicilio: Optional[str] = None,
        tipo: Optional[str] = None,
        activo: bool = True,
        notas: Optional[str] = None,
    ) -> int:
        def clean(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str):
                trimmed = value.strip()
                return trimmed if trimmed else None
            trimmed = str(value).strip()
            return trimmed if trimmed else None

        nombre_clean = clean(nombre)
        apellido_clean = clean(apellido)
        razon_social_clean = clean(razon_social)
        if not any([nombre_clean, apellido_clean, razon_social_clean]):
            raise ValueError("Completá al menos nombre/apellido o razón social.")

        tipo_clean = clean(tipo)
        if tipo_clean is not None:
            tipo_clean = tipo_clean.upper()
            if tipo_clean not in {"CLIENTE", "PROVEEDOR", "AMBOS"}:
                raise ValueError("Tipo inválido (usa CLIENTE/PROVEEDOR/AMBOS).")

        query = """
            INSERT INTO app.entidad_comercial (
                nombre,
                apellido,
                razon_social,
                cuit,
                telefono,
                email,
                domicilio,
                tipo,
                activo,
                notas
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(
                    query,
                    (
                        nombre_clean,
                        apellido_clean,
                        razon_social_clean,
                        clean(cuit),
                        clean(telefono),
                        clean(email),
                        clean(domicilio),
                        tipo_clean,
                        bool(activo),
                        clean(notas),
                    ),
                )
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_client_list_data(self, entity_id: int, list_id: Optional[int], discount: float = 0, credit_limit: float = 0) -> None:
        if list_id is None or list_id == "" or str(list_id) == "":
            query = "DELETE FROM app.lista_cliente WHERE id_entidad_comercial = %s"
            params = (entity_id,)
        else:
            query = """
                INSERT INTO app.lista_cliente (id_entidad_comercial, id_lista_precio, descuento, limite_credito)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_entidad_comercial) DO UPDATE SET
                    id_lista_precio = EXCLUDED.id_lista_precio,
                    descuento = EXCLUDED.descuento,
                    limite_credito = EXCLUDED.limite_credito
            """
            params = (entity_id, int(list_id), discount, credit_limit)
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()


    def update_entity_fields(self, entity_id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "apellido", "razon_social", "cuit", "domicilio", "telefono", "email", "activo", "tipo"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered:
            return

        if "activo" in filtered and isinstance(filtered["activo"], str):
            raw = filtered["activo"].strip().lower()
            if raw in {"1", "true", "si", "sí", "activo", "yes"}:
                filtered["activo"] = True
            elif raw in {"0", "false", "no", "inactivo"}:
                filtered["activo"] = False
            else:
                raise ValueError("Valor inválido para activo (usa true/false).")
        assignments = ", ".join(f"{col} = %s" for col in filtered)
        params = [filtered[col] for col in filtered]
        params.append(entity_id)
        query = f"UPDATE app.entidad_comercial SET {assignments} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def bulk_update_entities(self, ids: Sequence[int], updates: Dict[str, Any]) -> None:
        for entity_id in ids:
            self.update_entity_fields(entity_id, updates)

    def delete_entities(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        query = "DELETE FROM app.entidad_comercial WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    def create_article(
        self,
        *,
        nombre: str,
        marca: Optional[str] = None,
        rubro: Optional[str] = None,
        costo: Any = 0,
        stock_minimo: Any = 0,
        ubicacion: Optional[str] = None,
        activo: bool = True,
        id_tipo_iva: Optional[int] = None,
        id_unidad_medida: Optional[int] = None,
        id_proveedor: Optional[int] = None,
        observacion: Optional[str] = None,
        descuento_base: Any = 0,
        redondeo: bool = False,
    ) -> int:
        def clean(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str):
                trimmed = value.strip()
                return trimmed if trimmed else None
            trimmed = str(value).strip()
            return trimmed if trimmed else None

        def coerce_non_negative_number(value: Any, field: str) -> float:
            if value in (None, ""):
                return 0.0
            if isinstance(value, (int, float)):
                number = float(value)
            elif isinstance(value, str):
                raw = value.strip().replace(",", ".")
                if not raw:
                    return 0.0
                try:
                    number = float(raw)
                except Exception as exc:
                    raise ValueError(f"Valor inválido para {field}.") from exc
            else:
                raise ValueError(f"Valor inválido para {field}.")

            if number < 0:
                raise ValueError(f"{field} no puede ser negativo.")
            return number

        nombre_clean = clean(nombre)
        if not nombre_clean:
            raise ValueError("El nombre del artículo no puede estar vacío.")

        marca_name = clean(marca)
        rubro_name = clean(rubro)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                marca_id: Optional[int] = None
                if marca_name:
                    cur.execute("SELECT id FROM ref.marca WHERE nombre = %s", (marca_name,))
                    res = cur.fetchone()
                    marca_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                    if marca_id is None:
                        raise ValueError("Marca inválida.")

                rubro_id: Optional[int] = None
                if rubro_name:
                    cur.execute("SELECT id FROM ref.rubro WHERE nombre = %s", (rubro_name,))
                    res = cur.fetchone()
                    rubro_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                    if rubro_id is None:
                        raise ValueError("Rubro inválido.")

                costo_value = coerce_non_negative_number(costo, "costo")
                stock_minimo_value = coerce_non_negative_number(stock_minimo, "stock mínimo")
                descuento_base_value = coerce_non_negative_number(descuento_base, "descuento base")

                query = """
                    INSERT INTO app.articulo (
                        nombre,
                        id_marca,
                        id_rubro,
                        costo,
                        stock_minimo,
                        ubicacion,
                        activo,
                        id_tipo_iva,
                        id_unidad_medida,
                        id_proveedor,
                        observacion,
                        descuento_base,
                        redondeo
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cur.execute(
                    query,
                    (
                        nombre_clean,
                        marca_id,
                        rubro_id,
                        costo_value,
                        stock_minimo_value,
                        clean(ubicacion),
                        bool(activo),
                        id_tipo_iva,
                        id_unidad_medida,
                        id_proveedor,
                        clean(observacion),
                        descuento_base_value,
                        bool(redondeo)
                    ),
                )
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_article_fields(self, article_id: int, updates: Dict[str, Any]) -> None:
        allowed = {
            "nombre", "costo", "stock_minimo", "activo", 
            "marca", "rubro", "ubicacion", "observacion",
            "id_tipo_iva", "id_unidad_medida", "id_proveedor",
            "descuento_base", "redondeo"
        }
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered:
            return

        def coerce_float(value: Any, field: str) -> Optional[float]:
            if value in (None, ""):
                return None
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                raw = value.strip().replace(",", ".")
                if not raw:
                    return None
                try:
                    return float(raw)
                except Exception as exc:
                    raise ValueError(f"Valor inválido para {field}.") from exc
            raise ValueError(f"Valor inválido para {field}.")

        if "costo" in filtered:
            filtered["costo"] = coerce_float(filtered["costo"], "costo")
        if "stock_minimo" in filtered:
            filtered["stock_minimo"] = coerce_float(filtered["stock_minimo"], "stock_minimo")
        if "descuento_base" in filtered:
            filtered["descuento_base"] = coerce_float(filtered["descuento_base"], "descuento_base")
        if "activo" in filtered and isinstance(filtered["activo"], str):
            raw = filtered["activo"].strip().lower()
            if raw in {"1", "true", "si", "sí", "activo", "yes"}:
                filtered["activo"] = True
            elif raw in {"0", "false", "no", "inactivo"}:
                filtered["activo"] = False
            else:
                raise ValueError("Valor inválido para activo (usa true/false).")
        
        assignments: List[str] = []
        params: List[Any] = []

        if "marca" in filtered:
            marca = filtered.pop("marca")
            if marca in (None, ""):
                assignments.append("id_marca = NULL")
            else:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM ref.marca WHERE nombre = %s", (str(marca),))
                        res = cur.fetchone()
                        marca_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                if marca_id is None:
                    raise ValueError("Marca inválida.")
                assignments.append("id_marca = %s")
                params.append(marca_id)

        if "rubro" in filtered:
            rubro = filtered.pop("rubro")
            if rubro in (None, ""):
                assignments.append("id_rubro = NULL")
            else:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM ref.rubro WHERE nombre = %s", (str(rubro),))
                        res = cur.fetchone()
                        rubro_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                if rubro_id is None:
                    raise ValueError("Rubro inválido.")
                assignments.append("id_rubro = %s")
                params.append(rubro_id)

        for col, value in filtered.items():
            assignments.append(f"{col} = %s")
            params.append(value)

        if not assignments:
            return
        params.append(article_id)
        query = f"UPDATE app.articulo SET {', '.join(assignments)} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def fetch_article_by_id(self, article_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT 
                a.id, a.nombre, a.id_marca, m.nombre as marca_nombre,
                a.id_rubro, r.nombre as rubro_nombre,
                a.costo, a.stock_minimo, a.ubicacion, a.activo, a.observacion,
                a.id_tipo_iva, a.id_unidad_medida, a.id_proveedor,
                a.descuento_base, a.redondeo
            FROM app.articulo a
            LEFT JOIN ref.marca m ON a.id_marca = m.id
            LEFT JOIN ref.rubro r ON a.id_rubro = r.id
            WHERE a.id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(article_id),))
                rows = _rows_to_dicts(cur)
                return rows[0] if rows else None

    def fetch_article_prices(self, article_id: int) -> List[Dict[str, Any]]:
        query = """
            SELECT lp.id as id_lista_precio, lp.nombre as lista_nombre,
                   ap.precio, ap.porcentaje, ap.id_tipo_porcentaje
            FROM ref.lista_precio lp
            LEFT JOIN app.articulo_precio ap ON lp.id = ap.id_lista_precio AND ap.id_articulo = %s
            WHERE lp.activa = True
            ORDER BY lp.orden ASC
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                return _rows_to_dicts(cur)

    def update_article_prices(self, article_id: int, prices: List[Dict[str, Any]]) -> None:
        """
        prices: list of dicts with {id_lista_precio, precio, porcentaje, id_tipo_porcentaje}
        """
        query = """
            INSERT INTO app.articulo_precio (id_articulo, id_lista_precio, precio, porcentaje, id_tipo_porcentaje, fecha_actualizacion)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (id_articulo, id_lista_precio) DO UPDATE SET
                precio = EXCLUDED.precio,
                porcentaje = EXCLUDED.porcentaje,
                id_tipo_porcentaje = EXCLUDED.id_tipo_porcentaje,
                fecha_actualizacion = now()
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                for p in prices:
                    cur.execute(query, (
                        article_id,
                        p["id_lista_precio"],
                        p.get("precio"),
                        p.get("porcentaje"),
                        p.get("id_tipo_porcentaje")
                    ))
                conn.commit()

    def list_proveedores(self) -> List[Dict[str, Any]]:
        query = """
            SELECT id, COALESCE(razon_social, TRIM(COALESCE(apellido, '') || ' ' || COALESCE(nombre, ''))) as nombre 
            FROM app.entidad_comercial 
            WHERE tipo IN ('PROVEEDOR', 'AMBOS') AND activo = true
            ORDER BY nombre
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def bulk_update_articles(self, ids: Sequence[int], updates: Dict[str, Any]) -> None:
        for article_id in ids:
            self.update_article_fields(article_id, updates)

    def delete_articles(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        query = "DELETE FROM app.articulo WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # Provinces
    def fetch_provincias(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre FROM ref.provincia WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_provincias(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.provincia WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_provincia(self, nombre: str) -> int:
        query = "INSERT INTO ref.provincia (nombre) VALUES (%s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(),))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_provincia_fields(self, id: int, updates: Dict[str, Any]) -> None:
        if "nombre" not in updates: return
        query = "UPDATE ref.provincia SET nombre = %s WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (updates["nombre"].strip(), id))
                conn.commit()

    def delete_provincias(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.provincia WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    def list_provincias(self) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre FROM ref.provincia ORDER BY nombre"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    # Localities
    def fetch_localidades(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(lower(l.nombre) LIKE %s OR lower(p.nombre) LIKE %s)")
            params.extend([f"%{search.lower()}%"] * 2)
        where_clause = " AND ".join(filters)
        sort_columns = {"id": "l.id", "nombre": "l.nombre", "provincia": "p.nombre"}
        order_by = self._build_order_by(sorts, sort_columns, default="l.nombre ASC", tiebreaker="l.id ASC")
        query = f"""
            SELECT l.id, l.nombre, l.id_provincia, p.nombre as provincia
            FROM ref.localidad l
            JOIN ref.provincia p ON l.id_provincia = p.id
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_localidades(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(lower(l.nombre) LIKE %s OR lower(p.nombre) LIKE %s)")
            params.extend([f"%{search.lower()}%"] * 2)
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.localidad l JOIN ref.provincia p ON l.id_provincia = p.id WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_localidad(self, nombre: str, id_provincia: int) -> int:
        query = "INSERT INTO ref.localidad (nombre, id_provincia) VALUES (%s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), id_provincia))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_localidad_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "id_provincia"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.localidad SET {set_clause} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def delete_localidades(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.localidad WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # Units of Measure
    def fetch_unidades_medida(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre", "abreviatura": "abreviatura"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre, abreviatura FROM ref.unidad_medida WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_unidades_medida(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.unidad_medida WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_unidad_medida(self, nombre: str, abreviatura: str) -> int:
        query = "INSERT INTO ref.unidad_medida (nombre, abreviatura) VALUES (%s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), abreviatura.strip()))
                res = cur.fetchone()
                conn.commit()
                self.invalidate_catalog_cache("unidades")
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_unidad_medida_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "abreviatura"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.unidad_medida SET {set_clause} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def delete_unidades_medida(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.unidad_medida WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # IVA Conditions
    def fetch_condiciones_iva(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre FROM ref.condicion_iva WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_condiciones_iva(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.condicion_iva WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_condicion_iva(self, nombre: str) -> int:
        query = "INSERT INTO ref.condicion_iva (nombre) VALUES (%s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(),))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_condicion_iva_fields(self, id: int, updates: Dict[str, Any]) -> None:
        if "nombre" not in updates: return
        query = "UPDATE ref.condicion_iva SET nombre = %s WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (updates["nombre"].strip(), id))
                conn.commit()

    def delete_condiciones_iva(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.condicion_iva WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (list(ids),))
                conn.commit()

    # IVA Types
    def fetch_tipos_iva(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "codigo": "codigo", "porcentaje": "porcentaje", "descripcion": "descripcion"}
        order_by = self._build_order_by(sorts, sort_columns, default="porcentaje ASC", tiebreaker="id ASC")
        query = f"SELECT id, codigo, porcentaje, descripcion FROM ref.tipo_iva WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_tipos_iva(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_iva WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_tipo_iva(self, codigo: int, porcentaje: float, descripcion: str) -> int:
        query = "INSERT INTO ref.tipo_iva (codigo, porcentaje, descripcion) VALUES (%s, %s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (codigo, porcentaje, descripcion.strip()))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_tipo_iva_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"codigo", "porcentaje", "descripcion"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.tipo_iva SET {set_clause} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def delete_tipos_iva(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.tipo_iva WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # Deposits
    def fetch_depositos(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre", "ubicacion": "ubicacion", "activo": "activo"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre, ubicacion, activo FROM ref.deposito WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_depositos(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.deposito WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_deposito(self, nombre: str, ubicacion: str, activo: bool = True) -> int:
        query = "INSERT INTO ref.deposito (nombre, ubicacion, activo) VALUES (%s, %s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), ubicacion.strip(), activo))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_deposito_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "ubicacion", "activo"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.deposito SET {set_clause} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def delete_depositos(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.deposito WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # Payment Methods
    def fetch_formas_pago(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "descripcion": "descripcion", "activa": "activa"}
        order_by = self._build_order_by(sorts, sort_columns, default="descripcion ASC", tiebreaker="id ASC")
        query = f"SELECT id, descripcion, activa FROM ref.forma_pago WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_formas_pago(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.forma_pago WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_forma_pago(self, descripcion: str, activa: bool = True) -> int:
        query = "INSERT INTO ref.forma_pago (descripcion, activa) VALUES (%s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (descripcion.strip(), activa))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_forma_pago_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"descripcion", "activa"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.forma_pago SET {set_clause} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def delete_formas_pago(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.forma_pago WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # Price Lists
    def fetch_listas_precio(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search)
        sort_columns = {"id": "id", "nombre": "nombre", "activa": "activa", "orden": "orden"}
        order_by = self._build_order_by(sorts, sort_columns, default="orden ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre, activa, orden FROM ref.lista_precio WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_listas_precio(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.lista_precio WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_lista_precio(self, nombre: str, activa: bool = True, orden: int = 0) -> int:
        query = "INSERT INTO ref.lista_precio (nombre, activa, orden) VALUES (%s, %s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), activa, orden))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def update_lista_precio_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "activa", "orden"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.lista_precio SET {set_clause} WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def delete_listas_precio(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.lista_precio WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (list(ids),))
                conn.commit()

    # Logs
    def fetch_logs(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(u.nombre) LIKE %s OR lower(l.entidad) LIKE %s OR lower(l.accion) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 3)
        
        user = advanced.get("usuario")
        if user:
            filters.append("lower(u.nombre) LIKE %s")
            params.append(f"%{user.lower().strip()}%")
        
        ent = advanced.get("entidad")
        if ent:
            filters.append("lower(l.entidad) LIKE %s")
            params.append(f"%{ent.lower().strip()}%")
        
        acc = advanced.get("accion")
        if acc:
            filters.append("lower(l.accion) LIKE %s")
            params.append(f"%{acc.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("l.fecha_hora::date >= %s")
            params.append(desde)

        where_clause = " AND ".join(filters)
        sort_columns = {"id": "l.id", "fecha": "l.fecha_hora", "usuario": "u.nombre", "entidad": "l.entidad", "accion": "l.accion", "resultado": "l.resultado"}
        order_by = self._build_order_by(sorts, sort_columns, default="l.fecha_hora DESC", tiebreaker="l.id DESC")
        query = f"""
            SELECT l.id, l.fecha_hora as fecha, u.nombre as usuario, l.entidad, l.id_entidad, l.accion, l.resultado, l.ip, l.detalle
            FROM seguridad.log_actividad l
            LEFT JOIN seguridad.usuario u ON l.id_usuario = u.id
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_logs(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(u.nombre) LIKE %s OR lower(l.entidad) LIKE %s OR lower(l.accion) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 3)
        
        user = advanced.get("usuario")
        if user:
            filters.append("lower(u.nombre) LIKE %s")
            params.append(f"%{user.lower().strip()}%")
        
        ent = advanced.get("entidad")
        if ent:
            filters.append("lower(l.entidad) LIKE %s")
            params.append(f"%{ent.lower().strip()}%")
        
        acc = advanced.get("accion")
        if acc:
            filters.append("lower(l.accion) LIKE %s")
            params.append(f"%{acc.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("l.fecha_hora::date >= %s")
            params.append(desde)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM seguridad.log_actividad l LEFT JOIN seguridad.usuario u ON l.id_usuario = u.id WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None: return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    def create_tipo_porcentaje(self, tipo: str) -> int:
        query = "INSERT INTO ref.tipo_porcentaje (tipo) VALUES (%s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (tipo.strip(),))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def create_tipo_documento(self, nombre: str, clase: str, letra: str, afecta_stock: bool, afecta_cta: bool) -> int:
        query = """
            INSERT INTO ref.tipo_documento (nombre, clase, letra, afecta_stock, afecta_cuenta_corriente)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), clase.strip(), letra.strip(), afecta_stock, afecta_cta))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def create_tipo_movimiento_articulo(self, nombre: str, signo: int) -> int:
        query = "INSERT INTO ref.tipo_movimiento_articulo (nombre, signo_stock) VALUES (%s, %s) RETURNING id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), signo))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    # Percentage Types
    def fetch_tipos_porcentaje(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("lower(tipo) LIKE %s")
            params.append(f"%{search.lower().strip()}%")
        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "tipo": "tipo"}
        order_by = self._build_order_by(sorts, sort_columns, default="tipo ASC", tiebreaker="id ASC")
        query = f"SELECT id, tipo FROM ref.tipo_porcentaje WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_tipos_porcentaje(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("lower(tipo) LIKE %s")
            params.append(f"%{search.lower().strip()}%")
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_porcentaje WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    # Document Types
    def fetch_tipos_documento(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(lower(nombre) LIKE %s OR lower(clase) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "nombre": "nombre", "clase": "clase", "letra": "letra"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre, clase, afecta_stock, afecta_cuenta_corriente, codigo_afip, letra FROM ref.tipo_documento WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_tipos_documento(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(lower(nombre) LIKE %s OR lower(clase) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_documento WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    # Article Movement Types
    def fetch_tipos_movimiento_articulo(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("lower(nombre) LIKE %s")
            params.append(f"%{search.lower().strip()}%")
        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "nombre": "nombre", "signo_stock": "signo_stock"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC", tiebreaker="id ASC")
        query = f"SELECT id, nombre, signo_stock FROM ref.tipo_movimiento_articulo WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_tipos_movimiento_articulo(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("lower(nombre) LIKE %s")
            params.append(f"%{search.lower().strip()}%")
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_movimiento_articulo WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    # Security: Users and Roles
    def fetch_users(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 40, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(lower(nombre) LIKE %s OR lower(email) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "nombre": "nombre", "email": "email", "rol": "rol"}
        order_by = self._build_order_by(sorts, sort_columns, default="nombre ASC")
        query = f"SELECT * FROM seguridad.v_usuario_publico WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_users(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(lower(nombre) LIKE %s OR lower(email) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM seguridad.usuario WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    def list_roles(self) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre FROM seguridad.rol ORDER BY nombre"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def update_user_fields(self, user_id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "email", "id_rol", "activo"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = list(filtered.values())
        params.append(user_id)
        query = f"UPDATE seguridad.usuario SET {set_clause}, fecha_actualizacion = now() WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                conn.commit()

    def create_user(self, nombre: str, email: str, contrasena: str, id_rol: int) -> int:
        query = """
            INSERT INTO seguridad.usuario (nombre, email, contrasena_hash, id_rol)
            VALUES (%s, %s, crypt(%s, gen_salt('bf', 12)), %s) RETURNING id
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (nombre.strip(), email.strip(), contrasena, id_rol))
                res = cur.fetchone()
                conn.commit()
                return res.get("id") if isinstance(res, dict) else res[0]

    def fetch_roles(self) -> List[Dict[str, Any]]:
        """Fetch all available roles for user creation."""
        query = "SELECT id, nombre FROM seguridad.rol ORDER BY id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def fetch_active_sessions(self, search: str = "", limit: int = 10, offset: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """Fetch users who have logged in recently. Approximate active status."""
        query = """
            WITH last_states AS (
                SELECT 
                    id_usuario,
                    accion,
                    fecha_hora,
                    ip,
                    ROW_NUMBER() OVER (PARTITION BY id_usuario ORDER BY fecha_hora DESC) as rn
                FROM seguridad.log_actividad
                WHERE id_usuario IS NOT NULL
                  AND accion IN ('LOGIN_OK', 'LOGOUT')
                  AND fecha_hora > NOW() - INTERVAL '24 hours'
            )
            SELECT 
                u.id,
                u.nombre,
                u.email,
                l.fecha_hora as desde,
                l.ip,
                r.nombre as rol
            FROM last_states l
            JOIN seguridad.usuario u ON l.id_usuario = u.id
            JOIN seguridad.rol r ON u.id_rol = r.id
            WHERE l.rn = 1 
              AND l.accion = 'LOGIN_OK'
        """
        params = []
        if search and search.strip():
            query += " AND (u.nombre ILIKE %s OR u.email ILIKE %s)"
            s = f"%{search.strip()}%"
            params.extend([s, s])
            
        query += " ORDER BY l.fecha_hora DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_active_sessions(self, search: str = "", **kwargs) -> int:
        """Count active sessions with optional filtering."""
        query = """
            WITH last_states AS (
                SELECT 
                    id_usuario,
                    accion,
                    ROW_NUMBER() OVER (PARTITION BY id_usuario ORDER BY fecha_hora DESC) as rn
                FROM seguridad.log_actividad
                WHERE id_usuario IS NOT NULL
                  AND accion IN ('LOGIN_OK', 'LOGOUT')
                  AND fecha_hora > NOW() - INTERVAL '24 hours'
            )
            SELECT COUNT(*) 
            FROM last_states l
            JOIN seguridad.usuario u ON l.id_usuario = u.id
            JOIN seguridad.rol r ON u.id_rol = r.id
            WHERE l.rn = 1 
              AND l.accion = 'LOGIN_OK'
        """
        params = []
        if search and search.strip():
            query += " AND (u.nombre ILIKE %s OR u.email ILIKE %s)"
            s = f"%{search.strip()}%"
            params.extend([s, s])
            
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                return res.get("count") if isinstance(res, dict) else res[0]

    # Backup Config
    def fetch_backup_config(self) -> Dict[str, Any]:
        query = "SELECT * FROM seguridad.backup_config LIMIT 1"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                res = cur.fetchone()
                return res if isinstance(res, dict) else (dict(zip([d[0] for d in cur.description], res)) if res else {})

    def update_backup_config(self, updates: Dict[str, Any]) -> None:
        allowed = {"frecuencia", "hora", "destino_local", "retencion_dias"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = list(filtered.values())
        query = f"UPDATE seguridad.backup_config SET {set_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()

    def run_manual_backup(self, target_dir: str) -> Tuple[bool, str]:
        """Runs a manual backup using pg_dump.exe."""
        import subprocess
        import os
        from urllib.parse import urlparse, unquote
        
        try:
            # Parse DSN
            parsed = urlparse(self.dsn)
            
            dbname = parsed.path.lstrip('/')
            user = unquote(parsed.username or "postgres")
            password = unquote(parsed.password or "")
            host = parsed.hostname or "localhost"
            port = str(parsed.port or 5432)
            
            # Ensure target directory exists
            if not os.path.exists(target_dir):
                try:
                    os.makedirs(target_dir, exist_ok=True)
                except Exception as de:
                    return False, f"No se pudo crear el directorio de destino: {de}"
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backup_{dbname}_{timestamp}.sql"
            filepath = os.path.join(target_dir, filename)
            
            # Path to pg_dump (verified in system)
            pg_dump_path = r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"
            
            if not os.path.exists(pg_dump_path):
                # Fallback to simple pg_dump if in PATH
                pg_dump_path = "pg_dump"
                
            # Prepare environment with password
            env = os.environ.copy()
            env["PGPASSWORD"] = password
            
            cmd = [
                pg_dump_path,
                "-h", host,
                "-p", port,
                "-U", user,
                "-F", "p", # Plain text script
                "-f", filepath,
                dbname
            ]
            
            process = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if process.returncode == 0:
                self.log_activity("BACKUP", "CREATE", detalle={"file": filename, "path": target_dir})
                return True, f"Respaldo creado con éxito: {filename}"
            else:
                error_msg = process.stderr.strip()
                if not error_msg and process.stdout:
                    error_msg = process.stdout.strip()
                return False, f"Error ejecuando pg_dump: {error_msg or 'Error desconocido'}"
                
        except Exception as e:
            return False, f"Error excepcional durante el respaldo: {str(e)}"


    # System Configuration
    def fetch_config_sistema(self) -> Dict[str, Any]:
        """Fetch all system configuration as a dictionary keyed by 'clave'."""
        query = "SELECT clave, valor, tipo, descripcion FROM seguridad.config_sistema"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = _rows_to_dicts(cur)
                return {r["clave"]: {"valor": r["valor"], "tipo": r["tipo"], "descripcion": r["descripcion"]} for r in rows}

    def get_config_value(self, clave: str) -> Optional[str]:
        """Get a single configuration value by key."""
        query = "SELECT valor FROM seguridad.config_sistema WHERE clave = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (clave,))
                res = cur.fetchone()
                if res is None:
                    return None
                return res["valor"] if isinstance(res, dict) else res[0]

    def update_config_sistema(self, clave: str, valor: str) -> None:
        """Update a single configuration value."""
        query = """
            INSERT INTO seguridad.config_sistema (clave, valor)
            VALUES (%s, %s)
            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (clave, valor))
                conn.commit()

    def update_config_sistema_bulk(self, updates: Dict[str, str]) -> None:
        """Update multiple configuration values at once."""
        if not updates:
            return
        query = """
            INSERT INTO seguridad.config_sistema (clave, valor)
            VALUES (%s, %s)
            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for clave, valor in updates.items():
                    cur.execute(query, (clave, valor))
                conn.commit()

    # Documents Resumen
    def fetch_documentos_resumen(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 60, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(entidad) LIKE %s OR lower(tipo_documento) LIKE %s OR numero_serie LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 3)
        
        ent = advanced.get("entidad")
        if ent:
            filters.append("lower(entidad) LIKE %s")
            params.append(f"%{ent.lower().strip()}%")
        
        tipo = advanced.get("tipo")
        if tipo:
            filters.append("lower(tipo_documento) LIKE %s")
            params.append(f"%{tipo.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)
        
        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)

        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "fecha": "fecha", "tipo_documento": "tipo_documento", "entidad": "entidad", "total": "total", "estado": "estado"}
        order_by = self._build_order_by(sorts, sort_columns, default="fecha DESC")
        query = f"SELECT * FROM app.v_documento_resumen WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_documentos_resumen(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(entidad) LIKE %s OR lower(tipo_documento) LIKE %s OR numero_serie LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 3)
            
        ent = advanced.get("entidad")
        if ent:
            filters.append("lower(entidad) LIKE %s")
            params.append(f"%{ent.lower().strip()}%")
        
        tipo = advanced.get("tipo")
        if tipo:
            filters.append("lower(tipo_documento) LIKE %s")
            params.append(f"%{tipo.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)
        
        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM app.v_documento_resumen WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None: return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    # Stock Movements
    def fetch_movimientos_stock(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(articulo) LIKE %s OR lower(tipo_movimiento) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
        
        art = advanced.get("articulo")
        if art:
            filters.append("lower(articulo) LIKE %s")
            params.append(f"%{art.lower().strip()}%")
        
        tipo = advanced.get("tipo")
        if tipo:
            filters.append("lower(tipo_movimiento) LIKE %s")
            params.append(f"%{tipo.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha::date >= %s")
            params.append(desde)

        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "fecha": "fecha", "articulo": "articulo", "cantidad": "cantidad", "deposito": "deposito"}
        order_by = self._build_order_by(sorts, sort_columns, default="fecha DESC")
        query = f"SELECT * FROM app.v_movimientos_full WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_movimientos_stock(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(articulo) LIKE %s OR lower(tipo_movimiento) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
            
        art = advanced.get("articulo")
        if art:
            filters.append("lower(articulo) LIKE %s")
            params.append(f"%{art.lower().strip()}%")
        
        tipo = advanced.get("tipo")
        if tipo:
            filters.append("lower(tipo_movimiento) LIKE %s")
            params.append(f"%{tipo.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha::date >= %s")
            params.append(desde)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM app.v_movimientos_full WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None: return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    # Payments
    def fetch_pagos(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 60, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(p.referencia) LIKE %s OR lower(fp.descripcion) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
        
        ref = advanced.get("referencia")
        if ref:
            filters.append("lower(p.referencia) LIKE %s")
            params.append(f"%{ref.lower().strip()}%")
        
        forma = advanced.get("forma")
        if forma:
            filters.append("lower(fp.descripcion) LIKE %s")
            params.append(f"%{forma.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("p.fecha::date >= %s")
            params.append(desde)

        where_clause = " AND ".join(filters)
        sort_columns = {"id": "p.id", "fecha": "p.fecha", "monto": "p.monto", "forma": "fp.descripcion"}
        order_by = self._build_order_by(sorts, sort_columns, default="p.fecha DESC")
        query = f"""
            SELECT p.*, fp.descripcion as forma, d.numero_serie as documento
            FROM app.pago p
            JOIN ref.forma_pago fp ON p.id_forma_pago = fp.id
            JOIN app.documento d ON p.id_documento = d.id
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_pagos(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(lower(p.referencia) LIKE %s OR lower(fp.descripcion) LIKE %s)")
            params.extend([f"%{search.lower().strip()}%"] * 2)
            
        ref = advanced.get("referencia")
        if ref:
            filters.append("lower(p.referencia) LIKE %s")
            params.append(f"%{ref.lower().strip()}%")
        
        forma = advanced.get("forma")
        if forma:
            filters.append("lower(fp.descripcion) LIKE %s")
            params.append(f"%{forma.lower().strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("p.fecha::date >= %s")
            params.append(desde)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM app.pago p JOIN ref.forma_pago fp ON p.id_forma_pago = fp.id WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None: return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    def fetch_documento_detalle(self, documento_id: int) -> List[Dict[str, Any]]:
        query = """
            SELECT dd.*, a.nombre as articulo
            FROM app.documento_detalle dd
            JOIN app.articulo a ON dd.id_articulo = a.id
            WHERE dd.id_documento = %s
            ORDER BY dd.nro_linea
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (documento_id,))
                return _rows_to_dicts(cur)

    def create_document(self, *, id_tipo_documento: int, id_entidad_comercial: int, id_deposito: int, 
                        items: List[Dict[str, Any]], observacion: Optional[str] = None, 
                        numero_serie: Optional[str] = None, descuento_porcentaje: float = 0) -> int:
        """
        items: list of {id_articulo, cantidad, precio_unitario, porcentaje_iva}
        """
        header_query = """
            INSERT INTO app.documento (
                id_tipo_documento, id_entidad_comercial, id_deposito, 
                observacion, numero_serie, descuento_porcentaje, id_usuario,
                neto, subtotal, iva_total, total, fecha
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            RETURNING id
        """

        detail_query = """
            INSERT INTO app.documento_detalle (
                id_documento, nro_linea, id_articulo, cantidad, 
                precio_unitario, porcentaje_iva, total_linea
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Calculate totals
        neto_total = 0
        iva_total = 0
        for item in items:
            sub = float(item["cantidad"]) * float(item["precio_unitario"])
            neto_total += sub
            iva_total += sub * (float(item["porcentaje_iva"]) / 100.0)
        
        # Apply header discount to NETO? Usually it's on subtotal. Let's keep it simple for now.
        subtotal = neto_total
        desc_val = subtotal * (float(descuento_porcentaje) / 100.0)
        total = (subtotal - desc_val) + iva_total

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                # Header
                cur.execute(header_query, (
                    id_tipo_documento, id_entidad_comercial, id_deposito,
                    observacion, numero_serie, descuento_porcentaje, self.current_user_id,
                    neto_total, subtotal, iva_total, total
                ))
                res = cur.fetchone()
                doc_id = res[0] if isinstance(res, (list, tuple)) else res["id"]
                
                # Details
                for i, item in enumerate(items, 1):
                    line_total = float(item["cantidad"]) * float(item["precio_unitario"])
                    cur.execute(detail_query, (
                        doc_id, i, item["id_articulo"], item["cantidad"],
                        item["precio_unitario"], item["porcentaje_iva"], line_total
                    ))
                
                conn.commit()
                return doc_id

    def list_entidades_simple(self) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre_completo, tipo FROM app.v_entidad_detallada WHERE activo = True ORDER BY nombre_completo ASC"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def confirm_document(self, doc_id: int) -> None:
        """
        Confirms a document:
        1. Updates status to 'CONFIRMADO'.
        2. Generates stock movements if the document type affects stock.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                
                # Fetch doc info
                cur.execute("""
                    SELECT d.id_tipo_documento, d.id_deposito, td.clase, td.afecta_stock 
                    FROM app.documento d
                    JOIN ref.tipo_documento td ON d.id_tipo_documento = td.id
                    WHERE d.id = %s AND d.estado = 'BORRADOR'
                """, (doc_id,))
                doc = cur.fetchone()
                if not doc:
                    raise Exception("Comprobante no encontrado o ya confirmado.")
                
                # Update status
                cur.execute("UPDATE app.documento SET estado = 'CONFIRMADO' WHERE id = %s", (doc_id,))
                
                # Stock Movements
                # Extract results (assuming tuple-based return from fetchone in psycopg3)
                depo_id = doc[1] if isinstance(doc, (list, tuple)) else doc["id_deposito"]
                clase = doc[2] if isinstance(doc, (list, tuple)) else doc["clase"]
                afecta_stk = doc[3] if isinstance(doc, (list, tuple)) else doc["afecta_stock"]

                if afecta_stk:
                    id_tipo_mov = 2 if clase == 'VENTA' else 1 # Venta (-1) or Compra (+1)
                    
                    cur.execute("""
                        INSERT INTO app.movimiento_articulo (id_articulo, id_tipo_movimiento, cantidad, id_deposito, id_documento, observacion)
                        SELECT id_articulo, %s, cantidad, %s, %s, 'Confirmación de ' || %s
                        FROM app.documento_detalle
                        WHERE id_documento = %s
                    """, (id_tipo_mov, depo_id, doc_id, clase, doc_id))
                
                conn.commit()
