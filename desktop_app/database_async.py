"""
Async database layer for Flet application.

This module provides async versions of critical database operations to prevent
UI blocking when executing queries. It uses psycopg's AsyncConnectionPool.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg_pool import AsyncConnectionPool

from desktop_app.database import (
    _to_id,
    Database,
)

logger = logging.getLogger(__name__)


class AsyncDatabase:
    """
    Async wrapper over Database class using AsyncConnectionPool.
    
    This class provides async versions of heavy-lifting database operations
    that are typically called from UI event handlers in Flet.
    
    Heavy operations are those that:
    - Do complex queries with joins/aggregations
    - Return large datasets (>100 rows)
    - Are called during user-triggered actions (search, filter, load)
    
    Simple operations (config, logging) remain synchronous on the base Database.
    """
    
    def __init__(self, db: Database):
        """Initialize async wrapper from existing Database instance."""
        self.db = db
        self.dsn = db.dsn
        self.pool_min = db.pool_min
        self.pool_max = db.pool_max
        self._async_pool: Optional[AsyncConnectionPool] = None
        
    async def _get_pool(self) -> AsyncConnectionPool:
        """Lazily initialize async connection pool."""
        if self._async_pool is None:
            self._async_pool = AsyncConnectionPool(
                conninfo=self.dsn,
                min_size=self.pool_min,
                max_size=self.pool_max,
            )
        return self._async_pool
    
    async def close_async(self) -> None:
        """Close the async connection pool."""
        if self._async_pool:
            try:
                await self._async_pool.close()
            except Exception as e:
                logger.error(f"Error closing async pool: {e}")
            self._async_pool = None
    
    def set_context(self, user_id: Optional[int], ip: Optional[str] = None) -> None:
        """Set user context (delegates to sync DB)."""
        self.db.set_context(user_id, ip)
    
    async def _setup_session_async(self, cur: Any) -> None:
        """Setup session variables in cursor (async version)."""
        if self.db.current_user_id:
            await cur.execute(
                "SELECT set_config('app.user_id', %s, true)",
                (str(self.db.current_user_id),)
            )
        if self.db.current_ip:
            await cur.execute(
                "SELECT set_config('app.ip', %s, true)",
                (self.db.current_ip,)
            )
    
    # ========== CRITICAL FETCH OPERATIONS (ASYNC) ==========
    
    async def fetch_entities(
        self,
        search: Optional[str] = None,
        tipo: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 60,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Async version of fetch_entities - potentially heavy operation.
        Fetches entidades with filtering and pagination.
        """
        pool = await self._get_pool()
        
        # Build filters (using sync database's logic)
        where_clause, params = self.db._build_catalog_filters(
            search, "entidad", tipo
        )
        
        # Build sorting
        sort_columns = {
            "id": "id",
            "nombre_completo": "nombre_completo",
            "cuit": "cuit",
            "email": "email",
            "tipo": "tipo",
            "condicion_iva": "condicion_iva",
            "saldo_cuenta": "saldo_cuenta",
        }
        order_by = self.db._build_order_by(
            sorts, sort_columns, default="nombre_completo ASC", tiebreaker="id ASC"
        )
        
        # Apply advanced filters if provided
        filters = [where_clause] if where_clause else []
        if advanced:
            if advanced.get("condicion_iva"):
                filters.append("condicion_iva = %s")
                params.append(advanced["condicion_iva"])
            
            id_tipo = _to_id(advanced.get("id_tipo"))
            if id_tipo:
                filters.append("id_tipo = %s")
                params.append(id_tipo)
        
        where_clause = " AND ".join(filters) if filters else "TRUE"
        
        query = f"""
            SELECT id, COALESCE(apellido || ' ' || nombre, razon_social) as nombre_completo,
                   cuit, domicilio, email, tipo, condicion_iva, saldo_cuenta, activo
            FROM app.v_entidad_detallada
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s
            OFFSET %s
        """
        params.extend([limit, offset])
        
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                return _async_rows_to_dicts(rows, cur.description)
    
    async def fetch_documentos_resumen(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 60,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Async version of fetch_documentos_resumen - complex query with joins.
        """
        pool = await self._get_pool()
        
        filters = []
        params = []
        
        # Date filters
        if advanced:
            f_desde = advanced.get("f_desde")
            if f_desde:
                filters.append("fecha >= %s")
                params.append(f_desde)
            
            f_hasta = advanced.get("f_hasta")
            if f_hasta:
                filters.append("fecha <= %s")
                params.append(f_hasta)
            
            id_entidad = _to_id(advanced.get("id_entidad"))
            if id_entidad:
                filters.append("id_entidad = %s")
                params.append(id_entidad)
            
            numero = advanced.get("numero")
            if numero:
                filters.append("numero_serie ILIKE %s")
                params.append(f"%{numero.strip()}%")
        
        where_clause = " AND ".join(filters) if filters else "TRUE"
        
        sort_columns = {
            "id": "id",
            "fecha": "fecha",
            "numero": "numero_serie",
            "entidad": "entidad",
            "tipo": "tipo",
            "total": "total",
        }
        order_by = self.db._build_order_by(
            sorts, sort_columns, default="fecha DESC", tiebreaker="id DESC"
        )
        
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                return _async_rows_to_dicts(rows, cur.description)
    
    async def fetch_pagos(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 60,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Async version of fetch_pagos - heavy join operation.
        """
        pool = await self._get_pool()
        
        filters = []
        params = []
        
        if advanced:
            f_desde = advanced.get("f_desde")
            if f_desde:
                filters.append("p.fecha >= %s")
                params.append(f_desde)
            
            f_hasta = advanced.get("f_hasta")
            if f_hasta:
                filters.append("p.fecha <= %s")
                params.append(f_hasta)
        
        where_clause = " AND ".join(filters) if filters else "TRUE"
        
        sort_columns = {
            "id": "p.id",
            "fecha": "p.fecha",
            "monto": "p.monto",
            "forma": "forma",
        }
        order_by = self.db._build_order_by(
            sorts, sort_columns, default="p.fecha DESC"
        )
        
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                return _async_rows_to_dicts(rows, cur.description)
    
    async def fetch_articulos(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 60,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Async version of fetch_articulos - inventory heavy operation.
        """
        pool = await self._get_pool()
        
        where_clause, params = self.db._build_catalog_filters(search, "articulo", None)
        
        sort_columns = {
            "id": "a.id",
            "codigo": "a.codigo",
            "descripcion": "a.descripcion",
            "stock": "asr.stock_total",
            "precio": "a.precio_costo",
        }
        order_by = self.db._build_order_by(
            sorts, sort_columns, default="a.descripcion ASC", tiebreaker="a.id ASC"
        )
        
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                return _async_rows_to_dicts(rows, cur.description)
    
    async def count_results(
        self,
        table_view: str,
        search: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Async count for pagination - helps prevent UI freeze on large datasets.
        """
        pool = await self._get_pool()
        
        where_clause, params = self.db._build_catalog_filters(search, table_view, None)
        
        # Apply advanced filters
        filters = [where_clause] if where_clause else []
        if advanced and table_view == "documentos":
            f_desde = advanced.get("f_desde")
            if f_desde:
                filters.append("fecha >= %s")
                params.append(f_desde)
            
            f_hasta = advanced.get("f_hasta")
            if f_hasta:
                filters.append("fecha <= %s")
                params.append(f_hasta)
        
        where_clause = " AND ".join(filters) if filters else "TRUE"
        
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                row = await cur.fetchone()
                if row:
                    return row[0] if isinstance(row, tuple) else row.get("total", 0)
                return 0


def _async_rows_to_dicts(rows: List[tuple], description) -> List[Dict[str, Any]]:
    """
    Convert async cursor rows to list of dicts.
    Handles psycopg async cursor results.
    """
    if not rows or not description:
        return []
    
    # Get column names from description
    columns = []
    for col in description:
        if hasattr(col, "name"):
            columns.append(col.name)
        elif isinstance(col, (list, tuple)):
            columns.append(col[0])
        else:
            columns.append(str(col))
    
    return [dict(zip(columns, row)) for row in rows]
