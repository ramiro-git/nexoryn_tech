import csv
import json
import logging
import re
import threading
import time
import unicodedata
from decimal import Decimal
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg.errors import ForeignKeyViolation, IntegrityError
from psycopg_pool import ConnectionPool

try:
    from desktop_app.enums import (
        DocumentoEstado, RemotoEstado, BackupEstado, ClaseDocumento,
        DOCUMENTO_ESTADOS_CONFIRMADOS, DOCUMENTO_ESTADOS_PENDIENTES, DOCUMENTO_ESTADOS_ACTIVOS
    )
except ImportError:
    from enums import (  # type: ignore
        DocumentoEstado, RemotoEstado, BackupEstado, ClaseDocumento,
        DOCUMENTO_ESTADOS_CONFIRMADOS, DOCUMENTO_ESTADOS_PENDIENTES, DOCUMENTO_ESTADOS_ACTIVOS
    )

try:
    from desktop_app.services.document_pricing import calculate_document_totals
except ImportError:
    from services.document_pricing import calculate_document_totals  # type: ignore

logger = logging.getLogger(__name__)

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
GUEST_USER_NAME = "Invitado"
GUEST_USER_EMAIL = "invitado@nexoryn.local"
GUEST_USER_ROLE = "GERENTE"
_ACTIVITY_LOG_COLUMNS = [
    "id",
    "fecha_hora",
    "id_usuario",
    "entidad",
    "id_entidad",
    "accion",
    "resultado",
    "ip",
    "user_agent",
    "session_id",
    "detalle",
]

def _rows_to_dicts(cursor) -> List[Dict[str, Any]]:
    columns = [
        col.name if hasattr(col, "name") else col[0]
        for col in cursor.description
    ]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def _to_id(val: Any) -> Optional[int]:
    """Safely convert a filter value to an integer ID."""
    if val in (None, "", "Todas", "Todos", "---"):
        return None
    if isinstance(val, int):
        return val
    try:
        raw = str(val).strip()
    except Exception:
        return None
    if raw in ("", "Todas", "Todos", "---"):
        return None
    if raw.isdigit():
        return int(raw)
    # Allow common thousands separators (., , space, _) but only in 3-digit groups
    if re.fullmatch(r"\d{1,3}([\s,._]\d{3})+", raw):
        digits = re.sub(r"[\s,._]", "", raw)
        if not digits:
            return None
        try:
            return int(digits)
        except ValueError:
            return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_log_result_filter(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    try:
        raw = str(value).strip()
    except Exception:
        return None
    if raw in ("", "Todas", "Todos", "---"):
        return None
    normalized = _strip_accents(raw).upper()
    if normalized in ("FAIL", "FALLO", "FALLA", "FALLIDO", "ERROR", "ERR", "FAILED"):
        return ["FAIL", "ERROR", "FALLO", "FALLA", "FALLIDO", "ERR", "FAILED"]
    if normalized in ("WARNING", "WARN", "ADVERTENCIA", "ADVERTENCIAS"):
        return ["WARNING", "WARN", "ADVERTENCIA"]
    if normalized in ("OK", "EXITOSO", "EXITO", "SUCCESS", "SUCCESSFUL"):
        return ["OK", "EXITOSO", "EXITO", "SUCCESS", "SUCCESSFUL"]
    return [normalized]


def _parse_date_only(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        raw = value.strip()
        if _DATE_ONLY_RE.fullmatch(raw):
            try:
                return datetime.strptime(raw, "%Y-%m-%d")
            except ValueError:
                return None
    return None


def _coerce_optional_positive_int(value: Any, field: str) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError(f"Valor inválido para {field}.")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field} debe ser un entero.")
        parsed = int(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if not raw.lstrip("+-").isdigit():
            raise ValueError(f"{field} debe ser un entero.")
        parsed = int(raw)
    else:
        raise ValueError(f"Valor inválido para {field}.")
    if parsed <= 0:
        raise ValueError(f"{field} debe ser mayor a 0.")
    return parsed


_ARTICLE_SORT_COLUMNS: Dict[str, str] = {
    "id": "id",
    "codigo": "codigo",
    "nombre": "nombre",
    "marca": "marca",
    "rubro": "rubro",
    "costo": "costo",
    "precio_lista": "precio_lista",
    "unidad_abreviatura": "unidad_abreviatura",
    "unidad_medida": "unidad_medida",
    "id_unidad_medida": "unidad_abreviatura",
    "id_tipo_iva": "porcentaje_iva",
    "porcentaje_iva": "porcentaje_iva",
    "proveedor": "proveedor",
    "id_proveedor": "proveedor",
    "id_marca": "marca",
    "id_rubro": "rubro",
    "stock_minimo": "stock_minimo",
    "stock_actual": "stock_actual",
    "unidades_por_bulto": "unidades_por_bulto",
    "ubicacion": "ubicacion",
    "activo": "activo",
}


def _build_article_order_by_clause(
    sorts: Optional[Sequence[Tuple[str, str]]],
    sort_columns: Dict[str, str],
) -> str:
    if not sorts:
        return "nombre ASC, id ASC"

    clauses: List[str] = []
    for key, direction in sorts:
        column = sort_columns.get((key or "").strip())
        if not column:
            continue

        dir_sql = "DESC" if (direction or "").lower() == "desc" else "ASC"
        if column in {"precio_lista", "unidades_por_bulto"}:
            clauses.append(f"{column} {dir_sql} NULLS LAST")
        elif column == "codigo":
            numeric_flag = "CASE WHEN NULLIF(BTRIM(codigo), '') ~ '^[0-9]+$' THEN 0 ELSE 1 END"
            numeric_value = "CASE WHEN NULLIF(BTRIM(codigo), '') ~ '^[0-9]+$' THEN (BTRIM(codigo))::numeric END"
            clauses.append(f"{numeric_flag} ASC")
            clauses.append(f"{numeric_value} {dir_sql} NULLS LAST")
            clauses.append(f"codigo {dir_sql}")
        else:
            clauses.append(f"{column} {dir_sql}")

    if not clauses:
        return "nombre ASC, id ASC"

    order_by = ", ".join(clauses)
    if "id ASC" not in order_by:
        order_by += ", id ASC"
    return order_by


class Database:
    def __init__(self, dsn: str, *, pool_min_size: int = 1, pool_max_size: int = 4):
        self.dsn = dsn
        try:
            pool_min = int(pool_min_size)
        except (TypeError, ValueError):
            pool_min = 1
        try:
            pool_max = int(pool_max_size)
        except (TypeError, ValueError):
            pool_max = 4
        if pool_min < 1:
            pool_min = 1
        if pool_max < pool_min:
            pool_max = pool_min
        
        self.pool_min = pool_min
        self.pool_max = pool_max
        self.pool = ConnectionPool(conninfo=dsn, min_size=pool_min, max_size=pool_max)
        self.current_user_id: Optional[int] = None
        self.current_ip: Optional[str] = None
        self.is_closing = False
        self._dashboard_stats_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._dashboard_cache_lock = threading.RLock()
        self._activity_state_lock = threading.RLock()
        self._file_log_lock = threading.RLock()
        self._entity_last_activity: Dict[str, float] = {}
        self._last_activity_ts = 0.0
        self._file_log_seq = int(time.time() * 1000)
        self._logs_dir = Path.cwd() / "logs"
        
        # Apply necessary schema patches automatically
        self._run_migrations()

    def get_config(self, key: str, default: Any = None) -> Any:
        """Fetch a configuration value from seguridad.config_sistema."""
        query = "SELECT valor FROM seguridad.config_sistema WHERE clave = %s"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (key,))
                    row = cur.fetchone()
                    if row:
                        return row.get("valor") if isinstance(row, dict) else row[0]
                    return default
        except Exception as e:
            logger.error(f"Error fetching config for key {key}: {e}")
            return default
    
    def set_config(self, key: str, value: Any, tipo: str = 'STRING', descripcion: str = '') -> None:
        """Update or insert a configuration value in seguridad.config_sistema."""
        query = """
            INSERT INTO seguridad.config_sistema (clave, valor, tipo, descripcion)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (clave) DO UPDATE 
            SET valor = EXCLUDED.valor, 
                descripcion = EXCLUDED.descripcion
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (key, str(value), tipo, descripcion))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error setting config for key {key}: {e}")

    def _run_migrations(self) -> None:
        """Run safe, idempotent schema updates to ensure the DB matches code requirements."""
        logger.info("Checking for database schema updates...")
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    # 1. Ensure id_documento in app.pago is nullable
                    # This is required for Cuenta Corriente payments which don't target a specific doc
                    cur.execute("""
                        DO $$ 
                        BEGIN 
                            IF EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_schema = 'app' 
                                  AND table_name = 'pago' 
                                  AND column_name = 'id_documento' 
                                  AND is_nullable = 'NO'
                            ) THEN 
                                ALTER TABLE app.pago ALTER COLUMN id_documento DROP NOT NULL;
                            END IF;
                        END $$;
                    """)
                    
                    # 2. Add stock_resultante column to movimiento_articulo for stock history tracking
                    cur.execute("""
                        ALTER TABLE app.movimiento_articulo 
                        ADD COLUMN IF NOT EXISTS stock_resultante NUMERIC(14,4);
                    """)
                    
                    # 3. Ensure per-line discount amount exists for legacy DBs
                    cur.execute("""
                        ALTER TABLE app.documento_detalle
                        ADD COLUMN IF NOT EXISTS descuento_importe NUMERIC(14,4) NOT NULL DEFAULT 0;
                    """)
                    cur.execute("""
                        UPDATE app.documento_detalle
                        SET descuento_importe = 0
                        WHERE descuento_importe IS NULL;
                    """)
                    cur.execute("""
                        ALTER TABLE app.documento_detalle
                        ADD COLUMN IF NOT EXISTS unidades_por_bulto_historico INTEGER;
                    """)
                    cur.execute("""
                        UPDATE app.documento_detalle
                        SET unidades_por_bulto_historico = NULL
                        WHERE unidades_por_bulto_historico IS NOT NULL
                          AND unidades_por_bulto_historico <= 0;
                    """)
                    cur.execute("""
                        UPDATE app.documento_detalle dd
                        SET unidades_por_bulto_historico = a.unidades_por_bulto
                        FROM app.articulo a
                        WHERE dd.id_articulo = a.id
                          AND dd.unidades_por_bulto_historico IS NULL
                          AND a.unidades_por_bulto IS NOT NULL
                          AND a.unidades_por_bulto > 0;
                    """)
                    cur.execute("""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1
                                FROM pg_constraint
                                WHERE conname = 'ck_det_unidades_por_bulto_hist'
                                  AND conrelid = 'app.documento_detalle'::regclass
                            ) THEN
                                ALTER TABLE app.documento_detalle
                                ADD CONSTRAINT ck_det_unidades_por_bulto_hist
                                CHECK (unidades_por_bulto_historico IS NULL OR unidades_por_bulto_historico > 0);
                            END IF;
                        END $$;
                    """)

                    # 4. Ensure article code exists for product identification
                    cur.execute("""
                        ALTER TABLE app.articulo
                        ADD COLUMN IF NOT EXISTS codigo VARCHAR(80);
                    """)
                    cur.execute("""
                        UPDATE app.articulo
                        SET codigo = NULLIF(BTRIM(codigo), '')
                        WHERE codigo IS DISTINCT FROM NULLIF(BTRIM(codigo), '');
                    """)
                    cur.execute("""
                        UPDATE app.articulo
                        SET codigo = id::text
                        WHERE codigo IS NULL;
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_articulo_codigo ON app.articulo(codigo);
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_articulo_codigo_lower_trgm
                        ON app.articulo USING gin (lower(codigo) gin_trgm_ops);
                    """)

                    # 5. Ensure article package units exist and are valid
                    cur.execute("""
                        ALTER TABLE app.articulo
                        ADD COLUMN IF NOT EXISTS unidades_por_bulto INTEGER;
                    """)
                    cur.execute("""
                        UPDATE app.articulo
                        SET unidades_por_bulto = NULL
                        WHERE unidades_por_bulto IS NOT NULL
                          AND unidades_por_bulto <= 0;
                    """)
                    cur.execute("""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1
                                FROM pg_constraint
                                WHERE conname = 'ck_art_unidades_por_bulto'
                                  AND conrelid = 'app.articulo'::regclass
                            ) THEN
                                ALTER TABLE app.articulo
                                ADD CONSTRAINT ck_art_unidades_por_bulto
                                CHECK (unidades_por_bulto IS NULL OR unidades_por_bulto > 0);
                            END IF;
                        END $$;
                    """)

                    # 6. Keep article detailed view in sync with latest schema
                    cur.execute("""
                        CREATE OR REPLACE VIEW app.v_articulo_detallado AS
                        SELECT
                          a.id AS id,
                          a.id AS id_articulo,
                          a.nombre,
                          a.id_marca,
                          m.nombre AS marca,
                          a.id_rubro,
                          r.nombre AS rubro,
                          a.costo,
                          a.id_tipo_iva,
                          ti.porcentaje AS porcentaje_iva,
                          a.id_unidad_medida,
                          um.nombre AS unidad_medida,
                          um.abreviatura AS unidad_abreviatura,
                          a.id_proveedor,
                          COALESCE(prov.razon_social, TRIM(COALESCE(prov.apellido, '') || ' ' || COALESCE(prov.nombre, ''))) AS proveedor,
                          a.stock_minimo,
                          a.descuento_base,
                          a.redondeo,
                          a.porcentaje_ganancia_2,
                          a.unidades_por_bulto,
                          a.activo,
                          a.observacion,
                          a.ubicacion,
                          COALESCE(st.stock_total, 0) AS stock_actual,
                          ap.precio AS precio_lista,
                          a.codigo
                        FROM app.articulo a
                        LEFT JOIN ref.marca m ON m.id = a.id_marca
                        LEFT JOIN ref.rubro r ON r.id = a.id_rubro
                        LEFT JOIN ref.tipo_iva ti ON ti.id = a.id_tipo_iva
                        LEFT JOIN ref.unidad_medida um ON um.id = a.id_unidad_medida
                        LEFT JOIN app.entidad_comercial prov ON prov.id = a.id_proveedor
                        LEFT JOIN app.v_stock_total st ON st.id_articulo = a.id
                        LEFT JOIN app.articulo_precio ap ON ap.id_articulo = a.id AND ap.id_lista_precio = 1;
                    """)

                    # 7. Update trigger to save stock_resultante on INSERT
                    cur.execute("""
                        CREATE OR REPLACE FUNCTION app.fn_sync_stock_resumen()
                        RETURNS TRIGGER AS $fn$
                        DECLARE
                          v_signo INTEGER;
                          v_new_stock NUMERIC(14,4);
                        BEGIN
                          -- Get the sign from the movement type
                          SELECT signo_stock INTO v_signo 
                          FROM ref.tipo_movimiento_articulo 
                          WHERE id = COALESCE(NEW.id_tipo_movimiento, OLD.id_tipo_movimiento);

                          IF (TG_OP = 'INSERT') THEN
                            INSERT INTO app.articulo_stock_resumen (id_articulo, stock_total)
                            VALUES (NEW.id_articulo, NEW.cantidad * v_signo)
                            ON CONFLICT (id_articulo) DO UPDATE 
                            SET stock_total = app.articulo_stock_resumen.stock_total + (NEW.cantidad * v_signo),
                                ultima_actualizacion = now();
                            
                            -- Get the new stock total and save it to the movement
                            SELECT stock_total INTO v_new_stock 
                            FROM app.articulo_stock_resumen 
                            WHERE id_articulo = NEW.id_articulo;
                            
                            UPDATE app.movimiento_articulo 
                            SET stock_resultante = v_new_stock 
                            WHERE id = NEW.id;
                                    
                          ELSIF (TG_OP = 'UPDATE') THEN
                            UPDATE app.articulo_stock_resumen 
                            SET stock_total = stock_total - (OLD.cantidad * v_signo) + (NEW.cantidad * v_signo),
                                ultima_actualizacion = now()
                            WHERE id_articulo = NEW.id_articulo;
                                
                          ELSIF (TG_OP = 'DELETE') THEN
                            UPDATE app.articulo_stock_resumen 
                            SET stock_total = stock_total - (OLD.cantidad * v_signo),
                                ultima_actualizacion = now()
                            WHERE id_articulo = OLD.id_articulo;
                          END IF;
                            
                          RETURN NULL;
                        END;
                        $fn$ LANGUAGE plpgsql;
                    """)

                    # 8. Ensure the default guest account exists for quick access.
                    if self._ensure_guest_user(cur) is None:
                        logger.warning(
                            "Could not provision guest user because role %s was not found.",
                            GUEST_USER_ROLE,
                        )
                    conn.commit()
                    logger.info("Database schema updates applied successfully.")
        except Exception as e:
            logger.error(f"Error during automatic database migrations: {e}")
    def close_pool(self) -> None:
        """Close the connection pool temporarily."""
        if self.pool:
            try:
                self.pool.close()
            except Exception:
                pass
            self.pool = None

    def close(self) -> None:
        """Alias for close_pool to ensure compatibility."""
        self.close_pool()

    def reconnect(self) -> None:
        """Reinitialize the connection pool."""
        if self.pool:
            return
        self.pool = ConnectionPool(
            conninfo=self.dsn, 
            min_size=self.pool_min, 
            max_size=self.pool_max
        )

    def set_context(self, user_id: Optional[int], ip: Optional[str] = None) -> None:
        self.current_user_id = user_id
        self.current_ip = ip

    def fetch_depositos(self, limit: int = 100) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre, ubicacion FROM ref.deposito WHERE activo = TRUE ORDER BY nombre LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def _setup_session(self, cur: Any) -> None:
        if self.current_user_id:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(self.current_user_id),))
        if self.current_ip:
            cur.execute("SELECT set_config('app.ip', %s, true)", (self.current_ip,))

    @contextmanager
    def _transaction(self, *, set_context: bool = True):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                if set_context:
                    self._setup_session(cur)
                try:
                    yield cur
                except Exception:
                    conn.rollback()
                    raise
                else:
                    conn.commit()

    def _ensure_logs_dir(self) -> Path:
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        return self._logs_dir

    def _daily_activity_log_path(self, when: datetime) -> Path:
        return self._ensure_logs_dir() / f"activity_{when.strftime('%Y-%m-%d')}.txt"

    def _next_activity_id(self) -> int:
        with self._activity_state_lock:
            self._file_log_seq += 1
            return self._file_log_seq

    def _entity_activity_keys(self, entidad: Optional[str]) -> List[str]:
        raw = str(entidad or "").strip()
        if not raw:
            return []

        keys: set[str] = {raw, raw.upper(), raw.lower()}
        parts = [part for part in re.split(r"[^A-Za-z0-9_]+", raw) if part]
        for part in parts:
            keys.add(part)
            keys.add(part.upper())
            keys.add(part.lower())
            if "_" in part:
                for token in [tk for tk in part.split("_") if tk]:
                    keys.add(token)
                    keys.add(token.upper())
                    keys.add(token.lower())

        upper_raw = raw.upper()
        alias_map = {
            "ENTIDAD": ("ENTIDAD",),
            "ARTICULO": ("ARTICULO",),
            "DOCUMENTO": ("DOCUMENTO",),
            "PAGO_CC": ("PAGO_CC",),
            "AJUSTE_CC": ("AJUSTE_CC",),
            "CUENTA_CORRIENTE": ("PAGO_CC", "AJUSTE_CC", "CUENTA_CORRIENTE"),
            "PAGO": ("PAGO",),
            "REMITO": ("REMITO",),
            "MOVIMIENTO": ("MOVIMIENTO",),
            "USUARIO": ("USUARIO",),
            "SISTEMA": ("SISTEMA",),
            "CONFIG": ("CONFIG",),
            "LISTA_PRECIO": ("PRECIOS", "LISTA_PRECIO"),
        }
        for token, aliases in alias_map.items():
            if token in upper_raw:
                for alias in aliases:
                    keys.add(alias)
                    keys.add(alias.upper())
                    keys.add(alias.lower())

        return [key for key in keys if key]

    def _should_track_runtime_activity(self, accion: Optional[str]) -> bool:
        action = str(accion or "").strip().upper()
        if not action:
            return True
        readonly_actions = {
            "SELECT",
            "VIEW",
            "VIEW_DETAIL",
            "NAVEGACION",
            "CONFIG_TAB",
            "LOGIN_OK",
            "LOGIN_FAIL",
            "LOGOUT",
        }
        if action in readonly_actions:
            return False
        if action.startswith("SELECT_") or action.startswith("VIEW_"):
            return False
        return True

    def _record_runtime_activity(self, entidad: Optional[str], ts: float) -> None:
        with self._activity_state_lock:
            if ts > self._last_activity_ts:
                self._last_activity_ts = ts
            for key in self._entity_activity_keys(entidad):
                self._entity_last_activity[key] = ts

    def _serialize_activity_detail(self, detalle: Optional[Dict[str, Any]]) -> str:
        if not detalle:
            return ""
        try:
            return json.dumps(detalle, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return json.dumps({"raw": str(detalle)}, ensure_ascii=False, separators=(",", ":"))

    def _append_activity_file_row(
        self,
        *,
        id_usuario: Optional[int],
        entidad: Optional[str],
        id_entidad: Optional[int],
        accion: Optional[str],
        resultado: Optional[str],
        ip: Optional[str],
        detalle: Optional[Dict[str, Any]],
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> bool:
        now = datetime.now()
        ts = now.timestamp()
        row = {
            "id": self._next_activity_id(),
            "fecha_hora": now.isoformat(timespec="seconds"),
            "id_usuario": id_usuario if id_usuario is not None else "",
            "entidad": (str(entidad).strip() if entidad is not None else "") or "",
            "id_entidad": id_entidad if id_entidad is not None else "",
            "accion": (str(accion).strip() if accion is not None else "") or "",
            "resultado": (str(resultado).strip().upper() if resultado is not None else "") or "OK",
            "ip": (str(ip).strip() if ip is not None else "") or "",
            "user_agent": (str(user_agent).strip() if user_agent is not None else "") or "",
            "session_id": (str(session_id).strip() if session_id is not None else "") or "",
            "detalle": self._serialize_activity_detail(detalle),
        }
        filepath = self._daily_activity_log_path(now)
        try:
            with self._file_log_lock:
                file_exists = filepath.exists()
                with filepath.open("a", encoding="utf-8", newline="") as fh:
                    writer = csv.DictWriter(fh, fieldnames=_ACTIVITY_LOG_COLUMNS, delimiter=",")
                    if not file_exists or filepath.stat().st_size == 0:
                        writer.writeheader()
                    writer.writerow(row)
            if self._should_track_runtime_activity(row.get("accion")):
                self._record_runtime_activity(row.get("entidad"), ts)
            return True
        except Exception:
            logger.exception(
                "Error writing activity file log (entidad=%s, accion=%s, id_entidad=%s)",
                entidad,
                accion,
                id_entidad,
            )
            return False

    def log_activity(self, entidad: str, accion: str, id_entidad: Optional[int] = None, resultado: str = "OK", detalle: Optional[Dict[str, Any]] = None) -> None:
        if self.is_closing:
            return # Silent skip on shutdown
        self._append_activity_file_row(
            id_usuario=self.current_user_id,
            entidad=entidad,
            id_entidad=id_entidad,
            accion=accion,
            resultado=resultado,
            ip=self.current_ip,
            detalle=detalle,
        )

    def log_logout(self, motivo: str, usuario: Optional[str] = None, *, use_pool: bool = True) -> bool:
        """Log a logout event to activity file logs."""
        _ = use_pool  # kept for backward compatibility
        user_id = self.current_user_id
        if not user_id:
            return False

        detalle = {"motivo": motivo} if motivo else {}
        if usuario:
            detalle["usuario"] = usuario
        logged = self._append_activity_file_row(
            id_usuario=user_id,
            entidad="SISTEMA",
            id_entidad=None,
            accion="LOGOUT",
            resultado="OK",
            ip=self.current_ip,
            detalle=detalle,
        )
        return logged

    def check_recent_activity(self, since_timestamp: float, tables: List[str] = None) -> bool:
        """
        Check if there has been any activity in the specified tables since the given timestamp.
        """
        if self.is_closing:
            return False
        try:
            since_value = float(since_timestamp)
        except (TypeError, ValueError):
            return False

        with self._activity_state_lock:
            if self._last_activity_ts <= since_value:
                return False
            if not tables:
                return True
            for table in tables:
                for key in self._entity_activity_keys(table):
                    if self._entity_last_activity.get(key, 0.0) > since_value:
                        return True
            return False

    # =========================================================================
    # In-Memory Catalog Cache (reduces DB hits for frequently accessed data)
    # =========================================================================
    _catalog_cache: Dict[str, Tuple[float, List[str]]] = {}  # {key: (timestamp, data)}
    _CACHE_TTL = 300  # 5 minutes
    _DASHBOARD_STATS_CACHE_TTL = 60.0  # seconds - aligned with default refresh interval

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

    def _get_cached_catalog_raw(self, cache_key: str, query: str) -> List[Dict[str, Any]]:
        """Get raw catalog (dicts) from cache or fetch from DB if expired."""
        import time
        now = time.time()
        if cache_key in self._catalog_cache:
            cached_time, cached_data = self._catalog_cache[cache_key]
            if now - cached_time < self._CACHE_TTL:
                return cached_data # type: ignore
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = _rows_to_dicts(cur)
                self._catalog_cache[cache_key] = (now, result) # type: ignore
                return result

    def invalidate_catalog_cache(self, cache_key: Optional[str] = None) -> None:
        """Invalidate catalog cache. Call after modifying catalogs."""
        if cache_key:
            self._catalog_cache.pop(cache_key, None)
        else:
            self._catalog_cache.clear()

    def invalidate_dashboard_stats_cache(self, role: Optional[str] = None) -> None:
        """Clear cached dashboard statistics for a specific role or all roles."""
        with self._dashboard_cache_lock:
            if role:
                self._dashboard_stats_cache.pop(role.upper(), None)
            else:
                self._dashboard_stats_cache.clear()

    # =========================================================================
    # Batch Dashboard Statistics (single connection, fewer round-trips)
    # =========================================================================
    def get_full_dashboard_stats(self, role: str = "EMPLEADO", period: str = "Mes", *, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Fetch 100+ dashboard statistics filtered by user role and time period.
        Roles: 'ADMIN', 'GERENTE', 'EMPLEADO'
        Period: 'Hoy', 'Semana', 'Mes', 'Año'
        """
        role_key = (role or "EMPLEADO").upper()
        period_key = (period or "Mes").capitalize()
        cache_key = f"{role_key}_{period_key}"
        now = time.time()

        if not force_refresh:
            with self._dashboard_cache_lock:
                cache_entry = self._dashboard_stats_cache.get(cache_key)
                if cache_entry and now - cache_entry[0] < self._DASHBOARD_STATS_CACHE_TTL:
                    return cache_entry[1]

        stats = self._fetch_full_dashboard_stats(role_key, period_key)

        with self._dashboard_cache_lock:
            self._dashboard_stats_cache[cache_key] = (now, stats)

        return stats

    def _fetch_full_dashboard_stats(self, role: str, period: str) -> Dict[str, Any]:
        role = (role or "EMPLEADO").upper()
        stats: Dict[str, Any] = {}
        
        # Calculate start date based on period
        start_date_sql = "date_trunc('month', now())"  # Default
        if period == "Hoy":
            start_date_sql = "current_date"
        elif period == "Semana":
            start_date_sql = "date_trunc('week', now())"
        elif period == "Año":
            start_date_sql = "date_trunc('year', now())"

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # 1. Basic Operating Stats (Accessible to all)
                stats["operativas"] = self._get_stats_operativas(cur, role, start_date_sql)

                # 2. Sales Stats (Always includes Hoy/Mes/Total columns, so keeping it generic but potentially could filter "metrics" if needed)
                # For now, we keep the global "boxes" as fixed (Hoy/Mes/Año) but we could filter the breakdowns.
                stats["ventas"] = self._get_stats_ventas_extended(cur, role, start_date_sql, period)

                # 3. Stock Stats
                stats["stock"] = self._get_stats_stock_extended(cur, role, start_date_sql)

                # 5. Entities (Clients/Providers)
                stats["entidades"] = self._get_stats_entidades_extended(cur, role, start_date_sql)

                # 6. Movement Stats (Today summary - maybe should also respect period? Kept as today for specific box)
                stats["movimientos"] = self._get_stats_movimientos_extended(cur, role)

                # 7. Financial Stats (Restricted)
                if role in ("ADMIN", "GERENTE"):
                    stats["finanzas"] = self._get_stats_finanzas_extended(cur, role, start_date_sql)

                # 8. Technical/System Stats (Admin only)
                if role == "ADMIN":
                    stats["sistema"] = self._get_stats_sistema_extended(cur, role)

                # 9. Extended Chart Data
                stats["charts"] = {
                    "ventas_mensuales": self.get_reporte_ventas_dinamico(period, limit=12) if role in ("ADMIN", "GERENTE") else [],
                    "top_articulos": self.get_top_articulos_dinamico(start_date_sql, limit=5) if role in ("ADMIN", "GERENTE") else [],
                    "bottom_articulos": self.get_bottom_articulos_dinamico(start_date_sql, limit=5) if role in ("ADMIN", "GERENTE") else [],
                    "alertas_stock": self.get_alertas_stock(limit=5),
                    "stock_por_rubro": self.get_stock_by_rubro(limit=8),
                    "entidades_por_tipo": self.get_entidades_by_tipo()
                }
        return stats

    def _get_stats_ventas_extended(self, cur, role, start_date_sql: str, period: str) -> Dict[str, Any]:
        """Sales statistics (25 metrics)"""
        # EMPLEADO has restricted access to some financial values
        show_money = role in ("ADMIN", "GERENTE")
        
        # Main KPI query (Hoy, Mes, Sem) - keeps fixed logic for KPI cards
        cur.execute("""
            WITH base_ventas AS (
                SELECT 
                    total, fecha, estado,
                    CASE WHEN fecha >= current_date THEN 1 ELSE 0 END as es_hoy,
                    CASE WHEN fecha >= date_trunc('week', now()) THEN 1 ELSE 0 END as es_semana,
                    CASE WHEN fecha >= date_trunc('month', now()) THEN 1 ELSE 0 END as es_mes,
                    CASE WHEN fecha >= date_trunc('year', now()) THEN 1 ELSE 0 END as es_anio,
                    CASE WHEN fecha >= date_trunc('month', now() - interval '1 month') 
                          AND fecha < date_trunc('month', now()) THEN 1 ELSE 0 END as es_mes_ant
                FROM app.v_documento_resumen 
                WHERE clase = 'VENTA'
            )
            SELECT 
                SUM(total) FILTER (WHERE es_hoy = 1 AND estado != 'ANULADO') as hoy_total,
                COUNT(*) FILTER (WHERE es_hoy = 1 AND estado != 'ANULADO') as hoy_cant,
                AVG(total) FILTER (WHERE es_hoy = 1 AND estado != 'ANULADO') as hoy_ticket_prom,
                SUM(total) FILTER (WHERE es_semana = 1 AND estado != 'ANULADO') as semana_total,
                SUM(total) FILTER (WHERE es_mes = 1 AND estado != 'ANULADO') as mes_total,
                SUM(total) FILTER (WHERE es_anio = 1 AND estado != 'ANULADO') as anio_total,
                COUNT(*) FILTER (WHERE estado = 'CONFIRMADO') as docs_pendientes,
                COUNT(*) FILTER (WHERE estado = 'ANULADO' AND es_mes = 1) as anulados_mes,
                SUM(total) FILTER (WHERE es_mes_ant = 1 AND estado != 'ANULADO') as mes_ant_total
            FROM base_ventas
        """)

        row = cur.fetchone()
        
        val_mes = float(row[4] or 0)
        val_mes_ant = float(row[8] or 0)
        tendencia = 0.0
        if val_mes_ant > 1:
            tendencia = ((val_mes - val_mes_ant) / val_mes_ant) * 100

        res = {
            "hoy_total": float(row[0] or 0) if show_money else "—",
            "hoy_cant": row[1] or 0,
            "hoy_ticket_prom": float(row[2] or 0) if show_money else "—",
            "semana_total": float(row[3] or 0) if show_money else "—",
            "mes_total": float(row[4] or 0) if show_money else "—",
            "anio_total": float(row[5] or 0) if role == "ADMIN" else "—",
            "docs_pendientes": row[6] or 0,
            "presupuestos_pend": row[6] or 0, # compatibility
            "anulados_mes": row[7] or 0,
            "tendencia_mes_pct": round(tendencia, 1) if show_money else 0.0
        }

        
        # Add filtered stats if Gerente/Admin
        if role in ("ADMIN", "GERENTE"):
            # Determine date filter as SQL literal expression (safe from injection)
            date_expr_map = {
                "Hoy": "current_date",
                "Semana": "date_trunc('week', now())",
                "Mes": "date_trunc('month', now())",
                "Año": "date_trunc('year', now())"
            }
            # Use literal expression or fall back to month
            date_expr = date_expr_map.get(period, "date_trunc('month', now())")
            
            # Query 1: Documents by type (using literal date expression)
            query_tipo = f"""
                SELECT 
                    tipo_documento, COUNT(*) 
                FROM app.v_documento_resumen
                WHERE clase = 'VENTA' AND fecha >= {date_expr}
                GROUP BY tipo_documento
            """
            cur.execute(query_tipo)
            res["por_tipo"] = {r[0]: r[1] for r in cur.fetchall()}
            
            # Query 2: Payment methods (using literal date expression)
            query_fp = f"""
                SELECT 
                    COALESCE(fp.descripcion, 'Efectivo'), 
                    SUM(COALESCE(p.monto, d.total))
                FROM app.v_documento_resumen d
                LEFT JOIN app.pago p ON p.id_documento = d.id
                LEFT JOIN ref.forma_pago fp ON p.id_forma_pago = fp.id
                WHERE d.clase = 'VENTA' 
                  AND d.estado != 'ANULADO'
                  AND d.fecha >= {date_expr}
                GROUP BY COALESCE(fp.descripcion, 'Efectivo')
            """
            cur.execute(query_fp)
            res["por_forma_pago"] = {r[0]: float(r[1] or 0) for r in cur.fetchall()}
            
        return res

    def _get_stats_stock_extended(self, cur, role, start_date_sql: str) -> Dict[str, Any]:
        """Stock statistics (18 metrics)"""
        # Use literal date expression map to safely determine the date filter without f-string injection
        date_expr_map = {
            "Hoy": "current_date",
            "Semana": "date_trunc('week', now())",
            "Mes": "date_trunc('month', now())",
            "Año": "date_trunc('year', now())"
        }
        # Extract period from start_date_sql to map to the correct literal expression
        # start_date_sql like "current_date" or "date_trunc('month', now())" etc.
        # For backward compatibility, try to map or default to 'Mes'
        date_expr = "date_trunc('month', now())"
        if "current_date" in start_date_sql:
            date_expr = "current_date"
        elif "'week'" in start_date_sql:
            date_expr = "date_trunc('week', now())"
        elif "'year'" in start_date_sql:
            date_expr = "date_trunc('year', now())"
        
        # Build query with literal date expression (no f-string injection risk)
        query = f"""
            SELECT 
                (SELECT COUNT(*) FROM app.articulo) as total,
                (SELECT COUNT(*) FROM app.articulo WHERE activo = true) as activos,
                (SELECT COUNT(*) FROM app.v_articulo_detallado WHERE stock_actual <= stock_minimo) as bajo_stock,
                (SELECT COUNT(*) FROM app.v_articulo_detallado WHERE stock_actual <= 0) as sin_stock,
                (SELECT COALESCE(SUM(costo * stock_actual), 0) FROM app.v_articulo_detallado) as valor_costo,
                (SELECT COUNT(*) FROM app.movimiento_articulo WHERE fecha >= {date_expr} AND cantidad > 0) as entradas_mes,
                (SELECT COUNT(*) FROM app.movimiento_articulo WHERE fecha >= {date_expr} AND cantidad < 0) as salidas_mes,
                (SELECT COALESCE(SUM(stock_actual), 0) FROM app.v_articulo_detallado) as stock_total_unidades
        """
        cur.execute(query)
        row = cur.fetchone()
        
        show_values = role in ("ADMIN", "GERENTE")
        
        return {
            "total": row[0] or 0,
            "activos": row[1] or 0,
            "bajo_stock": row[2] or 0,
            "sin_stock": row[3] or 0,
            "valor_costo": float(row[4] or 0) if show_values else "—",
            "valor_inventario": float(row[4] or 0) if show_values else 0,  # Same as valor_costo for now
            "entradas_mes": row[5] or 0,
            "salidas_mes": row[6] or 0,
            "stock_unidades": row[7] or 0
        }

    def _get_stats_movimientos_extended(self, cur, role) -> Dict[str, Any]:
        """Movement statistics for today (3 metrics)"""
        cur.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN signo_stock > 0 THEN 1 ELSE 0 END), 0) as ingresos,
                COALESCE(SUM(CASE WHEN signo_stock < 0 THEN 1 ELSE 0 END), 0) as salidas,
                (SELECT COUNT(*) FROM app.movimiento_articulo WHERE id_documento IS NULL AND fecha >= current_date) as ajustes
            FROM app.v_movimientos_full 
            WHERE fecha >= current_date
        """)
        row = cur.fetchone()
        return {
            "ingresos": row[0] or 0,
            "salidas": row[1] or 0,
            "ajustes": row[2] or 0
        }

    def _get_stats_entidades_extended(self, cur, role, start_date_sql: str) -> Dict[str, Any]:
        """Clients and Providers stats (25 metrics)"""
        # Use literal date expression map to safely determine the date filter
        date_expr = "date_trunc('month', now())"
        if "current_date" in start_date_sql:
            date_expr = "current_date"
        elif "'week'" in start_date_sql:
            date_expr = "date_trunc('week', now())"
        elif "'year'" in start_date_sql:
            date_expr = "date_trunc('year', now())"
        
        query = f"""
            SELECT 
                (SELECT COUNT(*) FROM app.entidad_comercial WHERE tipo IN ('CLIENTE', 'AMBOS')) as clientes_total,
                (SELECT COUNT(*) FROM app.entidad_comercial WHERE tipo IN ('PROVEEDOR', 'AMBOS')) as prov_total,
                (SELECT COUNT(*) FROM app.entidad_comercial WHERE fecha_creacion >= {date_expr}) as nuevos_mes,
                (SELECT COALESCE(SUM(saldo_cuenta), 0) FROM app.lista_cliente WHERE saldo_cuenta > 0) as deuda_clientes_total,
                (SELECT COUNT(*) FROM app.lista_cliente WHERE saldo_cuenta > 0) as deudores_cant
        """
        cur.execute(query)
        row = cur.fetchone()
        
        show_money = role in ("ADMIN", "GERENTE")
        
        return {
            "clientes_total": row[0] or 0,
            "proveedores_total": row[1] or 0,
            "nuevos_mes": row[2] or 0,
            "deuda_clientes": float(row[3] or 0) if show_money else "—",
            "deudores_cant": row[4] or 0
        }

    def _get_stats_finanzas_extended(self, cur, role, start_date_sql: str) -> Dict[str, Any]:
        """Financial stats (15 metrics) - Restricted to GERENTE/ADMIN"""
        # Use literal date expression map to safely determine the date filter
        date_expr = "date_trunc('month', now())"
        if "current_date" in start_date_sql:
            date_expr = "current_date"
        elif "'week'" in start_date_sql:
            date_expr = "date_trunc('week', now())"
        elif "'year'" in start_date_sql:
            date_expr = "date_trunc('year', now())"
        
        query = f"""
            SELECT 
                (SELECT COALESCE(SUM(monto), 0) FROM app.pago WHERE fecha >= current_date) as ingresos_hoy,
                (SELECT COALESCE(SUM(total), 0) FROM app.v_documento_resumen WHERE clase = 'COMPRA' AND fecha >= {date_expr}) as egresos_mes,
                (SELECT COALESCE(SUM(p.monto), 0) FROM app.pago p JOIN app.v_documento_resumen d ON p.id_documento = d.id WHERE d.clase = 'VENTA' AND p.fecha >= {date_expr}) as ingresos_mes,
                (SELECT COALESCE(SUM(total * 0.21), 0) FROM app.v_documento_resumen WHERE clase = 'VENTA' AND fecha >= {date_expr}) as iva_estimado_mes,
                (SELECT COUNT(*) FROM app.pago WHERE fecha >= now() - interval '7 days') as pagos_recientes
        """
        cur.execute(query)
        row = cur.fetchone()
        
        return {
            "ingresos_hoy": float(row[0] or 0),
            "egresos_mes": float(row[1] or 0),
            "ingresos_mes": float(row[2] or 0),
            "balance_mes": float(row[2] or 0) - float(row[1] or 0),
            "iva_estimado": float(row[3] or 0),
            "pagos_recientes": int(row[4] or 0)
        }

    def _get_stats_operativas(self, cur, role, start_date_sql: str) -> Dict[str, Any]:
        """Operational and Productivity stats (20 metrics)"""
        query = """
            SELECT 
                (SELECT COUNT(*) FROM app.remito WHERE estado = 'PENDIENTE') as remitos_pend,
                (SELECT COUNT(*) FROM app.remito WHERE fecha >= current_date AND estado = 'ENTREGADO') as entregas_hoy
        """
        cur.execute(query)
        row = cur.fetchone()
        
        res = {
            "remitos_pend": row[0] or 0,
            "entregas_hoy": row[1] or 0,
            "actividad_sistema": 0
        }
        
        # Add "My Stats" for employees
        if self.current_user_id:
            cur.execute("""
                SELECT COUNT(*) FROM app.documento 
                WHERE id_usuario = %s AND fecha >= current_date
            """, (self.current_user_id,))
            res["mis_operaciones_hoy"] = cur.fetchone()[0]
            
        return res

    def _get_stats_sistema_extended(self, cur, role) -> Dict[str, Any]:
        """Technical system stats (10 metrics) - ADMIN only"""
        cur.execute("SELECT MAX(ultimo_login) FROM seguridad.usuario")
        row = cur.fetchone()
        ultimo_login = row[0] if row else None

        return {
            "errores_mes": 0,
            "backups_mes": 0,
            "ultimo_login": ultimo_login.strftime("%Y-%m-%d %H:%M") if ultimo_login else "N/A"
        }


    def get_reporte_ventas_dinamico(self, period: str, limit: int = 12) -> List[Dict[str, Any]]:
        """
        Dynamic sales report based on period.
        - Hoy/Semana: Group by day
        - Mes: Group by day
        - Año: Group by month
        """
        trunc = 'month'
        limit_clause = "LIMIT %s"
        
        # Construct query based on period
        if period == "Hoy":
            # Hourly grouping for today
            query = """
                SELECT 
                    to_char(fecha, 'HH24:00') as label,
                    SUM(total) as total_ventas,
                    COUNT(*) as cantidad_ventas
                FROM app.v_documento_resumen
                WHERE clase = %s AND estado IN (%s, %s)
                  AND fecha >= current_date
                GROUP BY 1
                ORDER BY 1 ASC
            """
            limit_clause = ""
        elif period == "Semana":
            # Daily grouping for this week
            query = """
                SELECT 
                    to_char(fecha, 'Dy DD') as label,
                    SUM(total) as total_ventas,
                    COUNT(*) as cantidad_ventas
                FROM app.v_documento_resumen
                WHERE clase = %s AND estado IN (%s, %s)
                  AND fecha >= date_trunc('week', now())
                GROUP BY 1, date_trunc('day', fecha)
                ORDER BY date_trunc('day', fecha) ASC
            """
            limit_clause = ""
        elif period == "Mes":
             # Daily grouping for this month
            query = """
                SELECT 
                    to_char(fecha, 'DD/MM') as label,
                    SUM(total) as total_ventas,
                    COUNT(*) as cantidad_ventas
                FROM app.v_documento_resumen
                WHERE clase = %s AND estado IN (%s, %s)
                  AND fecha >= date_trunc('month', now())
                GROUP BY 1, date_trunc('day', fecha)
                ORDER BY date_trunc('day', fecha) ASC
            """
            limit_clause = ""
        else: # Año or fallback
            # Monthly grouping for this year
            query = """
                SELECT 
                    to_char(fecha, 'Mon') as label,
                    SUM(total) as total_ventas,
                    COUNT(*) as cantidad_ventas
                FROM app.v_documento_resumen
                WHERE clase = %s AND estado IN (%s, %s)
                  AND fecha >= date_trunc('year', now())
                GROUP BY 1, date_trunc('month', fecha)
                ORDER BY date_trunc('month', fecha) ASC
                LIMIT %s
            """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                if period not in ("Hoy", "Semana", "Mes"):
                     cur.execute(query, (ClaseDocumento.VENTA.value, DocumentoEstado.CONFIRMADO.value, 
                                       DocumentoEstado.PAGADO.value, limit))
                else:
                     cur.execute(query, (ClaseDocumento.VENTA.value, DocumentoEstado.CONFIRMADO.value, 
                                       DocumentoEstado.PAGADO.value))
                
                rows = cur.fetchall()
                # Map to list of dicts. Note: "mes" key used by dashboard_view for label
                return [{"mes": r[0], "total_ventas": float(r[1] or 0), "cantidad_ventas": r[2]} for r in rows]

    def get_top_articulos_dinamico(self, start_date_sql: str, limit: int = 10) -> List[Dict[str, Any]]:
        # Use literal date expression map to safely determine the date filter
        date_expr = "date_trunc('month', now())"
        if "current_date" in start_date_sql:
            date_expr = "current_date"
        elif "'week'" in start_date_sql:
            date_expr = "date_trunc('week', now())"
        elif "'year'" in start_date_sql:
            date_expr = "date_trunc('year', now())"
        
        query = f"""
            SELECT 
                a.id, a.nombre,
                COALESCE(SUM(dd.cantidad), 0) as cantidad_vendida,
                COALESCE(SUM(dd.total_linea), 0) as total_facturado
            FROM app.articulo a
            JOIN app.documento_detalle dd ON a.id = dd.id_articulo
            JOIN app.v_documento_resumen d ON dd.id_documento = d.id
            WHERE a.activo = true
              AND d.clase = 'VENTA'
              AND d.estado IN ('CONFIRMADO', 'PAGADO')
              AND d.fecha >= {date_expr}
            GROUP BY a.id, a.nombre
            ORDER BY total_facturado DESC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_bottom_articulos_dinamico(self, start_date_sql: str, limit: int = 10) -> List[Dict[str, Any]]:
        # Use literal date expression map to safely determine the date filter
        date_expr = "date_trunc('month', now())"
        if "current_date" in start_date_sql:
            date_expr = "current_date"
        elif "'week'" in start_date_sql:
            date_expr = "date_trunc('week', now())"
        elif "'year'" in start_date_sql:
            date_expr = "date_trunc('year', now())"
        
        query = f"""
            SELECT 
                a.id, a.nombre,
                COALESCE(SUM(dd.cantidad), 0) as cantidad_vendida,
                COALESCE(SUM(dd.total_linea), 0) as total_facturado
            FROM app.articulo a
            LEFT JOIN app.documento_detalle dd ON a.id = dd.id_articulo
            LEFT JOIN app.v_documento_resumen d ON dd.id_documento = d.id
            WHERE a.activo = true
              AND (d.id IS NULL OR (d.fecha >= {date_expr} AND d.estado IN ('CONFIRMADO', 'PAGADO') AND d.clase = 'VENTA'))
            GROUP BY a.id, a.nombre
            HAVING COALESCE(SUM(dd.total_linea), 0) > 0
            ORDER BY total_facturado ASC, cantidad_vendida ASC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

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
                    WHERE clase = %s AND fecha >= date_trunc('month', now())
                """, (ClaseDocumento.VENTA.value,))
                ventas = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT SUM(total) FROM app.v_documento_resumen 
                    WHERE clase = %s AND fecha >= date_trunc('month', now())
                """, (ClaseDocumento.COMPRA.value,))
                compras = cur.fetchone()[0] or 0
                cur.execute("SELECT COUNT(*) FROM app.documento WHERE estado IN (%s, %s)", 
                           (DocumentoEstado.BORRADOR.value, DocumentoEstado.CONFIRMADO.value))
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
    def _ensure_guest_user(self, cur: Any) -> Optional[int]:
        """
        Ensure the default guest user exists and is aligned with the GERENTE role.
        Returns guest user ID, or None if the role does not exist.
        """
        cur.execute(
            "SELECT id FROM seguridad.rol WHERE upper(nombre) = %s ORDER BY id LIMIT 1",
            (GUEST_USER_ROLE,),
        )
        role_row = cur.fetchone()
        role_id = role_row.get("id") if isinstance(role_row, dict) else (role_row[0] if role_row else None)
        if not role_id:
            return None

        cur.execute(
            """
            SELECT id
            FROM seguridad.usuario
            WHERE lower(email) = lower(%s)
            ORDER BY id
            LIMIT 1
            """,
            (GUEST_USER_EMAIL,),
        )
        guest_row = cur.fetchone()

        if guest_row:
            guest_id = guest_row.get("id") if isinstance(guest_row, dict) else guest_row[0]
            cur.execute(
                """
                UPDATE seguridad.usuario
                SET nombre = %s,
                    id_rol = %s,
                    activo = TRUE,
                    fecha_actualizacion = now()
                WHERE id = %s
                  AND (
                    nombre IS DISTINCT FROM %s
                    OR id_rol IS DISTINCT FROM %s
                    OR activo IS DISTINCT FROM TRUE
                  )
                """,
                (GUEST_USER_NAME, role_id, guest_id, GUEST_USER_NAME, role_id),
            )
            return guest_id

        cur.execute(
            """
            INSERT INTO seguridad.usuario (nombre, email, contrasena_hash, id_rol, activo)
            VALUES (%s, %s, crypt(gen_random_uuid()::text, gen_salt('bf', 12)), %s, TRUE)
            RETURNING id
            """,
            (GUEST_USER_NAME, GUEST_USER_EMAIL, role_id),
        )
        created = cur.fetchone()
        if not created:
            return None
        return created.get("id") if isinstance(created, dict) else created[0]

    def authenticate_guest_user(self) -> Optional[Dict[str, Any]]:
        """
        Authenticate using the default guest user without requiring credentials.
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    guest_id = self._ensure_guest_user(cur)
                    if guest_id is None:
                        conn.rollback()
                        self._log_login_attempt(None, GUEST_USER_EMAIL, False, "Rol GERENTE no disponible")
                        return None

                    cur.execute(
                        """
                        SELECT
                            u.id,
                            u.nombre,
                            u.email,
                            u.activo,
                            r.nombre AS rol
                        FROM seguridad.usuario u
                        JOIN seguridad.rol r ON r.id = u.id_rol
                        WHERE u.id = %s
                        """,
                        (guest_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        conn.rollback()
                        self._log_login_attempt(None, GUEST_USER_EMAIL, False, "Usuario invitado no encontrado")
                        return None

                    if isinstance(row, dict):
                        user_id = row.get("id")
                        nombre = row.get("nombre")
                        email = row.get("email")
                        activo = row.get("activo")
                        rol = row.get("rol")
                    else:
                        user_id, nombre, email, activo, rol = row

                    if not activo:
                        conn.rollback()
                        self._log_login_attempt(user_id, GUEST_USER_EMAIL, False, "Usuario inactivo")
                        return None

                    cur.execute(
                        "UPDATE seguridad.usuario SET ultimo_login = now() WHERE id = %s",
                        (user_id,),
                    )
                    conn.commit()
                    self._log_login_attempt(user_id, GUEST_USER_EMAIL, True, "Modo invitado")
                    return {
                        "id": user_id,
                        "nombre": nombre,
                        "email": email,
                        "rol": rol,
                    }
        except Exception as e:
            logger.error("Error during guest authentication", exc_info=e)
            return None

    def authenticate_user(self, email_or_username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user by email and password.
        Returns user dict with role if successful, None if authentication fails.
        Uses bcrypt via PostgreSQL's pgcrypto extension.
        """
        if not email_or_username or not password:
            return None
        identifier = email_or_username.strip()
        if not identifier:
            return None

        # Email login must match case exactly; username remains case-insensitive.
        if "@" in identifier:
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
                WHERE u.email = %s
            """
            params = (identifier,)
        else:
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
                WHERE lower(u.nombre) = lower(%s)
            """
            params = (identifier,)
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    row = cur.fetchone()
                    
                    if not row:
                        self._log_login_attempt(None, identifier, False)
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
                        self._log_login_attempt(user_id, identifier, False, "Usuario inactivo")
                        return None
                    
                    # Verify password using bcrypt via PostgreSQL
                    cur.execute(
                        "SELECT crypt(%s, %s) = %s AS valid",
                        (password, stored_hash, stored_hash)
                    )
                    result = cur.fetchone()
                    is_valid = result.get("valid") if isinstance(result, dict) else result[0]
                    
                    if not is_valid:
                        self._log_login_attempt(user_id, identifier, False, "Contraseña incorrecta")
                        return None
                    
                    # Update ultimo_login
                    cur.execute(
                        "UPDATE seguridad.usuario SET ultimo_login = now() WHERE id = %s",
                        (user_id,)
                    )
                    conn.commit()
                    # Log successful login
                    self._log_login_attempt(user_id, identifier, True)
                    
                    return {
                        "id": user_id,
                        "nombre": nombre,
                        "email": email,
                        "rol": rol
                    }
        except Exception as e:
            logger.error("Error during authentication", exc_info=e)
            return None
    
    def _log_login_attempt(self, user_id: Optional[int], identifier: str, success: bool, detail: str = None) -> None:
        """Log login attempt to activity log."""
        try:
            event_code = "LOGIN_OK" if success else "LOGIN_FAIL"
            detalle = {"identifier": identifier}
            if detail:
                detalle["motivo"] = detail
            self._append_activity_file_row(
                id_usuario=user_id,
                entidad="seguridad.usuario",
                id_entidad=user_id,
                accion=event_code,
                resultado="OK" if success else "FAIL",
                ip=self.current_ip,
                detalle=detalle,
            )
        except Exception as e:
            logger.error("Error logging login attempt", exc_info=e)


    def get_reporte_ventas(self, limit: int = 12) -> List[Dict[str, Any]]:
        query = "SELECT * FROM app.v_reporte_ventas_mensual LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_top_articulos(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = "SELECT * FROM app.v_top_articulos_mes WHERE total_facturado > 0 LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_bottom_articulos(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = """
            SELECT 
                a.id, a.nombre,
                COALESCE(SUM(dd.cantidad), 0) as cantidad_vendida,
                COALESCE(SUM(dd.total_linea), 0) as total_facturado
            FROM app.articulo a
            LEFT JOIN app.documento_detalle dd ON a.id = dd.id_articulo
            LEFT JOIN app.documento d ON dd.id_documento = d.id
            WHERE a.activo = true
              AND (d.id IS NULL OR (d.fecha >= date_trunc('month', now()) AND d.estado IN ('CONFIRMADO', 'PAGADO')))
            GROUP BY a.id, a.nombre
            HAVING COALESCE(SUM(dd.total_linea), 0) > 0
            ORDER BY total_facturado ASC, cantidad_vendida ASC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_alertas_stock(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = """
            SELECT id, nombre, stock_actual, stock_minimo, 
                   (stock_minimo - stock_actual) as faltante
            FROM app.v_articulo_detallado
            WHERE stock_actual <= stock_minimo AND activo = true
            ORDER BY faltante DESC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_stock_by_rubro(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Returns stock count grouped by rubro."""
        query = """
            SELECT r.nombre, COUNT(a.id) as cantidad
            FROM app.articulo a
            JOIN ref.rubro r ON a.id_rubro = r.id
            WHERE a.activo = true
            GROUP BY r.nombre
            ORDER BY cantidad DESC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_entidades_by_tipo(self) -> List[Dict[str, Any]]:
        """Returns entity count grouped by type."""
        query = """
            SELECT tipo as nombre, COUNT(*) as cantidad
            FROM app.entidad_comercial
            GROUP BY tipo
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def get_deudores(self, limit: int = 50) -> List[Dict[str, Any]]:
        query = "SELECT * FROM app.v_deudores LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return _rows_to_dicts(cur)

    def get_max_cost(self) -> float:
        query = "SELECT MAX(costo) FROM app.articulo"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                res = cur.fetchone()
                val = res.get("max") if isinstance(res, dict) else (res[0] if res else None)
                return float(val) if val is not None else 1000.0

    def close(self) -> None:
        """Gracefully close the connection pool and join worker threads."""
        self.is_closing = True
        if hasattr(self, 'pool') and self.pool:
            try:
                # Explicitly close the pool to join worker threads.
                # We catch RuntimeError in case this is called from a thread that cannot be joined.
                self.pool.close()
            except RuntimeError as e:
                if "cannot join current thread" in str(e):
                    logger.debug("Database.close() called from a thread that cannot be joined (likely a pool thread).")
                else:
                    logger.warning(f"RuntimeError closing database pool: {e}")
            except Exception as e:
                logger.error(f"Error closing database pool: {e}")
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
        search_by_cuit: bool = True,
    ) -> Tuple[str, List[Any]]:
        filters: List[str] = ["1=1"]
        params: List[Any] = []

        advanced = advanced or {}

        if not tipo:
            tipo = advanced.get("tipo")
        if isinstance(tipo, str):
            tipo = tipo.strip()
            if tipo.lower() in {"todos", "todas"}:
                tipo = None

        if tipo:
            tipo_upper = str(tipo).upper()
            if tipo_upper == "CLIENTE":
                filters.append("tipo IN ('CLIENTE', 'AMBOS')")
            elif tipo_upper == "PROVEEDOR":
                filters.append("tipo IN ('PROVEEDOR', 'AMBOS')")
            else:
                filters.append("tipo = %s")
                params.append(tipo_upper)

        if search:
            search_pattern = f"%{search.strip()}%"
            search_columns = [
                "id::text",
                "nombre_completo",
                "razon_social",
                "apellido",
                "nombre",
                "domicilio",
            ]
            if search_by_cuit:
                search_columns.insert(3, "cuit")
            search_sql = " OR ".join(f"{col} ILIKE %s" for col in search_columns)
            filters.append(f"({search_sql})")
            params.extend([search_pattern] * len(search_columns))

        cuit = advanced.get("cuit")
        if isinstance(cuit, str) and cuit.strip():
            filters.append("cuit ILIKE %s")
            params.append(f"%{cuit.strip()}%")

        id_localidad = _to_id(advanced.get("id_localidad"))
        if id_localidad is not None:
            filters.append("id_localidad = %s")
            params.append(id_localidad)

        id_provincia = _to_id(advanced.get("id_provincia"))
        if id_provincia is not None:
            filters.append("id_provincia = %s")
            params.append(id_provincia)

        id_lista_precio = _to_id(advanced.get("id_lista_precio"))
        if id_lista_precio is not None:
            filters.append("id_lista_precio = %s")
            params.append(id_lista_precio)

        id_condicion_iva = _to_id(advanced.get("id_condicion_iva"))
        if id_condicion_iva is not None:
            filters.append("id_condicion_iva = %s")
            params.append(id_condicion_iva)

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

        # Date range filters
        desde = advanced.get("desde")
        if isinstance(desde, str) and desde.strip():
            filters.append("fecha_creacion::date >= %s::date")
            params.append(desde.strip())

        hasta = advanced.get("hasta")
        if isinstance(hasta, str) and hasta.strip():
            filters.append("fecha_creacion::date <= %s::date")
            params.append(hasta.strip())

        # Additional text filters
        text_fields = {
            "apellido": "apellido",
            "nombre": "nombre",
            "razon_social": "razon_social",
            "domicilio": "domicilio",
            "email": "email",
            "telefono": "telefono",
            "notas": "notas",
            "localidad": "localidad",
            "provincia": "provincia",
            "condicion_iva": "condicion_iva",
            "lista_precio": "lista_precio",
        }
        skip_text = set()
        if id_localidad is not None:
            skip_text.add("localidad")
        if id_provincia is not None:
            skip_text.add("provincia")
        if id_lista_precio is not None:
            skip_text.add("lista_precio")
        if id_condicion_iva is not None:
            skip_text.add("condicion_iva")
        for key, col in text_fields.items():
            if key in skip_text:
                continue
            val = advanced.get(key)
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    continue
                if key == "condicion_iva" and val.lower() in {"todos", "todas"}:
                    continue
                filters.append(f"{col} ILIKE %s")
                params.append(f"%{val}%")

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
        search_by_cuit: bool = True,
    ) -> List[Dict[str, Any]]:
        where_clause, params = self._build_entity_filters(
            search,
            tipo,
            advanced,
            search_by_cuit=search_by_cuit,
        )

        sort_columns = {
            "id": "id",
            "codigo": "id",
            "tipo": "tipo",
            "nombre_completo": "nombre_completo",
            "apellido": "apellido",
            "nombre": "nombre",
            "razon_social": "razon_social",
            "cuit": "cuit",
            "domicilio": "domicilio",
            "telefono": "telefono",
            "email": "email",
            "localidad": "localidad",
            "id_localidad": "localidad",
            "provincia": "provincia",
            "id_provincia": "provincia",
            "condicion_iva": "condicion_iva",
            "lista_precio": "lista_precio",
            "id_lista_precio": "lista_precio",
            "descuento": "descuento",
            "saldo_cuenta": "saldo_cuenta",
            "fecha_creacion": "fecha_creacion",
            "activo": "activo",
        }
        order_by = self._build_order_by(sorts, sort_columns, default="nombre_completo ASC", tiebreaker="id ASC")

        query = f"""
            SELECT
                id,
                id::text AS codigo,
                tipo,
                nombre_completo,
                apellido,
                nombre,
                razon_social,
                cuit,
                domicilio,
                localidad,
                provincia,
                condicion_iva,
                telefono,
                email,
                notas,
                fecha_creacion,
                id_localidad,
                id_provincia,
                id_lista_precio,
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
        article_id_expr: str = "id",
    ) -> Tuple[str, List[Any]]:
        filters: List[str] = ["1=1"]
        params: List[Any] = []

        advanced = advanced or {}

        if activo_only is True:
            filters.append("activo = TRUE")
        elif activo_only is False:
            filters.append("activo = FALSE")

        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                f"(nombre ILIKE %s OR codigo ILIKE %s OR {article_id_expr}::text ILIKE %s)"
            )
            params.extend([pattern, pattern, pattern])

        def add_like(field: str, value: Any) -> None:
            if isinstance(value, str) and value.strip():
                filters.append(f"{field} ILIKE %s")
                params.append(f"%{value.strip()}%")

        add_like("nombre", advanced.get("nombre"))
        add_like("codigo", advanced.get("codigo"))
        
        marca_id = _to_id(advanced.get("id_marca"))
        if marca_id is not None:
            filters.append("id_marca = %s")
            params.append(marca_id)
        else:
            add_like("marca", advanced.get("marca")) # Keep text search as fallback or secondary

        rubro_id = _to_id(advanced.get("id_rubro"))
        if rubro_id is not None:
            filters.append("id_rubro = %s")
            params.append(rubro_id)
        else:
            add_like("rubro", advanced.get("rubro"))

        add_like("proveedor", advanced.get("proveedor"))

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

        # Range Stock filter
        stock_min = advanced.get("stock_min")
        stock_max = advanced.get("stock_max")
        try:
            if stock_min is not None and stock_min != "":
                filters.append("COALESCE(stock_actual, 0) >= %s")
                params.append(float(stock_min))
        except:
            pass
        try:
            if stock_max is not None and stock_max != "":
                filters.append("COALESCE(stock_actual, 0) <= %s")
                params.append(float(stock_max))
        except:
            pass

        stock_bajo = advanced.get("stock_bajo_minimo")
        if stock_bajo is True:
            filters.append("COALESCE(stock_actual, 0) < COALESCE(stock_minimo, 0)")

        # List price filter
        lp_id = _to_id(advanced.get("id_lista_precio"))
        if lp_id is not None:
             # If filtering by specific list, we just ensure the article has a price on that list
            filters.append(
                f"{article_id_expr} IN "
                "(SELECT id_articulo FROM app.articulo_precio WHERE id_lista_precio = %s)"
            )
            params.append(lp_id)

        iva_id = _to_id(advanced.get("id_tipo_iva"))
        if iva_id is not None:
            filters.append("id_tipo_iva = %s")
            params.append(iva_id)

        unidad_id = _to_id(advanced.get("id_unidad_medida"))
        if unidad_id is not None:
            filters.append("id_unidad_medida = %s")
            params.append(unidad_id)

        prov_id = _to_id(advanced.get("id_proveedor"))
        if prov_id is not None:
            filters.append("id_proveedor = %s")
            params.append(prov_id)

        ubicacion = advanced.get("ubicacion_exacta")
        if ubicacion not in (None, "", "Todas", "Todas/os"):
            filters.append("ubicacion = %s")
            params.append(str(ubicacion))

        redon = advanced.get("redondeo")
        if redon == "SI":
            filters.append("redondeo = TRUE")
        elif redon == "NO":
            filters.append("redondeo = FALSE")

        return " AND ".join(filters), params

    def _apply_mass_update_operation(self, current: Any, operation: str, value: Any) -> float:
        try:
            current_val = float(current or 0)
        except Exception:
            current_val = 0.0
        try:
            op_value = float(value or 0)
        except Exception:
            op_value = 0.0

        if operation == "PCT_ADD":
            new_val = current_val * (1 + op_value / 100.0)
        elif operation == "PCT_SUB":
            new_val = current_val * (1 - op_value / 100.0)
        elif operation == "AMT_ADD":
            new_val = current_val + op_value
        elif operation == "AMT_SUB":
            new_val = current_val - op_value
        elif operation == "SET_VAL":
            new_val = op_value
        else:
            new_val = current_val
        return max(0.0, new_val)

    def _build_mass_update_sql_expr(self, base_expr: str, operation: str, value: float) -> Optional[str]:
        val_sql = str(float(value))
        if operation == "PCT_ADD":
            return f"{base_expr} * (1 + {val_sql}/100.0)"
        if operation == "PCT_SUB":
            return f"{base_expr} * (1 - {val_sql}/100.0)"
        if operation == "AMT_ADD":
            return f"{base_expr} + {val_sql}"
        if operation == "AMT_SUB":
            return f"{base_expr} - {val_sql}"
        if operation == "SET_VAL":
            return f"{val_sql}"
        return None

    def _price_factor_from_tipo(self, tipo: Any, porcentaje: Any) -> float:
        raw = str(tipo or "").strip().upper()
        try:
            pct = float(porcentaje or 0)
        except Exception:
            pct = 0.0
        pct = max(0.0, pct)
        if "DESC" in raw:
            return 1.0 - (pct / 100.0)
        return 1.0 + (pct / 100.0)

    def _calc_price_from_cost_factor(self, cost: Any, factor: float) -> float:
        try:
            safe_cost = max(0.0, float(cost or 0))
        except Exception:
            safe_cost = 0.0
        return max(0.0, safe_cost * factor)

    def _calc_diff_pct(self, current: Any, new_value: Any) -> float:
        try:
            cur_val = float(current or 0)
        except Exception:
            cur_val = 0.0
        try:
            new_val = float(new_value or 0)
        except Exception:
            new_val = 0.0
        if cur_val <= 0:
            return 0.0
        return ((new_val - cur_val) / cur_val) * 100.0

    def preview_mass_update(
        self,
        filters: Dict[str, Any],
        target: str, # 'COSTO' or 'LISTA_PRECIO'
        operation: str, # 'PCT_ADD', 'PCT_SUB', 'AMT_ADD', 'AMT_SUB', 'SET_VAL'
        value: float,
        list_id: Optional[int] = None,
        limit: Optional[int] = 5,
        offset: int = 0
    ) -> Dict[str, Any]:
        where_clause, params = self._build_article_filters(
            search=filters.get("search"),
            activo_only=filters.get("activo_only"),
            advanced=filters,
            article_id_expr="a.id",
        )

        limit_offset_sql = ""
        paging_params: List[Any] = []
        if limit is not None:
            limit_offset_sql += " LIMIT %s"
            paging_params.append(int(limit))
        if offset > 0:
            limit_offset_sql += " OFFSET %s"
            paging_params.append(int(offset))

        payload: Dict[str, Any] = {
            "rows": [],
            "meta": {
                "target_mode": target,
                "active_lists": [],
                "selected_list": None,
                "skipped_invalid_factor": 0,
            },
        }

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                if target == "COSTO":
                    cur.execute(
                        """
                        SELECT id, nombre
                        FROM ref.lista_precio
                        WHERE activa = TRUE
                        ORDER BY orden ASC, id ASC
                        """
                    )
                    active_lists = _rows_to_dicts(cur)
                    payload["meta"]["active_lists"] = active_lists

                    query = f"""
                        SELECT a.id, a.nombre, a.costo
                        FROM app.articulo a
                        WHERE {where_clause}
                        ORDER BY a.nombre ASC, a.id ASC
                        {limit_offset_sql}
                    """
                    query_params = params + paging_params
                    cur.execute(query, query_params)
                    article_rows = cur.fetchall()

                    article_ids = [int(r[0]) for r in article_rows]
                    active_list_ids = [int(lp["id"]) for lp in active_lists]
                    prices_by_article: Dict[int, Dict[int, Dict[str, Any]]] = {}
                    if article_ids and active_list_ids:
                        article_placeholders = ",".join(["%s"] * len(article_ids))
                        list_placeholders = ",".join(["%s"] * len(active_list_ids))
                        prices_sql = f"""
                            SELECT
                                ap.id_articulo,
                                ap.id_lista_precio,
                                ap.precio,
                                ap.porcentaje,
                                tp.tipo
                            FROM app.articulo_precio ap
                            LEFT JOIN ref.tipo_porcentaje tp ON tp.id = ap.id_tipo_porcentaje
                            WHERE ap.id_articulo IN ({article_placeholders})
                              AND ap.id_lista_precio IN ({list_placeholders})
                        """
                        cur.execute(prices_sql, article_ids + active_list_ids)
                        for p_row in cur.fetchall():
                            art_id = int(p_row[0])
                            lp_id = int(p_row[1])
                            prices_by_article.setdefault(art_id, {})[lp_id] = {
                                "precio": float(p_row[2] or 0),
                                "porcentaje": float(p_row[3] or 0),
                                "tipo": p_row[4],
                            }

                    rows: List[Dict[str, Any]] = []
                    for art_row in article_rows:
                        art_id = int(art_row[0])
                        name = art_row[1]
                        current_cost = float(art_row[2] or 0)
                        new_cost = self._apply_mass_update_operation(current_cost, operation, value)
                        list_changes: Dict[int, Optional[Dict[str, float]]] = {}
                        article_prices = prices_by_article.get(art_id, {})

                        for lp in active_lists:
                            lp_id = int(lp["id"])
                            price_info = article_prices.get(lp_id)
                            if not price_info:
                                list_changes[lp_id] = None
                                continue

                            current_price = float(price_info.get("precio") or 0)
                            factor = self._price_factor_from_tipo(
                                price_info.get("tipo"),
                                price_info.get("porcentaje"),
                            )
                            new_price = self._calc_price_from_cost_factor(new_cost, factor)
                            list_changes[lp_id] = {
                                "current": current_price,
                                "new": new_price,
                                "diff_pct": self._calc_diff_pct(current_price, new_price),
                            }

                        rows.append(
                            {
                                "id": art_id,
                                "nombre": name,
                                "costo_current": current_cost,
                                "costo_new": new_cost,
                                "costo_diff_pct": self._calc_diff_pct(current_cost, new_cost),
                                "list_changes": list_changes,
                            }
                        )

                    payload["rows"] = rows
                    return payload

                if target == "LISTA_PRECIO" and list_id is not None:
                    list_id_int = int(list_id)
                    cur.execute(
                        "SELECT id, nombre FROM ref.lista_precio WHERE id = %s",
                        (list_id_int,),
                    )
                    selected = cur.fetchone()
                    if selected:
                        payload["meta"]["selected_list"] = {
                            "id": int(selected[0]),
                            "nombre": selected[1],
                        }

                    query = f"""
                        SELECT
                            a.id,
                            a.nombre,
                            a.costo,
                            ap.precio,
                            ap.porcentaje,
                            tp.tipo
                        FROM app.articulo a
                        JOIN app.articulo_precio ap
                          ON ap.id_articulo = a.id
                         AND ap.id_lista_precio = %s
                        LEFT JOIN ref.tipo_porcentaje tp ON tp.id = ap.id_tipo_porcentaje
                        WHERE {where_clause}
                        ORDER BY a.nombre ASC, a.id ASC
                        {limit_offset_sql}
                    """
                    query_params = [list_id_int] + params + paging_params
                    cur.execute(query, query_params)
                    rows: List[Dict[str, Any]] = []
                    skipped_invalid_factor = 0

                    for r in cur.fetchall():
                        art_id = int(r[0])
                        name = r[1]
                        current_cost = float(r[2] or 0)
                        current_price = float(r[3] or 0)
                        factor = self._price_factor_from_tipo(r[5], r[4])

                        new_selected_price = self._apply_mass_update_operation(current_price, operation, value)
                        if factor <= 0:
                            skipped_invalid_factor += 1
                            continue

                        new_cost = max(0.0, new_selected_price / factor)
                        rows.append(
                            {
                                "id": art_id,
                                "nombre": name,
                                "costo_current": current_cost,
                                "costo_new": new_cost,
                                "costo_diff_pct": self._calc_diff_pct(current_cost, new_cost),
                                "selected_current": current_price,
                                "selected_new": new_selected_price,
                                "selected_diff_pct": self._calc_diff_pct(current_price, new_selected_price),
                            }
                        )

                    payload["rows"] = rows
                    payload["meta"]["skipped_invalid_factor"] = skipped_invalid_factor
                    return payload

        return payload

    def mass_update_articles(
        self,
        filters: Dict[str, Any],
        target: str,
        operation: str,
        value: float,
        list_id: Optional[int] = None,
        ids: Optional[List[int]] = None
    ) -> Dict[str, int]:
        list_id_int = int(list_id) if list_id is not None else None
        summary = {"updated_count": 0, "skipped_invalid_factor": 0}

        expr = self._build_mass_update_sql_expr("current_val", operation, value)
        if not expr:
            return summary

        if ids:
            id_placeholders = ",".join(["%s"] * len(ids))
            where_clause = f"id IN ({id_placeholders})"
            params = list(ids)
        else:
            where_clause, params = self._build_article_filters(
                search=filters.get("search"),
                activo_only=filters.get("activo_only"),
                advanced=filters,
                article_id_expr="a.id",
            )
        with self._transaction() as cur:
            if target == "COSTO":
                expr_cost = expr.replace("current_val", "costo")
                sql = f"""
                    UPDATE app.articulo a
                    SET costo = GREATEST(0, {expr_cost})
                    WHERE {where_clause}
                """
                cur.execute(sql, params)
                summary["updated_count"] = cur.rowcount

                sql_prices = f"""
                    WITH target_articles AS (
                        SELECT a.id
                        FROM app.articulo a
                        WHERE {where_clause}
                    )
                    UPDATE app.articulo_precio ap
                    SET precio = GREATEST(
                            0,
                            CASE
                                WHEN COALESCE(
                                    (
                                        SELECT tp.tipo
                                        FROM ref.tipo_porcentaje tp
                                        WHERE tp.id = ap.id_tipo_porcentaje
                                    ),
                                    'MARGEN'
                                ) = 'DESCUENTO'
                                    THEN a.costo * (1 - COALESCE(ap.porcentaje, 0) / 100.0)
                                ELSE
                                    a.costo * (1 + COALESCE(ap.porcentaje, 0) / 100.0)
                            END
                        ),
                        fecha_actualizacion = now()
                    FROM app.articulo a
                    WHERE ap.id_articulo = a.id
                      AND a.id IN (SELECT id FROM target_articles)
                """
                cur.execute(sql_prices, params)

            elif target == "LISTA_PRECIO" and list_id_int:
                expr_selected = expr.replace("current_val", "ap.precio")
                sql_cost = f"""
                    WITH target_articles AS (
                        SELECT a.id
                        FROM app.articulo a
                        WHERE {where_clause}
                    ),
                    selected_base AS (
                        SELECT
                            a.id AS id_articulo,
                            GREATEST(0, {expr_selected}) AS selected_new_price,
                            CASE
                                WHEN COALESCE(tp.tipo, 'MARGEN') = 'DESCUENTO'
                                    THEN 1 - COALESCE(ap.porcentaje, 0) / 100.0
                                ELSE
                                    1 + COALESCE(ap.porcentaje, 0) / 100.0
                            END AS factor
                        FROM app.articulo a
                        JOIN target_articles ta ON ta.id = a.id
                        JOIN app.articulo_precio ap
                          ON ap.id_articulo = a.id
                         AND ap.id_lista_precio = %s
                        LEFT JOIN ref.tipo_porcentaje tp ON tp.id = ap.id_tipo_porcentaje
                    ),
                    invalid AS (
                        SELECT id_articulo
                        FROM selected_base
                        WHERE factor <= 0
                    ),
                    valid AS (
                        SELECT id_articulo, selected_new_price, factor
                        FROM selected_base
                        WHERE factor > 0
                    ),
                    updated_cost AS (
                        UPDATE app.articulo a
                        SET costo = GREATEST(0, v.selected_new_price / v.factor)
                        FROM valid v
                        WHERE a.id = v.id_articulo
                        RETURNING a.id
                    )
                    SELECT
                        (SELECT COUNT(*)::int FROM updated_cost) AS updated_count,
                        (SELECT COUNT(*)::int FROM invalid) AS skipped_invalid_factor
                """
                cur.execute(sql_cost, params + [list_id_int])
                stats_row = cur.fetchone()
                if isinstance(stats_row, dict):
                    summary["updated_count"] = int(stats_row.get("updated_count") or 0)
                    summary["skipped_invalid_factor"] = int(stats_row.get("skipped_invalid_factor") or 0)
                elif stats_row:
                    summary["updated_count"] = int(stats_row[0] or 0)
                    summary["skipped_invalid_factor"] = int(stats_row[1] or 0)

                sql_prices = f"""
                    WITH target_articles AS (
                        SELECT a.id
                        FROM app.articulo a
                        WHERE {where_clause}
                    ),
                    valid_articles AS (
                        SELECT a.id
                        FROM app.articulo a
                        JOIN target_articles ta ON ta.id = a.id
                        JOIN app.articulo_precio ap_sel
                          ON ap_sel.id_articulo = a.id
                         AND ap_sel.id_lista_precio = %s
                        LEFT JOIN ref.tipo_porcentaje tp_sel ON tp_sel.id = ap_sel.id_tipo_porcentaje
                        WHERE (
                            CASE
                                WHEN COALESCE(tp_sel.tipo, 'MARGEN') = 'DESCUENTO'
                                    THEN 1 - COALESCE(ap_sel.porcentaje, 0) / 100.0
                                ELSE
                                    1 + COALESCE(ap_sel.porcentaje, 0) / 100.0
                            END
                        ) > 0
                    )
                    UPDATE app.articulo_precio ap
                    SET precio = GREATEST(
                            0,
                            CASE
                                WHEN COALESCE(
                                    (
                                        SELECT tp.tipo
                                        FROM ref.tipo_porcentaje tp
                                        WHERE tp.id = ap.id_tipo_porcentaje
                                    ),
                                    'MARGEN'
                                ) = 'DESCUENTO'
                                    THEN a.costo * (1 - COALESCE(ap.porcentaje, 0) / 100.0)
                                ELSE
                                    a.costo * (1 + COALESCE(ap.porcentaje, 0) / 100.0)
                            END
                        ),
                        fecha_actualizacion = now()
                    FROM app.articulo a
                    WHERE ap.id_articulo = a.id
                      AND a.id IN (SELECT id FROM valid_articles)
                """
                cur.execute(sql_prices, params + [list_id_int])

            self.log_activity("ARTICULO", "MASS_UPDATE", detalle={
                "target": target,
                "operation": operation,
                "value": value,
                "count": summary["updated_count"],
                "list_id": list_id_int,
                "skipped_invalid_factor": summary["skipped_invalid_factor"],
            })

        return summary

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
        where_clause, params = self._build_article_filters(
            search,
            activo_only,
            advanced,
            article_id_expr="ad.id",
        )
        order_by = _build_article_order_by_clause(sorts, _ARTICLE_SORT_COLUMNS)

        lp_id = _to_id((advanced or {}).get("id_lista_precio"))
        if lp_id is not None:
            query = f"""
                SELECT ad.*, ap.precio as precio_lista
                FROM app.v_articulo_detallado ad
                LEFT JOIN app.articulo_precio ap ON ad.id = ap.id_articulo AND ap.id_lista_precio = %s
            """
            params.insert(0, lp_id)
        else:
            query = f"""
                SELECT ad.*
                FROM app.v_articulo_detallado ad
            """

        # Build complete query with WHERE and ORDER BY clauses
        # where_clause is safe (from _build_article_filters)
        # order_by is safe (built from validated sort_columns mapping)
        query = f"{query} WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
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
        where_clause, params = self._build_article_filters(
            search,
            activo_only,
            advanced,
            article_id_expr="ad.id",
        )
        # where_clause is safe (from _build_article_filters with parametrized conditions)
        query = f"SELECT COUNT(*) AS total FROM app.v_articulo_detallado ad WHERE {where_clause}"
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
                cur.execute("SELECT * FROM app.v_articulo_detallado WHERE id_articulo = %s", (article_id,))
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
            logger.error("Error updating article status", exc_info=e)
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
            filters.append("nombre ILIKE %s")
            params.append(f"%{search.strip()}%")
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

    def list_marcas_full(self) -> List[Dict[str, Any]]:
        return self._get_cached_catalog_raw("marcas_full", "SELECT id, nombre FROM ref.marca ORDER BY nombre")

    def list_rubros_full(self) -> List[Dict[str, Any]]:
        return self._get_cached_catalog_raw("rubros_full", "SELECT id, nombre FROM ref.rubro ORDER BY nombre")

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


    def _build_catalog_filters(
        self,
        search: Optional[str],
        columns: Optional[Sequence[str]] = None,
    ) -> Tuple[str, List[Any]]:
        filters: List[str] = ["1=1"]
        params: List[Any] = []
        if isinstance(search, str) and search.strip():
            pattern = f"%{search.strip()}%"
            cols = list(columns) if columns else ["nombre"]
            if len(cols) == 1:
                filters.append(f"{cols[0]} ILIKE %s")
                params.append(pattern)
            else:
                filters.append("(" + " OR ".join([f"{col} ILIKE %s" for col in cols]) + ")")
                params.extend([pattern] * len(cols))
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
        where_clause, params = self._build_catalog_filters(search, columns=("nombre",))
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
        where_clause, params = self._build_catalog_filters(search, columns=("nombre",))
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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
                    self.invalidate_catalog_cache("marcas")
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: una o más marcas están asignadas a artículos. "
                "Primero reasigná los artículos a otra marca."
            )

    def fetch_rubros(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 80,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search, columns=("nombre",))
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
        where_clause, params = self._build_catalog_filters(search, columns=("nombre",))
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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
                    self.invalidate_catalog_cache("rubros")
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: uno o más rubros están asignados a artículos. "
                "Primero reasigná los artículos a otro rubro."
            )

    def count_stock_alerts(self, search: Optional[str] = None) -> int:
        filters: List[str] = ["COALESCE(stock_actual, 0) < COALESCE(stock_minimo, 0)"]
        params: List[Any] = []
        if search:
            filters.append("nombre ILIKE %s")
            params.append(f"%{search.strip()}%")
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM app.v_articulo_detallado WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def create_entity_full(
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
        id_localidad: Optional[int] = None,
        id_condicion_iva: Optional[int] = None,
        # Pricing info
        id_lista_precio: Optional[int] = None,
        descuento: float = 0,
        limite_credito: float = 0,
    ) -> int:
        """Atomic operation to create an entity and its associated pricing info."""
        with self._transaction() as cur:
            # 1. Create Entity
            def clean(value: Any) -> Optional[str]:
                if value is None: return None
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

            q_ent = """
                INSERT INTO app.entidad_comercial (
                    nombre, apellido, razon_social, cuit, telefono, email, 
                    domicilio, tipo, activo, notas, id_localidad, id_condicion_iva, 
                    fecha_creacion, fecha_actualizacion
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                RETURNING id
            """
            cur.execute(q_ent, (
                nombre_clean, apellido_clean, razon_social_clean, clean(cuit),
                clean(telefono), clean(email), clean(domicilio), tipo_clean,
                bool(activo), clean(notas), id_localidad, id_condicion_iva
            ))
            entity_id = cur.fetchone()[0]

            # 2. Assign Pricing List if provided
            if id_lista_precio and str(id_lista_precio).strip():
                q_list = """
                    INSERT INTO app.lista_cliente (id_entidad_comercial, id_lista_precio, descuento, limite_credito)
                    VALUES (%s, %s, %s, %s)
                """
                cur.execute(q_list, (entity_id, int(id_lista_precio), descuento, limite_credito))
            
            
            self.invalidate_catalog_cache("proveedores")
            
            self.log_activity(
                entidad="app.entidad_comercial",
                accion="ALTA",
                id_entidad=entity_id,
                detalle={"tipo": tipo_clean, "nombre": nombre_clean or razon_social_clean}
            )
            
            return entity_id

    def update_entity_full(
        self,
        entity_id: int,
        *,
        updates: Dict[str, Any],
        # Pricing info
        id_lista_precio: Optional[Any] = None,
        descuento: float = 0,
        limite_credito: float = 0,
    ) -> None:
        """Atomic operation to update entity fields and its associated pricing info."""
        with self._transaction() as cur:
            # 1. Update Entity fields if any
            if updates:
                # Ensure date is updated
                updates['fecha_actualizacion'] = datetime.now()
                
                # Check tipo
                if 'tipo' in updates and updates['tipo']:
                    updates['tipo'] = updates['tipo'].upper()
                    if updates['tipo'] not in {"CLIENTE", "PROVEEDOR", "AMBOS"}:
                        raise ValueError("Tipo inválido.")

                cols = []
                vals = []
                for k, v in updates.items():
                    cols.append(f"{k} = %s")
                    vals.append(v)
                vals.append(entity_id)
                
                q_upd = f"UPDATE app.entidad_comercial SET {', '.join(cols)} WHERE id = %s"
                cur.execute(q_upd, vals)

            # 2. Update Pricing List
            if id_lista_precio is None or str(id_lista_precio).strip() == "":
                cur.execute("DELETE FROM app.lista_cliente WHERE id_entidad_comercial = %s", (entity_id,))
            else:
                q_list = """
                    INSERT INTO app.lista_cliente (id_entidad_comercial, id_lista_precio, descuento, limite_credito)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id_entidad_comercial) DO UPDATE SET
                        id_lista_precio = EXCLUDED.id_lista_precio,
                        descuento = EXCLUDED.descuento,
                        limite_credito = EXCLUDED.limite_credito
                """
                cur.execute(q_list, (entity_id, int(id_lista_precio), descuento, limite_credito))
            
            self.invalidate_catalog_cache("proveedores")
            
            self.log_activity(
                entidad="app.entidad_comercial",
                accion="MODIFICACION",
                id_entidad=entity_id,
                detalle={"updates": updates, "id_lista_precio": id_lista_precio}
            )

    def create_entity(self, **kwargs) -> int:
        """Legacy wrapper for create_entity_full"""
        return self.create_entity_full(**kwargs)

    def update_entity_fields(self, entity_id: int, updates: Dict[str, Any]) -> None:
        allowed = {
            "nombre",
            "apellido",
            "razon_social",
            "cuit",
            "domicilio",
            "telefono",
            "email",
            "activo",
            "tipo",
            "notas",
            "id_localidad",
            "id_provincia",
            "id_condicion_iva",
            "condicion_iva",
            "lista_precio",
            "id_lista_precio",
        }
        missing = object()
        lista_raw = updates.get("id_lista_precio", missing)
        lista_nombre = updates.get("lista_precio", missing)
        filtered = {
            k: v
            for k, v in updates.items()
            if k in allowed and k not in {"id_lista_precio", "lista_precio"}
        }
        if not filtered and lista_raw is missing and lista_nombre is missing:
            return

        if "activo" in filtered and isinstance(filtered["activo"], str):
            raw = filtered["activo"].strip().lower()
            if raw in {"1", "true", "si", "sí", "activo", "yes"}:
                filtered["activo"] = True
            elif raw in {"0", "false", "no", "inactivo"}:
                filtered["activo"] = False
            else:
                raise ValueError("Valor inválido para activo (usa true/false).")

        lista_update = lista_raw is not missing or lista_nombre is not missing
        lista_id: Optional[int] = None

        with self._transaction() as cur:
            if "condicion_iva" in filtered:
                iva_nombre = filtered.pop("condicion_iva")
                if iva_nombre:
                    cur.execute("SELECT id FROM ref.condicion_iva WHERE nombre = %s", (iva_nombre,))
                    res = cur.fetchone()
                    iva_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                    if iva_id:
                        filtered["id_condicion_iva"] = iva_id
                    else:
                        raise ValueError(f"Condición de IVA inválida: {iva_nombre}")

            if lista_update:
                if lista_raw is not missing:
                    if lista_raw is None or str(lista_raw).strip() == "":
                        lista_id = None
                    else:
                        try:
                            lista_id = int(lista_raw)
                        except (TypeError, ValueError):
                            raise ValueError("Lista de Precio inválida.")
                else:
                    if lista_nombre:
                        cur.execute("SELECT id FROM ref.lista_precio WHERE nombre = %s", (lista_nombre,))
                        res = cur.fetchone()
                        lista_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                        if not lista_id:
                            raise ValueError(f"Lista de Precio inválida: {lista_nombre}")
                    else:
                        lista_id = None

            if "id_provincia" in filtered and "id_localidad" not in filtered:
                prov_raw = filtered.pop("id_provincia")
                if prov_raw is None or str(prov_raw).strip() == "":
                    filtered["id_localidad"] = None
                else:
                    try:
                        prov_id = int(prov_raw)
                    except (TypeError, ValueError):
                        raise ValueError("Provincia inválida.")
                    cur.execute(
                        """
                        SELECT l.nombre
                        FROM app.entidad_comercial e
                        LEFT JOIN ref.localidad l ON l.id = e.id_localidad
                        WHERE e.id = %s
                        """,
                        (entity_id,),
                    )
                    res = cur.fetchone()
                    current_localidad = res.get("nombre") if isinstance(res, dict) else (res[0] if res else None)
                    loc_id = None
                    if current_localidad:
                        cur.execute(
                            """
                            SELECT id
                            FROM ref.localidad
                            WHERE id_provincia = %s AND lower(nombre) = lower(%s)
                            LIMIT 1
                            """,
                            (prov_id, current_localidad),
                        )
                        res = cur.fetchone()
                        loc_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                    if not loc_id:
                        # Keep province/locality consistent by picking a valid locality in the new province.
                        cur.execute(
                            "SELECT id FROM ref.localidad WHERE id_provincia = %s ORDER BY nombre LIMIT 1",
                            (prov_id,),
                        )
                        res = cur.fetchone()
                        loc_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                    filtered["id_localidad"] = loc_id
            else:
                filtered.pop("id_provincia", None)

            if "id_localidad" in filtered:
                raw_localidad = filtered["id_localidad"]
                if raw_localidad is None or str(raw_localidad).strip() == "":
                    filtered["id_localidad"] = None
                else:
                    try:
                        filtered["id_localidad"] = int(raw_localidad)
                    except (TypeError, ValueError):
                        raise ValueError("Localidad inválida.")

            if filtered:
                assignments = ", ".join(f"{col} = %s" for col in filtered)
                params = [filtered[col] for col in filtered]
                params.append(entity_id)
                query = f"UPDATE app.entidad_comercial SET {assignments} WHERE id = %s"
                cur.execute(query, params)

            if lista_update:
                if lista_id is None or str(lista_id).strip() == "":
                    cur.execute("DELETE FROM app.lista_cliente WHERE id_entidad_comercial = %s", (entity_id,))
                else:
                    cur.execute(
                        "SELECT descuento, limite_credito FROM app.lista_cliente WHERE id_entidad_comercial = %s",
                        (entity_id,),
                    )
                    res = cur.fetchone()
                    descuento = res.get("descuento") if isinstance(res, dict) else (res[0] if res else 0)
                    limite_credito = res.get("limite_credito") if isinstance(res, dict) else (res[1] if res else 0)
                    q_list = """
                        INSERT INTO app.lista_cliente (id_entidad_comercial, id_lista_precio, descuento, limite_credito)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id_entidad_comercial) DO UPDATE SET
                            id_lista_precio = EXCLUDED.id_lista_precio,
                            descuento = EXCLUDED.descuento,
                            limite_credito = EXCLUDED.limite_credito
                    """
                    cur.execute(q_list, (entity_id, int(lista_id), descuento, limite_credito))

        self.invalidate_catalog_cache("proveedores")
        detalle_log: Dict[str, Any] = {"updates": dict(filtered)}
        if lista_update:
            detalle_log["id_lista_precio"] = lista_id
        self.log_activity(
            entidad="app.entidad_comercial",
            accion="MODIFICACION",
            id_entidad=entity_id,
            detalle=detalle_log,
        )

    def bulk_update_entities(self, ids: Sequence[int], updates: Dict[str, Any]) -> None:
        if not ids or not updates: return
        cols = ", ".join([f"{k} = %s" for k in updates.keys()])
        params = list(updates.values())
        params.append(list(ids))
        query = f"UPDATE app.entidad_comercial SET {cols}, fecha_actualizacion = now() WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                count = cur.rowcount
                conn.commit()
                
                self.log_activity(
                    entidad="app.entidad_comercial",
                    accion="UPDATE_MASIVO",
                    detalle={"count": count, "updates": updates, "ids": list(ids)[:50]}
                )

    def delete_entities(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        query = "DELETE FROM app.entidad_comercial WHERE id = ANY(%s)"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
                    self.invalidate_catalog_cache("proveedores")
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: la entidad tiene documentos, artículos o movimientos asociados. "
                "Desactivapla en su lugar."
            )

    def create_article(
        self,
        *,
        nombre: str,
        codigo: Optional[str] = None,
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
        porcentaje_ganancia_2: Any = None,
        unidades_por_bulto: Any = None,
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
        codigo_clean = clean(codigo)

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
                pgan2_value = coerce_non_negative_number(porcentaje_ganancia_2, "ganancia 2") if porcentaje_ganancia_2 is not None else None
                unidades_por_bulto_value = _coerce_optional_positive_int(
                    unidades_por_bulto,
                    "unidades por bulto",
                )

                if codigo_clean:
                    cur.execute(
                        "SELECT id FROM app.articulo WHERE lower(codigo) = lower(%s) LIMIT 1",
                        (codigo_clean,),
                    )
                    if cur.fetchone():
                        raise ValueError("Ya existe un artículo con ese código.")

                query = """
                    INSERT INTO app.articulo (
                        nombre,
                        codigo,
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
                        redondeo,
                        porcentaje_ganancia_2,
                        unidades_por_bulto
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cur.execute(
                    query,
                    (
                        nombre_clean,
                        codigo_clean,
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
                        bool(redondeo),
                        pgan2_value,
                        unidades_por_bulto_value,
                    ),
                )
                res = cur.fetchone()
                art_id = res.get("id") if isinstance(res, dict) else res[0]
                conn.commit()
                
                self.log_activity(
                    entidad="app.articulo",
                    accion="ALTA",
                    id_entidad=art_id,
                    detalle={"nombre": nombre_clean, "codigo": codigo_clean, "costo": costo_value}
                )
                
                return art_id

    def update_article_fields(self, article_id: int, updates: Dict[str, Any]) -> None:
        allowed = {
            "nombre", "codigo", "costo", "stock_minimo", "activo", 
            "marca", "rubro", "ubicacion", "observacion",
            "id_tipo_iva", "id_unidad_medida", "id_proveedor",
            "descuento_base", "redondeo", "porcentaje_ganancia_2",
            "unidades_por_bulto",
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
        if "porcentaje_ganancia_2" in filtered:
            filtered["porcentaje_ganancia_2"] = coerce_float(filtered["porcentaje_ganancia_2"], "porcentaje_ganancia_2")
        if "unidades_por_bulto" in filtered:
            filtered["unidades_por_bulto"] = _coerce_optional_positive_int(
                filtered["unidades_por_bulto"],
                "unidades por bulto",
            )
        if "activo" in filtered and isinstance(filtered["activo"], str):
            raw = filtered["activo"].strip().lower()
            if raw in {"1", "true", "si", "sí", "activo", "yes"}:
                filtered["activo"] = True
            elif raw in {"0", "false", "no", "inactivo"}:
                filtered["activo"] = False
            else:
                raise ValueError("Valor inválido para activo (usa true/false).")
        if "codigo" in filtered:
            codigo_raw = filtered["codigo"]
            codigo_clean = str(codigo_raw).strip() if codigo_raw is not None else ""
            filtered["codigo"] = codigo_clean or None
            if codigo_clean:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT id
                            FROM app.articulo
                            WHERE lower(codigo) = lower(%s) AND id <> %s
                            LIMIT 1
                            """,
                            (codigo_clean, int(article_id)),
                        )
                        if cur.fetchone():
                            raise ValueError("Ya existe otro artículo con ese código.")
        
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

        if "id_tipo_iva" in filtered and isinstance(filtered["id_tipo_iva"], str):
            iva_desc = filtered.pop("id_tipo_iva")
            if iva_desc in (None, ""):
                assignments.append("id_tipo_iva = NULL")
            else:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM ref.tipo_iva WHERE descripcion = %s", (str(iva_desc),))
                        res = cur.fetchone()
                        iva_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                if iva_id is None:
                    raise ValueError(f"Alícuota de IVA inválida: {iva_desc}")
                assignments.append("id_tipo_iva = %s")
                params.append(iva_id)

        if "id_unidad_medida" in filtered and isinstance(filtered["id_unidad_medida"], str):
            um_name = filtered.pop("id_unidad_medida")
            if um_name in (None, ""):
                assignments.append("id_unidad_medida = NULL")
            else:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM ref.unidad_medida WHERE nombre = %s OR abreviatura = %s", (str(um_name), str(um_name)))
                        res = cur.fetchone()
                        um_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                if um_id is None:
                    raise ValueError(f"Unidad de medida inválida: {um_name}")
                assignments.append("id_unidad_medida = %s")
                params.append(um_id)

        if "id_proveedor" in filtered and isinstance(filtered["id_proveedor"], str):
            prov_name = filtered.pop("id_proveedor")
            if prov_name in (None, ""):
                assignments.append("id_proveedor = NULL")
            else:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        q = """
                            SELECT id FROM app.entidad_comercial 
                            WHERE (razon_social = %s OR TRIM(COALESCE(apellido, '') || ' ' || COALESCE(nombre, '')) = %s)
                            AND tipo IN ('PROVEEDOR', 'AMBOS')
                        """
                        cur.execute(q, (str(prov_name), str(prov_name)))
                        res = cur.fetchone()
                        prov_id = res.get("id") if isinstance(res, dict) else (res[0] if res else None)
                if prov_id is None:
                    raise ValueError(f"Proveedor inválido: {prov_name}")
                assignments.append("id_proveedor = %s")
                params.append(prov_id)

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
                
                self.log_activity(
                    entidad="app.articulo",
                    accion="MODIFICACION",
                    id_entidad=article_id,
                    detalle={"updates": updates}
                )

    def fetch_article_by_id(self, article_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT 
                a.id, a.nombre, a.id_marca, m.nombre as marca_nombre,
                a.codigo,
                a.id_rubro, r.nombre as rubro_nombre,
                a.costo, a.stock_minimo, a.ubicacion, a.activo, a.observacion,
                a.id_tipo_iva, a.id_unidad_medida, a.id_proveedor,
                a.descuento_base, a.redondeo, a.porcentaje_ganancia_2, a.unidades_por_bulto,
                COALESCE(sr.stock_total, 0) as stock_actual
            FROM app.articulo a
            LEFT JOIN ref.marca m ON a.id_marca = m.id
            LEFT JOIN ref.rubro r ON a.id_rubro = r.id
            LEFT JOIN app.articulo_stock_resumen sr ON a.id = sr.id_articulo
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
        if not prices:
            return
        params = [
            (
                article_id,
                p["id_lista_precio"],
                p.get("precio"),
                p.get("porcentaje"),
                p.get("id_tipo_porcentaje"),
            )
            for p in prices
        ]
        with self._transaction() as cur:
            cur.executemany(query, params)



    def bulk_update_articles(self, ids: Sequence[int], updates: Dict[str, Any]) -> None:
        if not ids or not updates: return
        cols = ", ".join([f"{k} = %s" for k in updates.keys()])
        params = list(updates.values())
        params.append(list(ids))
        query = f"UPDATE app.articulo SET {cols} WHERE id = ANY(%s)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, params)
                count = cur.rowcount
                conn.commit()
                
                self.log_activity(
                    entidad="app.articulo",
                    accion="UPDATE_MASIVO",
                    detalle={"count": count, "updates": updates, "ids": list(ids)[:50]}
                )

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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: una o más provincias tienen localidades asociadas. "
                "Primero eliminá o reasigná las localidades."
            )

    def list_provincias(self) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre FROM ref.provincia ORDER BY nombre"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    # Localities
    def fetch_localidades(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch localities with filtering support."""
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(l.nombre ILIKE %s OR p.nombre ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
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

    def fetch_localidades_by_provincia(
        self,
        id_provincia: int,
        *,
        search: Optional[str] = None,
        limit: int = 80,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        filters = ["l.id_provincia = %s"]
        params: List[Any] = [int(id_provincia)]
        if search:
            search_value = str(search).strip()
            if search_value:
                filters.append("l.nombre ILIKE %s")
                params.append(f"%{search_value}%")
        where_clause = " AND ".join(filters)
        query = f"""
            SELECT l.id, l.nombre, l.id_provincia, p.nombre AS provincia
            FROM ref.localidad l
            JOIN ref.provincia p ON p.id = l.id_provincia
            WHERE {where_clause}
            ORDER BY l.nombre
            LIMIT %s OFFSET %s
        """
        params.extend([int(limit), int(offset)])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_localidades(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(l.nombre ILIKE %s OR p.nombre ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: una o más localidades están asignadas a clientes o proveedores. "
                "Primero reasigná esas entidades a otra localidad."
            )

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
                self.invalidate_catalog_cache("unidades")

    def delete_unidades_medida(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.unidad_medida WHERE id = ANY(%s)"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
                    self.invalidate_catalog_cache("unidades")
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: la unidad de medida está asignada a uno o más artículos. "
                "Reemplazala antes de eliminar."
            )

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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: la condición de IVA está asignada a clientes o proveedores."
            )

    # IVA Types
    def fetch_tipos_iva(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search, columns=("descripcion", "codigo::text"))
        sort_columns = {"id": "id", "codigo": "codigo", "porcentaje": "porcentaje", "descripcion": "descripcion"}
        order_by = self._build_order_by(sorts, sort_columns, default="porcentaje ASC", tiebreaker="id ASC")
        query = f"SELECT id, codigo, porcentaje, descripcion FROM ref.tipo_iva WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_tipos_iva(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search, columns=("descripcion", "codigo::text"))
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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: el tipo de IVA está asignado a uno o más artículos."
            )

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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: el depósito tiene movimientos o documentos asociados. "
                "Desactivá el depósito en su lugar."
            )

    # Payment Methods
    def fetch_formas_pago(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        where_clause, params = self._build_catalog_filters(search, columns=("descripcion",))
        sort_columns = {"id": "id", "descripcion": "descripcion", "activa": "activa"}
        order_by = self._build_order_by(sorts, sort_columns, default="descripcion ASC", tiebreaker="id ASC")
        query = f"SELECT id, descripcion, activa FROM ref.forma_pago WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_formas_pago(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search, columns=("descripcion",))
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
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: la forma de pago tiene registros históricos asociados. "
                "Desactivala en su lugar."
            )

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

    def get_lista_precio_simple(self, lista_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single price list by ID, regardless of active status."""
        query = "SELECT id, nombre, activa, orden FROM ref.lista_precio WHERE id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (lista_id,))
                rows = _rows_to_dicts(cur)
                return rows[0] if rows else None

    def count_listas_precio(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        where_clause, params = self._build_catalog_filters(search)
        query = f"SELECT COUNT(*) AS total FROM ref.lista_precio WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def _next_lista_precio_orden(self, cur: Any, start: int = 1, exclude_id: Optional[int] = None) -> int:
        start_val = max(1, int(start))
        query = "SELECT orden FROM ref.lista_precio"
        params: List[Any] = []
        if exclude_id:
            query += " WHERE id <> %s"
            params.append(int(exclude_id))
        query += " ORDER BY orden ASC"
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        rows = cur.fetchall()
        next_order = start_val
        for row in rows:
            value = row.get("orden") if isinstance(row, dict) else row[0]
            if value is None:
                continue
            try:
                value_int = int(value)
            except (TypeError, ValueError):
                continue
            if value_int < next_order:
                continue
            if value_int == next_order:
                next_order += 1
                continue
            break
        return next_order

    def get_next_lista_precio_orden(self, start: int = 1, *, exclude_id: Optional[int] = None) -> int:
        try:
            start_val = int(start)
        except (TypeError, ValueError):
            start_val = 1
        if start_val < 1:
            start_val = 1
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                return self._next_lista_precio_orden(cur, start_val, exclude_id)

    def create_lista_precio(self, nombre: str, activa: bool = True, orden: int = 0) -> int:
        query = "INSERT INTO ref.lista_precio (nombre, activa, orden) VALUES (%s, %s, %s) RETURNING id"
        if not isinstance(nombre, str) or not nombre.strip():
            raise ValueError("El nombre no puede estar vacío.")
        try:
            orden_val = 0
            if orden is not None and str(orden).strip() != "":
                try:
                    orden_val = int(str(orden).strip())
                except (TypeError, ValueError):
                    raise ValueError("El orden debe ser un número entero.")
            if orden_val <= 0:
                orden_val = 1
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    orden_val = self._next_lista_precio_orden(cur, orden_val)
                    cur.execute(query, (nombre.strip(), activa, orden_val))
                    res = cur.fetchone()
                    conn.commit()
                    return res.get("id") if isinstance(res, dict) else res[0]
        except IntegrityError as e:
            if getattr(e, "diag", None) and getattr(e.diag, "constraint_name", None) == "lista_precio_nombre_key":
                raise ValueError("Ya existe una lista de precios con ese nombre. Usá otro nombre.")
            if "lista_precio_nombre_key" in str(e):
                raise ValueError("Ya existe una lista de precios con ese nombre. Usá otro nombre.")
            raise ValueError(f"Error al crear lista de precios: {e}")

    def update_lista_precio_fields(self, id: int, updates: Dict[str, Any]) -> None:
        allowed = {"nombre", "activa", "orden"}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return
        if "orden" in filtered:
            orden_val = 0
            if filtered["orden"] is not None and str(filtered["orden"]).strip() != "":
                try:
                    orden_val = int(str(filtered["orden"]).strip())
                except (TypeError, ValueError):
                    raise ValueError("El orden debe ser un número entero.")
            if orden_val <= 0:
                orden_val = 1
            filtered["orden"] = orden_val
        set_clause = ", ".join([f"{k} = %s" for k in filtered.keys()])
        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
        params.append(id)
        query = f"UPDATE ref.lista_precio SET {set_clause} WHERE id = %s"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    if "orden" in filtered:
                        params = [v.strip() if isinstance(v, str) else v for v in filtered.values()]
                        params.append(id)
                        orden_idx = list(filtered.keys()).index("orden")
                        params[orden_idx] = self._next_lista_precio_orden(cur, int(params[orden_idx]), exclude_id=id)
                    cur.execute(query, params)
                    conn.commit()
        except IntegrityError as e:
            if getattr(e, "diag", None) and getattr(e.diag, "constraint_name", None) == "lista_precio_nombre_key":
                raise ValueError("Ya existe una lista de precios con ese nombre. Usá otro nombre.")
            if "lista_precio_nombre_key" in str(e):
                raise ValueError("Ya existe una lista de precios con ese nombre. Usá otro nombre.")
            raise ValueError(f"Error al actualizar lista de precios: {e}")

    def delete_listas_precio(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.lista_precio WHERE id = ANY(%s)"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: la lista de precios está asignada a clientes o tiene precios definidos."
                "Desactivala en su lugar."
            )

    # Logs
    def fetch_logs(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 100, offset: int = 0, solo_hoy: bool = True) -> List[Dict[str, Any]]:
        _ = (search, simple, advanced, sorts, limit, offset, solo_hoy)
        return []

    def count_logs(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, solo_hoy: bool = True) -> int:
        _ = (search, simple, advanced, solo_hoy)
        return 0

    def fetch_remitos(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 60, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params: List[Any] = []
        advanced = advanced or {}

        if search:
            filters.append("(numero ILIKE %s OR entidad ILIKE %s OR documento_numero ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 3)

        estado = advanced.get("estado")
        if estado and estado not in ("Todos", "Todas", "---", ""):
            filters.append("estado = %s")
            params.append(estado)

        deposito = advanced.get("deposito")
        if deposito and str(deposito).strip() not in ("Todos", "Todas", "---", "0", ""):
            filters.append("id_deposito = %s")
            params.append(int(deposito))

        entidad = advanced.get("entidad")
        if entidad:
            filters.append("entidad ILIKE %s")
            params.append(f"%{entidad.strip()}%")

        documento = advanced.get("documento")
        if documento:
            filters.append("documento_numero ILIKE %s")
            params.append(f"%{documento.strip()}%")

        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)

        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)

        entidad_id = _to_id(advanced.get("id_entidad"))
        if entidad_id:
            filters.append("id_entidad_comercial = %s")
            params.append(entidad_id)

        where_clause = " AND ".join(filters)
        sort_columns = {
            "id": "id",
            "numero": "numero",
            "fecha": "fecha",
            "estado": "estado",
            "entidad": "entidad",
            "deposito": "deposito",
            "documento_numero": "documento_numero",
            "total_unidades": "total_unidades",
        }
        order_by = self._build_order_by(sorts, sort_columns, default="fecha DESC")
        query = f"""
            SELECT
                id, numero, fecha, estado, entidad, id_entidad_comercial,
                deposito, id_deposito, id_documento, documento_numero, documento_estado,
                direccion_entrega, observacion, fecha_despacho, fecha_entrega,
                usuario, total_unidades
            FROM app.v_remito_resumen
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_remitos(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params: List[Any] = []
        advanced = advanced or {}

        if search:
            filters.append("(numero ILIKE %s OR entidad ILIKE %s OR documento_numero ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 3)

        estado = advanced.get("estado")
        if estado and estado not in ("Todos", "Todas", "---", ""):
            filters.append("estado = %s")
            params.append(estado)

        deposito = advanced.get("deposito")
        if deposito and str(deposito).strip() not in ("Todos", "Todas", "---", "0", ""):
            filters.append("id_deposito = %s")
            params.append(int(deposito))

        entidad = advanced.get("entidad")
        if entidad:
            filters.append("entidad ILIKE %s")
            params.append(f"%{entidad.strip()}%")

        documento = advanced.get("documento")
        if documento:
            filters.append("documento_numero ILIKE %s")
            params.append(f"%{documento.strip()}%")

        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)

        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)

        entidad_id = _to_id(advanced.get("id_entidad"))
        if entidad_id:
            filters.append("id_entidad_comercial = %s")
            params.append(entidad_id)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM app.v_remito_resumen WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None:
                    return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    def fetch_remito_detalle(self, remito_id: int) -> List[Dict[str, Any]]:
        query = """
            SELECT
                rd.nro_linea,
                a.nombre AS articulo,
                rd.cantidad,
                rd.observacion,
                rd.id_articulo,
                a.unidades_por_bulto
            FROM app.remito_detalle rd
            JOIN app.articulo a ON a.id = rd.id_articulo
            WHERE rd.id_remito = %s
            ORDER BY rd.nro_linea
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (remito_id,))
                return _rows_to_dicts(cur)

    def create_tipo_porcentaje(self, tipo: str) -> int:
        query = "INSERT INTO ref.tipo_porcentaje (tipo) VALUES (%s) RETURNING id"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (tipo.strip(),))
                    res = cur.fetchone()
                    conn.commit()
                    return res.get("id") if isinstance(res, dict) else res[0]
        except IntegrityError as e:
            if "ck_tipo_porcentaje_tipo" in str(e):
                raise ValueError("El tipo de porcentaje debe ser 'Recargo' o 'Descuento'.")
            raise ValueError(f"Error al crear tipo de porcentaje: {e}")

    def create_tipo_documento(self, nombre: str, clase: str, letra: str, afecta_stock: bool, afecta_cta: bool) -> int:
        query = """
            INSERT INTO ref.tipo_documento (nombre, clase, letra, afecta_stock, afecta_cuenta_corriente)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (nombre.strip(), clase.strip(), letra.strip(), afecta_stock, afecta_cta))
                    res = cur.fetchone()
                    conn.commit()
                    return res.get("id") if isinstance(res, dict) else res[0]
        except IntegrityError as e:
            raise ValueError(f"Error de integridad al crear tipo documento: {e}")

    def create_tipo_movimiento_articulo(self, nombre: str, signo: int) -> int:
        query = "INSERT INTO ref.tipo_movimiento_articulo (nombre, signo_stock) VALUES (%s, %s) RETURNING id"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    self._setup_session(cur)
                    cur.execute(query, (nombre.strip(), signo))
                    res = cur.fetchone()
                    conn.commit()
                    return res.get("id") if isinstance(res, dict) else res[0]
        except IntegrityError as e:
            raise ValueError(f"Error al crear tipo de movimiento: {e}")

    # Percentage Types
    def fetch_tipos_porcentaje(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("tipo ILIKE %s")
            params.append(f"%{search.strip()}%")
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
            filters.append("tipo ILIKE %s")
            params.append(f"%{search.strip()}%")
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_porcentaje WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def delete_tipos_porcentaje(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.tipo_porcentaje WHERE id = ANY(%s)"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: tipo de porcentaje en uso."
            )

    # Document Types
    def fetch_tipos_documento(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(nombre ILIKE %s OR clase ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
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
            filters.append("(nombre ILIKE %s OR clase ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_documento WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def delete_tipos_documento(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.tipo_documento WHERE id = ANY(%s)"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: existen documentos de este tipo."
            )

    # Article Movement Types
    def fetch_tipos_movimiento_articulo(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("nombre ILIKE %s")
            params.append(f"%{search.strip()}%")
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
            filters.append("nombre ILIKE %s")
            params.append(f"%{search.strip()}%")
        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) AS total FROM ref.tipo_movimiento_articulo WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                return result.get("total", 0) if isinstance(result, dict) else result[0]

    def delete_tipos_movimiento_articulo(self, ids: Sequence[int]) -> None:
        query = "DELETE FROM ref.tipo_movimiento_articulo WHERE id = ANY(%s)"
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (list(ids),))
                    conn.commit()
        except IntegrityError:
            raise ValueError(
                "No se puede eliminar: existen movimientos de este tipo asociado."
            )

    # Security: Users and Roles
    def fetch_users(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 40, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        if search:
            filters.append("(nombre ILIKE %s OR email ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        sort_columns = {"id": "id", "nombre": "nombre", "email": "email", "rol": "rol", "activo": "activo", "ultimo_login": "ultimo_login"}
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
            filters.append("(nombre ILIKE %s OR email ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
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
                
                self.log_activity(
                    entidad="seguridad.usuario",
                    accion="MODIFICACION",
                    id_entidad=user_id,
                    detalle={"updates": {k: v for k, v in updates.items() if k != "contrasena"}}
                )

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
                user_id = res.get("id") if isinstance(res, dict) else res[0]
                conn.commit()
                
                self.log_activity(
                    entidad="seguridad.usuario",
                    accion="ALTA",
                    id_entidad=user_id,
                    detalle={"nombre": nombre, "email": email, "id_rol": id_rol}
                )
                
                return user_id

    def fetch_roles(self) -> List[Dict[str, Any]]:
        """Fetch all available roles for user creation."""
        query = "SELECT id, nombre FROM seguridad.rol ORDER BY id"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def fetch_active_sessions(self, search: str = "", sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 10, offset: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """Sesiones activas deshabilitadas: mantener compatibilidad devolviendo vacío."""
        _ = (search, sorts, limit, offset, kwargs)
        return []

    def count_active_sessions(self, search: str = "", **kwargs) -> int:
        """Sesiones activas deshabilitadas: mantener compatibilidad devolviendo cero."""
        _ = (search, kwargs)
        return 0

    # Backup Config
    def fetch_backup_config(self) -> Dict[str, Any]:
        query = "SELECT * FROM seguridad.backup_config LIMIT 1"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                res = cur.fetchone()
                return res if isinstance(res, dict) else (dict(zip([d[0] for d in cur.description], res)) if res else {})

    def update_backup_config(self, updates: Dict[str, Any]) -> None:
        allowed = {"frecuencia", "hora", "destino_local", "retencion_dias", "ultimo_daily", "ultimo_weekly", "ultimo_monthly"}
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
            
            # Path to pg_dump - try multiple versions
            pg_dump_path = None
            for version in [18, 17, 16, 15, 14, 13, 12]:
                candidate = rf"C:\Program Files\PostgreSQL\{version}\bin\pg_dump.exe"
                if os.path.exists(candidate):
                    pg_dump_path = candidate
                    break
            
            if not pg_dump_path:
                # Fallback to PATH
                import shutil
                pg_dump_path = shutil.which("pg_dump")
            
            if not pg_dump_path:
                return False, "pg_dump no encontrado. Instale PostgreSQL o agregue la carpeta bin al PATH del sistema."
                
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
        with self._transaction(set_context=False) as cur:
            cur.execute(query, (clave, valor))

    def update_config_sistema_bulk(self, updates: Dict[str, str]) -> None:
        """Update multiple configuration values at once."""
        if not updates:
            return
        query = """
            INSERT INTO seguridad.config_sistema (clave, valor)
            VALUES (%s, %s)
            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
        """
        with self._transaction(set_context=False) as cur:
            for clave, valor in updates.items():
                cur.execute(query, (clave, valor))

    # Documents Resumen
    def fetch_documentos_resumen(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 60, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(entidad ILIKE %s OR tipo_documento ILIKE %s OR numero_serie ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 3)
        
        ent = advanced.get("entidad")
        if ent:
            filters.append("entidad ILIKE %s")
            params.append(f"%{ent.strip()}%")
        
        tipo = advanced.get("tipo")
        if tipo and tipo not in ("Todos", "Todas", "---"):
            filters.append("tipo_documento ILIKE %s")
            params.append(f"%{tipo.strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)
        
        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)
            
        estado = advanced.get("estado")
        if estado and estado not in ("Todos", "Todas", "---"):
            filters.append("estado = %s")
            params.append(estado)
            
        total_min = advanced.get("total_min")
        if total_min is not None:
             filters.append("total >= %s")
             params.append(float(total_min))
        total_max = advanced.get("total_max")
        if total_max is not None:
             filters.append("total <= %s")
             params.append(float(total_max))

        letra = advanced.get("letra")
        if letra and letra not in ("Todos", "Todas", "---"):
            filters.append("letra = %s")
            params.append(letra)

        numero = advanced.get("numero")
        if numero:
            filters.append("numero_serie ILIKE %s")
            params.append(f"%{numero.strip()}%")

        id_entidad = _to_id(advanced.get("id_entidad"))
        if id_entidad:
            filters.append("id_entidad = %s")
            params.append(id_entidad)

        where_clause = " AND ".join(filters)
        sort_columns = {
        "id": "id", 
        "fecha": "fecha", 
        "tipo_documento": "tipo_documento", 
        "numero_serie": "CASE WHEN numero_serie ~ '^[0-9]+$' THEN LPAD(numero_serie, 20, '0') ELSE numero_serie END",
        "entidad": "entidad", 
        "total": "total", 
        "estado": "estado",
        "usuario": "usuario",
        "letra": "letra",
        "forma_pago": "forma_pago"
    }
        order_by = self._build_order_by(sorts, sort_columns, default="fecha DESC")
        query = f"SELECT * FROM app.v_documento_resumen WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def fetch_documento_resumen_by_id(self, doc_id: int) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM app.v_documento_resumen WHERE id = %s LIMIT 1"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(doc_id),))
                rows = _rows_to_dicts(cur)
                return rows[0] if rows else None

    def count_documentos_resumen(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(entidad ILIKE %s OR tipo_documento ILIKE %s OR numero_serie ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 3)
            
        ent = advanced.get("entidad")
        if ent:
            filters.append("entidad ILIKE %s")
            params.append(f"%{ent.strip()}%")
        
        tipo = advanced.get("tipo")
        if tipo and tipo not in ("Todos", "Todas", "---"):
            filters.append("tipo_documento ILIKE %s")
            params.append(f"%{tipo.strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)
        
        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)
            
        estado = advanced.get("estado")
        if estado and estado not in ("Todos", "Todas", "---"):
            filters.append("estado = %s")
            params.append(estado)
            
        total_min = advanced.get("total_min")
        if total_min is not None:
             filters.append("total >= %s")
             params.append(float(total_min))
             
        total_max = advanced.get("total_max")
        if total_max is not None:
             filters.append("total <= %s")
             params.append(float(total_max))

        letra = advanced.get("letra")
        if letra and letra not in ("Todos", "Todas", "---"):
            filters.append("letra = %s")
            params.append(letra)

        numero = advanced.get("numero")
        if numero:
            filters.append("numero_serie ILIKE %s")
            params.append(f"%{numero.strip()}%")

        id_entidad = _to_id(advanced.get("id_entidad"))
        if id_entidad:
            filters.append("id_entidad = %s")
            params.append(id_entidad)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM app.v_documento_resumen WHERE {where_clause}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None: return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    def fetch_documentos_pendientes(self, id_entidad: int, search: Optional[str] = None, limit: int = 60, offset: int = 0) -> List[Dict[str, Any]]:
        filters = ["doc.id_entidad_comercial = %s", "doc.estado NOT IN ('ANULADO', 'PAGADO', 'BORRADOR')"]
        params: List[Any] = [id_entidad]
        if search:
            filters.append("(doc.numero_serie ILIKE %s OR td.nombre ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
        where_clause = " AND ".join(filters)
        query = f"""
            SELECT doc.id, doc.fecha, doc.numero_serie, doc.total, doc.estado, td.nombre AS tipo_documento
            FROM app.documento doc
            JOIN ref.tipo_documento td ON td.id = doc.id_tipo_documento
            WHERE {where_clause}
            ORDER BY doc.fecha DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def list_tipos_documento(self) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre, clase, letra FROM ref.tipo_documento ORDER BY nombre"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return _rows_to_dicts(cur)

    def get_max_document_total(self) -> float:
        query = "SELECT MAX(total) FROM app.documento"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                res = cur.fetchone()
                val = res.get("max") if isinstance(res, dict) else (res[0] if res else None)
                return float(val) if val is not None else 1000000.0

    # Stock Movements
    def _movimientos_base_query(self) -> str:
        return """
            SELECT 
              m.id, m.fecha, a.nombre AS articulo, tm.nombre AS tipo_movimiento,
              m.cantidad, tm.signo_stock, d.nombre AS deposito, u.nombre AS usuario,
              m.observacion, doc.id AS id_documento, td.nombre AS tipo_documento,
              doc.numero_serie AS nro_comprobante,
              COALESCE(ec.razon_social, TRIM(COALESCE(ec.apellido, '') || ' ' || COALESCE(ec.nombre, ''))) AS entidad,
              m.id_articulo AS id_articulo,
              m.stock_resultante
            FROM app.movimiento_articulo m
            JOIN app.articulo a ON m.id_articulo = a.id
            JOIN ref.tipo_movimiento_articulo tm ON m.id_tipo_movimiento = tm.id
            JOIN ref.deposito d ON m.id_deposito = d.id
            LEFT JOIN app.documento doc ON m.id_documento = doc.id
            LEFT JOIN ref.tipo_documento td ON doc.id_tipo_documento = td.id
            LEFT JOIN app.entidad_comercial ec ON doc.id_entidad_comercial = ec.id
            LEFT JOIN seguridad.usuario u ON m.id_usuario = u.id
        """

    def fetch_movimientos_stock(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None, sorts: Optional[Sequence[Tuple[str, str]]] = None, limit: int = 80, offset: int = 0) -> List[Dict[str, Any]]:
        # Ensure view is updated (to support new traceability columns)
        # DDL Removed: View updates should be handled by migration scripts, not per-query.
        pass

        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(articulo ILIKE %s OR tipo_movimiento ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
        
        # Advanced Filters
        art = advanced.get("articulo")
        if art and art not in ("Todos", "Todas", ""):
            art_value = str(art).strip()
            if art_value.isdigit():
                filters.append("id_articulo = %s")
                params.append(int(art_value))
            else:
                filters.append("lower(articulo) LIKE %s")
                params.append(f"%{art_value.lower()}%")
        
        tipo_mov = advanced.get("tipo_movimiento") or advanced.get("tipo")
        if tipo_mov and tipo_mov not in ("Todos", "Todas", ""):
            filters.append("tipo_movimiento = %s")
            params.append(tipo_mov)
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)
            
        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha < %s::timestamp + interval '1 day'")
            params.append(hasta)

        deposito = advanced.get("deposito")
        if deposito and deposito not in ("Todos", "Todas", ""):
            filters.append("deposito = %s")
            params.append(deposito)

        usuario = advanced.get("usuario")
        if usuario and usuario not in ("Todos", "Todas", ""):
            filters.append("usuario = %s")
            params.append(usuario)

        where_clause = " AND ".join(filters)
        sort_columns = {
            "id": "id",
            "fecha": "fecha",
            "articulo": "articulo",
            "tipo_movimiento": "tipo_movimiento",
            "cantidad": "cantidad",
            "entidad": "entidad",
            "deposito": "deposito",
            "usuario": "usuario",
            "comprobante": "CASE WHEN nro_comprobante ~ '^[0-9]+$' THEN LPAD(nro_comprobante, 20, '0') ELSE nro_comprobante END",
        }
        order_by = self._build_order_by(sorts, sort_columns, default="fecha DESC")
        base_query = self._movimientos_base_query()
        query = f"SELECT * FROM ({base_query}) AS movs WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_movimientos_stock(self, search: Optional[str] = None, simple: Optional[str] = None, advanced: Optional[Dict[str, Any]] = None) -> int:
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as vcur:
                    vcur.execute(f"CREATE OR REPLACE VIEW app.v_movimientos_full AS {self._movimientos_base_query()}")
                    conn.commit()
        except Exception:
            pass

        filters = ["1=1"]
        params = []
        advanced = advanced or {}
        if search:
            filters.append("(articulo ILIKE %s OR tipo_movimiento ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)
            
        # Advanced Filters
        art = advanced.get("articulo")
        if art and art not in ("Todos", "Todas", ""):
            art_value = str(art).strip()
            if art_value.isdigit():
                filters.append("id_articulo = %s")
                params.append(int(art_value))
            else:
                filters.append("lower(articulo) LIKE %s")
                params.append(f"%{art_value.lower()}%")
        
        tipo_mov = advanced.get("tipo_movimiento") or advanced.get("tipo")
        if tipo_mov and tipo_mov not in ("Todos", "Todas", ""):
            filters.append("tipo_movimiento = %s")
            params.append(tipo_mov)
            
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)
            
        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha < %s::timestamp + interval '1 day'")
            params.append(hasta)

        deposito = advanced.get("deposito")
        if deposito and deposito not in ("Todos", "Todas", ""):
            filters.append("deposito = %s")
            params.append(deposito)

        usuario = advanced.get("usuario")
        if usuario and usuario not in ("Todos", "Todas", ""):
            filters.append("usuario = %s")
            params.append(usuario)

        where_clause = " AND ".join(filters)
        base_query = self._movimientos_base_query()
        query = f"SELECT COUNT(*) as total FROM ({base_query}) AS movs WHERE {where_clause}"
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
            filters.append("(p.referencia ILIKE %s OR fp.descripcion ILIKE %s OR (ec.apellido || ' ' || ec.nombre) ILIKE %s OR ec.razon_social ILIKE %s OR d.numero_serie ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 5)
        
        ref = advanced.get("referencia")
        if ref:
            filters.append("p.referencia ILIKE %s")
            params.append(f"%{ref.strip()}%")
        
        forma = advanced.get("forma")
        if forma and str(forma) not in ("0", "Todos", "Todas", "---"):
            if str(forma).isdigit():
                filters.append("p.id_forma_pago = %s")
                params.append(int(forma))
            else:
                filters.append("fp.descripcion ILIKE %s")
                params.append(f"%{forma.strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("p.fecha >= %s")
            params.append(desde)

        hasta = advanced.get("hasta")
        if hasta:
            filters.append("p.fecha <= %s")
            params.append(hasta)

        entidad = advanced.get("entidad")
        if entidad and str(entidad) not in ("0", "Todos", "Todas", "---"):
            # If it's a digit, it's an ID
            if str(entidad).isdigit():
                filters.append("d.id_entidad_comercial = %s")
                params.append(int(entidad))
            else:
                filters.append("(ec.apellido || ' ' || ec.nombre) ILIKE %s")
                params.append(f"%{entidad.strip()}%")

        m_min = advanced.get("monto_min")
        if m_min is not None:
            filters.append("p.monto >= %s")
            params.append(float(m_min))

        m_max = advanced.get("monto_max")
        if m_max is not None:
            filters.append("p.monto <= %s")
            params.append(float(m_max))

        where_clause = " AND ".join(filters)
        sort_columns = {
            "id": "p.id", 
            "fecha": "p.fecha", 
            "monto": "p.monto", 
            "forma": "forma",
            "entidad": "entidad",
            "documento": "CASE WHEN d.numero_serie ~ '^[0-9]+$' THEN LPAD(d.numero_serie, 20, '0') ELSE d.numero_serie END"
        }
        order_by = self._build_order_by(sorts, sort_columns, default="p.fecha DESC")
        query = f"""
            SELECT p.*, fp.descripcion as forma, d.numero_serie as documento,
                   COALESCE(ec.apellido || ' ' || ec.nombre, ec.razon_social) as entidad
            FROM app.pago p
            JOIN ref.forma_pago fp ON p.id_forma_pago = fp.id
            LEFT JOIN app.documento d ON p.id_documento = d.id
            LEFT JOIN app.entidad_comercial ec ON d.id_entidad_comercial = ec.id
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
            filters.append("(p.referencia ILIKE %s OR fp.descripcion ILIKE %s OR (ec.apellido || ' ' || ec.nombre) ILIKE %s OR ec.razon_social ILIKE %s OR d.numero_serie ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 5)
            
        ref = advanced.get("referencia")
        if ref:
            filters.append("p.referencia ILIKE %s")
            params.append(f"%{ref.strip()}%")
        
        forma = advanced.get("forma")
        if forma and str(forma) not in ("0", "Todos", "Todas", "---"):
            if str(forma).isdigit():
                filters.append("p.id_forma_pago = %s")
                params.append(int(forma))
            else:
                filters.append("fp.descripcion ILIKE %s")
                params.append(f"%{forma.strip()}%")
            
        desde = advanced.get("desde")
        if desde:
            filters.append("p.fecha >= %s")
            params.append(desde)

        hasta = advanced.get("hasta")
        if hasta:
            filters.append("p.fecha <= %s")
            params.append(hasta)

        entidad = advanced.get("entidad")
        if entidad and str(entidad) not in ("0", "Todos", "Todas", "---"):
            if str(entidad).isdigit():
                filters.append("d.id_entidad_comercial = %s")
                params.append(int(entidad))
            else:
                filters.append("(ec.apellido || ' ' || ec.nombre) ILIKE %s")
                params.append(f"%{entidad.strip()}%")

        m_min = advanced.get("monto_min")
        if m_min is not None:
            filters.append("p.monto >= %s")
            params.append(float(m_min))

        m_max = advanced.get("monto_max")
        if m_max is not None:
            filters.append("p.monto <= %s")
            params.append(float(m_max))

        where_clause = " AND ".join(filters)
        query = f"""
            SELECT COUNT(*) as total 
            FROM app.pago p 
            JOIN ref.forma_pago fp ON p.id_forma_pago = fp.id 
            LEFT JOIN app.documento d ON p.id_documento = d.id
            LEFT JOIN app.entidad_comercial ec ON d.id_entidad_comercial = ec.id
            WHERE {where_clause}
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if res is None: return 0
                return res.get("total", 0) if isinstance(res, dict) else res[0]

    def fetch_documento_detalle(self, documento_id: int) -> List[Dict[str, Any]]:
        query = """
            SELECT dd.*, 
                   COALESCE(a.nombre, dd.descripcion_historica, 'Artículo Descon.') as articulo, 
                   a.id as codigo_art,
                   a.unidades_por_bulto,
                   lp.nombre as lista_nombre
            FROM app.documento_detalle dd
            LEFT JOIN app.articulo a ON dd.id_articulo = a.id
            LEFT JOIN ref.lista_precio lp ON dd.id_lista_precio = lp.id
            WHERE dd.id_documento = %s
            ORDER BY dd.nro_linea
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (documento_id,))
                return _rows_to_dicts(cur)
    
    def create_stock_movement(
        self,
        *,
        id_articulo: int,
        id_tipo_movimiento: int,
        cantidad: float,
        id_deposito: int = 1,
        observacion: Optional[str] = None,
        id_documento: Optional[int] = None,
        id_usuario: Optional[int] = None
    ) -> int:
        query = """
            INSERT INTO app.movimiento_articulo (
                id_articulo,
                id_tipo_movimiento,
                cantidad,
                id_deposito,
                observacion,
                id_documento,
                id_usuario
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        # If id_usuario not passed, try to use session user if available, or current_user_id
        if id_usuario is None and hasattr(self, 'current_user_id'):
            id_usuario = self.current_user_id

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur) # Sets app.user_id for audit
                cur.execute(
                    query,
                    (
                        id_articulo,
                        id_tipo_movimiento,
                        cantidad,
                        id_deposito,
                        observacion,
                        id_documento,
                        id_usuario
                    )
                )
                res = cur.fetchone()
                movement_id = res.get("id") if isinstance(res, dict) else res[0]
                conn.commit()
                
                # Log audit activity (if not internal to a document confirm)
                if not id_documento:
                    self.log_activity(
                        entidad="app.articulo",
                        accion="AJUSTE_STOCK",
                        id_entidad=id_articulo,
                        detalle={
                            "cantidad": float(cantidad),
                            "tipo_movimiento": id_tipo_movimiento,
                            "deposito": id_deposito,
                            "observacion": observacion
                        }
                    )
                
                return movement_id

    def create_payment(
        self,
        *,
        id_documento: int,
        id_forma_pago: int,
        monto: float,
        fecha: Optional[str] = None,
        referencia: Optional[str] = None,
        observacion: Optional[str] = None
    ) -> int:
        query = """
            INSERT INTO app.pago (
                id_documento,
                id_forma_pago,
                monto,
                fecha,
                referencia,
                observacion
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        final_fecha = fecha if fecha else datetime.now()
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                
                # 0. Validar estado del documento antes de crear pago
                cur.execute("SELECT estado FROM app.documento WHERE id = %s", (id_documento,))
                doc_state = cur.fetchone()
                if doc_state:
                    state = doc_state.get("estado") if isinstance(doc_state, dict) else doc_state[0]
                    if state == 'BORRADOR':
                        raise ValueError("No se puede registrar un pago para un documento en estado BORRADOR.")

                # 1. Insert Payment
                cur.execute(
                    query,
                    (
                        id_documento,
                        id_forma_pago,
                        monto,
                        final_fecha,
                        referencia,
                        observacion
                    )
                )
                res = cur.fetchone()
                payment_id = res.get("id") if isinstance(res, dict) else res[0]

                # 2. Check balance and update status if fully paid
                cur.execute("SELECT total, id_entidad_comercial, numero_serie FROM app.documento WHERE id = %s", (id_documento,))
                doc_res = cur.fetchone()
                doc_total = 0.0
                doc_entidad = None
                doc_numero = None
                if doc_res:
                    if isinstance(doc_res, dict):
                        doc_total = float(doc_res.get("total") if doc_res.get("total") is not None else 0)
                        doc_entidad = doc_res.get("id_entidad_comercial")
                        doc_numero = doc_res.get("numero_serie")
                    else:
                        doc_total = float(doc_res[0] if doc_res[0] is not None else 0)
                        doc_entidad = doc_res[1]
                        doc_numero = doc_res[2]
                    
                    cur.execute("SELECT COALESCE(SUM(monto), 0) FROM app.pago WHERE id_documento = %s", (id_documento,))
                    pay_res = cur.fetchone()
                    total_paid = float(pay_res.get("coalesce") if isinstance(pay_res, dict) else (pay_res[0] or 0))
                    
                    # Use a small epsilon for float comparison
                    if total_paid >= (doc_total - 0.01):
                        cur.execute("UPDATE app.documento SET estado = 'PAGADO' WHERE id = %s", (id_documento,))
                        self._ensure_remito_for_document(cur, id_documento)

                if doc_entidad:
                    mov_concept = f"Pago doc {doc_numero or id_documento}"
                    mov_obs = (observacion or "").strip()
                    if referencia:
                        if mov_obs:
                            mov_obs = f"{mov_obs} Ref: {referencia}"
                        else:
                            mov_obs = f"Ref: {referencia}"
                    if not mov_obs:
                        mov_obs = None
                    cur.execute(
                        "SELECT app.registrar_movimiento_cc(%s::bigint, %s::varchar(20), %s::varchar(150), %s::numeric, %s::bigint, %s::bigint, %s::text, %s::bigint)",
                        (
                            int(doc_entidad),
                            "CREDITO",
                            mov_concept[:150],
                            Decimal(str(monto)),
                            int(id_documento),
                            int(payment_id),
                            mov_obs,
                            int(self.current_user_id) if self.current_user_id is not None else None
                        )
                    )
                conn.commit()
                
                # Log audit activity
                self.log_activity(
                    entidad="app.pago",
                    accion="REGISTRO",
                    id_entidad=payment_id,
                    detalle={
                        "documento": id_documento,
                        "forma_pago": id_forma_pago,
                        "monto": float(monto),
                        "referencia": referencia
                    }
                )
                
                return payment_id

    def _validate_document_item_quantities(self, items: List[Dict[str, Any]]) -> None:
        for item in items:
            raw_qty = item.get("cantidad")
            try:
                if isinstance(raw_qty, str):
                    raw_qty = raw_qty.strip().replace(",", ".")
                qty_dec = Decimal(str(raw_qty))
            except Exception:
                raise ValueError("La cantidad debe ser un número entero.")
            if qty_dec != qty_dec.to_integral_value():
                raise ValueError("La cantidad debe ser un número entero.")

    def _build_unidades_por_bulto_snapshot(
        self,
        cur: Any,
        items: Sequence[Dict[str, Any]],
    ) -> Dict[int, Optional[int]]:
        article_ids: List[int] = []
        seen: set = set()
        for item in items:
            art_id = _to_id((item or {}).get("id_articulo"))
            if art_id is None or art_id in seen:
                continue
            seen.add(art_id)
            article_ids.append(art_id)
        if not article_ids:
            return {}

        cur.execute(
            """
            SELECT id, unidades_por_bulto
            FROM app.articulo
            WHERE id = ANY(%s)
            """,
            (article_ids,),
        )
        result: Dict[int, Optional[int]] = {}
        for row in cur.fetchall():
            if isinstance(row, dict):
                result[int(row.get("id"))] = row.get("unidades_por_bulto")
            else:
                result[int(row[0])] = row[1]
        return result

    def create_document(self, *, id_tipo_documento: int, id_entidad_comercial: int, id_deposito: int, 
                        items: List[Dict[str, Any]], observacion: Optional[str] = None, 
                        numero_serie: Optional[str] = None, descuento_porcentaje: float = 0,
                        descuento_importe: float = 0,
                        descuento_global_mode: str = "percentage",
                        fecha: Optional[str] = None, fecha_vencimiento: Optional[str] = None,
                        id_lista_precio: Optional[int] = None,
                        direccion_entrega: Optional[str] = None,
                        sena: float = 0,
                        manual_values: Optional[Dict[str, float]] = None) -> int:
        """
        items: list of {
            id_articulo, cantidad, precio_unitario, porcentaje_iva,
            id_lista_precio, descuento_porcentaje, descuento_importe, observacion
        }
        """
        header_query = """
            INSERT INTO app.documento (
                id_tipo_documento, id_entidad_comercial, id_deposito, 
                observacion, numero_serie, descuento_porcentaje, descuento_importe, id_usuario,
                neto, subtotal, iva_total, total, sena, fecha, fecha_vencimiento, id_lista_precio, direccion_entrega
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        detail_query = """
            INSERT INTO app.documento_detalle (
                id_documento, nro_linea, id_articulo, cantidad, 
                precio_unitario, descuento_porcentaje, descuento_importe,
                porcentaje_iva, total_linea, id_lista_precio, observacion, unidades_por_bulto_historico
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        if not items:
            raise ValueError("El comprobante debe tener al menos un item.")
        self._validate_document_item_quantities(items)

        pricing = calculate_document_totals(
            items=items,
            descuento_global_porcentaje=descuento_porcentaje,
            descuento_global_importe=descuento_importe,
            descuento_global_mode="amount" if str(descuento_global_mode).lower() == "amount" else "percentage",
            sena=sena,
            pricing_mode="tax_included",
        )

        subtotal = pricing["subtotal_bruto"]
        neto_total = pricing["neto"]
        iva_total = pricing["iva_total"]
        total = pricing["total"]
        desc_pct_normalized = pricing["descuento_global_porcentaje"]
        desc_imp_normalized = pricing["descuento_global_importe"]
        
        # Default dates if not provided
        if not fecha:
            # Use SQL 'now()' if None, but we need to pass a value or change query.
            # Easier to let Python handle "now" or SQL handle it.
            # Since query expects %s, we should pass datetime.now() if None, OR generic 'now()'.
            # Let's pass datetime object or None if we change query. 
            # Current query has `VALUES (..., %s)` for fecha. 
            pass # We will handle in params
            
        final_fecha = fecha if fecha else datetime.now()
        fecha_vencimiento_value = fecha_vencimiento
        if isinstance(fecha_vencimiento_value, str):
            fecha_vencimiento_value = fecha_vencimiento_value.strip() or None

        # Override totals if manual values provided (User Manual Edit)
        if manual_values:
            # Map UI 'subtotal' (which is net after discount) to DB 'neto'
            if "subtotal" in manual_values:
                neto_total = Decimal(str(manual_values["subtotal"]))
            if "iva_total" in manual_values:
                iva_total = Decimal(str(manual_values["iva_total"]))
            if "total" in manual_values:
                total = Decimal(str(manual_values["total"]))

        with self._transaction() as cur:
            # Header
            numero_para_insertar = None
            if numero_serie:
                numero_para_insertar = str(numero_serie)
                self._ensure_unique_document_number(cur, id_tipo_documento, numero_para_insertar)
            else:
                self._lock_document_number(cur, id_tipo_documento)
                numero_para_insertar = str(self._next_document_number(cur, id_tipo_documento))
            cur.execute(header_query, (
                id_tipo_documento, id_entidad_comercial, id_deposito,
                observacion, numero_para_insertar, desc_pct_normalized, desc_imp_normalized, self.current_user_id,
                neto_total, subtotal, iva_total, total, sena,
                final_fecha, fecha_vencimiento_value, id_lista_precio, direccion_entrega
            ))
            res = cur.fetchone()
            doc_id = res[0] if isinstance(res, (list, tuple)) else res["id"]

            # Details (batch insert)
            unidades_por_bulto_snapshot = self._build_unidades_por_bulto_snapshot(cur, pricing["items"])
            detail_rows = []
            for i, item in enumerate(pricing["items"], 1):
                article_id = _to_id(item.get("id_articulo"))
                detail_rows.append(
                    (
                        doc_id,
                        i,
                        item["id_articulo"],
                        item["cantidad"],
                        item["precio_unitario"],
                        item["descuento_porcentaje"],
                        item["descuento_importe"],
                        item["porcentaje_iva"],
                        item["total_linea"],
                        item.get("id_lista_precio"),
                        item.get("observacion"),
                        unidades_por_bulto_snapshot.get(article_id) if article_id is not None else None,
                    )
                )
            cur.executemany(detail_query, detail_rows)
            
            # Log audit activity
            self.log_activity(
                entidad="app.documento",
                accion="CREACION",
                id_entidad=doc_id,
                detalle={
                    "tipo": id_tipo_documento,
                    "numero": numero_para_insertar,
                    "entidad": id_entidad_comercial,
                    "total": float(total),
                    "descuento_lineas": float(pricing["descuento_lineas_importe"]),
                }
            )
            
            return doc_id

    def get_entity_balance(self, entity_id: int) -> float:
        """Returns the current balance from app.lista_cliente."""
        query = "SELECT saldo_cuenta FROM app.lista_cliente WHERE id_entidad_comercial = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (entity_id,))
                res = cur.fetchone()
                return float(res[0]) if res else 0.0

    def list_entidades_simple(self, limit: int = 200, only_active: bool = True) -> List[Dict[str, Any]]:
        where = "WHERE activo = True" if only_active else ""
        # Use parameterized query for LIMIT
        query = f"""
            SELECT id, nombre_completo, tipo, activo, domicilio
            FROM app.v_entidad_detallada
            {where}
            ORDER BY nombre_completo ASC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(limit),))
                return _rows_to_dicts(cur)

    def get_entity_simple(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single entity by ID, regardless of active status."""
        query = """
            SELECT id, nombre_completo, tipo, activo, domicilio
            FROM app.v_entidad_detallada
            WHERE id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (entity_id,))
                rows = _rows_to_dicts(cur)
                return rows[0] if rows else None

    def get_entity_detail(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Fetch entity metadata useful for printing."""
        query = """
            SELECT
                id,
                nombre_completo,
                razon_social,
                apellido,
                nombre,
                cuit,
                domicilio,
                localidad,
                provincia,
                condicion_iva,
                telefono,
                email,
                tipo,
                activo
            FROM app.v_entidad_detallada
            WHERE id = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (entity_id,))
                rows = _rows_to_dicts(cur)
                if not rows:
                    return None
                return rows[0]

    def list_articulos_simple(self, limit: int = 200) -> List[Dict[str, Any]]:
        # Use parameterized query for LIMIT
        query = """
            SELECT
                id_articulo,
                id_articulo AS id,
                nombre,
                costo,
                porcentaje_iva,
                unidades_por_bulto,
                activo
            FROM app.v_articulo_detallado
            WHERE activo = True
            ORDER BY nombre ASC
            LIMIT %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(limit),))
                return _rows_to_dicts(cur)

    def get_article_simple(self, article_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single article by ID, regardless of active status."""
        query = """
            SELECT
                id_articulo,
                id_articulo AS id,
                nombre,
                codigo,
                costo,
                porcentaje_iva,
                unidad_medida,
                unidad_abreviatura,
                unidades_por_bulto,
                activo
            FROM app.v_articulo_detallado
            WHERE id_articulo = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                rows = _rows_to_dicts(cur)
                return rows[0] if rows else None

    def list_usuarios_simple(self, limit: int = 100) -> List[Dict[str, Any]]:
        # Use parameterized query for LIMIT
        query = "SELECT id, nombre FROM seguridad.usuario WHERE activo = True ORDER BY nombre ASC LIMIT %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(limit),))
                return _rows_to_dicts(cur)

    def list_tipos_movimiento_simple(self) -> List[Dict[str, Any]]:
        query = "SELECT id, nombre, signo_stock FROM ref.tipo_movimiento_articulo ORDER BY nombre ASC"
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
        # Ensure view is updated (Idempotent)
        with self.pool.connection() as conn:
            with conn.cursor() as vcur:
                vcur.execute(f"CREATE OR REPLACE VIEW app.v_movimientos_full AS {self._movimientos_base_query()}")
                conn.commit()

        with self._transaction() as cur:
            cur.execute(
                """
                    WITH updated AS (
                        UPDATE app.documento d
                        SET estado = 'CONFIRMADO'
                        FROM ref.tipo_documento td
                        WHERE d.id = %s
                          AND d.estado = 'BORRADOR'
                          AND td.id = d.id_tipo_documento
                        RETURNING d.id, d.id_deposito, d.id_entidad_comercial, d.direccion_entrega, d.observacion, d.numero_serie,
                                  td.clase, td.afecta_stock, d.total, d.sena
                    )
                    SELECT id, id_deposito, id_entidad_comercial, direccion_entrega, observacion, numero_serie, clase, afecta_stock, total, sena FROM updated
                    """,
                (doc_id,),
            )
            doc = cur.fetchone()
            if not doc:
                raise Exception("Comprobante no encontrado o ya confirmado.")

            if isinstance(doc, dict):
                doc_id = doc["id"]
                depo_id = doc["id_deposito"]
                entidad_id = doc.get("id_entidad_comercial")
                direccion = doc.get("direccion_entrega")
                observacion = doc.get("observacion")
                doc_numero = doc.get("numero_serie")
                clase = doc["clase"]
                afecta_stk = doc["afecta_stock"]
                doc_total = doc.get("total")
                doc_sena = doc.get("sena")
            else:
                (
                    doc_id,
                    depo_id,
                    entidad_id,
                    direccion,
                    observacion,
                    doc_numero,
                    clase,
                    afecta_stk,
                    doc_total,
                    doc_sena,
                ) = doc

            doc_total = Decimal(str(doc_total or 0))
            doc_sena = Decimal(str(doc_sena or 0))

            if afecta_stk:
                id_tipo_mov = 2 if clase == "VENTA" else 1
                cur.execute(
                    """
                    INSERT INTO app.movimiento_articulo (id_articulo, id_tipo_movimiento, cantidad, id_deposito, id_documento, observacion, id_usuario)
                    SELECT id_articulo, %s, cantidad, %s, %s, 'Confirmación de ' || %s, %s
                    FROM app.documento_detalle
                    WHERE id_documento = %s
                    """,
                    (id_tipo_mov, depo_id, doc_id, clase, self.current_user_id, doc_id),
                )

            if clase == "VENTA" and entidad_id:
                concepto_debito = f"{clase} {doc_numero or doc_id}"
                observacion_debito = (observacion or "").strip() or None
                if doc_total != 0:
                    cur.execute(
                        "SELECT app.registrar_movimiento_cc(%s::bigint, %s::varchar(20), %s::varchar(150), %s::numeric, %s::bigint, %s::bigint, %s::text, %s::bigint)",
                        (
                            int(entidad_id),
                            "DEBITO",
                            concepto_debito[:150],
                            doc_total,
                            int(doc_id),
                            None,
                            observacion_debito,
                            int(self.current_user_id) if self.current_user_id is not None else None
                        ),
                    )
                if doc_sena > 0:
                    cur.execute(
                        "SELECT COALESCE(SUM(monto), 0) FROM app.pago WHERE id_documento = %s",
                        (doc_id,),
                    )
                    pagos_row = cur.fetchone()
                    pagos_val = None
                    if isinstance(pagos_row, (tuple, list)):
                        pagos_val = pagos_row[0]
                    else:
                        pagos_val = pagos_row
                    pagos_existentes = Decimal(str(pagos_val if pagos_val is not None else 0))
                    credito_sena = doc_sena - pagos_existentes
                    if credito_sena > 0:
                        concepto_sena = f"Seña {doc_numero or doc_id}"
                        observacion_sena = f"Seña del comprobante {doc_numero or doc_id}"
                        cur.execute(
                            "SELECT app.registrar_movimiento_cc(%s::bigint, %s::varchar(20), %s::varchar(150), %s::numeric, %s::bigint, %s::bigint, %s::text, %s::bigint)",
                            (
                                int(entidad_id),
                                "CREDITO",
                                concepto_sena[:150],
                                credito_sena,
                                int(doc_id),
                                None,
                                observacion_sena,
                                int(self.current_user_id) if self.current_user_id is not None else None
                            ),
                        )
            
            # Generar remito si corresponde (para documentos de VENTA: facturas, presupuestos, etc.)
            self._ensure_remito_for_document(cur, doc_id)
            
            # Log audit activity
            self.log_activity(
                entidad="app.documento",
                accion="CONFIRMACION",
                id_entidad=doc_id,
                detalle={
                    "numero": doc_numero,
                    "clase": clase,
                    "entidad": entidad_id,
                    "total": float(doc_total)
                }
            )

    def _get_any_deposito_id(self, cur) -> int:
        cur.execute("SELECT id FROM ref.deposito ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            return 1
        if isinstance(row, dict):
            val = row.get("id")
        else:
            val = row[0]
        return val or 1

    def _build_remito_number(self, cur, doc_numero: Optional[str]) -> str:
        candidate = (doc_numero or "").strip()
        if candidate:
            return candidate
        cur.execute(
            "SELECT COALESCE(MAX((numero::bigint)) FILTER (WHERE numero ~ '^[0-9]+$'), 0) + 1 AS next_val FROM app.remito"
        )
        row = cur.fetchone()
        if not row:
            return "1"
        if isinstance(row, dict):
            next_val = row.get("next_val")
        else:
            next_val = row[0]
        try:
            numeric = int(next_val)
        except (TypeError, ValueError):
            numeric = 1
        return str(numeric)

    def _normalize_remito_quantity(self, raw_qty: Any, *, line_no: int) -> int:
        try:
            qty_dec = Decimal(str(raw_qty))
        except Exception:
            raise ValueError(f"La cantidad de la línea {line_no} no es un número válido para remito.")
        if qty_dec != qty_dec.to_integral_value():
            raise ValueError(f"La cantidad de la línea {line_no} debe ser un número entero para remito.")
        qty_int = int(qty_dec)
        if qty_int <= 0:
            raise ValueError(f"La cantidad de la línea {line_no} debe ser mayor a 0 para remito.")
        return qty_int

    def _insert_remito_detalle_from_document(self, cur, doc_id: int, remito_id: int) -> None:
        cur.execute(
            """
            SELECT id_articulo, cantidad, observacion
            FROM app.documento_detalle
            WHERE id_documento = %s
            ORDER BY nro_linea
            """,
            (doc_id,),
        )
        detalle = cur.fetchall()
        if not detalle:
            return

        entries = []
        for idx, rec in enumerate(detalle, 1):
            if isinstance(rec, dict):
                art_id_raw = rec.get("id_articulo")
                cantidad_raw = rec.get("cantidad")
                obs = rec.get("observacion")
            else:
                art_id_raw = rec[0]
                cantidad_raw = rec[1]
                obs = rec[2] if len(rec) > 2 else None

            art_id = _to_id(art_id_raw)
            if art_id is None:
                raise ValueError(f"La línea {idx} tiene un artículo inválido para remito.")

            cantidad = self._normalize_remito_quantity(cantidad_raw, line_no=idx)
            obs_text = str(obs).strip() if obs is not None else None
            obs = obs_text or None
            entries.append((remito_id, idx, art_id, cantidad, obs))

        cur.executemany(
            """
            INSERT INTO app.remito_detalle (
                id_remito, nro_linea, id_articulo, cantidad, observacion
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            entries,
        )

    def _insert_remito_from_metadata(self, cur, doc_id: int, metadata: Dict[str, Any]) -> Optional[int]:
        entidad_id = metadata.get("id_entidad_comercial")
        if not entidad_id:
            return None

        deposit_id = metadata.get("id_deposito") or self._get_any_deposito_id(cur)
        numero = self._build_remito_number(cur, metadata.get("numero_serie"))

        cur.execute(
            """
            INSERT INTO app.remito (
                numero, id_documento, id_entidad_comercial, id_deposito,
                direccion_entrega, observacion, id_usuario
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                numero,
                doc_id,
                entidad_id,
                deposit_id,
                metadata.get("direccion_entrega"),
                metadata.get("observacion"),
                self.current_user_id,
            ),
        )
        row = cur.fetchone()
        if not row:
            return None
        remito_id = row[0] if not isinstance(row, dict) else row.get("id")
        if not remito_id:
            return None

        self._insert_remito_detalle_from_document(cur, doc_id, remito_id)
        return remito_id

    def _fetch_document_metadata(self, cur, doc_id: int) -> Optional[Dict[str, Any]]:
        cur.execute(
            """
            SELECT
                d.id_entidad_comercial,
                d.id_deposito,
                d.direccion_entrega,
                d.observacion,
                d.numero_serie,
                td.clase
            FROM app.documento d
            JOIN ref.tipo_documento td ON td.id = d.id_tipo_documento
            WHERE d.id = %s
            """,
            (doc_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        if isinstance(row, dict):
            return {
                "id_entidad_comercial": row.get("id_entidad_comercial"),
                "id_deposito": row.get("id_deposito"),
                "direccion_entrega": row.get("direccion_entrega"),
                "observacion": row.get("observacion"),
                "numero_serie": row.get("numero_serie"),
                "clase": row.get("clase"),
            }

        return {
            "id_entidad_comercial": row[0],
            "id_deposito": row[1],
            "direccion_entrega": row[2],
            "observacion": row[3],
            "numero_serie": row[4],
            "clase": row[5],
        }

    def _ensure_remito_for_document(self, cur, doc_id: int, *, doc_meta: Optional[Dict[str, Any]] = None) -> Optional[int]:
        metadata = doc_meta or self._fetch_document_metadata(cur, doc_id)
        if not metadata:
            return None

        if str(metadata.get("clase") or "").upper() != "VENTA":
            return None

        entidad_id = metadata.get("id_entidad_comercial")
        if not entidad_id:
            return None

        cur.execute("SELECT id FROM app.remito WHERE id_documento = %s LIMIT 1", (doc_id,))
        existing = cur.fetchone()
        if existing:
            return existing.get("id") if isinstance(existing, dict) else existing[0]

        return self._insert_remito_from_metadata(cur, doc_id, metadata)

    def update_document_afip_data(
        self,
        doc_id: int,
        cae: str,
        cae_vencimiento: str,
        punto_venta: int,
        tipo_comprobante_afip: int,
        cuit_emisor: Optional[str] = None,
        qr_data: Optional[str] = None,
    ):
        """
        Actualiza los datos de AFIP (CAE, vencimiento, etc.) para un documento.
        Normalmente se llama después de una autorización exitosa en AFIP.
        """
        with self._transaction() as cur:
            cur.execute(
                """
                UPDATE app.documento
                SET cae = %s, 
                    cae_vencimiento = %s, 
                    punto_venta = %s, 
                    tipo_comprobante_afip = %s,
                    cuit_emisor = %s,
                    qr_data = %s,
                    estado = 'CONFIRMADO'
                WHERE id = %s
                """,
                (cae, cae_vencimiento, punto_venta, tipo_comprobante_afip, cuit_emisor, qr_data, doc_id),
            )
            self._ensure_remito_for_document(cur, doc_id)

        # Opcional: registrar actividad (fuera de la transacción principal)
        self.log_activity(
            entidad="app.documento",
            accion="AFIP_AUTH",
            id_entidad=doc_id,
            detalle={"cae": cae, "punto_venta": punto_venta},
        )

    def anular_documento(self, doc_id: int) -> bool:
        """
        Anula un comprobante, revierte movimientos de stock y cuenta corriente.
        """
        with self._transaction() as cur:
             cur.execute("SELECT estado, id_tipo_documento, numero_serie FROM app.documento WHERE id = %s FOR UPDATE", (doc_id,))
             res = cur.fetchone()
             if not res: raise ValueError("Documento no encontrado")
             if isinstance(res, dict):
                 estado = res["estado"]
                 id_tipo_doc = res["id_tipo_documento"]
                 numero_serie = res.get("numero_serie")
             else:
                 estado, id_tipo_doc, numero_serie = res
             
             if estado == 'ANULADO': return True

             # Set status
             cur.execute("UPDATE app.documento SET estado = 'ANULADO' WHERE id = %s", (doc_id,))
             
             # Revert stock movements
             cur.execute("""
                SELECT m.id_articulo, m.cantidad, tm.nombre as tipo_mov, m.id_deposito
                FROM app.movimiento_articulo m
                JOIN ref.tipo_movimiento_articulo tm ON tm.id = m.id_tipo_movimiento
                WHERE m.id_documento = %s
             """, (doc_id,))
             movs = cur.fetchall()
             
             # Get reverse types
             cur.execute("SELECT id, nombre FROM ref.tipo_movimiento_articulo WHERE nombre IN ('Devolución Cliente', 'Devolución Proveedor')")
             type_map = {row[1]: row[0] for row in cur.fetchall()}
             
             for mov in movs:
                 art_id, cant, tipo_nombre, dep_id = mov
                 new_tipo_id = None
                 
                 if "Venta" in tipo_nombre:
                      new_tipo_id = type_map.get('Devolución Cliente')
                 elif "Compra" in tipo_nombre:
                      new_tipo_id = type_map.get('Devolución Proveedor')
                 
                 if new_tipo_id:
                     cur.execute("""
                        INSERT INTO app.movimiento_articulo (id_articulo, id_tipo_movimiento, cantidad, id_deposito, id_documento, observacion, id_usuario)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                     """, (art_id, new_tipo_id, cant, dep_id, doc_id, "Anulación de Comprobante", self.current_user_id))
             
             # Revert current account movements (cuenta corriente)
             cur.execute("""
                SELECT id, id_entidad_comercial, tipo_movimiento, monto, concepto
                FROM app.movimiento_cuenta_corriente
                WHERE id_documento = %s AND anulado = FALSE
             """, (doc_id,))
             cc_movs = cur.fetchall()
             
             for cc_mov in cc_movs:
                 if isinstance(cc_mov, dict):
                     mov_id = cc_mov["id"]
                     entidad_id = cc_mov["id_entidad_comercial"]
                     tipo_mov = cc_mov["tipo_movimiento"]
                     monto = cc_mov["monto"]
                     concepto_orig = cc_mov.get("concepto", "")
                 else:
                     mov_id, entidad_id, tipo_mov, monto, concepto_orig = cc_mov
                 
                 # Determine reverse type: DEBITO -> CREDITO, CREDITO -> DEBITO
                 # Use ANULACION type which behaves like CREDITO (subtracts from balance)
                 if tipo_mov in ("DEBITO", "AJUSTE_DEBITO"):
                     # Original was a debit (increased debt), so we need to reverse it (decrease debt)
                     reverse_type = "ANULACION"
                 elif tipo_mov in ("CREDITO", "AJUSTE_CREDITO"):
                     # Original was a credit (decreased debt), so we need to reverse it (increase debt)
                     reverse_type = "AJUSTE_DEBITO"
                 else:
                     continue  # Skip already reversed movements
                 
                 concepto_anulacion = f"Anulación: {concepto_orig[:120]}" if concepto_orig else f"Anulación doc {numero_serie or doc_id}"
                 observacion_anulacion = f"Reversión automática por anulación de documento {numero_serie or doc_id}"
                 
                 # Register reverse movement
                 cur.execute(
                     "SELECT app.registrar_movimiento_cc(%s::bigint, %s::varchar(20), %s::varchar(150), %s::numeric, %s::bigint, %s::bigint, %s::text, %s::bigint)",
                     (
                         int(entidad_id),
                         reverse_type,
                         concepto_anulacion[:150],
                         Decimal(str(monto)),
                         int(doc_id),
                         None,
                         observacion_anulacion,
                         int(self.current_user_id) if self.current_user_id is not None else None
                     )
                 )
                 new_mov_res = cur.fetchone()
                 new_mov_id = new_mov_res[0] if new_mov_res else None
                 
                 # Mark original movement as annulled and link to the new one
                 cur.execute("""
                    UPDATE app.movimiento_cuenta_corriente 
                    SET anulado = TRUE, id_movimiento_anula = %s 
                    WHERE id = %s
                 """, (new_mov_id, mov_id))
        
        # Log audit activity
        self.log_activity(
            entidad="app.documento",
            accion="ANULACION",
            id_entidad=doc_id,
            detalle={"estado_previo": estado, "cc_movimientos_revertidos": len(cc_movs) if cc_movs else 0}
        )
        
        return True

    def get_article_stock(self, article_id: int) -> float:
        query = "SELECT stock_total FROM app.v_stock_total WHERE id_articulo = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                res = cur.fetchone()
                return float(res[0]) if res else 0.0

    def get_next_number(self, id_tipo_documento: int) -> int:
        """Get the next serial number for a document type."""
        query = "SELECT MAX(numero_serie::integer) FROM app.documento WHERE id_tipo_documento = %s AND numero_serie ~ '^[0-9]+$'"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (id_tipo_documento,))
                res = cur.fetchone()
                current_max = res[0] if res and res[0] is not None else 0
                return current_max + 1

    def _lock_document_number(self, cur, id_tipo_documento: int) -> None:
        """Acquire an advisory lock for a document type to serialize numbering."""
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (id_tipo_documento,))

    def _next_document_number(self, cur, id_tipo_documento: int) -> int:
        cur.execute("SELECT MAX(numero_serie::bigint) FROM app.documento WHERE id_tipo_documento = %s AND numero_serie ~ '^[0-9]+$'", (id_tipo_documento,))
        res = cur.fetchone()
        last = res[0] if res and res[0] is not None else 0
        return int(last) + 1

    def _ensure_unique_document_number(self, cur, id_tipo_documento: int, numero_serie: str) -> None:
        cur.execute(
            "SELECT 1 FROM app.documento WHERE id_tipo_documento = %s AND numero_serie = %s LIMIT 1",
            (id_tipo_documento, numero_serie),
        )
        if cur.fetchone():
            raise ValueError("El número de serie ya existe.")

    def get_document_full(self, doc_id: int) -> Optional[Dict[str, Any]]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT d.id, d.id_tipo_documento, d.id_entidad_comercial, d.id_deposito, 
                           d.observacion, d.numero_serie, d.descuento_porcentaje, d.descuento_importe,
                           d.fecha::text, d.fecha_vencimiento::text, d.id_lista_precio,
                           d.neto, d.subtotal, d.iva_total, d.total, d.sena, d.estado, d.cae,
                           d.cae_vencimiento, d.cuit_emisor, d.qr_data, d.punto_venta, d.tipo_comprobante_afip,
                           d.direccion_entrega, td.nombre, td.letra
                    FROM app.documento d
                    JOIN ref.tipo_documento td ON td.id = d.id_tipo_documento
                    WHERE d.id = %s
                """, (doc_id,))
                head = cur.fetchone()
                if not head: return None
                
                doc = {
                    "id": head[0], "id_tipo_documento": head[1], "id_entidad_comercial": head[2],
                    "id_deposito": head[3], "observacion": head[4], "numero_serie": head[5],
                    "descuento_porcentaje": float(head[6]), "descuento_importe": float(head[7]),
                    "fecha": head[8], "fecha_vencimiento": head[9],
                    "id_lista_precio": head[10], "neto": float(head[11]), "subtotal": float(head[12]),
                    "iva_total": float(head[13]), "total": float(head[14]), "sena": float(head[15]),
                    "estado": head[16], "cae": head[17], "cae_vencimiento": head[18],
                    "cuit_emisor": head[19], "qr_data": head[20], "punto_venta": head[21],
                    "tipo_comprobante_afip": head[22], "direccion_entrega": head[23],
                    "tipo_documento": head[24], "letra": head[25],
                }
                
                cur.execute("""
                    SELECT id_articulo, cantidad, precio_unitario, descuento_porcentaje, descuento_importe,
                           porcentaje_iva, total_linea, id_lista_precio, observacion, descripcion_historica,
                           unidades_por_bulto_historico
                    FROM app.documento_detalle WHERE id_documento = %s ORDER BY nro_linea
                """, (doc_id,))
                items = []
                for row in cur.fetchall():
                    items.append({
                        "id_articulo": row[0], "cantidad": float(row[1]), 
                        "precio_unitario": float(row[2]),
                        "descuento_porcentaje": float(row[3] or 0),
                        "descuento_importe": float(row[4] or 0),
                        "porcentaje_iva": float(row[5] or 0),
                        "total_linea": float(row[6] or 0),
                        "id_lista_precio": row[7],
                        "observacion": row[8],
                        "descripcion_historica": row[9],
                        "unidades_por_bulto_historico": row[10],
                    })
                doc["items"] = items
                return doc

    def update_document(self, doc_id: int, *, id_tipo_documento: int, id_entidad_comercial: int, id_deposito: int, 
                        items: List[Dict[str, Any]], observacion: Optional[str] = None, 
                        numero_serie: Optional[str] = None, descuento_porcentaje: float = 0,
                        descuento_importe: float = 0,
                        descuento_global_mode: str = "percentage",
                        fecha: Optional[str] = None, fecha_vencimiento: Optional[str] = None,
                        id_lista_precio: Optional[int] = None,
                        direccion_entrega: Optional[str] = None,
                        sena: float = 0,
                        manual_values: Optional[Dict[str, float]] = None) -> bool:
        self._validate_document_item_quantities(items)
        pricing = calculate_document_totals(
            items=items,
            descuento_global_porcentaje=descuento_porcentaje,
            descuento_global_importe=descuento_importe,
            descuento_global_mode="amount" if str(descuento_global_mode).lower() == "amount" else "percentage",
            sena=sena,
            pricing_mode="tax_included",
        )

        subtotal = pricing["subtotal_bruto"]
        neto_total = pricing["neto"]
        iva_total = pricing["iva_total"]
        total = pricing["total"]
        desc_pct_normalized = pricing["descuento_global_porcentaje"]
        desc_imp_normalized = pricing["descuento_global_importe"]
        
        if manual_values:
            if "subtotal" in manual_values:
                neto_total = Decimal(str(manual_values["subtotal"]))
            if "iva_total" in manual_values:
                iva_total = Decimal(str(manual_values["iva_total"]))
            if "total" in manual_values:
                total = Decimal(str(manual_values["total"]))

        final_fecha = fecha if fecha else datetime.now()
        fecha_vencimiento_value = fecha_vencimiento
        if isinstance(fecha_vencimiento_value, str):
            fecha_vencimiento_value = fecha_vencimiento_value.strip() or None

        with self._transaction() as cur:
            cur.execute("""
                UPDATE app.documento
                SET id_tipo_documento=%s, id_entidad_comercial=%s, id_deposito=%s,
                    observacion=%s, numero_serie=%s, descuento_porcentaje=%s, descuento_importe=%s,
                    neto=%s, subtotal=%s, iva_total=%s, total=%s, sena=%s,
                    fecha=%s, fecha_vencimiento=%s, id_lista_precio=%s,
                    direccion_entrega=%s, id_usuario=%s
                WHERE id=%s
            """, (
                id_tipo_documento, id_entidad_comercial, id_deposito,
                observacion, numero_serie, desc_pct_normalized, desc_imp_normalized,
                neto_total, subtotal, iva_total, total, sena,
                final_fecha, fecha_vencimiento_value, id_lista_precio,
                direccion_entrega, self.current_user_id,
                doc_id
            ))
            
            cur.execute("DELETE FROM app.documento_detalle WHERE id_documento = %s", (doc_id,))
            
            detail_query = ("""
                INSERT INTO app.documento_detalle (
                    id_documento, nro_linea, id_articulo, cantidad, 
                    precio_unitario, descuento_porcentaje, descuento_importe,
                    porcentaje_iva, total_linea, id_lista_precio, observacion, unidades_por_bulto_historico
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            unidades_por_bulto_snapshot = self._build_unidades_por_bulto_snapshot(cur, pricing["items"])
            detail_rows = []
            for i, item in enumerate(pricing["items"], 1):
                article_id = _to_id(item.get("id_articulo"))
                detail_rows.append((
                    doc_id, i, item["id_articulo"], item["cantidad"],
                    item["precio_unitario"], item["descuento_porcentaje"], item["descuento_importe"],
                    item["porcentaje_iva"], item["total_linea"],
                    item.get("id_lista_precio"), item.get("observacion"),
                    unidades_por_bulto_snapshot.get(article_id) if article_id is not None else None,
                ))
            
            cur.executemany(detail_query, detail_rows)
            
            return True

    # =========================================================================
    # CUENTAS CORRIENTES
    # =========================================================================

    def registrar_movimiento_cc(
        self,
        id_entidad: int,
        tipo: str,
        concepto: str,
        monto: float,
        id_documento: Optional[int] = None,
        id_pago: Optional[int] = None,
        observacion: Optional[str] = None
    ) -> int:
        """
        Registra un movimiento de cuenta corriente.
        tipo: 'DEBITO' (aumenta deuda), 'CREDITO' (reduce deuda), 
              'AJUSTE_DEBITO', 'AJUSTE_CREDITO', 'ANULACION'
        """
        query = "SELECT app.registrar_movimiento_cc(%s::bigint, %s::varchar(20), %s::varchar(150), %s::numeric, %s::bigint, %s::bigint, %s::text, %s::bigint)"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                cur.execute(query, (
                    id_entidad, tipo, concepto, monto,
                    id_documento, id_pago, observacion, self.current_user_id
                ))
                res = cur.fetchone()
                conn.commit()
                return res[0] if res else 0

    def get_saldo_entidad(self, id_entidad: int) -> Dict[str, Any]:
        """Obtiene el saldo actual y detalles de cuenta corriente de una entidad."""
        query = """
            SELECT id_entidad_comercial, saldo_actual, limite_credito, 
                   ultimo_movimiento, tipo_entidad
            FROM app.saldo_cuenta_corriente
            WHERE id_entidad_comercial = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (id_entidad,))
                row = cur.fetchone()
                if row:
                    return {
                        "id_entidad": row[0],
                        "saldo": float(row[1]),
                        "limite_credito": float(row[2]),
                        "ultimo_movimiento": row[3],
                        "tipo_entidad": row[4]
                    }
                return {"id_entidad": id_entidad, "saldo": 0.0, "limite_credito": 0.0}

    def fetch_cuentas_corrientes(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Lista resumen de cuentas corrientes con saldos."""
        filters = ["1=1"]
        params = []
        advanced = advanced or {}

        if search:
            filters.append("(entidad ILIKE %s OR cuit ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)

        # Filtro tipo entidad
        tipo = advanced.get("tipo_entidad") or simple
        if tipo and tipo not in ("", "Todos", "Todas"):
            filters.append("tipo_entidad = %s")
            params.append(tipo.upper())

        # Filtro estado
        estado = advanced.get("estado")
        if estado == "DEUDOR":
            filters.append("saldo_actual > 0")
        elif estado == "A_FAVOR":
            filters.append("saldo_actual < 0")
        elif estado == "AL_DIA":
            filters.append("saldo_actual = 0")

        # Solo con saldo
        if advanced.get("solo_con_saldo"):
            filters.append("saldo_actual != 0")

        where_clause = " AND ".join(filters)
        sort_cols = {
            "entidad": "entidad",
            "saldo_actual": "saldo_actual",
            "ultimo_movimiento": "ultimo_movimiento",
            "tipo_entidad": "tipo_entidad",
        }
        order_by = self._build_order_by(sorts, sort_cols, default="saldo_actual DESC")

        query = f"""
            SELECT * FROM app.v_cuenta_corriente_resumen
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_cuentas_corrientes(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None
    ) -> int:
        """Cuenta registros de cuentas corrientes."""
        filters = ["1=1"]
        params = []
        advanced = advanced or {}

        if search:
            filters.append("(entidad ILIKE %s OR cuit ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)

        tipo = advanced.get("tipo_entidad") or simple
        if tipo and tipo not in ("", "Todos", "Todas"):
            filters.append("tipo_entidad = %s")
            params.append(tipo.upper())

        estado = advanced.get("estado")
        if estado == "DEUDOR":
            filters.append("saldo_actual > 0")
        elif estado == "A_FAVOR":
            filters.append("saldo_actual < 0")
        elif estado == "AL_DIA":
            filters.append("saldo_actual = 0")

        if advanced.get("solo_con_saldo"):
            filters.append("saldo_actual != 0")

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) as total FROM app.v_cuenta_corriente_resumen WHERE {where_clause}"
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                return res[0] if res else 0

    def fetch_movimientos_cc(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Lista movimientos de cuenta corriente."""
        filters = ["1=1"]  # Show all movements including annulled ones for full history
        params = []
        advanced = advanced or {}

        if search:
            filters.append("(entidad ILIKE %s OR concepto ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)

        # Filtro por entidad específica
        entidad = advanced.get("id_entidad") or advanced.get("entidad")
        if entidad and str(entidad) not in ("", "0", "Todos"):
            if str(entidad).isdigit():
                filters.append("id_entidad_comercial = %s")
                params.append(int(entidad))
            else:
                filters.append("entidad ILIKE %s")
                params.append(f"%{entidad.strip()}%")

        # Tipo movimiento
        tipo = advanced.get("tipo_movimiento")
        if tipo and tipo not in ("", "Todos"):
            filters.append("tipo_movimiento = %s")
            params.append(tipo)

        # Fechas
        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)

        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)

        where_clause = " AND ".join(filters)
        sort_cols = {
            "fecha": "fecha",
            "monto": "monto",
            "entidad": "entidad",
            "tipo_movimiento": "tipo_movimiento",
        }
        order_by = self._build_order_by(sorts, sort_cols, default="fecha DESC")

        query = f"""
            SELECT * FROM app.v_movimiento_cc_full
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_movimientos_cc(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None
    ) -> int:
        """Cuenta movimientos de cuenta corriente."""
        filters = ["1=1"]  # Show all movements including annulled ones for full history
        params = []
        advanced = advanced or {}

        if search:
            filters.append("(entidad ILIKE %s OR concepto ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 2)

        entidad = advanced.get("id_entidad") or advanced.get("entidad")
        if entidad and str(entidad) not in ("", "0", "Todos"):
            if str(entidad).isdigit():
                filters.append("id_entidad_comercial = %s")
                params.append(int(entidad))
            else:
                filters.append("entidad ILIKE %s")
                params.append(f"%{entidad.strip()}%")

        tipo = advanced.get("tipo_movimiento")
        if tipo and tipo not in ("", "Todos"):
            filters.append("tipo_movimiento = %s")
            params.append(tipo)

        desde = advanced.get("desde")
        if desde:
            filters.append("fecha >= %s")
            params.append(desde)

        hasta = advanced.get("hasta")
        if hasta:
            filters.append("fecha <= %s")
            params.append(hasta)

        where_clause = " AND ".join(filters)
        query = f"SELECT COUNT(*) FROM app.v_movimiento_cc_full WHERE {where_clause}"
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                return res[0] if res else 0

    def get_stats_cuenta_corriente(self) -> Dict[str, Any]:
        """Obtiene estadísticas de cuentas corrientes."""
        query = "SELECT * FROM app.v_stats_cuenta_corriente"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                if row:
                    return {
                        "deuda_clientes": float(row[0] or 0),
                        "clientes_deudores": int(row[1] or 0),
                        "deuda_proveedores": float(row[2] or 0),
                        "proveedores_acreedores": int(row[3] or 0),
                        "movimientos_hoy": int(row[4] or 0),
                        "cobros_hoy": float(row[5] or 0),
                        "facturacion_hoy": float(row[6] or 0),
                    }
                return {}

    def registrar_pago_cuenta_corriente(
        self,
        id_entidad: int,
        id_forma_pago: int,
        monto: float,
        concepto: str = "Pago recibido",
        referencia: Optional[str] = None,
        observacion: Optional[str] = None
    ) -> int:
        """
        Registra un pago/cobro directo a cuenta corriente sin documento asociado.
        """
        # Primero crear el pago (sin documento)
        pago_query = """
            INSERT INTO app.pago (id_documento, id_forma_pago, monto, referencia, observacion)
            VALUES (NULL, %s, %s, %s, %s)
            RETURNING id
        """
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._setup_session(cur)
                
                # Insertar el pago
                cur.execute(pago_query, (id_forma_pago, monto, referencia, observacion))
                res_pago = cur.fetchone()
                id_pago = res_pago[0] if res_pago else None
                
                # Registrar movimiento de cuenta corriente como CREDITO (reduce deuda)
                # Se pasa id_pago al movimiento para conciliación
                cur.execute(
                    "SELECT app.registrar_movimiento_cc(%s::bigint, %s::varchar(20), %s::varchar(150), %s::numeric, %s::bigint, %s::bigint, %s::text, %s::bigint)",
                    (
                        int(id_entidad), 
                        'CREDITO', 
                        concepto[:150], 
                        Decimal(str(monto)), 
                        None, 
                        id_pago, 
                        f"{observacion or ''} Ref: {referencia or 'N/A'}".strip(), 
                        self.current_user_id
                    )
                )
                res = cur.fetchone()
                mov_id = res[0] if res else 0
                
                conn.commit()
                self.log_activity("CUENTA_CORRIENTE", "PAGO_RECIBIDO", id_entidad, 
                                  detalle={"monto": monto, "forma_pago": id_forma_pago, "id_pago": id_pago})
                return mov_id

    def ajustar_saldo_cc(
        self,
        id_entidad: int,
        tipo: str,  # 'AJUSTE_DEBITO' o 'AJUSTE_CREDITO'
        monto: float,
        concepto: str,
        observacion: Optional[str] = None
    ) -> int:
        """Realiza un ajuste manual de saldo."""
        if tipo not in ('AJUSTE_DEBITO', 'AJUSTE_CREDITO'):
            raise ValueError("Tipo debe ser 'AJUSTE_DEBITO' o 'AJUSTE_CREDITO'")
        
        mov_id = self.registrar_movimiento_cc(
            id_entidad, tipo, concepto, monto, 
            id_documento=None, id_pago=None, observacion=observacion
        )
        
        self.log_activity("CUENTA_CORRIENTE", "AJUSTE", id_entidad,
                          detalle={"tipo": tipo, "monto": monto, "concepto": concepto})
        return mov_id

    def get_movimientos_entidad(self, id_entidad: int, limit: int = 500) -> List[Dict[str, Any]]:
        """Obtiene los últimos movimientos de una entidad específica."""
        return self.fetch_movimientos_cc(
            advanced={"id_entidad": id_entidad},
            limit=limit,
            sorts=[("fecha", "DESC")]
        )

    def fetch_remitos(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None,
        sorts: Optional[Sequence[Tuple[str, str]]] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch remitos with filtering and sorting."""
        filters = ["1=1"]
        params = []
        advanced = advanced or {}

        if search:
            filters.append("(numero ILIKE %s OR entidad ILIKE %s OR documento_numero ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 3)

        entidad = advanced.get("entidad")
        if entidad and str(entidad) not in ("", "0", "Todas"):
            if str(entidad).isdigit():
                filters.append("id_entidad_comercial = %s")
                params.append(int(entidad))
            else:
                filters.append("entidad ILIKE %s")
                params.append(f"%{entidad.strip()}%")

        estado_raw = advanced.get("estado")
        estado_value = str(estado_raw).strip() if estado_raw is not None else ""
        if estado_value and estado_value.upper() not in ("TODOS", "TODAS", "---", "0"):
            filters.append("estado = %s")
            params.append(estado_value)

        if advanced.get("deposito"):
             if str(advanced["deposito"]).isdigit():
                filters.append("id_deposito = %s")
                params.append(int(advanced["deposito"]))

        if advanced.get("documento"):
             filters.append("(documento_numero ILIKE %s)")
             params.append(f"%{advanced['documento'].strip()}%")

        if advanced.get("desde"):
            filters.append("fecha >= %s")
            params.append(advanced["desde"])

        if advanced.get("hasta"):
            filters.append("fecha <= %s")
            params.append(advanced["hasta"])

        where_clause = " AND ".join(filters)

        # Mapping for sort columns
        sort_cols = {
            "numero": "numero",
            "fecha": "fecha",
            "estado": "estado",
            "entidad": "entidad",
            "deposito": "deposito",
            "documento_numero": "documento_numero",
            "total_unidades": "total_unidades",
            "fecha_despacho": "fecha_despacho",
            "fecha_entrega": "fecha_entrega"
        }
        order_by = self._build_order_by(sorts, sort_cols, default="fecha DESC, numero DESC")

        query = f"""
            SELECT * FROM app.v_remito_resumen
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return _rows_to_dicts(cur)

    def count_remitos(
        self,
        search: Optional[str] = None,
        simple: Optional[str] = None,
        advanced: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count remitos with filtering."""
        filters = ["1=1"]
        params = []
        advanced = advanced or {}

        if search:
            filters.append("(numero ILIKE %s OR entidad ILIKE %s OR documento_numero ILIKE %s)")
            params.extend([f"%{search.strip()}%"] * 3)

        entidad = advanced.get("entidad")
        if entidad and str(entidad) not in ("", "0", "Todas"):
            if str(entidad).isdigit():
                filters.append("id_entidad_comercial = %s")
                params.append(int(entidad))
            else:
                filters.append("entidad ILIKE %s")
                params.append(f"%{entidad.strip()}%")

        estado_raw = advanced.get("estado")
        estado_value = str(estado_raw).strip() if estado_raw is not None else ""
        if estado_value and estado_value.upper() not in ("TODOS", "TODAS", "---", "0"):
            filters.append("estado = %s")
            params.append(estado_value)

        if advanced.get("deposito"):
             if str(advanced["deposito"]).isdigit():
                filters.append("id_deposito = %s")
                params.append(int(advanced["deposito"]))

        if advanced.get("documento"):
             filters.append("(documento_numero ILIKE %s)")
             params.append(f"%{advanced['documento'].strip()}%")

        if advanced.get("desde"):
            filters.append("fecha >= %s")
            params.append(advanced["desde"])

        if advanced.get("hasta"):
            filters.append("fecha <= %s")
            params.append(advanced["hasta"])

        where_clause = " AND ".join(filters)

        query = f"SELECT COUNT(*) FROM app.v_remito_resumen WHERE {where_clause}"

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                return res[0] if res else 0

    def update_remito_estado(self, remito_id: int, nuevo_estado: str) -> None:
        """Actualiza el estado del remito y registra las marcas de despacho/entrega."""
        estado_value = (nuevo_estado or "").strip().upper()
        valid_states = ("PENDIENTE", "DESPACHADO", "ENTREGADO", "ANULADO")
        if estado_value not in valid_states:
            raise ValueError(f"Estado inválido para remito: {nuevo_estado}")

        set_clauses: List[str] = ["estado = %s"]
        params: List[Any] = [estado_value]
        now = datetime.now()

        if estado_value == "DESPACHADO":
            set_clauses.append("fecha_despacho = COALESCE(fecha_despacho, %s)")
            params.append(now)
            set_clauses.append("fecha_entrega = NULL")
        elif estado_value == "ENTREGADO":
            set_clauses.append("fecha_entrega = %s")
            params.append(now)
            set_clauses.append("fecha_despacho = COALESCE(fecha_despacho, %s)")
            params.append(now)
        elif estado_value == "PENDIENTE":
            set_clauses.append("fecha_despacho = NULL")
            set_clauses.append("fecha_entrega = NULL")
        elif estado_value == "ANULADO":
            set_clauses.append("fecha_entrega = NULL")

        set_clauses.append("id_usuario = %s")
        params.append(self.current_user_id)
        params.append(remito_id)

        query = f"UPDATE app.remito SET {', '.join(set_clauses)} WHERE id = %s"
        with self._transaction() as cur:
            cur.execute(query, tuple(params))
            if cur.rowcount == 0:
                raise ValueError("Remito no encontrado")
        self.log_activity("app.remito", "UPDATE_ESTADO", id_entidad=remito_id, detalle={"estado": estado_value})

    def fetch_remito_detalle(self, remito_id: int) -> List[Dict[str, Any]]:
        """Obtiene el detalle de items de un remito."""
        query = """
            SELECT 
                rd.nro_linea,
                rd.id_articulo,
                a.nombre AS articulo,
                rd.cantidad,
                rd.observacion,
                a.unidades_por_bulto
            FROM app.remito_detalle rd
            JOIN app.articulo a ON a.id = rd.id_articulo
            WHERE rd.id_remito = %s
            ORDER BY rd.nro_linea
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (remito_id,))
                return _rows_to_dicts(cur)
