from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class SyncResult:
    success: bool
    created_tables: int
    added_columns: int
    created_indexes: int
    skipped_columns: int
    error: Optional[str] = None


@dataclass(frozen=True)
class ColumnDef:
    name: str
    definition: str


@dataclass(frozen=True)
class TableDef:
    schema: str
    name: str
    statement: str
    columns: List[ColumnDef]


@dataclass(frozen=True)
class IndexDef:
    name: str
    schema: str
    table: str
    statement: str


class SchemaSync:
    def __init__(
        self,
        db,
        *,
        sql_path: Path,
        logs_dir: Optional[Path] = None,
        config_key: str = "schema_hash",
    ) -> None:
        self.db = db
        self.sql_path = sql_path
        self.config_key = config_key
        self.logs_dir = logs_dir or (Path(__file__).resolve().parents[2] / "logs")
        self.log_path = self.logs_dir / "schema_sync.log"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.logger = self._configure_logger()

    def _configure_logger(self) -> logging.Logger:
        logger = logging.getLogger("schema_sync")
        logger.setLevel(logging.INFO)
        if not any(
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename) == self.log_path
            for handler in logger.handlers
        ):
            handler = logging.FileHandler(self.log_path, encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _file_hash(self) -> str:
        data = self.sql_path.read_bytes()
        return hashlib.sha256(data).hexdigest()

    def _strip_comments(self, sql: str) -> str:
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
        sql = re.sub(r"--.*?$", "", sql, flags=re.M)
        return sql

    def _extract_statements(self, sql: str, pattern: re.Pattern) -> List[str]:
        statements: List[str] = []
        for match in pattern.finditer(sql):
            start = match.start()
            end = sql.find(";", match.end())
            if end == -1:
                continue
            statements.append(sql[start : end + 1].strip())
        return statements

    def _extract_create_tables(self, sql: str) -> List[TableDef]:
        pattern = re.compile(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[^\s(]+)",
            re.IGNORECASE,
        )
        tables: List[TableDef] = []
        for match in pattern.finditer(sql):
            start = match.start()
            statement = self._slice_statement(sql, start)
            if not statement:
                continue
            raw_name = match.group("name")
            schema, name = self._parse_qualified_name(raw_name)
            columns = self._parse_table_columns(statement)
            tables.append(TableDef(schema=schema, name=name, statement=statement, columns=columns))
        return tables

    def _slice_statement(self, sql: str, start: int) -> Optional[str]:
        depth = 0
        idx = start
        while idx < len(sql):
            char = sql[idx]
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif char == ";" and depth == 0:
                return sql[start : idx + 1].strip()
            idx += 1
        return None

    def _parse_qualified_name(self, raw: str) -> Tuple[str, str]:
        raw = raw.strip()
        if "." in raw:
            schema_raw, table_raw = raw.split(".", 1)
        else:
            schema_raw, table_raw = "public", raw
        return self._strip_quotes(schema_raw), self._strip_quotes(table_raw)

    def _strip_quotes(self, value: str) -> str:
        value = value.strip()
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        return value

    def _parse_table_columns(self, statement: str) -> List[ColumnDef]:
        start = statement.find("(")
        end = statement.rfind(")")
        if start == -1 or end == -1 or end <= start:
            return []
        block = statement[start + 1 : end]
        parts = self._split_top_level(block)
        columns: List[ColumnDef] = []
        for part in parts:
            if not part:
                continue
            if re.match(r"^(CONSTRAINT|PRIMARY|FOREIGN|UNIQUE|CHECK)\b", part.strip(), re.I):
                continue
            name_match = re.match(r'^"([^"]+)"', part.strip())
            if name_match:
                col_name = name_match.group(1)
            else:
                name_match = re.match(r"^([A-Za-z_][\w$]*)", part.strip())
                if not name_match:
                    continue
                col_name = name_match.group(1)
            columns.append(ColumnDef(name=col_name, definition=part.strip()))
        return columns

    def _split_top_level(self, block: str) -> List[str]:
        parts: List[str] = []
        buf: List[str] = []
        depth = 0
        for char in block:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            if char == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
            else:
                buf.append(char)
        if buf:
            parts.append("".join(buf).strip())
        return parts

    def _extract_indexes(self, sql: str) -> List[IndexDef]:
        pattern = re.compile(
            r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[^\s]+)\s+ON\s+(?P<table>[^\s(]+)",
            re.IGNORECASE,
        )
        indexes: List[IndexDef] = []
        for match in pattern.finditer(sql):
            start = match.start()
            statement = self._slice_statement(sql, start)
            if not statement:
                continue
            index_name = self._strip_quotes(match.group("name"))
            schema, table = self._parse_qualified_name(match.group("table"))
            indexes.append(
                IndexDef(
                    name=index_name,
                    schema=schema,
                    table=table,
                    statement=statement,
                )
            )
        return indexes

    def _is_safe_column(self, definition: str) -> bool:
        upper = definition.upper()
        if "PRIMARY KEY" in upper or "UNIQUE" in upper or "CHECK" in upper:
            return False
        has_not_null = "NOT NULL" in upper
        has_default = "DEFAULT" in upper
        has_generated = "GENERATED" in upper
        if has_not_null and not (has_default or has_generated):
            return False
        return True

    def _table_exists(self, cur, schema: str, table: str) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None

    def _column_names(self, cur, schema: str, table: str) -> List[str]:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        return [row[0] for row in cur.fetchall()]

    def _index_exists(self, cur, schema: str, name: str) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = %s
              AND indexname = %s
            """,
            (schema, name),
        )
        return cur.fetchone() is not None

    def _get_db_hash(self) -> Optional[str]:
        conn = None
        conn = None
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT valor
                        FROM seguridad.config_sistema
                        WHERE clave = %s
                        """,
                        (self.config_key,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    return row[0] if not isinstance(row, dict) else row.get("valor")
        except Exception:
            return None

    def _set_db_hash(self, value: str) -> None:
        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO seguridad.config_sistema (clave, valor, tipo, descripcion)
                        VALUES (%s, %s, 'TEXT', 'Hash schema desde database.sql')
                        ON CONFLICT (clave) DO UPDATE
                        SET valor = EXCLUDED.valor
                        """,
                        (self.config_key, value),
                    )
                    conn.commit()
        except Exception as exc:
            self.logger.warning("No se pudo guardar schema hash: %s", exc)

    def needs_sync(self) -> bool:
        if not self.sql_path.exists():
            return False
        return self._file_hash() != (self._get_db_hash() or "")

    def apply(self, *, progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> SyncResult:
        if not self.sql_path.exists():
            return SyncResult(
                success=False,
                created_tables=0,
                added_columns=0,
                created_indexes=0,
                skipped_columns=0,
                error=f"No se encontro {self.sql_path}",
            )

        sql = self._strip_comments(self.sql_path.read_text(encoding="utf-8"))
        create_extensions = self._extract_statements(
            sql,
            re.compile(r"CREATE\s+EXTENSION\s+IF\s+NOT\s+EXISTS\s+[^;]+", re.IGNORECASE),
        )
        create_schemas = self._extract_statements(
            sql,
            re.compile(r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+[^;]+", re.IGNORECASE),
        )
        tables = self._extract_create_tables(sql)
        indexes = self._extract_indexes(sql)

        created_tables = 0
        added_columns = 0
        created_indexes = 0
        skipped_columns = 0

        try:
            with self.db.pool.connection() as conn:
                with conn.cursor() as cur:
                    if progress_callback:
                        progress_callback({"phase": "extensions", "message": "Creando extensiones..."})
                    for stmt in create_extensions:
                        cur.execute(stmt)

                    if progress_callback:
                        progress_callback({"phase": "schemas", "message": "Creando esquemas..."})
                    for stmt in create_schemas:
                        cur.execute(stmt)

                    if progress_callback:
                        progress_callback(
                            {
                                "phase": "tables",
                                "message": "Sincronizando tablas...",
                                "current": 0,
                                "total": len(tables),
                            }
                        )

                    for idx, table in enumerate(tables, start=1):
                        if not self._table_exists(cur, table.schema, table.name):
                            cur.execute(table.statement)
                            created_tables += 1
                        else:
                            existing_columns = set(self._column_names(cur, table.schema, table.name))
                            for col in table.columns:
                                if col.name in existing_columns:
                                    continue
                                if not self._is_safe_column(col.definition):
                                    skipped_columns += 1
                                    self.logger.info(
                                        "Skipping unsafe column %s.%s.%s",
                                        table.schema,
                                        table.name,
                                        col.name,
                                    )
                                    continue
                                alter = (
                                    f'ALTER TABLE "{table.schema}"."{table.name}" '
                                    f"ADD COLUMN {col.definition}"
                                )
                                cur.execute(alter)
                                added_columns += 1

                        if progress_callback:
                            progress_callback(
                                {
                                    "phase": "tables",
                                    "message": f"{table.schema}.{table.name}",
                                    "current": idx,
                                    "total": len(tables),
                                }
                            )

                    if progress_callback:
                        progress_callback(
                            {
                                "phase": "indexes",
                                "message": "Creando indices...",
                                "current": 0,
                                "total": len(indexes),
                            }
                        )

                    for idx, index in enumerate(indexes, start=1):
                        if not self._table_exists(cur, index.schema, index.table):
                            continue
                        if self._index_exists(cur, index.schema, index.name):
                            continue
                        cur.execute(index.statement)
                        created_indexes += 1
                        if progress_callback:
                            progress_callback(
                                {
                                    "phase": "indexes",
                                    "message": index.name,
                                    "current": idx,
                                    "total": len(indexes),
                                }
                            )

                conn.commit()
        except Exception as exc:
            self.logger.exception("Schema sync failed")
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            return SyncResult(
                success=False,
                created_tables=created_tables,
                added_columns=added_columns,
                created_indexes=created_indexes,
                skipped_columns=skipped_columns,
                error=str(exc),
            )

        self._set_db_hash(self._file_hash())
        return SyncResult(
            success=True,
            created_tables=created_tables,
            added_columns=added_columns,
            created_indexes=created_indexes,
            skipped_columns=skipped_columns,
        )
