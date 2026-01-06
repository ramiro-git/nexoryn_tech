#!/usr/bin/env python3
"""
Nexoryn Tech - Database Initialization Script
Handles: Schema creation, CSV import, reset functionality
"""

import argparse
import logging
import os
import sys
import io
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set, Union

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("ERROR: pandas is required. Install with: pip install pandas")
    sys.exit(1)

# Constants
MAX_BIGINT = 9223372036854775807
MIN_BIGINT = -9223372036854775808


class SuppressTriggers:
    """Context manager to temporarily disable triggers on specific tables."""
    def __init__(self, conn, tables: List[str]):
        self.conn = conn
        self.tables = tables

    def __enter__(self):
        logger.info(f"Suspending triggers for {len(self.tables)} tables to speed up import...")
        with self.conn.cursor() as cur:
            for table in self.tables:
                try:
                    cur.execute(sql.SQL("ALTER TABLE {} DISABLE TRIGGER ALL").format(sql.SQL(table)))
                except Exception as e:
                    logger.warning(f"Could not disable triggers for {table}: {e}")
        self.conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("Re-enabling triggers...")
        with self.conn.cursor() as cur:
            for table in self.tables:
                try:
                    cur.execute(sql.SQL("ALTER TABLE {} ENABLE TRIGGER ALL").format(sql.SQL(table)))
                except Exception as e:
                    logger.error(f"Could not enable triggers for {table}: {e}")
        self.conn.commit()

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_batch
except ImportError:
    print("ERROR: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

# Configuration
SCRIPT_DIR = Path(__file__).parent
CSV_DIR = SCRIPT_DIR / "csvs"
SCHEMA_FILE = SCRIPT_DIR / "database.sql"
LOG_FILE = SCRIPT_DIR / "init_db.log"

# CSV Files
CSV_FILES = {
    "cliprov": CSV_DIR / "CLIPROV.csv",
    "articulos": CSV_DIR / "ARTICULOS.csv",
    "ventcab": CSV_DIR / "VENTCAB.csv",
    "ventdet": CSV_DIR / "VENTDET.csv",
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Batch size for fast imports
BATCH_SIZE = 5000

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize Nexoryn Tech database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python init_db.py --reset                    # Full reset and import
  python init_db.py --skip-csv                 # Schema only, no CSV import
  python init_db.py --db-name test_nexoryn     # Use different database
        """
    )
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="nexoryn", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="", help="Database password (or use PGPASSWORD env)")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all schemas")
    parser.add_argument("--skip-csv", action="store_true", help="Skip CSV import")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    return parser.parse_args()


def get_connection(args: argparse.Namespace, database: Optional[str] = None) -> psycopg2.extensions.connection:
    """Create database connection with proper error handling."""
    password = args.password or os.environ.get("PGPASSWORD", "")
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=database or args.db_name,
            user=args.user,
            password=password,
            connect_timeout=10
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def ensure_database_exists(args: argparse.Namespace) -> None:
    """Create database if it doesn't exist."""
    try:
        conn = get_connection(args, database="postgres")
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (args.db_name,)
            )
            if not cur.fetchone():
                logger.info(f"Creating database '{args.db_name}'...")
                cur.execute(
                    sql.SQL("CREATE DATABASE {} ENCODING 'UTF8'").format(
                        sql.Identifier(args.db_name)
                    )
                )
                logger.info(f"Database '{args.db_name}' created successfully")
            else:
                logger.info(f"Database '{args.db_name}' already exists")
        conn.close()
    except Exception as e:
        logger.error(f"Error checking/creating database: {e}")
        raise


def reset_database(conn: psycopg2.extensions.connection) -> None:
    """Drop all schemas and recreate fresh."""
    logger.warning("RESET MODE: Dropping all schemas...")
    with conn.cursor() as cur:
        # Drop schemas in correct order (dependent first)
        schemas = ["app", "seguridad", "ref"]
        for schema in schemas:
            try:
                cur.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )
                logger.info(f"Dropped schema: {schema}")
            except Exception as e:
                logger.warning(f"Could not drop schema {schema}: {e}")
    conn.commit()
    logger.info("All schemas dropped successfully")


def execute_schema(conn: psycopg2.extensions.connection) -> None:
    """Execute the database schema file."""
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_FILE}")
    
    logger.info(f"Executing schema from: {SCHEMA_FILE}")
    
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    
    with conn.cursor() as cur:
        try:
            cur.execute(schema_sql)
            conn.commit()
            logger.info("Schema executed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Schema execution failed: {e}")
            raise


def fast_bulk_insert(cur: psycopg2.extensions.cursor, data: Union[List[Tuple], pd.DataFrame], table_name: str, columns: List[str]) -> Tuple[int, int]:
    """
    Execute High-Speed ingest using Postgres COPY protocol.
    This effectively streams the data directly to the DB socket, avoiding SQL parsing overhead.
    """
    if isinstance(data, list):
        if not data: return 0, 0
        df = pd.DataFrame(data, columns=columns)
    else:
        df = data
        
    if df.empty:
        return 0, 0
    
    # Create an in-memory CSV buffer
    s_buf = io.StringIO()
    
    # Export to CSV format in memory
    # index=False: No line numbers
    # header=False: COPY doesn't want headers usually
    # sep='\t': Tab is safer than comma
    # na_rep='\\N': Postgres standard NULL
    df.to_csv(s_buf, index=False, header=False, sep='\t', na_rep='\\N', quoting=csv.QUOTE_MINIMAL)
    
    s_buf.seek(0)
    
    # Don't prepend 'app.' for temporary tables or fully qualified names
    if "." in table_name or table_name.startswith("tmp_"):
        full_table = table_name
    else:
        full_table = f"app.{table_name}"
    
    columns_str = ", ".join(columns)
    
    sql_copy = f"COPY {full_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')"
    
    try:
        cur.copy_expert(sql_copy, s_buf)
        return len(df), 0
    except Exception as e:
        logger.error(f"Bulk COPY failed for {table_name}: {e}")
        return 0, len(df)


# ============================================================================
# CSV Parsing Utilities
# ============================================================================

def read_csv_safe(filepath: Path, encoding: str = "utf-8-sig") -> pd.DataFrame:
    """Read CSV file into Pandas DataFrame with error handling."""
    if not filepath.exists():
        logger.warning(f"CSV file not found: {filepath}")
        return pd.DataFrame()
    
    encodings = [encoding, "utf-8", "latin-1", "cp1252"]
    
    for enc in encodings:
        try:
            # Low_memory=False to avoid mixed type warnings on large files
            # dtype=str to preserve leading zeros in CUITs/Codes before we clean them
            df = pd.read_csv(filepath, encoding=enc, dtype=str, on_bad_lines='skip')
            
            # Basic cleanup
            df = df.replace({np.nan: None, "nan": None, "NULL": None, "": None})
            
            logger.info(f"Read {len(df)} rows from {filepath.name} (encoding: {enc})")
            return df
        except Exception:
            continue
    
    logger.error(f"Could not read {filepath.name} with any encoding")
    return pd.DataFrame()


class LookupCache:
    """Cache for reference table lookups to avoid repeated queries."""
    
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        self._cache: Dict[str, Dict[str, int]] = {}
    
    def _get_cache(self, schema: str, table: str) -> Dict[str, int]:
        key = f"{schema}.{table}"
        if key not in self._cache:
            self._cache[key] = {}
        return self._cache[key]

    def get(self, table: str, name: str, schema: str = "ref") -> Optional[int]:
        """Get ID from cache only (no DB hit)."""
        if not name: return None
        return self._get_cache(schema, table).get(name.strip().lower())

    def get_or_create(self, table: str, name_column: str, value: Any, schema: str = "ref") -> Optional[int]:
        """Get ID from reference table, creating if necessary. Vectorized-friendly check."""
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        
        val_str = str(value).strip()
        if not val_str: return None
        
        cache = self._get_cache(schema, table)
        val_lower = val_str.lower()
        if val_lower in cache:
            return cache[val_lower]
        
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    sql.SQL("INSERT INTO {}.{} ({}) VALUES (%s) ON CONFLICT ({}) DO UPDATE SET {}=EXCLUDED.{} RETURNING id").format(
                        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(name_column),
                        sql.Identifier(name_column), sql.Identifier(name_column), sql.Identifier(name_column)
                    ),
                    (val_str,)
                )
                res = cur.fetchone()
                if res:
                    new_id = res[0]
                    cache[val_lower] = new_id
                    return new_id
            except Exception as e:
                logger.warning(f"Error in get_or_create for {table}: {e}")
                self.conn.rollback()
            return None

    def preload(self, table: str, name_column: str, schema: str = "ref") -> None:
        """Preload all values from a reference table into cache."""
        cache = self._get_cache(schema, table)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT id, {} FROM {}.{}").format(
                    sql.Identifier(name_column), sql.Identifier(schema), sql.Identifier(table)
                )
            )
            for row in cur.fetchall():
                if row[1]:
                    cache[str(row[1]).strip().lower()] = row[0]

    def bulk_create(self, table: str, name_column: str, values: set, schema: str = "ref") -> None:
        """Bulk create missing values and update cache."""
        if not values: return
        cache = self._get_cache(schema, table)
        missing = [v for v in values if v and str(v).strip().lower() not in cache]
        if not missing: return

        with self.conn.cursor() as cur:
            data = [(str(m).strip(),) for m in missing]
            try:
                execute_batch(cur, 
                    sql.SQL("INSERT INTO {}.{} ({}) VALUES (%s) ON CONFLICT DO NOTHING").format(
                        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(name_column)
                    ),
                    data
                )
                self.preload(table, name_column, schema)
            except Exception as e:
                logger.warning(f"Bulk create failed for {table}: {e}")
                self.conn.rollback()


# ============================================================================
# Import Functions
# ============================================================================

import difflib

def normalize_provincia_name(name: Any) -> Optional[str]:
    """
    Normalize province names using aggressive matching.
    """
    if not name or not isinstance(name, str):
        return None
    
    raw = name.strip()
    if not raw:
        return None
        
    lower = raw.lower()
    
    # Aggressive Buenos Aires mapping
    # Covers: Bs As, Ba As, Bs, Bsas, Bs.As, Pcia Bs As, Buenos Aries, etc.
    if any(x in lower for x in ['bs', 'ba as', 'buenos', 'bueno', 'b.a', 'pcia']):
        # Distinguish CABA/Capital
        if any(c in lower for c in ['caba', 'capital', 'ciudad', 'cap', 'autonoma']):
             return "Ciudad Autónoma de Buenos Aires"
        return "Buenos Aires"
        
    # CABA / Capital Federal
    if any(x in lower for x in ['caba', 'capital', 'cap. fed', 'c.a.b.a', 'federal', 'barracas']):
        return "Ciudad Autónoma de Buenos Aires"
        
    # Common mistakes/abbreviations
    if 'cord' in lower: return "Córdoba"
    if 'fe' in lower and 'santa' in lower: return "Santa Fe"
    if 'rios' in lower or 'ríos' in lower: return "Entre Ríos"
    if 'tuc' in lower: return "Tucumán"
    if 'mendoza' in lower: return "Mendoza"
    if 'san juan' in lower: return "San Juan"
    if 'san luis' in lower: return "San Luis"
    if 'neuq' in lower: return "Neuquén"
    if 'rio neg' in lower: return "Río Negro"
    if 'chubut' in lower: return "Chubut"
    if 'misiones' in lower or 'mision' in lower: return "Misiones"
    if 'corrientes' in lower: return "Corrientes"
    if 'formosa' in lower: return "Formosa"
    if 'santiago' in lower: return "Santiago del Estero"
    if 'catamarca' in lower: return "Catamarca"
    if 'jujuy' in lower: return "Jujuy"
    if 'salta' in lower: return "Salta"
    if 'rioja' in lower: return "La Rioja"
    if 'pampa' in lower: return "La Pampa"
    if 'cruz' in lower: return "Santa Cruz"
    if 'tierra' in lower: return "Tierra del Fuego"

    # Canonical list of provinces for final fuzzy check
    PROVINCIAS = [
        "Buenos Aires", "Ciudad Autónoma de Buenos Aires", "Catamarca", "Chaco", "Chubut",
        "Córdoba", "Corrientes", "Entre Ríos", "Formosa", "Jujuy", "La Pampa", "La Rioja",
        "Mendoza", "Misiones", "Neuquén", "Río Negro", "Salta", "San Juan", "San Luis",
        "Santa Cruz", "Santa Fe", "Santiago del Estero", "Tierra del Fuego", "Tucumán"
    ]

    # Fuzzy match with lower cutoff
    matches = difflib.get_close_matches(raw, PROVINCIAS, n=1, cutoff=0.5)
    if matches:
        return matches[0]
        
    return raw.title()

def import_cliprov(conn: psycopg2.extensions.connection, cache: LookupCache) -> int:
    """Import CLIPROV.csv (Clients and Suppliers)"""
    rows = read_csv_safe(CSV_FILES["cliprov"], encoding="utf-8-sig")
    if rows.empty: return 0
    
    # Standardize column names to lowercase
    df = rows.copy()
    df.columns = [c.lower() for c in df.columns]

    # Required cleaning
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id']).copy()
    df = df[(df['id'] <= MAX_BIGINT) & (df['id'] >= MIN_BIGINT)].copy()
    
    if df.empty: return 0
        
    string_cols = ['apell', 'nomb', 'dom', 'otros', 'loc', 'provincia', 'ref', 'telefono', 'iva', 'fchalta']
    for col in string_cols:
         if col in df.columns:
            if col == 'telefono':
                df[col] = df[col].astype(str).str.replace(r'[\r\n\t]+', ' ', regex=True).str.strip()
            else:
                df[col] = df[col].astype(str).str.strip()
            
            df[col] = df[col].replace({'nan': None, 'None': None, '': None})
    
    # Normalize Province Names
    if 'provincia' in df.columns:
        df['provincia'] = df['provincia'].apply(normalize_provincia_name)
            
    df['tipo_entidad'] = np.where(df['ref'].astype(str).str.upper() == 'P', 'PROVEEDOR', 'CLIENTE')

    # 2. Bulk Create References
    ivas = df['iva'].dropna().unique()
    provincias = df['provincia'].dropna().unique()
    
    iva_map_code = { 
        "RI": "Responsable Inscripto", 
        "M": "Monotributista", 
        "CF": "Consumidor Final", 
        "EX": "Exento",
        "0": "Consumidor Final",
        "1": "Responsable Inscripto"
    }
    
    cache.preload("condicion_iva", "nombre")
    real_iva_names = set()
    for c in ivas:
        val = str(c).strip().upper()
        if val in iva_map_code:
            real_iva_names.add(iva_map_code[val])
        elif not val.isdigit() and val:
            real_iva_names.add(val)

    cache.bulk_create("condicion_iva", "nombre", real_iva_names)

    cache.preload("provincia", "nombre")
    cache.bulk_create("provincia", "nombre", set(provincias))
    prov_cache = cache._get_cache("ref", "provincia")
    df['prov_filled'] = df['provincia'].fillna("Buenos Aires")
    unique_locs = df[['loc', 'prov_filled']].dropna(subset=['loc']).drop_duplicates()
    
    with conn.cursor() as cur:
        cur.execute("SELECT id, lower(nombre), id_provincia FROM ref.localidad")
        localidad_cache = {(row[1], row[2]): row[0] for row in cur.fetchall()}
            
        new_localities = []
        for row in unique_locs.itertuples(index=False):
            loc_name = row.loc
            prov_name = row.prov_filled
            pid = prov_cache.get(prov_name.lower())
            if pid:
                key = (loc_name.lower(), pid)
                if key not in localidad_cache:
                    new_localities.append((loc_name, pid))
                    localidad_cache[key] = -1 
        
        if new_localities:
            execute_batch(cur, "INSERT INTO ref.localidad (nombre, id_provincia) VALUES (%s, %s) ON CONFLICT DO NOTHING", new_localities)
            conn.commit()
            cur.execute("SELECT id, lower(nombre), id_provincia FROM ref.localidad")
            localidad_cache = {(row[1], row[2]): row[0] for row in cur.fetchall()}

    # 3. Map Data
    iva_cache = cache._get_cache("ref", "condicion_iva")
    def get_iva_id(val):
        if not val: return None
        return iva_cache.get(iva_map_code.get(val.upper(), val).lower())
    
    df['id_iva'] = df['iva'].map(get_iva_id).astype('Int64')
    
    # Locality Mapping
    def get_loc_id(r):
        pid = prov_cache.get(str(r['prov_filled']).lower())
        return localidad_cache.get((str(r['loc']).lower(), pid))
    
    df['id_loc'] = df.apply(get_loc_id, axis=1).astype('Int64')

    if 'cuit' in df.columns:
        df['cuit_clean'] = df['cuit'].astype(str).str.replace(r'[ -]', '', regex=True).str.slice(0, 13).replace({'None': None, 'nan': None, '': None})
    else:
        df['cuit_clean'] = None

    df['fchalta_dt'] = pd.to_datetime(df['fchalta'], format='mixed', errors='coerce').fillna(datetime.now())
    
    final_df = pd.DataFrame()
    final_df['id'] = df['id'].astype(int)
    final_df['apellido'] = df['apell'].str.slice(0, 100)
    final_df['nombre'] = df['nomb'].str.slice(0, 100)
    final_df['razon_social'] = df['apell'].str.slice(0, 200) # Often used as company name if Nomb is empty
    final_df['domicilio'] = df['dom'].str.slice(0, 255)
    final_df['id_localidad'] = df['id_loc']
    final_df['cuit'] = df['cuit_clean']
    final_df['id_condicion_iva'] = df['id_iva']
    final_df['notas'] = df['otros'].str.slice(0, 500)
    final_df['fecha_creacion'] = df['fchalta_dt']
    final_df['telefono'] = df['telefono'].str.slice(0, 100)
    
    def extract_email(text):
        if not text: return None
        import re
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', str(text))
        return match.group(0) if match else None
    
    final_df['email'] = df['otros'].apply(extract_email).str.slice(0, 150)
    final_df['tipo'] = df['tipo_entidad']

    # COPY 
    columns = ['id', 'apellido', 'nombre', 'razon_social', 'domicilio', 'id_localidad', 'cuit', 'id_condicion_iva', 'notas', 'fecha_creacion', 'telefono', 'email', 'tipo']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_entidad_comercial (LIKE app.entidad_comercial INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[columns], "tmp_entidad_comercial", columns)
        cur.execute("""
            INSERT INTO app.entidad_comercial (id, apellido, nombre, razon_social, domicilio, id_localidad, cuit, id_condicion_iva, notas, fecha_creacion, telefono, email, tipo)
            OVERRIDING SYSTEM VALUE
            SELECT id, apellido, nombre, razon_social, domicilio, id_localidad, cuit, id_condicion_iva, notas, fecha_creacion, telefono, email, tipo
            FROM tmp_entidad_comercial
            ON CONFLICT (id) DO UPDATE SET
                apellido = EXCLUDED.apellido, 
                nombre = EXCLUDED.nombre,
                razon_social = EXCLUDED.razon_social,
                domicilio = EXCLUDED.domicilio,
                id_localidad = EXCLUDED.id_localidad,
                cuit = EXCLUDED.cuit,
                id_condicion_iva = EXCLUDED.id_condicion_iva,
                notas = EXCLUDED.notas,
                fecha_creacion = EXCLUDED.fecha_creacion,
                telefono = EXCLUDED.telefono,
                email = EXCLUDED.email,
                tipo = EXCLUDED.tipo
        """)
        imported = cur.rowcount

    # 4. Map Price Lists and Discounts (NEW)
    if 'lista' in df.columns:
        cache.preload("lista_precio", "nombre")
        lp_cache = cache._get_cache("ref", "lista_precio")
        
        # Determine a safe default ID
        default_lp_id = lp_cache.get('lista 1')
        if not default_lp_id and lp_cache:
            default_lp_id = next(iter(lp_cache.values()))
            
        def get_lp_id(val):
            if not val: return default_lp_id
            v_str = str(val).strip().lower()
            if v_str == 'g': return lp_cache.get('lista gremio') or default_lp_id
            if v_str.isdigit():
                return lp_cache.get(f"lista {v_str}") or default_lp_id
            return default_lp_id

        df['id_lp'] = df['lista'].map(get_lp_id).astype('Int64')
        
        # If any remained null (e.g. empty cache), fallback to first ID
        if df['id_lp'].isnull().any():
            logger.warning("CLIPROV: Some entities have no valid price list mapping; using default ID 1.")
            df['id_lp'] = df['id_lp'].fillna(default_lp_id or 1)

        df['perc_desc'] = pd.to_numeric(df['desc'], errors='coerce').fillna(0)
        
        lista_cliente_df = pd.DataFrame()
        lista_cliente_df['id_entidad_comercial'] = df['id'].astype(int)
        lista_cliente_df['id_lista_precio'] = df['id_lp'].astype(int)
        lista_cliente_df['descuento'] = df['perc_desc']
        
        lista_cliente_df['limite_credito'] = 0
        lista_cliente_df['saldo_cuenta'] = 0
        
        lc_cols = ['id_entidad_comercial', 'id_lista_precio', 'descuento', 'limite_credito', 'saldo_cuenta']
        
        with conn.cursor() as cur:
             cur.execute("CREATE TEMP TABLE tmp_lista_cliente (id_entidad_comercial BIGINT, id_lista_precio BIGINT, descuento NUMERIC, limite_credito NUMERIC, saldo_cuenta NUMERIC) ON COMMIT DROP")
             fast_bulk_insert(cur, lista_cliente_df[lc_cols], "tmp_lista_cliente", lc_cols)
             cur.execute("""
                 INSERT INTO app.lista_cliente (id_entidad_comercial, id_lista_precio, descuento, limite_credito, saldo_cuenta)
                 SELECT id_entidad_comercial, id_lista_precio, descuento, limite_credito, saldo_cuenta FROM tmp_lista_cliente
                 ON CONFLICT (id_entidad_comercial) DO UPDATE SET
                     id_lista_precio = EXCLUDED.id_lista_precio,
                     descuento = EXCLUDED.descuento
             """)
             logger.info(f"CLIPROV: Assigned price lists to {cur.rowcount} entities.")

    conn.commit()
    logger.info(f"CLIPROV: Imported {imported}")
    return imported


def import_articulos(conn: psycopg2.extensions.connection, cache: LookupCache) -> int:
    """Import ARTICULOS.csv using Pandas + COPY Protocol."""
    rows = read_csv_safe(CSV_FILES["articulos"], encoding="utf-8-sig")
    if rows.empty: return 0

    # Standardize column names to lowercase early
    rows.columns = [c.strip().lower() for c in rows.columns]

    # Normalize legacy variants for stock columns if needed
    col_renames = {}
    for src, dst in {
        "stock_minimo": "stkmin",
        "stockmin": "stkmin",
        "stock_actual": "stk",
        "stock": "stk",
    }.items():
        if src in rows.columns and dst not in rows.columns:
            col_renames[src] = dst
    if col_renames:
        rows = rows.rename(columns=col_renames)
    
    # 2025-12-31: Map Legacy Placeholders to Generic (User Request)
    # Marca 25 -> Genérica
    # Rubro 23 -> Genérico
    if 'idmar' in rows.columns:
        rows['idmar'] = rows['idmar'].astype(str).replace({'25': 'LEGACY_INTERNAL_25', '25.0': 'LEGACY_INTERNAL_25'})
    if 'idrub' in rows.columns:
        rows['idrub'] = rows['idrub'].astype(str).replace({'23': 'LEGACY_INTERNAL_23', '23.0': 'LEGACY_INTERNAL_23'})
    
    # Required filtering by Id
    rows['idart'] = pd.to_numeric(rows['idart'], errors='coerce')
    df = rows.dropna(subset=['idart']).copy()
    df = df[(df['idart'] <= MAX_BIGINT) & (df['idart'] >= MIN_BIGINT)].copy()
    if df.empty: return 0

    # Required cleaning
    for col in ['art', 'idmar', 'idrub', 'unidad', 'stk', 'stkmin', 'idprov', 'pgan', 'pgan2', 'pventa']:
        if col in df.columns:
             # Strip .0 from numeric-like strings to handle float conversion artifacts
             df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None})
             df[col] = df[col].apply(lambda x: str(x).split('.')[0] if x and str(x).replace('.','',1).isdigit() and '.' in str(x) and str(x).endswith('.0') else x)

    stock_col = "stk" if "stk" in df.columns else None
    stock_min_col = "stkmin" if "stkmin" in df.columns else None
    if stock_min_col is None:
        logger.warning("ARTICULOS: Missing StkMin column; defaulting stock_minimo to 0.")
             
    # Create Brands, Rubros, Units preserving IDs if numeric
    def bulk_create_with_ids(table, values):
        with conn.cursor() as cur:
            # Separate numeric and non-numeric
            to_insert = []
            for v in values:
                if not v: continue
                v_str = str(v).strip()
                if v_str.isdigit():
                    to_insert.append((int(v_str), f"Marca {v_str}" if table=='marca' else (f"Rubro {v_str}" if table=='rubro' else v_str)))
                else:
                    to_insert.append((None, v_str))
            
            for cid, name in to_insert:
                if cid:
                    # ON CONFLICT DO NOTHING to avoid overwriting existing good names (like 'Unidad') with placeholders (like '1')
                    cur.execute(f"INSERT INTO ref.{table} (id, nombre) OVERRIDING SYSTEM VALUE VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", (cid, name))
                else:
                    cur.execute(f"INSERT INTO ref.{table} (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING", (name,))
            conn.commit()

    # Cleanup existing legacy markers if any (prevent them from sticking around)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ref.marca WHERE nombre LIKE '%LEGACY_MAP%'")
        cur.execute("DELETE FROM ref.rubro WHERE nombre LIKE '%LEGACY_MAP%'")
        conn.commit()

    marcas = [m for m in df['idmar'].dropna().unique() if 'LEGACY_INTERNAL' not in str(m)]
    bulk_create_with_ids("marca", marcas)
    # Ensure "Genérica" exists
    bulk_create_with_ids("marca", ["Genérica"])
    
    rubros = [r for r in df['idrub'].dropna().unique() if 'LEGACY_INTERNAL' not in str(r)]
    bulk_create_with_ids("rubro", rubros)
    # Ensure "Genérico" exists
    bulk_create_with_ids("rubro", ["Genérico"])
    
    unidades = df['unidad'].dropna().unique()
    bulk_create_with_ids("unidad_medida", unidades)
    
    # 3. Vectorized Mapping
    
    # Preload IVA types (standard ones seeded in database.sql)
    cache.preload("tipo_iva", "porcentaje")
    
    marca_map_name = cache._get_cache("ref", "marca")
    rubro_map_name = cache._get_cache("ref", "rubro")
    unidad_map = cache._get_cache("ref", "unidad_medida")
    
    # Also create ID-to-ID fallback maps
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM ref.marca")
        marca_ids = {str(r[0]): r[0] for r in cur.fetchall()}
        cur.execute("SELECT id FROM ref.rubro")
        rubro_ids = {str(r[0]): r[0] for r in cur.fetchall()}

    def map_relation(val, name_map, id_map):
        if val is None: return None
        v_str = str(val).lower()
        
        # Legacy Mapping Hooks
        if 'LEGACY_INTERNAL_25' in v_str:
            generic_brand = cache.get("marca", "Genérica")
            if generic_brand: return generic_brand
        if 'LEGACY_INTERNAL_23' in v_str:
            generic_rubro = cache.get("rubro", "Genérico")
            if generic_rubro: return generic_rubro

        # Try ID map first (since legacy IDs often clash with names)
        res = id_map.get(str(val))
        if res is not None: return res
        # Try name map
        return name_map.get(v_str)

    df['id_marca'] = df['idmar'].apply(lambda x: map_relation(x, marca_map_name, marca_ids)).astype('Int64')
    df['id_rubro'] = df['idrub'].apply(lambda x: map_relation(x, rubro_map_name, rubro_ids)).astype('Int64')
    
    # Also create ID-to-ID fallback maps for units
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM ref.unidad_medida")
        unidad_ids = {str(r[0]): r[0] for r in cur.fetchall()}
    
    df['id_unidad'] = df['unidad'].apply(lambda x: map_relation(x, unidad_map, unidad_ids)).astype('Int64')

    # Defaults for Rubro/Marca (Genérica / Genérico)
    id_generica = cache.get_or_create("marca", "nombre", "Genérica")
    id_generico = cache.get_or_create("rubro", "nombre", "Genérico")
    
    if id_generica is not None:
        df['id_marca'] = df['id_marca'].fillna(id_generica)
    if id_generico is not None:
        df['id_rubro'] = df['id_rubro'].fillna(id_generico)
    
    # Use cached ID for 21% (should be in seed data)
    id_iva_21 = cache.get("tipo_iva", "21.00") or 1 
    
    final_df = pd.DataFrame()
    final_df['id'] = df['idart'].astype(int)
    final_df['nombre'] = df['art'].fillna("Articulo Sin Nombre").str.slice(0, 200)
    final_df['id_marca'] = df['id_marca']
    final_df['id_rubro'] = df['id_rubro']
    final_df['id_tipo_iva'] = id_iva_21 
    final_df['costo'] = pd.to_numeric(df['costo'], errors='coerce').fillna(0)
    if stock_min_col:
        final_df['stock_minimo'] = pd.to_numeric(df[stock_min_col], errors='coerce').fillna(0)
    else:
        final_df['stock_minimo'] = 0
    final_df['id_unidad_medida'] = df['id_unidad']
    final_df['id_proveedor'] = pd.to_numeric(df['idprov'], errors='coerce').fillna(0).astype('Int64').replace(0, None)
    final_df['descuento_base'] = pd.to_numeric(df['desc'], errors='coerce').fillna(0)
    final_df['redondeo'] = df['redo'].apply(lambda x: str(x).lower() in ['1', 'true', 's'] if pd.notnull(x) else False)
    final_df['porcentaje_ganancia_2'] = pd.to_numeric(df['pgan2'], errors='coerce') if 'pgan2' in df.columns else None
    final_df['activo'] = True
    # Move legacy "ubicacion" (which are stock update notes) to observation
    obs_raw = df['obs'].fillna("").astype(str)
    ubic_raw = df['ubic'].fillna("").astype(str)
    final_df['observacion'] = (ubic_raw + ". " + obs_raw).str.strip(". ").str.slice(0, 500).replace('', None)

    # Use default physical location from the first deposit
    with conn.cursor() as cur:
        cur.execute("SELECT nombre FROM ref.deposito ORDER BY id LIMIT 1")
        default_loc_row = cur.fetchone()
        default_loc = default_loc_row[0] if default_loc_row else "Depósito Central"
    
    final_df['ubicacion'] = default_loc
    final_df['fecha_creacion'] = datetime.now()

    stock_df = pd.DataFrame()
    if stock_col:
        stock_df = df[df[stock_col].notnull()].copy()
        stock_df['stk_val'] = pd.to_numeric(stock_df[stock_col], errors='coerce').fillna(0)
        stock_df = stock_df[stock_df['stk_val'] != 0].copy()
    else:
        logger.warning("ARTICULOS: Missing STK column; skipping initial stock import.")
    
    columns = ['id', 'nombre', 'id_marca', 'id_rubro', 'id_tipo_iva', 'costo', 'stock_minimo', 'id_unidad_medida', 'id_proveedor', 'descuento_base', 'redondeo', 'porcentaje_ganancia_2', 'activo', 'observacion', 'ubicacion', 'fecha_creacion']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_articulo (LIKE app.articulo INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[columns], "tmp_articulo", columns)
        cur.execute("""
            INSERT INTO app.articulo (id, nombre, id_marca, id_rubro, id_tipo_iva, costo, stock_minimo, id_unidad_medida, id_proveedor, descuento_base, redondeo, porcentaje_ganancia_2, activo, observacion, ubicacion, fecha_creacion)
            OVERRIDING SYSTEM VALUE
            SELECT id, nombre, id_marca, id_rubro, id_tipo_iva, costo, stock_minimo, id_unidad_medida, id_proveedor, descuento_base, redondeo, porcentaje_ganancia_2, activo, observacion, ubicacion, fecha_creacion
            FROM tmp_articulo
            ON CONFLICT (id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                id_marca = EXCLUDED.id_marca,
                id_rubro = EXCLUDED.id_rubro,
                id_tipo_iva = EXCLUDED.id_tipo_iva,
                costo = EXCLUDED.costo,
                stock_minimo = EXCLUDED.stock_minimo,
                id_unidad_medida = EXCLUDED.id_unidad_medida,
                id_proveedor = EXCLUDED.id_proveedor,
                descuento_base = EXCLUDED.descuento_base,
                redondeo = EXCLUDED.redondeo,
                porcentaje_ganancia_2 = EXCLUDED.porcentaje_ganancia_2,
                observacion = EXCLUDED.observacion,
                ubicacion = EXCLUDED.ubicacion
        """)
        imported = cur.rowcount

        # NEW: Import Prices (L1..L7)
        if imported > 0:
            logger.info("Importing prices for articles...")
            
            # Fetch Metadata
            cache.preload("lista_precio", "nombre")
            cache.preload("tipo_porcentaje", "tipo")
            
            list_cache = cache._get_cache("ref", "lista_precio")
            pct_cache = cache._get_cache("ref", "tipo_porcentaje")
            
            id_margen = pct_cache.get('margen')
            id_descuento = pct_cache.get('descuento')
            
            price_rows = []
            
            # List 1 (Base/Retail)
            l1_id = list_cache.get('lista 1')
            if l1_id:
                l1_df = pd.DataFrame()
                l1_df['id_articulo'] = df['idart'].astype(int)
                l1_df['id_lista_precio'] = l1_id
                l1_df['precio'] = pd.to_numeric(df['pventa'], errors='coerce').fillna(0)
                l1_df['porcentaje'] = pd.to_numeric(df['pgan'], errors='coerce').fillna(0)
                l1_df['id_tipo_porcentaje'] = id_margen
                price_rows.append(l1_df)

            # Lists 2-7
            for i in range(2, 8):
                lname = f"lista {i}"
                lid = list_cache.get(lname)
                col_l = f"l{i}"
                col_pl = f"pl{i}"
                
                if lid and col_l in df.columns:
                    li_df = pd.DataFrame()
                    li_df['id_articulo'] = df['idart'].astype(int)
                    li_df['id_lista_precio'] = lid
                    li_df['precio'] = pd.to_numeric(df[col_l], errors='coerce').fillna(0)
                    # Safety check for missing PL columns
                    if col_pl in df.columns:
                        li_df['porcentaje'] = pd.to_numeric(df[col_pl], errors='coerce').fillna(0)
                    else:
                        li_df['porcentaje'] = 0
                    li_df['id_tipo_porcentaje'] = id_margen # Typically sale lists are markups
                    price_rows.append(li_df)
            
            # Lista Gremio (PVGremio column)
            gremio_id = list_cache.get('lista gremio')
            if gremio_id and 'pvgremio' in df.columns:
                gremio_df = pd.DataFrame()
                gremio_df['id_articulo'] = df['idart'].astype(int)
                gremio_df['id_lista_precio'] = gremio_id
                gremio_df['precio'] = pd.to_numeric(df['pvgremio'], errors='coerce').fillna(0)
                gremio_df['porcentaje'] = 0  # No percentage for gremio, just fixed prices
                gremio_df['id_tipo_porcentaje'] = id_descuento
                price_rows.append(gremio_df)
            
            if price_rows:
                all_prices = pd.concat(price_rows, ignore_index=True)
                # Filter out entries where BOTH price and percentage are zero or null
                all_prices = all_prices[(all_prices['precio'] != 0) | (all_prices['porcentaje'] != 0)].dropna(subset=['precio', 'porcentaje'], how='all')
                
                price_cols = ['id_articulo', 'id_lista_precio', 'precio', 'porcentaje', 'id_tipo_porcentaje']
                
                cur.execute("""
                    CREATE TEMP TABLE tmp_precios (
                        id_articulo BIGINT,
                        id_lista_precio BIGINT,
                        precio NUMERIC,
                        porcentaje NUMERIC,
                        id_tipo_porcentaje BIGINT
                    ) ON COMMIT DROP
                """)
                fast_bulk_insert(cur, all_prices[price_cols], "tmp_precios", price_cols)
                cur.execute("""
                    INSERT INTO app.articulo_precio (id_articulo, id_lista_precio, precio, porcentaje, id_tipo_porcentaje)
                    SELECT id_articulo, id_lista_precio, precio, porcentaje, id_tipo_porcentaje FROM tmp_precios
                    ON CONFLICT (id_articulo, id_lista_precio) DO UPDATE SET
                        precio = EXCLUDED.precio,
                        porcentaje = EXCLUDED.porcentaje,
                        id_tipo_porcentaje = EXCLUDED.id_tipo_porcentaje
                """)
                logger.info(f"Imported {cur.rowcount} price entries.")

        # NEW: Generate Initial Stock Movements
        if not stock_df.empty:
            logger.info(f"Generating initial stock movements from STK for {len(stock_df)} articles...")
            cache.preload("tipo_movimiento_articulo", "nombre")
            mov_type_cache = cache._get_cache("ref", "tipo_movimiento_articulo")
            id_ajuste_pos = mov_type_cache.get("ajuste positivo") or 5
            id_ajuste_neg = mov_type_cache.get("ajuste negativo") or 6

            mov_df = pd.DataFrame()
            mov_df['id_articulo'] = stock_df['idart'].astype(int)
            mov_df['cantidad'] = stock_df['stk_val'].abs()  # Always absolute value
            mov_df['id_tipo_movimiento'] = id_ajuste_pos
            mov_df['id_deposito'] = 1 # Deposito Central
            mov_df['observacion'] = "Stock Inicial (Legacy)"

            # Handle negative stock values from legacy if any
            mask_neg = stock_df['stk_val'] < 0
            mov_df.loc[mask_neg, 'id_tipo_movimiento'] = id_ajuste_neg

            mov_cols = ['id_articulo', 'id_tipo_movimiento', 'cantidad', 'id_deposito', 'observacion']
            cur.execute("""
                CREATE TEMP TABLE tmp_stk_mov (
                    id_articulo BIGINT,
                    id_tipo_movimiento BIGINT,
                    cantidad NUMERIC,
                    id_deposito BIGINT,
                    observacion TEXT
                ) ON COMMIT DROP
            """)
            fast_bulk_insert(cur, mov_df[mov_cols], "tmp_stk_mov", mov_cols)
            cur.execute("""
                INSERT INTO app.movimiento_articulo (id_articulo, id_tipo_movimiento, cantidad, id_deposito, observacion)
                SELECT id_articulo, id_tipo_movimiento, cantidad, id_deposito, observacion FROM tmp_stk_mov
            """)
            logger.info(f"Generated {len(mov_df)} initial stock movements.")

    conn.commit()
    logger.info(f"ARTICULOS: Imported {imported}")
    return imported


def import_ventcab(conn: psycopg2.extensions.connection, cache: LookupCache) -> int:
    """Import VENTCAB.csv (Sales Headers)"""
    rows = read_csv_safe(CSV_FILES["ventcab"])
    if rows.empty: return 0
    
    # Standardize column names to lowercase
    df = rows.copy()
    df.columns = [c.lower() for c in df.columns]

    df['idv'] = pd.to_numeric(df['idv'], errors='coerce')
    df = df.dropna(subset=['idv']).copy()
    
    cache.preload("tipo_documento", "nombre")
    type_map = {
        'FCA': 'FACTURA A', 'FCB': 'FACTURA B', 'FCC': 'FACTURA C',
        'NCA': 'NOTA CREDITO A', 'NCB': 'NOTA CREDITO B', 'NCC': 'NOTA CREDITO C',
        'PRE': 'PRESUPUESTO'
    }
    
    doc_type_cache = cache._get_cache("ref", "tipo_documento")
    
    def get_doc_id(code):
        if not code: return None
        clean = str(code).upper().replace('-','')
        name = type_map.get(clean, 'PRESUPUESTO')
        return doc_type_cache.get(name.lower())

    df['id_td'] = df['tipo'].map(get_doc_id).fillna(doc_type_cache.get('presupuesto')).astype('Int64')
    
    # Ensure all referenced entities exist (map missing to MOSTRADOR)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM app.entidad_comercial")
        existing_ids = set(row[0] for row in cur.fetchall())
    
    mostrador_id = 4
    df['id_entidad_clean'] = pd.to_numeric(df['idcli'], errors='coerce').fillna(mostrador_id).astype(int)
    # If the ID provided doesn't exist in master, use mostrador
    df['id_entidad_comercial_final'] = df['id_entidad_clean'].apply(lambda x: x if x in existing_ids else mostrador_id)
        
    final_df = pd.DataFrame()
    final_df['id'] = df['idv'].astype(int)
    final_df['id_tipo_documento'] = df['id_td']
    # Use 'mixed' format to handle ISO (YYYY-MM-DD) or Argentine (DD/MM/YYYY) without warnings
    final_df['fecha'] = pd.to_datetime(df['fch'], format='mixed', errors='coerce').fillna(datetime.now())
    # NFact represents the invoice number/series
    final_df['numero_serie'] = df['nfact'].astype(str).str.slice(0, 20).replace({'nan': None, 'None': None}, regex=True)
    final_df['id_entidad_comercial'] = df['id_entidad_comercial_final']
    final_df['observacion'] = df['obs'].astype(str).replace({'nan': None, 'None': None, '': None}, regex=True)

    # Determine status from CSV Paga/Anul columns
    def determine_status(row):
        if str(row.get('anul')) == '1': return 'ANULADO'
        if str(row.get('paga')) == '1': return 'PAGADO'
        return 'CONFIRMADO'
    
    final_df['estado'] = df.apply(determine_status, axis=1)
    final_df['total'] = pd.to_numeric(df['tventa'], errors='coerce').fillna(0)
    final_df['sena'] = pd.to_numeric(df['sena'], errors='coerce').fillna(0)
    
    # Use accurate CSV columns if available
    if 'neto' in df.columns:
        final_df['neto'] = pd.to_numeric(df['neto'], errors='coerce').fillna(0)
    else:
        final_df['neto'] = final_df['total'] / 1.21
        
    if 'subt' in df.columns:
        final_df['subtotal'] = pd.to_numeric(df['subt'], errors='coerce').fillna(final_df['neto'])
    else:
        final_df['subtotal'] = final_df['neto']

    if 'tiva' in df.columns:
        final_df['iva_total'] = pd.to_numeric(df['tiva'], errors='coerce').fillna(0)
    else:
        final_df['iva_total'] = final_df['total'] - final_df['neto']
    
    if 'desc' in df.columns:
        final_df['descuento_importe'] = pd.to_numeric(df['desc'], errors='coerce').fillna(0).abs()
    else:
        final_df['descuento_importe'] = 0
    
    final_df['descuento_porcentaje'] = 0
    
    columns = ['id', 'id_tipo_documento', 'fecha', 'numero_serie', 'id_entidad_comercial', 'estado', 'total', 'neto', 'subtotal', 'iva_total', 'sena', 'descuento_porcentaje', 'descuento_importe', 'observacion']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_doc (LIKE app.documento INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[columns], "tmp_doc", columns)
        cur.execute("""
            INSERT INTO app.documento (id, id_tipo_documento, fecha, numero_serie, id_entidad_comercial, estado, total, neto, subtotal, iva_total, sena, descuento_porcentaje, descuento_importe, observacion)
            OVERRIDING SYSTEM VALUE
            SELECT id, id_tipo_documento, fecha, numero_serie, id_entidad_comercial, estado, total, neto, subtotal, iva_total, sena, descuento_porcentaje, descuento_importe, observacion
            FROM tmp_doc
            ON CONFLICT (id) DO NOTHING
        """)
        imported = cur.rowcount

        # NEW: Generate Payments (assumed all legacy are paid CASH)
        if imported > 0:
            logger.info(f"Generating payments for {imported} documents...")
            pay_df = pd.DataFrame()
            pay_df['id_documento'] = final_df['id']
            pay_df['id_forma_pago'] = 1 # Efectivo / Contado
            pay_df['fecha'] = final_df['fecha']
            pay_df['monto'] = final_df['total']
            pay_df['referencia'] = "Importacion Legacy"
            
            pay_cols = ['id_documento', 'id_forma_pago', 'fecha', 'monto', 'referencia']
            cur.execute("""
                CREATE TEMP TABLE tmp_pago (
                    id_documento BIGINT,
                    id_forma_pago BIGINT,
                    fecha TIMESTAMPTZ,
                    monto NUMERIC,
                    referencia VARCHAR(255)
                ) ON COMMIT DROP
            """)
            fast_bulk_insert(cur, pay_df[pay_cols], "tmp_pago", pay_cols)
            cur.execute("""
                INSERT INTO app.pago (id_documento, id_forma_pago, fecha, monto, referencia)
                SELECT id_documento, id_forma_pago, fecha, monto, referencia FROM tmp_pago
            """)
    
    conn.commit()
    logger.info(f"VENTCAB: Imported {imported}")
    return imported

def import_ventdet(conn: psycopg2.extensions.connection, cache: LookupCache) -> int:
    """Import VENTDET.csv"""
    rows = read_csv_safe(CSV_FILES["ventdet"])
    if rows.empty: return 0
    
    # Standardize column names to lowercase
    df = rows.copy()
    df.columns = [c.lower() for c in df.columns]

    df['idv'] = pd.to_numeric(df['idv'], errors='coerce')
    df['idart'] = pd.to_numeric(df['idart'], errors='coerce')
    
    initial_count = len(df)
    df = df.dropna(subset=['idv', 'idart']).copy()
    coord_dropped = initial_count - len(df)
    if coord_dropped > 0:
        logger.warning(f"VENTDET: Dropped {coord_dropped} rows with invalid idv or idart.")

    # REQUIRED: Skip articles that don't exist instead of falling back to a dummy one
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM app.articulo")
        existing_art_ids = set(row[0] for row in cur.fetchall())
        cur.execute("SELECT id FROM app.documento")
        existing_doc_ids = set(row[0] for row in cur.fetchall())
    
    # Identify rows with missing articles
    missing_mask = ~df['idart'].astype(int).isin(existing_art_ids)
    missing_data = df[missing_mask][['idart', 'art']].copy()
    missing_data['idart'] = missing_data['idart'].astype(int)
    
    # Drop duplicates to create unique placeholders
    unique_missing = missing_data.drop_duplicates(subset=['idart'])
    
    if not unique_missing.empty:
        logger.warning(f"VENTDET: Creating {len(unique_missing)} placeholder articles using names from CSV to avoid data loss.")
        # Get generic brand and rubro
        id_generica = cache.get_or_create("marca", "nombre", "Genérica")
        id_generico = cache.get_or_create("rubro", "nombre", "Genérico")
        id_iva_21 = cache.get("tipo_iva", "21.00") or 1
        
        placeholders = pd.DataFrame({
            'id': unique_missing['idart'],
            'nombre': unique_missing['art'].fillna("Artículo Histórico").str.slice(0, 200),
            'id_marca': id_generica,
            'id_rubro': id_generico,
            'id_tipo_iva': id_iva_21,
            'costo': 0,
            'stock_minimo': 0,
            'activo': True,
            'fecha_creacion': datetime.now()
        })
        
        with conn.cursor() as cur:
            columns = ['id', 'nombre', 'id_marca', 'id_rubro', 'id_tipo_iva', 'costo', 'stock_minimo', 'activo', 'fecha_creacion']
            fast_bulk_insert(cur, placeholders, "app.articulo", columns)
            conn.commit()
        
        # Update existing ids set
        existing_art_ids.update(unique_missing['idart'])

    art_filtered_df = df[df['idart'].isin(existing_art_ids)].copy()
    art_dropped = len(df) - len(art_filtered_df)
    if art_dropped > 0:
        logger.warning(f"VENTDET: Dropped {art_dropped} rows because article ID is invalid (NaN).")
    
    doc_filtered_df = art_filtered_df[art_filtered_df['idv'].isin(existing_doc_ids)].copy()
    doc_dropped = len(art_filtered_df) - len(doc_filtered_df)
    if doc_dropped > 0:
        logger.warning(f"VENTDET: Dropped {doc_dropped} rows because document ID does not exist in app.documento.")
        
    df = doc_filtered_df
    df['id_articulo_final'] = df['idart'].astype(int)
    
    final_df = pd.DataFrame()
    final_df['id_documento'] = df['idv'].astype(int)
    final_df['nro_linea'] = df.groupby('idv').cumcount() + 1
    final_df['id_articulo'] = df['id_articulo_final']
    final_df['cantidad'] = pd.to_numeric(df['cant'], errors='coerce').fillna(1).abs()
    final_df['precio_unitario'] = pd.to_numeric(df['pvta'], errors='coerce').fillna(0)
    
    if 'pvxc' in df.columns:
        final_df['total_linea'] = pd.to_numeric(df['pvxc'], errors='coerce').fillna(0)
    else:
        final_df['total_linea'] = final_df['cantidad'] * final_df['precio_unitario']
    
    # Use 'Art' column for historical description if available
    if 'art' in df.columns:
        final_df['descripcion_historica'] = df['art'].fillna("Importado").str.slice(0, 255)
    else:
        final_df['descripcion_historica'] = "Importado"

    # 2026-01-06: Add Lista (Price List) and NtaPie (Observation) mapping
    if 'lista' in df.columns:
        final_df['id_lista_precio'] = pd.to_numeric(df['lista'], errors='coerce').astype('Int64')
    else:
        final_df['id_lista_precio'] = None
        
    if 'ntapie' in df.columns:
        final_df['observacion'] = df['ntapie'].fillna("").astype(str).str.slice(0, 500)
    else:
        final_df['observacion'] = None
    
    cols = ['id_documento', 'nro_linea', 'id_articulo', 'cantidad', 'precio_unitario', 'total_linea', 'descripcion_historica', 'id_lista_precio', 'observacion']
    
    imported = 0
    with conn.cursor() as cur:
        # Verify Price Lists exist before bulk insert to avoid FK violations
        if 'id_lista_precio' in final_df.columns:
            valid_lists = final_df['id_lista_precio'].dropna().unique()
            if len(valid_lists) > 0:
                cur.execute("SELECT id FROM ref.lista_precio")
                existing_lps = set(row[0] for row in cur.fetchall())
                # Replace invalid LPs with NULL
                final_df.loc[~final_df['id_lista_precio'].isin(existing_lps), 'id_lista_precio'] = None

        cur.execute("CREATE TEMP TABLE tmp_det (LIKE app.documento_detalle INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[cols], "tmp_det", cols)
        cur.execute("""
            INSERT INTO app.documento_detalle (id_documento, nro_linea, id_articulo, cantidad, precio_unitario, total_linea, descripcion_historica, id_lista_precio, observacion)
            SELECT id_documento, nro_linea, id_articulo, cantidad, precio_unitario, total_linea, descripcion_historica, id_lista_precio, observacion
            FROM tmp_det
            ON CONFLICT (id_documento, nro_linea) DO NOTHING
        """)
        imported = cur.rowcount

        # NOTE: We DO NOT generate movements for historical sales import. 
        # The stock snapshot from ARTICULOS.csv is treated as current and immutable.
        pass

    conn.commit()
    logger.info(f"VENTDET: Imported {imported}")
    return imported


def update_sequences(conn: psycopg2.extensions.connection) -> None:
    """Update sequences."""
    logger.info("Updating identity sequences...")
    with conn.cursor() as cur:
        tables = [
            'app.entidad_comercial', 'app.articulo', 'app.documento', 
            'app.movimiento_articulo', 'app.pago',
            'ref.localidad', 'ref.rubro', 'ref.marca', 'ref.unidad_medida'
        ]
        for t in tables:
            try:
                cur.execute(sql.SQL("SELECT setval(pg_get_serial_sequence('{}', 'id'), COALESCE(max(id), 0) + 1, false) FROM {}").format(sql.Identifier(t.split('.')[0], t.split('.')[1]), sql.Identifier(t.split('.')[0], t.split('.')[1])))
            except Exception: pass
    conn.commit()

def show_summary(conn: psycopg2.extensions.connection) -> None:
    """Show database statistics."""
    logger.info("="*60)
    logger.info("DATABASE SUMMARY")
    with conn.cursor() as cur:
        for t in ['app.entidad_comercial', 'app.articulo', 'app.documento', 'app.documento_detalle', 'app.movimiento_articulo', 'app.pago']:
            try:
                cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.SQL(t)))
                logger.info(f"{t}: {cur.fetchone()[0]}")
            except: pass
    logger.info("="*60)


def main():
    args = parse_args()
    
    logger.info("="*60)
    logger.info("NEXORYN TECH - Database Initialization (Optimized)")
    logger.info(f"Started at: {datetime.now().isoformat()}")
    logger.info(f"Database: {args.db_name}@{args.host}:{args.port}")
    logger.info("="*60)
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
        return
    
    try:
        ensure_database_exists(args)
        conn = get_connection(args)
        
        if args.reset:
            reset_database(conn)
            
        execute_schema(conn)
        

        if not args.skip_csv:
            # Define tables to disable triggers during import
            # This massively speeds up bulk inserts by avoiding per-row checks/logs
            tables_to_suspend = [
                'app.entidad_comercial', 
                'app.articulo', 
                'app.articulo_precio',
                'app.documento', 
                'app.documento_detalle', 
                'app.movimiento_articulo', 
                'app.pago',
                'app.remito'
            ]
            
            with SuppressTriggers(conn, tables_to_suspend):
                cache = LookupCache(conn)
                import_cliprov(conn, cache)
                import_articulos(conn, cache)
                import_ventcab(conn, cache)
                import_ventdet(conn, cache)
                update_sequences(conn)

            
            # Recalculate Stock Summary Table (Crucial for performance)
            logger.info("Initializing stock summary table from movements...")
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO app.articulo_stock_resumen (id_articulo, stock_total)
                    SELECT id_articulo, stock_actual 
                    FROM app.v_stock_actual
                    ON CONFLICT (id_articulo) DO UPDATE 
                    SET stock_total = EXCLUDED.stock_total, 
                        ultima_actualizacion = now()
                """)
            conn.commit()
            
            show_summary(conn)

        if not args.skip_csv:
            logger.info("Running ANALYZE to optimize query planner...")
            with conn.cursor() as cur:
                cur.execute("ANALYZE")
            conn.commit()
        
        conn.close()
        
        # VACUUM ANALYZE needs to run outside a transaction block
        logger.info("Running VACUUM ANALYZE for full optimization...")
        try:
            v_conn = get_connection(args)
            v_conn.autocommit = True
            with v_conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE")
            v_conn.close()
        except Exception as ve:
            logger.warning(f"Could not run VACUUM ANALYZE: {ve}")

        logger.info("Done.")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
