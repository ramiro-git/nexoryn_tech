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

def import_cliprov(conn: psycopg2.extensions.connection, cache: LookupCache) -> int:
    """Import CLIPROV.csv using Pandas + COPY Protocol."""
    rows = read_csv_safe(CSV_FILES["cliprov"])
    if rows.empty: return 0
    
    # 1. Clean Data using Vectorized Operations
    rows['Id'] = pd.to_numeric(rows['Id'], errors='coerce')
    df = rows.dropna(subset=['Id']).copy()
    df = df[(df['Id'] <= MAX_BIGINT) & (df['Id'] >= MIN_BIGINT)].copy()
    
    if df.empty: return 0
        
    string_cols = ['Apell', 'Nomb', 'Dom', 'Otros', 'Loc', 'Provincia', 'Ref', 'Telefono', 'Iva']
    for col in string_cols:
         if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None})
            
    df['tipo'] = np.where(df['Ref'].astype(str).str.upper() == 'P', 'PROVEEDOR', 'CLIENTE')

    # 2. Bulk Create References
    ivas = df['Iva'].dropna().unique()
    provincias = df['Provincia'].dropna().unique()
    
    iva_map_code = { "RI": "Responsable Inscripto", "M": "Monotributista", "CF": "Consumidor Final", "EX": "Exento" }
    
    cache.preload("condicion_iva", "nombre")
    real_iva_names = set()
    for c in ivas:
        val = str(c).strip()
        if not val.isdigit() and val:
            real_iva_names.add(iva_map_code.get(val.upper(), val))

    cache.bulk_create("condicion_iva", "nombre", real_iva_names)

    cache.preload("provincia", "nombre")
    cache.bulk_create("provincia", "nombre", set(provincias))
    prov_cache = cache._get_cache("ref", "provincia")
    df['Prov_Filled'] = df['Provincia'].fillna("Buenos Aires")
    unique_locs = df[['Loc', 'Prov_Filled']].dropna(subset=['Loc']).drop_duplicates()
    
    with conn.cursor() as cur:
        cur.execute("SELECT id, lower(nombre), id_provincia FROM ref.localidad")
        localidad_cache = {(row[1], row[2]): row[0] for row in cur.fetchall()}
            
        new_localities = []
        for row in unique_locs.itertuples(index=False):
            loc_name = row.Loc
            prov_name = row.Prov_Filled
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
    
    df['ID_IVA'] = df['Iva'].map(get_iva_id).astype('Int64')
    
    # Vectorized Locality Mapping (Merge instead of apply)
    df_loc_keys = df[['Loc', 'Prov_Filled']].copy()
    df_loc_keys['loc_lower'] = df_loc_keys['Loc'].str.lower()
    df_loc_keys['prov_lower'] = df_loc_keys['Prov_Filled'].str.lower()
    df_loc_keys['pid'] = df_loc_keys['prov_lower'].map(prov_cache.get)
    
    # Create lookup series from locality_cache dict: (name_lower, pid) -> id
    loc_lookup = pd.Series(localidad_cache)
    df['ID_LOC'] = df_loc_keys.apply(lambda r: localidad_cache.get((r['loc_lower'], r['pid'])), axis=1).astype('Int64')

    if 'Cuit' in df.columns:
        df['Cuit_Clean'] = df['Cuit'].astype(str).str.replace(r'[ -]', '', regex=True).str.slice(0, 13).replace({'None': None, 'nan': None, '': None})
    else:
        df['Cuit_Clean'] = None

    df['FchAlta'] = pd.to_datetime(df['FchAlta'], errors='coerce').fillna(datetime.now())
    
    final_df = pd.DataFrame()
    final_df['id'] = df['Id'].astype(int)
    final_df['apellido'] = df['Apell'].str.slice(0, 100)
    final_df['nombre'] = df['Nomb'].str.slice(0, 100)
    final_df['domicilio'] = df['Dom'].str.slice(0, 100)
    final_df['id_localidad'] = df['ID_LOC']
    final_df['cuit'] = df['Cuit_Clean']
    final_df['id_condicion_iva'] = df['ID_IVA']
    final_df['notas'] = df['Otros'].str.slice(0, 255)
    final_df['fecha_creacion'] = df['FchAlta']
    # Keep original strings for phone to avoid data loss, but respect DB limit (30)
    final_df['telefono'] = df['Telefono'].astype(str).replace({'nan': None, 'None': None, '': None}).str.slice(0, 30)
    
    # Extract email from 'Otros' if it looks like one
    def extract_email(text):
        if not text or not isinstance(text, str): return None
        import re
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        return match.group(0) if match else None
    
    final_df['email'] = df['Otros'].apply(extract_email).str.slice(0, 150)
    final_df['tipo'] = df['tipo']

    # COPY 
    columns = ['id', 'apellido', 'nombre', 'domicilio', 'id_localidad', 'cuit', 'id_condicion_iva', 'notas', 'fecha_creacion', 'telefono', 'email', 'tipo']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_entidad_comercial (LIKE app.entidad_comercial INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[columns], "tmp_entidad_comercial", columns)
        cur.execute("""
            INSERT INTO app.entidad_comercial (id, apellido, nombre, domicilio, id_localidad, cuit, id_condicion_iva, notas, fecha_creacion, telefono, email, tipo)
            OVERRIDING SYSTEM VALUE
            SELECT id, apellido, nombre, domicilio, id_localidad, cuit, id_condicion_iva, notas, fecha_creacion, telefono, email, tipo
            FROM tmp_entidad_comercial
            ON CONFLICT (id) DO UPDATE SET
                apellido = EXCLUDED.apellido, 
                nombre = EXCLUDED.nombre,
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

    conn.commit()
    logger.info(f"CLIPROV: Imported {imported}")
    return imported


def import_articulos(conn: psycopg2.extensions.connection, cache: LookupCache) -> int:
    """Import ARTICULOS.csv using Pandas + COPY Protocol."""
    rows = read_csv_safe(CSV_FILES["articulos"], encoding="utf-8-sig")
    if rows.empty: return 0
    
    rows['IdArt'] = pd.to_numeric(rows['IdArt'], errors='coerce')
    df = rows.dropna(subset=['IdArt']).copy()
    df = df[(df['IdArt'] <= MAX_BIGINT) & (df['IdArt'] >= MIN_BIGINT)].copy()
    if df.empty: return 0
    
    for col in ['Art', 'IdMar', 'IdRub', 'Unidad']:
        if col in df.columns:
             df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None})
             
    marcas = df['IdMar'].dropna().unique()
    cache.preload("marca", "nombre")
    real_marcas = set(m for m in marcas if not str(m).isdigit())
    cache.bulk_create("marca", "nombre", real_marcas)
    
    rubros = df['IdRub'].dropna().unique()
    cache.preload("rubro", "nombre")
    real_rubros = set(r for r in rubros if not str(r).isdigit())
    cache.bulk_create("rubro", "nombre", real_rubros)
    
    unidades = df['Unidad'].dropna().unique()
    cache.preload("unidad_medida", "nombre")
    cache.bulk_create("unidad_medida", "nombre", set(unidades))
    
    # 3. Vectorized Mapping
    
    # Preload IVA types (standard ones seeded in database.sql)
    cache.preload("tipo_iva", "porcentaje")
    
    marca_map = cache._get_cache("ref", "marca")
    rubro_map = cache._get_cache("ref", "rubro")
    unidad_map = cache._get_cache("ref", "unidad_medida")
    
    df['id_marca'] = df['IdMar'].str.lower().map(marca_map.get).astype('Int64')
    df['id_rubro'] = df['IdRub'].str.lower().map(rubro_map.get).astype('Int64')
    df['id_unidad'] = df['Unidad'].str.lower().map(unidad_map.get).astype('Int64')

    # Defaults for Rubro/Marca (first one found)
    default_marca = next(iter(marca_map.values()), None) if marca_map else None
    default_rubro = next(iter(rubro_map.values()), None) if rubro_map else None
    
    df['id_marca'] = df['id_marca'].fillna(default_marca)
    df['id_rubro'] = df['id_rubro'].fillna(default_rubro)
    
    # Use cached ID for 21% (should be in seed data)
    id_iva_21 = cache.get("tipo_iva", "21.00") or 1 
    
    final_df = pd.DataFrame()
    final_df['id'] = df['IdArt'].astype(int)
    final_df['nombre'] = df['Art'].fillna("Articulo Sin Nombre").str.slice(0, 200)
    final_df['id_marca'] = df['id_marca']
    final_df['id_rubro'] = df['id_rubro']
    final_df['id_tipo_iva'] = id_iva_21 # Assuming not null here or fixed val
    final_df['costo'] = pd.to_numeric(df['Costo'], errors='coerce').fillna(0)
    final_df['stock_minimo'] = pd.to_numeric(df['StkMin'], errors='coerce').fillna(0)
    final_df['id_unidad_medida'] = df['id_unidad']
    final_df['id_proveedor'] = pd.to_numeric(df['IdProv'], errors='coerce').fillna(0).astype('Int64').replace(0, None)
    final_df['descuento_base'] = pd.to_numeric(df['Desc'], errors='coerce').fillna(0)
    final_df['redondeo'] = df['Redo'].apply(lambda x: str(x).lower() in ['1', 'true', 's'] if pd.notnull(x) else False)
    final_df['activo'] = True
    final_df['fecha_creacion'] = datetime.now()
    
    columns = ['id', 'nombre', 'id_marca', 'id_rubro', 'id_tipo_iva', 'costo', 'stock_minimo', 'id_unidad_medida', 'id_proveedor', 'descuento_base', 'redondeo', 'activo', 'fecha_creacion']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_articulo (LIKE app.articulo INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[columns], "tmp_articulo", columns)
        cur.execute("""
            INSERT INTO app.articulo (id, nombre, id_marca, id_rubro, id_tipo_iva, costo, stock_minimo, id_unidad_medida, id_proveedor, descuento_base, redondeo, activo, fecha_creacion)
            OVERRIDING SYSTEM VALUE
            SELECT id, nombre, id_marca, id_rubro, id_tipo_iva, costo, stock_minimo, id_unidad_medida, id_proveedor, descuento_base, redondeo, activo, fecha_creacion
            FROM tmp_articulo
            ON CONFLICT (id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                costo = EXCLUDED.costo,
                stock_minimo = EXCLUDED.stock_minimo
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
                l1_df['id_articulo'] = df['IdArt'].astype(int)
                l1_df['id_lista_precio'] = l1_id
                l1_df['precio'] = pd.to_numeric(df['PVenta'], errors='coerce').fillna(0)
                l1_df['porcentaje'] = pd.to_numeric(df['Pgan'], errors='coerce').fillna(0)
                l1_df['id_tipo_porcentaje'] = id_margen
                price_rows.append(l1_df)

            # Lists 2-7
            for i in range(2, 8):
                lname = f"lista {i}"
                lid = list_cache.get(lname)
                col_l = f"L{i}"
                col_pl = f"PL{i}"
                
                if lid and col_l in df.columns:
                    li_df = pd.DataFrame()
                    li_df['id_articulo'] = df['IdArt'].astype(int)
                    li_df['id_lista_precio'] = lid
                    li_df['precio'] = pd.to_numeric(df[col_l], errors='coerce').fillna(0)
                    li_df['porcentaje'] = pd.to_numeric(df[col_pl], errors='coerce').fillna(0)
                    li_df['id_tipo_porcentaje'] = id_descuento
                    price_rows.append(li_df)
            
            if price_rows:
                all_prices = pd.concat(price_rows, ignore_index=True)
                all_prices = all_prices[all_prices['precio'] > 0]
                
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
        if imported > 0:
            logger.info(f"Generating initial stock movements for {imported} articles...")
            # Use the original df which has the STK column
            stock_df = df[df['STK'].notnull()].copy()
            stock_df['stk_val'] = pd.to_numeric(stock_df['STK'], errors='coerce').fillna(0)
            stock_df = stock_df[stock_df['stk_val'] != 0].copy()
            
            if not stock_df.empty:
                mov_df = pd.DataFrame()
                mov_df['id_articulo'] = stock_df['IdArt'].astype(int)
                mov_df['cantidad'] = stock_df['stk_val'].abs()  # Always absolute value
                mov_df['id_tipo_movimiento'] = 5 # Default Ajuste Positivo
                mov_df['id_deposito'] = 1 # Deposito Central
                mov_df['observacion'] = "Stock Inicial (Legacy)"
                
                # Handle negative stock values from legacy if any
                mask_neg = stock_df['stk_val'] < 0
                mov_df.loc[mask_neg, 'id_tipo_movimiento'] = 6 # Ajuste Negativo

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
    
    rows['IdV'] = pd.to_numeric(rows['IdV'], errors='coerce')
    df = rows.dropna(subset=['IdV']).copy()
    
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

    df['id_td'] = df['Tipo'].map(get_doc_id).fillna(doc_type_cache.get('presupuesto')).astype('Int64')
    
    # Ensure all referenced entities exist (map missing to MOSTRADOR)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM app.entidad_comercial")
        existing_ids = set(row[0] for row in cur.fetchall())
    
    mostrador_id = 4
    df['id_entidad_clean'] = pd.to_numeric(df['IdCli'], errors='coerce').fillna(mostrador_id).astype(int)
    # If the ID provided doesn't exist in master, use mostrador
    df['id_entidad_comercial_final'] = df['id_entidad_clean'].apply(lambda x: x if x in existing_ids else mostrador_id)
        
    final_df = pd.DataFrame()
    final_df['id'] = df['IdV'].astype(int)
    final_df['id_tipo_documento'] = df['id_td']
    final_df['fecha'] = pd.to_datetime(df['Fch'], errors='coerce').fillna(datetime.now())
    final_df['numero_serie'] = df['NFact'].astype(str).str.slice(0, 20).replace({'nan': None}, regex=True)
    final_df['id_entidad_comercial'] = df['id_entidad_comercial_final']

    final_df['estado'] = 'CONFIRMADO'
    final_df['total'] = pd.to_numeric(df['TVenta'], errors='coerce').fillna(0)
    
    # Use accurate CSV columns if available
    if 'Neto' in df.columns:
        final_df['neto'] = pd.to_numeric(df['Neto'], errors='coerce').fillna(0)
    else:
        final_df['neto'] = final_df['total'] / 1.21
        
    if 'SubT' in df.columns:
        final_df['subtotal'] = pd.to_numeric(df['SubT'], errors='coerce').fillna(final_df['neto'])
    else:
        final_df['subtotal'] = final_df['neto']

    if 'TIVA' in df.columns:
        final_df['iva_total'] = pd.to_numeric(df['TIVA'], errors='coerce').fillna(0)
    else:
        final_df['iva_total'] = final_df['total'] - final_df['neto']
    
    columns = ['id', 'id_tipo_documento', 'fecha', 'numero_serie', 'id_entidad_comercial', 'estado', 'total', 'neto', 'subtotal', 'iva_total']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_doc (LIKE app.documento INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[columns], "tmp_doc", columns)
        cur.execute("""
            INSERT INTO app.documento (id, id_tipo_documento, fecha, numero_serie, id_entidad_comercial, estado, total, neto, subtotal, iva_total)
            OVERRIDING SYSTEM VALUE
            SELECT id, id_tipo_documento, fecha, numero_serie, id_entidad_comercial, estado, total, neto, subtotal, iva_total
            FROM tmp_doc
            ON CONFLICT (id) DO NOTHING
        """)
        imported = cur.rowcount

        # NEW: Generate Payments (assumed all legacy are paid CASH)
        if imported > 0:
            logger.info(f"Generating payments for {imported} documents...")
            pay_df = pd.DataFrame()
            pay_df['id_documento'] = final_df['id']
            pay_df['id_forma_pago'] = 1 # Efectivo
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
    
    rows['IdV'] = pd.to_numeric(rows['IdV'], errors='coerce')
    rows['IdArt'] = pd.to_numeric(rows['IdArt'], errors='coerce')
    df = rows.dropna(subset=['IdV', 'IdArt']).copy()

    # Ensure all referenced articles exist (map missing to IdArt 9)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM app.articulo")
        existing_art_ids = set(row[0] for row in cur.fetchall())
    
    default_art_id = 9
    df['id_articulo_clean'] = pd.to_numeric(df['IdArt'], errors='coerce').fillna(default_art_id).astype(int)
    # Fast membership check using isin
    df['id_articulo_final'] = np.where(df['id_articulo_clean'].isin(existing_art_ids), df['id_articulo_clean'], default_art_id)
    
    final_df = pd.DataFrame()
    final_df['id_documento'] = df['IdV'].astype(int)
    final_df['nro_linea'] = df.groupby('IdV').cumcount() + 1
    final_df['id_articulo'] = df['id_articulo_final']
    final_df['cantidad'] = pd.to_numeric(df['Cant'], errors='coerce').fillna(1).abs()
    final_df['precio_unitario'] = pd.to_numeric(df['PVta'], errors='coerce').fillna(0)
    
    if 'PVxC' in df.columns:
        final_df['total_linea'] = pd.to_numeric(df['PVxC'], errors='coerce').fillna(0)
    else:
        final_df['total_linea'] = final_df['cantidad'] * final_df['precio_unitario']
    final_df['descripcion_historica'] = "Importado"
    
    cols = ['id_documento', 'nro_linea', 'id_articulo', 'cantidad', 'precio_unitario', 'total_linea', 'descripcion_historica']
    
    imported = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_det (LIKE app.documento_detalle INCLUDING DEFAULTS) ON COMMIT DROP")
        fast_bulk_insert(cur, final_df[cols], "tmp_det", cols)
        cur.execute("""
            INSERT INTO app.documento_detalle (id_documento, nro_linea, id_articulo, cantidad, precio_unitario, total_linea, descripcion_historica)
            SELECT id_documento, nro_linea, id_articulo, cantidad, precio_unitario, total_linea, descripcion_historica
            FROM tmp_det
            ON CONFLICT (id_documento, nro_linea) DO NOTHING
        """)
        imported = cur.rowcount

        # NEW: Generate Stock Movements
        if imported > 0:
            logger.info(f"Generating movements for {len(final_df)} lines...")
            # We need the date from the header... or just use fixed/now. 
            # Better to use actual session if we want it perfect, but let's keep it simple.
            mov_df = pd.DataFrame()
            mov_df['id_articulo'] = final_df['id_articulo']
            mov_df['id_tipo_movimiento'] = 2 # Venta (-1)
            mov_df['cantidad'] = final_df['cantidad'].abs()  # Always absolute value
            mov_df['id_deposito'] = 1 # Deposito Central
            mov_df['id_documento'] = final_df['id_documento']
            mov_df['observacion'] = "Importacion Legacy"
            
            mov_cols = ['id_articulo', 'id_tipo_movimiento', 'cantidad', 'id_deposito', 'id_documento', 'observacion']
            cur.execute("""
                CREATE TEMP TABLE tmp_mov (
                    id_articulo BIGINT,
                    id_tipo_movimiento BIGINT,
                    cantidad NUMERIC,
                    id_deposito BIGINT,
                    id_documento BIGINT,
                    observacion TEXT
                ) ON COMMIT DROP
            """)
            fast_bulk_insert(cur, mov_df[mov_cols], "tmp_mov", mov_cols)
            cur.execute("""
                INSERT INTO app.movimiento_articulo (id_articulo, id_tipo_movimiento, cantidad, id_deposito, id_documento, observacion)
                SELECT id_articulo, id_tipo_movimiento, cantidad, id_deposito, id_documento, observacion FROM tmp_mov
            """)

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
                    SELECT id_articulo, stock_total 
                    FROM app.v_stock_total
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
