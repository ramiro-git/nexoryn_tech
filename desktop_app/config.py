import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote_plus

from dotenv import load_dotenv
from urllib.parse import urlparse


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    db_pool_min: int = 1
    db_pool_max: int = 4
    afip_cuit: str = None
    afip_cert: str = None
    afip_key: str = None
    afip_prod: bool = False
    afip_punto_venta: int = 1
    pg_bin_path: str = None
    config_dir: str = None


def _get_app_data_dir() -> Path:
    """
    Returns the application data directory.
    Windows: %APPDATA%\\Nexoryn_Tech
    Linux/Mac: ~/.Nexoryn_Tech
    """
    app_name = "Nexoryn_Tech"
    if os.name == 'nt':
        base = os.getenv('APPDATA')
        if base:
            return Path(base) / app_name
    # Fallback or other OS
    return Path.home() / f".{app_name}"


def _parse_database_url(database_url: str) -> dict:
    """
    Parses a DATABASE_URL into its components (host, port, name, user, password).
    Supports postgresql:// URLs.
    """
    parsed = urlparse(database_url)
    
    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError(f"Unsupported database URL scheme: {parsed.scheme}")
    
    return {
        "host": parsed.hostname or "localhost",
        "port": str(parsed.port or 5432),
        "name": parsed.path.lstrip("/") if parsed.path else "postgres",
        "user": parsed.username or "postgres",
        "password": parsed.password or "",
    }


def _build_url_from_components() -> str:
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    missing = [
        var for var, val in (
            ("DB_HOST", host),
            ("DB_NAME", name),
            ("DB_USER", user),
            ("DB_PASSWORD", password),
        )
        if not val
    ]
    if missing:
        # We raise a clearer error message guiding the user to the config location
        raise EnvironmentError(
            "No se encontró DATABASE_URL ni las variables componentes (DB_HOST, etc). "
            "Asegúrese de configurar el archivo .env correctamente."
        )

    user_part = quote_plus(user)
    password_part = quote_plus(password)
    return f"postgresql://{user_part}:{password_part}@{host}:{port}/{name}"


def get_db_config() -> dict:
    """
    Returns database configuration (host, port, name, user, password) as a dictionary.
    
    Supports two sources of credentials:
    1. DATABASE_URL environment variable (recommended)
    2. Individual DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD variables (fallback)
    
    Returns a dict with keys: host, port, name, user, password
    """
    database_url = os.getenv("DATABASE_URL")
    
    if database_url:
        # Parse from DATABASE_URL
        try:
            return _parse_database_url(database_url)
        except ValueError as e:
            raise EnvironmentError(f"Invalid DATABASE_URL format: {e}")
    
    # Fallback to individual DB_* variables with defaults
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "name": os.getenv("DB_NAME", "nexoryn_tech"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "") or os.environ.get("PGPASSWORD", ""),
    }


def _read_int_env(name: str, default: int, *, min_value: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= min_value else default


def _read_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _read_afip_production_flag() -> bool:
    if os.getenv("AFIP_PRODUCCION") is not None:
        return _read_bool_env("AFIP_PRODUCCION", False)
    return _read_bool_env("AFIP_PRODUCTION", False)


def _resolve_path(base_dir: Path, path_str: Optional[str]) -> Optional[str]:
    """
    Resolves a path relative to the configuration directory if it is not absolute.
    """
    if not path_str:
        return None
    path = Path(path_str)
    if path.is_absolute():
        return str(path)
    # Resolve relative to config dir
    return str(base_dir / path)


def _select_afip_credentials(use_production: bool, config_dir: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if use_production:
        cuit = os.getenv("AFIP_CUIT_PRODUCCION") or os.getenv("AFIP_CUIT_PRODUCTION")
        cert = os.getenv("AFIP_CERT_PATH_PRODUCCION") or os.getenv("AFIP_CERT_PATH_PRODUCTION")
        key = os.getenv("AFIP_KEY_PATH_PRODUCCION") or os.getenv("AFIP_KEY_PATH_PRODUCTION")
    else:
        cuit = os.getenv("AFIP_CUIT_HOMOLOGACION") or os.getenv("AFIP_CUIT")
        cert = os.getenv("AFIP_CERT_PATH_HOMOLOGACION") or os.getenv("AFIP_CERT_PATH")
        key = os.getenv("AFIP_KEY_PATH_HOMOLOGACION") or os.getenv("AFIP_KEY_PATH")

    return (
        cuit,
        _resolve_path(config_dir, cert),
        _resolve_path(config_dir, key)
    )


def load_config() -> AppConfig:
    """
    Loads configuration prioritizing the User's AppData directory,
    then falling back to the executable's directory.
    """
    # 1. Determine config location priority:
    #    a) User AppData (Standard for installed apps)
    #    b) Current Working Directory / Executable Dir (Portable / Dev)
    
    app_data_dir = _get_app_data_dir()
    env_in_appdata = app_data_dir / ".env"
    
    # Check if we are running in a "frozen" bundle (PyInstaller)
    if getattr(sys, 'frozen', False):
        # Using sys.executable's dir for "portable" check next to exe
        portable_dir = Path(sys.executable).parent
    else:
        portable_dir = Path.cwd()
        
    env_in_portable = portable_dir / ".env"

    config_source = portable_dir # Default fallback

    if env_in_appdata.exists():
        load_dotenv(dotenv_path=env_in_appdata)
        config_source = app_data_dir
    elif env_in_portable.exists():
        load_dotenv(dotenv_path=env_in_portable)
        config_source = portable_dir
    else:
        # Fallback: try loading without path (system env vars)
        load_dotenv()
        # If loaded from sys env, we assume portable logic for relative paths
        pass

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        try:
            database_url = _build_url_from_components()
        except EnvironmentError as e:
            # Re-raise with location hint
            raise EnvironmentError(
                f"{str(e)}\n\nUbicación esperada de configuración (.env):\n"
                f"1. {env_in_appdata} (Recomendado)\n"
                f"2. {env_in_portable} (Portable)"
            )

    db_pool_min = _read_int_env("DB_POOL_MIN", 1, min_value=1)
    db_pool_max = _read_int_env("DB_POOL_MAX", 4, min_value=1)
    if db_pool_max < db_pool_min:
        db_pool_max = db_pool_min

    afip_prod = _read_afip_production_flag()
    afip_cuit, afip_cert, afip_key = _select_afip_credentials(afip_prod, config_source)
    afip_punto_venta = _read_int_env("AFIP_PUNTO_VENTA", 1, min_value=1)
    
    return AppConfig(
        database_url=database_url,
        db_pool_min=db_pool_min,
        db_pool_max=db_pool_max,
        afip_cuit=afip_cuit,
        afip_cert=afip_cert,
        afip_key=afip_key,
        afip_prod=afip_prod,
        afip_punto_venta=afip_punto_venta,
        pg_bin_path=os.getenv("PG_BIN_PATH"),
        config_dir=str(config_source)
    )
