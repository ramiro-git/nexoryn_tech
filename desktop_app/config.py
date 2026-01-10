import os
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import quote_plus

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    db_pool_min: int = 1
    db_pool_max: int = 4
    afip_cuit: str = None
    afip_cert: str = None
    afip_key: str = None
    afip_prod: bool = False
    pg_bin_path: str = None


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
        raise EnvironmentError(
            "Cuando no se define DATABASE_URL se necesitan las variables "
            "DB_HOST, DB_PORT (opcional), DB_NAME, DB_USER y DB_PASSWORD."
        )

    user_part = quote_plus(user)
    password_part = quote_plus(password)
    return f"postgresql://{user_part}:{password_part}@{host}:{port}/{name}"


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


def _select_afip_credentials(use_production: bool) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if use_production:
        return (
            os.getenv("AFIP_CUIT_PRODUCCION") or os.getenv("AFIP_CUIT_PRODUCTION"),
            os.getenv("AFIP_CERT_PATH_PRODUCCION") or os.getenv("AFIP_CERT_PATH_PRODUCTION"),
            os.getenv("AFIP_KEY_PATH_PRODUCCION") or os.getenv("AFIP_KEY_PATH_PRODUCTION"),
        )
    return (
        os.getenv("AFIP_CUIT_HOMOLOGACION") or os.getenv("AFIP_CUIT"),
        os.getenv("AFIP_CERT_PATH_HOMOLOGACION") or os.getenv("AFIP_CERT_PATH"),
        os.getenv("AFIP_KEY_PATH_HOMOLOGACION") or os.getenv("AFIP_KEY_PATH"),
    )


def load_config() -> AppConfig:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = _build_url_from_components()
    db_pool_min = _read_int_env("DB_POOL_MIN", 1, min_value=1)
    db_pool_max = _read_int_env("DB_POOL_MAX", 4, min_value=1)
    if db_pool_max < db_pool_min:
        db_pool_max = db_pool_min

    afip_prod = _read_afip_production_flag()
    afip_cuit, afip_cert, afip_key = _select_afip_credentials(afip_prod)
    return AppConfig(
        database_url=database_url,
        db_pool_min=db_pool_min,
        db_pool_max=db_pool_max,
        afip_cuit=afip_cuit,
        afip_cert=afip_cert,
        afip_key=afip_key,
        afip_prod=afip_prod,
        pg_bin_path=os.getenv("PG_BIN_PATH")
    )
