import os
from dataclasses import dataclass
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


def load_config() -> AppConfig:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = _build_url_from_components()
    db_pool_min = _read_int_env("DB_POOL_MIN", 1, min_value=1)
    db_pool_max = _read_int_env("DB_POOL_MAX", 4, min_value=1)
    if db_pool_max < db_pool_min:
        db_pool_max = db_pool_min
    
    return AppConfig(
        database_url=database_url,
        db_pool_min=db_pool_min,
        db_pool_max=db_pool_max,
        afip_cuit=os.getenv("AFIP_CUIT"),
        afip_cert=os.getenv("AFIP_CERT_PATH"),
        afip_key=os.getenv("AFIP_KEY_PATH"),
        afip_prod=os.getenv("AFIP_PRODUCTION", "False").lower() == "true",
        pg_bin_path=os.getenv("PG_BIN_PATH")
    )
