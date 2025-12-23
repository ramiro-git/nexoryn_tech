from dataclasses import dataclass
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    database_url: str


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


def load_config() -> AppConfig:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = _build_url_from_components()
    return AppConfig(database_url=database_url)
