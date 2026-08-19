from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from .database_url import parse_postgres_database_url


BASELINE_REVISION = "0001_legacy_baseline"
PRODUCTION_LEGACY_REVISION = "0001_production_legacy"
STRICT_LEGACY_REVISION = "0000_strict_legacy"
SCRIPT_LOCATION = Path(__file__).resolve().parents[1] / "alembic"


def make_script_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(SCRIPT_LOCATION))
    return config


def code_head_revisions() -> tuple[str, ...]:
    return tuple(sorted(ScriptDirectory.from_config(make_script_config()).get_heads()))


def make_alembic_config(database_url: str) -> Config:
    parse_postgres_database_url(database_url)
    config = make_script_config()
    # ConfigParser treats percent signs as interpolation markers.
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config
