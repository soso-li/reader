from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from ipaddress import ip_address
import re
import socket
from urllib.parse import unquote, urlsplit

from sqlalchemy.engine import URL

from .migrations.database_url import parse_postgres_database_url


KNOWN_PRODUCTION_DATABASES = frozenset({"reader"})
KNOWN_PRODUCTION_HOSTS = frozenset({"postgres", "reader-postgres"})
MAINTENANCE_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$"
)


@dataclass(frozen=True)
class ProductionTargetIdentity:
    url: URL = field(repr=False)
    host: str
    port: int
    database: str
    username: str
    known_production_host: bool
    known_production_database: bool

    @property
    def looks_like_production(self) -> bool:
        return self.known_production_host or self.known_production_database

    def is_authorized(
        self,
        *,
        command_confirmed: bool,
        authorization_env: str,
        maintenance_id: str,
        environ: Mapping[str, str],
    ) -> bool:
        return bool(
            self.looks_like_production
            and command_confirmed
            and environ.get(authorization_env) == "1"
            and MAINTENANCE_ID_PATTERN.fullmatch(maintenance_id)
        )


def production_target_identity(database_url: str) -> ProductionTargetIdentity:
    url = parse_postgres_database_url(database_url)
    if not url.host:
        raise ValueError("PostgreSQL 数据库地址必须显式包含 host")
    host = _canonical_database_host(url.host)
    database = url.database or ""
    return ProductionTargetIdentity(
        url=url,
        host=host,
        port=int(url.port or 5432),
        database=database,
        username=url.username or "",
        known_production_host=host in KNOWN_PRODUCTION_HOSTS,
        known_production_database=(
            database.lower() in KNOWN_PRODUCTION_DATABASES
        ),
    )


def _canonical_database_host(host: str) -> str:
    try:
        normalized = host.encode("idna").decode("ascii").lower().rstrip(".")
    except UnicodeError as exc:
        raise ValueError("PostgreSQL 数据库 host 无效") from exc
    if not normalized:
        raise ValueError("PostgreSQL 数据库地址必须显式包含 host")
    try:
        address = ip_address(normalized)
    except ValueError:
        try:
            socket.getaddrinfo(
                normalized,
                None,
                family=socket.AF_UNSPEC,
                type=socket.SOCK_STREAM,
                flags=socket.AI_NUMERICHOST,
            )
        except socket.gaierror:
            return normalized
        raise ValueError("PostgreSQL 数据库 host 不允许非标准数值 IP 表示")
    mapped_ipv4 = getattr(address, "ipv4_mapped", None)
    return str(mapped_ipv4 or address)


def credential_secrets(values: Iterable[str]) -> tuple[str, ...]:
    secrets: set[str] = set()
    for value in values:
        if not value:
            continue
        secrets.add(value)
        try:
            password = urlsplit(value).password
        except ValueError:
            password = None
        if password:
            secrets.update({password, unquote(password)})
    return tuple(secrets)


def sanitized_exception_message(
    exc: Exception,
    *,
    credential_values: Iterable[str] = (),
    extra_secrets: Iterable[str] = (),
) -> str:
    message = f"{type(exc).__name__}: {exc}"
    secrets = set(credential_secrets(credential_values))
    secrets.update(secret for secret in extra_secrets if secret)
    for secret in sorted(secrets, key=len, reverse=True):
        message = message.replace(secret, "***")
    return message[:1000]
