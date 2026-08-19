from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urlsplit


class IsolatedStageTargetError(ValueError):
    """An isolated stage command was pointed outside its fixed database."""


@dataclass(frozen=True)
class IsolatedStageTargetPolicy:
    stage_label: str
    database_name: str
    database_username: str
    service_host: str
    loopback_ports: frozenset[int]
    redis_service_host: str
    redis_loopback_ports: frozenset[int]
    queue_prefix: str
    deployment_validation_entrypoint: str
    deployment_smoke_entrypoint: str

    def owns_database_namespace(self, database: str) -> bool:
        return database.startswith(self.database_name)

    def validation_entrypoint_error(self) -> str:
        return (
            f"{self.stage_label} 证据必须使用 "
            f"{self.deployment_validation_entrypoint} 专属入口"
        )

    def smoke_entrypoint_error(self) -> str:
        return (
            f"{self.stage_label} Reader smoke 必须使用 "
            f"{self.deployment_smoke_entrypoint} 专属入口"
        )

    def validate(
        self,
        *,
        host: str,
        port: int,
        database: str,
        username: str,
        require_rehearsal: bool,
    ) -> None:
        rehearsal_prefix = f"{self.database_name}_rehearsal_"
        is_rehearsal = bool(
            len(database) <= 63
            and re.fullmatch(
                re.escape(rehearsal_prefix) + r"[a-z0-9][a-z0-9_]*",
                database,
            )
        )
        if not is_rehearsal and (
            require_rehearsal or database != self.database_name
        ):
            allowed = f"{rehearsal_prefix}*"
            if not require_rehearsal:
                allowed = f"{self.database_name} 或 {allowed}"
            raise IsolatedStageTargetError(
                f"{self.stage_label} 只允许 {allowed} 数据库"
            )
        if username != self.database_username:
            raise IsolatedStageTargetError(
                f"{self.stage_label} 数据库用户必须是 {self.database_username}"
            )
        service_endpoint = host == self.service_host and port == 5432
        loopback_endpoint = host in {"127.0.0.1", "::1", "localhost"} and (
            port in self.loopback_ports
        )
        if not service_endpoint and not loopback_endpoint:
            loopback_text = "/".join(str(value) for value in sorted(self.loopback_ports))
            raise IsolatedStageTargetError(
                f"{self.stage_label} 数据库 endpoint 必须是 "
                f"{self.service_host}:5432 或 loopback:{loopback_text}"
            )

    def validate_runtime_target(
        self,
        *,
        redis_url: str,
        queue_prefix: str,
    ) -> None:
        redis_error = (
            f"{self.stage_label} Redis endpoint 必须是 "
            f"{self.redis_service_host}:6379 或专属 loopback 端口，且使用 DB 0"
        )
        try:
            parsed = urlsplit(redis_url)
            host = (parsed.hostname or "").lower().rstrip(".")
            port = parsed.port or 6379
        except ValueError as exc:
            raise IsolatedStageTargetError(redis_error) from exc

        service_endpoint = host == self.redis_service_host and port == 6379
        loopback_endpoint = host in {"127.0.0.1", "::1", "localhost"} and (
            port in self.redis_loopback_ports
        )
        if (
            parsed.scheme != "redis"
            or parsed.path != "/0"
            or parsed.query
            or parsed.fragment
            or not (service_endpoint or loopback_endpoint)
        ):
            raise IsolatedStageTargetError(redis_error)
        if queue_prefix != self.queue_prefix:
            raise IsolatedStageTargetError(
                f"{self.stage_label} RQ queue prefix 必须是 {self.queue_prefix}"
            )


P03_DATABASE_TARGET_POLICY = IsolatedStageTargetPolicy(
    stage_label="P0.3",
    database_name="reader_p03",
    database_username="reader_p03",
    service_host="postgres-p03",
    loopback_ports=frozenset({43134, 55443}),
    redis_service_host="redis-p03",
    redis_loopback_ports=frozenset({6390, 43135}),
    queue_prefix="reader-p03",
    deployment_validation_entrypoint="reader_api.p03_deployment_validation",
    deployment_smoke_entrypoint="reader_api.p03_deployment_smoke",
)

P04_DATABASE_TARGET_POLICY = IsolatedStageTargetPolicy(
    stage_label="P0.4",
    database_name="reader_p04",
    database_username="reader_p04",
    service_host="postgres-p04",
    loopback_ports=frozenset({43138, 55445}),
    redis_service_host="redis-p04",
    redis_loopback_ports=frozenset({6391, 43139}),
    queue_prefix="reader-p04",
    deployment_validation_entrypoint="reader_api.p04_deployment_validation",
    deployment_smoke_entrypoint="reader_api.p04_deployment_smoke",
)

DEDICATED_STAGE_TARGET_POLICIES = (
    P03_DATABASE_TARGET_POLICY,
    P04_DATABASE_TARGET_POLICY,
)


def dedicated_stage_policy_for_database(
    database: str,
) -> IsolatedStageTargetPolicy | None:
    return next(
        (
            policy
            for policy in DEDICATED_STAGE_TARGET_POLICIES
            if policy.owns_database_namespace(database)
        ),
        None,
    )
