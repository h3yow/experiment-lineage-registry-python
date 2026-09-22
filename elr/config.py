"""Configuration loaded from environment variables.

Environment variables (prefix ``ELR_``):

``ELR_DB_PATH``        Path of the SQLite database file.
                       Default: ``./data/elr.db``.
``ELR_CAPACITY_POLICY`` Behavior when artifact capacity is exceeded:
                       ``reject`` (fail the register/gc request) or
                       ``defer`` (leave unreferenced artifacts pending and
                       report ``deferred``). Default: ``reject``.
``ELR_MAX_ARTIFACT_BYTES`` Hard capacity in bytes for stored artifact bytes.
                       ``0`` disables the limit. Default: ``0``.
``ELR_RETENTION_RUNS``  Number of most recent ended runs whose referenced
                       artifacts are always retained; older unreferenced
                       versions are collectable. ``0`` means no age-based
                       retention. Default: ``0``.
``ELR_ARTIFACT_STORE`` Directory holding content-addressed bytes.
                       Default: ``./data/artifacts``.
``ELR_SERIALIZE_WRITES`` ``1`` serializes all mutating operations process
                       wide (default); ``0`` relies on DB transactions only.

Unknown/unsupported values are rejected at startup (``Settings.from_env``)
with ``invalid_config``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import InvalidConfigError

_VALID_POLICIES = frozenset({"reject", "defer"})


@dataclass(frozen=True)
class Settings:
    db_path: str = "data/elr.db"
    artifact_store: str = "data/artifacts"
    capacity_policy: str = "reject"
    max_artifact_bytes: int = 0
    retention_runs: int = 0
    serialize_writes: bool = True

    @staticmethod
    def _int(name: str, raw: str) -> int:
        try:
            value = int(raw)
        except ValueError:
            raise InvalidConfigError(
                f"{name} must be an integer, got {raw!r}",
                details={"name": name, "value": raw},
            )
        if value < 0:
            raise InvalidConfigError(
                f"{name} must be >= 0, got {value}",
                details={"name": name, "value": value},
            )
        return value

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "Settings":
        env = environ if environ is not None else dict(os.environ)
        policy = env.get("ELR_CAPACITY_POLICY", "reject").strip().lower()
        if policy not in _VALID_POLICIES:
            raise InvalidConfigError(
                f"ELR_CAPACITY_POLICY must be one of "
                f"{sorted(_VALID_POLICIES)}, got {policy!r}",
                details={"name": "ELR_CAPACITY_POLICY", "value": policy},
            )
        serialize_raw = env.get("ELR_SERIALIZE_WRITES", "1").strip()
        if serialize_raw not in {"0", "1", "true", "false", "True", "False"}:
            raise InvalidConfigError(
                "ELR_SERIALIZE_WRITES must be one of 0/1/true/false, got "
                f"{serialize_raw!r}",
                details={"name": "ELR_SERIALIZE_WRITES", "value": serialize_raw},
            )
        return cls(
            db_path=env.get("ELR_DB_PATH", "data/elr.db"),
            artifact_store=env.get("ELR_ARTIFACT_STORE", "data/artifacts"),
            capacity_policy=policy,
            max_artifact_bytes=cls._int(
                "ELR_MAX_ARTIFACT_BYTES", env.get("ELR_MAX_ARTIFACT_BYTES", "0")
            ),
            retention_runs=cls._int(
                "ELR_RETENTION_RUNS", env.get("ELR_RETENTION_RUNS", "0")
            ),
            serialize_writes=serialize_raw in {"1", "true", "True"},
        )
