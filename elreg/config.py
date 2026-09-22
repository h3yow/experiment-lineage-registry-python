"""Configuration read from environment variables.

Environment variables (all optional; defaults shown):

============================== ================================= ===============
Variable                       Meaning                           Default
============================== ================================= ===============
ELR_DATA_DIR                   Directory for DB + artifacts      .elreg-data
ELR_DB_PATH                    SQLite database path              <data>/registry.db
ELR_ARTIFACT_DIR               Blob store directory              <data>/artifacts
ELR_RETENTION_MAX_AGE_SECONDS  Unreferenced artifact max age     604800 (7d)
ELR_CAPACITY_MAX_BYTES         Live artifact storage hard cap    1000000000
ELR_CAPACITY_POLICY            reject|evict-oldest|delay         reject
ELR_GC_ON_OVERFLOW             Try GC to free space first        true
ELR_CAPACITY_WAIT_TIMEOUT_SEC  Max wait when policy=delay        5.0
ELR_LOG_LEVEL                  logging level                     INFO
============================== ================================= ===============

Invalid values raise ConfigError at process startup (module load).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .errors import ConfigError

_VALID_CAPACITY_POLICIES = frozenset({"reject", "evict-oldest", "delay"})
_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


@dataclass(frozen=True)
class Settings:
    data_dir: str
    db_path: str
    artifact_dir: str
    retention_max_age_seconds: int
    capacity_max_bytes: int
    capacity_policy: str
    gc_on_overflow: bool
    capacity_wait_timeout_seconds: float
    log_level: str


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def _parse_int(name: str, raw: str | None, default: int, *, minimum: int = 0) -> int:
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(
            f"{name} must be an integer, got {raw!r}", code="invalid_config"
        ) from exc
    if value < minimum:
        raise ConfigError(
            f"{name} must be >= {minimum}, got {value}", code="invalid_config"
        )
    return value


def _parse_float(name: str, raw: str | None, default: float, *, minimum: float = 0.0) -> float:
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(
            f"{name} must be a number, got {raw!r}", code="invalid_config"
        ) from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}", code="invalid_config")
    return value


def _parse_bool(name: str, raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    raise ConfigError(
        f"{name} must be one of {sorted(_TRUE_VALUES | _FALSE_VALUES)}, got {raw!r}",
        code="invalid_config",
    )


def load_settings() -> Settings:
    """Load settings from the environment; fail fast on invalid values."""
    data_dir = _env("ELR_DATA_DIR") or ".elreg-data"
    db_path = _env("ELR_DB_PATH") or str(Path(data_dir) / "registry.db")
    artifact_dir = _env("ELR_ARTIFACT_DIR") or str(Path(data_dir) / "artifacts")

    retention = _parse_int(
        "ELR_RETENTION_MAX_AGE_SECONDS",
        _env("ELR_RETENTION_MAX_AGE_SECONDS"),
        default=7 * 24 * 3600,
    )
    capacity = _parse_int(
        "ELR_CAPACITY_MAX_BYTES", _env("ELR_CAPACITY_MAX_BYTES"), default=1_000_000_000
    )

    policy_raw = _env("ELR_CAPACITY_POLICY") or "reject"
    if policy_raw not in _VALID_CAPACITY_POLICIES:
        raise ConfigError(
            "ELR_CAPACITY_POLICY must be one of "
            f"{sorted(_VALID_CAPACITY_POLICIES)}, got {policy_raw!r}",
            code="invalid_config",
        )

    gc_on_overflow = _parse_bool(
        "ELR_GC_ON_OVERFLOW", _env("ELR_GC_ON_OVERFLOW"), default=True
    )
    wait_timeout = _parse_float(
        "ELR_CAPACITY_WAIT_TIMEOUT_SEC",
        _env("ELR_CAPACITY_WAIT_TIMEOUT_SEC"),
        default=5.0,
    )

    log_level = (
        _env("ELR_LOG_LEVEL") or "INFO"
    ).upper()
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(
            f"ELR_LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}, got {log_level!r}",
            code="invalid_config",
        )

    return Settings(
        data_dir=data_dir,
        db_path=db_path,
        artifact_dir=artifact_dir,
        retention_max_age_seconds=retention,
        capacity_max_bytes=capacity,
        capacity_policy=policy_raw,
        gc_on_overflow=gc_on_overflow,
        capacity_wait_timeout_seconds=wait_timeout,
        log_level=log_level,
    )
