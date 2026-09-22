"""Config defaults and startup-time rejection of unsupported values."""

from __future__ import annotations

import pytest

from elreg.config import load_settings
from elreg.errors import ConfigError


def test_defaults_are_documented_and_stable(monkeypatch):
    for key in list(__import__("os").environ):
        if key.startswith("ELR_"):
            monkeypatch.delenv(key)
    settings = load_settings()
    assert settings.data_dir == ".elreg-data"
    assert settings.capacity_policy == "reject"
    assert settings.retention_max_age_seconds == 7 * 24 * 3600
    assert settings.capacity_max_bytes == 1_000_000_000
    assert settings.gc_on_overflow is True
    assert settings.log_level == "INFO"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ELR_CAPACITY_POLICY", "nuclear"),
        ("ELR_RETENTION_MAX_AGE_SECONDS", "-5"),
        ("ELR_RETENTION_MAX_AGE_SECONDS", "soon"),
        ("ELR_CAPACITY_MAX_BYTES", "abc"),
        ("ELR_LOG_LEVEL", "CHATTY"),
        ("ELR_GC_ON_OVERFLOW", "maybe"),
        ("ELR_CAPACITY_WAIT_TIMEOUT_SEC", "-1"),
    ],
)
def test_invalid_values_rejected_at_load(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError) as exc:
        load_settings()
    assert exc.value.code == "invalid_config"


def test_valid_overrides(monkeypatch):
    monkeypatch.setenv("ELR_CAPACITY_POLICY", "evict-oldest")
    monkeypatch.setenv("ELR_GC_ON_OVERFLOW", "false")
    monkeypatch.setenv("ELR_LOG_LEVEL", "debug")
    settings = load_settings()
    assert settings.capacity_policy == "evict-oldest"
    assert settings.gc_on_overflow is False
    assert settings.log_level == "DEBUG"
