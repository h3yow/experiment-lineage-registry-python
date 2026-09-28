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
    assert settings.derivation_page_limit == 100
    assert settings.derivation_max_limit == 1000
    assert settings.derivation_scan_cap == 100_000


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
        ("ELR_DERIVATION_PAGE_LIMIT", "0"),
        ("ELR_DERIVATION_MAX_LIMIT", "abc"),
        ("ELR_DERIVATION_SCAN_CAP", "-2"),
    ],
)
def test_invalid_values_rejected_at_load(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError) as exc:
        load_settings()
    assert exc.value.code == "invalid_config"


@pytest.mark.parametrize(
    ("page", "maximum", "cap"),
    [
        ("100", "50", "1000"),    # page > max
        ("100", "2000", "1000"),  # max > scan cap
    ],
)
def test_derivation_limit_relationship_rejected(monkeypatch, page, maximum, cap):
    monkeypatch.setenv("ELR_DERIVATION_PAGE_LIMIT", page)
    monkeypatch.setenv("ELR_DERIVATION_MAX_LIMIT", maximum)
    monkeypatch.setenv("ELR_DERIVATION_SCAN_CAP", cap)
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
