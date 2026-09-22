"""Shared fixtures: isolated data dir per test, service + HTTP client."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from elreg.api import create_app
from elreg.config import Settings
from elreg.service import RegistryService


def build_settings(
    tmp_path: Path,
    *,
    retention: int = 999_999_999,
    capacity: int = 1_000_000_000,
    policy: str = "reject",
    gc_on_overflow: bool = False,
    wait: float = 1.0,
    log_level: str = "INFO",
) -> Settings:
    return Settings(
        data_dir=str(tmp_path),
        db_path=str(tmp_path / "registry.db"),
        artifact_dir=str(tmp_path / "artifacts"),
        retention_max_age_seconds=retention,
        capacity_max_bytes=capacity,
        capacity_policy=policy,
        gc_on_overflow=gc_on_overflow,
        capacity_wait_timeout_seconds=wait,
        log_level=log_level,
    )


@pytest.fixture(autouse=True)
def _enable_decision_logs(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """Ensure run ids, state transitions and decision reasons are logged."""
    from elreg.logging_setup import configure_logging

    configure_logging("INFO")
    caplog.set_level(logging.INFO, logger="elreg")
    yield


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return build_settings(tmp_path)


@pytest.fixture
def service(settings: Settings) -> Iterator[RegistryService]:
    svc = RegistryService(settings)
    svc.start()
    yield svc
    svc.close()


@pytest.fixture
def client(service: RegistryService) -> TestClient:
    return TestClient(create_app(service))


@pytest.fixture
def make_service(tmp_path: Path):
    created: list[RegistryService] = []

    def _make(**overrides) -> RegistryService:
        settings = build_settings(tmp_path, **overrides)
        svc = RegistryService(settings)
        svc.start()
        created.append(svc)
        return svc

    yield _make
    for svc in created:
        svc.close()
