"""Shared fixtures: fresh in-file kernel + TestClient per test."""
from __future__ import annotations

import base64

import pytest

from elr import Kernel, Settings
from elr import artifacts as artifacts_svc
from elr import experiments as experiments_svc
from elr import runs as runs_svc


@pytest.fixture
def tmp_root(tmp_path):
    return tmp_path


@pytest.fixture
def kernel(tmp_root):
    k = Kernel(Settings(
        db_path=str(tmp_root / "elr.db"),
        artifact_store=str(tmp_root / "artifacts"),
    ))
    yield k
    k.close()


@pytest.fixture
def kernel_factory(tmp_root):
    created = []

    def _make(**overrides):
        sub = tmp_root / f"k{len(created)}"
        sub.mkdir()
        k = Kernel(Settings(
            db_path=str(sub / "elr.db"),
            artifact_store=str(sub / "artifacts"),
            **overrides,
        ))
        created.append(k)
        return k

    yield _make
    for k in created:
        k.close()


@pytest.fixture
def client(tmp_root, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("ELR_DB_PATH", str(tmp_root / "http.db"))
    monkeypatch.setenv("ELR_ARTIFACT_STORE", str(tmp_root / "http_artifacts"))
    # Build a dedicated app with a dedicated kernel (main reads env at import
    # time, which is process-global in the test worker).
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from elr.api.routes import router
    from elr.config import Settings as S
    from elr.errors import ELRError

    app = FastAPI()
    app.state.kernel = Kernel(S(
        db_path=str(tmp_root / "http.db"),
        artifact_store=str(tmp_root / "http_artifacts"),
    ))
    app.include_router(router)

    @app.exception_handler(ELRError)
    async def _h(request, exc):
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.get("/")
    def _root():
        return {"Hello": "World"}

    with TestClient(app) as c:
        yield c
    app.state.kernel.close()


@pytest.fixture
def make_world(kernel):
    """Create an experiment, some artifact versions and submit helper."""
    class World:
        def __init__(self, kernel):
            self._kernel = kernel
            self.exp = experiments_svc.create_experiment(kernel, "exp")
            self.versions = {}

        def artifact(self, name: str, content: bytes):
            v = artifacts_svc.register_artifact(kernel, name, content)
            self.versions[name] = v
            return v

        def submit(self, **kw):
            kw.setdefault("experiment_id", self.exp.id)
            return runs_svc.submit_run(kernel, **kw)

    return World(kernel)


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()
