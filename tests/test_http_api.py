"""End-to-end HTTP classification: each failure reason keeps a distinct
status + error code through the API layer.
"""

from __future__ import annotations


def _experiment(client) -> str:
    return client.post("/experiments", json={"name": "e"}).json()["id"]


def _run(client, exp_id: str, **kw) -> str:
    return client.post("/runs", json={"experiment_id": exp_id, **kw}).json()["id"]


def test_root_remains_compatible(client):
    body = client.get("/").json()
    assert body["Hello"] == "World"
    assert client.get("/health").json() == {"status": "ok"}


def test_404s_are_distinct(client):
    assert client.get("/runs/run_ghost").status_code == 404
    assert client.get("/artifacts/a@ghost").status_code == 404
    resp = client.post("/lineage", json={"upstream_run_id": "g",
                                         "downstream_run_id": "g2"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "upstream_run_not_found"


def test_idempotent_conflict_http(client):
    exp = _experiment(client)
    body = {"experiment_id": exp, "idempotency_key": "k", "config": {"lr": 1}}
    assert client.post("/runs", json=body).status_code == 201
    repeat = client.post("/runs", json=body)
    assert repeat.status_code == 200 and repeat.json()["created"] is False
    conflict = client.post("/runs", json={**body, "config": {"lr": 2}})
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_payload_mismatch"


def test_state_machine_codes_over_http(client):
    rid = _run(client, _experiment(client))
    assert client.post(f"/runs/{rid}/transitions",
                       json={"state": "wat"}).json()["error"]["code"] == "unknown_state"
    jump = client.post(f"/runs/{rid}/transitions", json={"state": "succeeded"})
    assert jump.json()["error"]["code"] == "illegal_transition"
    for state in ("queued", "running", "failed"):
        assert client.post(f"/runs/{rid}/transitions",
                           json={"state": state}).status_code == 200
    terminal = client.post(f"/runs/{rid}/transitions", json={"state": "running"})
    assert terminal.status_code == 409
    assert terminal.json()["error"]["code"] == "terminal_state"


def test_artifact_upload_dedup_and_content(client):
    first = client.post("/artifacts?id=a@v1", content=b"abc123")
    assert first.status_code == 201 and first.json()["created"] is True
    second = client.post("/artifacts?id=a@v1", content=b"abc123")
    assert second.status_code == 200 and second.json()["created"] is False
    clash = client.post("/artifacts?id=a@v1", content=b"different")
    assert clash.status_code == 409
    assert clash.json()["error"]["code"] == "artifact_version_exists"
    assert client.get("/artifacts/a@v1/content").content == b"abc123"


def test_lineage_cycle_http(client):
    exp = _experiment(client)
    ids = [_run(client, exp) for _ in range(2)]
    client.post("/lineage", json={"upstream_run_id": ids[0],
                                  "downstream_run_id": ids[1]})
    resp = client.post("/lineage", json={"upstream_run_id": ids[1],
                                         "downstream_run_id": ids[0]})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "cycle_detected"


def test_metric_compare_missing_baseline_http(client):
    exp = _experiment(client)
    r1 = _run(client, exp)
    r2 = _run(client, exp)
    client.post(f"/runs/{r2}/metrics", json={"name": "acc", "value": 0.9})
    resp = client.post("/metrics/compare", json={
        "baseline_run_id": r1, "candidate_run_id": r2,
        "metrics": {"acc": {"direction": "maximize"}}})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "metric_missing_baseline"


def test_verify_mismatch_returns_divergence_http(client):
    exp = _experiment(client)
    rid = client.post("/runs", json={"experiment_id": exp,
                                     "config": {"lr": 0.1}}).json()["id"]
    resp = client.post(f"/runs/{rid}/verify",
                       json={"config": {"lr": 0.2}, "artifacts": []})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "fingerprint_mismatch"
    assert body["reproduction"]["reproducible"] is False
    assert any(d["key"] == "lr" for d in body["reproduction"]["divergence"])


def test_capacity_rejection_http(make_service):
    from fastapi.testclient import TestClient
    from elreg.api import create_app

    service = make_service(capacity=10, policy="reject", gc_on_overflow=False)
    client = TestClient(create_app(service))
    resp = client.post("/artifacts?id=big@v1", content=b"x" * 100)
    assert resp.status_code == 507
    assert resp.json()["error"]["code"] == "capacity_exceeded"


def test_gc_report_and_integrity_http(make_service):
    from fastapi.testclient import TestClient
    from elreg.api import create_app

    service = make_service(retention=0)
    client = TestClient(create_app(service))
    client.post("/artifacts?id=j@v1", content=b"junk!!")
    report = client.post("/artifacts/gc")
    assert report.status_code == 200
    assert report.json()["removed"] == ["j@v1"]
    assert client.get("/integrity").json()["healthy"] is True
