"""HTTP surface: error codes/status mapping, idempotent replay, lifecycle and
the preserved root endpoint.
"""
from tests.conftest import b64


def test_root_preserved(client):
    assert client.get("/").json() == {"Hello": "World"}


def test_full_flow_over_http(client):
    e = client.post("/api/v1/experiments", json={"name": "e"}).json()
    r = client.post("/api/v1/artifacts",
                    json={"name": "m", "content_b64": b64(b"abc")})
    assert r.status_code == 201
    h, vid = r.json()["content_hash"], r.json()["id"]

    body = {"experiment_id": e["id"], "idempotency_key": "K",
            "env": {"py": "3.14"}, "config": {"lr": 0.1},
            "artifacts": [{"name": "m", "content_hash": h}]}
    r1 = client.post("/api/v1/runs", json=body)
    r2 = client.post("/api/v1/runs", json=body)
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["id"] == r2.json()["id"]

    # conflict
    bad = dict(body, env={"py": "3.13"})
    rc = client.post("/api/v1/runs", json=bad)
    assert rc.status_code == 409
    assert rc.json()["error"] == "idempotency_conflict"
    assert rc.json()["details"]["divergences"]

    rid = r1.json()["id"]
    assert client.post(f"/api/v1/runs/{rid}/transitions",
                       json={"status": "RUNNING"}).status_code == 200
    ok = client.post(f"/api/v1/runs/{rid}/verify",
                     json={"env": {"py": "3.14"}, "config": {"lr": 0.1},
                           "artifacts": [{"name": "m", "content_hash": h}]})
    assert ok.json()["reproducible"] is True
    badv = client.post(f"/api/v1/runs/{rid}/verify",
                       json={"env": {"py": "3.9"}})
    assert badv.json()["reproducible"] is False

    # lineage cycle over HTTP
    b2 = client.post("/api/v1/runs",
                     json={"experiment_id": e["id"]}).json()
    edge = client.post("/api/v1/lineage/run-deps",
                       json={"src_run_id": rid, "dst_run_id": b2["id"]})
    assert edge.status_code == 201
    cyc = client.post("/api/v1/lineage/run-deps",
                      json={"src_run_id": b2["id"], "dst_run_id": rid})
    assert cyc.status_code == 409 and cyc.json()["error"] == "lineage_cycle"


def test_metric_and_gc_endpoints(client):
    e = client.post("/api/v1/experiments", json={"name": "e"}).json()
    run = client.post("/api/v1/runs",
                      json={"experiment_id": e["id"]}).json()
    rid = run["id"]
    assert client.put(f"/api/v1/runs/{rid}/metrics/acc",
                      json={"name": "acc", "value": 0.8}).status_code == 200
    assert client.get(f"/api/v1/runs/{rid}/metrics/acc").json()["value"] == 0.8

    art = client.post("/api/v1/artifacts",
                      json={"name": "tmp", "content_b64": b64(b"tmp")}).json()
    usage0 = client.get("/api/v1/artifacts/gc/usage").json()
    assert usage0["version_count"] >= 1
    rep = client.post("/api/v1/artifacts/gc/collect").json()
    assert any(d["version_id"] == art["id"] for d in rep["deleted"])
    dangling = client.get("/api/v1/artifacts/gc/dangling").json()
    assert dangling == {"dangling": []}


def test_not_found_status(client):
    r = client.get("/api/v1/runs/ghost")
    assert r.status_code == 404 and r.json()["error"] == "not_found"
