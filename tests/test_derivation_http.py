"""HTTP surface for derivation and descendant pagination."""

from __future__ import annotations


def _terminal(client, exp_id, *, artifacts=None, key="base"):
    run = client.post("/runs", json={
        "experiment_id": exp_id, "idempotency_key": key,
        "config": {"lr": 0.1}, "artifacts": artifacts or []}).json()
    for state in ("queued", "running", "succeeded"):
        assert client.post(f"/runs/{run['id']}/transitions",
                           json={"state": state}).status_code == 200
    return run


def test_derive_http_happy_path(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    client.post("/artifacts?id=a@v1", content=b"aaaa")
    client.post("/artifacts?id=b@v1", content=b"bbbb")
    source = _terminal(client, exp["id"], artifacts=["a@v1"])

    resp = client.post(f"/runs/{source['id']}/derivations", json={
        "artifact_replacements": {"a@v1": "b@v1"},
        "config": {"lr": 0.2}, "idempotency_key": "d1"})
    assert resp.status_code == 201
    body = resp.json()
    assert body["created"] is True
    assert body["source_run_id"] == source["id"]
    assert any(c["category"] == "artifact" and c["op"] == "replaced"
               for c in body["changes"])

    # Idempotent replay -> 200 same run.
    replay = client.post(f"/runs/{source['id']}/derivations", json={
        "artifact_replacements": {"a@v1": "b@v1"},
        "config": {"lr": 0.2}, "idempotency_key": "d1"})
    assert replay.status_code == 200
    assert replay.json()["derived_run_id"] == body["derived_run_id"]

    child = client.get(f"/runs/{body['derived_run_id']}").json()
    assert child["derived_from"] == source["id"]
    assert client.get(f"/runs/{source['id']}").json()["derived_from"] is None


def test_derive_http_key_conflict_409(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    source = _terminal(client, exp["id"])
    assert client.post(f"/runs/{source['id']}/derivations",
                       json={"config": {"lr": 0.2}, "idempotency_key": "k"}
                       ).status_code == 201
    resp = client.post(f"/runs/{source['id']}/derivations",
                       json={"config": {"lr": 0.9}, "idempotency_key": "k"})
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"]["code"] == "derivation_idempotency_mismatch"
    # The conflict must name the commit id, the source run and the exact
    # diverging input leaf rather than a generic message.
    message = body["error"]["message"]
    assert "'k'" in message
    assert source["id"] in message
    assert "'lr'" in message


def test_derive_http_non_terminal_409(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    run = client.post("/runs", json={"experiment_id": exp["id"]}).json()
    resp = client.post(f"/runs/{run['id']}/derivations", json={"config": {"x": 1}})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "source_not_terminal"


def test_derive_http_missing_target_404(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    client.post("/artifacts?id=a@v1", content=b"aaaa")
    source = _terminal(client, exp["id"], artifacts=["a@v1"])
    resp = client.post(f"/runs/{source['id']}/derivations", json={
        "artifact_replacements": {"a@v1": "ghost@v9"}})
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "artifact_not_found"
    assert "ghost@v9" in body["error"]["message"]


def test_derive_http_deleted_target_409(make_service):
    # A GCollected target is an explicit 409 artifact_deleted, never swapped.
    from fastapi.testclient import TestClient
    from elreg.api import create_app
    service = make_service(retention=0)
    client = TestClient(create_app(service))
    exp = client.post("/experiments", json={"name": "e"}).json()
    client.post("/artifacts?id=a@v1", content=b"aaaa")
    client.post("/artifacts?id=g@v1", content=b"gggg")
    source = _terminal(client, exp["id"], artifacts=["a@v1"])
    client.post("/artifacts/gc")
    resp = client.post(f"/runs/{source['id']}/derivations", json={
        "artifact_replacements": {"a@v1": "g@v1"}})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "artifact_deleted"
    assert "g@v1" in resp.json()["error"]["message"]


def test_derivation_lookup_http(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    source = _terminal(client, exp["id"])
    created = client.post(f"/runs/{source['id']}/derivations",
                          json={"config": {"x": 1}}).json()
    ok = client.get(f"/runs/{created['derived_run_id']}/derivation")
    assert ok.status_code == 200
    assert ok.json()["source_run_id"] == source["id"]
    missing = client.get(f"/runs/{source['id']}/derivation")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "derivation_not_found"


def test_descendants_http_pagination(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    source = _terminal(client, exp["id"])
    for i in range(3):
        r = client.post(f"/runs/{source['id']}/derivations",
                        json={"config": {"i": i}, "idempotency_key": f"c{i}"})
        assert r.status_code == 201

    page = client.get(f"/runs/{source['id']}/descendants", params={"limit": 2})
    assert page.status_code == 200
    body = page.json()
    assert body["returned"] == 2
    assert body["truncated"] is True
    assert body["truncation_reason"] == "page_limit"
    assert body["next_cursor"]

    page2 = client.get(f"/runs/{source['id']}/descendants",
                       params={"limit": 2, "cursor": body["next_cursor"]})
    assert page2.json()["returned"] == 1
    assert page2.json()["truncated"] is False


def test_descendants_http_limit_validation(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    source = _terminal(client, exp["id"])
    bad = client.get(f"/runs/{source['id']}/descendants", params={"limit": 0})
    assert bad.status_code == 400
    assert bad.json()["error"]["code"] == "invalid_limit"
