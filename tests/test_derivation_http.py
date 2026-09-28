"""HTTP surface for derivation: routes, status codes and error codes."""

from __future__ import annotations


def _terminal_source(client, *, artifacts=None, env=None, config=None):
    exp = client.post("/experiments", json={"name": "e"}).json()
    for aid in artifacts or []:
        resp = client.post(f"/artifacts?id={aid}", content=f"c-{aid}".encode())
        assert resp.status_code in (200, 201)
    run = client.post(
        "/runs",
        json={
            "experiment_id": exp["id"],
            "idempotency_key": "src",
            "env": env or {},
            "config": config or {},
            "artifacts": list(artifacts or []),
        },
    ).json()
    for state in ("queued", "running", "succeeded"):
        assert client.post(
            f"/runs/{run['id']}/transitions", json={"state": state}
        ).status_code == 200
    return exp, run


def test_derive_endpoint_full_flow(client):
    exp, src = _terminal_source(
        client, artifacts=["a@v1"], env={"py": "3.13"}, config={"lr": 0.1}
    )
    assert client.post("/artifacts?id=b@v1", content=b"c-b").status_code == 201
    resp = client.post(
        f"/runs/{src['id']}/derive",
        json={
            "idempotency_key": "d1",
            "config_overrides": {"lr": 0.2},
            "replace_artifacts": [{"artifact_id": "a@v1", "replacement_id": "b@v1"}],
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["created"] is True
    assert body["run"]["id"] != src["id"]
    assert body["derivation"]["parent_run_id"] == src["id"]
    assert body["run"]["experiment_id"] == exp["id"]

    # Idempotent replay -> 200 same run.
    replay = client.post(
        f"/runs/{src['id']}/derive",
        json={
            "idempotency_key": "d1",
            "config_overrides": {"lr": 0.2},
            "replace_artifacts": [{"artifact_id": "a@v1", "replacement_id": "b@v1"}],
        },
    )
    assert replay.status_code == 200
    assert replay.json()["run"]["id"] == body["run"]["id"]
    assert replay.json()["created"] is False


def test_same_key_mismatch_returns_409_with_code(client):
    _, src = _terminal_source(client, config={"lr": 0.1})
    assert client.post(
        f"/runs/{src['id']}/derive",
        json={"idempotency_key": "k", "config_overrides": {"lr": 0.2}},
    ).status_code == 201
    resp = client.post(
        f"/runs/{src['id']}/derive",
        json={"idempotency_key": "k", "config_overrides": {"lr": 0.5}},
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "derivation_payload_mismatch"


def test_non_terminal_source_409(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    run = client.post("/runs", json={"experiment_id": exp["id"]}).json()
    resp = client.post(f"/runs/{run['id']}/derive", json={})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "source_run_not_terminal"


def test_missing_target_404_and_no_half_run(client):
    _, src = _terminal_source(client)
    resp = client.post(
        f"/runs/{src['id']}/derive", json={"add_artifacts": ["ghost@v9"]}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "artifact_not_found"
    runs = client.get("/runs").json()
    assert len(runs) == 1


def test_lineage_query_endpoints(client):
    _, src = _terminal_source(client)
    child = client.post(f"/runs/{src['id']}/derive", json={}).json()["run"]
    for state in ("queued", "running", "succeeded"):
        client.post(f"/runs/{child['id']}/transitions", json={"state": state})
    grand = client.post(f"/runs/{child['id']}/derive", json={}).json()["run"]

    children = client.get(f"/runs/{src['id']}/children").json()
    assert {c["run_id"] for c in children} == {child["id"]}

    derivation = client.get(f"/runs/{grand['id']}/derivation").json()
    assert derivation["parent_run_id"] == child["id"]

    descendants = client.get(f"/runs/{src['id']}/descendants").json()
    assert descendants["truncated"] is False
    assert {d["id"] for d in descendants["descendants"]} == {child["id"], grand["id"]}
    assert descendants["next_cursor"] is None

    ancestors = client.get(f"/runs/{grand['id']}/ancestors").json()
    assert [a["id"] for a in ancestors["ancestors"]] == [child["id"], src["id"]]
    assert ancestors["truncated"] is False


def test_descendants_pagination_query_params(client):
    _, src = _terminal_source(client)
    child_ids = []
    for i in range(3):
        child = client.post(
            f"/runs/{src['id']}/derive", json={"config_overrides": {"k": i}}
        ).json()["run"]
        child_ids.append(child["id"])

    page1 = client.get(f"/runs/{src['id']}/descendants?limit=2").json()
    assert len(page1["descendants"]) == 2
    assert page1["truncated"] is True
    assert page1["next_cursor"]
    page2 = client.get(
        f"/runs/{src['id']}/descendants",
        params={"limit": 2, "cursor": page1["next_cursor"]},
    ).json()
    assert len(page2["descendants"]) == 1
    assert page2["truncated"] is False
    seen = {d["id"] for d in page1["descendants"]} | {
        d["id"] for d in page2["descendants"]
    }
    assert seen == set(child_ids)

    bad_limit = client.get(f"/runs/{src['id']}/descendants?limit=999999")
    assert bad_limit.status_code == 400
    assert bad_limit.json()["error"]["code"] == "pagination_limit_exceeded"
    bad_cursor = client.get(f"/runs/{src['id']}/descendants?cursor=junk")
    assert bad_cursor.status_code == 400
    assert bad_cursor.json()["error"]["code"] == "invalid_cursor"


def test_derivation_404_for_unknown_run(client):
    assert client.post("/runs/nope/derive", json={}).status_code == 404


def test_existing_routes_remain_compatible(client):
    # Root and health routes keep their historical responses.
    root = client.get("/").json()
    assert root["Hello"] == "World"
    assert "service" in root
    assert client.get("/health").json() == {"status": "ok"}
