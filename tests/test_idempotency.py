"""Idempotent run submission: same key -> same run; divergent payload ->
explicit, distinguishable rejection.
"""

from __future__ import annotations

import pytest

from elreg.errors import IdempotencyConflictError, NotFoundError
from elreg.schemas import ArtifactRegister, ExperimentCreate, RunCreate


def _experiment(service):
    return service.create_experiment(ExperimentCreate(name="e")).id


def test_repeated_submission_lands_on_same_run(service):
    exp_id = _experiment(service)
    payload = dict(experiment_id=exp_id, idempotency_key="dup-1",
                   env={"py": "3.14"}, config={"lr": 0.1})
    first, created1 = service.submit_run(RunCreate(**payload))
    second, created2 = service.submit_run(RunCreate(**payload))
    assert created1 is True and created2 is False
    assert first.id == second.id
    assert service.list_runs(exp_id) and len(service.list_runs(exp_id)) == 1


def test_key_order_and_whitespace_do_not_break_idempotency(service):
    exp_id = _experiment(service)
    first, _ = service.submit_run(RunCreate(
        experiment_id=exp_id, idempotency_key="k",
        env={"b": 1, "a": 2}, config={"nested": {"x": 1, "y": 2}}))
    # Same logical payload, different insertion order.
    second, created = service.submit_run(RunCreate(
        experiment_id=exp_id, idempotency_key="k",
        env={"a": 2, "b": 1}, config={"nested": {"y": 2, "x": 1}}))
    assert created is False and first.id == second.id


def test_volatile_timestamp_change_still_same_run(service):
    exp_id = _experiment(service)
    first, _ = service.submit_run(RunCreate(
        experiment_id=exp_id, idempotency_key="k",
        env={"submitted_at": "t1", "py": "3.14"}))
    # Only a volatile timestamp differs: still the same run.
    same, created = service.submit_run(RunCreate(
        experiment_id=exp_id, idempotency_key="k",
        env={"submitted_at": "t2", "py": "3.14"}))
    assert created is False and same.id == first.id
    # A real field change is still caught.
    with pytest.raises(IdempotencyConflictError) as exc:
        service.submit_run(RunCreate(
            experiment_id=exp_id, idempotency_key="k",
            env={"submitted_at": "t2", "py": "3.13"}))
    assert exc.value.code == "idempotency_payload_mismatch"
    assert "env" in str(exc.value)


@pytest.mark.parametrize(
    ("mutated", "expected_field"),
    [
        ({"env": {"py": "3.13"}}, "env"),
        ({"config": {"lr": 0.2}}, "config"),
        ({"metadata": {"note": "changed"}}, "metadata"),
    ],
)
def test_same_key_different_payload_rejected(service, mutated, expected_field):
    exp_id = _experiment(service)
    base = dict(experiment_id=exp_id, idempotency_key="k",
                env={"py": "3.14"}, config={"lr": 0.1}, metadata={"note": "v1"})
    first, _ = service.submit_run(RunCreate(**base))
    with pytest.raises(IdempotencyConflictError) as exc:
        service.submit_run(RunCreate(**{**base, **mutated}))
    assert exc.value.code == "idempotency_payload_mismatch"
    assert expected_field in str(exc.value)
    # Exactly one run exists: no silent second run.
    assert [r.id for r in service.list_runs(exp_id)] == [first.id]


def test_same_key_different_artifacts_rejected(service):
    exp_id = _experiment(service)
    service.register_artifact(b"aaaa", ArtifactRegister(id="a@v1"))
    service.register_artifact(b"bbbb", ArtifactRegister(id="b@v1"))
    service.submit_run(RunCreate(experiment_id=exp_id, idempotency_key="k",
                                 artifacts=["a@v1"]))
    with pytest.raises(IdempotencyConflictError) as exc:
        service.submit_run(RunCreate(experiment_id=exp_id, idempotency_key="k",
                                     artifacts=["b@v1"]))
    assert exc.value.code == "idempotency_payload_mismatch"
    assert "artifacts" in str(exc.value)


def test_idempotency_keys_are_scoped_per_experiment(service):
    e1 = _experiment(service)
    e2 = service.create_experiment(ExperimentCreate(name="e2")).id
    r1, c1 = service.submit_run(RunCreate(experiment_id=e1, idempotency_key="shared"))
    r2, c2 = service.submit_run(RunCreate(experiment_id=e2, idempotency_key="shared"))
    assert c1 and c2 and r1.id != r2.id


def test_submit_unknown_experiment_rejected(service):
    with pytest.raises(NotFoundError) as exc:
        service.submit_run(RunCreate(experiment_id="exp_missing"))
    assert exc.value.code == "experiment_not_found"


def test_idempotency_conflict_http_status(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    body = {"experiment_id": exp["id"], "idempotency_key": "k", "config": {"lr": 1}}
    assert client.post("/runs", json=body).status_code == 201
    resp = client.post("/runs", json={**body, "config": {"lr": 2}})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "idempotency_payload_mismatch"


def test_duplicate_dependency_in_one_submission_rejected(service):
    exp_id = _experiment(service)
    service.register_artifact(b"aaaa", ArtifactRegister(id="a@v1"))
    from elreg.errors import ConflictError
    with pytest.raises(ConflictError) as exc:
        service.submit_run(RunCreate(experiment_id=exp_id,
                                     idempotency_key="dupdep",
                                     artifacts=["a@v1", "a@v1"]))
    assert exc.value.code == "duplicate_artifact_dependency"
