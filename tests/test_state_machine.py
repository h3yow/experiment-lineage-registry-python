"""Illegal state transitions must be classified with distinguishable codes;
terminal runs must reject every later mutation.
"""

from __future__ import annotations

import logging

import pytest

from elreg.errors import (
    NotFoundError,
    TerminalWriteError,
)
from elreg.schemas import (
    ArtifactRegister,
    ExperimentCreate,
    MetricWrite,
    RunArtifactAttach,
    RunCreate,
)


def _submit(service, **overrides):
    exp = service.create_experiment(ExperimentCreate(name="e"))
    run, _ = service.submit_run(RunCreate(experiment_id=exp.id, **overrides))
    return exp, run


def test_legal_chain_created_to_succeeded(service):
    _, run = _submit(service)
    rid = run.id
    assert service.transition_run(rid, "queued").state == "queued"
    assert service.transition_run(rid, "running").state == "running"
    out = service.transition_run(rid, "succeeded")
    assert out.state == "succeeded" and out.terminal is True
    logging.getLogger("test").info(
        "test_legal_chain run_id=%s final_state=succeeded", rid
    )


def test_created_may_cancel_directly(service):
    _, run = _submit(service)
    assert service.transition_run(run.id, "canceled").state == "canceled"


def test_running_may_fail(service):
    _, run = _submit(service)
    service.transition_run(run.id, "running")
    assert service.transition_run(run.id, "failed").terminal


@pytest.mark.parametrize(
    ("path", "target", "code"),
    [
        (["queued", "running", "succeeded"], "running", "terminal_state"),
        (["queued", "running", "failed"], "queued", "terminal_state"),
        (["canceled"], "running", "terminal_state"),
        (["queued"], "created", "illegal_transition"),
        (["queued", "running"], "queued", "illegal_transition"),
        (["queued"], "succeeded", "illegal_transition"),
        (["queued"], "queued", "already_in_state"),
        ([], "nonsense", "unknown_state"),
    ],
)
def test_illegal_transitions_are_classified(service, path, target, code):
    _, run = _submit(service)
    for state in path:
        service.transition_run(run.id, state)
    with pytest.raises(Exception) as exc_info:
        service.transition_run(run.id, target)
    assert exc_info.value.code == code
    logging.getLogger("test").info(
        "test_illegal_transition run_id=%s attempted=%s decision=%s reason=%s",
        run.id, target, "rejected", code,
    )


def test_terminal_run_rejects_metric_artifact_and_attach(service):
    _, run = _submit(service)
    artifact, _ = service.register_artifact(b"weights-bytes", ArtifactRegister(id="w@v1"))
    service.transition_run(run.id, "running")
    service.transition_run(run.id, "succeeded")

    with pytest.raises(TerminalWriteError) as exc:
        service.write_metric(run.id, MetricWrite(name="acc", value=1.0))
    assert exc.value.code == "terminal_state"
    with pytest.raises(TerminalWriteError):
        service.attach_run_artifact(
            run.id, RunArtifactAttach(artifact_id="w@v1", role="output")
        )
    with pytest.raises(TerminalWriteError):
        service.transition_run(run.id, "failed")
    logging.getLogger("test").info(
        "test_terminal_immutability run_id=%s decision=denied terminal_fields=state,metrics,artifacts",
        run.id,
    )


def test_terminal_fingerprint_not_rewritten(service):
    _, run = _submit(
        service, idempotency_key="k1", env={"py": "3.14"}, config={"lr": 1.0}
    )
    fp_before = service.get_run(run.id).fingerprint
    service.transition_run(run.id, "queued")
    service.transition_run(run.id, "running")
    service.transition_run(run.id, "succeeded")
    # Replay with same idempotency key lands on same run, fingerprint intact.
    replay, created = service.submit_run(
        RunCreate(
            experiment_id=run.experiment_id,
            idempotency_key="k1",
            env={"py": "3.14"},
            config={"lr": 1.0},
        )
    )
    assert created is False
    assert replay.fingerprint == fp_before


def test_missing_run_transition(service):
    with pytest.raises(NotFoundError) as exc:
        service.transition_run("run_missing", "running")
    assert exc.value.code == "run_not_found"


def test_state_machine_via_http(client):
    exp = client.post("/experiments", json={"name": "e"}).json()
    rid = client.post("/runs", json={"experiment_id": exp["id"]}).json()["id"]
    ok = client.post(f"/runs/{rid}/transitions", json={"state": "succeeded"})
    assert ok.status_code == 409
    assert ok.json()["error"]["code"] == "illegal_transition"
    client.post(f"/runs/{rid}/transitions", json={"state": "queued"})
    client.post(f"/runs/{rid}/transitions", json={"state": "running"})
    client.post(f"/runs/{rid}/transitions", json={"state": "succeeded"})
    again = client.post(f"/runs/{rid}/transitions", json={"state": "running"})
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "terminal_state"
