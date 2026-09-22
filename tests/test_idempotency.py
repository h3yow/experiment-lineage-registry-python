"""Same idempotency key -> same run; different payload -> explicit conflict."""
import logging

import pytest

from elr import runs as runs_svc
from elr.errors import IdempotencyConflictError


def _ref(make_world, name="m", data=b"abc"):
    v = make_world.artifact(name, data)
    return [{"name": name, "content_hash": v.content_hash}]


def test_repeated_submission_lands_on_same_run(make_world, caplog):
    caplog.set_level(logging.INFO)
    k = make_world._kernel
    kw = dict(env={"py": "3.14"}, config={"lr": 0.1},
              artifacts=_ref(make_world))
    r1 = make_world.submit(idempotency_key="job-1", **kw)
    r2 = make_world.submit(idempotency_key="job-1", **kw)
    assert r1.id == r2.id
    assert r1.submitted_fingerprint == r2.submitted_fingerprint
    assert len(runs_svc.list_runs(k, make_world.exp.id)) == 1
    assert any("run_idempotent_replay" in m and r1.id in m
               for m in caplog.messages)


def test_replay_immune_to_key_order_whitespace_and_timestamps(make_world):
    k = make_world._kernel
    r1 = make_world.submit(
        idempotency_key="job-2",
        env={"py": "3.14", "created_at": "2020-01-01"},
        config={"z": 1, "a": {"deep": [1, 2, 3]}},
        artifacts=_ref(make_world),
    )
    r2 = make_world.submit(
        idempotency_key="job-2",
        env={"created_at": "2099-01-01", "py": "  3.14  "},
        config={"a": {"deep": [1, 2, 3]}, "z": 1},
        artifacts=_ref(make_world),
    )
    assert r1.id == r2.id


def test_conflicting_env_rejected_with_divergences(make_world):
    make_world.submit(idempotency_key="job-3", env={"py": "3.14"},
                      config={"lr": 0.1}, artifacts=_ref(make_world))
    with pytest.raises(IdempotencyConflictError) as ei:
        make_world.submit(idempotency_key="job-3", env={"py": "3.13"},
                          config={"lr": 0.1}, artifacts=_ref(make_world))
    d = ei.value.details
    assert d["existing_fingerprint"] != d["incoming_fingerprint"]
    assert any("$.env.py" in x for x in d["divergences"])
    assert "existing_run_id" in d


def test_conflicting_artifact_dependency_rejected(make_world):
    v1 = make_world.artifact("model", b"version-one")
    v2 = make_world.artifact("model", b"version-two")
    make_world.submit(idempotency_key="job-4",
                      artifacts=[{"name": "model",
                                  "content_hash": v1.content_hash}])
    with pytest.raises(IdempotencyConflictError) as ei:
        make_world.submit(idempotency_key="job-4",
                          artifacts=[{"name": "model",
                                      "content_hash": v2.content_hash}])
    assert any("$.artifacts.model" in x for x in ei.value.details["divergences"])


def test_conflict_does_not_create_second_run(make_world):
    k = make_world._kernel
    make_world.submit(idempotency_key="job-5", config={"a": 1})
    with pytest.raises(IdempotencyConflictError):
        make_world.submit(idempotency_key="job-5", config={"a": 2})
    runs = runs_svc.list_runs(k, make_world.exp.id)
    assert len([r for r in runs if r.idempotency_key == "job-5"]) == 1
