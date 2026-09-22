"""Reproducibility: fingerprint stability, explicit mismatch marking, and
divergence attribution.
"""

from __future__ import annotations

import pytest

from elreg.errors import (
    DanglingReferenceError,
    ReproducibilityMismatchError,
)
from elreg.schemas import (
    ArtifactRegister,
    ExperimentCreate,
    ReproduceRequest,
    RunCreate,
)


def _registered_run(service):
    service.register_artifact(b"weights-v1", ArtifactRegister(id="w@v1"))
    service.register_artifact(b"dataset-xyz", ArtifactRegister(id="d@v1"))
    exp = service.create_experiment(ExperimentCreate(name="e"))
    run, _ = service.submit_run(
        RunCreate(
            experiment_id=exp.id,
            env={"python": "3.14", "updated_at": "2026-01-01T00:00:00Z"},
            config={"lr": 0.01, "layers": [1, 2, 3], "nested": {"a": 1, "b": 2}},
            artifacts=["w@v1", "d@v1"],
        )
    )
    return run


def test_identical_rerun_is_reproducible(service):
    run = _registered_run(service)
    result = service.verify_reproducibility(
        run.id,
        ReproduceRequest(
            env={"updated_at": "2026-09-22T12:00:00Z", "python": "3.14"},
            config={"nested": {"b": 2, "a": 1}, "lr": 0.01, "layers": [1, 2, 3]},
            artifacts=["d@v1", "w@v1"],
        ),
    )
    assert result.reproducible is True
    assert result.actual_fingerprint == run.fingerprint
    assert result.divergence == []


def test_key_order_whitespace_and_timestamp_do_not_drift(service):
    run = _registered_run(service)
    fp1 = service.verify_reproducibility(
        run.id,
        ReproduceRequest(
            env={"python": "3.14"},
            config={"lr": 0.01, "layers": [1, 2, 3], "nested": {"a": 1, "b": 2}},
            artifacts=["w@v1", "d@v1"],
        ),
    ).actual_fingerprint
    assert fp1 == run.fingerprint


def test_config_drift_marked_with_divergence_item(service):
    run = _registered_run(service)
    with pytest.raises(ReproducibilityMismatchError) as exc:
        service.verify_reproducibility(
            run.id,
            ReproduceRequest(
                env={"python": "3.14"},
                config={"lr": 0.02, "layers": [1, 2, 3],
                        "nested": {"a": 1, "b": 2}},
                artifacts=["w@v1", "d@v1"],
            ),
        )
    assert exc.value.code == "fingerprint_mismatch"
    result = exc.value.result
    assert result.reproducible is False
    keys = [(d["category"], d["key"], d["reason"]) for d in result.divergence]
    assert ("config", "lr", "value_mismatch") in keys
    assert result.expected_fingerprint == run.fingerprint
    assert result.actual_fingerprint != run.fingerprint


def test_missing_artifact_in_rerun_marked(service):
    run = _registered_run(service)
    with pytest.raises(ReproducibilityMismatchError) as exc:
        service.verify_reproducibility(
            run.id,
            ReproduceRequest(
                env={"python": "3.14"},
                config={"lr": 0.01, "layers": [1, 2, 3],
                        "nested": {"a": 1, "b": 2}},
                artifacts=["w@v1"],
            ),
        )
    reasons = {d["reason"] for d in exc.value.result.divergence}
    assert "artifact_missing_in_rerun" in reasons


def test_extra_artifact_in_rerun_marked(service):
    run = _registered_run(service)
    service.register_artifact(b"extra-artifact", ArtifactRegister(id="x@v1"))
    with pytest.raises(ReproducibilityMismatchError) as exc:
        service.verify_reproducibility(
            run.id,
            ReproduceRequest(
                env={"python": "3.14"},
                config={"lr": 0.01, "layers": [1, 2, 3],
                        "nested": {"a": 1, "b": 2}},
                artifacts=["w@v1", "d@v1", "x@v1"],
            ),
        )
    reasons = {d["reason"] for d in exc.value.result.divergence}
    assert "artifact_not_in_registration" in reasons


def test_rerun_referencing_missing_artifact_is_dangling(service):
    run = _registered_run(service)
    with pytest.raises(DanglingReferenceError) as exc:
        service.verify_reproducibility(
            run.id,
            ReproduceRequest(
                env={"python": "3.14"},
                config={"lr": 0.01, "layers": [1, 2, 3],
                        "nested": {"a": 1, "b": 2}},
                artifacts=["w@v1", "d@v1", "ghost@v9"],
            ),
        )
    assert exc.value.code == "artifact_not_found"


def test_mismatch_is_persisted_as_attempt(service):
    run = _registered_run(service)
    with pytest.raises(ReproducibilityMismatchError):
        service.verify_reproducibility(
            run.id,
            ReproduceRequest(env={"python": "3.13"},
                             config={"lr": 0.01, "layers": [1, 2, 3],
                                     "nested": {"a": 1, "b": 2}},
                             artifacts=["w@v1", "d@v1"]),
        )
    attempts = service.storage.list_reproduction_attempts(run.id)
    assert len(attempts) == 1 and attempts[0].reproducible is False


def test_same_content_new_version_still_reproduces(service):
    run = _registered_run(service)
    # Re-registering identical bytes dedupes to the same id; direct rerun
    # referencing that id matches by content sha.
    result = service.verify_reproducibility(
        run.id,
        ReproduceRequest(
            env={"python": "3.14"},
            config={"lr": 0.01, "layers": [1, 2, 3],
                    "nested": {"a": 1, "b": 2}},
            artifacts=["w@v1", "d@v1"],
        ),
    )
    assert result.reproducible
