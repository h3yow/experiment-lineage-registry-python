"""Content-addressed artifacts, versioning rules and GC reference safety."""

from __future__ import annotations

import pytest

from elreg.errors import (
    CapacityDelayedError,
    CapacityRejectedError,
    ConflictError,
    DanglingReferenceError,
)
from elreg.schemas import (
    ArtifactRegister,
    ExperimentCreate,
    RunArtifactAttach,
    RunCreate,
)


def test_content_dedup_same_bytes_same_artifact(service):
    a1, created1 = service.register_artifact(b"same-bytes", ArtifactRegister(id="m@v1"))
    a2, created2 = service.register_artifact(b"same-bytes", ArtifactRegister(id="m@v1"))
    assert created1 is True and created2 is False
    assert a1.id == a2.id and a1.sha256 == a2.sha256


def test_same_version_different_content_rejected(service):
    service.register_artifact(b"v1-bytes", ArtifactRegister(id="m@v1"))
    with pytest.raises(ConflictError) as exc:
        service.register_artifact(b"v2-different-bytes", ArtifactRegister(id="m@v1"))
    assert exc.value.code == "artifact_version_exists"


def test_identical_content_cannot_be_reidentified(service):
    service.register_artifact(b"same-bytes", ArtifactRegister(id="m@v1"))
    with pytest.raises(ConflictError) as exc:
        service.register_artifact(b"same-bytes", ArtifactRegister(id="m@v2"))
    assert exc.value.code == "content_already_registered"


def test_referenced_artifact_survives_gc(make_service):
    service = make_service(retention=0)
    artifact, _ = service.register_artifact(b"keep-me", ArtifactRegister(id="k@v1"))
    junk, _ = service.register_artifact(b"junk!!", ArtifactRegister(id="j@v1"))
    exp = service.create_experiment(ExperimentCreate(name="e"))
    service.submit_run(RunCreate(experiment_id=exp.id, artifacts=["k@v1"]))

    result = service.garbage_collect()
    assert "j@v1" in result.removed
    assert "k@v1" not in result.removed
    assert any(
        item["artifact_id"] == "k@v1" and item["reason"] == "referenced_by_run"
        for item in result.retained
    )
    assert service.get_artifact("k@v1").deleted is False
    assert service.get_artifact("j@v1").deleted is True


def test_retention_window_blocks_eviction(make_service):
    service = make_service(retention=10**9)
    service.register_artifact(b"junk!!", ArtifactRegister(id="j@v1"))
    result = service.garbage_collect()
    assert result.removed == []
    assert result.retained[0]["reason"] == "within_retention_window"


def test_deleted_artifact_cannot_be_attached(make_service):
    service = make_service(retention=0)
    service.register_artifact(b"junk!!", ArtifactRegister(id="j@v1"))
    service.garbage_collect()
    exp = service.create_experiment(ExperimentCreate(name="e"))
    run, _ = service.submit_run(RunCreate(experiment_id=exp.id))
    with pytest.raises(DanglingReferenceError) as exc:
        service.attach_run_artifact(run.id, RunArtifactAttach(artifact_id="j@v1"))
    assert exc.value.code == "artifact_deleted"


def test_submit_with_deleted_dependency_exposes_dangling(make_service):
    service = make_service(retention=0)
    service.register_artifact(b"other!", ArtifactRegister(id="j@v2"))
    exp = service.create_experiment(ExperimentCreate(name="e"))
    # Unreferenced artifact is collected immediately by GC.
    service.garbage_collect()
    with pytest.raises(DanglingReferenceError) as exc:
        service.submit_run(RunCreate(experiment_id=exp.id, artifacts=["j@v2"]))
    assert exc.value.code == "artifact_deleted"


def test_capacity_reject_policy(make_service):
    service = make_service(capacity=20, policy="reject", gc_on_overflow=False)
    with pytest.raises(CapacityRejectedError) as exc:
        service.register_artifact(b"x" * 50, ArtifactRegister(id="big@v1"))
    assert exc.value.code == "capacity_exceeded"
    # Nothing was partially registered.
    assert service.list_artifacts() == []


def test_capacity_evict_oldest_frees_space(make_service):
    service = make_service(capacity=40, policy="evict-oldest",
                           retention=0, gc_on_overflow=True)
    service.register_artifact(b"junk" * 4, ArtifactRegister(id="j@v1"))  # 16
    kept, _ = service.register_artifact(b"keep" * 3, ArtifactRegister(id="k@v1"))  # 12
    exp = service.create_experiment(ExperimentCreate(name="e"))
    service.submit_run(RunCreate(experiment_id=exp.id, artifacts=["k@v1"]))
    # 12 live + 20 new = 32 <= 40 after j@v1 (16) evicted.
    view, created = service.register_artifact(b"y" * 20, ArtifactRegister(id="n@v1"))
    assert created is True
    assert service.get_artifact("j@v1").deleted is True
    assert service.get_artifact("k@v1").deleted is False


def test_capacity_delay_then_released(make_service):
    import threading
    import hashlib
    import time

    service = make_service(capacity=30, policy="delay",
                           retention=10**9, wait=2.0, gc_on_overflow=False)
    service.register_artifact(b"junkjunkjunkjunk", ArtifactRegister(id="j@v1"))

    def release():
        time.sleep(0.3)
        sha = hashlib.sha256(b"junkjunkjunkjunk").hexdigest()
        with service._lock:
            with service.storage.begin():
                service.storage.mark_artifact_deleted("j@v1", "t")
            service.storage.remove_blob(sha)
            service._capacity_cv.notify_all()

    thread = threading.Thread(target=release)
    thread.start()
    start = time.monotonic()
    view, created = service.register_artifact(b"z" * 20, ArtifactRegister(id="d@v1"))
    elapsed = time.monotonic() - start
    thread.join()
    assert created and elapsed >= 0.25


def test_capacity_delay_timeout_distinguishable(make_service):
    service = make_service(capacity=10, policy="delay",
                           retention=10**9, wait=0.2, gc_on_overflow=False)
    service.register_artifact(b"junkjunk!!", ArtifactRegister(id="j@v1"))
    with pytest.raises(CapacityDelayedError) as exc:
        service.register_artifact(b"z" * 20, ArtifactRegister(id="d@v1"))
    assert exc.value.code == "capacity_wait_timeout"
