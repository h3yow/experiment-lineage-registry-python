"""Retention window, two-phase deferral, capacity accounting, dangling
exposure and referenced-version protection."""
import os
from pathlib import Path

import pytest

from elr import artifacts as artifacts_svc
from elr import retention as gc_svc
from elr import runs as runs_svc
from elr.artifacts import _blob_path
from elr.errors import (
    ArtifactReferencedError,
    CapacityLimitReachedError,
)


def test_unreferenced_version_collected_under_reject(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    report = gc_svc.collect(k)
    assert [d["version_id"] for d in report["deleted"]] == [v.id]
    assert not _blob_path(k, v.content_hash).exists()
    from elr.errors import NotFoundError
    with pytest.raises(NotFoundError):
        artifacts_svc.get_version(k, v.id)


def test_referenced_version_never_collected(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    make_world.submit(artifacts=[{"name": "m", "content_hash": v.content_hash}])
    report = gc_svc.collect(k)
    assert report["deleted"] == []
    assert artifacts_svc.get_version(k, v.id).id == v.id


def test_explicit_delete_referenced_rejected_with_run_ids(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    r = make_world.submit(
        artifacts=[{"name": "m", "content_hash": v.content_hash}])
    with pytest.raises(ArtifactReferencedError) as ei:
        gc_svc.delete_version(k, v.id)
    assert ei.value.details["referenced_by"] == [r.id]
    assert artifacts_svc.get_version(k, v.id).id == v.id  # still present


def test_two_phase_defer_policy(kernel_factory):
    k = kernel_factory(capacity_policy="defer")
    from elr import experiments
    exp = experiments.create_experiment(k, "e")
    v = artifacts_svc.register_artifact(k, "m", b"abc")
    # referenced -> explicit delete only marks pending, never removes
    r = runs_svc.submit_run(
        k, exp.id,
        artifacts=[{"name": "m", "content_hash": v.content_hash}])
    out = gc_svc.delete_version(k, v.id)
    assert out["deferred"] is True and out["referenced_by"] == [r.id]
    assert artifacts_svc.get_version(k, v.id).id == v.id
    assert len(gc_svc.pending_deletions(k)) == 1
    # an unreferenced version is merely marked by the first collect...
    v2 = artifacts_svc.register_artifact(k, "m2", b"def")
    rep = gc_svc.collect(k)
    assert [d["version_id"] for d in rep["deferred"]] == [v2.id]
    assert artifacts_svc.get_version(k, v2.id)
    # ...and reaped by the second cycle while still unreferenced
    rep2 = gc_svc.collect(k)
    assert [d["version_id"] for d in rep2["deleted"]] == [v2.id]
    # referenced pending v survives collection; if it becomes unreferenced
    # later, a following cycle reaps it. Drop its reference by replacing the
    # pending run manifest while PENDING.
    runs_svc.update_run_manifest(k, r.id, [])
    rep3 = gc_svc.collect(k)
    assert [d["version_id"] for d in rep3["deleted"]] == [v.id]
    assert gc_svc.pending_deletions(k) == []
    assert gc_svc.dangling(k) == []


def test_retention_window_keeps_recent_unreferenced(kernel_factory):
    k = kernel_factory(retention_runs=2)
    from elr import experiments
    exp = experiments.create_experiment(k, "e")
    # fewer than 2 ended runs -> conservative: retain everything
    v0 = artifacts_svc.register_artifact(k, "m0", b"0")
    assert gc_svc.collect(k)["deleted"] == []
    r1 = runs_svc.submit_run(k, exp.id)
    runs_svc.transition_run(k, r1.id, "RUNNING")
    runs_svc.transition_run(k, r1.id, "SUCCEEDED")
    # still only 1 ended run: retain
    assert gc_svc.collect(k)["deleted"] == []
    r2 = runs_svc.submit_run(k, exp.id)
    runs_svc.transition_run(k, r2.id, "RUNNING")
    runs_svc.transition_run(k, r2.id, "SUCCEEDED")
    import time
    time.sleep(0.01)
    # now 2 ended runs; versions created *after* the cutoff (2nd latest ended)
    # remain retained. v0 was created before cutoff -> collectable.
    rep = gc_svc.collect(k)
    assert [d["version_id"] for d in rep["deleted"]] == [v0.id]
    v1 = artifacts_svc.register_artifact(k, "m1", b"1")
    rep = gc_svc.collect(k)
    assert any(d["reason"] == "within_retention_window"
               for d in rep["retained"])
    assert rep["deleted"] == []


def test_capacity_reject_and_defer_reclaim(kernel_factory):
    k = kernel_factory(max_artifact_bytes=10, capacity_policy="reject")
    artifacts_svc.register_artifact(k, "a", b"x" * 6)
    with pytest.raises(CapacityLimitReachedError) as ei:
        artifacts_svc.register_artifact(k, "b", b"y" * 5)
    assert ei.value.details["reason"] == "over_limit"

    # defer policy: registration over limit triggers reclaim of unreferenced
    k2 = kernel_factory(max_artifact_bytes=10, capacity_policy="defer")
    artifacts_svc.register_artifact(k2, "a", b"x" * 6)
    artifacts_svc.register_artifact(k2, "b", b"y" * 4)  # exactly 10, ok
    with pytest.raises(CapacityLimitReachedError) as ei2:
        artifacts_svc.register_artifact(k2, "c", b"z" * 1)
    # no unreferenced versions (none marked) -> reclaim frees nothing
    assert ei2.value.details["reason"] == "capacity_after_reclaim"
    usage = gc_svc.storage_usage(k2)
    assert usage["version_bytes"] == 10 and usage["limit_bytes"] == 10


def test_dangling_reports_missing_blobs(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    r = make_world.submit(
        artifacts=[{"name": "m", "content_hash": v.content_hash}])
    assert gc_svc.dangling(k) == []
    _blob_path(k, v.content_hash).unlink()
    d = gc_svc.dangling(k)
    assert len(d) == 1
    assert d[0]["run_id"] == r.id
    assert d[0]["artifact_version_id"] == v.id
    assert d[0]["reason"] == "blob_missing"


def test_shared_blob_not_deleted_until_all_versions_gone(make_world):
    """Same content under different names shares one blob; GC of one version
    must not remove the blob while another version row keeps the hash."""
    k = make_world._kernel
    va = make_world.artifact("a", b"shared-bytes")
    vb = make_world.artifact("b", b"shared-bytes")
    assert va.content_hash == vb.content_hash
    # protect vb via a run; collect removes va only
    make_world.submit(
        artifacts=[{"name": "b", "content_hash": vb.content_hash}])
    gc_svc.collect(k)
    assert _blob_path(k, va.content_hash).exists()
    # after the second reference goes away (manifest replaced while PENDING),
    # the final GC removes the shared blob too
    from elr import runs as _r
    run = runs_svc.list_runs(k, make_world.exp.id)[-1]
    runs_svc.update_run_manifest(k, run.id, [])
    gc_svc.collect(k)
    gc_svc.collect(k)  # defer not configured; single-phase, idempotent repeat
    assert not _blob_path(k, vb.content_hash).exists()
