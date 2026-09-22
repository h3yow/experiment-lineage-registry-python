"""Concurrent updates and GC races: one consistent outcome, no overshoot,
no double count, no dangling reference.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from elr import artifacts as artifacts_svc
from elr import lineage as lineage_svc
from elr import metrics as metrics_svc
from elr import retention as gc_svc
from elr import runs as runs_svc
from elr.errors import (
    CapacityLimitReachedError,
    InvalidStateTransitionError,
    NotFoundError,
)


def test_concurrent_submissions_same_idempotency_key_single_run(make_world):
    k = make_world._kernel
    eid = make_world.exp.id
    errors, ids = [], []
    barrier = threading.Barrier(8)

    def submit():
        barrier.wait()
        try:
            r = runs_svc.submit_run(k, eid, idempotency_key="concurrent-key",
                                    config={"lr": 0.1})
            ids.append(r.id)
        except Exception as ex:  # pragma: no cover - diagnostic path
            errors.append(ex)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda _: submit(), range(8)))
    assert not errors
    assert set(ids) == {ids[0]} and len(ids) == 8
    rows = runs_svc.list_runs(k, eid)
    assert len([r for r in rows if r.idempotency_key == "concurrent-key"]) == 1


def test_concurrent_transitions_never_overshoot(make_world):
    k = make_world._kernel
    outcomes = []
    start = threading.Event()

    def fire(target):
        start.wait()
        try:
            r = runs_svc.transition_run(k, run_id, target)
            outcomes.append(("ok", r.status))
        except InvalidStateTransitionError as ex:
            outcomes.append(("rejected", ex.details.get("reason")))

    # many workers race PENDING -> RUNNING; exactly one may win
    run = make_world.submit()
    run_id = run.id
    threads = [threading.Thread(target=fire, args=("RUNNING",)) for _ in range(10)]
    for t in threads: t.start()
    start.set()
    for t in threads: t.join()
    wins = [o for o in outcomes if o == ("ok", "RUNNING")]
    assert len(wins) == 1
    assert runs_svc.get_run(k, run_id).status == "RUNNING"

    # terminal transition from RUNNING: one success, rest terminal/illegal
    outcomes.clear()
    run2 = make_world.submit()
    run_id = run2.id
    runs_svc.transition_run(k, run_id, "RUNNING")
    start2 = threading.Event()

    def fire2():
        start2.wait()
        try:
            runs_svc.transition_run(k, run_id, "SUCCEEDED")
            outcomes.append("ok")
        except InvalidStateTransitionError:
            outcomes.append("rejected")

    ts = [threading.Thread(target=fire2) for _ in range(8)]
    for t in ts: t.start()
    start2.set()
    for t in ts: t.join()
    assert outcomes.count("ok") == 1
    assert runs_svc.get_run(k, run_id).status == "SUCCEEDED"


def test_concurrent_metric_writes_no_lost_sequence(make_world):
    k = make_world._kernel
    run = make_world.submit()
    runs_svc.transition_run(k, run.id, "RUNNING")
    n = 20

    def write(i):
        metrics_svc.put_metric(k, run.id, "acc", float(i))

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(write, range(n)))
    latest = metrics_svc.get_metric(k, run.id, "acc")
    assert latest["seq"] == n  # exactly one point per write, dense sequence


def test_concurrent_gc_never_deletes_referenced_versions(make_world):
    """GC racing with more references/links: every *referenced* version must
    survive and no dangling reference may ever appear. Referencing happens
    atomically in the submit transaction, so once committed the version is
    protected -- GC re-checks references inside its own write transaction.
    """
    k = make_world._kernel
    eid = make_world.exp.id
    versions = [make_world.artifact(f"m{i}", f"content-{i}".encode())
                for i in range(10)]
    # one unreferenced version that GC is free to collect
    make_world.artifact("orphan", b"orphan-bytes")
    # all target versions committed as run inputs atomically, before GC starts
    runs = [
        runs_svc.submit_run(
            k, eid,
            artifacts=[{"name": f"m{i}",
                       "content_hash": versions[i].content_hash}],
        )
        for i in range(10)
    ]

    start = threading.Event()

    def collect_repeatedly():
        start.wait()
        for _ in range(10):
            gc_svc.collect(k)

    def link_artifact(i):
        start.wait()
        # explicit lineage link is a second reference kind to the same version
        lineage_svc.register_artifact_link(k, runs[i].id, versions[i].id,
                                           "uses")

    threads = [threading.Thread(target=collect_repeatedly) for _ in range(3)]
    threads += [threading.Thread(target=link_artifact, args=(i,))
                for i in range(10)]
    for t in threads: t.start()
    start.set()
    for t in threads: t.join()

    # invariant: zero dangling refs; all referenced versions still resolvable
    assert gc_svc.dangling(k) == []
    for i, v in enumerate(versions):
        assert artifacts_svc.get_version(k, v.id).id == v.id
        manifest = runs_svc.get_run_manifest(k, runs[i].id)
        assert manifest[f"m{i}"]["version_id"] == v.id


def test_unreferenced_version_loses_registration_race_with_explicit_error(
        make_world, monkeypatch):
    """When GC wins between registration and a separate referencing step,
    the loser gets the distinguishable artifact_version_not_registered
    rejection -- never a silent dangling reference.
    """
    k = make_world._kernel

    def _boom(conn, arts):
        # simulate GC winning the race: resolution simply finds no such
        # version at the moment of referencing
        from elr.errors import NotFoundError as _NF

        raise _NF("artifact m@x is not registered",
                  details={"reason": "artifact_version_not_registered"})

    monkeypatch.setattr("elr.runs._resolve_manifest", _boom)
    with pytest.raises(NotFoundError) as ei:
        make_world.submit(artifacts=[{"name": "m", "content_hash": "x"}])
    assert ei.value.details["reason"] == "artifact_version_not_registered"


def test_concurrent_explicit_deletes_of_referenced_version(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"keep-me")
    run = make_world.submit(artifacts=[
        {"name": "m", "content_hash": v.content_hash}])
    results = []
    start = threading.Event()

    def delete():
        start.wait()
        try:
            gc_svc.delete_version(k, v.id)
            results.append("deleted")
        except Exception as ex:
            results.append(ex.code)

    ts = [threading.Thread(target=delete) for _ in range(6)]
    for t in ts: t.start()
    start.set()
    for t in ts: t.join()
    assert "deleted" not in results
    assert set(results) == {"artifact_referenced"}
    # artifact still retrievable
    assert artifacts_svc.get_version(k, v.id).id == v.id
    assert gc_svc.dangling(k) == []


def test_capacity_reject_under_concurrent_registration(kernel_factory):
    k = kernel_factory(max_artifact_bytes=100, capacity_policy="reject")
    exp_id = __import__("elr.experiments", fromlist=["x"])
    from elr import experiments
    e = experiments.create_experiment(k, "e")

    accepted, rejected = [], []
    start = threading.Event()

    def reg(i):
        start.wait()
        try:
            v = artifacts_svc.register_artifact(k, f"a{i}", b"x" * 30)
            accepted.append(v.id)
        except CapacityLimitReachedError:
            rejected.append(1)

    ts = [threading.Thread(target=reg, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    start.set()
    for t in ts: t.join()
    usage = gc_svc.storage_usage(k)
    assert usage["version_bytes"] <= 100
    # capacity accounting was not double counted or overshot
    assert len(accepted) * 30 == usage["version_bytes"]
