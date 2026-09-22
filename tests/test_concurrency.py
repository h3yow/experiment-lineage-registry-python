"""Concurrency: concurrent updates to one run and parallel GC/registration
must end in one consistent outcome - no state overshoot, double counting or
half-deleted state.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from elreg.errors import (
    InvalidTransitionError,
    TerminalWriteError,
)
from elreg.schemas import (
    ArtifactRegister,
    ExperimentCreate,
    RunCreate,
)


def _run(service):
    exp = service.create_experiment(ExperimentCreate(name="e"))
    return service.submit_run(RunCreate(experiment_id=exp.id))[0]


def test_concurrent_transitions_only_one_succeeds_per_step(service, caplog):
    run = _run(service)

    outcomes: list[tuple[str, str]] = []

    def attempt(target: str):
        try:
            service.transition_run(run.id, target)
            outcomes.append((target, "accepted"))
        except (InvalidTransitionError, TerminalWriteError) as exc:
            outcomes.append((target, exc.code))

    # Everyone races the same queued transition; exactly one must win.
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: attempt("queued"), range(16)))

    accepted = [o for o in outcomes if o[1] == "accepted"]
    assert len(accepted) == 1, outcomes
    assert service.get_run(run.id).state == "queued"
    # Service logs carry run id, transition and the decision basis.
    accepted_logs = [
        r for r in caplog.records
        if r.name == "elreg.service"
        and r.getMessage() == "run_transition"
        and getattr(r, "kv", {}).get("run_id") == run.id
    ]
    denied_logs = [
        r for r in caplog.records
        if r.name == "elreg.service"
        and r.getMessage() == "run_transition_denied"
        and getattr(r, "kv", {}).get("run_id") == run.id
    ]
    assert len(accepted_logs) == 1
    assert accepted_logs[0].kv["from"] == "created"
    assert accepted_logs[0].kv["to"] == "queued"
    assert accepted_logs[0].kv["reason"] == "accepted"
    assert len(denied_logs) == 15
    logging.getLogger("test").info(
        "concurrent_transition_race run_id=%s accepted=%d rejected=%d final_state=queued",
        run.id, len(accepted), len(outcomes) - 1,
    )


def test_concurrent_end_to_end_state_machine(service):
    run = _run(service)
    barrier = threading.Barrier(8)

    def worker(i: int):
        barrier.wait()
        path = ["queued", "running", "succeeded"] if i % 2 == 0 else [
            "queued", "running", "failed"]
        codes = []
        for target in path:
            try:
                service.transition_run(run.id, target)
                codes.append(f"{target}:accepted")
            except (InvalidTransitionError, TerminalWriteError) as exc:
                codes.append(f"{target}:{exc.code}")
        return codes

    with ThreadPoolExecutor(max_workers=8) as pool:
        all_codes = list(pool.map(worker, range(8)))

    final = service.get_run(run.id)
    assert final.state in {"succeeded", "failed"}
    assert final.terminal is True
    # Every accepted queued/running transition must be unique: count successes.
    accepted_steps = sum(c.endswith(":accepted") for codes in all_codes for c in codes)
    assert accepted_steps == 3, all_codes
    logging.getLogger("test").info(
        "concurrent_state_machine run_id=%s final_state=%s accepted_steps=3",
        run.id, final.state,
    )


def test_concurrent_idempotent_submission_single_run(service):
    exp = service.create_experiment(ExperimentCreate(name="e"))
    barrier = threading.Barrier(12)

    def submit(_):
        barrier.wait()
        return service.submit_run(RunCreate(
            experiment_id=exp.id, idempotency_key="race-key",
            env={"py": "3.14"}, config={"lr": 0.1}))

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(submit, range(12)))

    run_ids = {r[0].id for r in results}
    assert len(run_ids) == 1
    created_count = sum(1 for _, created in results if created)
    assert created_count == 1
    assert len(service.list_runs(exp.id)) == 1


def test_concurrent_same_content_registration_dedupes(make_service):
    service = make_service(retention=0)
    barrier = threading.Barrier(10)

    def register(_):
        barrier.wait()
        return service.register_artifact(
            b"identical-bytes", ArtifactRegister(id="m@v1"))

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(register, range(10)))

    ids = {v.id for v, _ in results}
    assert ids == {"m@v1"}
    created = sum(1 for _, c in results if c)
    assert created == 1
    # Total bytes counted once, not double counted.
    assert service.storage.live_artifact_total_bytes() == len(b"identical-bytes")


def test_concurrent_gc_and_references_no_dangling(make_service):
    service = make_service(retention=0)
    artifact, _ = service.register_artifact(b"gc-race-bytes", ArtifactRegister(id="g@v1"))
    exp = service.create_experiment(ExperimentCreate(name="e"))
    run, _ = service.submit_run(RunCreate(experiment_id=exp.id))

    errors: list[Exception] = []

    def attach(_):
        try:
            from elreg.schemas import RunArtifactAttach
            service.attach_run_artifact(
                run.id, RunArtifactAttach(artifact_id="g@v1", role="input"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def collect(_):
        try:
            service.garbage_collect()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = []
        for i in range(8):
            futures.append(pool.submit(attach, i))
            futures.append(pool.submit(collect, i))
        for future in futures:
            future.result()

    assert errors == []
    view = service.get_artifact("g@v1")
    assert view.deleted is False
    assert view.referenced_by_runs == [run.id]
    report = service.integrity_report()
    assert report["healthy"] is True
    # GC after this point must still retain the referenced artifact.
    assert "g@v1" not in service.garbage_collect().removed
    logging.getLogger("test").info(
        "concurrent_gc_attach run_id=%s artifact_id=g@v1 deleted=false dangling=false",
        run.id,
    )


def test_concurrent_gc_runs_collect_once(make_service):
    service = make_service(retention=0)
    for i in range(20):
        service.register_artifact(f"junk-{i}".encode(), ArtifactRegister(id=f"j@v{i}"))
    barrier = threading.Barrier(8)

    def collect(_):
        barrier.wait()
        return service.garbage_collect()

    with ThreadPoolExecutor(max_workers=8) as pool:
        reports = list(pool.map(collect, range(8)))

    removed_union = {aid for report in reports for aid in report.removed}
    # Each artifact is collected at most once across all GC runs.
    total_removals = sum(len(report.removed) for report in reports)
    assert total_removals == len(removed_union) == 20
    assert all(a.deleted for a in service.storage.list_artifacts(include_deleted=True))
    # Blob files gone; metadata still records tombstones (no dangling refs).
    assert service.integrity_report()["healthy"] is True
