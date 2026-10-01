"""Derivation uniqueness across service instances sharing one local store.

Two ``RegistryService`` instances are pointed at the same ``db_path`` /
``artifact_dir`` — the same situation as multiple service processes
(``uvicorn --workers N`` or two deployments) sharing one local data dir.
Each instance has its own process lock and its own SQLite connection, so the
only thing that can keep derivations consistent is the database itself.

Scenarios pinned here:

* same source + same idempotency key + *different* replacement content,
  raced concurrently -> exactly one derived run; the loser gets a
  distinguishable ``derivation_idempotency_mismatch`` failure naming the
  commit identifier and the source run, and leaves no half-written state;
* same source + same key + identical content, raced concurrently -> both
  calls converge on the same derived run (exactly one is created);
* equivalent keyless requests raced across instances -> one derived run.
"""

from __future__ import annotations

import threading

import pytest

from elreg.errors import IdempotencyConflictError
from elreg.schemas import (
    DerivationRequest,
    ExperimentCreate,
    RunCreate,
)


def _source_run(service, exp_id: str) -> str:
    run, _ = service.submit_run(RunCreate(
        experiment_id=exp_id, idempotency_key="base", config={"lr": 0.1}))
    for target in ("queued", "running", "succeeded"):
        service.transition_run(run.id, target)
    return run.id


def _race(calls: list[tuple[object, DerivationRequest]]) -> list[tuple[str, object]]:
    """Fire every (service, request) pair at once; collect outcomes in order."""
    barrier = threading.Barrier(len(calls))
    outcomes: list[tuple[str, object] | None] = [None] * len(calls)

    def worker(index: int, service, req: DerivationRequest) -> None:
        barrier.wait()
        try:
            view, created = service.derive_run(SOURCE_ID, req)
            outcomes[index] = ("ok", (view.derived_run_id, created))
        except IdempotencyConflictError as exc:
            outcomes[index] = ("conflict", exc)

    threads = [
        threading.Thread(target=worker, args=(i, svc, req))
        for i, (svc, req) in enumerate(calls)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return [outcome for outcome in outcomes if outcome is not None]


SOURCE_ID: str  # rebound per test by _install_source


def _install_source(service, exp_id: str) -> str:
    global SOURCE_ID
    SOURCE_ID = _source_run(service, exp_id)
    return SOURCE_ID


def test_concurrent_instances_same_key_different_replacements(make_service):
    svc_a = make_service()
    svc_b = make_service()  # same data dir: second "process"
    exp = svc_a.create_experiment(ExperimentCreate(name="e")).id
    source_id = _install_source(svc_a, exp)
    source_before = svc_b.get_run(source_id)

    # Several rounds with fresh keys: the invariant must hold no matter how
    # the two instances interleave (pre-check rejection or unique-constraint
    # recovery are both acceptable — the observable outcome is identical).
    for round_no in range(8):
        key = f"iter-{round_no}"
        outcomes = _race([
            (svc_a, DerivationRequest(config={"lr": 0.2}, idempotency_key=key)),
            (svc_b, DerivationRequest(config={"lr": 0.9}, idempotency_key=key)),
        ])

        oks = [payload for kind, payload in outcomes if kind == "ok"]
        conflicts = [payload for kind, payload in outcomes if kind == "conflict"]
        assert len(oks) == 1, outcomes
        assert len(conflicts) == 1, outcomes

        derived_run_id, created = oks[0]
        assert created is True

        # The failure is distinguishable and names the commit identifier
        # and the source run that collided.
        error = conflicts[0]
        assert error.code == "derivation_idempotency_mismatch"
        assert key in str(error)
        assert source_id in str(error)

        # Exactly one derivation exists for this key, visible from *both*
        # instances; the loser wrote no run, no input refs, no lineage edge.
        descendants = svc_b.list_descendants(source_id).items
        assert len(descendants) == round_no + 1
        assert descendants[-1].run_id == derived_run_id
        runs = svc_a.list_runs(exp)
        assert len(runs) == 1 + round_no + 1  # source + one derived per round
        edges = svc_a.lineage_downstream(source_id)
        assert len([e for e in edges if e.relation == "derived_from"]) == round_no + 1

    # The source run was never modified by any of this.
    source_after = svc_a.get_run(source_id)
    assert source_after.state == source_before.state == "succeeded"
    assert source_after.fingerprint == source_before.fingerprint
    assert source_after.config == source_before.config
    assert source_after.updated_at == source_before.updated_at


def test_concurrent_instances_same_key_same_content_converge(make_service):
    svc_a = make_service()
    svc_b = make_service()
    exp = svc_a.create_experiment(ExperimentCreate(name="e")).id
    source_id = _install_source(svc_a, exp)

    outcomes = _race([
        (svc_a, DerivationRequest(config={"lr": 0.2}, idempotency_key="iter-x")),
        (svc_b, DerivationRequest(config={"lr": 0.2}, idempotency_key="iter-x")),
    ])

    oks = [payload for kind, payload in outcomes if kind == "ok"]
    assert len(oks) == 2, outcomes
    run_ids = {run_id for run_id, _ in oks}
    assert len(run_ids) == 1  # both landed on the same derived run
    assert sum(1 for _, created in oks if created) == 1  # created exactly once
    assert len(svc_a.list_descendants(source_id).items) == 1
    assert len(svc_b.list_descendants(source_id).items) == 1


def test_concurrent_instances_keyless_equivalent_requests_dedupe(make_service):
    svc_a = make_service()
    svc_b = make_service()
    exp = svc_a.create_experiment(ExperimentCreate(name="e")).id
    source_id = _install_source(svc_a, exp)

    outcomes = _race([
        (svc_a, DerivationRequest(config={"lr": 0.5})),
        (svc_b, DerivationRequest(config={"lr": 0.5})),
    ])

    oks = [payload for kind, payload in outcomes if kind == "ok"]
    assert len(oks) == 2, outcomes
    assert len({run_id for run_id, _ in oks}) == 1
    assert len(svc_b.list_descendants(source_id).items) == 1


def test_concurrent_instances_distinct_keys_stay_independent(make_service):
    svc_a = make_service()
    svc_b = make_service()
    exp = svc_a.create_experiment(ExperimentCreate(name="e")).id
    source_id = _install_source(svc_a, exp)

    outcomes = _race([
        (svc_a, DerivationRequest(config={"lr": 0.2}, idempotency_key="k-a")),
        (svc_b, DerivationRequest(config={"lr": 0.2}, idempotency_key="k-b")),
    ])

    oks = [payload for kind, payload in outcomes if kind == "ok"]
    assert len(oks) == 2, outcomes
    # Same content but different commit identifiers: two independent runs.
    assert len({run_id for run_id, _ in oks}) == 2
    assert len(svc_a.list_descendants(source_id).items) == 2


def test_legacy_derivations_table_is_migrated(tmp_path):
    """A data dir created before key-scoped uniqueness is rebuilt in place:
    existing records survive and the new semantics apply afterwards."""
    import sqlite3

    from elreg.storage import Storage

    db_path = tmp_path / "registry.db"
    artifact_dir = tmp_path / "artifacts"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE experiments (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE runs (
            id TEXT PRIMARY KEY,
            experiment_id TEXT NOT NULL REFERENCES experiments(id),
            idempotency_key TEXT, state TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            env_json TEXT NOT NULL DEFAULT '{}',
            config_json TEXT NOT NULL DEFAULT '{}',
            fingerprint TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(experiment_id, idempotency_key));
        CREATE TABLE run_derivations (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            source_run_id      TEXT NOT NULL REFERENCES runs(id),
            derived_run_id     TEXT NOT NULL REFERENCES runs(id),
            idempotency_key    TEXT,
            replacement_digest TEXT NOT NULL,
            replacements_json  TEXT NOT NULL DEFAULT '{}',
            changes_json       TEXT NOT NULL DEFAULT '[]',
            created_at         TEXT NOT NULL,
            UNIQUE(source_run_id, replacement_digest));
        INSERT INTO experiments VALUES ('exp_1', 'e', '2026-01-01T00:00:00Z', '{}');
        INSERT INTO runs (id, experiment_id, state, created_at, updated_at)
            VALUES ('run_src', 'exp_1', 'succeeded', 't', 't');
        INSERT INTO runs (id, experiment_id, state, created_at, updated_at)
            VALUES ('run_child', 'exp_1', 'created', 't', 't');
        INSERT INTO run_derivations
            (source_run_id, derived_run_id, idempotency_key,
             replacement_digest, created_at)
            VALUES ('run_src', 'run_child', 'iter-1', 'digest-a', 't');
        """
    )
    conn.close()

    storage = Storage(db_path, artifact_dir)
    storage.initialize()

    # Legacy record survived the rebuild.
    record = storage.get_derivation_by_idempotency("run_src", "iter-1")
    assert record is not None
    assert record.derived_run_id == "run_child"
    assert record.replacement_digest == "digest-a"

    # The legacy constraint is gone: identical content under a *different*
    # commit identifier is now an independent derivation.
    storage.conn.execute(
        "INSERT INTO runs (id, experiment_id, state, created_at, updated_at)"
        " VALUES ('run_child2', 'exp_1', 'created', 't', 't')"
    )
    storage.conn.execute(
        "INSERT INTO run_derivations"
        " (source_run_id, derived_run_id, idempotency_key, replacement_digest,"
        "  created_at)"
        " VALUES ('run_src', 'run_child2', 'iter-2', 'digest-a', 't')"
    )

    # The new constraint is enforced: reusing iter-1 with other content fails.
    storage.conn.execute(
        "INSERT INTO runs (id, experiment_id, state, created_at, updated_at)"
        " VALUES ('run_child3', 'exp_1', 'created', 't', 't')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        storage.conn.execute(
            "INSERT INTO run_derivations"
            " (source_run_id, derived_run_id, idempotency_key,"
            "  replacement_digest, created_at)"
            " VALUES ('run_src', 'run_child3', 'iter-1', 'digest-b', 't')"
        )
    storage.close()
