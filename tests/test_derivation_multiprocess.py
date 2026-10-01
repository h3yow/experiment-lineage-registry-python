"""Multi-instance (multi-process) derivation identity on one shared local DB.

The process-local service lock cannot protect against two *service instances*
(separate processes) deriving from the same source simultaneously. These
tests spawn real OS processes, each with its own ``RegistryService`` pointed
at the same SQLite database, and force them to race at a barrier.

They deterministically reproduce the bug where the same commit id
(``idempotency_key``) with *different* replacement content produced two
derived runs, and pin the contract:

* same source + same commit id + different content -> exactly one derived
  run, the other side gets a distinguishable ``derivation_idempotency_mismatch``
  naming the commit id, the source and the exact diverging input;
* same source + same commit id + identical content -> both converge on one
  derived run (also true without a commit id, via replacement_digest);
* different commit ids + different content stay independent;
* the rejected side leaves no run / input reference / lineage record behind
  and the source run is never mutated.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import sys
from pathlib import Path

import pytest

from elreg.config import Settings
from elreg.schemas import (
    DerivationRequest,
    ExperimentCreate,
    RunCreate,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="test uses fork-based worker processes"
)


def _settings(db_path: str) -> Settings:
    data_dir = str(Path(db_path).parent)
    return Settings(
        data_dir=data_dir,
        db_path=db_path,
        artifact_dir=str(Path(data_dir) / "artifacts"),
        retention_max_age_seconds=999_999_999,
        capacity_max_bytes=1_000_000_000,
        capacity_policy="reject",
        gc_on_overflow=False,
        capacity_wait_timeout_seconds=5.0,
        derivation_page_limit=100,
        derivation_max_limit=1000,
        derivation_scan_cap=100_000,
        log_level="WARNING",
    )


def _setup_worker(payload):
    """Child process: create an experiment + a succeeded source run."""
    from elreg.service import RegistryService

    db_path = payload["db_path"]
    svc = RegistryService(_settings(db_path))
    svc.start()
    try:
        exp = svc.create_experiment(ExperimentCreate(name="mp-e"))
        source, _ = svc.submit_run(RunCreate(
            experiment_id=exp.id, idempotency_key="base", config={"lr": 0.1}))
        for target in ("queued", "running", "succeeded"):
            svc.transition_run(source.id, target)
        return source.id
    finally:
        svc.close()


def _derive_worker(payload, barrier, results, slot):
    """Child process: one racing derivation attempt against the shared DB."""
    from elreg.service import RegistryService

    svc = RegistryService(_settings(payload["db_path"]))
    svc.start()
    try:
        barrier.wait()
        req = DerivationRequest(**payload["request"])
        view, created = svc.derive_run(payload["source_run_id"], req)
        results[slot] = {
            "ok": True,
            "derived_run_id": view.derived_run_id,
            "created": created,
        }
    except Exception as exc:  # noqa: BLE001 - serialized back to the parent
        results[slot] = {
            "ok": False,
            "code": getattr(exc, "code", "unknown"),
            "message": str(exc),
        }
    finally:
        svc.close()


def _spawn_pair(db_path: str, source_run_id: str, request_a: dict,
                request_b: dict) -> list[dict]:
    """Run two derivation attempts in separate processes, released together."""
    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(2)
    with ctx.Manager() as manager:
        results = manager.dict()
        procs = [
            ctx.Process(
                target=_derive_worker,
                args=(
                    {"db_path": db_path, "source_run_id": source_run_id,
                     "request": request},
                    barrier,
                    results,
                    slot,
                ),
            )
            for slot, request in enumerate((request_a, request_b))
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=60)
            assert proc.exitcode == 0, f"worker crashed: exitcode={proc.exitcode}"
        return [results[0], results[1]]


def _new_source(tmp_path: Path) -> tuple[str, str]:
    """Fresh data dir + terminal source run created in a child process."""
    db_path = str(tmp_path / "registry.db")
    ctx = mp.get_context("fork")
    with ctx.Pool(1) as pool:
        source_run_id = pool.apply(
            _setup_worker, ({"db_path": db_path},)
        )
    return db_path, source_run_id


def _inspect(db_path: str, source_run_id: str) -> dict:
    """Open the shared DB from the parent and collect post-race invariants."""
    from elreg.service import RegistryService

    svc = RegistryService(_settings(db_path))
    svc.start()
    try:
        page = svc.list_descendants(source_run_id)
        derived_ids = [item.run_id for item in page.items]
        edges = svc.lineage_downstream(source_run_id)
        source = svc.get_run(source_run_id)
        return {
            "derived_ids": derived_ids,
            "lineage_edges": [
                (e.upstream_run_id, e.downstream_run_id, e.relation)
                for e in edges
            ],
            "source_state": source.state,
            "source_config": source.config,
        }
    finally:
        svc.close()


@pytest.mark.parametrize("round_", range(5))
def test_two_instances_same_commit_id_different_content_only_one_survives(
    tmp_path, round_
):
    """Stable repro: two instances, same source + commit id, different content.

    Exactly one derivation survives; the loser receives a distinguishable
    conflict error naming commit id, source and the exact input that differs.
    """
    db_path, source_run_id = _new_source(tmp_path)
    commit_id = f"iter-conflict-{round_}"
    results = _spawn_pair(
        db_path,
        source_run_id,
        {"config": {"lr": 0.2}, "idempotency_key": commit_id},
        {"config": {"lr": 0.9}, "idempotency_key": commit_id},
    )

    winners = [r for r in results if r["ok"]]
    losers = [r for r in results if not r["ok"]]
    assert len(winners) == 1, results
    assert len(losers) == 1, results

    loser = losers[0]
    assert loser["code"] == "derivation_idempotency_mismatch"
    message = loser["message"]
    # The error must name the commit id, the source run and the diverging
    # input (config leaf 'lr'), not a generic "conflict".
    assert commit_id in message
    assert source_run_id in message
    assert winners[0]["derived_run_id"] in message
    assert "'lr'" in message

    state = _inspect(db_path, source_run_id)
    # Exactly one derived run, one derivation lineage edge.
    assert state["derived_ids"] == [winners[0]["derived_run_id"]]
    derived_edges = [e for e in state["lineage_edges"] if e[2] == "derived_from"]
    assert derived_edges == [
        (source_run_id, winners[0]["derived_run_id"], "derived_from")
    ]
    # The source run is untouched.
    assert state["source_state"] == "succeeded"
    assert state["source_config"] == {"lr": 0.1}


@pytest.mark.parametrize("round_", range(3))
def test_two_instances_same_commit_id_same_content_converge(tmp_path, round_):
    db_path, source_run_id = _new_source(tmp_path)
    request = {"config": {"lr": 0.3}, "idempotency_key": f"iter-same-{round_}"}
    results = _spawn_pair(db_path, source_run_id, dict(request), dict(request))

    assert all(r["ok"] for r in results), results
    derived_ids = {r["derived_run_id"] for r in results}
    assert len(derived_ids) == 1, results
    # Exactly one side creates; the other replays.
    assert sorted(r["created"] for r in results) == [False, True]

    state = _inspect(db_path, source_run_id)
    assert state["derived_ids"] == [next(iter(derived_ids))]
    derived_edges = [e for e in state["lineage_edges"] if e[2] == "derived_from"]
    assert len(derived_edges) == 1


@pytest.mark.parametrize("round_", range(3))
def test_two_instances_no_commit_id_same_content_converge(tmp_path, round_):
    """Content-based (replacement_digest) dedupe must hold across processes."""
    db_path, source_run_id = _new_source(tmp_path)
    request = {"config": {"round": round_, "lr": 0.4}}
    results = _spawn_pair(db_path, source_run_id, dict(request), dict(request))

    assert all(r["ok"] for r in results), results
    derived_ids = {r["derived_run_id"] for r in results}
    assert len(derived_ids) == 1, results
    assert sorted(r["created"] for r in results) == [False, True]


def test_two_instances_different_commit_ids_different_content_are_independent(
    tmp_path,
):
    db_path, source_run_id = _new_source(tmp_path)
    results = _spawn_pair(
        db_path,
        source_run_id,
        {"config": {"lr": 0.2}, "idempotency_key": "iter-a"},
        {"config": {"lr": 0.9}, "idempotency_key": "iter-b"},
    )
    assert all(r["ok"] for r in results), results
    derived_ids = {r["derived_run_id"] for r in results}
    assert len(derived_ids) == 2, results

    state = _inspect(db_path, source_run_id)
    assert sorted(state["derived_ids"]) == sorted(derived_ids)
    derived_edges = [e for e in state["lineage_edges"] if e[2] == "derived_from"]
    assert len(derived_edges) == 2


def test_conflicting_derivation_leaves_no_orphan_input_reference(tmp_path):
    """The loser must not leave a run, run_artifacts row or lineage edge."""
    import sqlite3

    db_path, source_run_id = _new_source(tmp_path)
    _spawn_pair(
        db_path,
        source_run_id,
        {"config": {"lr": 0.2}, "idempotency_key": "iter-orphan"},
        {"config": {"lr": 0.9}, "idempotency_key": "iter-orphan"},
    )

    conn = sqlite3.connect(db_path)
    try:
        # source run + exactly one derived run
        assert conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM run_derivations"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM run_artifacts"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM lineage_edges WHERE relation = 'derived_from'"
        ).fetchone()[0] == 1
        # The single derived row carries the winning commit id exactly once.
        (key_count,) = conn.execute(
            "SELECT COUNT(*) FROM run_derivations WHERE idempotency_key = ?",
            ("iter-orphan",),
        ).fetchone()
        assert key_count == 1
    finally:
        conn.close()


def test_legacy_database_with_duplicate_commit_ids_is_migrated(tmp_path):
    """A DB written by older code may hold two rows for one (source, commit id).

    Reopening with the fixed build must keep the first derivation, drop the
    duplicate derivation row and its derivation edge (the orphan derived run
    itself is retained), build the unique partial index, and afterwards
    enforce the one-run-per-commit-id rule.
    """
    import sqlite3

    import pytest

    from elreg.errors import IdempotencyConflictError
    from elreg.service import RegistryService

    db_path = str(tmp_path / "registry.db")
    settings = _settings(db_path)

    svc = RegistryService(settings)
    svc.start()
    exp = svc.create_experiment(ExperimentCreate(name="legacy-e"))
    source, _ = svc.submit_run(RunCreate(
        experiment_id=exp.id, idempotency_key="base", config={"lr": 0.1}))
    for target in ("queued", "running", "succeeded"):
        svc.transition_run(source.id, target)
    winner, _ = svc.derive_run(
        source.id, DerivationRequest(config={"lr": 0.2}, idempotency_key="k"))
    svc.close()

    # Reshape the DB to what a pre-fix release could have left behind.
    conn = sqlite3.connect(db_path)
    conn.execute("DROP INDEX uq_deriv_source_idem")
    now = "2026-10-02T00:00:00+00:00"
    dup_run = "run_legacydup000001"
    conn.execute(
        """INSERT INTO runs (id, experiment_id, idempotency_key, state,
              created_at, updated_at, env_json, config_json, fingerprint,
              metadata_json)
           VALUES (?, ?, NULL, 'created', ?, ?, '{}', '{}', 'fp', '{}')""",
        (dup_run, exp.id, now, now),
    )
    conn.execute(
        """INSERT INTO run_derivations (source_run_id, derived_run_id,
              idempotency_key, replacement_digest, replacements_json,
              changes_json, created_at)
           VALUES (?, ?, 'k', 'differentdigest123', '{}', '[]', ?)""",
        (source.id, dup_run, now),
    )
    conn.execute(
        """INSERT INTO lineage_edges (upstream_run_id, downstream_run_id,
              relation, registered_at)
           VALUES (?, ?, 'derived_from', ?)""",
        (source.id, dup_run, now),
    )
    conn.commit()
    conn.close()

    # Reopen: migration + index creation must succeed.
    svc2 = RegistryService(_settings(db_path))
    svc2.start()
    try:
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM run_derivations WHERE idempotency_key='k'"
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name='uq_deriv_source_idem'"
            ).fetchone() is not None
            assert conn.execute(
                "SELECT COUNT(*) FROM lineage_edges WHERE relation='derived_from'"
            ).fetchone()[0] == 1
            # The duplicate derived run row is retained (no user run deleted),
            # but it is no longer wired as a derivation child.
            assert conn.execute(
                "SELECT COUNT(*) FROM runs"
            ).fetchone()[0] == 3
        finally:
            conn.close()

        # Rule enforced after migration: same id, different content rejected.
        with pytest.raises(IdempotencyConflictError) as exc:
            svc2.derive_run(
                source.id, DerivationRequest(config={"lr": 0.99}, idempotency_key="k"))
        assert exc.value.code == "derivation_idempotency_mismatch"
        # Identical content replays onto the surviving derivation.
        replay, created = svc2.derive_run(
            source.id, DerivationRequest(config={"lr": 0.2}, idempotency_key="k"))
        assert created is False
        assert replay.derived_run_id == winner.derived_run_id
    finally:
        svc2.close()


def test_two_instances_artifact_replacement_conflict_names_input(tmp_path):
    """Same commit id but a different artifact replacement target: the loser's
    error names the exact input artifact id, and only one derivation exists."""
    from elreg.schemas import ArtifactRegister
    from elreg.service import RegistryService

    db_path = str(tmp_path / "registry.db")
    settings = _settings(db_path)
    svc = RegistryService(settings)
    svc.start()
    exp = svc.create_experiment(ExperimentCreate(name="mp-art"))
    svc.register_artifact(b"aaaa", ArtifactRegister(id="a@v1"))
    svc.register_artifact(b"bbbb", ArtifactRegister(id="b@v1"))
    svc.register_artifact(b"cccc", ArtifactRegister(id="c@v1"))
    source, _ = svc.submit_run(RunCreate(
        experiment_id=exp.id, idempotency_key="base", artifacts=["a@v1"]))
    for target in ("queued", "running", "succeeded"):
        svc.transition_run(source.id, target)
    svc.close()

    results = _spawn_pair(
        db_path,
        source.id,
        {"artifact_replacements": {"a@v1": "b@v1"}, "idempotency_key": "swap"},
        {"artifact_replacements": {"a@v1": "c@v1"}, "idempotency_key": "swap"},
    )
    winners = [r for r in results if r["ok"]]
    losers = [r for r in results if not r["ok"]]
    assert len(winners) == 1 and len(losers) == 1, results
    message = losers[0]["message"]
    assert losers[0]["code"] == "derivation_idempotency_mismatch"
    assert "a@v1" in message and "b@v1" in message and "c@v1" in message

    state = _inspect(db_path, source.id)
    assert len(state["derived_ids"]) == 1
    assert state["derived_ids"] == [winners[0]["derived_run_id"]]
