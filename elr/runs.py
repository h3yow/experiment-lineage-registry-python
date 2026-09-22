"""Run registration, idempotent submission, queries and state transitions.

Idempotency contract
--------------------
``idempotency_key`` has a unique index. Re-submitting the same key:

* with the *same* normalized env/config/artifact inputs returns the original
  run (same id, same fingerprint) -- the duplicate never creates a second run;
* with *different* inputs is rejected with ``idempotency_conflict``; details
  carry both fingerprints and the divergence list, never silently overwriting
  the first run.

Freeze contract
---------------
Runs are created PENDING with a frozen artifact manifest. While PENDING the
manifest may be replaced (``update_run_manifest``); after any transition out
of PENDING the fingerprint inputs (env, config, manifest) are frozen and a
write attempt raises ``terminal_field_write``. Transitions out of a terminal
state raise ``invalid_state_transition``.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from .errors import (
    IdempotencyConflictError,
    InvalidConfigError,
    NotFoundError,
    TerminalFieldWriteError,
)
from .fingerprint import compute_fingerprint, diff_inputs
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow
from .states import RunStatus, ensure_transition, is_terminal

log = get_logger()


def _row_to_run(row):
    from .models import Run

    return Run(
        id=row["id"],
        experiment_id=row["experiment_id"],
        idempotency_key=row["idempotency_key"],
        status=row["status"],
        env=json.loads(row["env"]),
        config=json.loads(row["config"]),
        submitted_fingerprint=row["submitted_fingerprint"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        ended_at=row["ended_at"],
    )


def _resolve_manifest(conn, artifacts: list[dict]) -> dict[str, Any]:
    """Resolve [{name, content_hash, role?}] -> canonical manifest mapping.

    Every referenced version must already exist; an unresolvable reference is
    rejected at submission time, so a run is never created with a silently
    ignored dependency.
    """
    manifest: dict[str, Any] = {}
    for item in artifacts or []:
        name = item.get("name")
        content_hash = item.get("content_hash")
        role = item.get("role", "input")
        if not name or not content_hash:
            raise InvalidConfigError(
                "artifact dependency requires 'name' and 'content_hash'",
                details={"item": item},
            )
        row = conn.execute(
            "SELECT v.id FROM artifact_versions v JOIN artifacts a "
            "ON a.id=v.artifact_id WHERE a.name=? AND v.content_hash=?",
            (name, content_hash),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                f"artifact {name}@{content_hash[:12]} is not registered; "
                "cannot reference it from a run",
                details={"name": name, "content_hash": content_hash,
                         "reason": "artifact_version_not_registered"},
            )
        if name in manifest:
            raise InvalidConfigError(
                f"duplicate artifact dependency for {name!r}",
                details={"name": name},
            )
        manifest[name] = {"content_hash": content_hash, "role": role,
                          "version_id": row["id"]}
    return manifest


def _manifest_for_fingerprint(manifest: dict[str, Any]) -> dict[str, Any]:
    # version_id is derived; fingerprint covers name/hash/role only.
    return {
        name: {"content_hash": m["content_hash"], "role": m.get("role", "input")}
        for name, m in sorted(manifest.items())
    }


def get_run_manifest(kernel: Kernel, run_id: str) -> dict[str, Any]:
    row = kernel.query_one(
        "SELECT artifact_manifest FROM runs WHERE id=?", (run_id,))
    if row is None:
        raise NotFoundError(f"run {run_id} not found",
                            details={"run_id": run_id})
    return json.loads(row["artifact_manifest"])


def get_run(kernel: Kernel, run_id: str):
    row = kernel.query_one("SELECT * FROM runs WHERE id=?", (run_id,))
    if row is None:
        raise NotFoundError(f"run {run_id} not found",
                            details={"run_id": run_id})
    return _row_to_run(row)


def list_runs(kernel: Kernel, experiment_id: str | None = None) -> list:
    if experiment_id is not None:
        rows = kernel.query(
            "SELECT * FROM runs WHERE experiment_id=? ORDER BY created_at,id",
            (experiment_id,),
        )
    else:
        rows = kernel.query("SELECT * FROM runs ORDER BY created_at,id")
    return [_row_to_run(r) for r in rows]


def find_run_by_idempotency_key(kernel: Kernel, key: str):
    row = kernel.query_one(
        "SELECT * FROM runs WHERE idempotency_key=?", (key,))
    return _row_to_run(row) if row else None


def submit_run(
    kernel: Kernel,
    experiment_id: str,
    *,
    idempotency_key: str | None = None,
    env: dict | None = None,
    config: dict | None = None,
    artifacts: list[dict] | None = None,
) -> "Run":
    env = dict(env or {})
    config = dict(config or {})
    artifacts = list(artifacts or [])
    if idempotency_key is not None and not str(idempotency_key).strip():
        raise InvalidConfigError("idempotency_key must be a non-empty string")

    with kernel.write_tx() as conn:
        exp = conn.execute(
            "SELECT 1 FROM experiments WHERE id=?", (experiment_id,)
        ).fetchone()
        if exp is None:
            raise NotFoundError(
                f"experiment {experiment_id} not found",
                details={"experiment_id": experiment_id},
            )

        existing_row = None
        if idempotency_key is not None:
            existing_row = conn.execute(
                "SELECT * FROM runs WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()

        manifest = _resolve_manifest(conn, artifacts)
        fp_manifest = _manifest_for_fingerprint(manifest)
        fp = compute_fingerprint(env, config, fp_manifest)

        if existing_row is not None:
            existing = _row_to_run(existing_row)
            if existing.submitted_fingerprint == fp:
                log.info(
                    "run_idempotent_replay run_id=%s key=%s status=%s "
                    "decision=same_fingerprint",
                    existing.id, idempotency_key, existing.status,
                )
                record_audit(conn, existing.id, "run_idempotent_replay",
                             {"idempotency_key": idempotency_key,
                              "fingerprint": fp, "decision": "same_run"})
                return existing
            divergences = (
                diff_inputs(json.loads(existing_row["env"]), env, path="$.env")
                + diff_inputs(json.loads(existing_row["config"]), config,
                              path="$.config")
                + diff_inputs(json.loads(existing_row["artifact_manifest"]),
                              fp_manifest, path="$.artifacts")
            )
            log.info(
                "run_idempotency_conflict run_id=%s key=%s existing_fp=%s "
                "incoming_fp=%s decision=reject",
                existing.id, idempotency_key,
                existing.submitted_fingerprint, fp,
            )
            record_audit(conn, existing.id, "run_idempotency_conflict",
                         {"idempotency_key": idempotency_key,
                          "existing_fingerprint":
                              existing.submitted_fingerprint,
                          "incoming_fingerprint": fp,
                          "divergences": divergences})
            raise IdempotencyConflictError(
                f"idempotency_key {idempotency_key!r} was already used by run "
                f"{existing.id} with different inputs",
                details={
                    "idempotency_key": idempotency_key,
                    "existing_run_id": existing.id,
                    "existing_fingerprint": existing.submitted_fingerprint,
                    "incoming_fingerprint": fp,
                    "divergences": divergences,
                },
            )

        run_id = uuid.uuid4().hex
        now = utcnow()
        conn.execute(
            "INSERT INTO runs(id,experiment_id,idempotency_key,status,env,"
            "config,artifact_manifest,submitted_fingerprint,created_at,"
            "updated_at,ended_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, experiment_id, idempotency_key, RunStatus.PENDING.value,
             json.dumps(env, sort_keys=True, ensure_ascii=False),
             json.dumps(config, sort_keys=True, ensure_ascii=False),
             json.dumps(manifest, sort_keys=True, ensure_ascii=False),
             fp, now, now, None),
        )
        for name, m in sorted(manifest.items()):
            conn.execute(
                "INSERT INTO run_artifacts(run_id,artifact_version_id,role,"
                "registered_at) VALUES (?,?,?,?)",
                (run_id, m["version_id"], m.get("role", "input"), now),
            )
        record_audit(conn, run_id, "run_submitted",
                     {"idempotency_key": idempotency_key,
                      "fingerprint": fp,
                      "artifact_count": len(manifest)})
    log.info(
        "run_submitted run_id=%s experiment=%s key=%s status=PENDING fp=%s",
        run_id, experiment_id, idempotency_key, fp,
    )
    return get_run(kernel, run_id)


def transition_run(kernel: Kernel, run_id: str, target: str):
    with kernel.write_tx() as conn:
        row = conn.execute(
            "SELECT * FROM runs WHERE id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"run {run_id} not found",
                                details={"run_id": run_id})
        current_status = RunStatus(row["status"])
        target_status = ensure_transition(current_status, target)  # raises
        now = utcnow()
        ended = now if is_terminal(target_status) else None
        conn.execute(
            "UPDATE runs SET status=?, updated_at=?, "
            "ended_at=COALESCE(ended_at,?) WHERE id=?",
            (target_status.value, now, ended, run_id),
        )
        record_audit(conn, run_id, "run_transition",
                     {"from": current_status.value,
                      "to": target_status.value,
                      "basis": "state_machine_allowed"})
    log.info(
        "run_transition run_id=%s %s -> %s basis=allowed",
        run_id, current_status.value, target_status.value,
    )
    return get_run(kernel, run_id)


def update_run_manifest(kernel: Kernel, run_id: str,
                        artifacts: list[dict]) -> "Run":
    """Replace the artifact manifest of a PENDING run; frozen thereafter.

    Non-PENDING runs are rejected with ``terminal_field_write``.
    """
    with kernel.write_tx() as conn:
        row = conn.execute(
            "SELECT * FROM runs WHERE id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"run {run_id} not found",
                                details={"run_id": run_id})
        status = RunStatus(row["status"])
        if status != RunStatus.PENDING:
            raise TerminalFieldWriteError(
                f"run {run_id} is {status.value}; fingerprint inputs are "
                "frozen and the artifact manifest cannot be rewritten",
                details={"run_id": run_id, "status": status.value,
                         "field": "artifact_manifest"},
            )
        manifest = _resolve_manifest(conn, artifacts)
        fp_manifest = _manifest_for_fingerprint(manifest)
        fp = compute_fingerprint(json.loads(row["env"]),
                                 json.loads(row["config"]), fp_manifest)
        now = utcnow()
        conn.execute("DELETE FROM run_artifacts WHERE run_id=?", (run_id,))
        for name, m in sorted(manifest.items()):
            conn.execute(
                "INSERT INTO run_artifacts(run_id,artifact_version_id,role,"
                "registered_at) VALUES (?,?,?,?)",
                (run_id, m["version_id"], m.get("role", "input"), now),
            )
        conn.execute(
            "UPDATE runs SET artifact_manifest=?, submitted_fingerprint=?, "
            "updated_at=? WHERE id=?",
            (json.dumps(manifest, sort_keys=True, ensure_ascii=False),
             fp, now, run_id),
        )
        record_audit(conn, run_id, "run_manifest_updated",
                     {"fingerprint": fp, "artifact_count": len(manifest)})
    log.info("run_manifest_updated run_id=%s fp=%s", run_id, fp)
    return get_run(kernel, run_id)


def assert_run_writable(conn, run_id: str) -> str:
    """Guard for metric/param writes: PENDING/RUNNING only."""
    row = conn.execute("SELECT status FROM runs WHERE id=?",
                       (run_id,)).fetchone()
    if row is None:
        raise NotFoundError(f"run {run_id} not found",
                            details={"run_id": run_id})
    status = row["status"]
    if status not in (RunStatus.PENDING.value, RunStatus.RUNNING.value):
        raise TerminalFieldWriteError(
            f"run {run_id} is {status}; writes are no longer accepted",
            details={"run_id": run_id, "status": status},
        )
    return status
