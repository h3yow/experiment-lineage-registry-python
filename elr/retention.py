"""Artifact retention, capacity enforcement and garbage collection.

Rules (also documented in docs/):

* **Referenced versions are never removed.** A version is referenced when any
  run manifest (``run_artifacts``) or any explicit ``run_artifact`` lineage
  edge points to it. Every deletion re-checks this atomically inside the
  serialized write transaction, so concurrent registration/referencing can
  never produce a half-deleted artifact or a dangling run reference.
* **Two-phase collection under ``defer`` policy.** The first ``collect``
  marks unreferenced versions as pending (``gc_deletions``); a subsequent
  ``collect`` (or capacity-driven reclaim at registration) reaps pending
  rows after re-checking references. This is the documented "delay"
  behavior; ``reject`` policy collects unreferenced, non-retained versions in
  a single phase.
* **Retention window.** ``ELR_RETENTION_RUNS=N`` keeps unreferenced versions
  that are newer than the ``ended_at`` of the N-th most recently ended run;
  when fewer than N runs have ended, everything is retained (conservative).
  Pending rows are exempt: an explicitly marked version is reaped as soon as
  it is still unreferenced.
* **Capacity.** ``ELR_MAX_ARTIFACT_BYTES`` bounds summed version bytes. Under
  ``reject`` an over-capacity registration fails with
  ``capacity_limit_reached``/``over_limit``; under ``defer`` the registry
  first reaps pending rows then re-checks, failing with
  ``capacity_after_reclaim`` only if still over.
* **Dangling references are exposed, never ignored.** If a version row exists
  while its content-addressed blob is missing, ``dangling`` names every
  affected run/version; reproducibility checks fail on them.
"""
from __future__ import annotations

from .artifacts import blob_exists, reference_run_ids, remove_blob_if_orphan
from .errors import ArtifactReferencedError, NotFoundError
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow

log = get_logger()


def _referenced_ids(conn) -> set[str]:
    ids = {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT artifact_version_id FROM run_artifacts"
        ).fetchall()
    }
    ids.update(
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT dst_artifact_version_id FROM lineage "
            "WHERE kind='run_artifact'"
        ).fetchall()
    )
    return ids


def _retention_cutoff(conn, retention_runs: int) -> str | None:
    """ended_at of the N-th latest ended run; None => retain everything."""
    if retention_runs <= 0:
        return None
    rows = conn.execute(
        "SELECT ended_at FROM runs WHERE ended_at IS NOT NULL "
        "ORDER BY ended_at DESC, id DESC LIMIT ?",
        (retention_runs,),
    ).fetchall()
    if len(rows) < retention_runs:
        return None
    return rows[-1]["ended_at"]


def _unreferenced_version_ids(conn) -> set[str]:
    rows = conn.execute("SELECT id FROM artifact_versions").fetchall()
    return {r[0] for r in rows} - _referenced_ids(conn)


def _delete_versions(kernel: Kernel, conn, version_ids: list[str]) -> list[str]:
    """Delete rows + orphan blobs. Must run inside a write transaction.

    Blob removal is safe after row deletion: a future registration with the
    same bytes simply rewrites the content-addressed blob, while a reader that
    recorded this version can no longer resolve it and integrity checks report
    that explicitly (never silently).
    """
    deleted: list[str] = []
    for vid in version_ids:
        row = conn.execute(
            "SELECT content_hash FROM artifact_versions WHERE id=?", (vid,)
        ).fetchone()
        if row is None:
            continue
        conn.execute("DELETE FROM gc_deletions WHERE artifact_version_id=?",
                     (vid,))
        conn.execute("DELETE FROM artifact_versions WHERE id=?", (vid,))
        remove_blob_if_orphan(kernel, conn, row["content_hash"])
        deleted.append(vid)
    return deleted


def reclaim_within_tx(kernel: Kernel, conn) -> int:
    """Hard-delete still-unreferenced pending versions. Returns count.

    Runs inside an already open write transaction. A version that is still
    referenced stays pending (its deferred-deletion intent is preserved) and
    is only reaped once unreferenced on a later cycle.
    """
    referenced = _referenced_ids(conn)
    pending = [
        r[0]
        for r in conn.execute(
            "SELECT artifact_version_id FROM gc_deletions ORDER BY created_at,artifact_version_id"
        ).fetchall()
    ]
    reap: list[str] = []
    for vid in pending:
        exists = conn.execute(
            "SELECT 1 FROM artifact_versions WHERE id=?", (vid,)
        ).fetchone()
        if not exists:
            conn.execute("DELETE FROM gc_deletions WHERE artifact_version_id=?",
                         (vid,))
            continue
        if vid in referenced:
            # still referenced: keep the pending marker for a later cycle
            continue
        reap.append(vid)
    deleted = _delete_versions(kernel, conn, reap)
    record_audit(conn, None, "gc_reclaim", {"deleted": len(deleted)})
    if deleted:
        log.info("gc_reclaim deleted=%d pending=%d", len(deleted), len(pending))
    return len(deleted)


def collect(kernel: Kernel, *, defer: bool | None = None) -> dict:
    """Run one collection cycle. Returns an explainable decision report."""
    use_defer = (
        kernel.settings.capacity_policy == "defer" if defer is None else defer
    )
    report = {"deleted": [], "deferred": [], "retained": []}
    with kernel.write_tx() as conn:
        # phase 0: reap pending rows that still exist and are unreferenced
        pending = [
            r[0]
            for r in conn.execute(
                "SELECT artifact_version_id FROM gc_deletions ORDER BY "
                "created_at,artifact_version_id"
            ).fetchall()
        ]
        referenced = _referenced_ids(conn)
        reap: list[str] = []
        for vid in pending:
            exists = conn.execute(
                "SELECT 1 FROM artifact_versions WHERE id=?", (vid,)
            ).fetchone()
            if not exists:
                conn.execute(
                    "DELETE FROM gc_deletions WHERE artifact_version_id=?",
                    (vid,))
            elif vid in referenced:
                # pending deletion intent preserved until unreferenced
                report["retained"].append(
                    {"version_id": vid, "reason": "still_referenced_pending"})
            else:
                reap.append(vid)

        # phase 1: new unreferenced candidates, honoring the retention window
        rr = kernel.settings.retention_runs
        retention_active = rr > 0
        cutoff = _retention_cutoff(conn, rr) if retention_active else None
        pending_set = set(pending)
        rows = conn.execute(
            "SELECT id, created_at FROM artifact_versions ORDER BY created_at,id"
        ).fetchall()
        to_pend: list[str] = []
        for r in rows:
            vid = r["id"]
            if vid in referenced or vid in pending_set:
                continue
            # active window with no cutoff yet (fewer than N ended runs) or a
            # version newer than the N-th ended run => retained
            if retention_active and (
                cutoff is None or r["created_at"] > cutoff
            ):
                report["retained"].append(
                    {"version_id": vid,
                     "reason": "within_retention_window", "cutoff": cutoff})
                continue
            if use_defer:
                to_pend.append(vid)
            else:
                reap.append(vid)

        now = utcnow()
        for vid in to_pend:
            conn.execute(
                "INSERT OR IGNORE INTO gc_deletions"
                "(artifact_version_id,reason,created_at) VALUES (?,?,?)",
                (vid, "unreferenced", now),
            )
            report["deferred"].append(
                {"version_id": vid, "reason": "marked_pending_deletion"})

        deleted = _delete_versions(kernel, conn, reap)
        record_audit(conn, None, "gc_collect",
                     {"deleted": len(deleted), "deferred": len(to_pend)})
        report["deleted"] = [{"version_id": v} for v in deleted]
    log.info(
        "gc_collect deleted=%d deferred=%d retained=%d",
        len(report["deleted"]), len(report["deferred"]),
        len(report["retained"]),
    )
    return report


def delete_version(kernel: Kernel, version_id: str, *,
                   defer: bool | None = None) -> dict:
    """Explicitly request removal of one artifact version.

    Referenced versions are rejected with ``artifact_referenced`` (with the
    list of referencing runs) under the ``reject`` policy, or marked pending
    under ``defer`` -- they disappear only once unreferenced and a later
    collection reaps them.
    """
    use_defer = (
        kernel.settings.capacity_policy == "defer" if defer is None else defer
    )
    with kernel.write_tx() as conn:
        row = conn.execute(
            "SELECT * FROM artifact_versions WHERE id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(
                f"artifact version {version_id} not found",
                details={"artifact_version_id": version_id},
            )
        refs = reference_run_ids(kernel, version_id)
        if refs:
            if use_defer:
                conn.execute(
                    "INSERT OR IGNORE INTO gc_deletions"
                    "(artifact_version_id,reason,created_at) VALUES (?,?,?)",
                    (version_id, "explicit_defer", utcnow()),
                )
                record_audit(conn, None, "gc_delete_deferred",
                             {"version_id": version_id, "referenced_by": refs})
                return {"deferred": True, "version_id": version_id,
                        "referenced_by": refs}
            raise ArtifactReferencedError(
                f"artifact version {version_id} is referenced by run(s) "
                f"{refs}; removal rejected",
                details={"artifact_version_id": version_id,
                         "referenced_by": refs},
            )
        deleted = _delete_versions(kernel, conn, [version_id])
        record_audit(conn, None, "gc_delete",
                     {"version_id": version_id, "deleted": bool(deleted)})
    return {"deleted": True, "version_id": version_id}


def pending_deletions(kernel: Kernel) -> list[dict]:
    rows = kernel.query(
        "SELECT d.artifact_version_id, d.reason, d.created_at, v.content_hash "
        "FROM gc_deletions d JOIN artifact_versions v "
        "ON v.id=d.artifact_version_id "
        "ORDER BY d.created_at,d.artifact_version_id"
    )
    return [dict(r) for r in rows]


def dangling(kernel: Kernel) -> list[dict]:
    """Expose every reference whose content-addressed blob is missing."""
    manifest_refs = kernel.query(
        "SELECT ra.run_id, v.id AS version_id, v.content_hash, ra.role "
        "FROM run_artifacts ra JOIN artifact_versions v "
        "ON v.id=ra.artifact_version_id"
    )
    lineage_refs = kernel.query(
        "SELECT l.src_run_id AS run_id, "
        "l.dst_artifact_version_id AS version_id, v.content_hash, "
        "l.relation AS role FROM lineage l JOIN artifact_versions v "
        "ON v.id=l.dst_artifact_version_id WHERE l.kind='run_artifact'"
    )
    out: list[dict] = []
    for r in manifest_refs:
        if not blob_exists(kernel, r["content_hash"]):
            out.append({"run_id": r["run_id"],
                        "artifact_version_id": r["version_id"],
                        "content_hash": r["content_hash"],
                        "source": "run_manifest", "role": r["role"],
                        "reason": "blob_missing"})
    for r in lineage_refs:
        if not blob_exists(kernel, r["content_hash"]):
            out.append({"run_id": r["run_id"],
                        "artifact_version_id": r["version_id"],
                        "content_hash": r["content_hash"],
                        "source": "lineage_edge", "role": r["role"],
                        "reason": "blob_missing"})
    return out


def storage_usage(kernel: Kernel) -> dict:
    row = kernel.query_one(
        "SELECT COALESCE(SUM(size_bytes),0) bytes, COUNT(*) n "
        "FROM artifact_versions"
    )
    pending = kernel.query_one("SELECT COUNT(*) c FROM gc_deletions")
    return {
        "version_bytes": int(row["bytes"]),
        "version_count": int(row["n"]),
        "limit_bytes": kernel.settings.max_artifact_bytes,
        "pending_deletions": int(pending["c"]),
        "capacity_policy": kernel.settings.capacity_policy,
    }
