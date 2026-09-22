"""Explicit lineage registration with cycle/reference validation.

Lineage exists **only** through explicit registration records in the
``lineage`` table; it is never inferred from run payloads. Every edge is
returned with its record id and registration timestamp so conclusions can be
traced back to the statement that produced them.

Run-run edges (``src`` depends on ``dst``) form a directed graph that must
stay acyclic: an edge is rejected with ``lineage_cycle`` when a path already
exists from ``dst`` back to ``src`` (including the trivial src==dst case).
Unknown endpoints are rejected with ``lineage_reference``.
"""
from __future__ import annotations

import json
import uuid

from .errors import LineageCycleError, LineageReferenceError
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow
from .models import LineageEdge

log = get_logger()


def _edge_row(row) -> LineageEdge:
    return LineageEdge(
        id=row["id"], kind=row["kind"], src_run_id=row["src_run_id"],
        dst_run_id=row["dst_run_id"],
        dst_artifact_version_id=row["dst_artifact_version_id"],
        relation=row["relation"], created_at=row["created_at"],
        registered_by=row["registered_by"],
    )


def _run_exists(conn, run_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM runs WHERE id=?", (run_id,)
    ).fetchone() is not None


def _reaches(conn, start: str, target: str) -> bool:
    """True if a run_run path start -> ... -> target already exists."""
    seen: set[str] = set()
    stack = [start]
    while stack:
        cur = stack.pop()
        if cur == target:
            return True
        if cur in seen:
            continue
        seen.add(cur)
        rows = conn.execute(
            "SELECT dst_run_id FROM lineage WHERE kind='run_run' "
            "AND src_run_id=?",
            (cur,),
        ).fetchall()
        stack.extend(r[0] for r in rows)
    return False


def register_run_dependency(kernel: Kernel, src_run_id: str, dst_run_id: str,
                            relation: str = "depends_on") -> LineageEdge:
    if not src_run_id or not dst_run_id:
        raise LineageReferenceError(
            "both src_run_id and dst_run_id are required",
            details={"reason": "missing_endpoint"},
        )
    with kernel.write_tx() as conn:
        if not _run_exists(conn, src_run_id):
            raise LineageReferenceError(
                f"source run {src_run_id} does not exist",
                details={"run_id": src_run_id, "role": "src"})
        if not _run_exists(conn, dst_run_id):
            raise LineageReferenceError(
                f"target run {dst_run_id} does not exist",
                details={"run_id": dst_run_id, "role": "dst"})

        existing = conn.execute(
            "SELECT * FROM lineage WHERE kind='run_run' AND src_run_id=? "
            "AND dst_run_id=? AND relation=?",
            (src_run_id, dst_run_id, relation),
        ).fetchone()
        if existing is not None:
            log.info("lineage_edge_idempotent edge_id=%s kind=run_run",
                     existing["id"])
            return _edge_row(existing)

        if src_run_id == dst_run_id or _reaches(conn, dst_run_id, src_run_id):
            log.info(
                "lineage_cycle_rejected src=%s dst=%s decision=reject",
                src_run_id, dst_run_id,
            )
            record_audit(conn, src_run_id, "lineage_cycle_rejected",
                         {"src": src_run_id, "dst": dst_run_id,
                          "relation": relation})
            raise LineageCycleError(
                f"edge {src_run_id} -> {dst_run_id} would create a lineage "
                "cycle",
                details={"src_run_id": src_run_id, "dst_run_id": dst_run_id,
                         "relation": relation,
                         "basis": "path_dst_to_src_already_registered"},
            )

        edge_id = uuid.uuid4().hex
        now = utcnow()
        conn.execute(
            "INSERT INTO lineage(id,kind,src_run_id,dst_run_id,"
            "dst_artifact_version_id,relation,registered_by,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (edge_id, "run_run", src_run_id, dst_run_id, None, relation,
             "api", now),
        )
        record_audit(conn, src_run_id, "lineage_run_run_registered",
                     {"edge_id": edge_id, "dst": dst_run_id,
                      "relation": relation})
    log.info(
        "lineage_registered edge_id=%s src=%s dst=%s kind=run_run",
        edge_id, src_run_id, dst_run_id,
    )
    return get_edge(kernel, edge_id)


def register_artifact_link(kernel: Kernel, src_run_id: str,
                           artifact_version_id: str,
                           relation: str = "uses") -> LineageEdge:
    with kernel.write_tx() as conn:
        if not _run_exists(conn, src_run_id):
            raise LineageReferenceError(
                f"source run {src_run_id} does not exist",
                details={"run_id": src_run_id, "role": "src"})
        ver = conn.execute(
            "SELECT 1 FROM artifact_versions WHERE id=?",
            (artifact_version_id,),
        ).fetchone()
        if ver is None:
            raise LineageReferenceError(
                f"artifact version {artifact_version_id} does not exist",
                details={"artifact_version_id": artifact_version_id,
                         "role": "artifact_version"},
            )
        existing = conn.execute(
            "SELECT * FROM lineage WHERE kind='run_artifact' AND src_run_id=? "
            "AND dst_artifact_version_id=? AND relation=?",
            (src_run_id, artifact_version_id, relation),
        ).fetchone()
        if existing is not None:
            log.info("lineage_edge_idempotent edge_id=%s kind=run_artifact",
                     existing["id"])
            return _edge_row(existing)
        edge_id = uuid.uuid4().hex
        now = utcnow()
        conn.execute(
            "INSERT INTO lineage(id,kind,src_run_id,dst_run_id,"
            "dst_artifact_version_id,relation,registered_by,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (edge_id, "run_artifact", src_run_id, None,
             artifact_version_id, relation, "api", now),
        )
        record_audit(conn, src_run_id, "lineage_run_artifact_registered",
                     {"edge_id": edge_id,
                      "artifact_version_id": artifact_version_id,
                      "relation": relation})
    log.info(
        "lineage_registered edge_id=%s src=%s artifact_version=%s "
        "kind=run_artifact", edge_id, src_run_id, artifact_version_id,
    )
    return get_edge(kernel, edge_id)


def get_edge(kernel: Kernel, edge_id: str) -> LineageEdge:
    row = kernel.query_one("SELECT * FROM lineage WHERE id=?", (edge_id,))
    if row is None:
        from .errors import NotFoundError

        raise NotFoundError(f"lineage edge {edge_id} not found",
                            details={"edge_id": edge_id})
    return _edge_row(row)


def list_edges(kernel: Kernel) -> list[LineageEdge]:
    rows = kernel.query("SELECT * FROM lineage ORDER BY created_at,id")
    return [_edge_row(r) for r in rows]


def get_lineage(kernel: Kernel, run_id: str) -> dict:
    """Return registered up/downstream statements with provenance.

    * ``upstream_runs``  : runs this run explicitly depends on;
    * ``downstream_runs``: runs that explicitly depend on this run;
    * ``artifacts``      : artifact versions explicitly linked from this run.

    Every entry carries the edge id/created_at/relation so conclusions trace
    to registration records.
    """
    with kernel.read_lock():
        conn = kernel.store.conn
        if not _run_exists(conn, run_id):
            from .errors import NotFoundError

            raise NotFoundError(f"run {run_id} not found",
                                details={"run_id": run_id})

        def _edge_dict(r) -> dict:
            return {"edge_id": r["id"], "relation": r["relation"],
                    "registered_at": r["created_at"],
                    "registered_by": r["registered_by"]}

        upstream = [
            {**_edge_dict(r), "run_id": r["dst_run_id"]}
            for r in conn.execute(
                "SELECT * FROM lineage WHERE kind='run_run' AND src_run_id=? "
                "ORDER BY created_at,id", (run_id,)).fetchall()
        ]
        downstream = [
            {**_edge_dict(r), "run_id": r["src_run_id"]}
            for r in conn.execute(
                "SELECT * FROM lineage WHERE kind='run_run' AND dst_run_id=? "
                "ORDER BY created_at,id", (run_id,)).fetchall()
        ]
        artifacts = [
            {**_edge_dict(r),
             "artifact_version_id": r["dst_artifact_version_id"]}
            for r in conn.execute(
                "SELECT * FROM lineage WHERE kind='run_artifact' "
                "AND src_run_id=? ORDER BY created_at,id",
                (run_id,)).fetchall()
        ]
    return {"run_id": run_id, "upstream_runs": upstream,
            "downstream_runs": downstream, "artifacts": artifacts}
