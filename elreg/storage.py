"""Local persistence: SQLite metadata + content-addressed blob store on disk.

SQLite is opened in WAL mode with foreign keys enforced and a busy timeout;
the service layer additionally serializes writers with a process lock.

Blob files are stored at ``<artifact_dir>/ab/<sha256>`` and written
atomically (temp file + ``os.replace``), so a concurrent reader can never
observe a partially written blob.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

from .models import (
    Artifact,
    Experiment,
    LineageEdge,
    MetricPoint,
    ReproductionAttempt,
    Run,
    RunArtifact,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS experiments (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS runs (
    id              TEXT PRIMARY KEY,
    experiment_id   TEXT NOT NULL REFERENCES experiments(id),
    idempotency_key TEXT,
    state           TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    env_json        TEXT NOT NULL DEFAULT '{}',
    config_json     TEXT NOT NULL DEFAULT '{}',
    fingerprint     TEXT NOT NULL DEFAULT '',
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    UNIQUE(experiment_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id            TEXT PRIMARY KEY,
    sha256        TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    created_at    TEXT NOT NULL,
    filename      TEXT NOT NULL DEFAULT '',
    media_type    TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    deleted       INTEGER NOT NULL DEFAULT 0,
    deleted_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha ON artifacts(sha256);

CREATE TABLE IF NOT EXISTS run_artifacts (
    run_id        TEXT NOT NULL REFERENCES runs(id),
    artifact_id   TEXT NOT NULL REFERENCES artifacts(id),
    role          TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY (run_id, artifact_id, role)
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_run_id  TEXT NOT NULL REFERENCES runs(id),
    downstream_run_id TEXT NOT NULL REFERENCES runs(id),
    relation         TEXT NOT NULL,
    registered_at    TEXT NOT NULL,
    UNIQUE(upstream_run_id, downstream_run_id, relation)
);
CREATE INDEX IF NOT EXISTS idx_lineage_up ON lineage_edges(upstream_run_id);
CREATE INDEX IF NOT EXISTS idx_lineage_down ON lineage_edges(downstream_run_id);

CREATE TABLE IF NOT EXISTS metric_points (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL REFERENCES runs(id),
    name        TEXT NOT NULL,
    value       REAL NOT NULL,
    step        INTEGER,
    recorded_at TEXT NOT NULL,
    UNIQUE(run_id, name, step)
);
CREATE INDEX IF NOT EXISTS idx_metrics_run ON metric_points(run_id, name);

CREATE TABLE IF NOT EXISTS reproduction_attempts (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id               TEXT NOT NULL REFERENCES runs(id),
    expected_fingerprint TEXT NOT NULL,
    actual_fingerprint   TEXT NOT NULL,
    reproducible         INTEGER NOT NULL,
    divergence_json      TEXT NOT NULL DEFAULT '[]',
    attempted_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_repro_run ON reproduction_attempts(run_id);
"""


class Storage:
    def __init__(self, db_path: str | Path, artifact_dir: str | Path) -> None:
        self.db_path = Path(db_path)
        self.artifact_dir = Path(artifact_dir)
        self._conn: sqlite3.Connection | None = None

    # -- lifecycle ---------------------------------------------------------
    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None,  # explicit transaction control
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Storage is not initialized")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def begin(self) -> "Transaction":
        return Transaction(self.conn)

    # -- experiments -------------------------------------------------------
    def create_experiment(self, experiment: Experiment) -> None:
        self.conn.execute(
            "INSERT INTO experiments (id, name, created_at, metadata_json) VALUES (?,?,?,?)",
            (
                experiment.id,
                experiment.name,
                experiment.created_at,
                experiment.metadata_json,
            ),
        )

    def get_experiment(self, experiment_id: str) -> Experiment | None:
        row = self.conn.execute(
            "SELECT * FROM experiments WHERE id = ?", (experiment_id,)
        ).fetchone()
        return _row_to_experiment(row) if row else None

    def list_experiments(self) -> list[Experiment]:
        rows = self.conn.execute(
            "SELECT * FROM experiments ORDER BY created_at, id"
        ).fetchall()
        return [_row_to_experiment(row) for row in rows]

    # -- runs --------------------------------------------------------------
    def create_run(self, run: Run) -> None:
        self.conn.execute(
            """INSERT INTO runs
               (id, experiment_id, idempotency_key, state, created_at, updated_at,
                env_json, config_json, fingerprint, metadata_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                run.id,
                run.experiment_id,
                run.idempotency_key,
                run.state,
                run.created_at,
                run.updated_at,
                run.env_json,
                run.config_json,
                run.fingerprint,
                run.metadata_json,
            ),
        )

    def get_run(self, run_id: str) -> Run | None:
        row = self.conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return _row_to_run(row) if row else None

    def get_run_by_idempotency(self, experiment_id: str, key: str) -> Run | None:
        row = self.conn.execute(
            "SELECT * FROM runs WHERE experiment_id = ? AND idempotency_key = ?",
            (experiment_id, key),
        ).fetchone()
        return _row_to_run(row) if row else None

    def list_runs(self, experiment_id: str | None = None) -> list[Run]:
        if experiment_id is None:
            rows = self.conn.execute(
                "SELECT * FROM runs ORDER BY created_at, id"
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM runs WHERE experiment_id = ? ORDER BY created_at, id",
                (experiment_id,),
            ).fetchall()
        return [_row_to_run(row) for row in rows]

    def update_run_state(self, run_id: str, new_state: str, updated_at: str) -> int:
        cur = self.conn.execute(
            "UPDATE runs SET state = ?, updated_at = ? WHERE id = ?",
            (new_state, updated_at, run_id),
        )
        return cur.rowcount

    # -- artifacts ---------------------------------------------------------
    def create_artifact(self, artifact: Artifact) -> None:
        self.conn.execute(
            """INSERT INTO artifacts
               (id, sha256, size_bytes, created_at, filename, media_type,
                metadata_json, deleted, deleted_at)
               VALUES (?,?,?,?,?,?,?,0,NULL)""",
            (
                artifact.id,
                artifact.sha256,
                artifact.size_bytes,
                artifact.created_at,
                artifact.filename,
                artifact.media_type,
                artifact.metadata_json,
            ),
        )

    def get_artifact(self, artifact_id: str) -> Artifact | None:
        row = self.conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        return _row_to_artifact(row) if row else None

    def find_artifact_by_sha(self, sha256: str) -> Artifact | None:
        # Prefer live over deleted, then earliest registration.
        row = self.conn.execute(
            """SELECT * FROM artifacts WHERE sha256 = ?
               ORDER BY deleted ASC, created_at ASC, id ASC LIMIT 1""",
            (sha256,),
        ).fetchone()
        return _row_to_artifact(row) if row else None

    def list_artifacts(self, include_deleted: bool = False) -> list[Artifact]:
        sql = "SELECT * FROM artifacts"
        if not include_deleted:
            sql += " WHERE deleted = 0"
        sql += " ORDER BY created_at, id"
        return [_row_to_artifact(row) for row in self.conn.execute(sql).fetchall()]

    def mark_artifact_deleted(self, artifact_id: str, deleted_at: str) -> int:
        cur = self.conn.execute(
            "UPDATE artifacts SET deleted = 1, deleted_at = ? WHERE id = ? AND deleted = 0",
            (deleted_at, artifact_id),
        )
        return cur.rowcount

    def live_artifact_total_bytes(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) AS total FROM artifacts WHERE deleted = 0"
        ).fetchone()
        return int(row["total"])

    # -- blob store --------------------------------------------------------
    def blob_path(self, sha256: str) -> Path:
        if len(sha256) < 2:
            raise ValueError("invalid sha256")
        return self.artifact_dir / sha256[:2] / sha256

    def put_blob(self, sha256: str, data: bytes) -> Path:
        target = self.blob_path(sha256)
        if target.exists():
            # Content addressed: existing bytes are by definition identical.
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{sha256}.tmp-{os.getpid()}")
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
        return target

    def remove_blob(self, sha256: str) -> None:
        path = self.blob_path(sha256)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    # -- run/artifact references ------------------------------------------
    def add_run_artifact(self, ref: RunArtifact) -> None:
        self.conn.execute(
            """INSERT INTO run_artifacts (run_id, artifact_id, role, registered_at)
               VALUES (?,?,?,?)""",
            (ref.run_id, ref.artifact_id, ref.role, ref.registered_at),
        )

    def run_artifact_exists(self, run_id: str, artifact_id: str, role: str) -> bool:
        row = self.conn.execute(
            """SELECT 1 FROM run_artifacts
               WHERE run_id = ? AND artifact_id = ? AND role = ?""",
            (run_id, artifact_id, role),
        ).fetchone()
        return row is not None

    def artifacts_of_run(self, run_id: str) -> list[RunArtifact]:
        rows = self.conn.execute(
            """SELECT * FROM run_artifacts WHERE run_id = ?
               ORDER BY registered_at, artifact_id, role""",
            (run_id,),
        ).fetchall()
        return [_row_to_run_artifact(row) for row in rows]

    def runs_referencing_artifact(self, artifact_id: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT run_id FROM run_artifacts WHERE artifact_id = ? ORDER BY run_id",
            (artifact_id,),
        ).fetchall()
        return [row["run_id"] for row in rows]

    # -- lineage -----------------------------------------------------------
    def add_lineage_edge(self, edge: LineageEdge) -> LineageEdge:
        cur = self.conn.execute(
            """INSERT INTO lineage_edges
               (upstream_run_id, downstream_run_id, relation, registered_at)
               VALUES (?,?,?,?)""",
            (
                edge.upstream_run_id,
                edge.downstream_run_id,
                edge.relation,
                edge.registered_at,
            ),
        )
        return LineageEdge(
            id=int(cur.lastrowid),
            upstream_run_id=edge.upstream_run_id,
            downstream_run_id=edge.downstream_run_id,
            relation=edge.relation,
            registered_at=edge.registered_at,
        )

    def lineage_edge_exists(
        self, upstream_run_id: str, downstream_run_id: str, relation: str
    ) -> bool:
        row = self.conn.execute(
            """SELECT 1 FROM lineage_edges
               WHERE upstream_run_id = ? AND downstream_run_id = ? AND relation = ?""",
            (upstream_run_id, downstream_run_id, relation),
        ).fetchone()
        return row is not None

    def lineage_upstream(self, run_id: str) -> list[LineageEdge]:
        return self._lineage_query(
            "SELECT * FROM lineage_edges WHERE downstream_run_id = ?", run_id
        )

    def lineage_downstream(self, run_id: str) -> list[LineageEdge]:
        return self._lineage_query(
            "SELECT * FROM lineage_edges WHERE upstream_run_id = ?", run_id
        )

    def lineage_all(self, run_id: str) -> list[LineageEdge]:
        return self._lineage_query(
            "SELECT * FROM lineage_edges WHERE upstream_run_id = ? OR downstream_run_id = ?",
            run_id,
            run_id,
        )

    def _lineage_query(self, sql: str, *params: Any) -> list[LineageEdge]:
        rows = self.conn.execute(
            sql + " ORDER BY registered_at, id", params
        ).fetchall()
        return [_row_to_lineage_edge(row) for row in rows]

    # -- metrics -----------------------------------------------------------
    def add_metric_point(self, point: MetricPoint) -> MetricPoint:
        cur = self.conn.execute(
            """INSERT INTO metric_points (run_id, name, value, step, recorded_at)
               VALUES (?,?,?,?,?)""",
            (point.run_id, point.name, point.value, point.step, point.recorded_at),
        )
        return MetricPoint(
            id=int(cur.lastrowid),
            run_id=point.run_id,
            name=point.name,
            value=point.value,
            step=point.step,
            recorded_at=point.recorded_at,
        )

    def get_metric_point(
        self, run_id: str, name: str, step: int | None
    ) -> MetricPoint | None:
        row = self.conn.execute(
            "SELECT * FROM metric_points WHERE run_id = ? AND name = ? AND step IS ?",
            (run_id, name, step),
        ).fetchone()
        return _row_to_metric_point(row) if row else None

    def list_metrics(self, run_id: str, name: str | None = None) -> list[MetricPoint]:
        if name is None:
            rows = self.conn.execute(
                "SELECT * FROM metric_points WHERE run_id = ? ORDER BY name, step, id",
                (run_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM metric_points WHERE run_id = ? AND name = ? ORDER BY step, id",
                (run_id, name),
            ).fetchall()
        return [_row_to_metric_point(row) for row in rows]

    # -- reproduction ------------------------------------------------------
    def add_reproduction_attempt(
        self, attempt: ReproductionAttempt
    ) -> ReproductionAttempt:
        cur = self.conn.execute(
            """INSERT INTO reproduction_attempts
               (run_id, expected_fingerprint, actual_fingerprint, reproducible,
                divergence_json, attempted_at)
               VALUES (?,?,?,?,?,?)""",
            (
                attempt.run_id,
                attempt.expected_fingerprint,
                attempt.actual_fingerprint,
                1 if attempt.reproducible else 0,
                attempt.divergence_json,
                attempt.attempted_at,
            ),
        )
        return ReproductionAttempt(
            id=int(cur.lastrowid),
            run_id=attempt.run_id,
            expected_fingerprint=attempt.expected_fingerprint,
            actual_fingerprint=attempt.actual_fingerprint,
            reproducible=attempt.reproducible,
            divergence_json=attempt.divergence_json,
            attempted_at=attempt.attempted_at,
        )

    def list_reproduction_attempts(self, run_id: str) -> list[ReproductionAttempt]:
        rows = self.conn.execute(
            "SELECT * FROM reproduction_attempts WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        return [_row_to_reproduction_attempt(row) for row in rows]


class Transaction:
    """Context manager for an explicit SQLite write transaction."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._conn.execute("COMMIT")
        else:
            self._conn.execute("ROLLBACK")


# -- row mappers ------------------------------------------------------------
def _row_to_experiment(row: sqlite3.Row) -> Experiment:
    return Experiment(
        id=row["id"],
        name=row["name"],
        created_at=row["created_at"],
        metadata_json=row["metadata_json"],
    )


def _row_to_run(row: sqlite3.Row) -> Run:
    return Run(
        id=row["id"],
        experiment_id=row["experiment_id"],
        idempotency_key=row["idempotency_key"],
        state=row["state"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        env_json=row["env_json"],
        config_json=row["config_json"],
        fingerprint=row["fingerprint"],
        metadata_json=row["metadata_json"],
    )


def _row_to_artifact(row: sqlite3.Row) -> Artifact:
    return Artifact(
        id=row["id"],
        sha256=row["sha256"],
        size_bytes=int(row["size_bytes"]),
        created_at=row["created_at"],
        filename=row["filename"],
        media_type=row["media_type"],
        metadata_json=row["metadata_json"],
        deleted=bool(row["deleted"]),
        deleted_at=row["deleted_at"],
    )


def _row_to_run_artifact(row: sqlite3.Row) -> RunArtifact:
    return RunArtifact(
        run_id=row["run_id"],
        artifact_id=row["artifact_id"],
        role=row["role"],
        registered_at=row["registered_at"],
    )


def _row_to_lineage_edge(row: sqlite3.Row) -> LineageEdge:
    return LineageEdge(
        id=int(row["id"]),
        upstream_run_id=row["upstream_run_id"],
        downstream_run_id=row["downstream_run_id"],
        relation=row["relation"],
        registered_at=row["registered_at"],
    )


def _row_to_metric_point(row: sqlite3.Row) -> MetricPoint:
    return MetricPoint(
        id=int(row["id"]),
        run_id=row["run_id"],
        name=row["name"],
        value=float(row["value"]),
        step=row["step"],
        recorded_at=row["recorded_at"],
    )


def _row_to_reproduction_attempt(row: sqlite3.Row) -> ReproductionAttempt:
    return ReproductionAttempt(
        id=int(row["id"]),
        run_id=row["run_id"],
        expected_fingerprint=row["expected_fingerprint"],
        actual_fingerprint=row["actual_fingerprint"],
        reproducible=bool(row["reproducible"]),
        divergence_json=row["divergence_json"],
        attempted_at=row["attempted_at"],
    )
