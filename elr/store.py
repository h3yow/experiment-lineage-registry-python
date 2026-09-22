"""SQLite-backed local persistence.

The store is a single SQLite database (WAL mode). Every mutating service
operation runs inside ``BEGIN IMMEDIATE``; combined with the process-wide
write lock in the kernel this gives serializable, all-or-nothing writes
(no half-deleted artifacts, no duplicate counters).
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(id),
    idempotency_key TEXT,
    status TEXT NOT NULL,
    env TEXT NOT NULL,
    config TEXT NOT NULL,
    artifact_manifest TEXT NOT NULL DEFAULT '{}',
    submitted_fingerprint TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    ended_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_idem
    ON runs(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runs_experiment ON runs(experiment_id);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_versions (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id),
    version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(artifact_id, version),
    UNIQUE(artifact_id, content_hash)
);

-- run -> artifact version dependencies (frozen fingerprint inputs)
CREATE TABLE IF NOT EXISTS run_artifacts (
    run_id TEXT NOT NULL REFERENCES runs(id),
    artifact_version_id TEXT NOT NULL REFERENCES artifact_versions(id),
    role TEXT NOT NULL DEFAULT 'input',
    registered_at TEXT NOT NULL,
    PRIMARY KEY (run_id, artifact_version_id, role)
);
CREATE INDEX IF NOT EXISTS idx_run_artifacts_version
    ON run_artifacts(artifact_version_id);

-- explicit lineage statements only
CREATE TABLE IF NOT EXISTS lineage (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('run_run', 'run_artifact')),
    src_run_id TEXT REFERENCES runs(id),
    dst_run_id TEXT REFERENCES runs(id),
    dst_artifact_version_id TEXT REFERENCES artifact_versions(id),
    relation TEXT NOT NULL,
    registered_by TEXT NOT NULL DEFAULT 'api',
    created_at TEXT NOT NULL,
    UNIQUE(kind, src_run_id, dst_run_id, dst_artifact_version_id, relation)
);
CREATE INDEX IF NOT EXISTS idx_lineage_src ON lineage(src_run_id);
CREATE INDEX IF NOT EXISTS idx_lineage_dst_run ON lineage(dst_run_id);

CREATE TABLE IF NOT EXISTS run_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    name TEXT NOT NULL,
    value REAL NOT NULL,
    -- deterministic visibility: highest sequence wins; tie -> later row
    seq INTEGER NOT NULL,
    caliber TEXT NOT NULL DEFAULT 'default',
    created_at TEXT NOT NULL,
    UNIQUE(run_id, name, seq)
);
CREATE INDEX IF NOT EXISTS idx_metrics_lookup ON run_metrics(run_id, name);

CREATE TABLE IF NOT EXISTS run_params (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    seq INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, name, seq)
);

-- append-only audit trail (state transitions, decisions, gc actions)
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    action TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

-- artifacts bytes are deleted; rows here record pending (deferred) deletion
CREATE TABLE IF NOT EXISTS gc_deletions (
    artifact_version_id TEXT PRIMARY KEY
        REFERENCES artifact_versions(id),
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- reproducibility verdicts per run
CREATE TABLE IF NOT EXISTS repro_checks (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    reproducible INTEGER NOT NULL,
    expected_fingerprint TEXT NOT NULL,
    actual_fingerprint TEXT NOT NULL,
    divergences TEXT NOT NULL,
    checked_at TEXT NOT NULL
);
"""


def _connect(path: str) -> sqlite3.Connection:
    if path != ":memory:":
        Path(os.path.dirname(os.path.abspath(path))).mkdir(
            parents=True, exist_ok=True
        )
    conn = sqlite3.connect(
        path, timeout=30, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


class Store:
    """Thin SQLite wrapper: connection lifecycle and schema bootstrap.

    A single shared connection is used by all worker threads, guarded by a
    process-wide reentrant lock (:data:`Store._io_lock`). This is essential:
    SQLite manages transactions per connection (BEGIN/COMMIT are session
    state), and with ``isolation_level=None`` two threads sharing one
    connection can interleave their BEGIN/COMMIT and corrupt each other's
    transactions. The kernel's write transaction acquires the same lock, and
    read helpers acquire it for the duration of each statement/result scan.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._io_lock = threading.RLock()
        self._conn = _connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._io_lock, self._conn:
            self._conn.executescript(_SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @property
    def io_lock(self) -> threading.RLock:
        return self._io_lock

    def begin(self) -> sqlite3.Connection:
        """Begin an immediate write transaction (serial on SQLite)."""
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def close(self) -> None:
        with self._io_lock:
            self._conn.close()

    # ---- tiny json helpers -------------------------------------------------
    @staticmethod
    def dumps(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def loads(raw: str | None) -> Any:
        return json.loads(raw) if raw else None
