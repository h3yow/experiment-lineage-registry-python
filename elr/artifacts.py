"""Content-addressed artifact registration, versioning and lookup.

Storage model
-------------
* ``artifacts`` rows are logical names; ``artifact_versions`` rows are
  immutable versions with ``(artifact_id, version)`` and
  ``(artifact_id, content_hash)`` uniqueness.
* Bytes live at ``<artifact_store>/ab/<sha256>`` and are written atomically
  (tmp file + rename). Identical content under the same name de-duplicates to
  the existing version row and stores no second blob. Different content gets
  a new monotonic version number and can never reuse an existing id/hash.
* A version is *referenced* when a run lists it in its frozen manifest
  (``run_artifacts``) or an explicit lineage statement links it
  (``lineage`` kind ``run_artifact``). Referenced versions are permanently
  retained (the GC layer enforces this).
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any

from .errors import (
    CapacityLimitReachedError,
    InvalidConfigError,
    NotFoundError,
)
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow
from .models import Artifact, ArtifactVersion

log = get_logger()


# ---------------------------------------------------------------------------
# row mapping
# ---------------------------------------------------------------------------
def _artifact_row(row) -> Artifact:
    return Artifact(id=row["id"], name=row["name"], created_at=row["created_at"])


def _version_row(row) -> ArtifactVersion:
    return ArtifactVersion(
        id=row["id"],
        artifact_id=row["artifact_id"],
        version=row["version"],
        content_hash=row["content_hash"],
        size_bytes=row["size_bytes"],
        created_at=row["created_at"],
        metadata=json.loads(row["metadata"]),
    )


# ---------------------------------------------------------------------------
# blob IO
# ---------------------------------------------------------------------------
def _blob_path(kernel: Kernel, content_hash: str) -> Path:
    return Path(kernel.settings.artifact_store) / "ab" / content_hash


def write_blob(kernel: Kernel, content_hash: str, content: bytes) -> None:
    path = _blob_path(kernel, content_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    tmp = path.with_name(f".{content_hash}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "wb") as fh:
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def remove_blob_if_orphan(kernel: Kernel, conn, content_hash: str) -> bool:
    """Delete blob file only when no version row still references the hash."""
    other = conn.execute(
        "SELECT 1 FROM artifact_versions WHERE content_hash=? LIMIT 1",
        (content_hash,),
    ).fetchone()
    path = _blob_path(kernel, content_hash)
    if other is None and path.exists():
        path.unlink()
        return True
    return False


def blob_exists(kernel: Kernel, content_hash: str) -> bool:
    return _blob_path(kernel, content_hash).exists()


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------
def get_artifact(kernel: Kernel, artifact_id: str) -> Artifact:
    row = kernel.query_one(
        "SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    if row is None:
        raise NotFoundError(
            f"artifact {artifact_id} not found",
            details={"artifact_id": artifact_id},
        )
    return _artifact_row(row)


def get_artifact_by_name(kernel: Kernel, name: str) -> Artifact:
    row = kernel.query_one(
        "SELECT * FROM artifacts WHERE name=?", (name,))
    if row is None:
        raise NotFoundError(
            f"artifact named {name!r} not found", details={"name": name}
        )
    return _artifact_row(row)


def get_version(kernel: Kernel, version_id: str) -> ArtifactVersion:
    row = kernel.query_one(
        "SELECT * FROM artifact_versions WHERE id=?", (version_id,))
    if row is None:
        raise NotFoundError(
            f"artifact version {version_id} not found",
            details={"artifact_version_id": version_id},
        )
    return _version_row(row)


def list_versions(kernel: Kernel, name: str) -> list[ArtifactVersion]:
    rows = kernel.query(
        "SELECT v.* FROM artifact_versions v JOIN artifacts a "
        "ON a.id=v.artifact_id WHERE a.name=? ORDER BY v.version",
        (name,),
    )
    return [_version_row(r) for r in rows]


def find_version(
    kernel: Kernel, name: str, content_hash: str
) -> ArtifactVersion | None:
    row = kernel.query_one(
        "SELECT v.* FROM artifact_versions v JOIN artifacts a "
        "ON a.id=v.artifact_id WHERE a.name=? AND v.content_hash=?",
        (name, content_hash),
    )
    return _version_row(row) if row else None


def is_version_referenced(kernel: Kernel, version_id: str) -> bool:
    row = kernel.query_one(
        "SELECT 1 FROM run_artifacts WHERE artifact_version_id=? LIMIT 1",
        (version_id,),
    )
    if row:
        return True
    row = kernel.query_one(
        "SELECT 1 FROM lineage WHERE kind='run_artifact' "
        "AND dst_artifact_version_id=? LIMIT 1",
        (version_id,),
    )
    return row is not None


def reference_run_ids(kernel: Kernel, version_id: str) -> list[str]:
    ids = {
        r[0] for r in kernel.query(
            "SELECT run_id FROM run_artifacts WHERE artifact_version_id=?",
            (version_id,),
        )
    }
    ids.update(
        r[0] for r in kernel.query(
            "SELECT src_run_id FROM lineage WHERE kind='run_artifact' "
            "AND dst_artifact_version_id=?",
            (version_id,),
        )
    )
    return sorted(ids)


def stored_bytes(conn) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(size_bytes),0) AS n FROM artifact_versions"
    ).fetchone()
    return int(row["n"])


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------
def _ensure_artifact(conn, name: str) -> str:
    row = conn.execute(
        "SELECT * FROM artifacts WHERE name=?", (name,)
    ).fetchone()
    if row:
        return row["id"]
    artifact_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO artifacts(id,name,created_at) VALUES (?,?,?)",
        (artifact_id, name, utcnow()),
    )
    return artifact_id


def register_artifact(
    kernel: Kernel,
    name: str,
    content: bytes,
    metadata: dict[str, Any] | None = None,
) -> ArtifactVersion:
    """Register ``content`` under logical ``name``.

    Returns the existing version when the same content was registered before
    (deduplication). New content creates a new monotonic version. Capacity is
    charged once per version row; identical bytes never double-charge.
    """
    if not isinstance(name, str) or not name.strip():
        raise InvalidConfigError("artifact name must be a non-empty string")
    if not isinstance(content, (bytes, bytearray)):
        raise InvalidConfigError("artifact content must be bytes")
    content = bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    size = len(content)
    settings = kernel.settings

    with kernel.write_tx() as conn:
        artifact_id = _ensure_artifact(conn, name)
        existing = conn.execute(
            "SELECT * FROM artifact_versions WHERE artifact_id=? "
            "AND content_hash=?",
            (artifact_id, digest),
        ).fetchone()
        if existing is not None:
            v = _version_row(existing)
            log.info(
                "artifact_dedup name=%s version_id=%s version=%d hash=%s",
                name, v.id, v.version, digest,
            )
            record_audit(conn, None, "artifact_dedup",
                         {"name": name, "version_id": v.id,
                          "content_hash": digest})
            return v

        limit = settings.max_artifact_bytes
        current = stored_bytes(conn)
        if limit and current + size > limit:
            if settings.capacity_policy == "defer":
                # Best-effort immediate reclaim, then a distinguishable recheck.
                from . import retention as _gc

                reclaimed = _gc.reclaim_within_tx(kernel, conn)
                current = stored_bytes(conn)
                if current + size > limit:
                    raise CapacityLimitReachedError(
                        "artifact capacity still exceeded after reclaiming "
                        f"{reclaimed} unreferenced versions",
                        details={"limit": limit, "current_bytes": current,
                                 "incoming_bytes": size,
                                 "reclaimed_versions": reclaimed,
                                 "reason": "capacity_after_reclaim"},
                    )
            else:
                raise CapacityLimitReachedError(
                    "artifact capacity exceeded; registration rejected",
                    details={"limit": limit, "current_bytes": current,
                             "incoming_bytes": size, "reason": "over_limit"},
                )

        ver_row = conn.execute(
            "SELECT COALESCE(MAX(version),0) AS m FROM artifact_versions "
            "WHERE artifact_id=?",
            (artifact_id,),
        ).fetchone()
        version_id = uuid.uuid4().hex
        now = utcnow()
        conn.execute(
            "INSERT INTO artifact_versions(id,artifact_id,version,"
            "content_hash,size_bytes,metadata,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (version_id, artifact_id, int(ver_row["m"]) + 1, digest, size,
             json.dumps(metadata or {}, sort_keys=True, ensure_ascii=False), now),
        )
        record_audit(conn, None, "artifact_registered",
                     {"name": name, "version_id": version_id,
                      "version": int(ver_row["m"]) + 1,
                      "content_hash": digest, "size_bytes": size})

    # outside the DB tx: atomic blob write; a missing blob is reported as a
    # dangling reference by integrity checks rather than crashing readers.
    write_blob(kernel, digest, content)
    log.info(
        "artifact_registered name=%s version_id=%s version=%d hash=%s size=%d",
        name, version_id, int(ver_row["m"]) + 1, digest, size,
    )
    return get_version(kernel, version_id)
