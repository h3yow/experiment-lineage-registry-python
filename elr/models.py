"""Domain entity dataclasses (rows mapped from the local SQLite store)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Experiment:
    id: str
    name: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Run:
    id: str
    experiment_id: str
    idempotency_key: str | None
    status: str
    env: dict[str, Any]
    config: dict[str, Any]
    submitted_fingerprint: str | None
    created_at: str
    updated_at: str
    ended_at: str | None = None


@dataclass
class Artifact:
    """An artifact name (logical identity) with content-addressed versions."""

    id: str
    name: str
    created_at: str


@dataclass
class ArtifactVersion:
    """One immutable content version.

    ``content_hash`` is the sha256 over the registered bytes; ``(artifact_id,
    content_hash)`` is unique: identical content de-duplicates to the same
    version, different content never reuses a version identifier.
    """

    id: str
    artifact_id: str
    version: int
    content_hash: str
    size_bytes: int
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    """A registered lineage statement.

    Edge kinds:
      * ``run_run``: ``src_run_id`` -> ``dst_run_id`` (src depends on dst);
      * ``run_artifact``: ``src_run_id`` consumes ``dst_artifact_version_id``.

    Lineage only exists through explicit registration records; it is never
    inferred from request payloads.
    """

    id: str
    kind: str
    src_run_id: str | None
    dst_run_id: str | None
    dst_artifact_version_id: str | None
    relation: str
    created_at: str
    registered_by: str = "api"


@dataclass
class MetricPoint:
    run_id: str
    name: str
    value: float
    sequence: int
    caliber: str
    created_at: str
