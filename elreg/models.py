"""Persistent domain entities (plain dataclasses)."""

from __future__ import annotations

from dataclasses import dataclass

# Run state machine (see README):
#   created -> queued -> running -> succeeded | failed | canceled
# created/queued/running may also move to canceled; running may move to failed.
# succeeded / failed / canceled are terminal.
RUN_STATES: tuple[str, ...] = (
    "created",
    "queued",
    "running",
    "succeeded",
    "failed",
    "canceled",
)
TERMINAL_STATES: frozenset[str] = frozenset({"succeeded", "failed", "canceled"})

# Allowed forward transitions.
RUN_TRANSITIONS: dict[str, frozenset[str]] = {
    "created": frozenset({"queued", "running", "canceled"}),
    "queued": frozenset({"running", "canceled"}),
    "running": frozenset({"succeeded", "failed", "canceled"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "canceled": frozenset(),
}


@dataclass
class Experiment:
    id: str
    name: str
    created_at: str
    metadata_json: str = "{}"


@dataclass
class Run:
    id: str
    experiment_id: str
    idempotency_key: str | None
    state: str
    created_at: str
    updated_at: str
    env_json: str = "{}"
    config_json: str = "{}"
    # Canonical fingerprint computed at registration over (env, config, deps).
    fingerprint: str = ""
    metadata_json: str = "{}"


@dataclass
class Artifact:
    id: str            # versioned identifier name + version
    sha256: str        # content digest; same content dedupes across versions
    size_bytes: int
    created_at: str
    filename: str = ""
    media_type: str = ""
    metadata_json: str = "{}"
    deleted: bool = False
    deleted_at: str | None = None


@dataclass
class RunArtifact:
    run_id: str
    artifact_id: str
    role: str          # e.g. "input" / "output"
    registered_at: str


@dataclass
class LineageEdge:
    id: int
    upstream_run_id: str
    downstream_run_id: str
    relation: str
    registered_at: str


@dataclass
class MetricPoint:
    id: int
    run_id: str
    name: str
    value: float
    step: int | None
    recorded_at: str


@dataclass
class ReproductionAttempt:
    id: int
    run_id: str
    expected_fingerprint: str
    actual_fingerprint: str
    reproducible: bool
    divergence_json: str
    attempted_at: str


@dataclass
class RunDerivation:
    id: int
    source_run_id: str
    derived_run_id: str
    idempotency_key: str | None
    # Canonical digest of the replacement set; (source, digest) is unique so
    # identical re-derivations always land on the same derived run.
    replacement_digest: str
    replacements_json: str
    changes_json: str
    created_at: str
