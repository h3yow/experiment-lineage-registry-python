"""HTTP request/response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ExperimentCreate(BaseModel):
    id: str | None = None
    name: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExperimentView(BaseModel):
    id: str
    name: str
    created_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunCreate(BaseModel):
    experiment_id: str
    idempotency_key: str | None = None
    env: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    # artifact_version_id list declared as input dependencies at submit time
    artifacts: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunTransition(BaseModel):
    state: str


class RunView(BaseModel):
    id: str
    experiment_id: str
    idempotency_key: str | None
    state: str
    created_at: str
    updated_at: str
    env: dict[str, Any]
    config: dict[str, Any]
    fingerprint: str
    artifacts: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    terminal: bool = False


class ArtifactRegister(BaseModel):
    id: str | None = None      # versioned identifier; auto-derived if omitted
    filename: str = ""
    media_type: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactView(BaseModel):
    id: str
    sha256: str
    size_bytes: int
    created_at: str
    filename: str
    media_type: str
    deleted: bool
    referenced_by_runs: list[str] = Field(default_factory=list)


class RunArtifactAttach(BaseModel):
    artifact_id: str
    role: str = "input"


class LineageCreate(BaseModel):
    upstream_run_id: str
    downstream_run_id: str
    relation: str = "depends_on"


class LineageView(BaseModel):
    id: int
    upstream_run_id: str
    downstream_run_id: str
    relation: str
    registered_at: str


class MetricWrite(BaseModel):
    name: str
    value: float
    step: int | None = None


class MetricView(BaseModel):
    id: int
    run_id: str
    name: str
    value: float
    step: int | None
    recorded_at: str


class MetricComparisonRequest(BaseModel):
    baseline_run_id: str
    candidate_run_id: str
    # name -> {"direction": "maximize|minimize", "tolerance": float}
    metrics: dict[str, dict[str, Any]]


class MetricComparisonResult(BaseModel):
    baseline_run_id: str
    candidate_run_id: str
    results: dict[str, dict[str, Any]]
    summary: dict[str, Any]


class ReproduceRequest(BaseModel):
    env: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[str] = Field(default_factory=list)


class ReproduceResult(BaseModel):
    run_id: str
    expected_fingerprint: str
    actual_fingerprint: str
    reproducible: bool
    divergence: list[dict[str, Any]] = Field(default_factory=list)
    attempted_at: str


class GCResult(BaseModel):
    removed: list[str]
    retained: list[dict[str, str]]
    rejected: list[dict[str, str]]
    reclaimed_bytes: int


# -- derivation -------------------------------------------------------------
class ArtifactReplacement(BaseModel):
    artifact_id: str       # dependency of the source run being replaced
    replacement_id: str    # live artifact that takes its place


class DeriveRunCreate(BaseModel):
    # Client-supplied identity token; scoped per source run. Same key with a
    # different replacement request is rejected (409), never silently forked.
    idempotency_key: str | None = None
    # By default the child belongs to the source run's experiment.
    experiment_id: str | None = None
    env_overrides: dict[str, Any] = Field(default_factory=dict)
    config_overrides: dict[str, Any] = Field(default_factory=dict)
    env_remove: list[str] = Field(default_factory=list)
    config_remove: list[str] = Field(default_factory=list)
    add_artifacts: list[str] = Field(default_factory=list)
    remove_artifacts: list[str] = Field(default_factory=list)
    replace_artifacts: list[ArtifactReplacement] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DerivationView(BaseModel):
    id: int
    parent_run_id: str
    run_id: str
    idempotency_key: str | None
    request: dict[str, Any]
    changes: list[dict[str, Any]] = Field(default_factory=list)
    derived_at: str


class DeriveRunResult(BaseModel):
    run: RunView
    derivation: DerivationView
    created: bool


class DescendantsPage(BaseModel):
    run_id: str
    descendants: list[RunView]
    next_cursor: str | None = None
    truncated: bool = False
    limit: int


class AncestorsPage(BaseModel):
    run_id: str
    ancestors: list[RunView]
    truncated: bool = False
    limit: int
