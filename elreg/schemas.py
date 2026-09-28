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
    # Set when the run was created via derivation: the immediate source run.
    derived_from: str | None = None


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


# -- derivations ------------------------------------------------------------
class DerivationRequest(BaseModel):
    # Explicit replacement of input artifact dependencies, keyed by the
    # source-run artifact id. Anything not mentioned is inherited.
    artifact_replacements: dict[str, str] = Field(default_factory=dict)
    # Artifact ids to drop from the inherited input dependency set.
    remove_artifacts: list[str] = Field(default_factory=list)
    # Deep-merged over the source run's env / config; missing paths inherit.
    env: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class InputChange(BaseModel):
    category: str          # env | config | artifact
    op: str                # added | removed | replaced
    key: str               # dotted path (env/config) or artifact id
    source_value: Any = None
    derived_value: Any = None


class DerivationView(BaseModel):
    id: int
    source_run_id: str
    derived_run_id: str
    idempotency_key: str | None
    replacement_digest: str
    # Exactly what was asked for (normalized replacement set).
    replacements: dict[str, Any] = Field(default_factory=dict)
    # Itemized added/removed/replaced inputs after inheritance.
    changes: list[InputChange] = Field(default_factory=list)
    created: bool = True
    created_at: str


class DescendantItem(BaseModel):
    run_id: str
    source_run_id: str
    depth: int
    derivation_id: int
    created_at: str


class DescendantPage(BaseModel):
    root_run_id: str
    items: list[DescendantItem] = Field(default_factory=list)
    limit: int
    offset: int
    returned: int
    # True when the scan hit the hard traversal cap or more rows remain.
    truncated: bool = False
    truncation_reason: str | None = None  # scan_cap | page_limit | null
    next_offset: int | None = None
    next_cursor: str | None = None
