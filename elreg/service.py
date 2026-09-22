"""Core registry service: all business rules live here.

A process-wide RLock guards every read-modify-write sequence so concurrent
HTTP requests cannot produce torn writes, double counts or state overshoot.
SQLite transactions give atomicity of multi-row changes; blob files are
written before the metadata commit.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .errors import (
    CapacityDelayedError,
    CapacityRejectedError,
    ConflictError,
    DanglingReferenceError,
    IdempotencyConflictError,
    InvalidTransitionError,
    LineageCycleError,
    MetricComparisonError,
    NotFoundError,
    ReproducibilityMismatchError,
    TerminalWriteError,
)
from .fingerprint import (
    DEFAULT_VOLATILE_KEYS,
    canonicalize,
    diff_fingerprint_inputs,
    fingerprint_run_inputs,
    sha256_bytes,
)
from .logging_setup import get_logger
from .models import (
    Artifact,
    Experiment,
    LineageEdge,
    MetricPoint,
    ReproductionAttempt,
    RUN_STATES,
    RUN_TRANSITIONS,
    TERMINAL_STATES,
    Run,
    RunArtifact,
)
from .schemas import (
    ArtifactRegister,
    ArtifactView,
    ExperimentCreate,
    ExperimentView,
    GCResult,
    LineageCreate,
    LineageView,
    MetricComparisonRequest,
    MetricComparisonResult,
    MetricView,
    MetricWrite,
    ReproduceRequest,
    ReproduceResult,
    RunArtifactAttach,
    RunCreate,
    RunView,
)
from .storage import Storage

log = get_logger("service")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _loads(raw: str) -> Any:
    return json.loads(raw) if raw else {}


class RegistryService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.storage = Storage(settings.db_path, settings.artifact_dir)
        self._lock = threading.RLock()
        # Signaled after a GC cycle frees capacity; used by policy="delay".
        self._capacity_cv = threading.Condition(self._lock)
        self._closed = False

    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("service already closed")
            Path(self.settings.data_dir).mkdir(parents=True, exist_ok=True)
            self.storage.initialize()
            log.info("service_started", extra={"kv": {"db": self.settings.db_path}})

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self.storage.close()
                self._closed = True
                log.info("service_closed", extra={"kv": {}})

    # ======================================================================
    # Experiments
    # ======================================================================
    def create_experiment(self, req: ExperimentCreate) -> ExperimentView:
        experiment_id = req.id or _new_id("exp")
        experiment = Experiment(
            id=experiment_id,
            name=req.name,
            created_at=_utcnow(),
            metadata_json=canonicalize(req.metadata).decode("utf-8"),
        )
        with self._lock, self.storage.begin():
            if self.storage.get_experiment(experiment_id) is not None:
                raise ConflictError(
                    f"experiment {experiment_id!r} already exists",
                    code="experiment_exists",
                )
            self.storage.create_experiment(experiment)
        log.info(
            "experiment_created",
            extra={"kv": {"experiment_id": experiment_id, "name": req.name}},
        )
        return self.get_experiment(experiment_id)

    def get_experiment(self, experiment_id: str) -> ExperimentView:
        with self._lock:
            experiment = self.storage.get_experiment(experiment_id)
            if experiment is None:
                raise NotFoundError(
                    f"experiment {experiment_id!r} not found",
                    code="experiment_not_found",
                )
            return ExperimentView(
                id=experiment.id,
                name=experiment.name,
                created_at=experiment.created_at,
                metadata=_loads(experiment.metadata_json),
            )

    def list_experiments(self) -> list[ExperimentView]:
        with self._lock:
            return [
                ExperimentView(
                    id=experiment.id,
                    name=experiment.name,
                    created_at=experiment.created_at,
                    metadata=_loads(experiment.metadata_json),
                )
                for experiment in self.storage.list_experiments()
            ]

    # ======================================================================
    # Runs: idempotent submission + state machine
    # ======================================================================
    def _resolve_artifact_shas(self, artifact_ids: list[str]) -> list[str]:
        """Map versioned artifact ids to content shas; reject dangling refs."""
        shas: list[str] = []
        missing: list[str] = []
        deleted: list[str] = []
        for artifact_id in artifact_ids:
            artifact = self.storage.get_artifact(artifact_id)
            if artifact is None:
                missing.append(artifact_id)
            elif artifact.deleted:
                deleted.append(artifact_id)
            else:
                shas.append(artifact.sha256)
        if missing:
            raise DanglingReferenceError(
                f"artifacts do not exist: {missing}",
                code="artifact_not_found",
            )
        if deleted:
            raise DanglingReferenceError(
                f"artifacts have been garbage collected: {deleted}",
                code="artifact_deleted",
            )
        return shas

    def submit_run(self, req: RunCreate) -> tuple[RunView, bool]:
        with self._lock:
            experiment = self.storage.get_experiment(req.experiment_id)
            if experiment is None:
                raise NotFoundError(
                    f"experiment {req.experiment_id!r} not found",
                    code="experiment_not_found",
                )

            artifact_ids = list(req.artifacts)
            duplicate_ids = {a for a in artifact_ids if artifact_ids.count(a) > 1}
            if duplicate_ids:
                raise ConflictError(
                    f"duplicate artifact dependencies in submission: {sorted(duplicate_ids)}",
                    code="duplicate_artifact_dependency",
                )

            existing: Run | None = None
            if req.idempotency_key is not None:
                existing = self.storage.get_run_by_idempotency(
                    req.experiment_id, req.idempotency_key
                )

            if existing is not None:
                # Idempotent replay must land on the same run; divergent
                # payloads are explicitly rejected, never overwritten.
                self._assert_same_submission(existing, req, artifact_ids)
                log.info(
                    "run_idempotent_replay",
                    extra={"kv": {"run_id": existing.id, "idempotency_key": req.idempotency_key}},
                )
                return self._run_view(existing), False

            # New submission: validate dependencies under the same lock.
            shas = self._resolve_artifact_shas(artifact_ids)
            now = _utcnow()
            run_id = _new_id("run")
            fingerprint = fingerprint_run_inputs(req.env, req.config, shas)
            run = Run(
                id=run_id,
                experiment_id=req.experiment_id,
                idempotency_key=req.idempotency_key,
                state="created",
                created_at=now,
                updated_at=now,
                env_json=canonicalize(req.env).decode("utf-8"),
                config_json=canonicalize(req.config).decode("utf-8"),
                fingerprint=fingerprint,
                metadata_json=canonicalize(req.metadata).decode("utf-8"),
            )
            with self.storage.begin():
                self.storage.create_run(run)
                for artifact_id in artifact_ids:
                    self.storage.add_run_artifact(
                        RunArtifact(
                            run_id=run_id,
                            artifact_id=artifact_id,
                            role="input",
                            registered_at=now,
                        )
                    )
            log.info(
                "run_submitted",
                extra={
                    "kv": {
                        "run_id": run_id,
                        "experiment_id": req.experiment_id,
                        "idempotency_key": req.idempotency_key or "",
                        "fingerprint": fingerprint,
                    }
                },
            )
            return self._run_view(run), True

    def _assert_same_submission(
        self, existing: Run, req: RunCreate, artifact_ids: list[str]
    ) -> None:
        # Compare under the same canonicalization rules as fingerprints:
        # volatile timestamp fields cannot make two submissions differ.
        mismatch: dict[str, str] = {}
        if canonicalize(req.env, DEFAULT_VOLATILE_KEYS) != canonicalize(
            _loads(existing.env_json), DEFAULT_VOLATILE_KEYS
        ):
            mismatch["env"] = "environment differs from original submission"
        if canonicalize(req.config, DEFAULT_VOLATILE_KEYS) != canonicalize(
            _loads(existing.config_json), DEFAULT_VOLATILE_KEYS
        ):
            mismatch["config"] = "config differs from original submission"
        if canonicalize(req.metadata, DEFAULT_VOLATILE_KEYS) != canonicalize(
            _loads(existing.metadata_json), DEFAULT_VOLATILE_KEYS
        ):
            mismatch["metadata"] = "metadata differs from original submission"

        prior_ids = [ref.artifact_id for ref in self.storage.artifacts_of_run(existing.id)
                     if ref.role == "input"]
        if sorted(artifact_ids) != sorted(prior_ids):
            mismatch["artifacts"] = (
                f"artifact dependencies differ: registered={sorted(prior_ids)} "
                f"replayed={sorted(artifact_ids)}"
            )
        if mismatch:
            log.info(
                "run_idempotency_conflict",
                extra={"kv": {"run_id": existing.id, "fields": ",".join(sorted(mismatch))}},
            )
            raise IdempotencyConflictError(
                f"idempotency key {req.idempotency_key!r} already used by run "
                f"{existing.id!r} with different submission: "
                + "; ".join(f"{k}: {v}" for k, v in sorted(mismatch.items())),
                code="idempotency_payload_mismatch",
            )

    def get_run(self, run_id: str) -> RunView:
        with self._lock:
            run = self._require_run(run_id)
            return self._run_view(run)

    def list_runs(self, experiment_id: str | None = None) -> list[RunView]:
        with self._lock:
            return [self._run_view(run) for run in self.storage.list_runs(experiment_id)]

    def transition_run(self, run_id: str, target: str) -> RunView:
        if target not in RUN_STATES:
            raise InvalidTransitionError(
                f"unknown target state {target!r}; valid states: {list(RUN_STATES)}",
                code="unknown_state",
            )
        with self._lock:
            run = self._require_run(run_id)
            current = run.state
            if current in TERMINAL_STATES:
                log.info(
                    "run_transition_denied_terminal",
                    extra={"kv": {"run_id": run_id, "from": current, "to": target}},
                )
                raise TerminalWriteError(
                    f"run {run_id!r} is in terminal state {current!r}; "
                    f"transition to {target!r} is irreversible",
                    code="terminal_state",
                )
            if target == current:
                log.info(
                    "run_transition_denied",
                    extra={"kv": {"run_id": run_id, "from": current, "to": target,
                                  "reason": "already_in_state"}},
                )
                raise InvalidTransitionError(
                    f"run {run_id!r} is already in state {current!r}",
                    code="already_in_state",
                )
            allowed = RUN_TRANSITIONS[current]
            if target not in allowed:
                log.info(
                    "run_transition_denied",
                    extra={"kv": {"run_id": run_id, "from": current, "to": target}},
                )
                raise InvalidTransitionError(
                    f"illegal transition {current!r} -> {target!r}; "
                    f"allowed targets: {sorted(allowed)}",
                    code="illegal_transition",
                )
            now = _utcnow()
            with self.storage.begin():
                updated = self.storage.update_run_state(run_id, target, now)
                if updated != 1:
                    # Defensive: state overshoot under concurrency.
                    raise InvalidTransitionError(
                        f"run {run_id!r} transition lost under concurrency",
                        code="concurrent_transition",
                    )
            run.state = target
            run.updated_at = now
            log.info(
                "run_transition",
                extra={"kv": {"run_id": run_id, "from": current, "to": target, "reason": "accepted"}},
            )
            return self._run_view(run)

    # ======================================================================
    # Artifacts: content-addressed registration, versions, capacity
    # ======================================================================
    def register_artifact(
        self, content: bytes, req: ArtifactRegister
    ) -> tuple[ArtifactView, bool]:
        sha = sha256_bytes(content)
        size = len(content)
        with self._lock:
            # Content dedup: identical bytes resolve to the existing live
            # artifact regardless of the requested versioned identifier.
            existing = self.storage.find_artifact_by_sha(sha)
            if existing is not None and not existing.deleted:
                if req.id is not None and req.id != existing.id:
                    raise ConflictError(
                        f"content sha256={sha} already registered as "
                        f"{existing.id!r}; identical content cannot be "
                        f"re-identified as {req.id!r}",
                        code="content_already_registered",
                    )
                log.info(
                    "artifact_dedup",
                    extra={"kv": {"artifact_id": existing.id, "sha256": sha}},
                )
                return self._artifact_view(existing), False

            if req.id is not None and self.storage.get_artifact(req.id) is not None:
                # Same versioned id with different content must not reuse it.
                prior = self.storage.get_artifact(req.id)
                raise ConflictError(
                    f"artifact id {req.id!r} already exists with different "
                    f"content (existing sha256={prior.sha256}, new sha256={sha})",
                    code="artifact_version_exists",
                )

            artifact_id = req.id or f"artifact:{sha[:16]}"
            self._ensure_capacity(size)

            now = _utcnow()
            artifact = Artifact(
                id=artifact_id,
                sha256=sha,
                size_bytes=size,
                created_at=now,
                filename=req.filename,
                media_type=req.media_type,
                metadata_json=canonicalize(req.metadata).decode("utf-8"),
            )
            with self.storage.begin():
                # Write blob first (atomic temp+rename), then commit metadata.
                self.storage.put_blob(sha, content)
                self.storage.create_artifact(artifact)
            log.info(
                "artifact_registered",
                extra={"kv": {"artifact_id": artifact_id, "sha256": sha, "size_bytes": size}},
            )
            return self._artifact_view(artifact), True

    def _ensure_capacity(self, incoming_size: int) -> None:
        """Apply the configured capacity policy before accepting bytes."""
        policy = self.settings.capacity_policy
        if self.settings.gc_on_overflow:
            # Opportunistically purge unreferenced expired artifacts first.
            self._garbage_collect_locked(reason="overflow_probe")

        def over_capacity() -> bool:
            return (
                self.storage.live_artifact_total_bytes() + incoming_size
                > self.settings.capacity_max_bytes
            )

        if not over_capacity():
            return

        if policy == "reject":
            raise CapacityRejectedError(
                f"capacity limit {self.settings.capacity_max_bytes} bytes exceeded "
                f"by incoming artifact of {incoming_size} bytes; policy=reject",
                code="capacity_exceeded",
            )
        if policy == "evict-oldest":
            # One more bounded GC attempt; retention policy still protects
            # referenced and non-expired artifacts.
            removed = self._garbage_collect_locked(reason="evict_oldest")
            if over_capacity():
                raise CapacityRejectedError(
                    "capacity still exceeded after evicting all unreferenced "
                    "expired artifacts",
                    code="capacity_exceeded_after_eviction",
                )
            if not removed and over_capacity():
                raise CapacityRejectedError(
                    "capacity exceeded and nothing is eligible for eviction",
                    code="capacity_exceeded_no_evictable",
                )
            return
        if policy == "delay":
            deadline = self.settings.capacity_wait_timeout_seconds
            if not self._capacity_cv.wait_for(
                lambda: not over_capacity(), timeout=deadline
            ):
                raise CapacityDelayedError(
                    f"capacity still exceeded after waiting {deadline}s",
                    code="capacity_wait_timeout",
                )
            return
        raise CapacityRejectedError(
            f"unsupported capacity policy {policy!r}", code="invalid_config"
        )

    def get_artifact(self, artifact_id: str) -> ArtifactView:
        with self._lock:
            return self._artifact_view(self._require_artifact(artifact_id))

    def list_artifacts(self) -> list[ArtifactView]:
        with self._lock:
            return [
                self._artifact_view(artifact)
                for artifact in self.storage.list_artifacts(include_deleted=False)
            ]

    def attach_run_artifact(self, run_id: str, req: RunArtifactAttach) -> None:
        with self._lock:
            run = self._require_run(run_id)
            if run.state in TERMINAL_STATES:
                raise TerminalWriteError(
                    f"run {run_id!r} is terminal ({run.state}); artifact "
                    f"attachments are immutable",
                    code="terminal_state",
                )
            artifact = self._require_artifact(req.artifact_id)
            if artifact.deleted:
                raise DanglingReferenceError(
                    f"artifact {req.artifact_id!r} has been garbage collected",
                    code="artifact_deleted",
                )
            if req.role not in {"input", "output"}:
                from .errors import ConfigError as _ConfigError

                raise _ConfigError(
                    f"artifact role must be 'input' or 'output', got {req.role!r}",
                    code="invalid_artifact_role",
                )
            if self.storage.run_artifact_exists(run_id, req.artifact_id, req.role):
                log.info(
                    "run_artifact_attach_idempotent",
                    extra={"kv": {"run_id": run_id, "artifact_id": req.artifact_id, "role": req.role}},
                )
                return
            with self.storage.begin():
                self.storage.add_run_artifact(
                    RunArtifact(
                        run_id=run_id,
                        artifact_id=req.artifact_id,
                        role=req.role,
                        registered_at=_utcnow(),
                    )
                )
            log.info(
                "run_artifact_attached",
                extra={"kv": {"run_id": run_id, "artifact_id": req.artifact_id, "role": req.role}},
            )

    def _artifact_view(self, artifact: Artifact) -> ArtifactView:
        refs = self.storage.runs_referencing_artifact(artifact.id)
        return ArtifactView(
            id=artifact.id,
            sha256=artifact.sha256,
            size_bytes=artifact.size_bytes,
            created_at=artifact.created_at,
            filename=artifact.filename,
            media_type=artifact.media_type,
            deleted=artifact.deleted,
            referenced_by_runs=refs,
        )

    # ======================================================================
    # Lineage
    # ======================================================================
    def add_lineage(self, req: LineageCreate) -> LineageView:
        with self._lock:
            up = self.storage.get_run(req.upstream_run_id)
            if up is None:
                raise DanglingReferenceError(
                    f"upstream run {req.upstream_run_id!r} does not exist",
                    code="upstream_run_not_found",
                )
            down = self.storage.get_run(req.downstream_run_id)
            if down is None:
                raise DanglingReferenceError(
                    f"downstream run {req.downstream_run_id!r} does not exist",
                    code="downstream_run_not_found",
                )
            if not req.relation:
                raise ConflictError("relation must be non-empty", code="invalid_relation")
            if self.storage.lineage_edge_exists(
                req.upstream_run_id, req.downstream_run_id, req.relation
            ):
                # Idempotent registration: same edge returns same record.
                existing_edges = [
                    edge
                    for edge in self.storage.lineage_downstream(req.upstream_run_id)
                    if edge.downstream_run_id == req.downstream_run_id
                    and edge.relation == req.relation
                ]
                edge = existing_edges[0]
                log.info(
                    "lineage_idempotent_replay",
                    extra={"kv": {"edge_id": edge.id, "upstream": up.id, "downstream": down.id}},
                )
                return self._edge_view(edge)

            # Cycle detection: the proposed edge makes upstream reachable
            # from downstream (i.e. downstream already (transitively) feeds
            # upstream). Self-loops are cycles too.
            if req.upstream_run_id == req.downstream_run_id or self._reaches(
                req.downstream_run_id, req.upstream_run_id
            ):
                log.info(
                    "lineage_cycle_rejected",
                    extra={"kv": {"upstream": up.id, "downstream": down.id, "relation": req.relation}},
                )
                raise LineageCycleError(
                    f"adding {up.id!r} -[{req.relation}]-> {down.id!r} would "
                    f"create a lineage cycle",
                    code="cycle_detected",
                )

            edge = LineageEdge(
                id=0,
                upstream_run_id=up.id,
                downstream_run_id=down.id,
                relation=req.relation,
                registered_at=_utcnow(),
            )
            with self.storage.begin():
                edge = self.storage.add_lineage_edge(edge)
            log.info(
                "lineage_registered",
                extra={"kv": {"edge_id": edge.id, "upstream": up.id, "downstream": down.id}},
            )
            return self._edge_view(edge)

    def _reaches(self, source_run_id: str, target_run_id: str) -> bool:
        """True if target is reachable from source following edge direction
        upstream -> downstream (i.e. source transitively feeds target)."""
        seen: set[str] = set()
        stack = [source_run_id]
        while stack:
            current = stack.pop()
            if current == target_run_id:
                return True
            if current in seen:
                continue
            seen.add(current)
            for edge in self.storage.lineage_downstream(current):
                stack.append(edge.downstream_run_id)
        return False

    def lineage_upstream(self, run_id: str) -> list[LineageView]:
        with self._lock:
            self._require_run(run_id)
            return [self._edge_view(e) for e in self.storage.lineage_upstream(run_id)]

    def lineage_downstream(self, run_id: str) -> list[LineageView]:
        with self._lock:
            self._require_run(run_id)
            return [self._edge_view(e) for e in self.storage.lineage_downstream(run_id)]

    @staticmethod
    def _edge_view(edge: LineageEdge) -> LineageView:
        return LineageView(
            id=edge.id,
            upstream_run_id=edge.upstream_run_id,
            downstream_run_id=edge.downstream_run_id,
            relation=edge.relation,
            registered_at=edge.registered_at,
        )

    # ======================================================================
    # Metrics
    # ======================================================================
    def write_metric(self, run_id: str, req: MetricWrite) -> MetricView:
        if req.value != req.value or req.value in (float("inf"), float("-inf")):
            raise ConflictError(
                "metric value must be a finite number", code="invalid_metric_value"
            )
        with self._lock:
            run = self._require_run(run_id)
            if run.state in TERMINAL_STATES:
                raise TerminalWriteError(
                    f"run {run_id!r} is terminal ({run.state}); metric writes "
                    f"are no longer accepted",
                    code="terminal_state",
                )
            existing = self.storage.get_metric_point(run_id, req.name, req.step)
            if existing is not None:
                if existing.value == req.value:
                    # Deterministic, idempotent re-write: same value is a no-op.
                    log.info(
                        "metric_write_idempotent",
                        extra={"kv": {"run_id": run_id, "metric": req.name, "step": str(req.step)}},
                    )
                    return self._metric_view(existing)
                raise ConflictError(
                    f"metric {req.name!r} at step {req.step!r} was already "
                    f"recorded as {existing.value} for run {run_id!r}; "
                    f"changing it to {req.value} would rewrite history",
                    code="metric_value_conflict",
                )
            point = MetricPoint(
                id=0,
                run_id=run_id,
                name=req.name,
                value=float(req.value),
                step=req.step,
                recorded_at=_utcnow(),
            )
            with self.storage.begin():
                point = self.storage.add_metric_point(point)
            log.info(
                "metric_written",
                extra={"kv": {"run_id": run_id, "metric": req.name, "step": str(req.step),
                              "value": req.value}},
            )
            return self._metric_view(point)

    def list_metrics(self, run_id: str, name: str | None = None) -> list[MetricView]:
        with self._lock:
            self._require_run(run_id)
            return [
                self._metric_view(p)
                for p in self.storage.list_metrics(run_id, name)
            ]

    def _resolve_metric_point(
        self, run_id: str, name: str, step: int | None
    ) -> MetricPoint | None:
        """Fetch an exact step, or apply the deterministic latest-write rule.

        When no step is given the point with the greatest step wins (a point
        with step=None is treated as step -1); ties are impossible because
        (run, name, step) is unique.
        """
        if step is not None:
            return self.storage.get_metric_point(run_id, name, step)
        points = self.storage.list_metrics(run_id, name)
        if not points:
            return None
        return max(points, key=lambda p: (-1 if p.step is None else p.step))

    def compare_metrics(self, req: MetricComparisonRequest) -> MetricComparisonResult:
        with self._lock:
            baseline = self._require_run(req.baseline_run_id)
            candidate = self._require_run(req.candidate_run_id)

            if not req.metrics:
                raise MetricComparisonError(
                    "at least one metric spec is required",
                    code="empty_metric_spec",
                )

            results: dict[str, dict[str, Any]] = {}
            counts = {"better": 0, "worse": 0, "equal": 0}
            for name in sorted(req.metrics):
                spec = req.metrics[name]
                direction = spec.get("direction", "maximize")
                if direction not in {"maximize", "minimize"}:
                    raise MetricComparisonError(
                        f"metric {name!r}: direction must be 'maximize' or "
                        f"'minimize', got {direction!r}",
                        code="invalid_direction",
                    )
                tolerance = spec.get("tolerance", 0.0)
                if not isinstance(tolerance, (int, float)) or tolerance < 0:
                    raise MetricComparisonError(
                        f"metric {name!r}: tolerance must be a non-negative number",
                        code="invalid_tolerance",
                    )
                step = spec.get("step")

                base_point = self._resolve_metric_point(baseline.id, name, step)
                cand_point = self._resolve_metric_point(candidate.id, name, step)
                # Missing values must never silently count as zero.
                if base_point is None and cand_point is None:
                    raise MetricComparisonError(
                        f"metric {name!r} missing on both baseline {baseline.id!r} "
                        f"and candidate {candidate.id!r} at step {step!r}",
                        code="metric_missing_both",
                    )
                if base_point is None:
                    raise MetricComparisonError(
                        f"metric {name!r} missing on baseline run {baseline.id!r} "
                        f"at step {step!r}; cannot compare (missing is not zero)",
                        code="metric_missing_baseline",
                    )
                if cand_point is None:
                    raise MetricComparisonError(
                        f"metric {name!r} missing on candidate run {candidate.id!r} "
                        f"at step {step!r}; cannot compare (missing is not zero)",
                        code="metric_missing_candidate",
                    )

                delta = cand_point.value - base_point.value
                if abs(delta) <= tolerance:
                    verdict = "equal"
                elif direction == "maximize":
                    verdict = "better" if delta > 0 else "worse"
                else:
                    verdict = "better" if delta < 0 else "worse"
                counts[verdict] += 1
                results[name] = {
                    "baseline_value": base_point.value,
                    "candidate_value": cand_point.value,
                    "delta": delta,
                    "direction": direction,
                    "tolerance": tolerance,
                    "step": step,
                    "verdict": verdict,
                }
                log.info(
                    "metric_compared",
                    extra={"kv": {"metric": name, "verdict": verdict,
                                  "baseline_run_id": baseline.id,
                                  "candidate_run_id": candidate.id}},
                )
            return MetricComparisonResult(
                baseline_run_id=baseline.id,
                candidate_run_id=candidate.id,
                results=results,
                summary={
                    "total": len(results),
                    **counts,
                    "regressed": counts["worse"] > 0,
                },
            )

    @staticmethod
    def _metric_view(point: MetricPoint) -> MetricView:
        return MetricView(
            id=point.id,
            run_id=point.run_id,
            name=point.name,
            value=point.value,
            step=point.step,
            recorded_at=point.recorded_at,
        )

    # ======================================================================
    # Reproducibility
    # ======================================================================
    def verify_reproducibility(
        self, run_id: str, req: ReproduceRequest
    ) -> ReproduceResult:
        with self._lock:
            run = self._require_run(run_id)
            actual_shas = self._resolve_artifact_shas(list(req.artifacts))
            expected_env = _loads(run.env_json)
            expected_config = _loads(run.config_json)
            expected_refs = [
                ref.artifact_id
                for ref in self.storage.artifacts_of_run(run.id)
                if ref.role == "input"
            ]
            expected_shas = [
                self.storage.get_artifact(aid).sha256 for aid in expected_refs
            ]

            actual_fingerprint = fingerprint_run_inputs(
                req.env, req.config, actual_shas
            )
            divergence = diff_fingerprint_inputs(
                expected_env,
                expected_config,
                expected_shas,
                req.env,
                req.config,
                actual_shas,
            )
            reproducible = (
                actual_fingerprint == run.fingerprint and not divergence
            )
            attempt = ReproductionAttempt(
                id=0,
                run_id=run.id,
                expected_fingerprint=run.fingerprint,
                actual_fingerprint=actual_fingerprint,
                reproducible=reproducible,
                divergence_json=canonicalize(divergence).decode("utf-8"),
                attempted_at=_utcnow(),
            )
            with self.storage.begin():
                attempt = self.storage.add_reproduction_attempt(attempt)
            log.info(
                "reproduction_checked",
                extra={
                    "kv": {
                        "run_id": run.id,
                        "reproducible": str(reproducible),
                        "expected_fingerprint": run.fingerprint[:12],
                        "actual_fingerprint": actual_fingerprint[:12],
                        "divergence_count": len(divergence),
                    }
                },
            )
            result = ReproduceResult(
                run_id=run.id,
                expected_fingerprint=run.fingerprint,
                actual_fingerprint=actual_fingerprint,
                reproducible=reproducible,
                divergence=divergence,
                attempted_at=attempt.attempted_at,
            )
            if not reproducible:
                # Explicitly surfaced, never silently passed. The structured
                # result is attached so the HTTP layer can render divergence.
                error = ReproducibilityMismatchError(
                    f"run {run.id!r} is not reproducible: "
                    f"{len(divergence)} diverging item(s)",
                    code="fingerprint_mismatch",
                )
                error.result = result  # type: ignore[attr-defined]
                raise error
            return result

    # ======================================================================
    # Garbage collection & integrity
    # ======================================================================
    def garbage_collect(self) -> GCResult:
        with self._lock:
            return self._gc_result(self._garbage_collect_locked(reason="manual"))

    def _garbage_collect_locked(self, *, reason: str) -> dict[str, Any]:
        """Delete unreferenced artifacts older than the retention window.

        Invariant: an artifact with at least one run reference is never
        removed. Metadata (deleted flag) is committed before the blob file is
        unlinked, and each artifact is handled in its own transaction so a
        failure on one item never leaves a half-deleted state for others.
        Returns a structured report.
        """
        now = datetime.now(timezone.utc)
        removed: list[str] = []
        retained: list[dict[str, str]] = []
        rejected: list[dict[str, str]] = []
        reclaimed = 0

        for artifact in self.storage.list_artifacts(include_deleted=False):
            refs = self.storage.runs_referencing_artifact(artifact.id)
            if refs:
                retained.append(
                    {"artifact_id": artifact.id, "reason": "referenced_by_run",
                     "run_ids": ",".join(refs)}
                )
                continue
            try:
                created = datetime.fromisoformat(artifact.created_at)
            except ValueError:
                rejected.append(
                    {"artifact_id": artifact.id, "reason": "invalid_created_at"}
                )
                continue
            age = (now - created).total_seconds()
            if age < self.settings.retention_max_age_seconds:
                retained.append(
                    {"artifact_id": artifact.id, "reason": "within_retention_window"}
                )
                continue
            ts = now.isoformat()
            with self.storage.begin():
                changed = self.storage.mark_artifact_deleted(artifact.id, ts)
                if changed != 1:
                    # Concurrent GC already handled it; treat as retained.
                    retained.append(
                        {"artifact_id": artifact.id, "reason": "concurrently_collected"}
                    )
                    continue
            # Metadata durable first; unlink blob only after commit.
            self.storage.remove_blob(artifact.sha256)
            removed.append(artifact.id)
            reclaimed += artifact.size_bytes
            log.info(
                "artifact_collected",
                extra={"kv": {"artifact_id": artifact.id, "sha256": artifact.sha256,
                              "reason": reason, "reclaimed_bytes": artifact.size_bytes}},
            )

        report = {
            "removed": removed,
            "retained": retained,
            "rejected": rejected,
            "reclaimed_bytes": reclaimed,
        }
        if removed:
            self._capacity_cv.notify_all()
        return report

    @staticmethod
    def _gc_result(report: dict[str, Any]) -> GCResult:
        return GCResult(
            removed=report["removed"],
            retained=report["retained"],
            rejected=report["rejected"],
            reclaimed_bytes=report["reclaimed_bytes"],
        )

    def integrity_report(self) -> dict[str, Any]:
        """Expose dangling references rather than silently ignoring them."""
        dangling: list[dict[str, str]] = []
        orphan_blobs: list[str] = []
        with self._lock:
            for artifact in self.storage.list_artifacts(include_deleted=True):
                blob = self.storage.blob_path(artifact.sha256)
                if not artifact.deleted and not blob.exists():
                    dangling.append(
                        {"artifact_id": artifact.id, "reason": "metadata_live_blob_missing",
                         "sha256": artifact.sha256}
                    )
            live_shas = {
                a.sha256 for a in self.storage.list_artifacts(include_deleted=False)
            }
            if self.storage.artifact_dir.exists():
                for shard in sorted(self.storage.artifact_dir.iterdir()):
                    if not shard.is_dir():
                        continue
                    for blob in sorted(shard.iterdir()):
                        if blob.name.startswith("."):
                            continue
                        if blob.name not in live_shas:
                            orphan_blobs.append(str(blob))
        return {
            "dangling_references": dangling,
            "orphan_blobs": orphan_blobs,
            "healthy": not dangling,
        }

    # ======================================================================
    # Internal helpers
    # ======================================================================
    def _require_run(self, run_id: str) -> Run:
        run = self.storage.get_run(run_id)
        if run is None:
            raise NotFoundError(f"run {run_id!r} not found", code="run_not_found")
        return run

    def _require_artifact(self, artifact_id: str) -> Artifact:
        artifact = self.storage.get_artifact(artifact_id)
        if artifact is None:
            raise NotFoundError(
                f"artifact {artifact_id!r} not found", code="artifact_not_found"
            )
        return artifact

    def _run_view(self, run: Run) -> RunView:
        refs = self.storage.artifacts_of_run(run.id)
        return RunView(
            id=run.id,
            experiment_id=run.experiment_id,
            idempotency_key=run.idempotency_key,
            state=run.state,
            created_at=run.created_at,
            updated_at=run.updated_at,
            env=_loads(run.env_json),
            config=_loads(run.config_json),
            fingerprint=run.fingerprint,
            artifacts=sorted({ref.artifact_id for ref in refs}),
            metadata=_loads(run.metadata_json),
            terminal=run.state in TERMINAL_STATES,
        )
