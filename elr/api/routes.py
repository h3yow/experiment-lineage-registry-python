"""HTTP routes for the experiment lineage registry.

All endpoints are thin adapters over the service modules. Domain errors carry
stable codes (see elr/errors.py) and are rendered with their matching HTTP
status by the ELRError handler in main.py.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from .. import artifacts as artifacts_svc
from .. import experiments as experiments_svc
from .. import retention as gc_svc
from .. import lineage as lineage_svc
from .. import metrics as metrics_svc
from .. import repro as repro_svc
from .. import runs as runs_svc

router = APIRouter(prefix="/api/v1")


def _kernel(request: Request):
    return request.app.state.kernel


# ---------------------------------------------------------------------------
# request bodies
# ---------------------------------------------------------------------------
class ExperimentIn(BaseModel):
    name: str
    metadata: dict | None = None


class ArtifactIn(BaseModel):
    name: str
    # base64 of artifact bytes
    content_b64: str
    metadata: dict | None = None


class ArtifactRef(BaseModel):
    name: str
    content_hash: str
    role: str = "input"


class RunIn(BaseModel):
    experiment_id: str
    idempotency_key: str | None = None
    env: dict | None = None
    config: dict | None = None
    artifacts: list[ArtifactRef] | None = None


class TransitionIn(BaseModel):
    status: str


class ManifestIn(BaseModel):
    artifacts: list[ArtifactRef]


class LineageRunEdgeIn(BaseModel):
    src_run_id: str
    dst_run_id: str
    relation: str = "depends_on"


class LineageArtifactEdgeIn(BaseModel):
    src_run_id: str
    artifact_version_id: str
    relation: str = "uses"


class ReproIn(BaseModel):
    env: dict | None = None
    config: dict | None = None
    artifacts: list[ArtifactRef] | None = None


class MetricIn(BaseModel):
    name: str
    value: float
    caliber: str = "default"


class ParamIn(BaseModel):
    name: str
    value: object = None


class CompareIn(BaseModel):
    baseline_run_id: str
    name: str


class DeleteVersionIn(BaseModel):
    defer: bool | None = None


# ---------------------------------------------------------------------------
# experiments / runs
# ---------------------------------------------------------------------------
@router.post("/experiments", status_code=201)
def create_experiment(body: ExperimentIn, request: Request):
    e = experiments_svc.create_experiment(
        _kernel(request), body.name, body.metadata
    )
    return e.__dict__


@router.get("/experiments")
def list_experiments(request: Request):
    return [e.__dict__ for e in
            experiments_svc.list_experiments(_kernel(request))]


@router.get("/experiments/{experiment_id}")
def get_experiment(experiment_id: str, request: Request):
    return experiments_svc.get_experiment(
        _kernel(request), experiment_id
    ).__dict__


@router.post("/runs", status_code=201)
def submit_run(body: RunIn, request: Request):
    run = runs_svc.submit_run(
        _kernel(request), body.experiment_id,
        idempotency_key=body.idempotency_key, env=body.env, config=body.config,
        artifacts=[a.model_dump() for a in (body.artifacts or [])],
    )
    return run.__dict__


@router.get("/runs")
def list_runs(request: Request, experiment_id: str | None = None):
    return [r.__dict__ for r in
            runs_svc.list_runs(_kernel(request), experiment_id)]


@router.get("/runs/{run_id}")
def get_run(run_id: str, request: Request):
    return runs_svc.get_run(_kernel(request), run_id).__dict__


@router.post("/runs/{run_id}/transitions")
def transition_run(run_id: str, body: TransitionIn, request: Request):
    return runs_svc.transition_run(
        _kernel(request), run_id, body.status
    ).__dict__


@router.put("/runs/{run_id}/manifest")
def replace_manifest(run_id: str, body: ManifestIn, request: Request):
    return runs_svc.update_run_manifest(
        _kernel(request), run_id,
        [a.model_dump() for a in body.artifacts],
    ).__dict__


@router.get("/runs/{run_id}/manifest")
def get_manifest(run_id: str, request: Request):
    return runs_svc.get_run_manifest(_kernel(request), run_id)


# ---------------------------------------------------------------------------
# artifacts
# ---------------------------------------------------------------------------
@router.post("/artifacts", status_code=201)
def register_artifact(body: ArtifactIn, request: Request):
    import base64

    content = base64.b64decode(body.content_b64, validate=True)
    return artifacts_svc.register_artifact(
        _kernel(request), body.name, content, body.metadata
    ).__dict__


@router.get("/artifacts/by-name/{name}/versions")
def list_artifact_versions(name: str, request: Request):
    return [v.__dict__ for v in
            artifacts_svc.list_versions(_kernel(request), name)]


@router.get("/artifacts/versions/{version_id}")
def get_artifact_version(version_id: str, request: Request):
    return artifacts_svc.get_version(_kernel(request), version_id).__dict__


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------
@router.post("/lineage/run-deps", status_code=201)
def add_run_edge(body: LineageRunEdgeIn, request: Request):
    return lineage_svc.register_run_dependency(
        _kernel(request), body.src_run_id, body.dst_run_id, body.relation
    ).__dict__


@router.post("/lineage/artifact-links", status_code=201)
def add_artifact_edge(body: LineageArtifactEdgeIn, request: Request):
    return lineage_svc.register_artifact_link(
        _kernel(request), body.src_run_id, body.artifact_version_id,
        body.relation,
    ).__dict__


@router.get("/lineage/{run_id}")
def get_lineage(run_id: str, request: Request):
    return lineage_svc.get_lineage(_kernel(request), run_id)


# ---------------------------------------------------------------------------
# reproducibility
# ---------------------------------------------------------------------------
@router.post("/runs/{run_id}/verify")
def verify_run(run_id: str, body: ReproIn, request: Request):
    return repro_svc.verify_run(
        _kernel(request), run_id, env=body.env, config=body.config,
        artifacts=[a.model_dump() for a in (body.artifacts or [])],
    )


@router.get("/runs/{run_id}/verify")
def get_verdict(run_id: str, request: Request):
    v = repro_svc.get_verdict(_kernel(request), run_id)
    return {"checked": v is not None, "verdict": v}


# ---------------------------------------------------------------------------
# metrics / params
# ---------------------------------------------------------------------------
@router.put("/runs/{run_id}/metrics/{name}")
def put_metric(run_id: str, name: str, body: MetricIn, request: Request):
    return metrics_svc.put_metric(
        _kernel(request), run_id, name, body.value, body.caliber
    )


@router.get("/runs/{run_id}/metrics/{name}")
def get_metric(run_id: str, name: str, request: Request):
    return metrics_svc.get_metric(_kernel(request), run_id, name)


@router.get("/runs/{run_id}/metrics")
def list_metrics(run_id: str, request: Request):
    return metrics_svc.list_metrics(_kernel(request), run_id)


@router.post("/runs/{run_id}/metrics/{name}/compare")
def compare_metric(run_id: str, name: str, body: CompareIn, request: Request):
    return metrics_svc.compare_metric(
        _kernel(request), run_id, body.baseline_run_id, name
    )


@router.put("/runs/{run_id}/params/{name}")
def put_param(run_id: str, name: str, body: ParamIn, request: Request):
    return metrics_svc.put_param(
        _kernel(request), run_id, name, body.value
    )


@router.get("/runs/{run_id}/params/{name}")
def get_param(run_id: str, name: str, request: Request):
    return metrics_svc.get_param(_kernel(request), run_id, name)


# ---------------------------------------------------------------------------
# gc / introspection
# ---------------------------------------------------------------------------
@router.post("/artifacts/gc/collect")
def gc_collect(request: Request, defer: bool | None = None):
    return gc_svc.collect(_kernel(request), defer=defer)

@router.post("/artifacts/versions/{version_id}/delete")
def gc_delete_version(version_id: str, body: DeleteVersionIn, request: Request):
    return gc_svc.delete_version(
        _kernel(request), version_id, defer=body.defer
    )


@router.get("/artifacts/gc/pending")
def gc_pending(request: Request):
    return {"pending": gc_svc.pending_deletions(_kernel(request))}


@router.get("/artifacts/gc/dangling")
def gc_dangling(request: Request):
    return {"dangling": gc_svc.dangling(_kernel(request))}


@router.get("/artifacts/gc/usage")
def gc_usage(request: Request):
    return gc_svc.storage_usage(_kernel(request))
