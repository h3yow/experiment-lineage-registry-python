"""HTTP layer: wires FastAPI routes to the RegistryService.

Every registry failure becomes a JSON body of the form
``{"error": {"code": ..., "message": ...}}`` so callers can distinguish
failure reasons programmatically. Reproducibility failures additionally echo
the structured divergence report.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .errors import (
    RegistryError,
    ReproducibilityMismatchError,
)
from .schemas import (
    ArtifactRegister,
    ExperimentCreate,
    LineageCreate,
    MetricComparisonRequest,
    MetricWrite,
    ReproduceRequest,
    RunArtifactAttach,
    RunCreate,
    RunTransition,
    DeriveRunCreate,
)
from .service import RegistryService

# error code -> HTTP status; everything else is 400.
_STATUS_BY_CODE = {
    "not_found": 404,
    "experiment_not_found": 404,
    "run_not_found": 404,
    "artifact_not_found": 404,
    "upstream_run_not_found": 409,
    "downstream_run_not_found": 409,
    "conflict": 409,
    "experiment_exists": 409,
    "artifact_version_exists": 409,
    "content_already_registered": 409,
    "duplicate_artifact_dependency": 409,
    "invalid_relation": 400,
    "invalid_artifact_role": 400,
    "invalid_transition": 409,
    "unknown_state": 400,
    "illegal_transition": 409,
    "already_in_state": 409,
    "concurrent_transition": 409,
    "terminal_write_denied": 409,
    "terminal_state": 409,
    "idempotency_conflict": 409,
    "idempotency_payload_mismatch": 409,
    "lineage_cycle": 409,
    "cycle_detected": 409,
    "dangling_reference": 409,
    "artifact_deleted": 409,
    "metric_value_conflict": 409,
    "invalid_metric_value": 400,
    "metric_comparison_error": 422,
    "metric_missing_both": 422,
    "metric_missing_baseline": 422,
    "metric_missing_candidate": 422,
    "invalid_direction": 400,
    "invalid_tolerance": 400,
    "empty_metric_spec": 400,
    "reproducibility_mismatch": 422,
    "fingerprint_mismatch": 422,
    "capacity_rejected": 507,
    "capacity_exceeded": 507,
    "capacity_exceeded_after_eviction": 507,
    "capacity_exceeded_no_evictable": 507,
    "capacity_delayed": 503,
    "capacity_wait_timeout": 503,
    "derivation_error": 409,
    "source_run_not_terminal": 409,
    "invalid_derivation": 400,
    "derivation_payload_mismatch": 409,
    "derivation_not_found": 404,
    "invalid_pagination_limit": 400,
    "pagination_limit_exceeded": 400,
    "invalid_cursor": 400,
    "invalid_config": 500,
}


def create_app(service: RegistryService) -> FastAPI:
    app = FastAPI(
        title="Experiment Lineage Registry",
        version="0.1.0",
        description="Local experiment/run/artifact tracking with content-"
        "addressed artifacts, lineage and reproducibility verification.",
    )

    @app.exception_handler(RegistryError)
    async def _registry_error_handler(_: Request, exc: RegistryError) -> JSONResponse:
        status = _STATUS_BY_CODE.get(exc.code, 400)
        body: dict[str, object] = {
            "error": {"code": exc.code, "message": str(exc)}
        }
        if isinstance(exc, ReproducibilityMismatchError):
            body["reproduction"] = exc.result.model_dump()
        return JSONResponse(status_code=status, content=body)

    # -- backwards-compatible health route --------------------------------
    @app.get("/")
    def read_root() -> dict[str, str]:
        return {"Hello": "World", "service": "experiment-lineage-registry"}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    # -- experiments -------------------------------------------------------
    @app.post("/experiments", status_code=201)
    def create_experiment(req: ExperimentCreate):
        return service.create_experiment(req)

    @app.get("/experiments/{experiment_id}")
    def get_experiment(experiment_id: str):
        return service.get_experiment(experiment_id)

    @app.get("/experiments")
    def list_experiments():
        return service.list_experiments()

    # -- runs --------------------------------------------------------------
    @app.post("/runs", status_code=201)
    def submit_run(req: RunCreate):
        view, created = service.submit_run(req)
        return JSONResponse(
            status_code=201 if created else 200,
            content=view.model_dump() | {"created": created},
        )

    @app.get("/runs")
    def list_runs(experiment_id: str | None = None):
        return service.list_runs(experiment_id)

    @app.get("/runs/{run_id}")
    def get_run(run_id: str):
        return service.get_run(run_id)

    @app.post("/runs/{run_id}/transitions")
    def transition_run(run_id: str, req: RunTransition):
        return service.transition_run(run_id, req.state)

    @app.post("/runs/{run_id}/artifacts", status_code=201)
    def attach_run_artifact(run_id: str, req: RunArtifactAttach):
        service.attach_run_artifact(run_id, req)
        return service.get_run(run_id)

    # -- derivation --------------------------------------------------------
    @app.post("/runs/{run_id}/derive")
    def derive_run(run_id: str, req: DeriveRunCreate):
        result = service.derive_run(run_id, req)
        return JSONResponse(
            status_code=201 if result.created else 200,
            content=result.model_dump(),
        )

    @app.get("/runs/{run_id}/derivation")
    def get_derivation(run_id: str):
        return service.get_derivation(run_id)

    @app.get("/runs/{run_id}/children")
    def list_direct_children(run_id: str):
        return service.list_direct_children(run_id)

    @app.get("/runs/{run_id}/descendants")
    def list_descendants(
        run_id: str, limit: int | None = None, cursor: str | None = None
    ):
        return service.list_descendants(run_id, limit=limit, cursor=cursor)

    @app.get("/runs/{run_id}/ancestors")
    def list_ancestors(run_id: str, limit: int | None = None):
        return service.list_ancestors(run_id, limit=limit)

    # -- artifacts ---------------------------------------------------------
    @app.post("/artifacts", status_code=201)
    async def register_artifact(
        request: Request,
        id: str | None = None,
        filename: str = "",
        media_type: str = "",
    ):
        chunks: list[bytes] = []
        async for chunk in request.stream():
            chunks.append(chunk)
        data = b"".join(chunks)
        req = ArtifactRegister(id=id, filename=filename, media_type=media_type)
        view, created = service.register_artifact(data, req)
        return JSONResponse(
            status_code=201 if created else 200,
            content=view.model_dump() | {"created": created},
        )

    @app.get("/artifacts")
    def list_artifacts():
        return service.list_artifacts()

    @app.get("/artifacts/{artifact_id}")
    def get_artifact(artifact_id: str):
        return service.get_artifact(artifact_id)

    @app.get("/artifacts/{artifact_id}/content")
    def get_artifact_content(artifact_id: str):
        view = service.get_artifact(artifact_id)
        blob = service.storage.blob_path(view.sha256)
        if not blob.exists():
            return JSONResponse(
                status_code=409,
                content={
                    "error": {
                        "code": "artifact_deleted",
                        "message": f"artifact {artifact_id!r} blob is unavailable",
                    }
                },
            )
        return Response(content=blob.read_bytes(), media_type=view.media_type or "application/octet-stream")

    # -- lineage -----------------------------------------------------------
    @app.post("/lineage", status_code=201)
    def add_lineage(req: LineageCreate):
        return service.add_lineage(req)

    @app.get("/runs/{run_id}/lineage/upstream")
    def lineage_upstream(run_id: str):
        return service.lineage_upstream(run_id)

    @app.get("/runs/{run_id}/lineage/downstream")
    def lineage_downstream(run_id: str):
        return service.lineage_downstream(run_id)

    # -- metrics -----------------------------------------------------------
    @app.post("/runs/{run_id}/metrics", status_code=201)
    def write_metric(run_id: str, req: MetricWrite):
        return service.write_metric(run_id, req)

    @app.get("/runs/{run_id}/metrics")
    def list_metrics(run_id: str, name: str | None = None):
        return service.list_metrics(run_id, name)

    @app.post("/metrics/compare")
    def compare_metrics(req: MetricComparisonRequest):
        return service.compare_metrics(req)

    # -- reproducibility ---------------------------------------------------
    @app.post("/runs/{run_id}/verify")
    def verify_reproducibility(run_id: str, req: ReproduceRequest):
        return service.verify_reproducibility(run_id, req)

    # -- garbage collection & integrity -----------------------------------
    @app.post("/artifacts/gc", status_code=200)
    def garbage_collect():
        return service.garbage_collect()

    @app.get("/integrity")
    def integrity_report():
        return service.integrity_report()

    return app
