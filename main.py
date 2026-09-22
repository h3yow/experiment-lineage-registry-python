"""Service entrypoint.

Startup stays compatible with the original skeleton:
``uvicorn main:app`` / ``fastapi run``. Configuration is read from ELR_*
environment variables with documented defaults (see elr/config.py).
"""
from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from elr.api.routes import router
from elr.config import Settings
from elr.errors import ELRError
from elr.kernel import Kernel

settings = Settings.from_env()
app = FastAPI(title="experiment-lineage-registry", version="0.1.0")
kernel = Kernel(settings)
app.state.kernel = kernel
app.include_router(router)


@app.exception_handler(ELRError)
async def elr_error_handler(request: Request, exc: ELRError) -> JSONResponse:
    return JSONResponse(status_code=exc.http_status, content=exc.to_dict())


@app.get("/")
def read_root():
    # Preserved from the original skeleton for startup/interface compatibility.
    return {"Hello": "World"}


@app.on_event("shutdown")
def _shutdown() -> None:
    kernel.close()
