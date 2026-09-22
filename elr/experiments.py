"""Experiment registration and queries."""
from __future__ import annotations

import uuid
from typing import Any

from .errors import AlreadyExistsError, NotFoundError
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow
from .models import Experiment

log = get_logger()


def _row_to_experiment(row) -> Experiment:
    return Experiment(
        id=row["id"],
        name=row["name"],
        created_at=row["created_at"],
        metadata=__import__("json").loads(row["metadata"]),
    )


def create_experiment(
    kernel: Kernel, name: str, metadata: dict[str, Any] | None = None
) -> Experiment:
    if not isinstance(name, str) or not name.strip():
        from .errors import InvalidConfigError
        raise InvalidConfigError("experiment name must be a non-empty string")
    experiment_id = uuid.uuid4().hex
    now = utcnow()
    with kernel.write_tx() as conn:
        exists = conn.execute(
            "SELECT 1 FROM experiments WHERE name=?", (name,)
        ).fetchone()
        if exists:
            raise AlreadyExistsError(
                f"experiment named {name!r} already exists",
                details={"name": name},
            )
        conn.execute(
            "INSERT INTO experiments(id,name,metadata,created_at) "
            "VALUES (?,?,?,?)",
            (experiment_id, name,
             __import__("json").dumps(metadata or {}, sort_keys=True), now),
        )
        record_audit(conn, None, "experiment_created",
                     {"experiment_id": experiment_id, "name": name})
    log.info("experiment_registered id=%s name=%s", experiment_id, name)
    return get_experiment(kernel, experiment_id)


def get_experiment(kernel: Kernel, experiment_id: str) -> Experiment:
    row = kernel.query_one(
        "SELECT * FROM experiments WHERE id=?", (experiment_id,))
    if row is None:
        raise NotFoundError(
            f"experiment {experiment_id} not found",
            details={"experiment_id": experiment_id},
        )
    return _row_to_experiment(row)


def list_experiments(kernel: Kernel) -> list[Experiment]:
    rows = kernel.query(
        "SELECT * FROM experiments ORDER BY created_at, id")
    return [_row_to_experiment(r) for r in rows]
