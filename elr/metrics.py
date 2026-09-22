"""Metric/parameter writes, deterministic visibility and baseline compare.

Visibility rule: every write is appended with a monotonic per-(run,name)
sequence; reads return the highest sequence point. Repeated writes are
therefore deterministic (last committed write wins; commits serialize via
the kernel lock) and every historical point remains queryable.

Comparison rule (``compare_metric``):

* either side missing  -> ``missing_metric`` (never compared as zero);
* baseline run missing -> ``missing_baseline``;
* different calibers   -> ``inconsistent_caliber``;
* otherwise the delta/ratio is returned.
"""
from __future__ import annotations

import json

from .errors import (
    InconsistentCaliberError,
    InvalidConfigError,
    MissingBaselineError,
    MissingMetricError,
    NotFoundError,
)
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow
from .runs import assert_run_writable

log = get_logger()


def _next_seq(conn, table: str, run_id: str, name: str) -> int:
    row = conn.execute(
        f"SELECT COALESCE(MAX(seq),0) AS m FROM {table} "
        "WHERE run_id=? AND name=?",
        (run_id, name),
    ).fetchone()
    return int(row["m"]) + 1


def put_metric(kernel: Kernel, run_id: str, name: str, value,
               caliber: str = "default") -> dict:
    if not isinstance(name, str) or not name.strip():
        raise InvalidConfigError("metric name must be a non-empty string")
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise InvalidConfigError(
            f"metric {name!r} value must be numeric, got {value!r}",
            details={"name": name, "value": value},
        )
    if num != num or num in (float("inf"), float("-inf")):
        raise InvalidConfigError(
            f"metric {name!r} value must be finite", details={"name": name})
    if not isinstance(caliber, str) or not caliber.strip():
        raise InvalidConfigError("caliber must be a non-empty string")

    with kernel.write_tx() as conn:
        status = assert_run_writable(conn, run_id)
        seq = _next_seq(conn, "run_metrics", run_id, name)
        now = utcnow()
        conn.execute(
            "INSERT INTO run_metrics(run_id,name,value,seq,caliber,created_at)"
            " VALUES (?,?,?,?,?,?)",
            (run_id, name, num, seq, caliber, now),
        )
        record_audit(conn, run_id, "metric_put",
                     {"name": name, "value": num, "seq": seq,
                      "caliber": caliber, "run_status": status})
    log.info(
        "metric_put run_id=%s name=%s seq=%d value=%r caliber=%s "
        "run_status=%s", run_id, name, seq, num, caliber, status,
    )
    return {"run_id": run_id, "name": name, "value": num, "seq": seq,
            "caliber": caliber, "created_at": now}


def _latest_row(kernel: Kernel, run_id: str, name: str):
    return kernel.query_one(
        "SELECT * FROM run_metrics WHERE run_id=? AND name=? "
        "ORDER BY seq DESC, id DESC LIMIT 1",
        (run_id, name),
    )


def _require_run(kernel: Kernel, run_id: str) -> None:
    if kernel.query_one("SELECT 1 FROM runs WHERE id=?", (run_id,)) is None:
        raise NotFoundError(f"run {run_id} not found",
                            details={"run_id": run_id})


def get_metric(kernel: Kernel, run_id: str, name: str) -> dict:
    _require_run(kernel, run_id)
    row = _latest_row(kernel, run_id, name)
    if row is None:
        raise MissingMetricError(
            f"metric {name!r} has never been written to run {run_id}; "
            "absence must not be treated as zero",
            details={"run_id": run_id, "name": name,
                     "reason": "metric_not_written"},
        )
    return {"run_id": run_id, "name": name, "value": row["value"],
            "seq": row["seq"], "caliber": row["caliber"],
            "created_at": row["created_at"]}


def list_metrics(kernel: Kernel, run_id: str) -> dict[str, dict]:
    _require_run(kernel, run_id)
    rows = kernel.query(
        "SELECT * FROM run_metrics m WHERE id=(SELECT MAX(id) FROM run_metrics "
        "WHERE run_id=m.run_id AND name=m.name) ORDER BY name"
    )
    return {
        r["name"]: {"value": r["value"], "seq": r["seq"],
                    "caliber": r["caliber"], "created_at": r["created_at"]}
        for r in rows
    }


def put_param(kernel: Kernel, run_id: str, name: str, value) -> dict:
    """Write a parameter value (JSON stored). Append-only with seq visibility."""
    if not isinstance(name, str) or not name.strip():
        raise InvalidConfigError("param name must be a non-empty string")
    try:
        stored = json.dumps(value, sort_keys=True, ensure_ascii=False,
                            allow_nan=False)
    except (TypeError, ValueError):
        raise InvalidConfigError(
            f"param {name!r} value must be JSON-serializable",
            details={"name": name})
    with kernel.write_tx() as conn:
        assert_run_writable(conn, run_id)
        seq = _next_seq(conn, "run_params", run_id, name)
        now = utcnow()
        conn.execute(
            "INSERT INTO run_params(run_id,name,value,seq,created_at) "
            "VALUES (?,?,?,?,?)",
            (run_id, name, stored, seq, now),
        )
        record_audit(conn, run_id, "param_put",
                     {"name": name, "seq": seq})
    log.info("param_put run_id=%s name=%s seq=%d", run_id, name, seq)
    return {"run_id": run_id, "name": name, "value": value, "seq": seq,
            "created_at": now}


def get_param(kernel: Kernel, run_id: str, name: str):
    _require_run(kernel, run_id)
    row = kernel.query_one(
        "SELECT * FROM run_params WHERE run_id=? AND name=? "
        "ORDER BY seq DESC, id DESC LIMIT 1",
        (run_id, name),
    )
    if row is None:
        raise MissingMetricError(
            f"param {name!r} has never been written to run {run_id}",
            details={"run_id": run_id, "name": name,
                     "reason": "param_not_written"},
        )
    return {"run_id": run_id, "name": name,
            "value": json.loads(row["value"]), "seq": row["seq"],
            "created_at": row["created_at"]}


def compare_metric(kernel: Kernel, run_id: str, baseline_run_id: str,
                   name: str) -> dict:
    _require_run(kernel, run_id)
    base_run = kernel.query_one(
        "SELECT 1 FROM runs WHERE id=?", (baseline_run_id,))
    if base_run is None:
        raise MissingBaselineError(
            f"baseline run {baseline_run_id} does not exist",
            details={"baseline_run_id": baseline_run_id,
                     "reason": "baseline_run_missing"},
        )
    base_row = _latest_row(kernel, baseline_run_id, name)
    if base_row is None:
        raise MissingBaselineError(
            f"baseline run {baseline_run_id} has no metric {name!r}",
            details={"baseline_run_id": baseline_run_id, "name": name,
                     "reason": "baseline_metric_missing"},
        )
    cur_row = _latest_row(kernel, run_id, name)
    if cur_row is None:
        raise MissingMetricError(
            f"run {run_id} has no metric {name!r}; cannot treat missing as 0",
            details={"run_id": run_id, "name": name,
                     "reason": "candidate_metric_missing"},
        )
    if cur_row["caliber"] != base_row["caliber"]:
        raise InconsistentCaliberError(
            f"metric {name!r} caliber mismatch: run uses "
            f"{cur_row['caliber']!r}, baseline uses {base_row['caliber']!r}",
            details={"name": name, "run_caliber": cur_row["caliber"],
                     "baseline_caliber": base_row["caliber"]},
        )
    base_v = float(base_row["value"])
    cur_v = float(cur_row["value"])
    result = {
        "name": name,
        "run_id": run_id,
        "baseline_run_id": baseline_run_id,
        "caliber": cur_row["caliber"],
        "run_value": cur_v,
        "baseline_value": base_v,
        "delta": cur_v - base_v,
        "ratio": (cur_v / base_v) if base_v != 0 else None,
        "basis": "latest_seq_visible_value",
    }
    log.info(
        "metric_compare run_id=%s baseline=%s name=%s delta=%r caliber=%s "
        "basis=latest_seq", run_id, baseline_run_id, name, result["delta"],
        cur_row["caliber"],
    )
    return result
