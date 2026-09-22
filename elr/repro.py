"""Reproducibility verification.

Given a run and a rerun's claimed inputs (environment, config and artifact
dependencies), the registry recomputes the fingerprint with exactly the same
canonicalization used at submission and compares:

* equal normalized inputs -> ``reproducible=true``;
* any difference -> ``reproducible=false`` with a per-section divergence list
  (missing keys, unexpected keys, value mismatches) and missing content blobs
  reported as ``artifact_blob_missing``;
* the verdict is stored (``repro_checks``) so a run that could not be
  reproduced stays explicitly marked instead of being reported as passing.

The recorded verdict never deletes/upgrades: a failed check stays visible.
"""
from __future__ import annotations

import json

from .artifacts import blob_exists
from .errors import NotFoundError
from .fingerprint import compute_fingerprint, diff_inputs
from .kernel import Kernel
from .logging_utils import get_logger, record_audit, utcnow

log = get_logger()


def _load_run(kernel, run_id: str):
    row = kernel.query_one("SELECT * FROM runs WHERE id=?", (run_id,))
    if row is None:
        raise NotFoundError(f"run {run_id} not found",
                            details={"run_id": run_id})
    return row


def _fp_manifest(manifest: dict) -> dict:
    return {
        name: {"content_hash": m["content_hash"], "role": m.get("role", "input")}
        for name, m in sorted(manifest.items())
    }


def _rerun_manifest(artifacts: list[dict] | None) -> dict:
    out: dict = {}
    for item in artifacts or []:
        out[item["name"]] = {
            "content_hash": item["content_hash"],
            "role": item.get("role", "input"),
        }
    return out


def verify_run(kernel: Kernel, run_id: str,
               env: dict | None = None, config: dict | None = None,
               artifacts: list[dict] | None = None) -> dict:
    with kernel.write_tx() as conn:
        row = _load_run(kernel, run_id)
        rec_env = json.loads(row["env"])
        rec_config = json.loads(row["config"])
        rec_manifest_full = json.loads(row["artifact_manifest"])
        rec_manifest = _fp_manifest(rec_manifest_full)
        recorded_fp = row["submitted_fingerprint"]

        in_env = env if env is not None else rec_env
        in_config = config if config is not None else rec_config
        in_manifest = (
            _rerun_manifest(artifacts) if artifacts is not None
            else rec_manifest
        )
        actual_fp = compute_fingerprint(in_env, in_config, in_manifest)

        divergences: list[str] = []
        divergences += diff_inputs(rec_env, in_env, path="$.env")
        divergences += diff_inputs(rec_config, in_config, path="$.config")
        divergences += diff_inputs(rec_manifest, in_manifest,
                                   path="$.artifacts")

        missing_blobs: list[dict] = []
        for name, m in sorted(rec_manifest_full.items()):
            if not blob_exists(kernel, m["content_hash"]):
                entry = {"name": name, "content_hash": m["content_hash"],
                         "version_id": m.get("version_id"),
                         "reason": "artifact_blob_missing"}
                missing_blobs.append(entry)
                divergences.append(
                    f"$.artifacts.{name}: blob missing on disk "
                    f"(hash={m['content_hash'][:12]})"
                )

        reproducible = (
            actual_fp == recorded_fp and not missing_blobs
            and not divergences
        )
        now = utcnow()
        conn.execute(
            "INSERT INTO repro_checks(run_id,reproducible,"
            "expected_fingerprint,actual_fingerprint,divergences,checked_at) "
            "VALUES (?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET "
            "reproducible=excluded.reproducible, "
            "expected_fingerprint=excluded.expected_fingerprint, "
            "actual_fingerprint=excluded.actual_fingerprint, "
            "divergences=excluded.divergences, checked_at=excluded.checked_at",
            (run_id, 1 if reproducible else 0, recorded_fp, actual_fp,
             json.dumps(divergences, ensure_ascii=False), now),
        )
        record_audit(conn, run_id, "repro_check",
                     {"reproducible": reproducible,
                      "recorded_fp": recorded_fp, "actual_fp": actual_fp,
                      "divergence_count": len(divergences)})
    verdict = {
        "run_id": run_id,
        "reproducible": reproducible,
        "expected_fingerprint": recorded_fp,
        "actual_fingerprint": actual_fp,
        "divergences": divergences,
        "missing_blobs": missing_blobs,
        "checked_at": now,
    }
    log.info(
        "repro_verified run_id=%s reproducible=%s basis=%s divergences=%d",
        run_id, reproducible,
        "fingerprint_equal_and_blobs_present" if reproducible
        else "fingerprint_or_blob_divergence",
        len(divergences),
    )
    return verdict


def get_verdict(kernel: Kernel, run_id: str) -> dict | None:
    _load_run(kernel, run_id)
    row = kernel.query_one(
        "SELECT * FROM repro_checks WHERE run_id=?", (run_id,))
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "reproducible": bool(row["reproducible"]),
        "expected_fingerprint": row["expected_fingerprint"],
        "actual_fingerprint": row["actual_fingerprint"],
        "divergences": json.loads(row["divergences"]),
        "checked_at": row["checked_at"],
    }
