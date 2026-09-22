"""Deterministic reproducibility fingerprints.

Fingerprint semantics
---------------------
A run fingerprint is the sha256 of the canonical JSON encoding of a triple
``(env, config, artifact_manifest)``:

* JSON objects are emitted with keys sorted lexicographically, so input key
  order can never change the fingerprint;
* strings are normalized with NFKC and stripped of *harmless* leading/
  trailing whitespace before comparison (whitespace *inside* values is
  preserved);
* the closed, documented set :data:`IGNORED_TIME_KEYS` is pruned at every
  nesting level -- timestamp fields are not reproducibility inputs, so their
  changes cannot cause fingerprint drift;
* the artifact manifest is a mapping of
  ``{artifact_name: {"content_hash": ..., "role": ...}}`` sorted the same way.

Only values that survive normalization are registered inputs; unknown types
fail loudly rather than being coerced.
"""
from __future__ import annotations

import hashlib
import json
import unicodedata
from typing import Any

# Documented, closed set of timestamp-ish keys that never enter a fingerprint.
IGNORED_TIME_KEYS: frozenset[str] = frozenset({
    "created_at", "updated_at", "ended_at", "submitted_at", "started_at",
    "finished_at", "timestamp", "time", "date", "datetime", "ts",
})


def _scalar(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip()


def normalize(value: Any, *, _key: str | None = None) -> Any:
    """Normalize parsed JSON data.

    Recursively: prunes ignored time keys, sorts dict keys, NFKC+strip
    normalizes strings. Non-JSON-native types raise TypeError.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k in sorted(value.keys()):
            if not isinstance(k, str):
                raise TypeError(f"object keys must be strings, got {type(k)!r}")
            if k in IGNORED_TIME_KEYS:
                continue
            out[k] = normalize(value[k], _key=k)
        return out
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, str):
        return _scalar(value)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return value
    raise TypeError(f"cannot normalize value of type {type(value)!r}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        normalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def compute_fingerprint(
    env: dict[str, Any],
    config: dict[str, Any],
    artifact_manifest: dict[str, Any],
) -> str:
    """sha256 hex over canonical JSON of (env, config, artifact manifest)."""
    payload = {
        "env": normalize(env or {}),
        "config": normalize(config or {}),
        "artifacts": normalize(artifact_manifest or {}),
    }
    blob = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def diff_inputs(
    expected: dict[str, Any], actual: dict[str, Any], *, path: str = "$"
) -> list[str]:
    """Return a list of JSON-pointer-ish paths where two normalized values differ.

    Missing values are reported distinctly (``missing`` vs ``value``), so a
    divergence list never silently treats absence as equality.
    """
    expected = normalize(expected)
    actual = normalize(actual)
    diffs: list[str] = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            sub = f"{path}.{key}"
            if key not in expected:
                diffs.append(f"{sub}: unexpected in rerun")
            elif key not in actual:
                diffs.append(f"{sub}: missing in rerun")
            else:
                diffs.extend(diff_inputs(expected[key], actual[key], path=sub))
    elif expected != actual:
        diffs.append(f"{path}: expected={expected!r} actual={actual!r}")
    return diffs
