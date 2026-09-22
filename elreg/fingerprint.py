"""Deterministic fingerprinting for reproducibility checks.

Canonicalization rules (see README):

* JSON structures are normalized to compact JSON with recursively sorted
  keys, so key order and insignificant whitespace never change the digest;
* configured volatile fields (timestamps / wall-clock fields) are stripped
  before hashing, so updating e.g. ``updated_at`` never moves the fingerprint;
* artifact dependencies contribute their *content digest* (sha256), not their
  versioned identifiers; renaming a version without changing bytes does not
  affect reproducibility;
* the dependency set is order-insensitive (it is sorted before hashing).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Field names treated as harmless/volatile and excluded from fingerprints.
DEFAULT_VOLATILE_KEYS: frozenset[str] = frozenset(
    {
        "created_at",
        "updated_at",
        "timestamp",
        "time",
        "ts",
        "submitted_at",
        "started_at",
        "finished_at",
        "recorded_at",
        "wall_time",
        "wall_clock",
    }
)


def _normalize(value: Any, volatile_keys: frozenset[str]) -> Any:
    """Recursively strip volatile keys and produce a JSON-normal structure."""
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, val in value.items():
            if isinstance(key, str) and key in volatile_keys:
                continue
            result[str(key)] = _normalize(val, volatile_keys)
        return result
    if isinstance(value, (list, tuple)):
        return [_normalize(item, volatile_keys) for item in value]
    # bool is a subclass of int; keep native types so 1 vs true stays distinct.
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    # Unknown types: coerce via str for a stable, explicit representation.
    return str(value)


def canonicalize(value: Any, volatile_keys: frozenset[str] = DEFAULT_VOLATILE_KEYS) -> bytes:
    """Return deterministic compact UTF-8 JSON bytes for *value*."""
    normalized = _normalize(value, volatile_keys)
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def fingerprint_run_inputs(
    env: dict[str, Any],
    config: dict[str, Any],
    artifact_shas: list[str],
    volatile_keys: frozenset[str] = DEFAULT_VOLATILE_KEYS,
) -> str:
    """Compute the run fingerprint over env, config and dependency contents.

    Each section is domain-separated to avoid cross-section collisions;
    artifact digests are deduplicated and order-insensitive.
    """
    hasher = hashlib.sha256()
    hasher.update(b"env:")
    hasher.update(canonicalize(env, volatile_keys))
    hasher.update(b"|config:")
    hasher.update(canonicalize(config, volatile_keys))
    hasher.update(b"|artifacts:")
    hasher.update(canonicalize(sorted(set(artifact_shas)), volatile_keys))
    return hasher.hexdigest()


def _flat_mapping(value: Any, prefix: str = "") -> dict[str, str]:
    """Flatten a normalized mapping to dotted path -> canonical JSON."""
    normalized = _normalize(value, DEFAULT_VOLATILE_KEYS)
    out: dict[str, str] = {}
    if isinstance(normalized, dict):
        for key in sorted(normalized):
            child_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(normalized[key], dict):
                out.update(_flat_mapping(normalized[key], child_prefix))
            else:
                out[child_prefix] = json.dumps(
                    normalized[key], sort_keys=True, separators=(",", ":"), ensure_ascii=False
                )
    else:
        out[prefix or "$"] = json.dumps(
            normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
    return out


def _diff_mapping(
    category: str, expected: Any, actual: Any
) -> list[dict[str, str]]:
    expected_flat = _flat_mapping(expected)
    actual_flat = _flat_mapping(actual)
    items: list[dict[str, str]] = []
    for key in sorted(set(expected_flat) | set(actual_flat)):
        in_expected = key in expected_flat
        in_actual = key in actual_flat
        if in_expected and in_actual and expected_flat[key] == actual_flat[key]:
            continue
        if in_expected and not in_actual:
            reason = "missing_in_actual"
        elif in_actual and not in_expected:
            reason = "unexpected_in_actual"
        else:
            reason = "value_mismatch"
        items.append(
            {
                "category": category,
                "key": key,
                "reason": reason,
                "expected": expected_flat.get(key, ""),
                "actual": actual_flat.get(key, ""),
            }
        )
    return items


def diff_fingerprint_inputs(
    expected_env: Any,
    expected_config: Any,
    expected_shas: list[str],
    actual_env: Any,
    actual_config: Any,
    actual_shas: list[str],
    volatile_keys: frozenset[str] = DEFAULT_VOLATILE_KEYS,
) -> list[dict[str, str]]:
    """Return deterministic divergence items between two run input sets."""
    items: list[dict[str, str]] = _diff_mapping("env", expected_env, actual_env)
    items.extend(_diff_mapping("config", expected_config, actual_config))

    expected_set = set(expected_shas)
    actual_set = set(actual_shas)
    for sha in sorted(actual_set - expected_set):
        items.append(
            {
                "category": "artifact",
                "key": sha,
                "reason": "artifact_not_in_registration",
                "expected": "",
                "actual": sha,
            }
        )
    for sha in sorted(expected_set - actual_set):
        items.append(
            {
                "category": "artifact",
                "key": sha,
                "reason": "artifact_missing_in_rerun",
                "expected": sha,
                "actual": "",
            }
        )
    return items


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
