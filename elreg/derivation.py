"""Pure derivation logic: apply explicit replacements to a terminal run.

A derivation inherits every input of the source run except the parts the
caller explicitly changes:

* ``env`` / ``config`` patches are *deep-merged* over the source values, so
  unspecified paths are inherited rather than erased;
* ``artifact_replacements`` maps an existing source input artifact id to a
  replacement artifact id; ``remove_artifacts`` drops inherited inputs;
* nothing here mutates the source; the caller persists a brand new run.

All normalization follows the same canonicalization rules as fingerprints
(key order, harmless whitespace and volatile timestamp fields are ignored).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .fingerprint import DEFAULT_VOLATILE_KEYS, _normalize, canonicalize


def deep_merge(base: Any, patch: Any) -> Any:
    """Return a new mapping with *patch* deep-merged over *base*.

    Non-dict values in the patch replace the base value outright; the inputs
    are never mutated.
    """
    if isinstance(base, dict) and isinstance(patch, dict):
        merged: dict[str, Any] = dict(base)
        for key, value in patch.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged
    # JSON structures from pydantic: treat lists as plain values.
    return patch


def _flatten_leaves(value: Any, prefix: str = "") -> dict[str, str]:
    normalized = _normalize(value, DEFAULT_VOLATILE_KEYS)
    out: dict[str, str] = {}
    if isinstance(normalized, dict):
        for key in sorted(normalized):
            child = f"{prefix}.{key}" if prefix else key
            if isinstance(normalized[key], dict):
                out.update(_flatten_leaves(normalized[key], child))
            else:
                out[child] = json.dumps(
                    normalized[key], sort_keys=True, separators=(",", ":"),
                    ensure_ascii=False,
                )
    else:
        out[prefix or "$"] = json.dumps(
            normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
    return out


def diff_section(category: str, source: Any, derived: Any) -> list[dict[str, Any]]:
    """Itemize leaf-level added/removed/replaced entries of one JSON section."""
    source_flat = _flatten_leaves(source)
    derived_flat = _flatten_leaves(derived)
    changes: list[dict[str, Any]] = []
    for key in sorted(set(source_flat) | set(derived_flat)):
        in_source = key in source_flat
        in_derived = key in derived_flat
        if in_source and in_derived and source_flat[key] == derived_flat[key]:
            continue
        if in_source and not in_derived:
            op = "removed"
        elif in_derived and not in_source:
            op = "added"
        else:
            op = "replaced"
        changes.append(
            {
                "category": category,
                "op": op,
                "key": key,
                "source_value": _decode(source_flat.get(key)),
                "derived_value": _decode(derived_flat.get(key)),
            }
        )
    return changes


def _decode(leaf: str | None) -> Any:
    if leaf is None:
        return None
    return json.loads(leaf)


def diff_artifacts(
    source_ids: list[str],
    derived_ids: list[str],
    replacements: dict[str, str],
    removed: set[str],
) -> list[dict[str, Any]]:
    """Itemize added/removed/replaced artifact dependencies by id."""
    changes: list[dict[str, Any]] = []
    for old in sorted(replacements):
        new = replacements[old]
        if old in source_ids:
            changes.append(
                {
                    "category": "artifact",
                    "op": "replaced",
                    "key": old,
                    "source_value": old,
                    "derived_value": new,
                }
            )
    for aid in sorted(removed):
        changes.append(
            {
                "category": "artifact",
                "op": "removed",
                "key": aid,
                "source_value": aid,
                "derived_value": None,
            }
        )
    source_set = set(source_ids)
    replacement_targets = set(replacements.values())
    for aid in sorted(set(derived_ids)):
        if aid in source_set:
            continue
        # A fresh id that is not a replacement target is genuinely new.
        if aid in replacement_targets:
            continue
        changes.append(
            {
                "category": "artifact",
                "op": "added",
                "key": aid,
                "source_value": None,
                "derived_value": aid,
            }
        )
    return changes


def replacement_digest(
    artifact_replacements: dict[str, str],
    remove_artifacts: list[str],
    env_patch: Any,
    config_patch: Any,
) -> str:
    """Stable digest of the *requested* replacement set.

    Equivalent requests (different key order, whitespace, volatile fields,
    ordering/duplication of removed ids) produce the same digest.
    """
    payload = {
        "artifact_replacements": {
            k: artifact_replacements[k] for k in sorted(artifact_replacements)
        },
        "remove_artifacts": sorted(set(remove_artifacts)),
        "env": env_patch,
        "config": config_patch,
    }
    hasher = hashlib.sha256()
    hasher.update(b"derivation-replacements:v1|")
    hasher.update(canonicalize(payload, DEFAULT_VOLATILE_KEYS))
    return hasher.hexdigest()
