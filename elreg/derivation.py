"""Pure derivation logic: replacement canonicalization, merge and diffing.

A derivation takes a source run's inputs (env, config, input artifacts) and
produces a new input set by applying explicit replacements. Everything not
mentioned is inherited unchanged. This module contains no I/O so the merge and
diff rules can be unit-tested directly.

Merge semantics for ``env_overrides`` / ``config_overrides``:

* nested objects are merged recursively (RFC 7386 style);
* scalars and lists replace the value at their path;
* dotted paths listed in ``env_remove`` / ``config_remove`` are deleted and
  must exist in the source document;
* artifact changes are explicit ``add`` / ``remove`` / ``replace`` lists.

All rules follow the same canonicalization as fingerprints: key order,
insignificant whitespace, volatile time fields, dependency ordering and
duplicate listings never move the request hash.
"""

from __future__ import annotations

import hashlib
from typing import Any

from .fingerprint import DEFAULT_VOLATILE_KEYS, canonicalize

# Stable ordering of the artifact change kinds in canonical requests/hashes.
_CHANGE_KINDS = ("add", "remove", "replace")
# replace entry keys in canonical (sorted) JSON order.
_REPLACE_KEYS = ("artifact_id", "replacement_id")


class DerivationSpecError(ValueError):
    """Raised when a derivation request is structurally invalid."""


def deep_merge(base: Any, overrides: Any) -> Any:
    """Recursively merge *overrides* into *base* (inputs are not mutated)."""
    if isinstance(base, dict) and isinstance(overrides, dict):
        merged = dict(base)
        for key, value in overrides.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged
    return overrides


def remove_path(document: Any, path: str) -> Any:
    """Delete dotted *path* from a copy of *document*; error if absent."""
    if not path:
        raise DerivationSpecError("remove path must be non-empty")
    parts = path.split(".")
    result = dict(document) if isinstance(document, dict) else document
    current = result
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current:
            raise DerivationSpecError(f"remove path {path!r} does not exist")
        current = current[part]
    if not isinstance(current, dict) or parts[-1] not in current:
        raise DerivationSpecError(f"remove path {path!r} does not exist")
    del current[parts[-1]]
    return result


def canonical_request(
    env_overrides: dict[str, Any],
    config_overrides: dict[str, Any],
    env_remove: list[str],
    config_remove: list[str],
    add_artifacts: list[str],
    remove_artifacts: list[str],
    replace_artifacts: list[dict[str, str]],
    experiment_id: str | None,
) -> dict[str, Any]:
    """Return the order-insensitive canonical form of a derivation request."""
    replaces: list[dict[str, str]] = []
    for entry in replace_artifacts:
        source_id = entry["artifact_id"]
        replacement_id = entry["replacement_id"]
        replaces.append(
            {"artifact_id": source_id, "replacement_id": replacement_id}
        )
    return {
        "experiment_id": experiment_id,
        "env_overrides": env_overrides,
        "config_overrides": config_overrides,
        "env_remove": sorted(set(env_remove)),
        "config_remove": sorted(set(config_remove)),
        "artifacts": {
            "add": sorted(set(add_artifacts)),
            "remove": sorted(set(remove_artifacts)),
            "replace": sorted(
                replaces,
                key=lambda r: (r["artifact_id"], r["replacement_id"]),
            ),
        },
    }


def hash_derivation_request(canonical: dict[str, Any]) -> str:
    """Stable digest of a canonical derivation request."""
    raw = canonicalize(canonical, DEFAULT_VOLATILE_KEYS)
    return hashlib.sha256(b"derive:" + raw).hexdigest()


def validate_artifact_plan(
    source_artifact_ids: list[str],
    add_artifacts: list[str],
    remove_artifacts: list[str],
    replace_artifacts: list[dict[str, str]],
    *,
    unknown_targets: list[str] | None = None,
) -> tuple[set[str], set[str], dict[str, str]]:
    """Validate the structural consistency of an artifact replacement plan.

    Existence/liveness of *target* artifacts is checked by the service layer
    (it owns storage); this function only checks the plan against the source
    run's dependency set and its own internal consistency.
    """
    source_set = set(source_artifact_ids)
    add_set = set(add_artifacts)
    remove_set = set(remove_artifacts)
    pairs: dict[str, str] = {
        entry["artifact_id"]: entry["replacement_id"] for entry in replace_artifacts
    }

    if unknown_targets:
        raise DerivationSpecError(
            f"replacement artifacts do not resolve to live artifacts: {sorted(unknown_targets)}"
        )

    if len(replace_artifacts) != len(pairs):
        raise DerivationSpecError(
            "duplicate artifact_id in replace_artifacts; each source artifact "
            "can be replaced at most once per derivation"
        )
    if len(set(pairs.values())) != len(pairs):
        dupes = sorted(
            {target for target in pairs.values()
             if list(pairs.values()).count(target) > 1}
        )
        raise DerivationSpecError(
            f"duplicate replacement targets in replace_artifacts: {dupes}"
        )
    for source_id, replacement_id in pairs.items():
        if source_id == replacement_id:
            raise DerivationSpecError(
                f"replace entry for {source_id!r} names the same artifact; "
                "drop it to inherit the source dependency"
            )
        if source_id not in source_set:
            raise DerivationSpecError(
                f"cannot replace artifact {source_id!r}: not a dependency of "
                "the source run"
            )
        if source_id in remove_set:
            raise DerivationSpecError(
                f"artifact {source_id!r} is both replaced and removed; choose one"
            )
        if replacement_id in source_set:
            raise DerivationSpecError(
                f"replacement {replacement_id!r} is already a dependency of the "
                "source run; remove it from replacements"
            )
    for artifact_id in remove_set:
        if artifact_id not in source_set:
            raise DerivationSpecError(
                f"cannot remove artifact {artifact_id!r}: not a dependency of "
                "the source run"
            )
    conflicts = add_set & source_set
    if conflicts:
        raise DerivationSpecError(
            f"artifacts already inherited from the source run, drop them from "
            f"add_artifacts: {sorted(conflicts)}"
        )
    add_targets = set(pairs.values())
    conflicts = add_set & add_targets
    if conflicts:
        raise DerivationSpecError(
            f"artifacts are both added and used as replacements: {sorted(conflicts)}"
        )
    return add_set, remove_set, pairs


def apply_document(
    base: dict[str, Any],
    overrides: dict[str, Any],
    removes: list[str],
) -> dict[str, Any]:
    """Apply overrides (deep merge) then dotted-path removals to a document."""
    result = deep_merge(base, overrides)
    for path in sorted(set(removes)):
        result = remove_path(result, path)
    return result


def _doc_leaf_map(value: Any, prefix: str = "") -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(value, dict):
        for key, val in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(val, dict):
                out.update(_doc_leaf_map(val, child))
            else:
                out[child] = canonicalize(val, DEFAULT_VOLATILE_KEYS).decode("utf-8")
    else:
        out[prefix or "$"] = canonicalize(value, DEFAULT_VOLATILE_KEYS).decode("utf-8")
    return out


def _document_changes(category: str, source: Any, result: Any) -> list[dict[str, str]]:
    before = _doc_leaf_map(source)
    after = _doc_leaf_map(result)
    items: list[dict[str, str]] = []
    for key in sorted(set(before) | set(after)):
        if key in before and key not in after:
            change = "removed"
        elif key in after and key not in before:
            change = "added"
        elif before[key] != after[key]:
            change = "replaced"
        else:
            continue
        items.append(
            {
                "category": category,
                "key": key,
                "change": change,
                "from": before.get(key, ""),
                "to": after.get(key, ""),
            }
        )
    return items


def compute_changes(
    source_env: dict[str, Any],
    result_env: dict[str, Any],
    source_config: dict[str, Any],
    result_config: dict[str, Any],
    source_artifact_ids: list[str],
    result_artifact_ids: list[str],
    replace_pairs: dict[str, str],
) -> list[dict[str, str]]:
    """Itemize every effective input change (added/removed/replaced)."""
    items: list[dict[str, str]] = _document_changes("env", source_env, result_env)
    items += _document_changes("config", source_config, result_config)

    before = set(source_artifact_ids)
    after = set(result_artifact_ids)
    replaced_sources: set[str] = set()
    for source_id in sorted(replace_pairs):
        replacement_id = replace_pairs[source_id]
        if source_id in before and replacement_id in after:
            items.append(
                {
                    "category": "artifact",
                    "key": source_id,
                    "change": "replaced",
                    "from": source_id,
                    "to": replacement_id,
                }
            )
            replaced_sources.add(source_id)
    for artifact_id in sorted(after - before - set(replace_pairs.values())):
        items.append(
            {
                "category": "artifact",
                "key": artifact_id,
                "change": "added",
                "from": "",
                "to": artifact_id,
            }
        )
    for artifact_id in sorted(before - after - replaced_sources):
        items.append(
            {
                "category": "artifact",
                "key": artifact_id,
                "change": "removed",
                "from": artifact_id,
                "to": "",
            }
        )
    order = {"env": 0, "config": 1, "artifact": 2}
    items.sort(key=lambda item: (order[item["category"]], item["key"], item["change"]))
    return items
