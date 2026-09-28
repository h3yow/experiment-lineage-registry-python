"""Pure derivation logic: canonical hashing, deep merge, path removal and
artifact-plan validation.
"""

from __future__ import annotations

import pytest

from elreg.derivation import (
    DerivationSpecError,
    apply_document,
    canonical_request,
    compute_changes,
    deep_merge,
    hash_derivation_request,
    validate_artifact_plan,
)


def test_deep_merge_is_recursive_and_non_destructive():
    base = {"a": 1, "n": {"x": 1, "y": 2}}
    merged = deep_merge(base, {"n": {"y": 3, "z": 4}, "b": 5})
    assert merged == {"a": 1, "n": {"x": 1, "y": 3, "z": 4}, "b": 5}
    assert base == {"a": 1, "n": {"x": 1, "y": 2}}  # untouched
    assert deep_merge([1], {"a": 1}) == {"a": 1}
    assert deep_merge({"a": 1}, [2]) == [2]


def test_apply_document_removes_paths():
    result = apply_document(
        {"a": 1, "n": {"x": 1, "y": 2}}, {"b": 3}, ["n.x"]
    )
    assert result == {"a": 1, "n": {"y": 2}, "b": 3}
    with pytest.raises(DerivationSpecError):
        apply_document({"a": 1}, {}, ["missing"])
    with pytest.raises(DerivationSpecError):
        apply_document({"a": 1}, {}, ["a.b"])


def test_canonical_request_order_and_duplicate_insensitive():
    kwargs = dict(
        env_overrides={"b": 1, "a": 2},
        config_overrides={"z": 0},
        env_remove=["p", "p"],
        config_remove=["q"],
        add_artifacts=["x", "x"],
        remove_artifacts=["y"],
        replace_artifacts=[{"artifact_id": "s", "replacement_id": "t"}],
        experiment_id=None,
    )
    first = canonical_request(**kwargs)
    shuffled = dict(kwargs)
    shuffled["env_overrides"] = {"a": 2, "b": 1}
    shuffled["replace_artifacts"] = [
        {"replacement_id": "t", "artifact_id": "s"}
    ]
    second = canonical_request(**shuffled)
    assert hash_derivation_request(first) == hash_derivation_request(second)


def test_canonical_request_distinguishes_real_changes():
    base = canonical_request({"a": 1}, {}, [], [], [], [], [], None)
    assert hash_derivation_request(base) != hash_derivation_request(
        canonical_request({"a": 2}, {}, [], [], [], [], [], None)
    )
    assert hash_derivation_request(base) != hash_derivation_request(
        canonical_request({"a": 1}, {}, [], [], ["x"], [], [], None)
    )


@pytest.mark.parametrize(
    ("source", "add", "remove", "replace"),
    [
        (["s"], [], [], [{"artifact_id": "missing", "replacement_id": "t"}]),
        (["s"], ["s"], [], []),
        (["s"], [], ["missing"], []),
        (["s", "t"], [], [], [{"artifact_id": "s", "replacement_id": "t"}]),
        (["s"], [], ["s"], [{"artifact_id": "s", "replacement_id": "t"}]),
        (["s"], ["t"], [], [{"artifact_id": "s", "replacement_id": "t"}]),
        (["s1", "s2"], [], [], [
            {"artifact_id": "s1", "replacement_id": "t"},
            {"artifact_id": "s2", "replacement_id": "t"},
        ]),
    ],
)
def test_invalid_artifact_plans_rejected(source, add, remove, replace):
    with pytest.raises(DerivationSpecError):
        validate_artifact_plan(source, add, remove, replace)


def test_valid_plan_succeeds():
    add_set, remove_set, pairs = validate_artifact_plan(
        ["keep", "old", "gone"],
        ["added"],
        ["gone"],
        [{"artifact_id": "old", "replacement_id": "new"}],
    )
    assert add_set == {"added"}
    assert remove_set == {"gone"}
    assert pairs == {"old": "new"}


def test_compute_changes_itemizes_all_three_kinds():
    changes = compute_changes(
        source_env={"a": 1},
        result_env={"b": 2},
        source_config={"k": 1},
        result_config={"k": 2},
        source_artifact_ids=["keep", "old", "gone"],
        result_artifact_ids=["keep", "new", "added"],
        replace_pairs={"old": "new"},
    )
    indexed = {(c["category"], c["key"]): c["change"] for c in changes}
    assert indexed[("env", "a")] == "removed"
    assert indexed[("env", "b")] == "added"
    assert indexed[("config", "k")] == "replaced"
    assert indexed[("artifact", "gone")] == "removed"
    assert indexed[("artifact", "added")] == "added"
    assert indexed[("artifact", "old")] == "replaced"
    assert ("artifact", "keep") not in indexed
