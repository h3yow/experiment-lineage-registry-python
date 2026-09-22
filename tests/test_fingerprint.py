"""Deterministic fingerprint properties."""

from __future__ import annotations

from elreg.fingerprint import (
    canonicalize,
    diff_fingerprint_inputs,
    fingerprint_run_inputs,
    sha256_bytes,
)


def test_key_order_and_whitespace_stable():
    f1 = fingerprint_run_inputs({"b": 1, "a": 2}, {"x": [1, 2]}, ["s1"])
    f2 = fingerprint_run_inputs({"a": 2, "b": 1}, {"x": [1, 2]}, ["s1"])
    assert f1 == f2
    assert canonicalize({"a": 1, "b": 2}) == canonicalize({"b": 2, "a": 1})


def test_volatile_fields_ignored():
    f1 = fingerprint_run_inputs({"ts": "t1"}, {}, [])
    f2 = fingerprint_run_inputs({"ts": "t2"}, {}, [])
    assert f1 == f2


def test_dependency_order_and_duplicates_insensitive():
    f1 = fingerprint_run_inputs({}, {}, ["a", "b"])
    f2 = fingerprint_run_inputs({}, {}, ["b", "a", "a"])
    assert f1 == f2


def test_real_change_moves_fingerprint():
    base = fingerprint_run_inputs({"py": "3.14"}, {"lr": 0.1}, ["a"])
    assert fingerprint_run_inputs({"py": "3.13"}, {"lr": 0.1}, ["a"]) != base
    assert fingerprint_run_inputs({"py": "3.14"}, {"lr": 0.2}, ["a"]) != base
    assert fingerprint_run_inputs({"py": "3.14"}, {"lr": 0.1}, ["b"]) != base


def test_section_separation_no_collision():
    assert fingerprint_run_inputs({"x": 1}, {}, []) != fingerprint_run_inputs({}, {"x": 1}, [])


def test_divergence_reasons_are_distinct():
    items = diff_fingerprint_inputs(
        {"a": 1}, {"k": 1}, ["s1"],
        {"a": 1, "b": 2}, {"k": 2}, ["s2"],
    )
    by_key = {(i["category"], i["key"]): i["reason"] for i in items}
    assert by_key[("env", "b")] == "unexpected_in_actual"
    assert by_key[("config", "k")] == "value_mismatch"
    artifact_reasons = {i["reason"] for i in items if i["category"] == "artifact"}
    assert artifact_reasons == {"artifact_not_in_registration", "artifact_missing_in_rerun"}


def test_sha256_known_vector():
    assert sha256_bytes(b"").startswith("e3b0c442")
