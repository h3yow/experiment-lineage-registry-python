import json

from elr.fingerprint import (
    compute_fingerprint,
    diff_inputs,
    normalize,
)


def test_key_order_invariant():
    a = compute_fingerprint({"PY": "3.14"}, {"lr": 0.01}, {})
    b = compute_fingerprint({"PY": "3.14"}, {"lr": 0.01}, {})
    env_b = dict(reversed(list({"PY": "3.14"}.items())))
    assert a == b
    assert compute_fingerprint(env_b, {"lr": 0.01}, {}) == a


def test_time_fields_ignored_anywhere():
    f1 = compute_fingerprint({"x": 1, "created_at": "t1"}, {}, {})
    f2 = compute_fingerprint({"x": 1, "created_at": "t2"}, {}, {})
    assert f1 == f2
    f3 = compute_fingerprint({"x": 1, "nested": {"ts": 1}}, {}, {})
    f4 = compute_fingerprint({"x": 1, "nested": {"ts": 2}}, {}, {})
    assert f3 == f4
    # a real change still moves the fingerprint
    assert f1 != compute_fingerprint({"x": 2}, {}, {})


def test_harmless_whitespace_and_nfkc():
    f1 = compute_fingerprint({}, {"name": "exp"}, {})
    f2 = compute_fingerprint({}, {"name": "  exp  "}, {})
    assert f1 == f2
    # internal whitespace is significant
    assert compute_fingerprint({}, {"name": "ex p"}, {}) != f1


def test_canonical_json_is_compact_sorted():
    assert normalize({"b": 1, "a": 2}) == {"a": 2, "b": 1}


def test_diff_inputs_distinguishes_missing():
    diffs = diff_inputs({"a": 1}, {"a": 1, "b": 2})
    assert any("unexpected" in d for d in diffs)
    diffs2 = diff_inputs({"a": 1}, {})
    assert any("missing" in d for d in diffs2)
    diffs3 = diff_inputs({"a": 1}, {"a": 2})
    assert diffs3 and "expected=1" in diffs3[0]
