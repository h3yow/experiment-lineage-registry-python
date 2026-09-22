"""Unreproducible reruns must be explicitly marked with divergence items."""
import logging

from elr import repro as repro_svc
from elr.artifacts import _blob_path


def test_matching_rerun_passes(make_world, caplog):
    caplog.set_level(logging.INFO)
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    r = make_world.submit(env={"py": "3.14"}, config={"lr": 0.1},
                          artifacts=[{"name": "m", "content_hash": v.content_hash}])
    verdict = repro_svc.verify_run(
        k, r.id, env={"py": "3.14"}, config={"lr": 0.1},
        artifacts=[{"name": "m", "content_hash": v.content_hash}])
    assert verdict["reproducible"] is True
    assert repro_svc.get_verdict(k, r.id)["reproducible"] is True
    assert any("repro_verified" in m and r.id in m for m in caplog.messages)


def test_env_drift_marked_not_passed(make_world):
    k = make_world._kernel
    r = make_world.submit(env={"py": "3.14"}, config={"lr": 0.1})
    v = repro_svc.verify_run(k, r.id, env={"py": "3.13"},
                             config={"lr": 0.1})
    assert v["reproducible"] is False
    assert any("$.env.py" in d for d in v["divergences"])
    assert v["expected_fingerprint"] != v["actual_fingerprint"]


def test_missing_key_and_unexpected_key_distinguished(make_world):
    k = make_world._kernel
    r = make_world.submit(config={"a": 1, "b": 2})
    v = repro_svc.verify_run(k, r.id, config={"a": 1, "c": 3})
    joined = " | ".join(v["divergences"])
    assert "$.config.b: missing" in joined
    assert "$.config.c: unexpected" in joined


def test_timestamp_change_does_not_break_repro(make_world):
    k = make_world._kernel
    r = make_world.submit(env={"py": "3.14", "submitted_at": "t0"},
                          config={"lr": 0.1})
    v = repro_svc.verify_run(k, r.id,
                             env={"py": "3.14", "submitted_at": "t999"},
                             config={"lr": 0.1})
    assert v["reproducible"] is True


def test_missing_blob_is_a_divergence(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    r = make_world.submit(artifacts=[{"name": "m", "content_hash": v.content_hash}])
    # physically remove the content-addressed blob
    _blob_path(k, v.content_hash).unlink()
    verdict = repro_svc.verify_run(k, r.id)
    assert verdict["reproducible"] is False
    assert verdict["missing_blobs"] and \
        verdict["missing_blobs"][0]["reason"] == "artifact_blob_missing"
