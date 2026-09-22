"""Illegal state transitions must be rejected with distinguishable causes;
terminal fields must stay frozen. Logs print run id / transition / basis.
"""
import logging

import pytest

from elr import runs as runs_svc
from elr.errors import (
    InvalidStateTransitionError,
    TerminalFieldWriteError,
)


def test_legal_transitions_and_terminal_freeze(caplog, make_world):
    caplog.set_level(logging.INFO)
    k = make_world._kernel
    r = make_world.submit()
    runs_svc.transition_run(k, r.id, "RUNNING")
    ended = runs_svc.transition_run(k, r.id, "SUCCEEDED")
    assert ended.status == "SUCCEEDED" and ended.ended_at is not None

    # 1) leaving a terminal state is rejected, reason=terminal
    with pytest.raises(InvalidStateTransitionError) as ei:
        runs_svc.transition_run(k, r.id, "RUNNING")
    assert ei.value.details["reason"] == "terminal"
    assert ei.value.details["current"] == "SUCCEEDED"

    # 2) illegal jump PENDING -> SUCCEEDED, reason=not_allowed
    r2 = make_world.submit()
    with pytest.raises(InvalidStateTransitionError) as ei2:
        runs_svc.transition_run(k, r2.id, "SUCCEEDED")
    assert ei2.value.details["reason"] == "not_allowed"
    assert set(ei2.value.details["allowed"]) == {"RUNNING", "CANCELED", "FAILED"}

    # 3) unknown target state is distinguishable
    with pytest.raises(InvalidStateTransitionError) as ei3:
        runs_svc.transition_run(k, r2.id, "DONE")
    assert "unknown target" in ei3.value.message

    # 4) manifest write after start is terminal_field_write
    runs_svc.transition_run(k, r2.id, "RUNNING")
    with pytest.raises(TerminalFieldWriteError) as ei4:
        runs_svc.update_run_manifest(k, r2.id, [])
    assert ei4.value.code == "terminal_field_write"
    assert ei4.value.details["field"] == "artifact_manifest"

    # 5) logs identify the run, transition and basis
    rec = [x.message for x in caplog.records if r2.id in x.message]
    assert any("PENDING -> RUNNING" in m for m in rec)


def test_cancel_and_failure_paths(make_world):
    k = make_world._kernel
    r = make_world.submit()
    assert runs_svc.transition_run(k, r.id, "CANCELED").status == "CANCELED"
    with pytest.raises(InvalidStateTransitionError):
        runs_svc.transition_run(k, r.id, "RUNNING")

    r2 = make_world.submit()
    runs_svc.transition_run(k, r2.id, "RUNNING")
    assert runs_svc.transition_run(k, r2.id, "FAILED").status == "FAILED"
    with pytest.raises(InvalidStateTransitionError) as ei:
        runs_svc.transition_run(k, r2.id, "SUCCEEDED")
    assert ei.value.details["reason"] == "terminal"


def test_pending_manifest_replace_recalculates_fingerprint(make_world):
    k = make_world._kernel
    v = make_world.artifact("m", b"abc")
    r = make_world.submit(
        env={"a": 1},
        artifacts=[{"name": "m", "content_hash": v.content_hash}],
    )
    fp0 = runs_svc.get_run(k, r.id).submitted_fingerprint
    runs_svc.update_run_manifest(k, r.id, [])
    fp1 = runs_svc.get_run(k, r.id).submitted_fingerprint
    assert fp0 != fp1
