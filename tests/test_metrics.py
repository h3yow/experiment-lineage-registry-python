"""Missing values are never zero; repeated writes follow deterministic
visibility; baseline/caliber mismatches are rejected distinctly.
"""
import pytest

from elr import metrics as metrics_svc
from elr import runs as runs_svc
from elr.errors import (
    InconsistentCaliberError,
    InvalidConfigError,
    MissingBaselineError,
    MissingMetricError,
    TerminalFieldWriteError,
)


def test_latest_write_wins_with_dense_sequence(make_world):
    k = make_world._kernel
    r = make_world.submit()
    runs_svc.transition_run(k, r.id, "RUNNING")
    for v in (1.0, 2.0, 3.0):
        metrics_svc.put_metric(k, r.id, "acc", v)
    m = metrics_svc.get_metric(k, r.id, "acc")
    assert m["value"] == 3.0 and m["seq"] == 3


def test_missing_metric_not_zero(make_world):
    k = make_world._kernel
    cur = make_world.submit()
    base = make_world.submit()
    with pytest.raises(MissingBaselineError) as ei:
        metrics_svc.compare_metric(k, cur.id, base.id, "acc")
    assert ei.value.details["reason"] == "baseline_metric_missing"
    metrics_svc.put_metric(k, base.id, "acc", 0.9)
    with pytest.raises(MissingMetricError) as ei2:
        metrics_svc.compare_metric(k, cur.id, base.id, "acc")
    assert ei2.value.details["reason"] == "candidate_metric_missing"


def test_missing_baseline_run_distinct(make_world):
    k = make_world._kernel
    r = make_world.submit()
    with pytest.raises(MissingBaselineError) as ei:
        metrics_svc.compare_metric(k, r.id, "ghost", "acc")
    assert ei.value.details["reason"] == "baseline_run_missing"


def test_inconsistent_caliber_rejected(make_world):
    k = make_world._kernel
    cur = make_world.submit()
    base = make_world.submit()
    metrics_svc.put_metric(k, cur.id, "lat", 10, caliber="ms")
    metrics_svc.put_metric(k, base.id, "lat", 10, caliber="s")
    with pytest.raises(InconsistentCaliberError) as ei:
        metrics_svc.compare_metric(k, cur.id, base.id, "lat")
    assert ei.value.details == {
        "name": "lat", "run_caliber": "ms", "baseline_caliber": "s"}


def test_compare_happy_path_delta_ratio(make_world):
    k = make_world._kernel
    cur = make_world.submit()
    base = make_world.submit()
    metrics_svc.put_metric(k, base.id, "acc", 0.5)
    metrics_svc.put_metric(k, cur.id, "acc", 0.75)
    out = metrics_svc.compare_metric(k, cur.id, base.id, "acc")
    assert abs(out["delta"] - 0.25) < 1e-9
    assert out["ratio"] == 1.5 and out["caliber"] == "default"


def test_writes_closed_after_terminal(make_world):
    k = make_world._kernel
    r = make_world.submit()
    runs_svc.transition_run(k, r.id, "CANCELED")
    with pytest.raises(TerminalFieldWriteError):
        metrics_svc.put_metric(k, r.id, "acc", 1.0)
    with pytest.raises(TerminalFieldWriteError):
        metrics_svc.put_param(k, r.id, "lr", 0.1)


def test_non_ninite_value_rejected(make_world):
    k = make_world._kernel
    r = make_world.submit()
    with pytest.raises(InvalidConfigError):
        metrics_svc.put_metric(k, r.id, "acc", float("nan"))
