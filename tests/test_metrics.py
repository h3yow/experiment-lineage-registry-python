"""Metric writes and comparisons: missing is never zero, multi-step writes
follow a deterministic latest-write rule, and baseline/spec errors differ.
"""

from __future__ import annotations

import pytest

from elreg.errors import ConflictError, MetricComparisonError, TerminalWriteError
from elreg.schemas import ExperimentCreate, MetricComparisonRequest, MetricWrite, RunCreate


def _two_runs(service):
    exp = service.create_experiment(ExperimentCreate(name="e"))
    r1 = service.submit_run(RunCreate(experiment_id=exp.id))[0].id
    r2 = service.submit_run(RunCreate(experiment_id=exp.id))[0].id
    return r1, r2


def test_rewrite_same_value_is_idempotent(service):
    r1, _ = _two_runs(service)
    m1 = service.write_metric(r1, MetricWrite(name="acc", value=0.9, step=0))
    m2 = service.write_metric(r1, MetricWrite(name="acc", value=0.9, step=0))
    assert m1.id == m2.id


def test_rewrite_different_value_rejected(service):
    r1, _ = _two_runs(service)
    service.write_metric(r1, MetricWrite(name="acc", value=0.9, step=0))
    with pytest.raises(ConflictError) as exc:
        service.write_metric(r1, MetricWrite(name="acc", value=0.95, step=0))
    assert exc.value.code == "metric_value_conflict"


def test_compare_better_worse_equal(service):
    r1, r2 = _two_runs(service)
    service.write_metric(r1, MetricWrite(name="acc", value=0.9, step=0))
    service.write_metric(r2, MetricWrite(name="acc", value=0.95, step=0))
    result = service.compare_metrics(MetricComparisonRequest(
        baseline_run_id=r1, candidate_run_id=r2,
        metrics={"acc": {"direction": "maximize"}}))
    assert result.results["acc"]["verdict"] == "better"
    within = service.compare_metrics(MetricComparisonRequest(
        baseline_run_id=r1, candidate_run_id=r2,
        metrics={"acc": {"direction": "maximize", "tolerance": 0.1}}))
    assert within.results["acc"]["verdict"] == "equal"
    # minimize flips the interpretation.
    service.write_metric(r1, MetricWrite(name="loss", value=0.2, step=0))
    service.write_metric(r2, MetricWrite(name="loss", value=0.1, step=0))
    minim = service.compare_metrics(MetricComparisonRequest(
        baseline_run_id=r1, candidate_run_id=r2,
        metrics={"loss": {"direction": "minimize"}}))
    assert minim.results["loss"]["verdict"] == "better"


def test_missing_values_are_not_zero(service):
    r1, r2 = _two_runs(service)
    service.write_metric(r2, MetricWrite(name="acc", value=0.9, step=0))
    with pytest.raises(MetricComparisonError) as exc:
        service.compare_metrics(MetricComparisonRequest(
            baseline_run_id=r1, candidate_run_id=r2,
            metrics={"acc": {"direction": "maximize"}}))
    assert exc.value.code == "metric_missing_baseline"
    # Candidate missing is a different code.
    service.write_metric(r1, MetricWrite(name="loss", value=0.5, step=0))
    with pytest.raises(MetricComparisonError) as exc:
        service.compare_metrics(MetricComparisonRequest(
            baseline_run_id=r1, candidate_run_id=r2,
            metrics={"loss": {"direction": "minimize"}}))
    assert exc.value.code == "metric_missing_candidate"


def test_latest_step_is_visible(service):
    r1, r2 = _two_runs(service)
    service.write_metric(r1, MetricWrite(name="acc", value=0.8, step=0))
    service.write_metric(r1, MetricWrite(name="acc", value=0.95, step=1))
    service.write_metric(r2, MetricWrite(name="acc", value=0.9, step=0))
    service.write_metric(r2, MetricWrite(name="acc", value=0.99, step=1))
    result = service.compare_metrics(MetricComparisonRequest(
        baseline_run_id=r1, candidate_run_id=r2,
        metrics={"acc": {"direction": "maximize"}}))
    assert result.results["acc"]["baseline_value"] == 0.95
    assert result.results["acc"]["candidate_value"] == 0.99


def test_explicit_step_compares_that_point(service):
    r1, r2 = _two_runs(service)
    service.write_metric(r1, MetricWrite(name="acc", value=0.9, step=0))
    service.write_metric(r1, MetricWrite(name="acc", value=0.7, step=1))
    service.write_metric(r2, MetricWrite(name="acc", value=0.8, step=0))
    service.write_metric(r2, MetricWrite(name="acc", value=0.95, step=1))
    result = service.compare_metrics(MetricComparisonRequest(
        baseline_run_id=r1, candidate_run_id=r2,
        metrics={"acc": {"direction": "maximize", "step": 0}}))
    assert result.results["acc"]["verdict"] == "worse"


def test_invalid_direction_and_tolerance(service):
    r1, r2 = _two_runs(service)
    with pytest.raises(MetricComparisonError) as exc:
        service.compare_metrics(MetricComparisonRequest(
            baseline_run_id=r1, candidate_run_id=r2,
            metrics={"acc": {"direction": "sideways"}}))
    assert exc.value.code == "invalid_direction"
    with pytest.raises(MetricComparisonError) as exc:
        service.compare_metrics(MetricComparisonRequest(
            baseline_run_id=r1, candidate_run_id=r2,
            metrics={"acc": {"direction": "maximize", "tolerance": -1}}))
    assert exc.value.code == "invalid_tolerance"


def test_metrics_locked_after_terminal(service):
    r1, _ = _two_runs(service)
    service.transition_run(r1, "queued")
    service.transition_run(r1, "running")
    service.transition_run(r1, "succeeded")
    with pytest.raises(TerminalWriteError) as exc:
        service.write_metric(r1, MetricWrite(name="acc", value=1.0))
    assert exc.value.code == "terminal_state"


def test_non_finite_metric_value_rejected(service):
    r1, _ = _two_runs(service)
    with pytest.raises(ConflictError) as exc:
        service.write_metric(r1, MetricWrite(name="x", value=float("nan")))
    assert exc.value.code == "invalid_metric_value"


def test_empty_comparison_spec_rejected(service):
    r1, r2 = _two_runs(service)
    with pytest.raises(MetricComparisonError) as exc:
        service.compare_metrics(MetricComparisonRequest(
            baseline_run_id=r1, candidate_run_id=r2, metrics={}))
    assert exc.value.code == "empty_metric_spec"
