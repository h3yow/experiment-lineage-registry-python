"""Lineage registration: cycles and references to missing runs are rejected;
lineage conclusions always trace back to stored edge records.
"""

from __future__ import annotations

import pytest

from elreg.errors import DanglingReferenceError, LineageCycleError
from elreg.schemas import ExperimentCreate, LineageCreate, RunCreate


def _runs(service, n: int):
    exp = service.create_experiment(ExperimentCreate(name="e"))
    return [
        service.submit_run(RunCreate(experiment_id=exp.id))[0].id for _ in range(n)
    ]


def _link(service, up: str, down: str, relation: str = "depends_on"):
    return service.add_lineage(
        LineageCreate(upstream_run_id=up, downstream_run_id=down, relation=relation)
    )


def test_simple_edge_and_traversal(service):
    r1, r2 = _runs(service, 2)
    edge = _link(service, r1, r2)
    assert edge.upstream_run_id == r1
    assert [e.upstream_run_id for e in service.lineage_upstream(r2)] == [r1]
    assert [e.downstream_run_id for e in service.lineage_downstream(r1)] == [r2]


def test_edge_registration_is_idempotent(service):
    r1, r2 = _runs(service, 2)
    e1 = _link(service, r1, r2)
    e2 = _link(service, r1, r2)
    assert e1.id == e2.id


def test_diamond_dag_is_allowed(service):
    r1, r2, r3, r4 = _runs(service, 4)
    _link(service, r1, r2)
    _link(service, r1, r3)
    _link(service, r2, r4)
    _link(service, r3, r4)
    assert len(service.lineage_upstream(r4)) == 2


def test_self_loop_rejected(service):
    (r1,) = _runs(service, 1)
    with pytest.raises(LineageCycleError) as exc:
        _link(service, r1, r1)
    assert exc.value.code == "cycle_detected"


def test_transitive_cycle_rejected(service):
    r1, r2, r3 = _runs(service, 3)
    _link(service, r1, r2)
    _link(service, r2, r3)
    with pytest.raises(LineageCycleError) as exc:
        _link(service, r3, r1)
    assert exc.value.code == "cycle_detected"
    # The rejected edge must not exist; existing edges intact.
    assert [e.downstream_run_id for e in service.lineage_downstream(r3)] == []


def test_missing_upstream_or_downstream_are_distinct(service):
    (r1,) = _runs(service, 1)
    with pytest.raises(DanglingReferenceError) as exc:
        _link(service, "run_ghost", r1)
    assert exc.value.code == "upstream_run_not_found"
    with pytest.raises(DanglingReferenceError) as exc:
        _link(service, r1, "run_ghost")
    assert exc.value.code == "downstream_run_not_found"


def test_lineage_traces_to_records(service):
    r1, r2, r3 = _runs(service, 3)
    e1 = _link(service, r1, r2, relation="trained_from")
    e2 = _link(service, r2, r3, relation="trained_from")
    # Walk upstream by actually reading registered edge rows.
    edges = service.lineage_upstream(r3)
    assert edges[0].id == e2.id and edges[0].registered_at
    edges = service.lineage_upstream(edges[0].upstream_run_id)
    assert edges[0].id == e1.id


def test_cycle_rejected_over_http(client):
    exp = client.post("/experiments", json={"name": "e"}).json()["id"]
    ids = [client.post("/runs", json={"experiment_id": exp}).json()["id"]
           for _ in range(3)]
    client.post("/lineage", json={"upstream_run_id": ids[0], "downstream_run_id": ids[1]})
    client.post("/lineage", json={"upstream_run_id": ids[1], "downstream_run_id": ids[2]})
    resp = client.post("/lineage", json={"upstream_run_id": ids[2],
                                         "downstream_run_id": ids[0]})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "cycle_detected"
