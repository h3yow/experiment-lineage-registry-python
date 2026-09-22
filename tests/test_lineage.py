"""Lineage cycles and dangling references must be rejected; conclusions trace
to explicit registration records."""
import pytest

from elr import lineage as lineage_svc
from elr.errors import LineageCycleError, LineageReferenceError


def _chain(make_world, n=3):
    runs = [make_world.submit() for _ in range(n)]
    return runs


def test_self_dependency_is_a_cycle(make_world):
    r = make_world.submit()
    with pytest.raises(LineageCycleError) as ei:
        lineage_svc.register_run_dependency(make_world._kernel, r.id, r.id)
    assert ei.value.code == "lineage_cycle"


def test_indirect_cycle_rejected(make_world, caplog):
    a, b, c = _chain(make_world)
    k = make_world._kernel
    lineage_svc.register_run_dependency(k, a.id, b.id)
    lineage_svc.register_run_dependency(k, b.id, c.id)
    with pytest.raises(LineageCycleError) as ei:
        lineage_svc.register_run_dependency(k, c.id, a.id)
    assert ei.value.details["basis"] == "path_dst_to_src_already_registered"
    # the closing edge must not exist
    q = lineage_svc.get_lineage(k, c.id)
    assert {x["run_id"] for x in q["upstream_runs"]} == set()


def test_unknown_endpoints_reference_error(make_world):
    r = make_world.submit()
    with pytest.raises(LineageReferenceError) as ei:
        lineage_svc.register_run_dependency(make_world._kernel, r.id, "ghost")
    assert ei.value.details["role"] == "dst"
    with pytest.raises(LineageReferenceError) as ei2:
        lineage_svc.register_run_dependency(make_world._kernel, "ghost", r.id)
    assert ei2.value.details["role"] == "src"


def test_artifact_link_unknown_version_rejected(make_world):
    r = make_world.submit()
    with pytest.raises(LineageReferenceError):
        lineage_svc.register_artifact_link(
            make_world._kernel, r.id, "missing-version")


def test_edge_is_idempotent_and_queryable_with_provenance(make_world):
    k = make_world._kernel
    a, b = _chain(make_world, 2)
    e1 = lineage_svc.register_run_dependency(k, a.id, b.id)
    e2 = lineage_svc.register_run_dependency(k, a.id, b.id)
    assert e1.id == e2.id
    assert len(lineage_svc.list_edges(k)) == 1
    q = lineage_svc.get_lineage(k, b.id)
    down = q["downstream_runs"]
    assert down and down[0]["edge_id"] == e1.id
    assert down[0]["registered_at"] and down[0]["relation"] == "depends_on"


def test_artifact_lineage_traces_to_record(make_world):
    k = make_world._kernel
    r = make_world.submit()
    v = make_world.artifact("m", b"xyz")
    edge = lineage_svc.register_artifact_link(k, r.id, v.id, "uses")
    q = lineage_svc.get_lineage(k, r.id)
    assert q["artifacts"][0]["edge_id"] == edge.id
    assert q["artifacts"][0]["artifact_version_id"] == v.id
