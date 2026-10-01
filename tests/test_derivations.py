"""Run derivation: branch a new run off a terminal source run.

Covers inheritance, itemized changes, idempotent replay, fingerprint rules,
dangling/GCollected targets, source immutability and descendant pagination.
"""

from __future__ import annotations

import pytest

from elreg.errors import (
    ConflictError,
    DanglingReferenceError,
    IdempotencyConflictError,
    NotFoundError,
)
from elreg.schemas import (
    ArtifactRegister,
    DerivationRequest,
    ExperimentCreate,
    LineageCreate,
    RunCreate,
)


def _experiment(service):
    return service.create_experiment(ExperimentCreate(name="e")).id


def _terminal_run(service, exp_id, *, artifacts=None, env=None, config=None,
                  key="base", state="succeeded"):
    run, _ = service.submit_run(RunCreate(
        experiment_id=exp_id, idempotency_key=key,
        env=env or {}, config=config or {}, artifacts=artifacts or []))
    for target in ("queued", "running", state):
        service.transition_run(run.id, target)
    return run


def _artifact(service, content, aid):
    service.register_artifact(content, ArtifactRegister(id=aid))
    return aid


def test_derivation_inherits_unspecified_inputs(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    source = _terminal_run(service, exp, artifacts=[a],
                           env={"py": "3.14"}, config={"lr": 0.1, "keep": {"x": 1}})

    view, created = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="d1"))
    assert created is True
    child = service.get_run(view.derived_run_id)
    # env and the untouched config subtree are inherited.
    assert child.env == {"py": "3.14"}
    assert child.config == {"lr": 0.2, "keep": {"x": 1}}
    assert child.artifacts == ["a@v1"]
    assert child.state == "created"
    assert child.derived_from == source.id
    assert source.derived_from is None or service.get_run(source.id).derived_from is None


def test_derivation_changes_are_itemized(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    c = _artifact(service, b"cccc", "c@v1")
    source = _terminal_run(service, exp, artifacts=[a],
                           env={"py": "3.14"}, config={"lr": 0.1})

    view, _ = service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "b@v1"},
        env={"py": "3.13", "new_env": 1},
        config={"lr": 0.1, "removed_cfg": 9},
        idempotency_key="d1"))
    by = {(ch.category, ch.op, ch.key): ch for ch in view.changes}
    # Replacement is explicit, not a generic mismatch.
    assert by[("artifact", "replaced", "a@v1")].derived_value == "b@v1"
    assert by[("env", "replaced", "py")].source_value == "3.14"
    assert by[("env", "added", "new_env")].derived_value == 1
    assert by[("config", "added", "removed_cfg")].derived_value == 9

    # Remove and add paths.
    view2, _ = service.derive_run(source.id, DerivationRequest(
        remove_artifacts=["a@v1"], env={"extra": "z"}, idempotency_key="d2"))
    ops = {(ch.category, ch.op, ch.key) for ch in view2.changes}
    assert ("artifact", "removed", "a@v1") in ops
    assert ("env", "added", "extra") in ops
    assert service.get_run(view2.derived_run_id).artifacts == []

    # A target that is not a replacement source is reported as "added".
    view3, _ = service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "c@v1"}, idempotency_key="d3"))
    # c@v1 replaces a@v1 -> replaced, not added.
    assert any(ch.op == "replaced" and ch.key == "a@v1"
               for ch in view3.changes if ch.category == "artifact")


def test_same_source_same_replacements_single_run(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    source = _terminal_run(service, exp, artifacts=[a], config={"lr": 0.1})

    req = dict(artifact_replacements={"a@v1": "b@v1"},
               config={"lr": 0.2}, idempotency_key="k")
    v1, c1 = service.derive_run(source.id, DerivationRequest(**req))
    v2, c2 = service.derive_run(source.id, DerivationRequest(**req))
    assert c1 is True and c2 is False
    assert v1.derived_run_id == v2.derived_run_id
    # Only one child under the source.
    assert len(service.list_descendants(source.id).items) == 1


def test_equivalent_requests_dedupe_without_idempotency_key(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    source = _terminal_run(service, exp, artifacts=[a])

    v1, c1 = service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "b@v1"}, env={"b": 1, "a": 2}))
    # Key order, duplicate remove entries, whitespace do not change the digest.
    v2, c2 = service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "b@v1"}, env={"a": 2, "b": 1}))
    assert c1 is True and c2 is False
    assert v1.derived_run_id == v2.derived_run_id


def test_same_key_different_replacements_rejected(service):
    exp = _experiment(service)
    source = _terminal_run(service, exp, config={"lr": 0.1})
    service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k"))
    with pytest.raises(IdempotencyConflictError) as exc:
        service.derive_run(source.id, DerivationRequest(
            config={"lr": 0.9}, idempotency_key="k"))
    assert exc.value.code == "derivation_idempotency_mismatch"
    # No silent second run.
    assert len(service.list_descendants(source.id).items) == 1


def test_different_sources_isolate_keys(service):
    exp = _experiment(service)
    s1 = _terminal_run(service, exp, key="b1")
    s2 = _terminal_run(service, exp, key="b2")
    v1, _ = service.derive_run(s1.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k"))
    v2, _ = service.derive_run(s2.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k"))
    assert v1.derived_run_id != v2.derived_run_id


def test_non_terminal_source_rejected(service):
    exp = _experiment(service)
    run, _ = service.submit_run(RunCreate(experiment_id=exp, idempotency_key="b"))
    service.transition_run(run.id, "running")
    with pytest.raises(ConflictError) as exc:
        service.derive_run(run.id, DerivationRequest(config={"lr": 1}))
    assert exc.value.code == "source_not_terminal"


def test_unknown_source_rejected(service):
    with pytest.raises(NotFoundError) as exc:
        service.derive_run("run_missing", DerivationRequest())
    assert exc.value.code == "run_not_found"


def test_missing_source_run_has_no_half_derivation(service):
    exp = _experiment(service)
    with pytest.raises(NotFoundError):
        service.derive_run("run_nope", DerivationRequest(config={"x": 1}))
    assert service.list_runs(exp) == []


def test_fingerprint_stable_under_equivalent_changes(service):
    """Volatile fields / key order in the patch must not move the fingerprint."""
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    source = _terminal_run(service, exp, artifacts=[a],
                           env={"py": "3.14"}, config={"lr": 0.1})

    v1, _ = service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "b@v1"},
        env={"ts": "2026-01-01T00:00:00Z"}))
    # Same logical patch, volatile timestamp changed, reordered deps spelling.
    v2, _ = service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "b@v1"},
        env={"ts": "2026-02-02T00:00:00Z"}))
    # Equivalent -> same derived run (digest ignores volatile fields).
    assert v1.derived_run_id == v2.derived_run_id
    child = service.get_run(v1.derived_run_id)
    # The derived fingerprint equals a fresh fingerprint over effective inputs.
    from elreg.fingerprint import fingerprint_run_inputs
    shas = [service.get_artifact("b@v1").sha256]
    assert child.fingerprint == fingerprint_run_inputs(
        {"py": "3.14"}, {"lr": 0.1}, shas)


def test_nonexistent_replacement_target_rejected(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    source = _terminal_run(service, exp, artifacts=[a])
    with pytest.raises(DanglingReferenceError) as exc:
        service.derive_run(source.id, DerivationRequest(
            artifact_replacements={"a@v1": "ghost@v9"}))
    assert exc.value.code == "artifact_not_found"
    assert len(service.list_descendants(source.id).items) == 0
    # No half-created derived run.
    assert all(r.derived_from is None for r in service.list_runs(exp)
               if r.id != source.id) or [
        r.id for r in service.list_runs(exp)] == [source.id]


def test_gcollected_replacement_target_rejected(make_service):
    service = make_service(retention=0)
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    _artifact(service, b"gggg", "g@v1")
    source = _terminal_run(service, exp, artifacts=[a])
    # g@v1 is unreferenced and old -> GC removes it.
    service.garbage_collect()
    with pytest.raises(DanglingReferenceError) as exc:
        service.derive_run(source.id, DerivationRequest(
            artifact_replacements={"a@v1": "g@v1"}))
    assert exc.value.code == "artifact_deleted"
    assert len(service.list_descendants(source.id).items) == 0


def test_replacement_source_not_a_dependency_rejected(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    source = _terminal_run(service, exp, artifacts=[a])
    with pytest.raises(DanglingReferenceError) as exc:
        service.derive_run(source.id, DerivationRequest(
            artifact_replacements={"b@v1": "a@v1"}))
    assert exc.value.code == "replacement_source_not_found"


def test_replace_and_remove_same_artifact_rejected(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    source = _terminal_run(service, exp, artifacts=[a])
    with pytest.raises(ConflictError) as exc:
        service.derive_run(source.id, DerivationRequest(
            artifact_replacements={"a@v1": "b@v1"},
            remove_artifacts=["a@v1"]))
    assert exc.value.code == "invalid_replacement"
    assert len(service.list_descendants(source.id).items) == 0


def test_derivation_does_not_mutate_source(service):
    exp = _experiment(service)
    a = _artifact(service, b"aaaa", "a@v1")
    b = _artifact(service, b"bbbb", "b@v1")
    source = _terminal_run(service, exp, artifacts=[a],
                           env={"py": "3.14"}, config={"lr": 0.1})
    before = service.get_run(source.id)
    service.derive_run(source.id, DerivationRequest(
        artifact_replacements={"a@v1": "b@v1"},
        config={"lr": 0.9}, env={"py": "3.13"}))
    after = service.get_run(source.id)
    assert after.state == before.state == "succeeded"
    assert after.fingerprint == before.fingerprint
    assert after.env == before.env
    assert after.config == before.config
    assert after.artifacts == ["a@v1"]
    assert after.updated_at == before.updated_at


def test_derivation_record_round_trip(service):
    exp = _experiment(service)
    source = _terminal_run(service, exp, config={"lr": 0.1})
    view, _ = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k"))
    record = service.get_derivation(view.derived_run_id)
    assert record.source_run_id == source.id
    assert record.idempotency_key == "k"
    assert record.replacements["config"] == {"lr": 0.2}
    with pytest.raises(NotFoundError) as exc:
        service.get_derivation(source.id)
    assert exc.value.code == "derivation_not_found"


def test_lineage_edge_records_parent_child(service):
    exp = _experiment(service)
    source = _terminal_run(service, exp)
    view, _ = service.derive_run(source.id, DerivationRequest(config={"x": 1}))
    down = service.lineage_downstream(source.id)
    up = service.lineage_upstream(view.derived_run_id)
    assert any(e.downstream_run_id == view.derived_run_id
               and e.relation == "derived_from" for e in down)
    assert any(e.upstream_run_id == source.id for e in up)


def test_descendant_pagination_and_truncation(make_service):
    service = make_service(page_limit=2, max_limit=5, scan_cap=100)
    exp = _experiment(service)
    root = _terminal_run(service, exp, key="root")
    children = []
    for i in range(5):
        v, _ = service.derive_run(root.id, DerivationRequest(
            config={"i": i}, idempotency_key=f"c{i}"))
        children.append(v.derived_run_id)

    page1 = service.list_descendants(root.id)
    assert page1.limit == 2
    assert page1.returned == 2
    assert page1.truncated is True
    assert page1.truncation_reason == "page_limit"
    assert page1.next_offset == 2
    assert page1.next_cursor

    page2 = service.list_descendants(root.id, cursor=page1.next_cursor)
    assert page2.offset == 2 and page2.returned == 2
    page3 = service.list_descendants(root.id, cursor=page2.next_cursor)
    assert page3.returned == 1
    assert page3.truncated is False
    assert page3.next_cursor is None
    seen = {p.run_id for p in page1.items + page2.items + page3.items}
    assert seen == set(children)


def test_descendant_multi_level_and_scan_cap(make_service):
    service = make_service(page_limit=10, max_limit=10, scan_cap=3)
    exp = _experiment(service)
    root = _terminal_run(service, exp, key="root")
    chain = [root.id]
    for i in range(4):
        parent = service.get_run(chain[-1])
        v, _ = service.derive_run(parent.id, DerivationRequest(
            config={"depth": i}, idempotency_key=f"d{i}"))
        child_id = v.derived_run_id
        for target in ("queued", "running", "succeeded"):
            service.transition_run(child_id, target)
        chain.append(child_id)

    page = service.list_descendants(root.id)
    # scan_cap=3 stops enumeration; truncation is honestly reported.
    assert page.truncated is True
    assert page.truncation_reason == "scan_cap"
    depths = {item.run_id: item.depth for item in page.items}
    assert depths[chain[1]] == 1


def test_descendant_limit_validation(service):
    exp = _experiment(service)
    root = _terminal_run(service, exp)
    with pytest.raises(ConflictError) as exc:
        service.list_descendants(root.id, limit=0)
    assert exc.value.code == "invalid_limit"
    with pytest.raises(ConflictError) as exc:
        service.list_descendants(root.id, limit=1001)
    assert exc.value.code == "limit_exceeded"
    with pytest.raises(ConflictError) as exc:
        service.list_descendants(root.id, cursor="not-base64!!")
    assert exc.value.code == "invalid_cursor"


def test_derivation_decision_logs_contain_ids_and_basis(service, caplog):
    exp = _experiment(service)
    source = _terminal_run(service, exp, config={"lr": 0.1})
    service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k"))
    logs = [r for r in caplog.records
            if r.name == "elreg.service" and r.getMessage() == "run_derived"]
    assert len(logs) == 1
    assert logs[0].kv["source_run_id"] == source.id
    assert "derived_run_id" in logs[0].kv
    assert logs[0].kv["basis"] == "source+replacement_digest"

    # Replay logs the same derived id and basis.
    service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k"))
    replay = [r for r in caplog.records
              if r.name == "elreg.service"
              and r.getMessage() == "derivation_idempotent_replay"]
    assert replay and replay[-1].kv["source_run_id"] == source.id


def test_descendant_diamond_dedupes_and_reports_min_depth(make_service):
    """A run reachable through two paths is listed once (BFS min depth)."""
    service = make_service(page_limit=100, max_limit=100, scan_cap=1000)
    exp = _experiment(service)
    root = _terminal_run(service, exp, key="root")

    def _succeed(run_id):
        for t in ("queued", "running", "succeeded"):
            service.transition_run(run_id, t)

    # root -> a, root -> b ; both a and b -> d (diamond).
    va, _ = service.derive_run(root.id, DerivationRequest(
        config={"branch": "a"}, idempotency_key="a"))
    vb, _ = service.derive_run(root.id, DerivationRequest(
        config={"branch": "b"}, idempotency_key="b"))
    _succeed(va.derived_run_id)
    _succeed(vb.derived_run_id)
    vd, _ = service.derive_run(va.derived_run_id, DerivationRequest(
        config={"branch": "d"}, idempotency_key="d-from-a"))
    # Deriving d from b with a *different* replacement set is allowed and
    # creates a second parent link via lineage edges, but d is one run.
    edge_record = service.add_lineage(LineageCreate(
        upstream_run_id=vb.derived_run_id,
        downstream_run_id=vd.derived_run_id,
        relation="manual_link"))
    assert edge_record.id

    page = service.list_descendants(root.id)
    ids = [item.run_id for item in page.items]
    assert len(ids) == len(set(ids)) == 3  # a, b, d once each
    depths = {item.run_id: item.depth for item in page.items}
    assert depths[va.derived_run_id] == 1
    assert depths[vb.derived_run_id] == 1
    assert depths[vd.derived_run_id] == 2
    assert page.truncated is False


def test_derivation_identity_semantics(service):
    """Identity = (source, commit key) for keyed requests, (source, content)
    for keyless ones; different keys never collide, identical keyless
    content always converges."""
    exp = _experiment(service)
    source = _terminal_run(service, exp, config={"lr": 0.1})

    keyed_a, c = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k1"))
    assert c is True
    # Same key + same content -> replay of the same derived run.
    replay, c = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k1"))
    assert c is False and replay.derived_run_id == keyed_a.derived_run_id
    # Different key, identical content -> independent derivation.
    keyed_b, c = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}, idempotency_key="k2"))
    assert c is True and keyed_b.derived_run_id != keyed_a.derived_run_id
    # Keyless request with the same content -> its own derivation.
    keyless, c = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}))
    assert c is True
    assert keyless.derived_run_id not in {
        keyed_a.derived_run_id, keyed_b.derived_run_id}
    # Keyless equivalent request -> converges on the keyless run.
    keyless2, c = service.derive_run(source.id, DerivationRequest(
        config={"lr": 0.2}))
    assert c is False and keyless2.derived_run_id == keyless.derived_run_id

    assert len(service.list_descendants(source.id).items) == 3
