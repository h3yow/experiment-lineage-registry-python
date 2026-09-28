"""Derivation concurrency, garbage-collection races and descendant/ancestor
capacity (pagination + honest truncation marking).
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from elreg.errors import ConflictError, DanglingReferenceError
from elreg.schemas import (
    ArtifactRegister,
    DeriveRunCreate,
    ExperimentCreate,
    RunCreate,
)


def _terminal(service, *, artifacts=None, config=None, key="src"):
    exp = service.create_experiment(ExperimentCreate(name="e"))
    for aid in artifacts or []:
        service.register_artifact(f"content-{aid}".encode(), ArtifactRegister(id=aid))
    run, _ = service.submit_run(
        RunCreate(
            experiment_id=exp.id,
            idempotency_key=key,
            artifacts=list(artifacts or []),
            config=config or {},
        )
    )
    for state in ("queued", "running", "succeeded"):
        service.transition_run(run.id, state)
    return run


def _terminal_child(service, parent, **derive_kwargs):
    result = service.derive_run(parent.id, DeriveRunCreate(**derive_kwargs))
    for state in ("queued", "running", "succeeded"):
        service.transition_run(result.run.id, state)
    return result.run


def test_concurrent_derivations_of_same_source_produce_single_run(service):
    src = _terminal(service, config={"lr": 0.1})
    barrier = threading.Barrier(12)

    def derive(_):
        barrier.wait()
        return service.derive_run(
            src.id, DeriveRunCreate(idempotency_key="race", config_overrides={"lr": 0.2})
        )

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(derive, range(12)))

    ids = {r.run.id for r in results}
    assert ids == {results[0].run.id}
    assert sum(1 for r in results if r.created) == 1
    assert len(service.list_direct_children(src.id)) == 1
    assert len(service.list_runs()) == 2


def test_concurrent_content_equivalent_keyless_derivations_collapse(service):
    src = _terminal(service, config={"lr": 0.1})
    barrier = threading.Barrier(10)

    def derive(_):
        barrier.wait()
        return service.derive_run(
            src.id, DeriveRunCreate(config_overrides={"lr": 0.2})
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(derive, range(10)))

    assert len({r.run.id for r in results}) == 1
    assert sum(1 for r in results if r.created) == 1


def test_concurrent_derivation_vs_gc_winner_is_consistent(make_service):
    service = make_service(retention=0)
    service.register_artifact(b"race-bytes", ArtifactRegister(id="z@v1"))
    src = _terminal(service)
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, object]] = []

    def derive():
        barrier.wait()
        try:
            result = service.derive_run(src.id, DeriveRunCreate(add_artifacts=["z@v1"]))
            outcomes.append(("derived", result.run.id))
        except DanglingReferenceError as exc:
            outcomes.append(("rejected", exc.code))

    def collect():
        barrier.wait()
        outcomes.append(("gc", service.garbage_collect().removed))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(derive), pool.submit(collect)]
        for future in futures:
            future.result()

    kinds = {kind for kind, _ in outcomes}
    # Exactly one side wins; never both in a torn state.
    derived = [value for kind, value in outcomes if kind == "derived"]
    rejected = [value for kind, value in outcomes if kind == "rejected"]
    removed = [value for kind, value in outcomes if kind == "gc"][0]
    assert len(derived) + len(rejected) == 1
    artifact = service.storage.get_artifact("z@v1")
    if derived:
        # Derivation committed the reference first: GC must retain the artifact.
        assert artifact.deleted is False
        assert "z@v1" not in removed
    else:
        # GC won: derivation was explicitly rejected and no half-run remains.
        assert rejected == ["artifact_deleted"]
        assert artifact.deleted is True
        assert len(service.list_direct_children(src.id)) == 0
        assert len(service.list_runs()) == 1
    assert service.integrity_report()["healthy"] is True
    assert kinds  # sanity: barrier ran both branches


def test_concurrent_new_artifact_registration_and_derivation(service):
    src = _terminal(service)
    barrier = threading.Barrier(2)
    outcomes: list[object] = []

    def register():
        barrier.wait()
        view, created = service.register_artifact(
            b"fresh-bytes", ArtifactRegister(id="fresh@v1")
        )
        outcomes.append(("registered", created, view.id))

    def derive():
        barrier.wait()
        try:
            result = service.derive_run(
                src.id, DeriveRunCreate(add_artifacts=["fresh@v1"])
            )
            outcomes.append(("derived", result.created))
        except DanglingReferenceError as exc:
            outcomes.append(("rejected", exc.code))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(register), pool.submit(derive)]
        for future in futures:
            future.result()

    # Either the artifact existed at decision time (derived ok) or it did not
    # yet (artifact_not_found) - never a reference to a half-registered blob.
    derived = [o for o in outcomes if o[0] == "derived"]
    rejected = [o for o in outcomes if o[0] == "rejected"]
    assert len(derived) + len(rejected) == 1
    if rejected:
        assert rejected[0][1] == "artifact_not_found"
        assert len(service.list_direct_children(src.id)) == 0
    else:
        assert "fresh@v1" in service.list_direct_children(src.id)[0].run.artifacts
    assert service.integrity_report()["healthy"] is True


def test_deleted_target_rejected_without_half_run(make_service):
    service = make_service(retention=0)
    service.register_artifact(b"gone-bytes", ArtifactRegister(id="gone@v1"))
    # GC immediately: unreferenced and past zero-length retention window.
    assert "gone@v1" in service.garbage_collect().removed
    src = _terminal(service)

    with pytest.raises(DanglingReferenceError) as exc:
        service.derive_run(src.id, DeriveRunCreate(add_artifacts=["gone@v1"]))
    assert exc.value.code == "artifact_deleted"
    assert service.list_direct_children(src.id) == []
    assert len(service.list_runs()) == 1


def _chain(service, root, depth):
    current = root
    for i in range(depth):
        current = _terminal_child(service, current, config_overrides={"i": i})
    return current


def test_descendants_pagination_marks_truncation_honestly(tmp_path):
    from tests.conftest import build_settings
    from elreg.service import RegistryService

    service = RegistryService(
        build_settings(tmp_path, derivation_default_limit=3,
                       derivation_max_limit=10)
    )
    service.start()
    try:
        root = _terminal(service)
        _chain(service, root, 7)

        seen: list[str] = []
        cursor = None
        pages = 0
        truncated_flags: list[bool] = []
        while True:
            page = service.list_descendants(root.id, cursor=cursor)
            pages += 1
            seen.extend(r.id for r in page.descendants)
            truncated_flags.append(page.truncated)
            if page.next_cursor is None:
                assert page.truncated is False
                break
            cursor = page.next_cursor
            assert page.truncated is True
        assert pages == 3  # 3 + 3 + 1
        assert len(seen) == len(set(seen)) == 7
        assert truncated_flags[:-1] == [True, True]
    finally:
        service.close()


def test_descendants_fanout_respects_hard_cap(tmp_path):
    from tests.conftest import build_settings
    from elreg.service import RegistryService

    service = RegistryService(
        build_settings(tmp_path, derivation_default_limit=2, derivation_max_limit=5)
    )
    service.start()
    try:
        root = _terminal(service)
        for i in range(6):
            _terminal_child(service, root, config_overrides={"k": i})
        page = service.list_descendants(root.id)
        assert len(page.descendants) == 2
        assert page.truncated is True
        capped = service.list_descendants(root.id, limit=5)
        assert len(capped.descendants) == 5 and capped.truncated is True
        with pytest.raises(ConflictError) as exc:
            service.list_descendants(root.id, limit=6)
        assert exc.value.code == "pagination_limit_exceeded"
        with pytest.raises(ConflictError) as exc_zero:
            service.list_descendants(root.id, limit=0)
        assert exc_zero.value.code == "invalid_pagination_limit"
        with pytest.raises(ConflictError) as exc_cursor:
            service.list_descendants(root.id, cursor="not-a-cursor")
        assert exc_cursor.value.code == "invalid_cursor"
    finally:
        service.close()


def test_ancestors_are_bounded_and_mark_truncated(tmp_path):
    from tests.conftest import build_settings
    from elreg.service import RegistryService

    service = RegistryService(
        build_settings(tmp_path, derivation_default_limit=2, derivation_max_limit=5)
    )
    service.start()
    try:
        root = _terminal(service)
        leaf = _chain(service, root, 4)
        page = service.list_ancestors(leaf.id)  # default limit 2
        assert len(page.ancestors) == 2
        assert page.truncated is True
        full = service.list_ancestors(leaf.id, limit=5)
        assert len(full.ancestors) == 4
        assert full.truncated is False
        # Root itself is underived: its ancestor list is empty/complete.
        assert service.list_ancestors(root.id).ancestors == []
    finally:
        service.close()
