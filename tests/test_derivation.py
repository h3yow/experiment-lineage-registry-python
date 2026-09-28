"""Run derivation: branch new runs from terminal sources with explicit
replacements. Covers inheritance, itemized changes, identity rules,
fingerprint recomputation and source-run immutability.
"""

from __future__ import annotations

import pytest

from elreg.errors import (
    DanglingReferenceError,
    DerivationError,
    IdempotencyConflictError,
    NotFoundError,
)
from elreg.schemas import (
    ArtifactRegister,
    DeriveRunCreate,
    ExperimentCreate,
    RunCreate,
)


def _experiment(service) -> str:
    return service.create_experiment(ExperimentCreate(name="e")).id


def _terminal_run(service, *, artifacts=None, env=None, config=None, key="src"):
    exp_id = _experiment(service)
    for aid in artifacts or []:
        service.register_artifact(f"content-{aid}".encode(), ArtifactRegister(id=aid))
    run, _ = service.submit_run(
        RunCreate(
            experiment_id=exp_id,
            idempotency_key=key,
            env=env or {},
            config=config or {},
            artifacts=list(artifacts or []),
        )
    )
    for state in ("queued", "running", "succeeded"):
        service.transition_run(run.id, state)
    return exp_id, run


def test_derivation_inherits_unspecified_inputs(service):
    exp_id, src = _terminal_run(
        service, artifacts=["a@v1"], env={"py": "3.13"}, config={"lr": 0.1}
    )
    result = service.derive_run(
        src.id, DeriveRunCreate(config_overrides={"lr": 0.2})
    )
    child = result.run
    assert result.created is True
    assert child.state == "created"
    assert child.experiment_id == exp_id
    # Unmentioned parts are inherited.
    assert child.env == {"py": "3.13"}
    assert child.config == {"lr": 0.2}
    assert "a@v1" in child.artifacts
    # Child follows its own lifecycle; the record links it to its source.
    assert result.derivation.parent_run_id == src.id
    assert result.derivation.run_id == child.id


def test_only_terminal_runs_can_be_sources(service):
    exp_id = _experiment(service)
    run, _ = service.submit_run(RunCreate(experiment_id=exp_id))
    with pytest.raises(DerivationError) as exc:
        service.derive_run(run.id, DeriveRunCreate())
    assert exc.value.code == "source_run_not_terminal"
    # failed runs are terminal and valid sources too
    for state in ("queued", "running", "failed"):
        service.transition_run(run.id, state)
    result = service.derive_run(run.id, DeriveRunCreate())
    assert result.run.id != run.id


def test_unknown_source_run_rejected(service):
    with pytest.raises(NotFoundError) as exc:
        service.derive_run("run_missing", DeriveRunCreate())
    assert exc.value.code == "run_not_found"


def test_changes_are_itemized_added_removed_replaced(service):
    _, src = _terminal_run(
        service,
        artifacts=["keep@v1", "old@v1", "gone@v1"],
        env={"a": 1, "n": {"x": 1}},
        config={"lr": 0.1},
    )
    service.register_artifact(b"content-new@v2", ArtifactRegister(id="new@v2"))
    service.register_artifact(b"content-add@v1", ArtifactRegister(id="add@v1"))
    result = service.derive_run(
        src.id,
        DeriveRunCreate(
            env_overrides={"b": 2, "n": {"y": 3}},
            env_remove=["a"],
            config_overrides={"lr": 0.2},
            add_artifacts=["add@v1"],
            remove_artifacts=["gone@v1"],
            replace_artifacts=[{"artifact_id": "old@v1", "replacement_id": "new@v2"}],
        ),
    )
    changes = {
        (c["category"], c["key"]): c["change"]
        for c in result.derivation.changes
    }
    assert changes[("env", "a")] == "removed"
    assert changes[("env", "b")] == "added"
    assert changes[("config", "lr")] == "replaced"
    assert changes[("artifact", "gone@v1")] == "removed"
    assert changes[("artifact", "add@v1")] == "added"
    assert changes[("artifact", "old@v1")] == "replaced"
    # Unchanged inputs produce no change entries.
    assert ("artifact", "keep@v1") not in changes
    child_artifacts = set(result.run.artifacts)
    assert child_artifacts == {"keep@v1", "new@v2", "add@v1"}


def test_fingerprint_recomputed_over_replaced_inputs(service):
    _, src = _terminal_run(
        service, artifacts=["a@v1"], env={"py": "3.13"}, config={"lr": 0.1}
    )
    service.register_artifact(b"content-b@v1", ArtifactRegister(id="b@v1"))
    result = service.derive_run(
        src.id,
        DeriveRunCreate(
            config_overrides={"lr": 0.2},
            replace_artifacts=[{"artifact_id": "a@v1", "replacement_id": "b@v1"}],
        ),
    )
    from elreg.fingerprint import fingerprint_run_inputs

    expected = fingerprint_run_inputs(
        {"py": "3.13"}, {"lr": 0.2}, [service.get_artifact("b@v1").sha256]
    )
    assert result.run.fingerprint == expected
    assert result.run.fingerprint != src.fingerprint


def test_equivalent_requests_and_volatile_fields_do_not_move_fingerprint(service):
    _, src = _terminal_run(service, env={"py": "3.13"}, config={"lr": 0.1})
    r1 = service.derive_run(
        src.id,
        DeriveRunCreate(
            env_overrides={"ts": "t1", "k": "v"},
        ),
    )
    # Same logical override with a different volatile time value: the child
    # fingerprint must not drift and the request still canonicalizes to itself.
    r2 = service.derive_run(
        src.id,
        DeriveRunCreate(
            env_overrides={"k": "v", "ts": "t2"},
        ),
    )
    assert r1.run.fingerprint == r2.run.fingerprint
    assert r1.run.id == r2.run.id


def test_same_parent_same_replacements_return_same_child(service):
    _, src = _terminal_run(service, artifacts=["a@v1"], config={"lr": 0.1})
    service.register_artifact(b"content-b@v1", ArtifactRegister(id="b@v1"))
    body = dict(
        idempotency_key="d-key",
        config_overrides={"lr": 0.2},
        replace_artifacts=[{"artifact_id": "a@v1", "replacement_id": "b@v1"}],
    )
    first = service.derive_run(src.id, DeriveRunCreate(**body))
    # Reordered/duplicated dependency listings are equivalent requests.
    second = service.derive_run(
        src.id,
        DeriveRunCreate(
            idempotency_key="d-key",
            config_overrides={"lr": 0.2},
            replace_artifacts=[
                {"replacement_id": "b@v1", "artifact_id": "a@v1"},
            ],
            add_artifacts=[],
        ),
    )
    third = service.derive_run(
        src.id,
        DeriveRunCreate(
            config_overrides={"lr": 0.2},
            replace_artifacts=[{"artifact_id": "a@v1", "replacement_id": "b@v1"}],
        ),
    )
    assert first.created is True
    assert second.created is False and second.run.id == first.run.id
    assert third.created is False and third.run.id == first.run.id
    assert len(service.list_direct_children(src.id)) == 1


def test_same_key_different_replacements_rejected(service):
    _, src = _terminal_run(service, config={"lr": 0.1})
    service.derive_run(
        src.id, DeriveRunCreate(idempotency_key="k", config_overrides={"lr": 0.2})
    )
    with pytest.raises(IdempotencyConflictError) as exc:
        service.derive_run(
            src.id, DeriveRunCreate(idempotency_key="k", config_overrides={"lr": 0.9})
        )
    assert exc.value.code == "derivation_payload_mismatch"
    # No silent second run.
    assert len(service.list_direct_children(src.id)) == 1


def test_same_key_on_different_parents_is_independent(service):
    _, src1 = _terminal_run(service, key="s1", config={"lr": 0.1})
    _, src2 = _terminal_run(service, key="s2", config={"lr": 0.1})
    r1 = service.derive_run(src1.id, DeriveRunCreate(idempotency_key="k"))
    r2 = service.derive_run(src2.id, DeriveRunCreate(idempotency_key="k"))
    assert r1.run.id != r2.run.id


def test_source_run_is_never_mutated(service):
    exp_id, src = _terminal_run(
        service, artifacts=["a@v1"], env={"py": "3.13"}, config={"lr": 0.1}
    )
    before = service.get_run(src.id)
    service.derive_run(
        src.id,
        DeriveRunCreate(
            env_overrides={"py": "3.99"},
            config_overrides={"lr": 9.9},
            remove_artifacts=["a@v1"],
        ),
    )
    after = service.get_run(src.id)
    assert after.env == before.env
    assert after.config == before.config
    assert after.fingerprint == before.fingerprint
    assert after.state == before.state == "succeeded"
    assert after.artifacts == before.artifacts == ["a@v1"]
    # Terminal-state writes on the source remain forbidden.
    from elreg.errors import TerminalWriteError

    with pytest.raises(TerminalWriteError):
        service.transition_run(src.id, "running")


def test_derivation_rejects_target_that_does_not_exist(service):
    _, src = _terminal_run(service)
    with pytest.raises(DanglingReferenceError) as exc:
        service.derive_run(src.id, DeriveRunCreate(add_artifacts=["ghost@v9"]))
    assert exc.value.code == "artifact_not_found"
    assert service.list_direct_children(src.id) == []
    # No half-created run: total runs is just the source.
    assert len(service.list_runs()) == 1


def test_removing_path_that_does_not_exist_is_rejected(service):
    _, src = _terminal_run(service, env={"a": 1})
    with pytest.raises(DerivationError) as exc:
        service.derive_run(src.id, DeriveRunCreate(env_remove=["nope.path"]))
    assert exc.value.code == "invalid_derivation"
    assert service.list_direct_children(src.id) == []


def test_replacing_non_dependency_is_rejected(service):
    _, src = _terminal_run(service)
    service.register_artifact(b"content-x", ArtifactRegister(id="x@v1"))
    with pytest.raises(DerivationError) as exc:
        service.derive_run(
            src.id,
            DeriveRunCreate(
                replace_artifacts=[{"artifact_id": "x@v1", "replacement_id": "x@v1"}]
            ),
        )
    assert exc.value.code == "invalid_derivation"


def test_deep_merge_keeps_siblings_and_remove_paths(service):
    _, src = _terminal_run(service, env={"n": {"x": 1, "y": 2}})
    result = service.derive_run(
        src.id, DeriveRunCreate(env_overrides={"n": {"z": 3}}, env_remove=["n.x"])
    )
    assert result.run.env == {"n": {"y": 2, "z": 3}}


def test_reverse_trace_reports_source_and_request(service):
    _, src = _terminal_run(service, config={"lr": 0.1})
    result = service.derive_run(
        src.id,
        DeriveRunCreate(
            idempotency_key="trace",
            config_overrides={"lr": 0.2},
            metadata={"why": "tune lr"},
        ),
    )
    view = service.get_derivation(result.run.id)
    assert view.parent_run_id == src.id
    assert view.run_id == result.run.id
    assert view.idempotency_key == "trace"
    assert view.request["config_overrides"] == {"lr": 0.2}
    assert view.changes  # persisted, reverse-traceable


def test_get_derivation_of_underived_run_404(service):
    _, src = _terminal_run(service)
    with pytest.raises(NotFoundError) as exc:
        service.get_derivation(src.id)
    assert exc.value.code == "derivation_not_found"


def test_derivation_can_target_other_experiment(service):
    _, src = _terminal_run(service)
    other = service.create_experiment(ExperimentCreate(name="other"))
    result = service.derive_run(
        src.id, DeriveRunCreate(experiment_id=other.id)
    )
    assert result.run.experiment_id == other.id


def test_decision_logs_carry_run_and_parent_ids(service, caplog):
    _, src = _terminal_run(service, config={"lr": 0.1})
    result = service.derive_run(
        src.id, DeriveRunCreate(config_overrides={"lr": 0.2})
    )
    records = [
        r
        for r in caplog.records
        if r.name == "elreg.service" and r.getMessage() == "run_derived"
    ]
    assert len(records) == 1
    kv = records[0].kv
    assert kv["parent_run_id"] == src.id
    assert kv["child_run_id"] == result.run.id
    assert "request_hash" in kv and "change_count" in kv
