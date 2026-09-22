import pytest

from elr.config import Settings
from elr.errors import InvalidConfigError


def test_defaults_are_documented():
    s = Settings.from_env({})
    assert s.db_path == "data/elr.db"
    assert s.artifact_store == "data/artifacts"
    assert s.capacity_policy == "reject"
    assert s.max_artifact_bytes == 0
    assert s.retention_runs == 0
    assert s.serialize_writes is True


def test_bad_policy_rejected_at_startup():
    with pytest.raises(InvalidConfigError) as ei:
        Settings.from_env({"ELR_CAPACITY_POLICY": "maybe"})
    assert ei.value.details["name"] == "ELR_CAPACITY_POLICY"


def test_bad_integer_rejected():
    with pytest.raises(InvalidConfigError):
        Settings.from_env({"ELR_RETENTION_RUNS": "abc"})
    with pytest.raises(InvalidConfigError):
        Settings.from_env({"ELR_MAX_ARTIFACT_BYTES": "-1"})


def test_bool_and_defer_parsed():
    s = Settings.from_env({"ELR_SERIALIZE_WRITES": "0",
                           "ELR_CAPACITY_POLICY": "DEFER"})
    assert s.serialize_writes is False and s.capacity_policy == "defer"
