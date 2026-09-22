"""Run lifecycle state machine.

States::

    PENDING -> RUNNING -> SUCCEEDED | FAILED
    PENDING -> CANCELED
    RUNNING -> CANCELED

``SUCCEEDED``, ``FAILED`` and ``CANCELED`` are terminal: no transition may
leave them, and after reaching a terminal state the fingerprint-relevant key
fields (environment / config / artifact manifest) are frozen.
"""
from __future__ import annotations

from enum import Enum

from .errors import InvalidStateTransitionError


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


TERMINAL_STATES: frozenset[RunStatus] = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}
)

_ALLOWED: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.PENDING: frozenset(
        {RunStatus.RUNNING, RunStatus.CANCELED, RunStatus.FAILED}
    ),
    RunStatus.RUNNING: frozenset(
        {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}
    ),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELED: frozenset(),
}


def is_terminal(status: RunStatus | str) -> bool:
    return RunStatus(status) in TERMINAL_STATES


def ensure_transition(current: RunStatus | str, target: RunStatus | str) -> RunStatus:
    """Validate a state transition, raising a distinguishable error otherwise.

    Distinguishes:
      * target is not a known state -> invalid_state_transition (bad target);
      * current already terminal and target equals current -> no-op is the
        caller's choice, this function rejects any transition out of terminal;
      * transition not in the allowed map.
    """
    try:
        current_s = RunStatus(current)
    except ValueError:
        raise InvalidStateTransitionError(
            f"unknown current run state: {current!r}",
            details={"current": str(current)},
        )
    try:
        target_s = RunStatus(target)
    except ValueError:
        raise InvalidStateTransitionError(
            f"unknown target run state: {target!r}",
            details={"target": str(target)},
        )
    if current_s in TERMINAL_STATES:
        raise InvalidStateTransitionError(
            f"run already in terminal state {current_s.value}; transition to "
            f"{target_s.value} is forbidden",
            details={"current": current_s.value, "target": target_s.value,
                     "reason": "terminal"},
        )
    if target_s not in _ALLOWED[current_s]:
        raise InvalidStateTransitionError(
            f"illegal transition {current_s.value} -> {target_s.value}",
            details={"current": current_s.value, "target": target_s.value,
                     "allowed": sorted(s.value for s in _ALLOWED[current_s]),
                     "reason": "not_allowed"},
        )
    return target_s
