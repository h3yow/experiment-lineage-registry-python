"""Distinguishable error types.

Every rejected operation raises a RegistryError subclass whose ``code`` is a
stable, machine-readable string so callers (and tests) can distinguish failure
reasons instead of parsing messages.
"""

from __future__ import annotations


class RegistryError(Exception):
    """Base class for all registry failures."""

    code: str = "registry_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class NotFoundError(RegistryError):
    code = "not_found"


class ConflictError(RegistryError):
    code = "conflict"


class InvalidTransitionError(RegistryError):
    code = "invalid_transition"


class TerminalWriteError(RegistryError):
    code = "terminal_write_denied"


class IdempotencyConflictError(RegistryError):
    code = "idempotency_conflict"


class LineageCycleError(RegistryError):
    code = "lineage_cycle"


class DanglingReferenceError(RegistryError):
    code = "dangling_reference"


class ReproducibilityMismatchError(RegistryError):
    code = "reproducibility_mismatch"


class MetricComparisonError(RegistryError):
    code = "metric_comparison_error"


class CapacityRejectedError(RegistryError):
    code = "capacity_rejected"


class CapacityDelayedError(RegistryError):
    code = "capacity_delayed"


class DerivationError(RegistryError):
    code = "derivation_error"


class ConfigError(RegistryError):
    code = "config_error"
