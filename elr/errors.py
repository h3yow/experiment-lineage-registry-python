"""Error types with distinguishable, stable codes.

Every domain-level failure raises an :class:`ELRError` carrying a machine
readable ``code`` plus a human readable explanation. API layer maps codes to
HTTP statuses; tests assert on codes so failure causes stay distinguishable.
"""
from __future__ import annotations


class ELRError(Exception):
    """Base class for all registry errors."""

    code: str = "elr_error"
    http_status: int = 400

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "details": self.details,
        }


class NotFoundError(ELRError):
    code = "not_found"
    http_status = 404


class AlreadyExistsError(ELRError):
    code = "already_exists"
    http_status = 409


class InvalidStateTransitionError(ELRError):
    code = "invalid_state_transition"
    http_status = 409


class TerminalFieldWriteError(ELRError):
    code = "terminal_field_write"
    http_status = 409


class IdempotencyConflictError(ELRError):
    code = "idempotency_conflict"
    http_status = 409


class FingerprintMismatchError(ELRError):
    code = "fingerprint_mismatch"
    http_status = 422


class LineageCycleError(ELRError):
    code = "lineage_cycle"
    http_status = 409


class LineageReferenceError(ELRError):
    code = "lineage_reference"
    http_status = 409


class ArtifactReferencedError(ELRError):
    code = "artifact_referenced"
    http_status = 409


class CapacityLimitReachedError(ELRError):
    code = "capacity_limit_reached"
    http_status = 507


class MissingBaselineError(ELRError):
    code = "missing_baseline"
    http_status = 409


class InconsistentCaliberError(ELRError):
    code = "inconsistent_caliber"
    http_status = 422


class MissingMetricError(ELRError):
    code = "missing_metric"
    http_status = 409


class InvalidConfigError(ELRError):
    code = "invalid_config"
    http_status = 400


class DanglingReferenceError(ELRError):
    code = "dangling_reference"
    http_status = 409
