"""Logging helpers.

Log lines are single-line ``key=value`` pairs so test output can be grepped
for run identifiers, state transitions and decision reasons.
"""

from __future__ import annotations

import logging

_CONFIGURED = False


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Standard fields first, then any structured extras on the record.
        parts = [
            self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            f"level={record.levelname}",
            f"logger={record.name}",
            f"event={record.getMessage()}",
        ]
        for key, value in getattr(record, "kv", {}).items():
            parts.append(f"{key}={value}")
        return " ".join(parts)


def _mark_handler(handler: logging.StreamHandler) -> logging.StreamHandler:
    handler._elreg_handler = True  # type: ignore[attr-defined]
    return handler


def configure_logging(level: str = "INFO") -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = _mark_handler(logging.StreamHandler())
    handler.setFormatter(_KeyValueFormatter())
    root = logging.getLogger("elreg")
    if not any(getattr(h, "_elreg_handler", False) for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(level)
    # Propagate as well so pytest's caplog can capture run ids and decisions.
    root.propagate = True
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name if name.startswith("elreg") else f"elreg.{name}")
