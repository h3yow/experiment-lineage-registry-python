"""Structured audit logging helpers (run id, transitions, decision basis)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_logger(name: str = "elr") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def record_audit(conn, run_id: str | None, action: str, details: dict) -> None:
    conn.execute(
        "INSERT INTO audit_log(run_id, action, details, created_at) "
        "VALUES (?,?,?,?)",
        (run_id, action, __import__("json").dumps(details, sort_keys=True,
         ensure_ascii=False), utcnow()),
    )
