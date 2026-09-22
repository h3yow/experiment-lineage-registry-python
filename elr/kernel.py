"""Composition root + process-wide write serialization.

All mutating service operations go through :meth:`Kernel.write_tx`; read
operations use :meth:`Kernel.read_lock`. Both acquire the same reentrant lock
that guards the single shared SQLite connection (see store.Store). Effects:

* concurrent updates to the same run are serialized -> one commit wins, no
  state overshoot, no double counting, no cross-thread COMMIT/ROLLBACK mixup;
* concurrent garbage collections interleave with registration/reference as
  full transactions -> readers never see a half-deleted artifact;
* a failing block rolls back entirely, including audit rows written so far.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

from .config import Settings
from .store import Store


class Kernel:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = Store(settings.db_path)
        # serializes mutating operations semantically; reentrant so service
        # helpers may call other helpers inside the same transaction.
        self._write_lock = threading.RLock()

    @property
    def _guard(self):
        return self.store.io_lock

    @contextmanager
    def read_lock(self) -> Iterator[None]:
        self._guard.acquire()
        try:
            yield
        finally:
            self._guard.release()

    def query(self, sql: str, params: tuple | list = ()) -> list:
        """Execute a read query under the connection guard.

        Rows are materialized to a list while the lock is held (sqlite Row
        access after the cursor advances is unsafe across threads).
        """
        with self._guard:
            cur = self.store.conn.execute(sql, tuple(params))
            try:
                return cur.fetchall()
            finally:
                cur.close()

    def query_one(self, sql: str, params: tuple | list = ()):
        rows = self.query(sql, params)
        return rows[0] if rows else None

    @contextmanager
    def write_tx(self):
        if self.settings.serialize_writes:
            self._write_lock.acquire()
        self._guard.acquire()
        conn = self.store.conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
        finally:
            self._guard.release()
            if self.settings.serialize_writes:
                self._write_lock.release()

    def close(self) -> None:
        self.store.close()
