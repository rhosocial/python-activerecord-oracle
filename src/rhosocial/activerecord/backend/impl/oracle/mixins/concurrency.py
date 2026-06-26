# src/rhosocial/activerecord/backend/impl/oracle/mixins/concurrency.py
"""Oracle concurrency hint mixin."""

import logging
from typing import Optional

from rhosocial.activerecord.backend.protocols import ConcurrencyHint


class OracleConcurrencyMixin:
    """Mixin providing Oracle-specific concurrency hint."""

    _concurrency_hint: Optional[ConcurrencyHint] = None

    def connect(self):
        super().connect()
        self._fetch_concurrency_hint()

    def _fetch_concurrency_hint(self) -> None:
        try:
            cursor = self._connection.cursor()
            cursor.execute(
                "SELECT VALUE FROM V$PARAMETER WHERE NAME = 'processes'"
            )
            row = cursor.fetchone()
            cursor.close()

            if row:
                max_processes = int(row[0])
                pool_size = getattr(self.config, "pool_max", 5) or 5
                limit = min(max_processes, pool_size)
                self._concurrency_hint = ConcurrencyHint(
                    max_concurrency=limit,
                    reason=f"min(processes={max_processes}, pool_max={pool_size})",
                )
                self.log(
                    logging.DEBUG,
                    f"Concurrency hint: max_concurrency={limit}",
                )
        except Exception as e:
            self.log(logging.WARNING, f"Failed to fetch concurrency hint: {e}")
            self._concurrency_hint = None

    def get_concurrency_hint(self) -> Optional[ConcurrencyHint]:
        return self._concurrency_hint