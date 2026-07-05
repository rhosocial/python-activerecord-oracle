# src/rhosocial/activerecord/backend/impl/oracle/mixins/locking.py
"""Oracle row-level locking mixin.

Oracle provides multiple FOR UPDATE variants for row-level locking:
``FOR UPDATE``, ``FOR UPDATE NOWAIT``, ``FOR UPDATE WAIT n``,
``FOR UPDATE SKIP LOCKED`` (11g+) and ``FOR UPDATE OF cols``. There is no
native ``FOR SHARE`` syntax; SHARE-style locking is expressed via
``FOR UPDATE OF`` against specific columns.

This mixin is mixed into :class:`OracleDialect` alongside the other
dialect mixins. All methods are defensive and side-effect free; they
return strings or booleans only.
"""
from typing import Any, List, Optional, Tuple


class OracleLockingMixin(object):
    """Oracle row-level locking mixin.

    Oracle has no ``SET ... lock_wait_timeout`` equivalent; lock waiting
    is controlled inline via ``WAIT n`` / ``NOWAIT`` on the FOR UPDATE
    clause. The dialect's ``version`` tuple is consulted for
    version-gated features like ``SKIP LOCKED`` (11g+).
    """

    # region capability flags
    def supports_for_share(self) -> bool:
        """Oracle has no native FOR SHARE clause; use FOR UPDATE OF."""
        return False

    def supports_for_update_nowait(self) -> bool:
        """Oracle supports FOR UPDATE NOWAIT."""
        return True

    def supports_for_update_wait(self) -> bool:
        """Oracle supports FOR UPDATE WAIT n."""
        return True

    def supports_for_update_skip_locked(self) -> bool:
        """Oracle supports FOR UPDATE SKIP LOCKED since 11g."""
        version = getattr(self, "version", (11, 0, 0))
        return version >= (11, 0, 0)

    def supports_for_update_of(self) -> bool:
        """Oracle supports column-level FOR UPDATE OF."""
        return True

    def supports_lock_timeout(self) -> bool:
        """Oracle has no SET lock_wait timeout; uses WAIT n / NOWAIT inline."""
        return False
    # endregion

    # region syntax helpers
    def get_lock_wait_syntax(self, wait_seconds: int) -> str:
        """Render inline lock wait syntax for a wait duration.

        Args:
            wait_seconds: non-negative integer seconds.

        Returns:
            ``"WAIT <n>"`` for positive values, ``"NOWAIT"`` for ``0``.
        """
        if wait_seconds is None or wait_seconds == 0:
            return "NOWAIT"
        return f"WAIT {int(wait_seconds)}"

    def format_for_update_clause(
        self,
        strength=None,
        of_columns: Optional[List[str]] = None,
        nowait: bool = False,
        skip_locked: bool = False,
        wait: Optional[int] = None,
    ) -> Tuple[str, tuple]:
        """Compose an Oracle FOR UPDATE clause.

        Oracle lock options are mutually exclusive: at most one of
        NOWAIT / WAIT n / SKIP LOCKED may be applied. Composition order
        matches Oracle's documented grammar: ``FOR UPDATE [OF cols]
        [NOWAIT | WAIT n | SKIP LOCKED]``.

        Args:
            strength: ignored on Oracle; only UPDATE semantics exist.
            of_columns: optional list of column identifiers (strings)
                or expressions to lock in joins.
            nowait: if True, append NOWAIT.
            skip_locked: if True, append SKIP LOCKED (11g+).
            wait: if a non-negative int, append ``WAIT <wait>``.

        Returns:
            ``(sql_string, params_tuple)``; Oracle's FOR UPDATE option
            syntax takes no parameters, so the params tuple is always
            empty unless ``of_columns`` yields expression parameters.
        """
        # `strength` is accepted for API symmetry with PG/MySQL mixins;
        # Oracle only supports FOR UPDATE (SHARE is via FOR UPDATE OF).
        _ = strength

        all_params: List[Any] = []

        sql_parts: List[str] = ["FOR UPDATE"]

        # OF <columns>
        of_columns = of_columns if of_columns is not None else []
        of_parts: List[str] = []
        for col in of_columns:
            if isinstance(col, str):
                of_parts.append(self.format_identifier(col))
            else:
                col_sql, col_params = col.to_sql()
                of_parts.append(col_sql)
                all_params.extend(col_params)
        if of_parts:
            sql_parts.append(f"OF {', '.join(of_parts)}")

        # Wait options: NOWAIT | WAIT n | SKIP LOCKED (mutually exclusive)
        if nowait:
            sql_parts.append("NOWAIT")
        elif skip_locked:
            if not self.supports_for_update_skip_locked():
                from rhosocial.activerecord.backend.dialect.exceptions import (
                    UnsupportedFeatureError,
                )
                raise UnsupportedFeatureError(
                    self.name, "SKIP LOCKED (requires Oracle 11g+)"
                )
            sql_parts.append("SKIP LOCKED")
        elif isinstance(wait, int) and wait >= 0:
            sql_parts.append(f"WAIT {wait}")

        return " ".join(sql_parts), tuple(all_params)
    # endregion
