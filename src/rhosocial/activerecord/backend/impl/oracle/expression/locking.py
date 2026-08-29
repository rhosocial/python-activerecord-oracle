# src/rhosocial/activerecord/backend/impl/oracle/expression/locking.py
"""
Oracle locking expressions.

Oracle provides multiple FOR UPDATE variants for row-level locking,
enabling fine-grained control over locking behavior.
"""
from __future__ import annotations

from typing import Optional, List, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleForUpdateExpression(BaseExpression):
    """Oracle FOR UPDATE locking clause.

    Oracle supports multiple variants of FOR UPDATE for row-level locking:
    - FOR UPDATE - Basic locking, waits for locks
    - FOR UPDATE NOWAIT - Fail immediately if rows are locked
    - FOR UPDATE WAIT n - Wait n seconds for locks
    - FOR UPDATE SKIP LOCKED - Skip locked rows (11g+)
    - FOR UPDATE OF columns - Lock specific tables in joins

    Example SQL:
        SELECT * FROM users WHERE status = 'A' FOR UPDATE NOWAIT
        SELECT * FROM orders o, order_items i
        WHERE o.id = i.order_id FOR UPDATE OF o.id NOWAIT

    Example usage:
        # Basic FOR UPDATE
        lock = OracleForUpdateExpression(dialect)

        # FOR UPDATE NOWAIT
        lock = OracleForUpdateExpression(dialect, nowait=True)

        # FOR UPDATE WAIT 10
        lock = OracleForUpdateExpression(dialect, wait_seconds=10)

        # FOR UPDATE SKIP LOCKED
        lock = OracleForUpdateExpression(dialect, skip_locked=True)

        # FOR UPDATE OF specific columns
        lock = OracleForUpdateExpression(dialect, columns=["id", "name"], nowait=True)

    Args:
        dialect: the Oracle dialect instance.
        columns: Optional list of columns to lock (in joins)
        nowait: If True, fail immediately if rows are locked
        wait_seconds: Wait time in seconds (mutually exclusive with nowait)
        skip_locked: If True, skip locked rows (11g+)

    Raises:
        ValueError: if NOWAIT / WAIT n / SKIP LOCKED options are combined,
            or if ``wait_seconds`` is negative.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        columns: Optional[List[str]] = None,
        nowait: bool = False,
        wait_seconds: Optional[int] = None,
        skip_locked: bool = False,
    ):
        super().__init__(dialect)
        # Validate options are mutually exclusive
        options = [nowait, wait_seconds is not None, skip_locked]
        if sum(options) > 1:
            raise ValueError(
                "NOWAIT, WAIT n, and SKIP LOCKED are mutually exclusive"
            )
        if wait_seconds is not None and wait_seconds < 0:
            raise ValueError("wait_seconds must be non-negative")
        self.columns = columns
        self.nowait = bool(nowait)
        self.wait_seconds = wait_seconds
        self.skip_locked = bool(skip_locked)

    def to_sql(self) -> SQLQueryAndParams:
        """Generate FOR UPDATE SQL."""
        parts = ["FOR UPDATE"]

        if self.columns:
            cols = ", ".join(self.dialect.format_identifier(c) for c in self.columns)
            parts.append(f"OF {cols}")

        if self.nowait:
            parts.append("NOWAIT")
        elif self.wait_seconds is not None:
            parts.append(f"WAIT {self.wait_seconds}")
        elif self.skip_locked:
            parts.append("SKIP LOCKED")

        return (" ".join(parts), ())


def for_update(dialect: "OracleDialect") -> OracleForUpdateExpression:
    """Create basic FOR UPDATE expression.

    Args:
        dialect: the Oracle dialect instance.

    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(dialect)


def for_update_nowait(
    dialect: "OracleDialect",
    columns: Optional[List[str]] = None,
) -> OracleForUpdateExpression:
    """Create FOR UPDATE NOWAIT expression.

    Args:
        dialect: the Oracle dialect instance.
        columns: Optional columns to lock

    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(dialect, columns=columns, nowait=True)


def for_update_wait(
    dialect: "OracleDialect",
    seconds: int,
    columns: Optional[List[str]] = None,
) -> OracleForUpdateExpression:
    """Create FOR UPDATE WAIT n expression.

    Args:
        dialect: the Oracle dialect instance.
        seconds: Number of seconds to wait
        columns: Optional columns to lock

    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(dialect, columns=columns, wait_seconds=seconds)


def for_update_skip_locked(
    dialect: "OracleDialect",
    columns: Optional[List[str]] = None,
) -> OracleForUpdateExpression:
    """Create FOR UPDATE SKIP LOCKED expression.

    Args:
        dialect: the Oracle dialect instance.
        columns: Optional columns to lock

    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(dialect, columns=columns, skip_locked=True)


class OracleLockTableExpression(BaseExpression):
    """Oracle LOCK TABLE statement.

    Locks an entire table in exclusive or share mode.

    Example SQL:
        LOCK TABLE users IN EXCLUSIVE MODE NOWAIT
        LOCK TABLE users IN SHARE MODE

    Args:
        dialect: the Oracle dialect instance.
        table: Table name
        mode: Lock mode ('EXCLUSIVE' or 'SHARE')
        nowait: If True, don't wait for lock

    Raises:
        ValueError: if ``mode`` is not a valid Oracle lock mode.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        mode: str = "EXCLUSIVE",
        nowait: bool = False,
    ):
        super().__init__(dialect)
        if mode.upper() not in ("EXCLUSIVE", "SHARE", "ROW EXCLUSIVE", "ROW SHARE"):
            raise ValueError(f"Invalid lock mode: {mode}")
        self.table = table
        self.mode = mode
        self.nowait = bool(nowait)

    def to_sql(self) -> SQLQueryAndParams:
        """Generate LOCK TABLE SQL."""
        sql = f"LOCK TABLE {self.dialect.format_identifier(self.table)} IN {self.mode.upper()} MODE"
        if self.nowait:
            sql += " NOWAIT"
        return (sql, ())
