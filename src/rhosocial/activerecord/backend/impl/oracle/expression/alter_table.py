# src/rhosocial/activerecord/backend/impl/oracle/expression/alter_table.py
"""Oracle ALTER TABLE table-level clause action expressions.

The core ``AlterTableAction`` dispatch covers column/constraint operations
(ADD/DROP/MODIFY COLUMN, ADD/DROP CONSTRAINT, RENAME). Oracle additionally
supports a set of table-level clauses that are expressed through the same
``ALTER TABLE ...`` statement:

* ``OracleSetUnusedColumnsAction`` — ``SET UNUSED (c1, c2)`` (9i).
* ``OracleDropUnusedColumnsAction`` — ``DROP UNUSED COLUMNS`` (9i).
* ``OracleMoveTableAction`` — ``MOVE``.
* ``OracleShrinkSpaceAction`` — ``SHRINK SPACE [CASCADE]`` (10g).
* ``OracleReadOnlyAction`` — ``READ ONLY`` / ``READ WRITE``.
* ``OracleRowMovementAction`` — ``ENABLE | DISABLE ROW MOVEMENT``.

Each action overrides ``to_sql()`` to delegate to the corresponding dialect
``format_*`` formatter implemented by ``OracleModifyColumnMixin``, keeping the
core ``AlterTableExpression`` action-dispatch mechanism unchanged.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import SQLQueryAndParams
from rhosocial.activerecord.backend.expression.statements.ddl_alter import AlterTableAction

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleSetUnusedColumnsAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... SET UNUSED (c1, c2)`` action.

    Marks one or more columns as unused without physically dropping them or
    reclaiming their space; the metadata is retained until
    ``DROP UNUSED COLUMNS`` is issued.

    Args:
        dialect: the Oracle dialect instance.
        columns: list of column names to mark as unused.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``columns`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        columns: List[str],
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not columns:
            raise ValueError("columns must be a non-empty list")
        self.columns = list(columns)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_set_unused_action(self)


class OracleDropUnusedColumnsAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... DROP UNUSED COLUMNS`` action.

    Physically drops all columns previously marked with ``SET UNUSED`` and
    reclaims their space.

    Args:
        dialect: the Oracle dialect instance.
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_unused_columns_action(self)


class OracleMoveTableAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... MOVE`` action.

    Physically relocates the table (segment), typically to reclaim fragmented
    space or to move it to another tablespace.

    Args:
        dialect: the Oracle dialect instance.
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_move_table_statement(self)


class OracleShrinkSpaceAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... SHRINK SPACE [CASCADE]`` action.

    Shrinks the table segment online to reclaim unused space (10g+; requires
    row movement to be enabled).

    Args:
        dialect: the Oracle dialect instance.
        cascade: when True, also shrink dependent segments (indexes, LOBs).
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        cascade: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.cascade = bool(cascade)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_shrink_space_statement(self)


class OracleReadOnlyAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... READ ONLY | READ WRITE`` action.

    Toggles a table between read-only (no DML allowed on the table or its
    dependents) and read-write state.

    Args:
        dialect: the Oracle dialect instance.
        read_only: when True emit ``READ ONLY``, otherwise ``READ WRITE``.
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        read_only: bool = True,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.read_only = bool(read_only)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_read_only_statement(self)


class OracleRowMovementAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... ENABLE | DISABLE ROW MOVEMENT`` action.

    Enables or disables row movement, which allows Oracle to move a row to a
    different partition/segment during an update (required for partition
    updates and segment shrink).

    Args:
        dialect: the Oracle dialect instance.
        enable: when True emit ``ENABLE ROW MOVEMENT``, otherwise ``DISABLE
            ROW MOVEMENT``.
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        enable: bool = True,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.enable = bool(enable)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_row_movement_statement(self)
