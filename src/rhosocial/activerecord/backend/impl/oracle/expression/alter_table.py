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

from typing import List, TYPE_CHECKING

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

    Raises:
        ValueError: if ``columns`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        columns: List[str],
    ):
        super().__init__(dialect)
        if not columns:
            raise ValueError("columns must be a non-empty list")
        self.columns = list(columns)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_set_unused_action"


class OracleDropUnusedColumnsAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... DROP UNUSED COLUMNS`` action.

    Physically drops all columns previously marked with ``SET UNUSED`` and
    reclaims their space.

    Args:
        dialect: the Oracle dialect instance.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
    ):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_drop_unused_columns_action"


class OracleMoveTableAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... MOVE`` action.

    Physically relocates the table (segment), typically to reclaim fragmented
    space or to move it to another tablespace.

    Args:
        dialect: the Oracle dialect instance.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
    ):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_move_table_statement"


class OracleShrinkSpaceAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... SHRINK SPACE [CASCADE]`` action.

    Shrinks the table segment online to reclaim unused space (10g+; requires
    row movement to be enabled).

    Args:
        dialect: the Oracle dialect instance.
        cascade: when True, also shrink dependent segments (indexes, LOBs).
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        cascade: bool = False,
    ):
        super().__init__(dialect)
        self.cascade = bool(cascade)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_shrink_space_statement"


class OracleReadOnlyAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... READ ONLY | READ WRITE`` action.

    Toggles a table between read-only (no DML allowed on the table or its
    dependents) and read-write state.  Each spelling has its own parameter and
    the clause is mandatory in the action's grammar: exactly one of
    ``read_only=True`` / ``read_write=True`` must be set.

    Args:
        dialect: the Oracle dialect instance.
        read_only: when True emit ``READ ONLY``.
        read_write: when True emit ``READ WRITE``.

    Raises:
        ValueError: if both parameters are set, or neither is.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        read_only: bool = False,
        read_write: bool = False,
    ):
        super().__init__(dialect)
        if read_only and read_write:
            raise ValueError("read_only and read_write are mutually exclusive options")
        if not read_only and not read_write:
            raise ValueError(
                "READ ONLY/READ WRITE requires exactly one of read_only=True "
                "or read_write=True"
            )
        self.read_only = bool(read_only)
        self.read_write = bool(read_write)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_read_only_statement"


class OracleRowMovementAction(AlterTableAction):
    """Oracle ``ALTER TABLE ... ENABLE | DISABLE ROW MOVEMENT`` action.

    Enables or disables row movement, which allows Oracle to move a row to a
    different partition/segment during an update (required for partition
    updates and segment shrink).  Each spelling has its own parameter and the
    clause is mandatory in the action's grammar: exactly one of
    ``enable=True`` / ``disable=True`` must be set.

    Args:
        dialect: the Oracle dialect instance.
        enable: when True emit ``ENABLE ROW MOVEMENT``.
        disable: when True emit ``DISABLE ROW MOVEMENT``.

    Raises:
        ValueError: if both parameters are set, or neither is.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        enable: bool = False,
        disable: bool = False,
    ):
        super().__init__(dialect)
        if enable and disable:
            raise ValueError("enable and disable are mutually exclusive options")
        if not enable and not disable:
            raise ValueError(
                "ENABLE/DISABLE ROW MOVEMENT requires exactly one of "
                "enable=True or disable=True"
            )
        self.enable = bool(enable)
        self.disable = bool(disable)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_row_movement_statement"
