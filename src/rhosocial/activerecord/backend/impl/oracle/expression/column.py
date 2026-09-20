# src/rhosocial/activerecord/backend/impl/oracle/expression/column.py
"""Oracle-specific column definition expressions.

Oracle extends the standard column definition with per-column attributes that
have no generic equivalent:

* ``INVISIBLE`` — column hidden from ``SELECT *`` (Oracle 12c+).

These live on ``OracleColumnDefinition`` (deriving the generic
``ColumnDefinition``) and are rendered by the Oracle
``format_column_definition`` override. They are declared through
``OracleColumnOptions`` (deriving the generic ``ColumnOptions``).
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.statements import ColumnDefinition
from rhosocial.activerecord.base.ddl.options import ColumnOptions

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "OracleColumnDefinition",
    "OracleColumnOptions",
]


class OracleColumnDefinition(ColumnDefinition):
    """An Oracle column definition extending the generic one.

    Adds the Oracle-only ``invisible`` attribute.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        data_type,
        constraints=None,
        comment: Optional[str] = None,
        generated_expression=None,
        identity: Optional[str] = None,
        identity_start: Optional[int] = None,
        identity_increment: Optional[int] = None,
        identity_clause=None,
        *,
        invisible: Optional[bool] = None,
    ):
        super().__init__(
            dialect,
            name,
            data_type,
            constraints=constraints,
            comment=comment,
            generated_expression=generated_expression,
            identity=identity,
            identity_start=identity_start,
            identity_increment=identity_increment,
            identity_clause=identity_clause,
        )
        self.invisible = invisible


class OracleColumnOptions(ColumnOptions):
    """Oracle per-column options declaration."""

    def __init__(
        self,
        *,
        identity_start: Optional[int] = None,
        identity_increment: Optional[int] = None,
        invisible: Optional[bool] = None,
    ):
        super().__init__(
            identity_start=identity_start,
            identity_increment=identity_increment,
        )
        self.invisible = invisible

    def column_definition_class(self):
        """Build an ``OracleColumnDefinition`` for these options."""
        return OracleColumnDefinition

    def apply_to(self, column) -> None:
        """Transfer the Oracle-only fields onto the column definition."""
        if not isinstance(column, OracleColumnDefinition):
            raise TypeError(
                "OracleColumnOptions.apply_to requires an OracleColumnDefinition, "
                f"got {type(column).__name__}"
            )
        column.invisible = self.invisible
