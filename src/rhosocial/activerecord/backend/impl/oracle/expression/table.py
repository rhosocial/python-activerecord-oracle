# src/rhosocial/activerecord/backend/impl/oracle/expression/table.py
"""Oracle table-clause expression wrappers.

This module defines backend-specific expressions that wrap the mixin
format methods ``format_table_compression_clause`` and
``format_tablespace_clause`` into proper :class:`BaseExpression`
subclasses, making them composable within the expression tree and
renderable through the unified :meth:`BaseExpression.to_sql` entry
point.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class TableCompressionClauseExpression(BaseExpression):
    """Expression wrapping ``format_table_compression_clause``.

    Renders an Oracle table-compression clause such as ``NOCOMPRESS``
    or ``COMPRESS FOR OLTP``.

    Args:
        dialect: the Oracle dialect instance.
        mode: the compression mode — ``'none'`` (or empty) yields
            ``NOCOMPRESS``; any other value becomes
            ``COMPRESS FOR <MODE>`` (uppercased).
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        mode: str = "BASIC",
    ):
        super().__init__(dialect)
        self.mode = mode

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_table_compression_clause"


class TablespaceClauseExpression(BaseExpression):
    """Expression wrapping ``format_tablespace_clause``.

    Renders an Oracle ``TABLESPACE <name>`` clause.

    Args:
        dialect: the Oracle dialect instance.
        tablespace_name: the tablespace identifier.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        tablespace_name: str,
    ):
        super().__init__(dialect)
        self.tablespace_name = tablespace_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_tablespace_clause"
