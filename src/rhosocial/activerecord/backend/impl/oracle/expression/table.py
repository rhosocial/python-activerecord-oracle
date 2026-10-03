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

from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import TableExpression

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


class OracleTableExpression(TableExpression):
    """A schema-qualified table reference carrying Oracle's own modifiers.

    Oracle can hang two things off a table name that no other dialect has:
    a database-link suffix (``schema.table@dblink``) and a flashback query
    (``schema.table AS OF SCN ...``). They are declared here as real fields
    rather than attached to a plain core
    :class:`~...expression.core.TableExpression` after construction, so a
    formatter can read them without guessing whether they exist.

    Attributes:
        dblink: Database-link suffix, or None when the table is local.
        flashback: The flashback query expression, or None when the table is
            read at the present.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        name: str,
        schema_name: Optional[str] = None,
        alias: Optional[str] = None,
        temporal_options: Optional[Dict[str, Any]] = None,
        name_need_quote: bool = True,
        alias_need_quote: bool = True,
        schema_need_quote: bool = True,
        dblink: Optional[str] = None,
        flashback: Optional[BaseExpression] = None,
    ):
        """
        Args:
            schema_name: Namespace to qualify the table with, e.g. ``APP``.
                None leaves the name unqualified. An empty string raises
                ValueError, and a dialect with no namespace raises
                UnsupportedFeatureError.
            dblink: Database link through which to reach the table.
            flashback: Flashback query clause limiting the rows read.
        """
        super().__init__(
            dialect,
            name,
            schema_name=schema_name,
            alias=alias,
            temporal_options=temporal_options,
            name_need_quote=name_need_quote,
            alias_need_quote=alias_need_quote,
            schema_need_quote=schema_need_quote,
        )
        self.dblink = dblink
        self.flashback = flashback
