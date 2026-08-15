# src/rhosocial/activerecord/backend/impl/oracle/expression/materialized_view.py
"""Oracle materialized view DDL expressions.

This module defines backend-specific expressions for Oracle materialized
views and materialized view logs:

* ``OracleCreateMaterializedViewExpression`` — ``CREATE MATERIALIZED VIEW``
  with the Oracle-specific ``REFRESH`` / ``QUERY REWRITE`` / ``BUILD``
  option set.
* ``OracleCreateMaterializedViewLogExpression`` — ``CREATE MATERIALIZED VIEW
  LOG ON t WITH { ROWID | PRIMARY KEY }``.
* ``OracleDropMaterializedViewExpression`` — ``DROP MATERIALIZED VIEW`` with
  the optional ``PRESERVE TABLE`` clause.

Clause order follows the Oracle SQL Language Reference grammar:
``name [column_aliases] [TABLESPACE] [BUILD ...] [REFRESH ...]
[ENABLE|DISABLE QUERY REWRITE] AS subquery``.

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleMaterializedViewMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class MaterializedViewRefreshMethod(Enum):
    """Oracle materialized view refresh methods."""

    FAST = "FAST"
    COMPLETE = "COMPLETE"
    FORCE = "FORCE"


class MaterializedViewRefreshTrigger(Enum):
    """Oracle materialized view refresh trigger modes."""

    ON_COMMIT = "ON COMMIT"
    ON_DEMAND = "ON DEMAND"


class MaterializedViewBuildMode(Enum):
    """Oracle materialized view build (initial population) modes."""

    IMMEDIATE = "IMMEDIATE"
    DEFERRED = "DEFERRED"


class OracleCreateMaterializedViewExpression(BaseExpression):
    """Oracle ``CREATE MATERIALIZED VIEW ... AS SELECT ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        view_name: name of the materialized view to create.
        query: a ``BaseExpression`` (typically a ``QueryExpression``) whose
            SQL becomes the ``AS subquery`` clause.
        if_not_exists: if True, emit ``IF NOT EXISTS`` (Oracle 23ai+).
        column_aliases: optional column alias list rendered in parentheses
            after the view name.
        tablespace: optional ``TABLESPACE`` clause.
        build_mode: ``BUILD IMMEDIATE`` / ``BUILD DEFERRED`` clause.
        refresh_method: ``REFRESH FAST | COMPLETE | FORCE`` clause.
        refresh_trigger: ``ON COMMIT`` / ``ON DEMAND`` clause (requires the
            ``REFRESH`` clause).
        query_rewrite: ``ENABLE QUERY REWRITE`` when True, ``DISABLE QUERY
            REWRITE`` when False, omitted when None.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``view_name`` is empty.
        TypeError: if ``query`` is not a ``BaseExpression``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        view_name: str,
        query: BaseExpression,
        if_not_exists: bool = False,
        column_aliases: Optional[List[str]] = None,
        tablespace: Optional[str] = None,
        build_mode: Optional[MaterializedViewBuildMode] = None,
        refresh_method: Optional[MaterializedViewRefreshMethod] = None,
        refresh_trigger: Optional[MaterializedViewRefreshTrigger] = None,
        query_rewrite: Optional[bool] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(view_name, str) or not view_name.strip():
            raise ValueError("view_name must be a non-empty string")
        if not isinstance(query, BaseExpression):
            raise TypeError(
                "query must be a BaseExpression, "
                f"got {type(query).__name__}"
            )
        self.view_name = view_name
        self.query = query
        self.if_not_exists = bool(if_not_exists)
        self.column_aliases = list(column_aliases) if column_aliases else []
        self.tablespace = tablespace
        self.build_mode = build_mode
        self.refresh_method = refresh_method
        self.refresh_trigger = refresh_trigger
        self.query_rewrite = query_rewrite
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_materialized_view_statement(self)


class OracleCreateMaterializedViewLogExpression(BaseExpression):
    """Oracle ``CREATE MATERIALIZED VIEW LOG ON t WITH ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        table: name of the master (base) table to log.
        with_rowid: emit ``WITH ROWID``.
        with_primary_key: emit ``WITH PRIMARY KEY``.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``table`` is empty, or neither ``with_rowid`` nor
            ``with_primary_key`` is True.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        with_rowid: bool = False,
        with_primary_key: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(table, str) or not table.strip():
            raise ValueError("table must be a non-empty string")
        if not with_rowid and not with_primary_key:
            raise ValueError(
                "materialized view log requires WITH ROWID and/or WITH PRIMARY KEY"
            )
        self.table = table
        self.with_rowid = bool(with_rowid)
        self.with_primary_key = bool(with_primary_key)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_materialized_view_log_statement(self)


class OracleDropMaterializedViewExpression(BaseExpression):
    """Oracle ``DROP MATERIALIZED VIEW ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        view_name: name of the materialized view to drop.
        if_exists: if True, emit ``IF EXISTS`` (Oracle 23ai+).
        preserve_table: if True, append ``PRESERVE TABLE`` to keep the
            underlying container table.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``view_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        view_name: str,
        if_exists: bool = False,
        preserve_table: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(view_name, str) or not view_name.strip():
            raise ValueError("view_name must be a non-empty string")
        self.view_name = view_name
        self.if_exists = bool(if_exists)
        self.preserve_table = bool(preserve_table)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_materialized_view_statement(self)
