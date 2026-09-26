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
* ``OracleRefreshMaterializedViewExpression`` — ``DBMS_MVIEW.REFRESH`` inside a
  PL/SQL block. Oracle has **no** standalone ``REFRESH MATERIALIZED VIEW``
  statement, so refreshing is a stored-procedure call.

Clause order follows the Oracle SQL Language Reference grammar:
``name [column_aliases] [TABLESPACE] [BUILD ...] [REFRESH ...]
[ENABLE|DISABLE QUERY REWRITE] AS subquery``.

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleMaterializedViewMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

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

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_create_materialized_view_statement"


class OracleCreateMaterializedViewLogExpression(BaseExpression):
    """Oracle ``CREATE MATERIALIZED VIEW LOG ON t WITH ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        table: name of the master (base) table to log.
        with_rowid: emit ``WITH ROWID``.
        with_primary_key: emit ``WITH PRIMARY KEY``.

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

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_create_materialized_view_log_statement"


class OracleDropMaterializedViewExpression(BaseExpression):
    """Oracle ``DROP MATERIALIZED VIEW ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        view_name: name of the materialized view to drop.
        if_exists: if True, emit ``IF EXISTS`` (Oracle 23ai+).
        preserve_table: if True, append ``PRESERVE TABLE`` to keep the
            underlying container table.

    Raises:
        ValueError: if ``view_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        view_name: str,
        if_exists: bool = False,
        preserve_table: bool = False,
    ):
        super().__init__(dialect)
        if not isinstance(view_name, str) or not view_name.strip():
            raise ValueError("view_name must be a non-empty string")
        self.view_name = view_name
        self.if_exists = bool(if_exists)
        self.preserve_table = bool(preserve_table)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_drop_materialized_view_statement"


class OracleRefreshMethod(Enum):
    """``DBMS_MVIEW.REFRESH`` method codes.

    These are the *procedure* codes, which differ from the ``REFRESH ON DEMAND``
    / ``REFRESH FAST`` DDL keywords: ``f`` fast, ``C`` complete, ``?`` force,
    ``P`` partition change tracking, ``A`` always (equivalent to complete).
    """

    FAST = "F"
    COMPLETE = "C"
    FORCE = "?"
    PARTITION_CHANGE_TRACKING = "P"
    ALWAYS = "A"


class OracleRefreshMaterializedViewExpression(BaseExpression):
    """Oracle ``DBMS_MVIEW.REFRESH('mv', ...)`` expression.

    Oracle exposes materialized view refresh only through the
    ``DBMS_MVIEW.REFRESH`` PL/SQL procedure, so the rendered statement is a
    ``BEGIN ... END;`` anonymous block.

    Args:
        dialect: the Oracle dialect instance.
        view_name: name of the materialized view to refresh.
        schema: optional owner/schema of the materialized view.
        method: refresh method code, e.g. ``OracleRefreshMethod.COMPLETE``.
        atomic_refresh: refresh the list in a single transaction (server default
            is ``TRUE``).
        out_of_place: perform an out-of-place refresh.
        nested: also refresh dependent materialized views in dependency order.
        parallelism: DML parallelism degree (``0`` = serial).
        purge_option: ``0`` none, ``1`` lazy (server default), ``2`` aggressive.
        refresh_after_errors: continue past conflicts in ``DEFERROR``.
        push_deferred_rpc: push deferred changes from an updatable MV first.

    Raises:
        ValueError: if ``view_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        view_name: str,
        schema: Optional[str] = None,
        method: Optional[OracleRefreshMethod] = None,
        atomic_refresh: Optional[bool] = None,
        out_of_place: Optional[bool] = None,
        nested: Optional[bool] = None,
        parallelism: Optional[int] = None,
        purge_option: Optional[int] = None,
        refresh_after_errors: Optional[bool] = None,
        push_deferred_rpc: Optional[bool] = None,
    ):
        super().__init__(dialect)
        if not isinstance(view_name, str) or not view_name.strip():
            raise ValueError("view_name must be a non-empty string")
        if schema is not None and (not isinstance(schema, str) or not schema.strip()):
            raise ValueError("schema must be a non-empty string when provided")
        if purge_option is not None and purge_option not in (0, 1, 2):
            raise ValueError("purge_option must be 0 (none), 1 (lazy) or 2 (aggressive)")
        if parallelism is not None and (not isinstance(parallelism, int) or parallelism < 0):
            raise ValueError("parallelism must be a non-negative integer")
        self.view_name = view_name
        self.schema = schema
        self.method = method
        self.atomic_refresh = atomic_refresh
        self.out_of_place = out_of_place
        self.nested = nested
        self.parallelism = parallelism
        self.purge_option = purge_option
        self.refresh_after_errors = refresh_after_errors
        self.push_deferred_rpc = push_deferred_rpc

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_refresh_materialized_view_statement"
