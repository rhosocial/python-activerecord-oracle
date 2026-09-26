# src/rhosocial/activerecord/backend/impl/oracle/mixins/materialized_view.py
"""Oracle materialized view DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import (
        CreateMaterializedViewExpression,
        DropMaterializedViewExpression,
    )
    from ..expression.materialized_view import (
        OracleCreateMaterializedViewLogExpression,
        OracleRefreshMaterializedViewExpression,
    )


class OracleMaterializedViewMixin:
    """Oracle materialized view capability checks and formatters.

    Materialized views (and their logs) are supported since Oracle 8.1; the
    formatters here gate on ``(9, 0, 0)`` per the backend implementation
    contract. This mixin overrides the generic :class:`ViewMixin` formatters
    so that Oracle's ``REFRESH`` / ``QUERY REWRITE`` / ``BUILD`` option set
    is emitted.
    """

    def supports_materialized_view(self) -> bool:
        return True

    def supports_refresh_materialized_view(self) -> bool:
        """Oracle refreshes through ``DBMS_MVIEW.REFRESH`` (no SQL statement).

        The procedure has existed since Oracle 8i, so this is not version gated.
        """
        return True

    def supports_materialized_view_log(self) -> bool:
        return True

    def supports_materialized_view_tablespace(self) -> bool:
        return True

    def format_create_materialized_view_statement(
        self, expr: "CreateMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE MATERIALIZED VIEW",
                suggestion=(
                    f"Oracle {self.version} does not support materialized "
                    "views; they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE MATERIALIZED VIEW"]
        if getattr(expr, "if_not_exists", False):
            if self.version < (23, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS",
                    suggestion=(
                        f"Oracle {self.version} does not support IF NOT "
                        "EXISTS; graceful DDL requires Oracle 23ai or later."
                    ),
                )
            parts.append("IF NOT EXISTS")
        parts.append(self.format_identifier(expr.view_name))
        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")
        if expr.tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")
        build_mode = getattr(expr, "build_mode", None)
        if build_mode is not None:
            parts.append(f"BUILD {build_mode.value}")
        elif hasattr(expr, "with_data"):
            parts.append("BUILD IMMEDIATE" if expr.with_data else "BUILD DEFERRED")
        refresh_method = getattr(expr, "refresh_method", None)
        refresh_trigger = getattr(expr, "refresh_trigger", None)
        if refresh_method is not None or refresh_trigger is not None:
            refresh_parts = ["REFRESH"]
            if refresh_method is not None:
                refresh_parts.append(refresh_method.value)
            if refresh_trigger is not None:
                refresh_parts.append(refresh_trigger.value)
            parts.append(" ".join(refresh_parts))
        query_rewrite = getattr(expr, "query_rewrite", None)
        if query_rewrite is not None:
            parts.append("ENABLE QUERY REWRITE" if query_rewrite else "DISABLE QUERY REWRITE")
        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")
        return " ".join(parts), query_params

    def format_create_materialized_view_log_statement(
        self, expr: "OracleCreateMaterializedViewLogExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE MATERIALIZED VIEW LOG",
                suggestion=(
                    f"Oracle {self.version} does not support materialized "
                    "view logs; they require Oracle 9i or later."
                ),
            )
        with_parts = []
        if expr.with_rowid:
            with_parts.append("ROWID")
        if expr.with_primary_key:
            with_parts.append("PRIMARY KEY")
        table_sql = self.format_identifier(expr.table)
        return f"CREATE MATERIALIZED VIEW LOG ON {table_sql} WITH {', '.join(with_parts)}", ()

    def format_drop_materialized_view_statement(
        self, expr: "DropMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "DROP MATERIALIZED VIEW",
                suggestion=(
                    f"Oracle {self.version} does not support materialized "
                    "views; they require Oracle 9i or later."
                ),
            )
        parts = ["DROP MATERIALIZED VIEW"]
        if getattr(expr, "if_exists", False):
            if self.version < (23, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP MATERIALIZED VIEW IF EXISTS",
                    suggestion=(
                        f"Oracle {self.version} does not support IF EXISTS; "
                        "graceful DDL requires Oracle 23ai or later."
                    ),
                )
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.view_name))
        if getattr(expr, "preserve_table", False):
            parts.append("PRESERVE TABLE")
        return " ".join(parts), ()

    def format_refresh_materialized_view_statement(
        self, expr: "OracleRefreshMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Refresh a materialized view via ``DBMS_MVIEW.REFRESH``.

        Oracle has no ``REFRESH MATERIALIZED VIEW`` statement; refresh is a call
        to the ``DBMS_MVIEW`` PL/SQL package, so the rendered SQL is an
        anonymous ``BEGIN ... END;`` block.

        Also serves the generic ``RefreshMaterializedViewExpression``; its
        SQL-standard options have no Oracle counterpart and are handled as:
        ``concurrent`` is rejected (Oracle refreshes hold the appropriate locks
        and offer no concurrent mode), ``with_data`` is ignored (a refresh
        always repopulates).

        Args:
            expr: Oracle or generic refresh expression.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: if ``concurrent`` was requested.
        """
        if getattr(expr, "concurrent", False):
            raise UnsupportedFeatureError(
                self.name,
                "REFRESH MATERIALIZED VIEW CONCURRENTLY",
                suggestion=(
                    "Oracle has no concurrent refresh mode; use "
                    "DBMS_MVIEW.REFRESH with atomic_refresh or out_of_place."
                ),
            )

        name = self._materialized_view_refresh_target(expr)
        args = [f"'{name}'"]

        method = getattr(expr, "method", None)
        if method is not None:
            args.append(f"method => '{method.value}'")
        for attr in (
            "atomic_refresh",
            "out_of_place",
            "nested",
            "refresh_after_errors",
            "push_deferred_rpc",
        ):
            value = getattr(expr, attr, None)
            if value is not None:
                args.append(f"{attr} => {'TRUE' if value else 'FALSE'}")
        for attr in ("parallelism", "purge_option"):
            value = getattr(expr, attr, None)
            if value is not None:
                args.append(f"{attr} => {value}")

        call = f"DBMS_MVIEW.REFRESH({', '.join(args)});"
        return f"BEGIN {call} END;", ()

    def _materialized_view_refresh_target(self, expr) -> str:
        """Build the materialized view name passed to ``DBMS_MVIEW.REFRESH``.

        The procedure takes a *string* name, not an identifier, so the name is
        upper-cased (unquoted identifiers fold to upper case in Oracle) and any
        embedded single quote is escaped.
        """
        name = str(getattr(expr, "view_name", "") or "").strip()
        if not name:
            raise ValueError("materialized view refresh requires a view name")
        schema = getattr(expr, "schema", None)
        if schema:
            name = f"{schema}.{name}"
        return name.replace("'", "''").upper()
