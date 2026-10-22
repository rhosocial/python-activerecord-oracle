# src/rhosocial/activerecord/backend/impl/oracle/mixins/materialized_view.py
"""Oracle materialized view DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import (
        CreateMaterializedViewStatement,
        DropMaterializedViewStatement,
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

    def supports_with_data_clause(self) -> bool:
        """Oracle has no ``WITH [NO] DATA`` clause.

        Measured on 18c/21c/23c: the SQL-standard clause is rejected both on
        ``CREATE TABLE ... AS`` and on ``CREATE MATERIALIZED VIEW`` (ORA-00933
        on 18c/21c, ORA-03048 on 23c, in either clause position).  The only
        population spellings Oracle accepts are ``BUILD IMMEDIATE`` / ``BUILD
        DEFERRED``, which this mixin's formatter emits for ``with_data`` /
        ``no_data``; the ``CREATE TABLE ... AS`` and ``REFRESH MATERIALIZED
        VIEW`` paths have no such hook and therefore refuse the request by
        name.
        """
        return False

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
        self, expr: "CreateMaterializedViewStatement"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE MATERIALIZED VIEW`` with Oracle's option set.

        Raises:
            TypeError: ``expr.view`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.MaterializedView`.
                A plain view would render its own name, producing a well-formed
                CREATE MATERIALIZED VIEW over that view's name.
            UnsupportedFeatureError: The Oracle version is below 9i, or a
                graceful-DDL option was asked for below 23ai.
        """
        from rhosocial.activerecord.backend.expression.objects import MaterializedView
        from ..expression.materialized_view import OracleCreateMaterializedViewExpression

        if not isinstance(expr.view, MaterializedView):
            raise TypeError(
                f"CreateMaterializedViewStatement.view must be a MaterializedView, "
                f"got {type(expr.view).__name__}"
            )

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
        # The Oracle statement and the core statement are two contracts that
        # share one formatter: the Oracle one declares ``if_not_exists`` and
        # the BUILD / REFRESH / QUERY REWRITE option set, the core one the
        # SQL-standard ``with_data`` / ``no_data`` pair. Each is read on the
        # contract that declares it, dispatched by class rather than probed.
        if isinstance(expr, OracleCreateMaterializedViewExpression):
            if expr.if_not_exists:
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
        parts.append(expr.view.to_sql()[0])
        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")
        if expr.tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")
        if isinstance(expr, OracleCreateMaterializedViewExpression):
            build_mode = expr.build_mode
            if build_mode is not None:
                parts.append(f"BUILD {build_mode.value}")
            refresh_method = expr.refresh_method
            refresh_trigger = expr.refresh_trigger
            if refresh_method is not None or refresh_trigger is not None:
                refresh_parts = ["REFRESH"]
                if refresh_method is not None:
                    refresh_parts.append(refresh_method.value)
                if refresh_trigger is not None:
                    refresh_parts.append(refresh_trigger.value)
                parts.append(" ".join(refresh_parts))
            if expr.enable_query_rewrite:
                parts.append("ENABLE QUERY REWRITE")
            elif expr.disable_query_rewrite:
                parts.append("DISABLE QUERY REWRITE")
        else:
            # Oracle spells the SQL-standard WITH DATA as BUILD IMMEDIATE
            # and WITH NO DATA as BUILD DEFERRED.
            if expr.with_data:
                parts.append("BUILD IMMEDIATE")
            elif expr.no_data:
                parts.append("BUILD DEFERRED")
        query_sql, query_params = expr.query.to_sql()
        if query_params:
            # Oracle's DDL statements never carry bind variables, and the
            # driver refuses the statement before the server sees it
            # ("DPY-4009: 0 positional bind values are required but 1 were
            # provided").  The statement is refused by name instead.
            raise UnsupportedFeatureError(
                self.name,
                "CREATE MATERIALIZED VIEW with bound parameters",
                suggestion=(
                    "Oracle's DDL statements never carry bind variables; the "
                    "driver refuses the statement before the server sees it "
                    "('DPY-4009: 0 positional bind values are required but 1 "
                    "were provided'). Build the query with an inlined literal "
                    "instead."
                ),
            )
        parts.append(f"AS {query_sql}")
        return " ".join(parts), query_params

    def format_create_materialized_view_log_statement(
        self, expr: "OracleCreateMaterializedViewLogExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE MATERIALIZED VIEW LOG`` for Oracle.

        Raises:
            TypeError: ``expr.table`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Table`.
                Another catalogue object would render its own name, so the log
                would be taken on it while naming a table.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import Table

        if not isinstance(expr.table, Table):
            raise TypeError(
                f"OracleCreateMaterializedViewLogExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

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
        table_sql = expr.table.to_sql()[0]
        return f"CREATE MATERIALIZED VIEW LOG ON {table_sql} WITH {', '.join(with_parts)}", ()

    def format_drop_materialized_view_statement(
        self, expr: "DropMaterializedViewStatement"
    ) -> Tuple[str, tuple]:
        """Format ``DROP MATERIALIZED VIEW`` for Oracle.

        Raises:
            TypeError: ``expr.view`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.MaterializedView`.
                A plain view would render its own name, so the statement would
                drop a view and name a materialized one.
            UnsupportedFeatureError: The Oracle version is below 9i, or IF
                EXISTS was asked for below 23ai.
        """
        from rhosocial.activerecord.backend.expression.objects import MaterializedView
        from ..expression.materialized_view import OracleDropMaterializedViewExpression

        if not isinstance(expr.view, MaterializedView):
            raise TypeError(
                f"DropMaterializedViewStatement.view must be a MaterializedView, "
                f"got {type(expr.view).__name__}"
            )

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
        if expr.if_exists:
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
        parts.append(expr.view.to_sql()[0])
        # The two statements share one formatter: the Oracle one declares
        # PRESERVE TABLE and neither CASCADE nor RESTRICT, while the core one
        # declares CASCADE / RESTRICT and has no PRESERVE TABLE slot. Each
        # is read on the contract that declares it, dispatched by class
        # rather than probed.
        if isinstance(expr, OracleDropMaterializedViewExpression):
            if expr.preserve_table:
                parts.append("PRESERVE TABLE")
        else:
            # Oracle's DROP MATERIALIZED VIEW grammar has no CASCADE; there
            # is no probe that declares one, so a requested CASCADE is
            # refused by name.
            if expr.cascade:
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP MATERIALIZED VIEW CASCADE",
                    f"{self.name} does not support DROP MATERIALIZED VIEW CASCADE.",
                )
            if expr.restrict:
                if not self.supports_materialized_view_restrict():
                    raise UnsupportedFeatureError(
                        self.name,
                        "DROP MATERIALIZED VIEW RESTRICT",
                        f"{self.name} does not support DROP MATERIALIZED VIEW "
                        "RESTRICT.",
                    )
                parts.append("RESTRICT")
        return " ".join(parts), ()

    def format_refresh_materialized_view_statement(
        self, expr: "OracleRefreshMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Refresh a materialized view via ``DBMS_MVIEW.REFRESH``.

        Oracle has no ``REFRESH MATERIALIZED VIEW`` statement; refresh is a call
        to the ``DBMS_MVIEW`` PL/SQL package, so the rendered SQL is an
        anonymous ``BEGIN ... END;`` block.

        Also serves the generic ``RefreshMaterializedViewStatement``; its
        SQL-standard options have no Oracle counterpart and are handled as:
        ``concurrent`` is rejected (Oracle refreshes hold the appropriate locks
        and offer no concurrent mode), and the ``with_data`` / ``no_data``
        pair is refused by name (``DBMS_MVIEW.REFRESH`` always repopulates, so
        Oracle can spell neither WITH DATA nor WITH NO DATA).

        Args:
            expr: Oracle or generic refresh expression.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            TypeError: ``expr.view`` is not a ``MaterializedView``. The name
                passed to the procedure is read from that object, so any other
                catalogue object would refresh something else while naming it.
            UnsupportedFeatureError: if ``concurrent``, ``with_data`` or
                ``no_data`` was requested.
        """
        from rhosocial.activerecord.backend.expression.objects import MaterializedView
        from ..expression.materialized_view import OracleRefreshMaterializedViewExpression

        if not isinstance(expr.view, MaterializedView):
            raise TypeError(
                f"{type(expr).__name__}.view must be a MaterializedView, "
                f"got {type(expr.view).__name__}"
            )

        # The three SQL-standard options are declared by the core statement
        # alone; Oracle's own refresh expression carries none of them, and
        # its PL/SQL procedure call is built from the Oracle statement's own
        # fields below.
        if isinstance(expr, OracleRefreshMaterializedViewExpression):
            standard_options = ()
        else:
            standard_options = (
                (expr.concurrent, "REFRESH MATERIALIZED VIEW CONCURRENTLY",
                 "Oracle has no concurrent refresh mode; use "
                 "DBMS_MVIEW.REFRESH with atomic_refresh or out_of_place."),
                (expr.with_data, "REFRESH MATERIALIZED VIEW WITH DATA",
                 "Oracle's DBMS_MVIEW.REFRESH always repopulates the view; "
                 "there is no WITH DATA clause to emit."),
                (expr.no_data, "REFRESH MATERIALIZED VIEW WITH NO DATA",
                 "Oracle's DBMS_MVIEW.REFRESH always repopulates the view; "
                 "it cannot refresh without data."),
            )
        for requested, feature, suggestion in standard_options:
            if requested:
                raise UnsupportedFeatureError(
                    self.name,
                    feature,
                    suggestion=suggestion,
                )

        name = self._materialized_view_refresh_target(expr)
        args = [f"'{name}'"]

        if isinstance(expr, OracleRefreshMaterializedViewExpression):
            method = expr.method
            if method is not None:
                args.append(f"method => '{method.value}'")
            # The named arguments are all declared on the Oracle statement;
            # each is spelled where the caller supplied it.
            for flag_name, value in (
                ("atomic_refresh", expr.atomic_refresh),
                ("out_of_place", expr.out_of_place),
                ("nested", expr.nested),
                ("refresh_after_errors", expr.refresh_after_errors),
                ("push_deferred_rpc", expr.push_deferred_rpc),
            ):
                if value is not None:
                    args.append(f"{flag_name} => {'TRUE' if value else 'FALSE'}")
            for value_name, value in (
                ("parallelism", expr.parallelism),
                ("purge_option", expr.purge_option),
            ):
                if value is not None:
                    args.append(f"{value_name} => {value}")

        call = f"DBMS_MVIEW.REFRESH({', '.join(args)});"
        return f"BEGIN {call} END;", ()

    def _materialized_view_refresh_target(self, expr) -> str:
        """Build the materialized view name passed to ``DBMS_MVIEW.REFRESH``.

        The procedure takes a *string* name, not an identifier, so this cannot
        use the SQL identifier renderer -- that would emit double quotes into a
        string literal. What it does do is read the name and its owner from the
        same :class:`MaterializedView` object every other statement in this
        mixin uses, so the owner cannot be dropped on this one path; the slots
        are then folded the way Oracle folds an unquoted name (upper case) and
        the literal's single quotes are escaped.
        """
        parts = [
            part
            for part in (expr.view.schema_name, expr.view.name)
            if part
        ]
        if not parts:
            raise ValueError("materialized view refresh requires a view name")
        return ".".join(parts).replace("'", "''").upper()
