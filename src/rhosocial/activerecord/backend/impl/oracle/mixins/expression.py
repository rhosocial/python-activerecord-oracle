# src/rhosocial/activerecord/backend/impl/oracle/mixins/expression.py
"""Oracle expression-formatting delegation mixin.

Routes Oracle-specific expression formatters (connect_by, pivot,
unpivot, hint, for_update) back through the expression's own
``to_sql(self)`` dispatch, which is the standard pattern used by
the core ``ExpressionMixin``.
"""

from typing import List, Tuple, TYPE_CHECKING


class OracleExpressionMixin:
    """Oracle-specific expression formatting that delegates to ``to_sql``."""

    def format_table(self, expr) -> Tuple[str, Tuple]:
        """Format a :class:`TableExpression` for Oracle.

        Oracle-specific ``@dblink`` suffixes and flashback clauses carried on
        the expression are rendered after the name, then the optional alias.
        """
        schema_name = getattr(expr, "schema_name", None)
        alias = getattr(expr, "alias", None)
        dblink = getattr(expr, "dblink", None)
        flashback = getattr(expr, "flashback", None)
        name_need_quote = getattr(expr, "name_need_quote", True)
        schema_need_quote = getattr(expr, "schema_need_quote", True)
        alias_need_quote = getattr(expr, "alias_need_quote", True)

        if schema_name:
            table_sql = (
                f"{self.format_identifier(schema_name, schema_need_quote)}."
                f"{self.format_identifier(expr.name, name_need_quote)}"
            )
        else:
            table_sql = self.format_identifier(expr.name, name_need_quote)

        table_params: Tuple = ()
        if dblink:
            table_sql = f"{table_sql}@{self.format_identifier(dblink)}"
        if flashback is not None:
            flash_sql, flash_params = flashback.to_sql()
            table_sql = f"{table_sql} {flash_sql}"
            table_params = tuple(flash_params)
        if alias:
            table_sql = f"{table_sql} {self.format_identifier(alias, alias_need_quote)}"
        return table_sql, table_params

    def format_connect_by(self, expr) -> Tuple[str, List]:
        return expr.to_sql()

    def format_pivot(self, expr) -> Tuple[str, List]:
        return expr.to_sql()

    def format_unpivot(self, expr) -> Tuple[str, List]:
        return expr.to_sql()

    def format_hint(self, expr) -> Tuple[str, List]:
        return expr.to_sql()

    def format_for_update(self, expr) -> Tuple[str, List]:
        return expr.to_sql()

    def format_query_statement(self, expr) -> Tuple[str, tuple]:
        """Oracle SELECT builder.

        Oracle (unlike PostgreSQL/MySQL/SQLite) rejects ``SELECT *, expr AS x``
        with ``ORA-00923: FROM keyword not found where expected``. The wildcard
        must be qualified with a table qualifier (``t.*``) whenever it shares
        the select-list with other expressions — which is exactly the shape
        produced by ActiveRecord's ``derived=True`` query option.

        This override runs the core builder, then post-processes the resulting
        SQL by qualifying any bare ``*`` select-item with the FROM source's
        table alias (or name, when no alias is present). The rewrite is only
        applied when a bare ``*`` coexists with other select items, so single-
        column selects and plain ``SELECT *`` statements are untouched.
        """
        sql, params = super().format_query_statement(expr)

        if len(expr.select) <= 1:
            return sql, params

        from_ = getattr(expr, "from_", None)
        qualifier = None
        if from_ is not None and not isinstance(from_, (str, list)):
            qualifier = getattr(from_, "alias", None) or getattr(from_, "name", None)

        if qualifier is None:
            return sql, params

        try:
            upper_sql = sql.upper()
            sel_idx = upper_sql.find("SELECT")
            if sel_idx == -1:
                return sql, params
            # Skip past optional DISTINCT/ALL modifier to reach the select list.
            from_idx = upper_sql.find(" FROM ", sel_idx + 6)
            if from_idx == -1:
                return sql, params
            select_body = sql[sel_idx + 6:from_idx]
            upper_body = select_body.upper()
            items = [s.strip() for s in select_body.split(",")]
            upper_items = [s.strip() for s in upper_body.split(",")]

            wildcard_positions = [
                i for i, item in enumerate(upper_items) if item == "*"
            ]
            if not wildcard_positions:
                return sql, params

            qualifier_sql = self.format_identifier(qualifier)
            for i in wildcard_positions:
                items[i] = f"{qualifier_sql}.*"

            new_select_body = ", ".join(items)
            return sql[:sel_idx + 6] + " " + new_select_body + sql[from_idx:], params
        except Exception:
            return sql, params