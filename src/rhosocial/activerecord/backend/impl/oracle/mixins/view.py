# src/rhosocial/activerecord/backend/impl/oracle/mixins/view.py
"""Oracle view DDL formatting mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateViewExpression,
        DropViewExpression,
    )


class OracleViewMixin:
    """CREATE VIEW / DROP VIEW formatters for Oracle."""

    def format_create_view_statement(
        self, expr: "CreateViewExpression"
    ) -> Tuple[str, tuple]:
        parts = ["CREATE"]
        if expr.replace:
            parts.append("OR REPLACE")
        parts.append("VIEW")
        parts.append(self.format_identifier(expr.view_name))
        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")
        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")
        if expr.options and expr.options.check_option:
            check_option = expr.options.check_option.value
            parts.append(f"WITH {check_option} CHECK OPTION")
        return " ".join(parts), query_params

    def format_drop_view_statement(
        self, expr: "DropViewExpression"
    ) -> Tuple[str, tuple]:
        parts = ["DROP VIEW"]
        parts.append(self.format_identifier(expr.view_name))
        return " ".join(parts), ()