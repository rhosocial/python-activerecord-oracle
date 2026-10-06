# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl_view.py
"""Oracle view DDL formatting mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateViewExpression,
        DropViewExpression,
    )


class OracleViewMixin:
    """CREATE VIEW / DROP VIEW formatters for Oracle."""

    def supports_create_or_replace_view(self) -> bool:
        """Oracle supports CREATE OR REPLACE VIEW."""
        return True

    def supports_if_not_exists_view(self) -> bool:
        """Oracle does not support IF NOT EXISTS for views."""
        return False

    def supports_view_check_option(self) -> bool:
        """Oracle supports ``WITH [LOCAL|CASCADED] CHECK OPTION`` on views."""
        return True

    def format_create_view_statement(
        self, expr: "CreateViewExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [OR REPLACE] VIEW`` for Oracle.

        Raises:
            TypeError: ``expr.view`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.View`.
                A materialized view or a table would each render its own name,
                producing a well-formed CREATE VIEW over that object's name.
        """
        from rhosocial.activerecord.backend.expression.objects import View

        if not isinstance(expr.view, View):
            raise TypeError(
                f"CreateViewExpression.view must be a View, "
                f"got {type(expr.view).__name__}"
            )

        parts = ["CREATE"]
        if expr.replace and self.supports_create_or_replace_view():
            parts.append("OR REPLACE")
        parts.append("VIEW")
        # The view is a catalogue object on the expression, so an owner -- when
        # it carries one -- is quoted by its own protocol, the same rules as
        # every other Oracle schema object.
        parts.append(expr.view.to_sql()[0])
        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")
        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")
        if expr.options and expr.options.check_option:
            if not self.supports_view_check_option():
                from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
                raise UnsupportedFeatureError(
                    self.name, "WITH CHECK OPTION",
                    f"{self.name} does not support WITH CHECK OPTION.",
                )
            check_option = expr.options.check_option.value
            parts.append(f"WITH {check_option} CHECK OPTION")
        return " ".join(parts), query_params

    def format_drop_view_statement(
        self, expr: "DropViewExpression"
    ) -> Tuple[str, tuple]:
        """Format ``DROP VIEW`` for Oracle.

        Raises:
            TypeError: ``expr.view`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.View`.
                Another catalogue object would render its own name, so the
                statement would drop something else and name a view.
        """
        from rhosocial.activerecord.backend.expression.objects import View

        if not isinstance(expr.view, View):
            raise TypeError(
                f"DropViewExpression.view must be a View, "
                f"got {type(expr.view).__name__}"
            )

        parts = ["DROP VIEW"]
        parts.append(expr.view.to_sql()[0])
        return " ".join(parts), ()