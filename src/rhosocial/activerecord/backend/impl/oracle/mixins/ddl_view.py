# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl_view.py
"""Oracle view DDL formatting mixin."""

from typing import Tuple, TYPE_CHECKING

from .if_exists import supports_statement_if_exists

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateViewStatement,
        DropViewStatement,
    )


class OracleViewMixin:
    """CREATE VIEW / DROP VIEW formatters for Oracle."""

    def supports_create_or_replace_view(self) -> bool:
        """Oracle supports CREATE OR REPLACE VIEW."""
        return True

    def supports_if_not_exists_view(self) -> bool:
        """Whether CREATE VIEW accepts the native ``IF NOT EXISTS`` clause.

        Oracle added it with 19c Release Update 19.28 and ships it in the 23ai
        base release; 21c has no such spelling. See
        :func:`~.if_exists.supports_statement_if_exists`.
        """
        return supports_statement_if_exists(self)

    def supports_if_exists_view(self) -> bool:
        """Whether DROP VIEW accepts the native ``IF EXISTS`` clause.

        Same two anchors as :meth:`supports_if_not_exists_view`.
        """
        return supports_statement_if_exists(self)

    def supports_view_check_option(self) -> bool:
        """Oracle supports ``WITH [LOCAL|CASCADED] CHECK OPTION`` on views."""
        return True


    def supports_create_view_bind_parameters(self) -> bool:
        """Oracle's DDL statements never carry bind variables.

        Measured on 18c / 21c / 26ai: the driver rejects the statement
        before the server sees it -- "DPY-4009: 0 positional bind values are
        required but 1 were provided".
        """
        return False
    def format_create_view_statement(
        self, expr: "CreateViewStatement"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [OR REPLACE] VIEW [IF NOT EXISTS]`` for Oracle.

        ``IF NOT EXISTS`` is rendered from 19c Release Update 19.28 and from
        23ai; below that capability, and when combined with ``OR REPLACE``
        (ORA-11541 forbids the pair on every release), the request is refused
        by name rather than dropped.

        Raises:
            TypeError: ``expr.view`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.View`.
                A materialized view or a table would each render its own name,
                producing a well-formed CREATE VIEW over that object's name.
            UnsupportedFeatureError: ``IF NOT EXISTS`` was requested below the
                documented capability, or together with ``OR REPLACE``.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.objects import View

        if not isinstance(expr.view, View):
            raise TypeError(
                f"CreateViewStatement.view must be a View, "
                f"got {type(expr.view).__name__}"
            )

        parts = ["CREATE"]
        if expr.replace and self.supports_create_or_replace_view():
            parts.append("OR REPLACE")
        parts.append("VIEW")
        if expr.if_not_exists:
            if expr.replace:
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE VIEW OR REPLACE IF NOT EXISTS",
                    "ORA-11541: REPLACE and IF NOT EXISTS cannot coexist in the "
                    "same DDL statement.",
                )
            if not self.supports_if_not_exists_view():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE VIEW IF NOT EXISTS",
                    "Oracle has no CREATE VIEW IF NOT EXISTS before 19c Release "
                    "Update 19.28 / 23ai (21c has no such spelling).",
                )
            parts.append("IF NOT EXISTS")
        # The view is a catalogue object on the expression, so an owner -- when
        # it carries one -- is quoted by its own protocol, the same rules as
        # every other Oracle schema object.
        parts.append(expr.view.to_sql()[0])
        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")
        query_sql, query_params = expr.query.to_sql()
        if query_params and not self.supports_create_view_bind_parameters():
            self.refuse_ddl_bind_parameters("CREATE VIEW with bound parameters")
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
        self, expr: "DropViewStatement"
    ) -> Tuple[str, tuple]:
        """Format ``DROP VIEW [IF EXISTS]`` for Oracle.

        Oracle accepts ``IF EXISTS`` from 19c Release Update 19.28 and from
        23ai; below that capability it is refused by name rather than dropped.

        Raises:
            TypeError: ``expr.view`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.View`.
                Another catalogue object would render its own name, so the
                statement would drop something else and name a view.
            UnsupportedFeatureError: ``IF EXISTS`` was requested below the
                documented capability, or CASCADE / RESTRICT was requested;
                Oracle declares neither modifier, so it is refused by name
                rather than dropped.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.objects import View

        if not isinstance(expr.view, View):
            raise TypeError(
                f"DropViewStatement.view must be a View, "
                f"got {type(expr.view).__name__}"
            )

        if expr.if_exists and not self.supports_if_exists_view():
            raise UnsupportedFeatureError(
                self.name,
                "DROP VIEW IF EXISTS",
                "Oracle has no DROP VIEW IF EXISTS before 19c Release Update "
                "19.28 / 23ai (21c has no such spelling).",
            )
        if expr.cascade and not self.supports_cascade_view():
            raise UnsupportedFeatureError(
                self.name,
                "DROP VIEW CASCADE",
                f"{self.name} does not support DROP VIEW CASCADE.",
            )
        if expr.restrict and not self.supports_restrict_view():
            raise UnsupportedFeatureError(
                self.name,
                "DROP VIEW RESTRICT",
                f"{self.name} does not support DROP VIEW RESTRICT.",
            )

        parts = ["DROP VIEW"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(expr.view.to_sql()[0])
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()