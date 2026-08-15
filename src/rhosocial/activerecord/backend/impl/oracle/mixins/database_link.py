# src/rhosocial/activerecord/backend/impl/oracle/mixins/database_link.py
"""Oracle DATABASE LINK DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.ddl.database_link import (
        OracleCreateDatabaseLinkExpression,
        OracleDropDatabaseLinkExpression,
    )


class OracleDatabaseLinkMixin:
    """Oracle database link capability checks and formatters.

    Database links have existed since early Oracle releases; the formatters
    gate on ``(9, 0, 0)`` per the backend implementation contract. Remote
    table references use the ``@dblink`` suffix (see
    :meth:`OracleIdentifierMixin.format_table`).
    """

    def supports_create_database_link(self) -> bool:
        return True

    def supports_drop_database_link(self) -> bool:
        return True

    def format_create_database_link_statement(
        self, expr: "OracleCreateDatabaseLinkExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE DATABASE LINK",
                suggestion=(
                    f"Oracle {self.version} does not support database "
                    "links; they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE"]
        if expr.shared:
            parts.append("SHARED")
        if expr.public:
            parts.append("PUBLIC")
        parts.append("DATABASE LINK")
        parts.append(self.format_identifier(expr.link_name))
        if expr.user is not None:
            parts.append(
                f"CONNECT TO {self.format_identifier(expr.user)} "
                f"IDENTIFIED BY {self.format_identifier(expr.identified_by)}"
            )
        if expr.using:
            escaped = self._escape_sql_string(expr.using)
            parts.append(f"USING '{escaped}'")
        return " ".join(parts), ()

    def format_drop_database_link_statement(
        self, expr: "OracleDropDatabaseLinkExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "DROP DATABASE LINK",
                suggestion=(
                    f"Oracle {self.version} does not support database "
                    "links; they require Oracle 9i or later."
                ),
            )
        parts = ["DROP"]
        if expr.public:
            parts.append("PUBLIC")
        parts.append("DATABASE LINK")
        parts.append(self.format_identifier(expr.link_name))
        return " ".join(parts), ()
