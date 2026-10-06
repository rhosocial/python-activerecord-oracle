# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl_database.py
"""Oracle database DDL mixin."""
from __future__ import annotations

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_database import (
        AlterDatabaseExpression,
        CreateDatabaseExpression,
        DropDatabaseExpression,
    )


class OracleDatabaseMixin:
    """Oracle database DDL support.

    Oracle supports CREATE DATABASE (requires SYSDBA) and DROP DATABASE.
    ALTER DATABASE is primarily for instance-level operations.

    Not composed into ``OracleDialect`` today: the backend puts
    ``CreateDatabaseSupport`` / ``DropDatabaseSupport`` / ``AlterDatabaseSupport``
    in its declared-unsupported list, because Oracle databases are instances
    rather than objects an application creates. The mixin is kept -- and kept
    correct -- because a backend that later enables the feature should not
    discover that the formatters here were never run. Nothing here executes on
    any path the dialect currently reaches.
    """

    def supports_database(self) -> bool:
        return True

    def supports_create_database(self) -> bool:
        """Oracle supports CREATE DATABASE (requires SYSDBA)."""
        return True

    def supports_drop_database(self) -> bool:
        """Oracle supports DROP DATABASE."""
        return True

    def supports_database_encoding(self) -> bool:
        """Oracle supports CHARACTER SET."""
        return True

    def format_create_database_statement(
        self, expr: CreateDatabaseExpression
    ) -> Tuple[str, tuple]:
        """Render ``CREATE DATABASE`` for Oracle.

        Reads the name off ``expr.database``, the
        :class:`~rhosocial.activerecord.backend.expression.objects.Database`
        object the expression carries, and renders it through that object's own
        protocol rather than re-quoting a string here. The attribute used to be
        ``database_name`` and was renamed when statements began carrying objects
        instead of names; because this mixin sits behind
        :class:`CreateDatabaseSupport`, which Oracle declares unsupported, the
        stale read could not fail anything -- which is exactly why a latent
        ``AttributeError`` is worth fixing rather than deleting: the day CREATE
        DATABASE is enabled, this line is the first thing that runs.

        Raises:
            TypeError: ``expr.database`` is not a ``Database``. Another object
                carries its own ``format_method`` and would render its own name.
        """
        from rhosocial.activerecord.backend.expression.objects import Database

        if not isinstance(expr.database, Database):
            raise TypeError(
                f"CreateDatabaseExpression.database must be a Database, "
                f"got {type(expr.database).__name__}"
            )
        parts = ["CREATE DATABASE"]
        parts.append(expr.database.to_sql()[0])
        if expr.encoding:
            parts.append(f"CHARACTER SET {expr.encoding}")
        return " ".join(parts), ()

    def format_drop_database_statement(
        self, expr: DropDatabaseExpression
    ) -> Tuple[str, tuple]:
        parts = ["DROP DATABASE"]
        return " ".join(parts), ()

    def format_alter_database_statement(
        self, expr: AlterDatabaseExpression
    ) -> Tuple[str, tuple]:
        raise UnsupportedFeatureError(
            self.name, "ALTER DATABASE",
            f"{self.name} does not support ALTER DATABASE."
        )


__all__ = ['OracleDatabaseMixin']
