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
        parts = ["CREATE DATABASE"]
        parts.append(self.format_identifier(expr.database_name))
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
