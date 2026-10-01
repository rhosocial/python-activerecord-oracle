# src/rhosocial/activerecord/backend/impl/oracle/functions/schema.py
"""
Oracle schema resolution functions.

Provides a SQL expression factory for asking the server which namespace an
unqualified reference resolves against.

Follows the expression-dialect separation architecture:
- First parameter is always the dialect instance
- Returns an Expression object (FunctionCall)
- Does not concatenate SQL strings directly
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import core

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def current_schema(dialect: "SQLDialectBase") -> "core.FunctionCall":
    """Create a function call for the current schema.

    Returns the schema currently in use for name resolution. Retrieved via
    SYS_CONTEXT because Oracle has no bare CURRENT_SCHEMA function.

    The returned value function cannot stand alone in a SELECT list on Oracle;
    the caller must wrap it in a QueryExpression with FROM DUAL. See
    OracleBackend.get_current_schema.

    Usage:
        - current_schema(dialect)

    Args:
        dialect: The SQL dialect instance

    Returns:
        A FunctionCall instance that evaluates to the current schema name
    """
    return core.FunctionCall(
        dialect,
        "SYS_CONTEXT",
        core.Literal(dialect, "USERENV"),
        core.Literal(dialect, "CURRENT_SCHEMA"),
    )
