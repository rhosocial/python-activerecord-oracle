# src/rhosocial/activerecord/backend/impl/oracle/functions/null.py
"""Oracle NULL handling function factories."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def nvl(
    dialect: "OracleDialect",
    expr1: "bases.BaseExpression",
    expr2: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle NVL: return expr2 if expr1 is NULL.

    Args:
        dialect: The Oracle dialect instance
        expr1: The expression to test for NULL
        expr2: The expression to substitute when expr1 is NULL

    Returns:
        A FunctionCall instance representing NVL
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "NVL", expr1, expr2)


def nvl2(
    dialect: "OracleDialect",
    expr1: "bases.BaseExpression",
    expr2: "bases.BaseExpression",
    expr3: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle NVL2: return expr2 if expr1 is NOT NULL, otherwise expr3.

    Args:
        dialect: The Oracle dialect instance
        expr1: The expression to test for NULL
        expr2: The expression to use when expr1 is not NULL
        expr3: The expression to use when expr1 is NULL

    Returns:
        A FunctionCall instance representing NVL2
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "NVL2", expr1, expr2, expr3)


def coalesce_oracle(
    dialect: "OracleDialect",
    *expressions: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle COALESCE: return the first non-NULL expression.

    Args:
        dialect: The Oracle dialect instance
        *expressions: The candidate expressions, most preferred first

    Returns:
        A FunctionCall instance representing COALESCE
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "COALESCE", *expressions)


def nullif(
    dialect: "OracleDialect",
    expr1: "bases.BaseExpression",
    expr2: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle NULLIF: return NULL if expr1 equals expr2.

    Args:
        dialect: The Oracle dialect instance
        expr1: The expression to return when it differs from expr2
        expr2: The expression to compare against

    Returns:
        A FunctionCall instance representing NULLIF
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "NULLIF", expr1, expr2)


def lnnvl(
    dialect: "OracleDialect",
    condition: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle LNNVL: TRUE if condition is FALSE or NULL.

    Args:
        dialect: The Oracle dialect instance
        condition: The predicate to invert

    Returns:
        A FunctionCall instance representing LNNVL
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "LNNVL", condition)