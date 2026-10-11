# src/rhosocial/activerecord/backend/impl/oracle/functions/conversion.py
"""Oracle type conversion function factories."""

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def to_number(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_NUMBER: convert a value to a NUMBER.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression to convert
        fmt: Optional format model

    Returns:
        A FunctionCall instance representing TO_NUMBER
    """
    from rhosocial.activerecord.backend.expression import core
    if fmt:
        return core.FunctionCall(dialect, "TO_NUMBER", expr, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_NUMBER", expr)


def to_binary_double(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle TO_BINARY_DOUBLE: convert a value to BINARY_DOUBLE.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression to convert

    Returns:
        A FunctionCall instance representing TO_BINARY_DOUBLE
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "TO_BINARY_DOUBLE", expr)


def to_binary_float(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle TO_BINARY_FLOAT: convert a value to BINARY_FLOAT.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression to convert

    Returns:
        A FunctionCall instance representing TO_BINARY_FLOAT
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "TO_BINARY_FLOAT", expr)