# src/rhosocial/activerecord/backend/impl/oracle/functions/conversion.py
"""Oracle type conversion function factories."""

from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def to_number(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_NUMBER: convert a value to a NUMBER."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        return core.FunctionCall(dialect, "TO_NUMBER", expr_val, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_NUMBER", expr_val)


def to_binary_double(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle TO_BINARY_DOUBLE: convert a value to BINARY_DOUBLE."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(dialect, "TO_BINARY_DOUBLE", _convert_to_expression(dialect, expr))


def to_binary_float(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle TO_BINARY_FLOAT: convert a value to BINARY_FLOAT."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(dialect, "TO_BINARY_FLOAT", _convert_to_expression(dialect, expr))