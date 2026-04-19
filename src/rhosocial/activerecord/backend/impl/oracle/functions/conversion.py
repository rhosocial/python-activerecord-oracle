# functions/conversion.py
"""
Oracle type conversion function factories.
"""

from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core, operators
    from ..dialect import OracleDialect


def to_number(dialect: "OracleDialect",
              expr: Union[str, "bases.BaseExpression"],
              format: Optional[str] = None) -> "core.FunctionCall":
    """Creates a TO_NUMBER function call.
    
    Converts a value to a number.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Number format string
    
    Returns:
        A FunctionCall instance representing TO_NUMBER
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_NUMBER", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_NUMBER", expr_val)


def cast_expr(dialect: "OracleDialect",
              expr: Union[str, "bases.BaseExpression"],
              type_name: str) -> "operators.RawSQLExpression":
    """Creates a CAST expression.
    
    Casts an expression to a specified type.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to cast
        type_name: Target type name
    
    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    expr_sql, expr_params = _convert_to_expression(dialect, expr).to_sql()
    sql = f"CAST({expr_sql} AS {type_name})"
    return operators.RawSQLExpression(dialect, sql, expr_params)


def to_binary_double(dialect: "OracleDialect",
                     expr: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a TO_BINARY_DOUBLE function call.
    
    Converts a value to a binary double.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
    
    Returns:
        A FunctionCall instance representing TO_BINARY_DOUBLE
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    return core.FunctionCall(dialect, "TO_BINARY_DOUBLE", expr_val)


def to_binary_float(dialect: "OracleDialect",
                    expr: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a TO_BINARY_FLOAT function call.
    
    Converts a value to a binary float.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
    
    Returns:
        A FunctionCall instance representing TO_BINARY_FLOAT
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    return core.FunctionCall(dialect, "TO_BINARY_FLOAT", expr_val)
