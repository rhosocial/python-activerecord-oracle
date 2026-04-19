# functions/null.py
"""
Oracle NULL handling function factories.
"""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core
    from ..dialect import OracleDialect


def nvl(dialect: "OracleDialect",
        expr1: Union[str, "bases.BaseExpression"],
        expr2: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates an NVL function call.
    
    Returns expr2 if expr1 is null, otherwise returns expr1.
    
    Args:
        dialect: The Oracle dialect instance
        expr1: First expression
        expr2: Replacement value if expr1 is null
    
    Returns:
        A FunctionCall instance representing NVL
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr1_val = _convert_to_expression(dialect, expr1)
    expr2_val = _convert_to_expression(dialect, expr2)
    return core.FunctionCall(dialect, "NVL", expr1_val, expr2_val)


def nvl2(dialect: "OracleDialect",
         expr1: Union[str, "bases.BaseExpression"],
         expr2: Union[str, "bases.BaseExpression"],
         expr3: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates an NVL2 function call.
    
    Returns expr2 if expr1 is not null, otherwise returns expr3.
    
    Args:
        dialect: The Oracle dialect instance
        expr1: Expression to check
        expr2: Value if expr1 is not null
        expr3: Value if expr1 is null
    
    Returns:
        A FunctionCall instance representing NVL2
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr1_val = _convert_to_expression(dialect, expr1)
    expr2_val = _convert_to_expression(dialect, expr2)
    expr3_val = _convert_to_expression(dialect, expr3)
    return core.FunctionCall(dialect, "NVL2", expr1_val, expr2_val, expr3_val)


def coalesce_oracle(dialect: "OracleDialect",
                    *expressions: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a COALESCE function call (Oracle version).
    
    Returns the first non-null expression.
    
    Args:
        dialect: The Oracle dialect instance
        *expressions: Expressions to evaluate
    
    Returns:
        A FunctionCall instance representing COALESCE
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    args = [_convert_to_expression(dialect, expr) for expr in expressions]
    return core.FunctionCall(dialect, "COALESCE", *args)


def nullif(dialect: "OracleDialect",
           expr1: Union[str, "bases.BaseExpression"],
           expr2: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a NULLIF function call.
    
    Returns null if expr1 equals expr2, otherwise returns expr1.
    
    Args:
        dialect: The Oracle dialect instance
        expr1: First expression
        expr2: Second expression
    
    Returns:
        A FunctionCall instance representing NULLIF
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr1_val = _convert_to_expression(dialect, expr1)
    expr2_val = _convert_to_expression(dialect, expr2)
    return core.FunctionCall(dialect, "NULLIF", expr1_val, expr2_val)


def lnnvl(dialect: "OracleDialect",
          condition: "bases.BaseExpression") -> "operators.RawSQLExpression":
    """Creates an LNNVL function call.
    
    LNNVL returns TRUE if the condition is FALSE or NULL.
    Useful for handling NULL in conditions.
    
    Args:
        dialect: The Oracle dialect instance
        condition: Condition to evaluate
    
    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    cond_expr = _convert_to_expression(dialect, condition)
    cond_sql, cond_params = cond_expr.to_sql()
    sql = f"LNNVL({cond_sql})"
    return operators.RawSQLExpression(dialect, sql, tuple(cond_params))
