# functions/analytic.py
"""
Oracle analytic function factories.
"""

from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, operators
    from ..dialect import OracleDialect


def listagg(dialect: "OracleDialect",
            expr: Union[str, "bases.BaseExpression"],
            delimiter: str = ",",
            order_by: Optional[str] = None,
            on_overflow: Optional[str] = None) -> "operators.RawSQLExpression":
    """Creates a LISTAGG expression.
    
    Aggregates values into a delimited list.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to aggregate
        delimiter: Delimiter string
        order_by: ORDER BY clause for list
        on_overflow: Overflow behavior (TRUNCATE or ERROR)
    
    Returns:
        A RawSQLExpression instance
    
    Version: Oracle 11g R2+
    """
    from rhosocial.activerecord.backend.expression import core, operators
    from . import _convert_to_expression
    expr_sql, expr_params = _convert_to_expression(dialect, expr).to_sql()
    delim_expr = core.Literal(dialect, delimiter)
    delim_sql, delim_params = delim_expr.to_sql()
    
    parts = [f"LISTAGG({expr_sql}, {delim_sql})"]
    params = expr_params + delim_params
    
    if order_by:
        parts.append(f"WITHIN GROUP (ORDER BY {order_by})")
    
    if on_overflow:
        if on_overflow.upper() == "TRUNCATE":
            parts.append("ON OVERFLOW TRUNCATE")
        elif on_overflow.upper() == "ERROR":
            parts.append("ON OVERFLOW ERROR")
    
    sql = " ".join(parts)
    return operators.RawSQLExpression(dialect, sql, tuple(params))


def percentile_cont(dialect: "OracleDialect",
                    fraction: float,
                    expr: Union[str, "bases.BaseExpression"],
                    order_by: str) -> "operators.RawSQLExpression":
    """Creates a PERCENTILE_CONT analytic function.
    
    Computes a continuous percentile.
    
    Args:
        dialect: The Oracle dialect instance
        fraction: Percentile fraction (0 to 1)
        expr: Expression to compute percentile for
        order_by: ORDER BY clause
    
    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    expr_sql, expr_params = _convert_to_expression(dialect, expr).to_sql()
    sql = f"PERCENTILE_CONT({fraction}) WITHIN GROUP (ORDER BY {order_by})"
    return operators.RawSQLExpression(dialect, sql, tuple(expr_params))


def percentile_disc(dialect: "OracleDialect",
                    fraction: float,
                    expr: Union[str, "bases.BaseExpression"],
                    order_by: str) -> "operators.RawSQLExpression":
    """Creates a PERCENTILE_DISC analytic function.
    
    Computes a discrete percentile.
    
    Args:
        dialect: The Oracle dialect instance
        fraction: Percentile fraction (0 to 1)
        expr: Expression to compute percentile for
        order_by: ORDER BY clause
    
    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    expr_sql, expr_params = _convert_to_expression(dialect, expr).to_sql()
    sql = f"PERCENTILE_DISC({fraction}) WITHIN GROUP (ORDER BY {order_by})"
    return operators.RawSQLExpression(dialect, sql, tuple(expr_params))
