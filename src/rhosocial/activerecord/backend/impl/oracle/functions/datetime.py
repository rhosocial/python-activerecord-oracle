# functions/datetime.py
"""
Oracle date/time function factories.
"""

from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core
    from ..dialect import OracleDialect


def to_date(dialect: "OracleDialect",
            expr: Union[str, "bases.BaseExpression"],
            format: Optional[str] = None) -> "core.FunctionCall":
    """Creates a TO_DATE function call.
    
    Converts a string to a DATE value.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Date format string
    
    Returns:
        A FunctionCall instance representing TO_DATE
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_DATE", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_DATE", expr_val)


def to_char(dialect: "OracleDialect",
            expr: Union[str, "bases.BaseExpression"],
            format: Optional[str] = None) -> "core.FunctionCall":
    """Creates a TO_CHAR function call.
    
    Converts a value to a string.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Format string
    
    Returns:
        A FunctionCall instance representing TO_CHAR
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_CHAR", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_CHAR", expr_val)


def to_timestamp(dialect: "OracleDialect",
                 expr: Union[str, "bases.BaseExpression"],
                 format: Optional[str] = None) -> "core.FunctionCall":
    """Creates a TO_TIMESTAMP function call.
    
    Converts a string to a TIMESTAMP value.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Timestamp format string
    
    Returns:
        A FunctionCall instance representing TO_TIMESTAMP
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_TIMESTAMP", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_TIMESTAMP", expr_val)


def to_timestamp_tz(dialect: "OracleDialect",
                    expr: Union[str, "bases.BaseExpression"],
                    format: Optional[str] = None) -> "core.FunctionCall":
    """Creates a TO_TIMESTAMP_TZ function call.
    
    Converts a string to a TIMESTAMP WITH TIME ZONE value.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Timestamp format string
    
    Returns:
        A FunctionCall instance representing TO_TIMESTAMP_TZ
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_TIMESTAMP_TZ", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_TIMESTAMP_TZ", expr_val)


def trunc_date(dialect: "OracleDialect",
               expr: Union[str, "bases.BaseExpression"],
               fmt: Optional[str] = None) -> "core.FunctionCall":
    """Creates a TRUNC function call for dates.
    
    Truncates a date to a specified precision.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Date expression
        fmt: Truncation format (YYYY, MM, DD, etc.)
    
    Returns:
        A FunctionCall instance representing TRUNC
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        fmt_expr = core.Literal(dialect, fmt)
        return core.FunctionCall(dialect, "TRUNC", expr_val, fmt_expr)
    return core.FunctionCall(dialect, "TRUNC", expr_val)


def add_months(dialect: "OracleDialect",
               date_expr: Union[str, "bases.BaseExpression"],
               months: int) -> "core.FunctionCall":
    """Creates an ADD_MONTHS function call.
    
    Adds months to a date.
    
    Args:
        dialect: The Oracle dialect instance
        date_expr: Date expression
        months: Number of months to add (can be negative)
    
    Returns:
        A FunctionCall instance representing ADD_MONTHS
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    date_val = _convert_to_expression(dialect, date_expr)
    months_expr = core.Literal(dialect, months)
    return core.FunctionCall(dialect, "ADD_MONTHS", date_val, months_expr)


def months_between(dialect: "OracleDialect",
                   date1: Union[str, "bases.BaseExpression"],
                   date2: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a MONTHS_BETWEEN function call.
    
    Returns the number of months between two dates.
    
    Args:
        dialect: The Oracle dialect instance
        date1: First date
        date2: Second date
    
    Returns:
        A FunctionCall instance representing MONTHS_BETWEEN
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    date1_val = _convert_to_expression(dialect, date1)
    date2_val = _convert_to_expression(dialect, date2)
    return core.FunctionCall(dialect, "MONTHS_BETWEEN", date1_val, date2_val)


def last_day(dialect: "OracleDialect",
             date_expr: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a LAST_DAY function call.
    
    Returns the last day of the month for the given date.
    
    Args:
        dialect: The Oracle dialect instance
        date_expr: Date expression
    
    Returns:
        A FunctionCall instance representing LAST_DAY
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    date_val = _convert_to_expression(dialect, date_expr)
    return core.FunctionCall(dialect, "LAST_DAY", date_val)


def next_day(dialect: "OracleDialect",
             date_expr: Union[str, "bases.BaseExpression"],
             day: str) -> "core.FunctionCall":
    """Creates a NEXT_DAY function call.
    
    Returns the date of the next specified day of the week.
    
    Args:
        dialect: The Oracle dialect instance
        date_expr: Date expression
        day: Day of week (SUNDAY, MONDAY, etc.)
    
    Returns:
        A FunctionCall instance representing NEXT_DAY
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    date_val = _convert_to_expression(dialect, date_expr)
    day_expr = core.Literal(dialect, day)
    return core.FunctionCall(dialect, "NEXT_DAY", date_val, day_expr)


def extract_date(dialect: "OracleDialect",
                 component: str,
                 expr: Union[str, "bases.BaseExpression"]) -> "operators.RawSQLExpression":
    """Creates an EXTRACT expression for date components.
    
    Args:
        dialect: The Oracle dialect instance
        component: Date component (YEAR, MONTH, DAY, HOUR, etc.)
        expr: Date expression
    
    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    expr_sql, expr_params = expr_val.to_sql()
    sql = f"EXTRACT({component} FROM {expr_sql})"
    return operators.RawSQLExpression(dialect, sql, tuple(expr_params))
