# src/rhosocial/activerecord/backend/impl/oracle/functions/datetime.py
"""Oracle date/time function factories."""

from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def to_date(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_DATE: convert a string to a DATE value."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        return core.FunctionCall(dialect, "TO_DATE", expr_val, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_DATE", expr_val)


def to_char(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_CHAR: convert a value to a string."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        return core.FunctionCall(dialect, "TO_CHAR", expr_val, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_CHAR", expr_val)


def to_timestamp(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_TIMESTAMP: convert a string to a TIMESTAMP value."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        return core.FunctionCall(dialect, "TO_TIMESTAMP", expr_val, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_TIMESTAMP", expr_val)


def to_timestamp_tz(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_TIMESTAMP_TZ: convert to TIMESTAMP WITH TIME ZONE."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        return core.FunctionCall(dialect, "TO_TIMESTAMP_TZ", expr_val, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_TIMESTAMP_TZ", expr_val)


def trunc_date(
    dialect: "OracleDialect",
    expr: Union[str, "bases.BaseExpression"],
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TRUNC(date): truncate a date to a specified precision."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        return core.FunctionCall(dialect, "TRUNC", expr_val, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TRUNC", expr_val)


def add_months(
    dialect: "OracleDialect",
    date_expr: Union[str, "bases.BaseExpression"],
    months: int,
) -> "bases.BaseExpression":
    """Oracle ADD_MONTHS: add months to a date."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "ADD_MONTHS",
        _convert_to_expression(dialect, date_expr),
        core.Literal(dialect, months),
    )


def months_between(
    dialect: "OracleDialect",
    date1: Union[str, "bases.BaseExpression"],
    date2: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle MONTHS_BETWEEN: number of months between two dates."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "MONTHS_BETWEEN",
        _convert_to_expression(dialect, date1),
        _convert_to_expression(dialect, date2),
    )


def last_day(
    dialect: "OracleDialect",
    date_expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle LAST_DAY: last day of the month."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(dialect, "LAST_DAY", _convert_to_expression(dialect, date_expr))


def next_day(
    dialect: "OracleDialect",
    date_expr: Union[str, "bases.BaseExpression"],
    day: str,
) -> "bases.BaseExpression":
    """Oracle NEXT_DAY: next occurrence of a specified weekday."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "NEXT_DAY",
        _convert_to_expression(dialect, date_expr),
        core.Literal(dialect, day),
    )


def extract_date(
    dialect: "OracleDialect",
    component: str,
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle EXTRACT: extract a date-time component (YEAR, MONTH, etc.).

    Delegates to the framework ``ExtractExpression``, which dispatches
    through ``dialect.format_extract_expression(...)``.
    """
    from rhosocial.activerecord.backend.expression.datetime import ExtractExpression
    from ._convert import _convert_to_expression
    target = _convert_to_expression(dialect, expr, handle_numeric_literals=False)
    return ExtractExpression(dialect, component, target)