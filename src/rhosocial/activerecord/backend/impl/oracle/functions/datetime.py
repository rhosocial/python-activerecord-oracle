# src/rhosocial/activerecord/backend/impl/oracle/functions/datetime.py
"""Oracle date/time function factories."""

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def to_date(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_DATE: convert a string to a DATE value.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression holding the text to parse
        fmt: Optional Oracle format model

    Returns:
        A FunctionCall instance representing TO_DATE
    """
    from rhosocial.activerecord.backend.expression import core
    if fmt:
        return core.FunctionCall(dialect, "TO_DATE", expr, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_DATE", expr)


def to_char(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_CHAR: convert a value to a string.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression to format
        fmt: Optional Oracle format model

    Returns:
        A FunctionCall instance representing TO_CHAR
    """
    from rhosocial.activerecord.backend.expression import core
    if fmt:
        return core.FunctionCall(dialect, "TO_CHAR", expr, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_CHAR", expr)


def to_timestamp(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_TIMESTAMP: convert a string to a TIMESTAMP value.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression holding the text to parse
        fmt: Optional Oracle format model

    Returns:
        A FunctionCall instance representing TO_TIMESTAMP
    """
    from rhosocial.activerecord.backend.expression import core
    if fmt:
        return core.FunctionCall(dialect, "TO_TIMESTAMP", expr, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TO_TIMESTAMP", expr)


def to_timestamp_tz(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TO_TIMESTAMP_TZ: convert to TIMESTAMP WITH TIME ZONE.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression holding the text to parse
        fmt: Optional Oracle format model

    Returns:
        A FunctionCall instance representing TO_TIMESTAMP_TZ
    """
    from rhosocial.activerecord.backend.expression import core
    if fmt:
        return core.FunctionCall(
            dialect, "TO_TIMESTAMP_TZ", expr, core.Literal(dialect, fmt)
        )
    return core.FunctionCall(dialect, "TO_TIMESTAMP_TZ", expr)


def trunc_date(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    fmt: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle TRUNC(date): truncate a date to a specified precision.

    Args:
        dialect: The Oracle dialect instance
        expr: The date expression to truncate
        fmt: Optional Oracle format model naming the precision

    Returns:
        A FunctionCall instance representing TRUNC
    """
    from rhosocial.activerecord.backend.expression import core
    if fmt:
        return core.FunctionCall(dialect, "TRUNC", expr, core.Literal(dialect, fmt))
    return core.FunctionCall(dialect, "TRUNC", expr)


def add_months(
    dialect: "OracleDialect",
    date_expr: "bases.BaseExpression",
    months: int,
) -> "bases.BaseExpression":
    """Oracle ADD_MONTHS: add months to a date.

    Args:
        dialect: The Oracle dialect instance
        date_expr: The date expression
        months: Number of months to add

    Returns:
        A FunctionCall instance representing ADD_MONTHS
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(
        dialect, "ADD_MONTHS",
        date_expr,
        core.Literal(dialect, months),
    )


def months_between(
    dialect: "OracleDialect",
    date1: "bases.BaseExpression",
    date2: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle MONTHS_BETWEEN: number of months between two dates.

    Args:
        dialect: The Oracle dialect instance
        date1: The first date expression
        date2: The second date expression

    Returns:
        A FunctionCall instance representing MONTHS_BETWEEN
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "MONTHS_BETWEEN", date1, date2)


def last_day(
    dialect: "OracleDialect",
    date_expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle LAST_DAY: last day of the month.

    Args:
        dialect: The Oracle dialect instance
        date_expr: The date expression

    Returns:
        A FunctionCall instance representing LAST_DAY
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "LAST_DAY", date_expr)


def next_day(
    dialect: "OracleDialect",
    date_expr: "bases.BaseExpression",
    day: str,
) -> "bases.BaseExpression":
    """Oracle NEXT_DAY: next occurrence of a specified weekday.

    Args:
        dialect: The Oracle dialect instance
        date_expr: The date expression
        day: Weekday name Oracle recognises, e.g. ``MONDAY``

    Returns:
        A FunctionCall instance representing NEXT_DAY
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(
        dialect, "NEXT_DAY",
        date_expr,
        core.Literal(dialect, day),
    )


def extract_date(
    dialect: "OracleDialect",
    component: str,
    expr: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle EXTRACT: extract a date-time component (YEAR, MONTH, etc.).

    Delegates to the framework ``ExtractExpression``, which dispatches
    through ``dialect.format_extract_expression(...)``.

    Args:
        dialect: The Oracle dialect instance
        component: The component to extract, e.g. ``YEAR``
        expr: The date expression to extract from

    Returns:
        An ExtractExpression instance
    """
    from rhosocial.activerecord.backend.expression.datetime import ExtractExpression
    return ExtractExpression(dialect, component, expr)