# src/rhosocial/activerecord/backend/impl/oracle/functions/analytic.py
"""Oracle analytic function factories."""

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def listagg(
    dialect: "OracleDialect",
    expr: "bases.BaseExpression",
    delimiter: str = ",",
    within_group_order_by: Optional[str] = None,
    on_overflow: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle LISTAGG: aggregate values into a delimited list.

    Uses ``FunctionCall`` with Oracle-specific metadata so the dialect's
    ``format_function_call`` override can emit ``WITHIN GROUP`` and
    ``ON OVERFLOW`` clauses.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression whose values are aggregated
        delimiter: Separator placed between values
        within_group_order_by: Order applied inside ``WITHIN GROUP``
        on_overflow: Action taken when the result overflows

    Returns:
        A FunctionCall instance representing LISTAGG
    """
    from rhosocial.activerecord.backend.expression import core

    func = core.FunctionCall(
        dialect, "LISTAGG",
        expr,
        core.Literal(dialect, delimiter),
    )
    if within_group_order_by:
        func._oracle_within_group = within_group_order_by
    if on_overflow:
        func._oracle_on_overflow = on_overflow
    return func


def percentile_cont(
    dialect: "OracleDialect",
    fraction: float,
    within_group_order_by: str,
) -> "bases.BaseExpression":
    """Oracle PERCENTILE_CONT: continuous percentile (ordered-set aggregate).

    Uses ``FunctionCall`` with ``_oracle_within_group`` metadata so the
    ``format_function_call`` override emits the ``WITHIN GROUP (...)`` clause.

    Args:
        dialect: The Oracle dialect instance
        fraction: Percentile fraction in [0, 1]
        within_group_order_by: Order applied inside ``WITHIN GROUP``

    Returns:
        A FunctionCall instance representing PERCENTILE_CONT
    """
    from rhosocial.activerecord.backend.expression import core

    func = core.FunctionCall(dialect, "PERCENTILE_CONT", core.Literal(dialect, fraction))
    if within_group_order_by:
        func._oracle_within_group = within_group_order_by
    return func


def percentile_disc(
    dialect: "OracleDialect",
    fraction: float,
    within_group_order_by: str,
) -> "bases.BaseExpression":
    """Oracle PERCENTILE_DISC: discrete percentile (ordered-set aggregate).

    Uses ``FunctionCall`` with ``_oracle_within_group`` metadata so the
    ``format_function_call`` override emits the ``WITHIN GROUP (...)`` clause.

    Args:
        dialect: The Oracle dialect instance
        fraction: Percentile fraction in [0, 1]
        within_group_order_by: Order applied inside ``WITHIN GROUP``

    Returns:
        A FunctionCall instance representing PERCENTILE_DISC
    """
    from rhosocial.activerecord.backend.expression import core

    func = core.FunctionCall(dialect, "PERCENTILE_DISC", core.Literal(dialect, fraction))
    if within_group_order_by:
        func._oracle_within_group = within_group_order_by
    return func