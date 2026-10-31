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

    Builds :class:`OracleListaggFunction`, the backend's derived function
    node, so ``WITHIN GROUP`` and ``ON OVERFLOW`` travel on the node's own
    contract fields rather than as metadata the formatter reflects back.

    Args:
        dialect: The Oracle dialect instance
        expr: The expression whose values are aggregated
        delimiter: Separator placed between values
        within_group_order_by: Order applied inside ``WITHIN GROUP``
        on_overflow: Action taken when the result overflows

    Returns:
        An OracleListaggFunction node representing LISTAGG
    """
    from rhosocial.activerecord.backend.expression import core
    from ..expression.functions import OracleListaggFunction

    return OracleListaggFunction(
        dialect,
        expr,
        core.Literal(dialect, delimiter),
        within_group_order_by=within_group_order_by or None,
        on_overflow=on_overflow or None,
    )


def percentile_cont(
    dialect: "OracleDialect",
    fraction: float,
    within_group_order_by: str,
) -> "bases.BaseExpression":
    """Oracle PERCENTILE_CONT: continuous percentile (ordered-set aggregate).

    Builds :class:`OraclePercentileContFunction`, whose
    ``within_group_order_by`` contract field carries the clause to the
    formatter.

    Args:
        dialect: The Oracle dialect instance
        fraction: Percentile fraction in [0, 1]
        within_group_order_by: Order applied inside ``WITHIN GROUP``

    Returns:
        An OraclePercentileContFunction node representing PERCENTILE_CONT
    """
    from rhosocial.activerecord.backend.expression import core
    from ..expression.functions import OraclePercentileContFunction

    return OraclePercentileContFunction(
        dialect,
        core.Literal(dialect, fraction),
        within_group_order_by=within_group_order_by or None,
    )


def percentile_disc(
    dialect: "OracleDialect",
    fraction: float,
    within_group_order_by: str,
) -> "bases.BaseExpression":
    """Oracle PERCENTILE_DISC: discrete percentile (ordered-set aggregate).

    Builds :class:`OraclePercentileDiscFunction`, whose
    ``within_group_order_by`` contract field carries the clause to the
    formatter.

    Args:
        dialect: The Oracle dialect instance
        fraction: Percentile fraction in [0, 1]
        within_group_order_by: Order applied inside ``WITHIN GROUP``

    Returns:
        An OraclePercentileDiscFunction node representing PERCENTILE_DISC
    """
    from rhosocial.activerecord.backend.expression import core
    from ..expression.functions import OraclePercentileDiscFunction

    return OraclePercentileDiscFunction(
        dialect,
        core.Literal(dialect, fraction),
        within_group_order_by=within_group_order_by or None,
    )
