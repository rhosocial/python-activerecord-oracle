# src/rhosocial/activerecord/backend/impl/oracle/functions/_convert.py
"""Internal helper: convert raw values to BaseExpression instances."""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
    handle_numeric_literals: bool = True,
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression."""
    from rhosocial.activerecord.backend.expression import bases, core

    if isinstance(expr, bases.BaseExpression):
        return expr
    elif handle_numeric_literals and isinstance(expr, (int, float)):
        return core.Literal(dialect, expr)
    else:
        return core.Column(dialect, expr)
