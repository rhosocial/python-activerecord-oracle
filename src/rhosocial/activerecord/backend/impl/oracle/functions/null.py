# src/rhosocial/activerecord/backend/impl/oracle/functions/null.py
"""Oracle NULL handling function factories."""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def nvl(
    dialect: "OracleDialect",
    expr1: Union[str, "bases.BaseExpression"],
    expr2: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle NVL: return expr2 if expr1 is NULL."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "NVL",
        _convert_to_expression(dialect, expr1),
        _convert_to_expression(dialect, expr2),
    )


def nvl2(
    dialect: "OracleDialect",
    expr1: Union[str, "bases.BaseExpression"],
    expr2: Union[str, "bases.BaseExpression"],
    expr3: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle NVL2: return expr2 if expr1 is NOT NULL, otherwise expr3."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "NVL2",
        _convert_to_expression(dialect, expr1),
        _convert_to_expression(dialect, expr2),
        _convert_to_expression(dialect, expr3),
    )


def coalesce_oracle(
    dialect: "OracleDialect",
    *expressions: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle COALESCE: return the first non-NULL expression."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    args = [_convert_to_expression(dialect, expr) for expr in expressions]
    return core.FunctionCall(dialect, "COALESCE", *args)


def nullif(
    dialect: "OracleDialect",
    expr1: Union[str, "bases.BaseExpression"],
    expr2: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle NULLIF: return NULL if expr1 equals expr2."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "NULLIF",
        _convert_to_expression(dialect, expr1),
        _convert_to_expression(dialect, expr2),
    )


def lnnvl(
    dialect: "OracleDialect",
    condition: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle LNNVL: TRUE if condition is FALSE or NULL."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    cond_expr = _convert_to_expression(dialect, condition)
    return core.FunctionCall(dialect, "LNNVL", cond_expr)