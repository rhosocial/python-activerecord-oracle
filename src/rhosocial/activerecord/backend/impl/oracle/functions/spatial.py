# src/rhosocial/activerecord/backend/impl/oracle/functions/spatial.py
"""Oracle Spatial function factories."""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def sdo_geom_distance(
    dialect: "OracleDialect",
    geom1: Union[str, "bases.BaseExpression"],
    geom2: Union[str, "bases.BaseExpression"],
    tolerance: float = 0.005,
) -> "bases.BaseExpression":
    """Oracle SDO_GEOM.SDO_DISTANCE: distance between two geometries."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    return core.FunctionCall(
        dialect, "SDO_GEOM.SDO_DISTANCE",
        _convert_to_expression(dialect, geom1),
        _convert_to_expression(dialect, geom2),
        core.Literal(dialect, tolerance),
    )


def sdo_within_distance(
    dialect: "OracleDialect",
    geom1: Union[str, "bases.BaseExpression"],
    geom2: Union[str, "bases.BaseExpression"],
    distance: float,
    tolerance: float = 0.005,
) -> "bases.BaseExpression":
    """Oracle SDO_WITHIN_DISTANCE: True if two geometries are within distance.

    Returns a predicate (``FunctionCall == Literal('TRUE')``), consistent with
    other backends' use of ``ComparisonExpression`` instead of ``RawSQLExpression``.
    """
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    func = core.FunctionCall(
        dialect, "SDO_WITHIN_DISTANCE",
        _convert_to_expression(dialect, geom1),
        _convert_to_expression(dialect, geom2),
        core.Literal(dialect, f"distance={distance}"),
    )
    return func == core.Literal(dialect, "TRUE")


def sdo_contains(
    dialect: "OracleDialect",
    geom1: Union[str, "bases.BaseExpression"],
    geom2: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle SDO_CONTAINS: True if geom1 contains geom2."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    func = core.FunctionCall(
        dialect, "SDO_CONTAINS",
        _convert_to_expression(dialect, geom1),
        _convert_to_expression(dialect, geom2),
    )
    return func == core.Literal(dialect, "TRUE")


def sdo_inside(
    dialect: "OracleDialect",
    geom1: Union[str, "bases.BaseExpression"],
    geom2: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle SDO_INSIDE: True if geom1 is inside geom2."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    func = core.FunctionCall(
        dialect, "SDO_INSIDE",
        _convert_to_expression(dialect, geom1),
        _convert_to_expression(dialect, geom2),
    )
    return func == core.Literal(dialect, "TRUE")


def sdo_relate(
    dialect: "OracleDialect",
    geom1: Union[str, "bases.BaseExpression"],
    geom2: Union[str, "bases.BaseExpression"],
    mask: str = "ANYINTERACT",
) -> "bases.BaseExpression":
    """Oracle SDO_RELATE: True if spatial relationship mask matches."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    func = core.FunctionCall(
        dialect, "SDO_RELATE",
        _convert_to_expression(dialect, geom1),
        _convert_to_expression(dialect, geom2),
        core.Literal(dialect, f"mask={mask}"),
    )
    return func == core.Literal(dialect, "TRUE")


def sdo_geom_from_wkt(
    dialect: "OracleDialect",
    wkt: str,
    srid: int = 4326,
) -> "bases.BaseExpression":
    """Oracle SDO_GEOMETRY constructor from WKT string."""
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(
        dialect, "SDO_GEOMETRY",
        core.Literal(dialect, wkt),
        core.Literal(dialect, srid),
    )