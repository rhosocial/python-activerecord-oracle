# src/rhosocial/activerecord/backend/impl/oracle/functions/spatial.py
"""Oracle Spatial function factories.

Every argument is an expression.  A geometry held as data is built by
``sdo_geom_from_wkt`` below, which is the named constructor for this module:
a bare string used to be handed to the spatial functions, and whether that
string was the name of a column or the WKT of a value had to be guessed from
the type alone.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def sdo_geom_distance(
    dialect: "OracleDialect",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
    tolerance: float = 0.005,
) -> "bases.BaseExpression":
    """Oracle SDO_GEOM.SDO_DISTANCE: distance between two geometries.

    Args:
        dialect: The Oracle dialect instance
        geom1: The first geometry expression
        geom2: The second geometry expression
        tolerance: Tolerance used by the spatial index

    Returns:
        A FunctionCall instance representing SDO_GEOM.SDO_DISTANCE
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(
        dialect, "SDO_GEOM.SDO_DISTANCE",
        geom1,
        geom2,
        core.Literal(dialect, tolerance),
    )


def sdo_within_distance(
    dialect: "OracleDialect",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
    distance: float,
    tolerance: float = 0.005,
) -> "bases.BaseExpression":
    """Oracle SDO_WITHIN_DISTANCE: True if two geometries are within distance.

    Returns a predicate (``FunctionCall == Literal('TRUE')``), consistent with
    other backends' use of ``ComparisonExpression`` instead of ``RawSQLExpression``.

    Args:
        dialect: The Oracle dialect instance
        geom1: The first geometry expression
        geom2: The second geometry expression
        distance: Maximum separation, in the units of the geometries' SRID
        tolerance: Tolerance used by the spatial index

    Returns:
        A predicate that is TRUE when the geometries are within distance
    """
    from rhosocial.activerecord.backend.expression import core
    func = core.FunctionCall(
        dialect, "SDO_WITHIN_DISTANCE",
        geom1,
        geom2,
        core.Literal(dialect, f"distance={distance}"),
    )
    return func == core.Literal(dialect, "TRUE")


def sdo_contains(
    dialect: "OracleDialect",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle SDO_CONTAINS: True if geom1 contains geom2.

    Args:
        dialect: The Oracle dialect instance
        geom1: The containing geometry expression
        geom2: The contained geometry expression

    Returns:
        A predicate that is TRUE when geom1 contains geom2
    """
    from rhosocial.activerecord.backend.expression import core
    func = core.FunctionCall(dialect, "SDO_CONTAINS", geom1, geom2)
    return func == core.Literal(dialect, "TRUE")


def sdo_inside(
    dialect: "OracleDialect",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle SDO_INSIDE: True if geom1 is inside geom2.

    Args:
        dialect: The Oracle dialect instance
        geom1: The geometry expression to test
        geom2: The containing geometry expression

    Returns:
        A predicate that is TRUE when geom1 is inside geom2
    """
    from rhosocial.activerecord.backend.expression import core
    func = core.FunctionCall(dialect, "SDO_INSIDE", geom1, geom2)
    return func == core.Literal(dialect, "TRUE")


def sdo_relate(
    dialect: "OracleDialect",
    geom1: "bases.BaseExpression",
    geom2: "bases.BaseExpression",
    mask: str = "ANYINTERACT",
) -> "bases.BaseExpression":
    """Oracle SDO_RELATE: True if spatial relationship mask matches.

    Args:
        dialect: The Oracle dialect instance
        geom1: The first geometry expression
        geom2: The second geometry expression
        mask: The spatial relationship mask Oracle tests for

    Returns:
        A predicate that is TRUE when the mask matches
    """
    from rhosocial.activerecord.backend.expression import core
    func = core.FunctionCall(
        dialect, "SDO_RELATE",
        geom1,
        geom2,
        core.Literal(dialect, f"mask={mask}"),
    )
    return func == core.Literal(dialect, "TRUE")


def sdo_geom_from_wkt(
    dialect: "OracleDialect",
    wkt: str,
    srid: int = 4326,
) -> "bases.BaseExpression":
    """Oracle SDO_GEOMETRY constructor from WKT string.

    The named construction for a geometry given as data: it takes the WKT
    itself rather than a column name, so a caller states which it has instead
    of leaving a converter to work it out.

    Args:
        dialect: The Oracle dialect instance
        wkt: Well-Known Text describing the geometry
        srid: Spatial reference system ID

    Returns:
        A FunctionCall instance representing SDO_GEOMETRY
    """
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(
        dialect, "SDO_GEOMETRY",
        core.Literal(dialect, wkt),
        core.Literal(dialect, srid),
    )