# functions/spatial.py
"""
Oracle Spatial function factories.

Spatial functions require Oracle with Spatial option.
"""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core, operators
    from ..dialect import OracleDialect


def sdo_geom_distance(dialect: "OracleDialect",
                      geom1: Union[str, "bases.BaseExpression"],
                      geom2: Union[str, "bases.BaseExpression"],
                      tolerance: float = 0.005) -> "core.FunctionCall":
    """Creates an SDO_GEOM.SDO_DISTANCE function call.
    
    Returns the distance between two geometries.
    
    Args:
        dialect: The Oracle dialect instance
        geom1: First geometry (SDO_GEOMETRY)
        geom2: Second geometry (SDO_GEOMETRY)
        tolerance: Tolerance value (default 0.005)
    
    Returns:
        A FunctionCall instance representing SDO_GEOM.SDO_DISTANCE
    
    Version: Oracle with Spatial option
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    geom1_expr = _convert_to_expression(dialect, geom1)
    geom2_expr = _convert_to_expression(dialect, geom2)
    tol_expr = core.Literal(dialect, tolerance)
    return core.FunctionCall(dialect, "SDO_GEOM.SDO_DISTANCE", geom1_expr, geom2_expr, tol_expr)


def sdo_within_distance(dialect: "OracleDialect",
                        geom1: Union[str, "bases.BaseExpression"],
                        geom2: Union[str, "bases.BaseExpression"],
                        distance: float,
                        tolerance: float = 0.005) -> "operators.RawSQLExpression":
    """Creates an SDO_WITHIN_DISTANCE expression.
    
    Checks if two geometries are within a specified distance.
    
    Args:
        dialect: The Oracle dialect instance
        geom1: First geometry
        geom2: Second geometry
        distance: Distance value
        tolerance: Tolerance value
    
    Returns:
        A RawSQLExpression instance
    
    Version: Oracle with Spatial option
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    params = geom1_params + geom2_params + [distance, tolerance]
    sql = f"SDO_WITHIN_DISTANCE({geom1_sql}, {geom2_sql}, 'distance={distance}') = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, tuple(params))


def sdo_contains(dialect: "OracleDialect",
                 geom1: Union[str, "bases.BaseExpression"],
                 geom2: Union[str, "bases.BaseExpression"]) -> "operators.RawSQLExpression":
    """Creates an SDO_CONTAINS expression.
    
    Checks if geom1 contains geom2.
    
    Args:
        dialect: The Oracle dialect instance
        geom1: First geometry (container)
        geom2: Second geometry (contained)
    
    Returns:
        A RawSQLExpression instance
    
    Version: Oracle with Spatial option
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    sql = f"SDO_CONTAINS({geom1_sql}, {geom2_sql}) = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, geom1_params + geom2_params)


def sdo_inside(dialect: "OracleDialect",
               geom1: Union[str, "bases.BaseExpression"],
               geom2: Union[str, "bases.BaseExpression"]) -> "operators.RawSQLExpression":
    """Creates an SDO_INSIDE expression.
    
    Checks if geom1 is inside geom2.
    
    Args:
        dialect: The Oracle dialect instance
        geom1: First geometry (inside)
        geom2: Second geometry (container)
    
    Returns:
        A RawSQLExpression instance
    
    Version: Oracle with Spatial option
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    sql = f"SDO_INSIDE({geom1_sql}, {geom2_sql}) = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, geom1_params + geom2_params)


def sdo_relate(dialect: "OracleDialect",
               geom1: Union[str, "bases.BaseExpression"],
               geom2: Union[str, "bases.BaseExpression"],
               mask: str = "ANYINTERACT") -> "operators.RawSQLExpression":
    """Creates an SDO_RELATE expression.
    
    Checks spatial relationship between two geometries.
    
    Args:
        dialect: The Oracle dialect instance
        geom1: First geometry
        geom2: Second geometry
        mask: Relationship mask (ANYINTERACT, CONTAINS, INSIDE, etc.)
    
    Returns:
        A RawSQLExpression instance
    
    Version: Oracle with Spatial option
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    sql = f"SDO_RELATE({geom1_sql}, {geom2_sql}, 'mask={mask}') = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, geom1_params + geom2_params)


def sdo_geom_from_wkt(dialect: "OracleDialect",
                      wkt: str,
                      srid: int = 4326) -> "operators.RawSQLExpression":
    """Creates an SDO_GEOMETRY from WKT string.
    
    Args:
        dialect: The Oracle dialect instance
        wkt: Well-Known Text string
        srid: Spatial reference ID (default 4326)
    
    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    sql = f"SDO_GEOMETRY('{wkt}', {srid})"
    return operators.RawSQLExpression(dialect, sql, ())
