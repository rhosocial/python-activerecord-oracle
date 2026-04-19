# functions/json.py
"""
Oracle JSON function factories.

JSON functions require Oracle 12c (12.1.0.2+) for basic support,
and Oracle 21c+ for native JSON type.
"""

from typing import Union, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core
    from ..dialect import OracleDialect


def json_value(dialect: "OracleDialect",
               json_doc: Union[str, "bases.BaseExpression"],
               path: str,
               returning: Optional[str] = None) -> "core.FunctionCall":
    """Creates a JSON_VALUE function call.
    
    Extracts a scalar value from a JSON document at the specified path.
    
    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document (column or literal)
        path: JSON path expression
        returning: Optional return type (VARCHAR2, NUMBER, DATE, etc.)
    
    Returns:
        A FunctionCall instance representing JSON_VALUE
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    if returning:
        return core.FunctionCall(dialect, "JSON_VALUE", doc_expr, path_expr,
                                  core.RawSQLExpression(dialect, f"RETURNING {returning}"))
    return core.FunctionCall(dialect, "JSON_VALUE", doc_expr, path_expr)


def json_query(dialect: "OracleDialect",
               json_doc: Union[str, "bases.BaseExpression"],
               path: str,
               returning: Optional[str] = None) -> "core.FunctionCall":
    """Creates a JSON_QUERY function call.
    
    Extracts a JSON object or array from a JSON document.
    
    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document or column
        path: JSON path expression
        returning: Optional return type
    
    Returns:
        A FunctionCall instance representing JSON_QUERY
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    if returning:
        return core.FunctionCall(dialect, "JSON_QUERY", doc_expr, path_expr,
                                  core.RawSQLExpression(dialect, f"RETURNING {returning}"))
    return core.FunctionCall(dialect, "JSON_QUERY", doc_expr, path_expr)


def json_exists(dialect: "OracleDialect",
                json_doc: Union[str, "bases.BaseExpression"],
                path: str) -> "core.FunctionCall":
    """Creates a JSON_EXISTS function call.
    
    Checks if a JSON path exists in a JSON document.
    
    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document or column
        path: JSON path expression
    
    Returns:
        A FunctionCall instance representing JSON_EXISTS
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    return core.FunctionCall(dialect, "JSON_EXISTS", doc_expr, path_expr)


def json_object_expr(dialect: "OracleDialect", *key_value_pairs: Any) -> "core.FunctionCall":
    """Creates a JSON_OBJECT function call.
    
    Creates a JSON object from key-value pairs.
    
    Args:
        dialect: The Oracle dialect instance
        *key_value_pairs: Alternating keys and values
    
    Returns:
        A FunctionCall instance representing JSON_OBJECT
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    if not key_value_pairs:
        return core.FunctionCall(dialect, "JSON_OBJECT")
    args = [core.Literal(dialect, val) for val in key_value_pairs]
    return core.FunctionCall(dialect, "JSON_OBJECT", *args)


def json_array_expr(dialect: "OracleDialect", *values: Any) -> "core.FunctionCall":
    """Creates a JSON_ARRAY function call.
    
    Creates a JSON array from values.
    
    Args:
        dialect: The Oracle dialect instance
        *values: Values to include in the array
    
    Returns:
        A FunctionCall instance representing JSON_ARRAY
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    if not values:
        return core.FunctionCall(dialect, "JSON_ARRAY")
    args = [core.Literal(dialect, v) for v in values]
    return core.FunctionCall(dialect, "JSON_ARRAY", *args)


def json_serialize(dialect: "OracleDialect",
                   json_doc: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """Creates a JSON_SERIALIZE function call.
    
    Serializes a JSON document to a string.
    
    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document or column
    
    Returns:
        A FunctionCall instance representing JSON_SERIALIZE
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    return core.FunctionCall(dialect, "JSON_SERIALIZE", doc_expr)


def json_table(dialect: "OracleDialect",
               json_doc: Union[str, "bases.BaseExpression"],
               path: str,
               columns: str) -> "operators.RawSQLExpression":
    """Creates a JSON_TABLE expression.
    
    Queries JSON data as a relational table.
    
    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document or column
        path: JSON path expression for root
        columns: Column definitions
    
    Returns:
        A RawSQLExpression instance
    
    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import operators
    from . import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    doc_sql, doc_params = doc_expr.to_sql()
    sql = f"JSON_TABLE({doc_sql}, '{path}' COLUMNS ({columns}))"
    return operators.RawSQLExpression(dialect, sql, tuple(doc_params))
