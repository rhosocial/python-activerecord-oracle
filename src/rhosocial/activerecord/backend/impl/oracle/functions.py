# src/rhosocial/activerecord/backend/impl/oracle/functions.py
"""
Oracle-specific SQL function factories.

This module provides factory functions for creating Oracle-specific SQL expression
objects, including JSON functions, spatial functions, analytic functions,
and Oracle-specific utility functions.

Usage Rules:
- All functions accept a dialect instance as the first argument
- For column references, pass Column objects or column name strings
- For literal values, pass the value directly (will be converted to Literal)
- Functions return appropriate expression objects (FunctionCall, RawSQLExpression, etc.)

Version Requirements:
- JSON functions: Oracle 12c (12.1.0.2+) / 21c for JSON type
- Spatial functions: All Oracle versions with Spatial option
- Analytic functions: Oracle 8i+ (enhanced in 9i, 10g, 11g)
- LISTAGG: Oracle 11g R2+
"""
from typing import Union, Optional, List, Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core, operators
    from .dialect import OracleDialect


def _convert_to_expression(dialect: "SQLDialectBase", expr: Union[str, "bases.BaseExpression"],
                           handle_numeric_literals: bool = True) -> "bases.BaseExpression":
    """
    Helper function to convert an input value to an appropriate BaseExpression.

    Args:
        dialect: The SQL dialect instance
        expr: The expression to convert
        handle_numeric_literals: Whether to treat numeric values as literals

    Returns:
        A BaseExpression instance
    """
    from rhosocial.activerecord.backend.expression import bases, core
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif handle_numeric_literals and isinstance(expr, (int, float)):
        return core.Literal(dialect, expr)
    else:
        return core.Column(dialect, expr)


# region JSON Function Factories (Oracle 12c+)

def json_value(dialect: "OracleDialect",
               json_doc: Union[str, "bases.BaseExpression"],
               path: str,
               returning: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a JSON_VALUE function call.

    Extracts a scalar value from a JSON document at the specified path.

    Usage rules:
    - To extract from a column: json_value(dialect, Column(dialect, "json_col"), "$.name")
    - With returning type: json_value(dialect, col, "$.id", returning="NUMBER")

    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document (column or literal)
        path: JSON path expression
        returning: Optional return type (VARCHAR2, NUMBER, DATE, etc.)

    Returns:
        A FunctionCall instance representing JSON_VALUE

    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core, operators
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
    """
    Creates a JSON_QUERY function call.

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
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)

    if returning:
        return core.FunctionCall(dialect, "JSON_QUERY", doc_expr, path_expr,
                                 core.RawSQLExpression(dialect, f"RETURNING {returning}"))
    return core.FunctionCall(dialect, "JSON_QUERY", doc_expr, path_expr)


def json_exists(dialect: "OracleDialect",
                json_doc: Union[str, "bases.BaseExpression"],
                path: str) -> "core.FunctionCall":
    """
    Creates a JSON_EXISTS function call.

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
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    return core.FunctionCall(dialect, "JSON_EXISTS", doc_expr, path_expr)


def json_object_expr(dialect: "OracleDialect",
                     *key_value_pairs: Any) -> "core.FunctionCall":
    """
    Creates a JSON_OBJECT function call.

    Creates a JSON object from key-value pairs.

    Usage rules:
    - Empty object: json_object_expr(dialect)
    - With values: json_object_expr(dialect, "name", "John", "age", 30)
    - Using KEY keyword: json_object_expr(dialect, "name" KEY "John")

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
    args = []
    for val in key_value_pairs:
        args.append(core.Literal(dialect, val))
    return core.FunctionCall(dialect, "JSON_OBJECT", *args)


def json_array_expr(dialect: "OracleDialect",
                    *values: Any) -> "core.FunctionCall":
    """
    Creates a JSON_ARRAY function call.

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
    """
    Creates a JSON_SERIALIZE function call.

    Serializes a JSON document to a string.

    Args:
        dialect: The Oracle dialect instance
        json_doc: JSON document or column

    Returns:
        A FunctionCall instance representing JSON_SERIALIZE

    Version: Oracle 12c (12.1.0.2+)
    """
    from rhosocial.activerecord.backend.expression import core
    doc_expr = _convert_to_expression(dialect, json_doc)
    return core.FunctionCall(dialect, "JSON_SERIALIZE", doc_expr)


# endregion JSON Function Factories


# region Spatial Function Factories (Oracle Spatial)

def sdo_geom_distance(dialect: "OracleDialect",
                      geom1: Union[str, "bases.BaseExpression"],
                      geom2: Union[str, "bases.BaseExpression"],
                      tolerance: float = 0.005) -> "core.FunctionCall":
    """
    Creates an SDO_GEOM.SDO_DISTANCE function call.

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
    geom1_expr = _convert_to_expression(dialect, geom1)
    geom2_expr = _convert_to_expression(dialect, geom2)
    tol_expr = core.Literal(dialect, tolerance)
    return core.FunctionCall(dialect, "SDO_GEOM.SDO_DISTANCE", geom1_expr, geom2_expr, tol_expr)


def sdo_within_distance(dialect: "OracleDialect",
                        geom1: Union[str, "bases.BaseExpression"],
                        geom2: Union[str, "bases.BaseExpression"],
                        distance: float,
                        tolerance: float = 0.005) -> "operators.RawSQLExpression":
    """
    Creates an SDO_WITHIN_DISTANCE expression.

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
    from rhosocial.activerecord.backend.expression import core, operators
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    params = geom1_params + geom2_params + [distance, tolerance]
    sql = f"SDO_WITHIN_DISTANCE({geom1_sql}, {geom2_sql}, 'distance={distance}') = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, tuple(params))


def sdo_contains(dialect: "OracleDialect",
                 geom1: Union[str, "bases.BaseExpression"],
                 geom2: Union[str, "bases.BaseExpression"]) -> "operators.RawSQLExpression":
    """
    Creates an SDO_CONTAINS expression.

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
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    sql = f"SDO_CONTAINS({geom1_sql}, {geom2_sql}) = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, geom1_params + geom2_params)


def sdo_inside(dialect: "OracleDialect",
               geom1: Union[str, "bases.BaseExpression"],
               geom2: Union[str, "bases.BaseExpression"]) -> "operators.RawSQLExpression":
    """
    Creates an SDO_INSIDE expression.

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
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    sql = f"SDO_INSIDE({geom1_sql}, {geom2_sql}) = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, geom1_params + geom2_params)


def sdo_relate(dialect: "OracleDialect",
               geom1: Union[str, "bases.BaseExpression"],
               geom2: Union[str, "bases.BaseExpression"],
               mask: str = "ANYINTERACT") -> "operators.RawSQLExpression":
    """
    Creates an SDO_RELATE expression.

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
    geom1_sql, geom1_params = _convert_to_expression(dialect, geom1).to_sql()
    geom2_sql, geom2_params = _convert_to_expression(dialect, geom2).to_sql()
    sql = f"SDO_RELATE({geom1_sql}, {geom2_sql}, 'mask={mask}') = 'TRUE'"
    return operators.RawSQLExpression(dialect, sql, geom1_params + geom2_params)


# endregion Spatial Function Factories


# region Null Handling Functions

def nvl(dialect: "OracleDialect",
        expr1: Union[str, "bases.BaseExpression"],
        expr2: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """
    Creates an NVL function call.

    Returns expr2 if expr1 is null, otherwise returns expr1.

    Args:
        dialect: The Oracle dialect instance
        expr1: First expression
        expr2: Replacement value if expr1 is null

    Returns:
        A FunctionCall instance representing NVL
    """
    from rhosocial.activerecord.backend.expression import core
    expr1_val = _convert_to_expression(dialect, expr1)
    expr2_val = _convert_to_expression(dialect, expr2)
    return core.FunctionCall(dialect, "NVL", expr1_val, expr2_val)


def nvl2(dialect: "OracleDialect",
         expr1: Union[str, "bases.BaseExpression"],
         expr2: Union[str, "bases.BaseExpression"],
         expr3: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """
    Creates an NVL2 function call.

    Returns expr2 if expr1 is not null, otherwise returns expr3.

    Args:
        dialect: The Oracle dialect instance
        expr1: Expression to check
        expr2: Value if expr1 is not null
        expr3: Value if expr1 is null

    Returns:
        A FunctionCall instance representing NVL2
    """
    from rhosocial.activerecord.backend.expression import core
    expr1_val = _convert_to_expression(dialect, expr1)
    expr2_val = _convert_to_expression(dialect, expr2)
    expr3_val = _convert_to_expression(dialect, expr3)
    return core.FunctionCall(dialect, "NVL2", expr1_val, expr2_val, expr3_val)


def coalesce_oracle(dialect: "OracleDialect",
                    *expressions: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """
    Creates a COALESCE function call (Oracle version).

    Returns the first non-null expression.

    Args:
        dialect: The Oracle dialect instance
        *expressions: Expressions to evaluate

    Returns:
        A FunctionCall instance representing COALESCE
    """
    from rhosocial.activerecord.backend.expression import core
    args = [_convert_to_expression(dialect, expr) for expr in expressions]
    return core.FunctionCall(dialect, "COALESCE", *args)


def nullif(dialect: "OracleDialect",
           expr1: Union[str, "bases.BaseExpression"],
           expr2: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """
    Creates a NULLIF function call.

    Returns null if expr1 equals expr2, otherwise returns expr1.

    Args:
        dialect: The Oracle dialect instance
        expr1: First expression
        expr2: Second expression

    Returns:
        A FunctionCall instance representing NULLIF
    """
    from rhosocial.activerecord.backend.expression import core
    expr1_val = _convert_to_expression(dialect, expr1)
    expr2_val = _convert_to_expression(dialect, expr2)
    return core.FunctionCall(dialect, "NULLIF", expr1_val, expr2_val)


# endregion Null Handling Functions


# region String Functions

def decode_expr(dialect: "OracleDialect",
                expr: Union[str, "bases.BaseExpression"],
                *search_result_pairs: Any,
                default: Any = None) -> "core.FunctionCall":
    """
    Creates a DECODE function call.

    Oracle's DECODE is similar to CASE expression.

    Usage rules:
    - Basic: decode_expr(dialect, col, "A", "Apple", "B", "Banana", "Unknown")
    - With default: decode_expr(dialect, col, "A", "Apple", default="Other")

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to compare
        *search_result_pairs: Pairs of search value and result
        default: Optional default value

    Returns:
        A FunctionCall instance representing DECODE
    """
    from rhosocial.activerecord.backend.expression import core
    args = [_convert_to_expression(dialect, expr)]
    for val in search_result_pairs:
        args.append(core.Literal(dialect, val))
    if default is not None:
        args.append(core.Literal(dialect, default))
    return core.FunctionCall(dialect, "DECODE", *args)


def regexp_substr(dialect: "OracleDialect",
                  source: Union[str, "bases.BaseExpression"],
                  pattern: str,
                  position: int = 1,
                  occurrence: int = 1,
                  match_param: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a REGEXP_SUBSTR function call.

    Extracts substring matching a regular expression.

    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        position: Starting position (default 1)
        occurrence: Occurrence to find (default 1)
        match_param: Match parameters (i, c, m, n, x)

    Returns:
        A FunctionCall instance representing REGEXP_SUBSTR

    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    position_expr = core.Literal(dialect, position)
    occurrence_expr = core.Literal(dialect, occurrence)

    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_SUBSTR", source_expr, pattern_expr,
                                 position_expr, occurrence_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_SUBSTR", source_expr, pattern_expr,
                             position_expr, occurrence_expr)


def regexp_instr(dialect: "OracleDialect",
                 source: Union[str, "bases.BaseExpression"],
                 pattern: str,
                 position: int = 1,
                 occurrence: int = 1,
                 return_opt: int = 0,
                 match_param: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a REGEXP_INSTR function call.

    Returns the position of a pattern match.

    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        position: Starting position (default 1)
        occurrence: Occurrence to find (default 1)
        return_opt: 0 for start position, 1 for end position
        match_param: Match parameters

    Returns:
        A FunctionCall instance representing REGEXP_INSTR

    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    position_expr = core.Literal(dialect, position)
    occurrence_expr = core.Literal(dialect, occurrence)
    return_opt_expr = core.Literal(dialect, return_opt)

    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_INSTR", source_expr, pattern_expr,
                                 position_expr, occurrence_expr, return_opt_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_INSTR", source_expr, pattern_expr,
                             position_expr, occurrence_expr, return_opt_expr)


def regexp_like(dialect: "OracleDialect",
                source: Union[str, "bases.BaseExpression"],
                pattern: str,
                match_param: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a REGEXP_LIKE function call.

    Checks if a string matches a regular expression.

    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        match_param: Match parameters

    Returns:
        A FunctionCall instance representing REGEXP_LIKE

    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_LIKE", source_expr, pattern_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_LIKE", source_expr, pattern_expr)


def regexp_replace(dialect: "OracleDialect",
                   source: Union[str, "bases.BaseExpression"],
                   pattern: str,
                   replace: str,
                   position: int = 1,
                   occurrence: int = 0,
                   match_param: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a REGEXP_REPLACE function call.

    Replaces pattern matches in a string.

    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        replace: Replacement string
        position: Starting position (default 1)
        occurrence: Occurrence to replace (0 = all)
        match_param: Match parameters

    Returns:
        A FunctionCall instance representing REGEXP_REPLACE

    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    replace_expr = core.Literal(dialect, replace)
    position_expr = core.Literal(dialect, position)
    occurrence_expr = core.Literal(dialect, occurrence)

    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_REPLACE", source_expr, pattern_expr,
                                 replace_expr, position_expr, occurrence_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_REPLACE", source_expr, pattern_expr,
                             replace_expr, position_expr, occurrence_expr)


# endregion String Functions


# region Date Functions

def to_date(dialect: "OracleDialect",
            expr: Union[str, "bases.BaseExpression"],
            format: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a TO_DATE function call.

    Converts a string to a DATE value.

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Date format string

    Returns:
        A FunctionCall instance representing TO_DATE
    """
    from rhosocial.activerecord.backend.expression import core
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_DATE", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_DATE", expr_val)


def to_char(dialect: "OracleDialect",
            expr: Union[str, "bases.BaseExpression"],
            format: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a TO_CHAR function call.

    Converts a value to a string.

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Format string

    Returns:
        A FunctionCall instance representing TO_CHAR
    """
    from rhosocial.activerecord.backend.expression import core
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_CHAR", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_CHAR", expr_val)


def to_timestamp(dialect: "OracleDialect",
                 expr: Union[str, "bases.BaseExpression"],
                 format: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a TO_TIMESTAMP function call.

    Converts a string to a TIMESTAMP value.

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Timestamp format string

    Returns:
        A FunctionCall instance representing TO_TIMESTAMP
    """
    from rhosocial.activerecord.backend.expression import core
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_TIMESTAMP", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_TIMESTAMP", expr_val)


def trunc_date(dialect: "OracleDialect",
               expr: Union[str, "bases.BaseExpression"],
               fmt: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a TRUNC function call for dates.

    Truncates a date to a specified precision.

    Args:
        dialect: The Oracle dialect instance
        expr: Date expression
        fmt: Truncation format (YYYY, MM, DD, etc.)

    Returns:
        A FunctionCall instance representing TRUNC
    """
    from rhosocial.activerecord.backend.expression import core
    expr_val = _convert_to_expression(dialect, expr)
    if fmt:
        fmt_expr = core.Literal(dialect, fmt)
        return core.FunctionCall(dialect, "TRUNC", expr_val, fmt_expr)
    return core.FunctionCall(dialect, "TRUNC", expr_val)


def add_months(dialect: "OracleDialect",
               date_expr: Union[str, "bases.BaseExpression"],
               months: int) -> "core.FunctionCall":
    """
    Creates an ADD_MONTHS function call.

    Adds months to a date.

    Args:
        dialect: The Oracle dialect instance
        date_expr: Date expression
        months: Number of months to add (can be negative)

    Returns:
        A FunctionCall instance representing ADD_MONTHS
    """
    from rhosocial.activerecord.backend.expression import core
    date_val = _convert_to_expression(dialect, date_expr)
    months_expr = core.Literal(dialect, months)
    return core.FunctionCall(dialect, "ADD_MONTHS", date_val, months_expr)


def months_between(dialect: "OracleDialect",
                   date1: Union[str, "bases.BaseExpression"],
                   date2: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """
    Creates a MONTHS_BETWEEN function call.

    Returns the number of months between two dates.

    Args:
        dialect: The Oracle dialect instance
        date1: First date
        date2: Second date

    Returns:
        A FunctionCall instance representing MONTHS_BETWEEN
    """
    from rhosocial.activerecord.backend.expression import core
    date1_val = _convert_to_expression(dialect, date1)
    date2_val = _convert_to_expression(dialect, date2)
    return core.FunctionCall(dialect, "MONTHS_BETWEEN", date1_val, date2_val)


def last_day(dialect: "OracleDialect",
             date_expr: Union[str, "bases.BaseExpression"]) -> "core.FunctionCall":
    """
    Creates a LAST_DAY function call.

    Returns the last day of the month for the given date.

    Args:
        dialect: The Oracle dialect instance
        date_expr: Date expression

    Returns:
        A FunctionCall instance representing LAST_DAY
    """
    from rhosocial.activerecord.backend.expression import core
    date_val = _convert_to_expression(dialect, date_expr)
    return core.FunctionCall(dialect, "LAST_DAY", date_val)


def next_day(dialect: "OracleDialect",
             date_expr: Union[str, "bases.BaseExpression"],
             day: str) -> "core.FunctionCall":
    """
    Creates a NEXT_DAY function call.

    Returns the date of the next specified day of the week.

    Args:
        dialect: The Oracle dialect instance
        date_expr: Date expression
        day: Day of week (SUNDAY, MONDAY, etc.)

    Returns:
        A FunctionCall instance representing NEXT_DAY
    """
    from rhosocial.activerecord.backend.expression import core
    date_val = _convert_to_expression(dialect, date_expr)
    day_expr = core.Literal(dialect, day)
    return core.FunctionCall(dialect, "NEXT_DAY", date_val, day_expr)


# endregion Date Functions


# region Analytic Functions

def listagg(dialect: "OracleDialect",
            expr: Union[str, "bases.BaseExpression"],
            delimiter: str = ",",
            order_by: Optional[str] = None,
            on_overflow: Optional[str] = None) -> "operators.RawSQLExpression":
    """
    Creates a LISTAGG expression.

    Aggregates values into a delimited list.

    Usage rules:
    - Basic: listagg(dialect, col, ",")
    - With order: listagg(dialect, col, ",", "col ASC")
    - With overflow: listagg(dialect, col, ",", on_overflow="TRUNCATE")

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to aggregate
        delimiter: Delimiter string
        order_by: ORDER BY clause for list
        on_overflow: Overflow behavior (TRUNCATE or ERROR)

    Returns:
        A RawSQLExpression instance

    Version: Oracle 11g R2+
    """
    from rhosocial.activerecord.backend.expression import core, operators
    expr_sql, expr_params = _convert_to_expression(dialect, expr).to_sql()
    delim_expr = core.Literal(dialect, delimiter)
    delim_sql, delim_params = delim_expr.to_sql()

    parts = [f"LISTAGG({expr_sql}, {delim_sql})"]
    params = expr_params + delim_params

    if order_by:
        parts.append(f"WITHIN GROUP (ORDER BY {order_by})")

    if on_overflow:
        if on_overflow.upper() == "TRUNCATE":
            parts.append("ON OVERFLOW TRUNCATE")
        elif on_overflow.upper() == "ERROR":
            parts.append("ON OVERFLOW ERROR")

    sql = " ".join(parts)
    return operators.RawSQLExpression(dialect, sql, tuple(params))


# endregion Analytic Functions


# region Conversion Functions

def to_number(dialect: "OracleDialect",
              expr: Union[str, "bases.BaseExpression"],
              format: Optional[str] = None) -> "core.FunctionCall":
    """
    Creates a TO_NUMBER function call.

    Converts a value to a number.

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to convert
        format: Number format string

    Returns:
        A FunctionCall instance representing TO_NUMBER
    """
    from rhosocial.activerecord.backend.expression import core
    expr_val = _convert_to_expression(dialect, expr)
    if format:
        format_expr = core.Literal(dialect, format)
        return core.FunctionCall(dialect, "TO_NUMBER", expr_val, format_expr)
    return core.FunctionCall(dialect, "TO_NUMBER", expr_val)


def cast_expr(dialect: "OracleDialect",
              expr: Union[str, "bases.BaseExpression"],
              type_name: str) -> "operators.RawSQLExpression":
    """
    Creates a CAST expression.

    Casts an expression to a specified type.

    Args:
        dialect: The Oracle dialect instance
        expr: Expression to cast
        type_name: Target type name

    Returns:
        A RawSQLExpression instance
    """
    from rhosocial.activerecord.backend.expression import operators
    expr_sql, expr_params = _convert_to_expression(dialect, expr).to_sql()
    sql = f"CAST({expr_sql} AS {type_name})"
    return operators.RawSQLExpression(dialect, sql, expr_params)


# endregion Conversion Functions


__all__ = [
    # JSON functions (12c+)
    "json_value",
    "json_query",
    "json_exists",
    "json_object_expr",
    "json_array_expr",
    "json_serialize",
    # Spatial functions
    "sdo_geom_distance",
    "sdo_within_distance",
    "sdo_contains",
    "sdo_inside",
    "sdo_relate",
    # Null handling functions
    "nvl",
    "nvl2",
    "coalesce_oracle",
    "nullif",
    # String functions
    "decode_expr",
    "regexp_substr",
    "regexp_instr",
    "regexp_like",
    "regexp_replace",
    # Date functions
    "to_date",
    "to_char",
    "to_timestamp",
    "trunc_date",
    "add_months",
    "months_between",
    "last_day",
    "next_day",
    # Analytic functions
    "listagg",
    # Conversion functions
    "to_number",
    "cast_expr",
]
