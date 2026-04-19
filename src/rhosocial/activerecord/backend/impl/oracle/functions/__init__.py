# functions/__init__.py
"""
Oracle-specific SQL function factories.

This package provides factory functions for creating Oracle-specific SQL expression
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

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases


def _convert_to_expression(dialect: "SQLDialectBase", expr: Union[str, "bases.BaseExpression"],
                           handle_numeric_literals: bool = True) -> "bases.BaseExpression":
    """Helper function to convert an input value to an appropriate BaseExpression."""
    from rhosocial.activerecord.backend.expression import bases, core
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif handle_numeric_literals and isinstance(expr, (int, float)):
        return core.Literal(dialect, expr)
    else:
        return core.Column(dialect, expr)


# Import all function modules
from .json import (
    json_value, json_query, json_exists,
    json_object_expr, json_array_expr, json_serialize,
    json_table
)

from .spatial import (
    sdo_geom_distance, sdo_within_distance,
    sdo_contains, sdo_inside, sdo_relate,
    sdo_geom_from_wkt
)

from .null import (
    nvl, nvl2, coalesce_oracle, nullif, lnnvl
)

from .string import (
    decode_expr, regexp_substr, regexp_instr,
    regexp_like, regexp_replace, regexp_count
)

from .datetime import (
    to_date, to_char, to_timestamp, to_timestamp_tz,
    trunc_date, add_months, months_between,
    last_day, next_day, extract_date
)

from .analytic import (
    listagg, percentile_cont, percentile_disc
)

from .conversion import (
    to_number, cast_expr, to_binary_double, to_binary_float
)

__all__ = [
    # JSON functions (12c+)
    "json_value", "json_query", "json_exists",
    "json_object_expr", "json_array_expr", "json_serialize",
    "json_table",
    # Spatial functions
    "sdo_geom_distance", "sdo_within_distance",
    "sdo_contains", "sdo_inside", "sdo_relate",
    "sdo_geom_from_wkt",
    # Null handling functions
    "nvl", "nvl2", "coalesce_oracle", "nullif", "lnnvl",
    # String functions
    "decode_expr", "regexp_substr", "regexp_instr",
    "regexp_like", "regexp_replace", "regexp_count",
    # Date functions
    "to_date", "to_char", "to_timestamp", "to_timestamp_tz",
    "trunc_date", "add_months", "months_between",
    "last_day", "next_day", "extract_date",
    # Analytic functions
    "listagg", "percentile_cont", "percentile_disc",
    # Conversion functions
    "to_number", "cast_expr", "to_binary_double", "to_binary_float",
]
