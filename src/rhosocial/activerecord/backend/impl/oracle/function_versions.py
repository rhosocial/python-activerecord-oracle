# src/rhosocial/activerecord/backend/impl/oracle/function_versions.py
"""
Oracle function version requirements.

Defines the minimum and maximum Oracle versions for each SQL function
supported by the backend. Used by dialect.supports_functions() to
build the function availability map.

Version format: (major, minor, patch)
Reference:
- Oracle 8i: (8, 1, 0)
- Oracle 9i: (9, 0, 0) / (9, 2, 0)
- Oracle 10g: (10, 0, 0)
- Oracle 11g: (11, 0, 0) / (11, 2, 0)
- Oracle 12c: (12, 0, 0) / (12, 1, 0) / (12, 2, 0)
- Oracle 18c: (18, 0, 0)
- Oracle 19c: (19, 0, 0)
- Oracle 21c: (21, 0, 0)
- Oracle 23ai: (23, 0, 0)
"""

from typing import Dict, Optional, Tuple

# Each entry: function_name -> (min_version, max_version)
# max_version=None means no upper bound

# JSON functions (12c+)
#
# Every entry here is a **SQL-callable function or operator**, because that is
# what ``Dialect.supports_functions()`` answers about: whether the dialect can
# put this name in a statement.  ``ALL_PROCEDURES`` is where the server
# registers the ones it actually has, and it is the check to run before adding
# an entry - see the note on ``json_schema`` below, which is an entry this file
# used to carry.
JSON_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "json_value": ((12, 1, 0), None),
    "json_query": ((12, 1, 0), None),
    "json_exists": ((12, 1, 0), None),
    "json_object": ((12, 2, 0), None),
    "json_array": ((12, 2, 0), None),
    "json_serialize": ((19, 0, 0), None),
    "json_table": ((12, 1, 0), None),
    "json_dataguide": ((12, 2, 0), None),
    # ``json_transform`` used to be gated at ((23, 0, 0)), which was
    # measurably too high: the function works on 21c too, so the old threshold
    # made the backend refuse a capability a 21 server has.  Measured, as
    # ``system``:
    #
    #   21c  (21.3.0.0.0)  and  26ai (23.26.1.0.0), both return {"a":42}:
    #       SELECT JSON_TRANSFORM('{"a":0}', SET '$.a' = 42) FROM DUAL
    #
    # Note this is the *opposite* failure from a fabrication: the old number was
    # real, just later than the feature.  A too-high threshold refuses something
    # the server does do, which is recoverable; a too-low one claims something
    # it does not, which is not.
    "json_transform": ((21, 0, 0), None),
    "json_mergepatch": ((19, 0, 0), None),
    "json_equal": ((21, 0, 0), None),
    # ``json_schema`` was here at ((21, 0, 0), None) and has been withdrawn.
    # There is no SQL function of that name, so no version could be a true
    # minimum and any threshold would be a fabrication - the same defect the
    # version-detection fix removed elsewhere.  Measured, as ``system``:
    #
    #   21c  (21.3.0.0.0) and 26ai (23.26.1.0.0), all ORA-00904 "invalid
    #   identifier":
    #       JSON_SCHEMA('{}')  JSON_SCHEMA(CAST('{}' AS JSON))
    #       SYS.JSON_SCHEMA('{}')  JSON_SCHEMA  JSON_SCHEMA()
    #       JSON_SCHEMAVALID('{}')  JSON_SCHEMA_VALID('{}')
    #
    #   ``ALL_PROCEDURES`` on 26ai - where Oracle registers every SQL-callable
    #   JSON function it has - holds JSON$AUTO_INDEX, JSON_DATAGUIDE and
    #   JSON_HIERDATAGUIDE, and **no** row named JSON_SCHEMA.
    #
    #   Oracle AI Database 26ai SQL Language Reference has no JSON_SCHEMA
    #   function page either: ``/26/sqlrf/JSON_SCHEMA.html`` is a 404.
    #
    # The three things the name *does* denote, none of which is what this entry
    # named, and each measured on both wired servers:
    #
    #   * ``DBMS_JSON_SCHEMA`` - a PL/SQL **package**, not a SQL function.
    #     ``ALL_OBJECTS`` holds 2 rows for it on 26ai and **0** on 21c, so it
    #     arrived on the 23ai line and is PL/SQL-only either way.
    #   * the ``JSON_SCHEMA`` **clause** of ``JSON_TABLE`` - a clause inside a
    #     table function, not a scalar. The 26ai parser knows the keyword (it
    #     rejects the clause on its *argument*: ORA-03046 pointing at
    #     ``... PATH '$.a')) JSON_SCHEMA '``); 21c does not recognise it at all
    #     (ORA-00933).
    #   * ``JSON_SCHEMA_VALID`` - a **condition**, not a function. Measured
    #     unusable as a function on both servers, and in a predicate on 21c it
    #     is ORA-00920 "invalid relational operator".
    #
    # What replaces it, for a caller that wants JSON Schema validation: call
    # ``DBMS_JSON_SCHEMA`` from PL/SQL on 23ai and later.
}

# Spatial functions (SDO)
SPATIAL_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "sdo_geom_distance": ((10, 0, 0), None),
    "sdo_within_distance": ((9, 0, 0), None),
    "sdo_contains": ((9, 0, 0), None),
    "sdo_inside": ((9, 0, 0), None),
    "sdo_relate": ((9, 0, 0), None),
    "sdo_geom_from_wkt": ((10, 0, 0), None),
    "sdo_nn": ((9, 0, 0), None),
    "sdo_filter": ((9, 0, 0), None),
    "sdo_join": ((10, 0, 0), None),
    "sdo_intersection": ((10, 0, 0), None),
    "sdo_union": ((10, 0, 0), None),
    "sdo_difference": ((10, 0, 0), None),
    "sdo_buffer": ((9, 0, 0), None),
    "sdo_area": ((9, 0, 0), None),
    "sdo_length": ((9, 0, 0), None),
    "sdo_distance": ((9, 0, 0), None),
}

# Null handling functions
NULL_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "nvl": ((8, 0, 0), None),
    "nvl2": ((9, 0, 0), None),
    "coalesce": ((9, 0, 0), None),
    "nullif": ((9, 0, 0), None),
    "lnnvl": ((10, 0, 0), None),
}

# String functions
STRING_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "decode": ((8, 0, 0), None),
    "regexp_substr": ((10, 0, 0), None),
    "regexp_instr": ((10, 0, 0), None),
    "regexp_like": ((10, 0, 0), None),
    "regexp_replace": ((10, 0, 0), None),
    "regexp_count": ((12, 2, 0), None),
    "instr": ((8, 0, 0), None),
    "substr": ((8, 0, 0), None),
    "length": ((8, 0, 0), None),
    "upper": ((8, 0, 0), None),
    "lower": ((8, 0, 0), None),
    "trim": ((8, 0, 0), None),
    "ltrim": ((8, 0, 0), None),
    "rtrim": ((8, 0, 0), None),
    "replace": ((8, 0, 0), None),
    "translate": ((8, 0, 0), None),
    "concat": ((8, 0, 0), None),
    "lengthb": ((8, 0, 0), None),
    "substrb": ((8, 0, 0), None),
}

# Date/Time functions
DATETIME_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "to_date": ((8, 0, 0), None),
    "to_char": ((8, 0, 0), None),
    "to_timestamp": ((9, 0, 0), None),
    "to_timestamp_tz": ((9, 0, 0), None),
    "trunc": ((8, 0, 0), None),
    "sysdate": ((8, 0, 0), None),
    "systimestamp": ((9, 0, 0), None),
    "add_months": ((8, 0, 0), None),
    "months_between": ((8, 0, 0), None),
    "last_day": ((8, 0, 0), None),
    "next_day": ((8, 0, 0), None),
    "extract": ((9, 0, 0), None),
    "current_date": ((8, 0, 0), None),
    "current_timestamp": ((8, 0, 0), None),
    "localtimestamp": ((9, 0, 0), None),
    "new_time": ((8, 0, 0), None),
    "numtodsinterval": ((9, 0, 0), None),
    "numtoyminterval": ((9, 0, 0), None),
    "from_tz": ((9, 0, 0), None),
}

# Analytic/Window functions
ANALYTIC_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "listagg": ((11, 2, 0), None),
    "percentile_cont": ((9, 0, 0), None),
    "percentile_disc": ((9, 0, 0), None),
    "ratio_to_report": ((9, 0, 0), None),
    "ntile": ((8, 1, 0), None),
    "lag": ((8, 1, 0), None),
    "lead": ((8, 1, 0), None),
    "first_value": ((8, 1, 0), None),
    "last_value": ((8, 1, 0), None),
    "row_number": ((8, 1, 0), None),
    "rank": ((8, 1, 0), None),
    "dense_rank": ((8, 1, 0), None),
    "cume_dist": ((8, 1, 0), None),
    "percent_rank": ((8, 1, 0), None),
    "stddev": ((8, 1, 0), None),
    "variance": ((8, 1, 0), None),
}

# Conversion functions
CONVERSION_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "to_number": ((8, 0, 0), None),
    "cast": ((8, 0, 0), None),
    "convert": ((8, 0, 0), None),
    "to_binary_double": ((10, 0, 0), None),
    "to_binary_float": ((10, 0, 0), None),
    "to_clob": ((8, 0, 0), None),
    "to_nclob": ((9, 0, 0), None),
    "to_blob": ((8, 0, 0), None),
    "bin_to_num": ((9, 0, 0), None),
    "numtoyminterval": ((9, 0, 0), None),
    "numtodsinterval": ((9, 0, 0), None),
}

# Aggregate functions
AGGREGATE_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "count": ((8, 0, 0), None),
    "sum": ((8, 0, 0), None),
    "avg": ((8, 0, 0), None),
    "min": ((8, 0, 0), None),
    "max": ((8, 0, 0), None),
    "stddev": ((8, 0, 0), None),
    "variance": ((8, 0, 0), None),
    "corr": ((8, 1, 0), None),
    "median": ((10, 0, 0), None),
    "stats_mode": ((10, 0, 0), None),
}

# Mathematical functions
MATH_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "abs": ((8, 0, 0), None),
    "ceil": ((8, 0, 0), None),
    "floor": ((8, 0, 0), None),
    "mod": ((8, 0, 0), None),
    "power": ((8, 0, 0), None),
    "sqrt": ((8, 0, 0), None),
    "exp": ((8, 0, 0), None),
    "ln": ((8, 0, 0), None),
    "log": ((8, 0, 0), None),
    "round": ((8, 0, 0), None),
    "trunc": ((8, 0, 0), None),
    "sin": ((8, 0, 0), None),
    "cos": ((8, 0, 0), None),
    "tan": ((8, 0, 0), None),
    "asin": ((8, 0, 0), None),
    "acos": ((8, 0, 0), None),
    "atan": ((8, 0, 0), None),
    "sign": ((8, 0, 0), None),
    "greatest": ((8, 0, 0), None),
    "least": ((8, 0, 0), None),
}

# XML functions
XML_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {
    "xmltype": ((9, 0, 0), None),
    "xmlelement": ((9, 0, 0), None),
    "xmlforest": ((9, 0, 0), None),
    "xmlagg": ((9, 0, 0), None),
    "xmlquery": ((9, 2, 0), None),
    "xmltable": ((9, 2, 0), None),
    "existsnode": ((9, 2, 0), None),
    "extract": ((9, 0, 0), None),
    "extractvalue": ((9, 0, 0), None),
    "xmltransform": ((9, 0, 0), None),
    "xmlsequence": ((9, 2, 0), None),
    "xmlpi": ((9, 0, 0), None),
    "xmlroot": ((9, 0, 0), None),
    "xmlserialize": ((9, 0, 0), None),
}

# Combine all function versions
ORACLE_FUNCTION_VERSIONS: Dict[str, Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]] = {}

for _category in [
    JSON_FUNCTION_VERSIONS,
    SPATIAL_FUNCTION_VERSIONS,
    NULL_FUNCTION_VERSIONS,
    STRING_FUNCTION_VERSIONS,
    DATETIME_FUNCTION_VERSIONS,
    ANALYTIC_FUNCTION_VERSIONS,
    CONVERSION_FUNCTION_VERSIONS,
    AGGREGATE_FUNCTION_VERSIONS,
    MATH_FUNCTION_VERSIONS,
    XML_FUNCTION_VERSIONS,
]:
    ORACLE_FUNCTION_VERSIONS.update(_category)