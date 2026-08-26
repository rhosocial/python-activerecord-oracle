# src/rhosocial/activerecord/backend/impl/oracle/functions/json.py
"""Oracle JSON function factories."""

import re
from typing import Union, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from rhosocial.activerecord.backend.expression.types._base import DataType
    from ..dialect import OracleDialect

_RETURNING_WHITELIST = {
    "VARCHAR2": ("VarCharType", {"length": 4000}),
    "CLOB": ("TextType", {}),
    "NUMBER": ("DecimalType", {}),
    "DATE": ("DateType", {}),
    "TIMESTAMP": ("TimestampType", {}),
    "BOOLEAN": ("BooleanType", {}),
    "FLOAT": ("FloatType", {}),
    "BINARY_FLOAT": ("FloatType", {}),
    "BINARY_DOUBLE": ("DoubleType", {}),
    "INTEGER": ("IntegerType", {}),
    "INT": ("IntegerType", {}),
}

_PARAM_RE = re.compile(r"^(\w+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$")


def _coerce_returning_type(
    dialect: "OracleDialect",
    returning: Union[str, "DataType"],
) -> "DataType":
    """Coerce a returning-type specification into a safe DataType instance."""
    from rhosocial.activerecord.backend.expression.types._base import DataType

    if isinstance(returning, DataType):
        return returning
    if not isinstance(returning, str):
        raise TypeError(
            f"returning must be a DataType instance or type-name string, "
            f"got {type(returning).__name__}"
        )
    m = _PARAM_RE.match(returning.strip())
    if not m:
        raise TypeError(
            f"Unsupported Oracle RETURNING type: {returning!r}. "
            f"Use a DataType instance or one of: "
            f"{', '.join(sorted(_RETURNING_WHITELIST))}."
        )
    base_name = m.group(1).upper()
    spec = _RETURNING_WHITELIST.get(base_name)
    if spec is None:
        raise TypeError(
            f"Unsupported Oracle RETURNING type: {returning!r}. "
            f"Allowed: {', '.join(sorted(_RETURNING_WHITELIST))}."
        )
    cls_name, kwargs = spec
    from rhosocial.activerecord.backend.expression import types as _types
    cls = getattr(_types, cls_name)
    if m.group(2):
        if cls_name == "VarCharType":
            kwargs = {"length": int(m.group(2))}
        elif cls_name == "DecimalType":
            p = int(m.group(2))
            s = int(m.group(3)) if m.group(3) else None
            kwargs = {"precision": p, "scale": s}
    return cls(dialect=dialect, **kwargs)


def json_value(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
    path: str,
    returning: Union[str, "DataType", None] = None,
) -> "bases.BaseExpression":
    """Oracle JSON_VALUE: extract a scalar from a JSON document."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    func = core.FunctionCall(dialect, "JSON_VALUE", doc_expr, path_expr)
    if returning is not None:
        func._oracle_returning_type = _coerce_returning_type(dialect, returning)
    return func


def json_query(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
    path: str,
    returning: Union[str, "DataType", None] = None,
) -> "bases.BaseExpression":
    """Oracle JSON_QUERY: extract a JSON object/array from a document."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    func = core.FunctionCall(dialect, "JSON_QUERY", doc_expr, path_expr)
    if returning is not None:
        func._oracle_returning_type = _coerce_returning_type(dialect, returning)
    return func


def json_exists(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
    path: str,
) -> "bases.BaseExpression":
    """Oracle JSON_EXISTS: check if a path exists in a JSON document."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    return core.FunctionCall(dialect, "JSON_EXISTS", doc_expr, path_expr)


def json_object_expr(
    dialect: "OracleDialect",
    *key_value_pairs: Any,
) -> "bases.BaseExpression":
    """Oracle JSON_OBJECT: create a JSON object from key-value pairs."""
    from rhosocial.activerecord.backend.expression import core
    if not key_value_pairs:
        return core.FunctionCall(dialect, "JSON_OBJECT")
    args = [core.Literal(dialect, val) for val in key_value_pairs]
    return core.FunctionCall(dialect, "JSON_OBJECT", *args)


def json_array_expr(
    dialect: "OracleDialect",
    *values: Any,
) -> "bases.BaseExpression":
    """Oracle JSON_ARRAY: create a JSON array from values."""
    from rhosocial.activerecord.backend.expression import core
    if not values:
        return core.FunctionCall(dialect, "JSON_ARRAY")
    args = [core.Literal(dialect, v) for v in values]
    return core.FunctionCall(dialect, "JSON_ARRAY", *args)


def json_serialize(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Oracle JSON_SERIALIZE: serialize a JSON document to a string."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    return core.FunctionCall(dialect, "JSON_SERIALIZE", doc_expr)


def json_table(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
    path: str,
    columns: str,
) -> "bases.BaseExpression":
    """Oracle JSON_TABLE: query JSON data as a relational table.

    Uses ``FunctionCall`` with ``_oracle_json_table_columns`` metadata
    so the ``format_function_call`` override emits the ``COLUMNS (...)``
    sub-clause after the root path.
    """
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    func = core.FunctionCall(
        dialect, "JSON_TABLE",
        doc_expr, core.Literal(dialect, path),
    )
    func._oracle_json_table_columns = columns
    return func