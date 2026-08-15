# src/rhosocial/activerecord/backend/impl/oracle/functions/json.py
"""Oracle JSON function factories."""

from typing import Union, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import bases
    from ..dialect import OracleDialect


def json_value(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
    path: str,
    returning: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle JSON_VALUE: extract a scalar from a JSON document."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    if returning:
        func = core.FunctionCall(
            dialect, "JSON_VALUE", doc_expr, path_expr,
            core.RawSQLExpression(dialect, f"RETURNING {returning}"),
        )
        return func
    return core.FunctionCall(dialect, "JSON_VALUE", doc_expr, path_expr)


def json_query(
    dialect: "OracleDialect",
    json_doc: Union[str, "bases.BaseExpression"],
    path: str,
    returning: Optional[str] = None,
) -> "bases.BaseExpression":
    """Oracle JSON_QUERY: extract a JSON object/array from a document."""
    from rhosocial.activerecord.backend.expression import core
    from ._convert import _convert_to_expression
    doc_expr = _convert_to_expression(dialect, json_doc)
    path_expr = core.Literal(dialect, path)
    if returning:
        return core.FunctionCall(
            dialect, "JSON_QUERY", doc_expr, path_expr,
            core.RawSQLExpression(dialect, f"RETURNING {returning}"),
        )
    return core.FunctionCall(dialect, "JSON_QUERY", doc_expr, path_expr)


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