# src/rhosocial/activerecord/backend/impl/oracle/functions/json.py
"""Oracle JSON function factories."""

import re
from typing import Union, Any, TYPE_CHECKING

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
    # Oracle's IEEE words get Oracle's own classes: RETURNING FLOAT(126) --
    # what FloatType/DoubleType render -- is ORA-40449 "invalid data type for
    # return value" on 21c, while RETURNING BINARY_FLOAT / BINARY_DOUBLE is
    # accepted on 21c and 26ai (measured on 21.3 XE and 23.26.1).  The two
    # words are also storage that must not collapse into the NUMBER family
    # (finding F.4-4), so this route now names the same class the catalog
    # word does.
    "BINARY_FLOAT": ("OracleBinaryFloatType", {}),
    "BINARY_DOUBLE": ("OracleBinaryDoubleType", {}),
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
    cls = getattr(_types, cls_name, None)
    if cls is None:
        # Oracle-namespaced classes (BINARY_FLOAT / BINARY_DOUBLE) live in
        # this backend's own type module, not in core's expression.types.
        from ..expression import types as _oracle_types
        cls = getattr(_oracle_types, cls_name)
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
    json_doc: "bases.BaseExpression",
    path: str,
    returning: Union[str, "DataType", None] = None,
) -> "bases.BaseExpression":
    """Oracle JSON_VALUE: extract a scalar from a JSON document.

    Builds :class:`OracleJsonValueFunction`, whose ``returning_type``
    contract field carries the ``RETURNING`` clause to the formatter.
    """
    from rhosocial.activerecord.backend.expression import core
    from ..expression.functions import OracleJsonValueFunction
    # Oracle rejects a bound path: ORA-40454 "path expression not a
    # literal". mixins/json.py already inlines it on the expression path;
    # this is the same requirement on the factory path. Whether a literal
    # inlines is a class-level strategy now, not a per-instance switch, so
    # the inlining class is DdlLiteral -- the only class declaring
    # ``inline_params = True`` -- and format_literal escapes the value.
    path_expr = core.DdlLiteral(dialect, path)
    return OracleJsonValueFunction(
        dialect,
        json_doc,
        path_expr,
        returning_type=(
            _coerce_returning_type(dialect, returning) if returning is not None else None
        ),
    )


def json_query(
    dialect: "OracleDialect",
    json_doc: "bases.BaseExpression",
    path: str,
    returning: Union[str, "DataType", None] = None,
) -> "bases.BaseExpression":
    """Oracle JSON_QUERY: extract a JSON object/array from a document.

    Builds :class:`OracleJsonQueryFunction`, whose ``returning_type``
    contract field carries the ``RETURNING`` clause to the formatter.
    """
    from rhosocial.activerecord.backend.expression import core
    from ..expression.functions import OracleJsonQueryFunction
    # Oracle rejects a bound path: ORA-40454 "path expression not a
    # literal". mixins/json.py already inlines it on the expression path;
    # this is the same requirement on the factory path. Whether a literal
    # inlines is a class-level strategy now, not a per-instance switch, so
    # the inlining class is DdlLiteral -- the only class declaring
    # ``inline_params = True`` -- and format_literal escapes the value.
    path_expr = core.DdlLiteral(dialect, path)
    return OracleJsonQueryFunction(
        dialect,
        json_doc,
        path_expr,
        returning_type=(
            _coerce_returning_type(dialect, returning) if returning is not None else None
        ),
    )


def json_exists(
    dialect: "OracleDialect",
    json_doc: "bases.BaseExpression",
    path: str,
) -> "bases.BaseExpression":
    """Oracle JSON_EXISTS: check if a path exists in a JSON document."""
    from rhosocial.activerecord.backend.expression import core
    # Oracle rejects a bound path: ORA-40454 "path expression not a
    # literal". mixins/json.py already inlines it on the expression path;
    # this is the same requirement on the factory path. Whether a literal
    # inlines is a class-level strategy now, not a per-instance switch, so
    # the inlining class is DdlLiteral -- the only class declaring
    # ``inline_params = True`` -- and format_literal escapes the value.
    path_expr = core.DdlLiteral(dialect, path)
    return core.FunctionCall(dialect, "JSON_EXISTS", json_doc, path_expr)


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
    json_doc: "bases.BaseExpression",
) -> "bases.BaseExpression":
    """Oracle JSON_SERIALIZE: serialize a JSON document to a string."""
    from rhosocial.activerecord.backend.expression import core
    return core.FunctionCall(dialect, "JSON_SERIALIZE", json_doc)


def json_table(
    dialect: "OracleDialect",
    json_doc: "bases.BaseExpression",
    path: str,
    columns: str,
) -> "bases.BaseExpression":
    """Oracle JSON_TABLE: query JSON data as a relational table.

    Builds :class:`OracleJsonTableFunction`, whose ``columns`` contract
    field carries the ``COLUMNS (...)`` sub-clause to the formatter after
    the root path.
    """
    from rhosocial.activerecord.backend.expression import core
    from ..expression.functions import OracleJsonTableFunction

    return OracleJsonTableFunction(
        dialect,
        json_doc,
        core.Literal(dialect, path),
        columns=columns or None,
    )