# src/rhosocial/activerecord/backend/impl/oracle/functions/json.py
"""Oracle JSON function factories."""

from typing import NamedTuple, Optional, Union, Any, TYPE_CHECKING

from .._scan import read_digits, read_word, skip_space

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

class ReturningTypeSpec(NamedTuple):
    """A parsed ``NAME`` or ``NAME(p)`` / ``NAME(p, s)`` RETURNING spelling."""

    name: str
    precision: Optional[str] = None
    scale: Optional[str] = None


def parse_returning_type_spec(text: str) -> Optional[ReturningTypeSpec]:
    """Parse ``NAME[ ( digits [, digits] ) ]`` exactly, or ``None``.

    The shape is the closed grammar Oracle's ``RETURNING`` clause accepts for
    this backend's whitelist: a word, optional whitespace, then either nothing
    or one parenthesised pair of decimal runs. Arguments are returned as the
    digit strings the caller converts, so ``NAME(4000)`` and ``NAME(10, 2)``
    keep the distinction between "scale omitted" and "scale zero".
    """
    digits, index = read_word(text, 0)
    if not digits:
        return None
    index = skip_space(text, index)
    length = len(text)
    if index == length:
        return ReturningTypeSpec(digits)
    if text[index] != "(":
        return None
    index = skip_space(text, index + 1)
    precision, index = read_digits(text, index)
    if not precision:
        return None
    index = skip_space(text, index)
    scale = None
    if index < length and text[index] == ",":
        index = skip_space(text, index + 1)
        scale, index = read_digits(text, index)
        if not scale:
            return None
        index = skip_space(text, index)
    if index < length and text[index] == ")":
        index += 1
        if index == length or (index + 1 == length and text[index] == "\n"):
            return ReturningTypeSpec(digits, precision, scale)
    return None


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
    # inlines is a per-position flag on the one Literal class (core
    # 239678e5 -- the DdlLiteral class became ``inline=True``), so the
    # path is a Literal saying inline=True, and format_literal escapes
    # the value.
    path_expr = core.Literal(dialect, path, inline=True)
    return OracleJsonValueFunction(
        dialect,
        json_doc,
        path_expr,
        returning_type=(
            returning
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
    # inlines is a per-position flag on the one Literal class (core
    # 239678e5 -- the DdlLiteral class became ``inline=True``), so the
    # path is a Literal saying inline=True, and format_literal escapes
    # the value.
    path_expr = core.Literal(dialect, path, inline=True)
    return OracleJsonQueryFunction(
        dialect,
        json_doc,
        path_expr,
        returning_type=(
            returning
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
    # inlines is a per-position flag on the one Literal class (core
    # 239678e5 -- the DdlLiteral class became ``inline=True``), so the
    # path is a Literal saying inline=True, and format_literal escapes
    # the value.
    path_expr = core.Literal(dialect, path, inline=True)
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
