# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_dialect_security.py
"""
Tests for Oracle dialect SQL injection security.

Oracle upper-cases and double-quotes identifiers, escaping embedded quotes,
so no quoting breakout is possible. These tests verify this behavior and the
absence of injection vectors.
"""
import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.mixins.backend_mixin import (
    OracleBackendMixin,
)


@pytest.fixture
def dialect():
    """Create an Oracle test dialect."""
    return OracleDialect(version=(19, 0, 0))


def test_format_identifier_normal(dialect):
    """Normal identifier is uppercased and quoted."""
    result = dialect.format_identifier("users")
    assert result == '"USERS"'


def test_format_identifier_already_upper(dialect):
    """Already-uppercase identifier is quoted unchanged."""
    result = dialect.format_identifier("USERS")
    assert result == '"USERS"'


def test_format_identifier_mixed_case(dialect):
    """Mixed-case identifier is uppercased and quoted."""
    result = dialect.format_identifier("UserOrders")
    assert result == '"USERORDERS"'


def test_format_identifier_with_double_quote(dialect):
    """Identifier with double-quote char is escaped (no quoting breakout)."""
    result = dialect.format_identifier('table"name')
    assert result == '"TABLE""NAME"'


def test_format_identifier_injection_payload(dialect):
    """Injection payload is escaped and quoted — no breakout possible."""
    payload = 'users"; DROP TABLE users--'
    result = dialect.format_identifier(payload)
    assert '"' in result
    assert result == '"USERS""; DROP TABLE USERS--"'


def test_format_identifier_naive_vs_proper_safe(dialect):
    """For safe input, format_identifier is the quoted uppercase form."""
    names = ["users", "orders", "products", "table_1"]
    for name in names:
        proper = dialect.format_identifier(name)
        assert proper == f'"{name.upper()}"', f"Mismatch for '{name}': proper={proper}"


def test_format_identifier_empty_string(dialect):
    """Empty identifier formats to empty quotes."""
    assert dialect.format_identifier("") == '""'


def test_escape_sql_string_inherited(dialect):
    """Test Oracle inherits _escape_sql_string from base dialect."""
    result = dialect._escape_sql_string("test's value")
    assert result == "test''s value"


def test_malicious_data_type_rejected_at_construction(dialect):
    """Malicious data_type string is rejected at ColumnDefinition construction.

    Core #108: ``ColumnDefinition.data_type`` must be a ``DataType`` instance.
    A raw string payload (e.g. SQL injection attempt) raises ``TypeError``
    at construction time, before any dialect formatting is invoked.
    """
    from rhosocial.activerecord.backend.expression.statements import ColumnDefinition

    with pytest.raises(TypeError, match="data_type must be a DataType instance"):
        ColumnDefinition(
            dialect,
            name="test_col",
            data_type="VARCHAR2(255); DROP TABLE users--",
        )


def test_data_type_instance_rendered_via_dialect(dialect):
    """A DataType instance is rendered through format_data_type dispatch.

    Verifies the #108 chain: ``data_type.to_sql(dialect)`` delegates to
    ``dialect.format_data_type`` registered in ``OracleTypeSupportMixin``.
    """
    from rhosocial.activerecord.backend.expression.statements import ColumnDefinition
    from rhosocial.activerecord.backend.expression.types import VarCharType

    col_def = ColumnDefinition(dialect, name="test_col", data_type=VarCharType(length=255, dialect=dialect))
    sql, params = dialect.format_column_definition(col_def)
    assert sql == '"TEST_COL" VARCHAR2(255)'
    assert params == ()


# ── _quote_identifier escaping (backend-level utility) ────────────────


def test_quote_identifier_dot_separated():
    """Dot-separated paths are quoted segment-by-segment."""
    result = OracleBackendMixin._quote_identifier("AR_CRM.CUSTOMERS")
    assert result == '"AR_CRM"."CUSTOMERS"', f"dot path: {result}"


def test_quote_identifier_single_segment():
    """Single identifier is quoted as a whole."""
    result = OracleBackendMixin._quote_identifier("customers")
    assert result == '"CUSTOMERS"'


def test_quote_identifier_embedded_quote():
    """Embedded double-quote is escaped by doubling."""
    result = OracleBackendMixin._quote_identifier('table"name')
    assert result == '"TABLE""NAME"'


def test_quote_identifier_empty():
    """Empty identifier returns empty double-quotes."""
    result = OracleBackendMixin._quote_identifier("")
    assert result == '""'


def test_quote_identifier_three_part():
    """Three-part catalog.schema.table path is quoted segment-by-segment."""
    result = OracleBackendMixin._quote_identifier("catalog.schema.table")
    assert result == '"CATALOG"."SCHEMA"."TABLE"'


def test_format_identifier_quotes_whole_qualified_path(dialect):
    """format_identifier does not split dotted paths.

    Qualified references are rendered with separate ``schema_name``/``name``
    components by the expression layer, so a raw dotted string is quoted as a
    single (uppercased) identifier.
    """
    assert dialect.format_identifier("users") == '"USERS"'
    assert dialect.format_identifier("AR_CRM.CUSTOMERS") == '"AR_CRM.CUSTOMERS"'
