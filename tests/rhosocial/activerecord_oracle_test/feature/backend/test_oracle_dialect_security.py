# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_dialect_security.py
"""
Tests for Oracle dialect SQL injection security.

Oracle stores unquoted identifiers as uppercase and format_identifier
simply uppercases without adding quotes, so no quoting breakout is possible.
These tests verify this behavior and the absence of injection vectors.
"""
import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    """Create an Oracle test dialect."""
    return OracleDialect(version=(19, 0, 0))


def test_format_identifier_normal(dialect):
    """Normal identifier is uppercased."""
    result = dialect.format_identifier("users")
    assert result == "USERS"


def test_format_identifier_already_upper(dialect):
    """Already-uppercase identifier is unchanged."""
    result = dialect.format_identifier("USERS")
    assert result == "USERS"


def test_format_identifier_mixed_case(dialect):
    """Mixed-case identifier is uppercased."""
    result = dialect.format_identifier("UserOrders")
    assert result == "USERORDERS"


def test_format_identifier_with_double_quote(dialect):
    """Identifier with double-quote char is uppercased (no quoting breakout)."""
    result = dialect.format_identifier('table"name')
    assert result == 'TABLE"NAME'


def test_format_identifier_injection_payload(dialect):
    """Injection payload is uppercased — no breakout possible without quoting."""
    payload = 'users"; DROP TABLE users--'
    result = dialect.format_identifier(payload)
    assert '"' in result
    assert result == 'USERS"; DROP TABLE USERS--'


def test_format_identifier_naive_vs_proper_safe(dialect):
    """For safe input, naive identifier and format_identifier produce same usable result."""
    names = ["users", "orders", "products", "table_1"]
    for name in names:
        naive = name
        proper = dialect.format_identifier(name)
        assert proper == naive.upper(), f"Mismatch for '{name}': naive={naive}, proper={proper}"


def test_format_identifier_empty_string(dialect):
    """Empty identifier returns empty string."""
    assert dialect.format_identifier("") == ""


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

    col_def = ColumnDefinition(name="test_col", data_type=VarCharType(255))
    sql, params = dialect.format_column_definition(col_def)
    assert sql == "TEST_COL VARCHAR2(255)"
    assert params == ()


# ── _quote_identifier escaping ────────────────────────────────────────


def test_quote_identifier_dot_separated(dialect):
    """Dot-separated paths are quoted segment-by-segment."""
    result = dialect._quote_identifier("AR_CRM.CUSTOMERS")
    assert result == '"AR_CRM"."CUSTOMERS"', f"dot path: {result}"


def test_quote_identifier_single_segment(dialect):
    """Single identifier is quoted as a whole."""
    result = dialect._quote_identifier("customers")
    assert result == '"CUSTOMERS"'


def test_quote_identifier_embedded_quote(dialect):
    """Embedded double-quote is escaped by doubling."""
    result = dialect._quote_identifier('table"name')
    assert result == '"TABLE""NAME"'


def test_quote_identifier_empty(dialect):
    """Empty identifier returns empty double-quotes."""
    result = dialect._quote_identifier("")
    assert result == '""'


def test_quote_identifier_three_part(dialect):
    """Three-part catalog.schema.table path is quoted segment-by-segment."""
    result = dialect._quote_identifier("catalog.schema.table")
    assert result == '"CATALOG"."SCHEMA"."TABLE"'


def test_format_identifier_no_quoting(dialect):
    """format_identifier returns uppercase only, no quotes (Oracle convention)."""
    assert dialect.format_identifier("users") == "USERS"
    assert dialect.format_identifier("AR_CRM.CUSTOMERS") == "AR_CRM.CUSTOMERS"
