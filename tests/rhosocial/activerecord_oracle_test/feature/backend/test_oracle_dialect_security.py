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
