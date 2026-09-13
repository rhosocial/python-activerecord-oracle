# tests/rhosocial/activerecord_oracle_test/feature/backend/test_identifier_quoting.py
"""
Tests for OracleDialect format_identifier and reserved-word handling.

Oracle's format_identifier now:
- When need_quote=True (default): adds double quotes, uppercases, and
  escapes internal double-quote characters.
- When need_quote=False: returns the identifier as-is without quoting
  or uppercasing.
"""
import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.warnings import IdentifierQuotingWarning


class TestOracleIdentifierQuoting:
    """Test OracleDialect format_identifier and reserved words."""

    def test_format_identifier_default_double_quote_uppercase(self):
        d = OracleDialect()
        assert d.format_identifier("users") == '"USERS"'

    def test_format_identifier_need_quote_false(self):
        d = OracleDialect()
        assert d.format_identifier("users", need_quote=False) == "users"

    def test_format_identifier_escapes_internal_quotes(self):
        d = OracleDialect()
        assert d.format_identifier('my"table') == '"MY""TABLE"'

    def test_format_identifier_need_quote_false_no_escaping(self):
        d = OracleDialect()
        assert d.format_identifier('my"table', need_quote=False) == 'my"table'

    def test_format_identifier_need_quote_false_no_uppercase(self):
        d = OracleDialect()
        assert d.format_identifier("users", need_quote=False) == "users"
        assert d.format_identifier("Users", need_quote=False) == "Users"

    def test_reserved_words_is_frozenset(self):
        d = OracleDialect()
        assert isinstance(d.reserved_words, frozenset)

    def test_is_reserved_word_case_insensitive(self):
        d = OracleDialect()
        assert d.is_reserved_word("SELECT") is True
        assert d.is_reserved_word("select") is True

    def test_is_reserved_word_non_reserved(self):
        d = OracleDialect()
        assert d.is_reserved_word("users") is False

    def test_reserved_word_warning_emitted(self):
        d = OracleDialect()
        with pytest.warns(IdentifierQuotingWarning, match="select"):
            d.format_identifier("select", need_quote=False)

    def test_balanced_quotes_security(self):
        d = OracleDialect()
        for ident in ["users", 'my"table', 'a""b']:
            result = d.format_identifier(ident)
            assert result.count('"') % 2 == 0, f"Unbalanced quotes: {result}"
