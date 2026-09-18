# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_returning_capabilities.py
"""Oracle RETURNING capability and RETURNING ... INTO formatting tests."""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Column, FunctionCall, WildcardExpression
from rhosocial.activerecord.backend.expression.statements import ReturningClause
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

pytestmark = [pytest.mark.feature, pytest.mark.backend]


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


class TestOracleReturningCapabilities:
    def test_dml_returning_supported(self, dialect):
        assert dialect.supports_returning_insert() is True
        assert dialect.supports_returning_update() is True
        assert dialect.supports_returning_delete() is True

    def test_expressions_unsupported(self, dialect):
        assert dialect.supports_returning_expressions() is False

    def test_wildcard_unsupported(self, dialect):
        assert dialect.supports_returning_wildcard() is False

    def test_single_row(self, dialect):
        assert dialect.supports_returning_single_row() is True

    def test_into_supported(self, dialect):
        assert dialect.supports_returning_into() is True


class TestOracleReturningFormatting:
    def test_column_renders(self, dialect):
        clause = ReturningClause(dialect, expressions=[Column(dialect, "id")])
        sql, _ = dialect.format_returning_clause(clause)
        assert sql == 'RETURNING "ID"'

    def test_into_renders(self, dialect):
        clause = ReturningClause(
            dialect,
            expressions=[Column(dialect, "id"), Column(dialect, "name")],
            output_into=":1, :2",
        )
        sql, _ = dialect.format_returning_clause(clause)
        assert sql == 'RETURNING "ID", "NAME" INTO :1, :2'

    def test_rejects_expression(self, dialect):
        clause = ReturningClause(
            dialect, expressions=[FunctionCall(dialect, "UPPER", Column(dialect, "name"))]
        )
        with pytest.raises(UnsupportedFeatureError, match="expressions in RETURNING"):
            dialect.format_returning_clause(clause)

    def test_rejects_wildcard(self, dialect):
        clause = ReturningClause(dialect, expressions=[WildcardExpression(dialect)])
        with pytest.raises(UnsupportedFeatureError, match="wildcard in RETURNING"):
            dialect.format_returning_clause(clause)
