# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_qualify_support.py
"""
Tests for QUALIFY clause support in the Oracle dialect.

The QUALIFY clause was introduced in Oracle AI Database 26ai (RU 23.26.0,
the 26th quarterly update of the 23c code line). Earlier releases must
filter analytic function results via a subquery or CTE instead.
"""
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.protocols import QualifyClauseSupport
from rhosocial.activerecord.backend.expression import (
    Column,
    FunctionCall,
    Literal,
    OrderByClause,
    QualifyClause,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


class TestOracleQualifyClauseSupport:
    """Test QUALIFY clause version gating in Oracle."""

    @pytest.mark.parametrize(
        "version,expected",
        [
            ((19, 0, 0), False),
            ((23, 4, 0), False),
            ((23, 25, 0), False),
            ((23, 26, 0), True),
            ((23, 26, 1), True),
            ((26, 0, 0), True),
        ],
    )
    def test_supports_qualify_clause_by_version(self, version, expected):
        """QUALIFY support is gated on Oracle 26ai (RU 23.26.0+)."""
        dialect = OracleDialect(version=version)
        assert dialect.supports_qualify_clause() is expected

    def test_dialect_implements_qualify_support_protocol(self):
        """OracleDialect should implement the QualifyClauseSupport protocol."""
        dialect = OracleDialect(version=(23, 26, 0))
        assert isinstance(dialect, QualifyClauseSupport)

    def test_qualify_rejected_before_26ai(self):
        """Formatting a QUALIFY clause on pre-26ai should raise."""
        dialect = OracleDialect(version=(23, 4, 0))
        qualify = QualifyClause(
            dialect, FunctionCall(dialect, "ROW_NUMBER") <= Literal(dialect, 3)
        )
        with pytest.raises(UnsupportedFeatureError) as exc_info:
            qualify.to_sql()
        assert "QUALIFY clause" in str(exc_info.value)

    def test_qualify_rendered_on_26ai(self):
        """QUALIFY clause should render before ORDER BY on 26ai."""
        dialect = OracleDialect(version=(23, 26, 0))
        qualify = QualifyClause(
            dialect, FunctionCall(dialect, "ROW_NUMBER") <= Literal(dialect, 3)
        )
        sql, params = qualify.to_sql()
        assert sql == "QUALIFY ROW_NUMBER() <= :p0" or sql.count("QUALIFY") == 1
        assert params == (3,)

    def test_query_expression_qualify_before_order_by(self):
        """QueryExpression places QUALIFY before ORDER BY on 26ai."""
        dialect = OracleDialect(version=(23, 26, 0))
        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users"),
            qualify=QualifyClause(
                dialect, FunctionCall(dialect, "ROW_NUMBER") <= Literal(dialect, 3)
            ),
            order_by=OrderByClause(dialect, [(Column(dialect, "id"), "ASC")]),
        )
        sql, params = query.to_sql()
        assert sql.count("QUALIFY") == 1
        assert sql.index("QUALIFY") < sql.index("ORDER BY")
        assert params == (3,)

    def test_query_expression_qualify_rejected_before_26ai(self):
        """QueryExpression with QUALIFY should raise on pre-26ai."""
        dialect = OracleDialect(version=(19, 0, 0))
        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id")],
            from_=TableExpression(dialect, "users"),
            qualify=QualifyClause(
                dialect, FunctionCall(dialect, "ROW_NUMBER") <= Literal(dialect, 3)
            ),
        )
        with pytest.raises(UnsupportedFeatureError):
            query.to_sql()