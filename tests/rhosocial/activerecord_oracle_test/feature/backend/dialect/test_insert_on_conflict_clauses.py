# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_insert_on_conflict_clauses.py
"""Tests for Oracle ON CONFLICT clause capability.

Oracle expresses upsert via MERGE, not the ON CONFLICT clause form.
Covers:
- Capability switches: both on_conflict switches False.
- Any ON CONFLICT clause rejected by the generic gate with
  UnsupportedFeatureError.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    InsertExpression,
    Literal,
    OnConflictClause,
    ValuesSource,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


class TestOracleOnConflictCapabilities:
    """Capability switch tests."""

    def test_supports_upsert_via_merge(self, dialect):
        assert dialect.supports_upsert() is True
        assert dialect.get_upsert_syntax_type() == "MERGE"

    def test_does_not_support_on_conflict_clause(self, dialect):
        assert dialect.supports_on_conflict_clause() is False
        assert dialect.supports_multiple_on_conflict_clauses() is False

    def test_on_conflict_clause_rejected(self, dialect):
        """Oracle has no ON CONFLICT clause form; the gate raises."""
        source = ValuesSource(dialect, values_list=[[Literal(dialect, 1)]])
        clause = OnConflictClause(dialect, conflict_target=["id"], do_nothing=True)
        expr = InsertExpression(dialect, into="users", source=source, on_conflict=clause)

        with pytest.raises(UnsupportedFeatureError, match="does not support ON CONFLICT"):
            expr.to_sql()

    def test_multiple_on_conflict_clauses_rejected(self, dialect):
        """Multiple clauses are rejected as well (via the on_conflict gate)."""
        source = ValuesSource(dialect, values_list=[[Literal(dialect, 1)]])
        clause1 = OnConflictClause(dialect, conflict_target=["a"], do_nothing=True)
        clause2 = OnConflictClause(dialect, conflict_target=["b"], do_nothing=True)
        expr = InsertExpression(dialect, into="t", source=source, on_conflict=[clause1, clause2])

        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_plain_insert_still_works(self, dialect):
        """An INSERT without on_conflict renders normally."""
        source = ValuesSource(dialect, values_list=[[Literal(dialect, 1)]])
        expr = InsertExpression(dialect, into="users", columns=["id"], source=source)
        sql, params = expr.to_sql()
        assert "INSERT INTO" in sql
        assert params == (1,)
