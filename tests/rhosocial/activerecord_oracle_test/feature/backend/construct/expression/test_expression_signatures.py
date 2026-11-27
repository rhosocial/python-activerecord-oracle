# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/expression/test_expression_signatures.py
"""Tests for Oracle expression class signatures.

Covers the six new expression classes (OracleVectorLiteralExpression,
OracleVectorOperandExpression, OracleTableCompressionClauseExpression,
OracleTablespaceClauseExpression, OracleDisableTriggerStatement,
OracleEnableTriggerStatement) and the ``format_vector_operand_expression`` placeholder
fix (``%s`` → ``?``).

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.expression.objects import Table, Trigger
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.vector import (
    OracleVectorLiteralExpression,
    OracleVectorOperandExpression,
)
from rhosocial.activerecord.backend.impl.oracle.expression.table import (
    OracleTableCompressionClauseExpression,
    OracleTablespaceClauseExpression,
)
from rhosocial.activerecord.backend.impl.oracle.expression.trigger import (
    OracleDisableTriggerStatement,
    OracleEnableTriggerStatement,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


# ── OracleVectorLiteralExpression ──────────────────────────────────────────

class TestVectorLiteralExpression:
    def test_list_vec(self, dialect):
        expr = OracleVectorLiteralExpression(dialect, [1.0, 2.0, 3.0])
        sql, params = expr.to_sql()
        assert sql == '[1.0,2.0,3.0]'
        assert params == ()

    def test_string_vec(self, dialect):
        expr = OracleVectorLiteralExpression(dialect, '[1,2,3]')
        sql, params = expr.to_sql()
        assert sql == '[1,2,3]'
        assert params == ()


# ── OracleVectorOperandExpression ──────────────────────────────────────────

class TestVectorOperandExpression:
    def test_string_operand(self, dialect):
        expr = OracleVectorOperandExpression(dialect, '[1,2,3]')
        sql, params = expr.to_sql()
        assert sql == '?'
        assert params == ('[1,2,3]',)

    def test_list_operand(self, dialect):
        expr = OracleVectorOperandExpression(dialect, [1.0, 2.0])
        sql, params = expr.to_sql()
        assert sql == '?'
        assert params == ('[1.0,2.0]',)


# ── OracleTableCompressionClauseExpression ─────────────────────────────────

class TestTableCompressionClauseExpression:
    def test_oltp(self, dialect):
        expr = OracleTableCompressionClauseExpression(dialect, 'OLTP')
        sql, params = expr.to_sql()
        assert sql == 'COMPRESS FOR OLTP'
        assert params == ()

    def test_none_mode(self, dialect):
        expr = OracleTableCompressionClauseExpression(dialect, 'none')
        sql, params = expr.to_sql()
        assert sql == 'NOCOMPRESS'
        assert params == ()


# ── OracleTablespaceClauseExpression ───────────────────────────────────────

class TestTablespaceClauseExpression:
    def test_basic(self, dialect):
        expr = OracleTablespaceClauseExpression(dialect, 'users_data')
        sql, params = expr.to_sql()
        assert sql == 'TABLESPACE "USERS_DATA"'
        assert params == ()

    def test_quoted_identifier(self, dialect):
        expr = OracleTablespaceClauseExpression(dialect, 'My Tablespace')
        sql, params = expr.to_sql()
        assert sql == 'TABLESPACE "MY TABLESPACE"'
        assert params == ()


# ── OracleDisableTriggerStatement ─────────────────────────────────────────

class TestDisableTriggerExpression:
    def test_basic(self, dialect):
        expr = OracleDisableTriggerStatement(dialect, Trigger(dialect, 'trg_audit'))
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" DISABLE'
        assert params == ()

    def test_with_table_name(self, dialect):
        expr = OracleDisableTriggerStatement(dialect, Trigger(dialect, 'trg_audit'), table=Table(dialect, 'employees'))
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" DISABLE'
        assert params == ()


# ── OracleEnableTriggerStatement ──────────────────────────────────────────

class TestEnableTriggerExpression:
    def test_basic(self, dialect):
        expr = OracleEnableTriggerStatement(dialect, Trigger(dialect, 'trg_audit'))
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" ENABLE'
        assert params == ()

    def test_with_table_name(self, dialect):
        expr = OracleEnableTriggerStatement(dialect, Trigger(dialect, 'trg_audit'), table=Table(dialect, 'employees'))
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" ENABLE'
        assert params == ()


# ── Placeholder fix verification ────────────────────────────────────

class TestVectorOperandPlaceholder:
    def test_uses_question_mark_placeholder(self, dialect):
        """Verify format_vector_operand_expression returns '?' not '%s'."""
        from rhosocial.activerecord.backend.impl.oracle.expression.vector import OracleVectorOperandExpression
        expr = OracleVectorOperandExpression(dialect, 'some_value')
        sql, params = expr.to_sql()
        assert sql == '?'
        assert params == ('some_value',)

    def test_distance_expression_uses_question_mark(self, dialect):
        """Verify build_vector_distance builds SQL with '?' placeholders."""
        from rhosocial.activerecord.backend.impl.oracle.expression.vector import OracleVectorOperandExpression
        class FakeExpr:
            vector1 = OracleVectorOperandExpression(dialect, 'vec_a')
            vector2 = OracleVectorOperandExpression(dialect, 'vec_b')
            metric = 'COSINE'
        sql, params = dialect.build_vector_distance(FakeExpr())
        assert '?' in sql
        assert '%s' not in sql

    def test_unsupported_distance_metric_is_refused(self, dialect):
        """A metric without a per-metric switch is refused by name."""
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        class FakeExpr:
            vector1 = OracleVectorOperandExpression(dialect, 'vec_a')
            vector2 = OracleVectorOperandExpression(dialect, 'vec_b')
            metric = 'JACCARD'
        with pytest.raises(UnsupportedFeatureError):
            dialect.build_vector_distance(FakeExpr())
