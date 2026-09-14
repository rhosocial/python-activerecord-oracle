# tests/rhosocial/activerecord_oracle_test/feature/backend/expression/test_expression_signatures.py
"""Tests for Oracle expression class signatures.

Covers the six new expression classes (VectorLiteralExpression,
VectorOperandExpression, TableCompressionClauseExpression,
TablespaceClauseExpression, DisableTriggerExpression,
EnableTriggerExpression) and the ``format_vector_operand`` placeholder
fix (``%s`` → ``?``).

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.vector import (
    VectorLiteralExpression,
    VectorOperandExpression,
)
from rhosocial.activerecord.backend.impl.oracle.expression.table import (
    TableCompressionClauseExpression,
    TablespaceClauseExpression,
)
from rhosocial.activerecord.backend.impl.oracle.expression.trigger import (
    DisableTriggerExpression,
    EnableTriggerExpression,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


# ── VectorLiteralExpression ──────────────────────────────────────────

class TestVectorLiteralExpression:
    def test_list_vec(self, dialect):
        expr = VectorLiteralExpression(dialect, [1.0, 2.0, 3.0])
        sql, params = expr.to_sql()
        assert sql == '[1.0,2.0,3.0]'
        assert params == ()

    def test_string_vec(self, dialect):
        expr = VectorLiteralExpression(dialect, '[1,2,3]')
        sql, params = expr.to_sql()
        assert sql == '[1,2,3]'
        assert params == ()


# ── VectorOperandExpression ──────────────────────────────────────────

class TestVectorOperandExpression:
    def test_string_operand(self, dialect):
        expr = VectorOperandExpression(dialect, '[1,2,3]')
        sql, params = expr.to_sql()
        assert sql == '?'
        assert params == ('[1,2,3]',)

    def test_list_operand(self, dialect):
        expr = VectorOperandExpression(dialect, [1.0, 2.0])
        sql, params = expr.to_sql()
        assert sql == '?'
        assert params == ('[1.0,2.0]',)


# ── TableCompressionClauseExpression ─────────────────────────────────

class TestTableCompressionClauseExpression:
    def test_oltp(self, dialect):
        expr = TableCompressionClauseExpression(dialect, 'OLTP')
        sql, params = expr.to_sql()
        assert sql == 'COMPRESS FOR OLTP'
        assert params == ()

    def test_none_mode(self, dialect):
        expr = TableCompressionClauseExpression(dialect, 'none')
        sql, params = expr.to_sql()
        assert sql == 'NOCOMPRESS'
        assert params == ()


# ── TablespaceClauseExpression ───────────────────────────────────────

class TestTablespaceClauseExpression:
    def test_basic(self, dialect):
        expr = TablespaceClauseExpression(dialect, 'users_data')
        sql, params = expr.to_sql()
        assert sql == 'TABLESPACE "USERS_DATA"'
        assert params == ()

    def test_quoted_identifier(self, dialect):
        expr = TablespaceClauseExpression(dialect, 'My Tablespace')
        sql, params = expr.to_sql()
        assert sql == 'TABLESPACE "MY TABLESPACE"'
        assert params == ()


# ── DisableTriggerExpression ─────────────────────────────────────────

class TestDisableTriggerExpression:
    def test_basic(self, dialect):
        expr = DisableTriggerExpression(dialect, 'trg_audit')
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" DISABLE'
        assert params == ()

    def test_with_table_name(self, dialect):
        expr = DisableTriggerExpression(dialect, 'trg_audit', table_name='employees')
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" DISABLE'
        assert params == ()


# ── EnableTriggerExpression ──────────────────────────────────────────

class TestEnableTriggerExpression:
    def test_basic(self, dialect):
        expr = EnableTriggerExpression(dialect, 'trg_audit')
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" ENABLE'
        assert params == ()

    def test_with_table_name(self, dialect):
        expr = EnableTriggerExpression(dialect, 'trg_audit', table_name='employees')
        sql, params = expr.to_sql()
        assert sql == 'ALTER TRIGGER "TRG_AUDIT" ENABLE'
        assert params == ()


# ── Placeholder fix verification ────────────────────────────────────

class TestVectorOperandPlaceholder:
    def test_uses_question_mark_placeholder(self, dialect):
        """Verify format_vector_operand returns '?' not '%s'."""
        from rhosocial.activerecord.backend.impl.oracle.expression.vector import VectorOperandExpression
        expr = VectorOperandExpression(dialect, 'some_value')
        sql, params = dialect.format_vector_operand(expr)
        assert sql == '?'
        assert params == ('some_value',)

    def test_distance_expression_uses_question_mark(self, dialect):
        """Verify format_vector_distance builds SQL with '?' placeholders."""
        from rhosocial.activerecord.backend.impl.oracle.expression.vector import VectorOperandExpression
        class FakeExpr:
            vector1 = VectorOperandExpression(dialect, 'vec_a')
            vector2 = VectorOperandExpression(dialect, 'vec_b')
            metric = 'COSINE'
        sql, params = dialect.format_vector_distance(FakeExpr())
        assert '?' in sql
        assert '%s' not in sql
