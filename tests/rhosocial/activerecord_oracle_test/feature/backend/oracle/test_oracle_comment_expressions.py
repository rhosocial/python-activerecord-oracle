# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_comment_expressions.py
"""Tests for Oracle COMMENT ON expressions.

Covers the ``COMMENT ON {TABLE|COLUMN|VIEW|INDEX|SEQUENCE|PROCEDURE|...}
obj IS 'text'`` statement, the ``IS NULL`` clear-comment form, identifier
uppercasing, string escaping, and the ``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCommentExpression,
    OracleCommentObjectType,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleCommentCapabilities:
    def test_supports_comment(self, dialect):
        assert dialect.supports_comment() is True


class TestOracleCommentExpression:
    def test_comment_on_table(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.TABLE, "t", "用户表"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE T IS '用户表'"
        assert params == ()

    def test_comment_on_column(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.COLUMN, "t.c", "列注释"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON COLUMN T.C IS '列注释'"
        assert params == ()

    def test_comment_on_view(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.VIEW, "v", "view")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON VIEW V IS 'view'"
        assert params == ()

    def test_comment_on_index(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.INDEX, "idx_t", "i")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON INDEX IDX_T IS 'i'"
        assert params == ()

    def test_comment_on_sequence(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.SEQUENCE, "seq", "s")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON SEQUENCE SEQ IS 's'"
        assert params == ()

    def test_comment_on_procedure(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.PROCEDURE, "p", "proc")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON PROCEDURE P IS 'proc'"
        assert params == ()

    def test_comment_on_package(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.PACKAGE, "pk", "pkg")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON PACKAGE PK IS 'pkg'"
        assert params == ()

    def test_comment_on_materialized_view(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.MATERIALIZED_VIEW, "mv", "mv"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON MATERIALIZED VIEW MV IS 'mv'"
        assert params == ()

    def test_clear_comment_with_null(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.TABLE, "t")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE T IS NULL"
        assert params == ()

    def test_apostrophe_escaped(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.TABLE, "t", "it's")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE T IS 'it''s'"
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleCommentExpression(dialect, OracleCommentObjectType.TABLE, "My_Table", "x")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE MY_TABLE IS 'x'"
        assert params == ()

    def test_schema_qualified_column(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.COLUMN, "scott.emp.sal", "工资"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON COLUMN SCOTT.EMP.SAL IS '工资'"
        assert params == ()

    def test_invalid_object_type_rejected(self, dialect):
        with pytest.raises(TypeError, match="object_type must be an OracleCommentObjectType"):
            OracleCommentExpression(dialect, "TABLE", "t", "x")

    def test_empty_object_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="object_name must be a non-empty string"):
            OracleCommentExpression(dialect, OracleCommentObjectType.TABLE, "  ")


class TestOracleCommentVersionBoundary:
    def test_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCommentExpression(d8, OracleCommentObjectType.TABLE, "t", "x")
        with pytest.raises(UnsupportedFeatureError, match="COMMENT ON"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        expr = OracleCommentExpression(d9, OracleCommentObjectType.TABLE, "t", "x")
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE T IS 'x'"
        assert params == ()
