# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_comment_expressions.py
"""Tests for Oracle COMMENT ON expressions.

Covers the ``COMMENT ON {TABLE|COLUMN|VIEW|INDEX|SEQUENCE|PROCEDURE|...}
obj IS 'text'`` statement, the ``IS NULL`` clear-comment form, identifier
uppercasing, string escaping, and the ``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import (
    Index,
    MaterializedView,
    Procedure,
    Sequence,
    Table,
    View,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCommentExpression,
    OracleCommentObjectType,
)
from rhosocial.activerecord.backend.impl.oracle.expression.objects import OraclePackage


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleCommentCapabilities:
    def test_supports_comment_on(self, dialect):
        assert dialect.supports_comment_on() is True


class TestOracleCommentExpression:
    def test_comment_on_table(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.TABLE, Table(dialect, "t"), "用户表"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE \"T\" IS '用户表'"
        assert params == ()

    def test_comment_on_column(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.COLUMN, Table(dialect, "t"), "列注释",
            column="c",
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON COLUMN \"T\".\"C\" IS '列注释'"
        assert params == ()

    def test_comment_on_view(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.VIEW, View(dialect, "v"), "view"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON VIEW \"V\" IS 'view'"
        assert params == ()

    def test_comment_on_index(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.INDEX, Index(dialect, "idx_t"), "i"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON INDEX \"IDX_T\" IS 'i'"
        assert params == ()

    def test_comment_on_sequence(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.SEQUENCE, Sequence(dialect, "seq"), "s"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON SEQUENCE \"SEQ\" IS 's'"
        assert params == ()

    def test_comment_on_procedure(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.PROCEDURE, Procedure(dialect, "p"), "proc"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON PROCEDURE \"P\" IS 'proc'"
        assert params == ()

    def test_comment_on_package(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.PACKAGE, OraclePackage(dialect, "pk"), "pkg"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON PACKAGE \"PK\" IS 'pkg'"
        assert params == ()

    def test_comment_on_materialized_view(self, dialect):
        expr = OracleCommentExpression(
            dialect,
            OracleCommentObjectType.MATERIALIZED_VIEW,
            MaterializedView(dialect, "mv"),
            "mv",
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON MATERIALIZED VIEW \"MV\" IS 'mv'"
        assert params == ()

    def test_clear_comment_with_null(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.TABLE, Table(dialect, "t")
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE \"T\" IS NULL"
        assert params == ()

    def test_apostrophe_escaped(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.TABLE, Table(dialect, "t"), "it's"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE \"T\" IS 'it''s'"
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleCommentExpression(
            dialect, OracleCommentObjectType.TABLE, Table(dialect, "My_Table"), "x"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE \"MY_TABLE\" IS 'x'"
        assert params == ()

    def test_schema_qualified_column(self, dialect):
        """The owner rides on the table object; the column is named beside it."""
        expr = OracleCommentExpression(
            dialect,
            OracleCommentObjectType.COLUMN,
            Table(dialect, "emp", schema_name="scott"),
            "工资",
            column="sal",
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON COLUMN \"SCOTT\".\"EMP\".\"SAL\" IS '工资'"
        assert params == ()

    def test_invalid_object_type_rejected(self, dialect):
        with pytest.raises(TypeError, match="object_type must be an OracleCommentObjectType"):
            OracleCommentExpression(dialect, "TABLE", Table(dialect, "t"), "x")

    def test_object_kind_must_match_the_keyword(self, dialect):
        """A TABLE comment cannot hold a view: the statement would name the wrong object."""
        with pytest.raises(TypeError, match="TABLE names a Table"):
            OracleCommentExpression(
                dialect, OracleCommentObjectType.TABLE, View(dialect, "v"), "x"
            )

    def test_column_requires_a_column(self, dialect):
        with pytest.raises(ValueError, match="COLUMN comment requires a column name"):
            OracleCommentExpression(
                dialect, OracleCommentObjectType.COLUMN, Table(dialect, "t"), "x"
            )

    def test_non_column_rejects_a_column(self, dialect):
        with pytest.raises(ValueError, match="does not take a column name"):
            OracleCommentExpression(
                dialect,
                OracleCommentObjectType.TABLE,
                Table(dialect, "t"),
                "x",
                column="c",
            )


class TestOracleCommentVersionBoundary:
    def test_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCommentExpression(
            d8, OracleCommentObjectType.TABLE, Table(d8, "t"), "x"
        )
        with pytest.raises(UnsupportedFeatureError, match="COMMENT ON"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        expr = OracleCommentExpression(
            d9, OracleCommentObjectType.TABLE, Table(d9, "t"), "x"
        )
        sql, params = expr.to_sql()
        assert sql == "COMMENT ON TABLE \"T\" IS 'x'"
        assert params == ()
