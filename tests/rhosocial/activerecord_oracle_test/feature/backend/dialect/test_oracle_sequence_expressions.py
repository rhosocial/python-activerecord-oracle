# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_sequence_expressions.py
"""Tests for Oracle sequence expressions.

Covers the Oracle-specific ``seq.NEXTVAL`` / ``seq.CURRVAL`` value
expressions and the ``CREATE/DROP SEQUENCE`` DDL formatters, including
capability switches, identifier quoting, the ``IF NOT EXISTS`` / ``IF
EXISTS`` version gate (23ai) and the ``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    CreateSequenceExpression,
    DropSequenceExpression,
    InsertExpression,
    QueryExpression,
    ValuesSource,
)
from rhosocial.activerecord.backend.expression.core import Literal, TableExpression
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCreateSequenceExpression,
    OracleDropSequenceExpression,
    OracleSequenceValueExpression,
    OracleSequenceValueMode,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleSequenceCapabilities:
    def test_supports_create_and_drop(self, dialect):
        assert dialect.supports_create_sequence() is True
        assert dialect.supports_drop_sequence() is True


class TestOracleSequenceValueExpression:
    def test_nextval(self, dialect):
        expr = OracleSequenceValueExpression(dialect, "user_seq")
        sql, params = expr.to_sql()
        assert sql == '"USER_SEQ".NEXTVAL'
        assert params == ()

    def test_currval(self, dialect):
        expr = OracleSequenceValueExpression(
            dialect, "user_seq", OracleSequenceValueMode.CURRVAL
        )
        sql, params = expr.to_sql()
        assert sql == '"USER_SEQ".CURRVAL'
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleSequenceValueExpression(dialect, "My_Seq")
        sql, params = expr.to_sql()
        assert sql == '"MY_SEQ".NEXTVAL'
        assert params == ()

    def test_invalid_mode_rejected(self, dialect):
        with pytest.raises(TypeError, match="mode must be an OracleSequenceValueMode"):
            OracleSequenceValueExpression(dialect, "seq", mode="NEXTVAL")

    def test_empty_sequence_rejected(self, dialect):
        with pytest.raises(ValueError, match="sequence must be a non-empty string"):
            OracleSequenceValueExpression(dialect, "  ")

    def test_nextval_in_select(self, dialect):
        value = OracleSequenceValueExpression(dialect, "user_seq")
        query = QueryExpression(dialect, select=[value], from_=TableExpression(dialect, "dual"))
        sql, params = query.to_sql()
        assert sql == 'SELECT "USER_SEQ".NEXTVAL FROM "DUAL"'
        assert params == ()

    def test_nextval_in_insert(self, dialect):
        value = OracleSequenceValueExpression(dialect, "user_seq")
        source = ValuesSource(dialect, values_list=[[value, Literal(dialect, "x")]])
        expr = InsertExpression(dialect, into="t", source=source, columns=["id", "name"])
        sql, params = expr.to_sql()
        assert sql == 'INSERT INTO "T" ("ID", "NAME") VALUES ("USER_SEQ".NEXTVAL, ?)'
        assert params == ("x",)


class TestOracleCreateSequenceExpression:
    def test_full_options(self, dialect):
        expr = OracleCreateSequenceExpression(
            dialect,
            sequence_name="seq",
            start=1,
            increment=1,
            minvalue=1,
            maxvalue=999999,
            cycle=True,
            cache=20,
        )
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE SEQUENCE "SEQ" START WITH 1 INCREMENT BY 1 '
            "MINVALUE 1 MAXVALUE 999999 CYCLE CACHE 20"
        )
        assert params == ()

    def test_minimal_omits_optional_clauses(self, dialect):
        expr = OracleCreateSequenceExpression(dialect, sequence_name="seq")
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ"'
        assert params == ()

    def test_nocycle_nocache_noorder(self, dialect):
        expr = OracleCreateSequenceExpression(
            dialect,
            sequence_name="seq",
            cycle=False,
            cache=0,
            order=False,
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ" NOCYCLE NOCACHE NOORDER'
        assert params == ()

    def test_order_and_cache(self, dialect):
        expr = OracleCreateSequenceExpression(
            dialect, sequence_name="seq", order=True, cache=100
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ" CACHE 100 ORDER'
        assert params == ()

    def test_core_create_sequence_expression(self, dialect):
        expr = CreateSequenceExpression(dialect, sequence_name="seq")
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ" NOCYCLE NOORDER'
        assert params == ()

    def test_owned_by_unsupported(self, dialect):
        expr = CreateSequenceExpression(dialect, sequence_name="seq", owned_by="t.id")
        with pytest.raises(UnsupportedFeatureError, match="OWNED BY"):
            expr.to_sql()

    def test_if_not_exists_pre_23ai_raises(self, dialect):
        expr = OracleCreateSequenceExpression(dialect, sequence_name="seq", if_not_exists=True)
        with pytest.raises(UnsupportedFeatureError, match="IF NOT EXISTS"):
            expr.to_sql()

    def test_if_not_exists_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = OracleCreateSequenceExpression(d23, sequence_name="seq", if_not_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE IF NOT EXISTS "SEQ"'
        assert params == ()

    def test_negative_cache_rejected(self, dialect):
        with pytest.raises(ValueError, match="cache must be non-negative"):
            OracleCreateSequenceExpression(dialect, sequence_name="seq", cache=-1)


class TestOracleDropSequenceExpression:
    def test_basic_drop(self, dialect):
        expr = OracleDropSequenceExpression(dialect, sequence_name="seq")
        sql, params = expr.to_sql()
        assert sql == 'DROP SEQUENCE "SEQ"'
        assert params == ()

    def test_core_drop_sequence_expression(self, dialect):
        expr = DropSequenceExpression(dialect, sequence_name="seq")
        sql, params = expr.to_sql()
        assert sql == 'DROP SEQUENCE "SEQ"'
        assert params == ()

    def test_if_exists_pre_23ai_raises(self, dialect):
        expr = OracleDropSequenceExpression(dialect, sequence_name="seq", if_exists=True)
        with pytest.raises(UnsupportedFeatureError, match="IF EXISTS"):
            expr.to_sql()

    def test_if_exists_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = OracleDropSequenceExpression(d23, sequence_name="seq", if_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP SEQUENCE IF EXISTS "SEQ"'
        assert params == ()


class TestOracleSequenceVersionBoundary:
    def test_nextval_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleSequenceValueExpression(d8, "seq")
        with pytest.raises(UnsupportedFeatureError, match="NEXTVAL"):
            expr.to_sql()

    def test_currval_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleSequenceValueExpression(d8, "seq", OracleSequenceValueMode.CURRVAL)
        with pytest.raises(UnsupportedFeatureError, match="CURRVAL"):
            expr.to_sql()

    def test_create_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateSequenceExpression(d8, sequence_name="seq")
        with pytest.raises(UnsupportedFeatureError, match="CREATE SEQUENCE"):
            expr.to_sql()

    def test_drop_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropSequenceExpression(d8, sequence_name="seq")
        with pytest.raises(UnsupportedFeatureError, match="DROP SEQUENCE"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        assert OracleSequenceValueExpression(d9, "seq").to_sql()[0] == '"SEQ".NEXTVAL'
        assert OracleCreateSequenceExpression(d9, "seq").to_sql()[0] == 'CREATE SEQUENCE "SEQ"'
