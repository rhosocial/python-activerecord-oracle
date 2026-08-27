# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_flashback_expressions.py
"""Tests for Oracle FLASHBACK family expressions.

Covers the ``AS OF`` / ``VERSIONS BETWEEN`` flashback query clauses, their
attachment to table references via ``format_table(..., flashback=...)``,
the ``FLASHBACK TABLE`` statement (TO BEFORE DROP / TO SCN / TO TIMESTAMP,
RENAME TO, ENABLE/DISABLE TRIGGERS), and the ``PURGE`` statement, plus the
``(10, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleAsOfClause,
    OracleAsOfMode,
    OracleFlashbackTableExpression,
    OraclePurgeExpression,
    OraclePurgeObjectType,
    OracleVersionsBetweenClause,
    OracleVersionsBetweenMode,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleFlashbackCapabilities:
    def test_supports_flashback(self, dialect):
        assert dialect.supports_flashback_query() is True
        assert dialect.supports_flashback_table() is True
        assert dialect.supports_purge() is True


class TestOracleAsOfClause:
    def test_as_of_timestamp_raw_fragment(self, dialect):
        expr = OracleAsOfClause(
            dialect,
            OracleAsOfMode.TIMESTAMP,
            "(SYSTIMESTAMP - INTERVAL '1' DAY)",
        )
        sql, params = expr.to_sql()
        assert sql == "AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '1' DAY)"
        assert params == ()

    def test_as_of_scn(self, dialect):
        expr = OracleAsOfClause(dialect, OracleAsOfMode.SCN, "1234567")
        sql, params = expr.to_sql()
        assert sql == "AS OF SCN 1234567"
        assert params == ()

    def test_as_of_timestamp_expression(self, dialect):
        expr = OracleAsOfClause(
            dialect, OracleAsOfMode.TIMESTAMP, Literal(dialect, "2026-01-01")
        )
        sql, params = expr.to_sql()
        assert sql == "AS OF TIMESTAMP ?"
        assert params == ("2026-01-01",)

    def test_attached_to_table_reference(self, dialect):
        as_of = OracleAsOfClause(
            dialect, OracleAsOfMode.TIMESTAMP, "(SYSTIMESTAMP - INTERVAL '1' DAY)"
        )
        sql, params = dialect.format_table("t", flashback=as_of)
        assert sql == '"T" AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL \'1\' DAY)'
        assert params == ()

    def test_attached_with_alias(self, dialect):
        as_of = OracleAsOfClause(dialect, OracleAsOfMode.SCN, "100")
        sql, params = dialect.format_table("t", alias="x", flashback=as_of)
        assert sql == '"T" AS OF SCN 100 "X"'
        assert params == ()

    def test_schema_qualified_with_flashback(self, dialect):
        as_of = OracleAsOfClause(dialect, OracleAsOfMode.SCN, "100")
        sql, params = dialect.format_table("t", schema_name="scott", flashback=as_of)
        assert sql == '"SCOTT"."T" AS OF SCN 100'
        assert params == ()

    def test_invalid_mode_rejected(self, dialect):
        with pytest.raises(TypeError, match="mode must be an OracleAsOfMode"):
            OracleAsOfClause(dialect, "TIMESTAMP", "x")


class TestOracleVersionsBetweenClause:
    def test_versions_between_timestamp(self, dialect):
        expr = OracleVersionsBetweenClause(
            dialect,
            OracleVersionsBetweenMode.TIMESTAMP,
            "TIMESTAMP '2026-01-01 00:00:00'",
            "SYSTIMESTAMP",
        )
        sql, params = expr.to_sql()
        assert sql == (
            "VERSIONS BETWEEN TIMESTAMP TIMESTAMP '2026-01-01 00:00:00' AND SYSTIMESTAMP"
        )
        assert params == ()

    def test_versions_between_scn(self, dialect):
        expr = OracleVersionsBetweenClause(
            dialect, OracleVersionsBetweenMode.SCN, "100", "200"
        )
        sql, params = expr.to_sql()
        assert sql == "VERSIONS BETWEEN SCN 100 AND 200"
        assert params == ()

    def test_attached_to_table_reference(self, dialect):
        versions = OracleVersionsBetweenClause(
            dialect, OracleVersionsBetweenMode.SCN, "100", "200"
        )
        sql, params = dialect.format_table("t", flashback=versions)
        assert sql == '"T" VERSIONS BETWEEN SCN 100 AND 200'
        assert params == ()

    def test_expression_bounds_bind_params(self, dialect):
        expr = OracleVersionsBetweenClause(
            dialect,
            OracleVersionsBetweenMode.TIMESTAMP,
            Literal(dialect, "a"),
            Literal(dialect, "b"),
        )
        sql, params = expr.to_sql()
        assert sql == "VERSIONS BETWEEN TIMESTAMP ? AND ?"
        assert params == ("a", "b")

    def test_invalid_mode_rejected(self, dialect):
        with pytest.raises(TypeError, match="mode must be an OracleVersionsBetweenMode"):
            OracleVersionsBetweenClause(dialect, "SCN", "1", "2")


class TestOracleFlashbackTableExpression:
    def test_to_before_drop(self, dialect):
        expr = OracleFlashbackTableExpression(dialect, "t", to_before_drop=True)
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "T" TO BEFORE DROP'
        assert params == ()

    def test_to_before_drop_rename_to(self, dialect):
        expr = OracleFlashbackTableExpression(
            dialect, "t", to_before_drop=True, rename_to="t2"
        )
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "T" TO BEFORE DROP RENAME TO "T2"'
        assert params == ()

    def test_to_scn(self, dialect):
        expr = OracleFlashbackTableExpression(dialect, "t", to_scn=1234567)
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "T" TO SCN 1234567'
        assert params == ()

    def test_to_timestamp(self, dialect):
        expr = OracleFlashbackTableExpression(
            dialect, "t", to_timestamp="TIMESTAMP '2026-01-01 00:00:00'"
        )
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "T" TO TIMESTAMP TIMESTAMP \'2026-01-01 00:00:00\''
        assert params == ()

    def test_enable_triggers(self, dialect):
        expr = OracleFlashbackTableExpression(
            dialect, "t", to_before_drop=True, enable_triggers=True
        )
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "T" TO BEFORE DROP ENABLE TRIGGERS'
        assert params == ()

    def test_disable_triggers(self, dialect):
        expr = OracleFlashbackTableExpression(
            dialect, "t", to_timestamp="SYSTIMESTAMP", disable_triggers=True
        )
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "T" TO TIMESTAMP SYSTIMESTAMP DISABLE TRIGGERS'
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleFlashbackTableExpression(dialect, "My_Table", to_before_drop=True)
        sql, params = expr.to_sql()
        assert sql == 'FLASHBACK TABLE "MY_TABLE" TO BEFORE DROP'
        assert params == ()

    def test_no_target_rejected(self, dialect):
        with pytest.raises(ValueError, match="exactly one of to_scn, to_timestamp or to_before_drop"):
            OracleFlashbackTableExpression(dialect, "t")

    def test_multiple_targets_rejected(self, dialect):
        with pytest.raises(ValueError, match="exactly one of to_scn, to_timestamp or to_before_drop"):
            OracleFlashbackTableExpression(dialect, "t", to_scn=1, to_before_drop=True)

    def test_rename_without_before_drop_rejected(self, dialect):
        with pytest.raises(ValueError, match="rename_to requires to_before_drop"):
            OracleFlashbackTableExpression(dialect, "t", to_scn=1, rename_to="t2")

    def test_triggers_mutually_exclusive_rejected(self, dialect):
        with pytest.raises(ValueError, match="mutually exclusive"):
            OracleFlashbackTableExpression(
                dialect, "t", to_before_drop=True,
                enable_triggers=True, disable_triggers=True,
            )

    def test_empty_table_rejected(self, dialect):
        with pytest.raises(ValueError, match="table must be a non-empty string"):
            OracleFlashbackTableExpression(dialect, "  ", to_before_drop=True)


class TestOraclePurgeExpression:
    def test_purge_table(self, dialect):
        expr = OraclePurgeExpression(dialect, OraclePurgeObjectType.TABLE, "t")
        sql, params = expr.to_sql()
        assert sql == 'PURGE TABLE "T"'
        assert params == ()

    def test_purge_index(self, dialect):
        expr = OraclePurgeExpression(dialect, OraclePurgeObjectType.INDEX, "idx_t")
        sql, params = expr.to_sql()
        assert sql == 'PURGE INDEX "IDX_T"'
        assert params == ()

    def test_purge_recyclebin(self, dialect):
        expr = OraclePurgeExpression(dialect, OraclePurgeObjectType.RECYCLEBIN)
        sql, params = expr.to_sql()
        assert sql == "PURGE RECYCLEBIN"
        assert params == ()

    def test_recyclebin_with_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="RECYCLEBIN purge does not take an object name"):
            OraclePurgeExpression(dialect, OraclePurgeObjectType.RECYCLEBIN, "t")

    def test_table_without_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="object_name must be a non-empty string"):
            OraclePurgeExpression(dialect, OraclePurgeObjectType.TABLE)

    def test_invalid_object_type_rejected(self, dialect):
        with pytest.raises(TypeError, match="object_type must be an OraclePurgeObjectType"):
            OraclePurgeExpression(dialect, "TABLE", "t")


class TestOracleFlashbackVersionBoundary:
    def test_as_of_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        expr = OracleAsOfClause(d9, OracleAsOfMode.TIMESTAMP, "SYSTIMESTAMP")
        with pytest.raises(UnsupportedFeatureError, match="AS OF"):
            expr.to_sql()

    def test_versions_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        expr = OracleVersionsBetweenClause(d9, OracleVersionsBetweenMode.SCN, "1", "2")
        with pytest.raises(UnsupportedFeatureError, match="VERSIONS BETWEEN"):
            expr.to_sql()

    def test_flashback_table_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        expr = OracleFlashbackTableExpression(d9, "t", to_before_drop=True)
        with pytest.raises(UnsupportedFeatureError, match="FLASHBACK TABLE"):
            expr.to_sql()

    def test_purge_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        expr = OraclePurgeExpression(d9, OraclePurgeObjectType.TABLE, "t")
        with pytest.raises(UnsupportedFeatureError, match="PURGE"):
            expr.to_sql()

    def test_at_10g_works(self):
        d10 = OracleDialect(version=(10, 0, 0))
        assert OracleAsOfClause(d10, OracleAsOfMode.SCN, "1").to_sql()[0] == "AS OF SCN 1"
        assert OracleFlashbackTableExpression(
            d10, "t", to_before_drop=True
        ).to_sql()[0] == 'FLASHBACK TABLE "T" TO BEFORE DROP'
        assert OraclePurgeExpression(
            d10, OraclePurgeObjectType.RECYCLEBIN
        ).to_sql()[0] == "PURGE RECYCLEBIN"
