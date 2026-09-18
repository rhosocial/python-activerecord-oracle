# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_alter_table_clauses.py
"""Tests for Oracle ALTER TABLE table-level clause actions.

Covers ``SET UNUSED``, ``DROP UNUSED COLUMNS``, ``MOVE``, ``SHRINK SPACE
[CASCADE]``, ``READ ONLY | READ WRITE`` and ``ENABLE | DISABLE ROW
MOVEMENT`` when composed into the core ``AlterTableExpression`` action
dispatch, plus the per-clause version boundaries.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.statements import AlterTableExpression
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleDropUnusedColumnsAction,
    OracleMoveTableAction,
    OracleReadOnlyAction,
    OracleRowMovementAction,
    OracleSetUnusedColumnsAction,
    OracleShrinkSpaceAction,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleAlterTableCapabilities:
    def test_supports_table_level_clauses(self, dialect):
        assert dialect.supports_set_unused() is True
        assert dialect.supports_drop_unused_columns() is True
        assert dialect.supports_move_table() is True
        assert dialect.supports_shrink_space() is True
        assert dialect.supports_read_only() is True
        assert dialect.supports_row_movement() is True


class TestOracleSetUnusedColumnsAction:
    def test_set_unused(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleSetUnusedColumnsAction(dialect, ["c1", "c2"])]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" SET UNUSED ("C1", "C2")'
        assert params == ()

    def test_set_unused_single_column(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleSetUnusedColumnsAction(dialect, ["c1"])]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" SET UNUSED ("C1")'
        assert params == ()

    def test_empty_columns_rejected(self, dialect):
        with pytest.raises(ValueError, match="columns must be a non-empty list"):
            OracleSetUnusedColumnsAction(dialect, [])


class TestOracleDropUnusedColumnsAction:
    def test_drop_unused_columns(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleDropUnusedColumnsAction(dialect)]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" DROP UNUSED COLUMNS'
        assert params == ()


class TestOracleMoveTableAction:
    def test_move(self, dialect):
        alter = AlterTableExpression(dialect, "t", actions=[OracleMoveTableAction(dialect)])
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" MOVE'
        assert params == ()


class TestOracleShrinkSpaceAction:
    def test_shrink_space(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleShrinkSpaceAction(dialect)]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" SHRINK SPACE'
        assert params == ()

    def test_shrink_space_cascade(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleShrinkSpaceAction(dialect, cascade=True)]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" SHRINK SPACE CASCADE'
        assert params == ()


class TestOracleReadOnlyAction:
    def test_read_only(self, dialect):
        alter = AlterTableExpression(dialect, "t", actions=[OracleReadOnlyAction(dialect)])
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" READ ONLY'
        assert params == ()

    def test_read_write(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleReadOnlyAction(dialect, read_only=False)]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" READ WRITE'
        assert params == ()


class TestOracleRowMovementAction:
    def test_enable_row_movement(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleRowMovementAction(dialect, enable=True)]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" ENABLE ROW MOVEMENT'
        assert params == ()

    def test_disable_row_movement(self, dialect):
        alter = AlterTableExpression(
            dialect, "t", actions=[OracleRowMovementAction(dialect, enable=False)]
        )
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" DISABLE ROW MOVEMENT'
        assert params == ()


class TestOracleAlterTableVersionBoundary:
    def test_set_unused_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        alter = AlterTableExpression(
            d8, "t", actions=[OracleSetUnusedColumnsAction(d8, ["c1"])]
        )
        with pytest.raises(UnsupportedFeatureError, match="SET UNUSED"):
            alter.to_sql()

    def test_drop_unused_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        alter = AlterTableExpression(
            d8, "t", actions=[OracleDropUnusedColumnsAction(d8)]
        )
        with pytest.raises(UnsupportedFeatureError, match="DROP UNUSED COLUMNS"):
            alter.to_sql()

    def test_move_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        alter = AlterTableExpression(d8, "t", actions=[OracleMoveTableAction(d8)])
        with pytest.raises(UnsupportedFeatureError, match="MOVE"):
            alter.to_sql()

    def test_shrink_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        alter = AlterTableExpression(d9, "t", actions=[OracleShrinkSpaceAction(d9)])
        with pytest.raises(UnsupportedFeatureError, match="SHRINK SPACE"):
            alter.to_sql()

    def test_read_only_below_11g_raises(self):
        d10 = OracleDialect(version=(10, 0, 0))
        alter = AlterTableExpression(d10, "t", actions=[OracleReadOnlyAction(d10)])
        with pytest.raises(UnsupportedFeatureError, match="READ ONLY"):
            alter.to_sql()

    def test_read_only_at_11g_works(self):
        d11 = OracleDialect(version=(11, 0, 0))
        alter = AlterTableExpression(d11, "t", actions=[OracleReadOnlyAction(d11)])
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "T" READ ONLY'
        assert params == ()

    def test_row_movement_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        alter = AlterTableExpression(
            d8, "t", actions=[OracleRowMovementAction(d8, enable=True)]
        )
        with pytest.raises(UnsupportedFeatureError, match="ROW MOVEMENT"):
            alter.to_sql()

    def test_at_10g_all_work(self):
        d10 = OracleDialect(version=(10, 0, 0))
        cases = [
            ([OracleSetUnusedColumnsAction(d10, ["c1"])], 'ALTER TABLE "T" SET UNUSED ("C1")'),
            ([OracleDropUnusedColumnsAction(d10)], 'ALTER TABLE "T" DROP UNUSED COLUMNS'),
            ([OracleMoveTableAction(d10)], 'ALTER TABLE "T" MOVE'),
            ([OracleShrinkSpaceAction(d10, cascade=True)], 'ALTER TABLE "T" SHRINK SPACE CASCADE'),
            ([OracleRowMovementAction(d10, enable=False)], 'ALTER TABLE "T" DISABLE ROW MOVEMENT'),
        ]
        for actions, expected in cases:
            alter = AlterTableExpression(d10, "t", actions=actions)
            assert alter.to_sql()[0] == expected
