# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase3_expressions.py
"""
Tests for Phase 3 expression system.

Tests all Oracle-specific expressions including hierarchical queries,
PIVOT/UNPIVOT, query hints, and enhanced locking.
"""

import pytest


class TestHierarchicalExpressions:
    """Test hierarchical query expressions."""

    def test_import_hierarchical(self):
        """Test importing hierarchical expressions."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            ConnectByExpression, PriorExpression,
            ConnectByRootExpression, SysConnectByPathExpression,
            ConnectByIsLeafExpression, ConnectByIsCycleExpression,
            LevelExpression, SiblingsExpression
        )

    def test_connect_by_root(self):
        """Test CONNECT_BY_ROOT expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import ConnectByRootExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = ConnectByRootExpression(column="employee_id")
        sql, params = expr.to_sql(dialect)
        assert "CONNECT_BY_ROOT" in sql
        assert "EMPLOYEE_ID" in sql  # Oracle uppercase
        assert params == []

    def test_sys_connect_by_path(self):
        """Test SYS_CONNECT_BY_PATH expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import SysConnectByPathExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = SysConnectByPathExpression(column="name", separator="/")
        sql, params = expr.to_sql(dialect)
        assert "SYS_CONNECT_BY_PATH" in sql
        assert "'/'" in sql
        assert params == []

    def test_level_expression(self):
        """Test LEVEL pseudo-column."""
        from rhosocial.activerecord.backend.impl.oracle.expression import LevelExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = LevelExpression()
        sql, params = expr.to_sql(dialect)
        assert sql == "LEVEL"
        assert params == []

    def test_connect_by_isleaf(self):
        """Test CONNECT_BY_ISLEAF pseudo-column."""
        from rhosocial.activerecord.backend.impl.oracle.expression import ConnectByIsLeafExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = ConnectByIsLeafExpression()
        sql, params = expr.to_sql(dialect)
        assert sql == "CONNECT_BY_ISLEAF"
        assert params == []

    def test_connect_by_iscycle(self):
        """Test CONNECT_BY_ISCYCLE pseudo-column."""
        from rhosocial.activerecord.backend.impl.oracle.expression import ConnectByIsCycleExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = ConnectByIsCycleExpression()
        sql, params = expr.to_sql(dialect)
        assert sql == "CONNECT_BY_ISCYCLE"
        assert params == []

    def test_prior_expression(self):
        """Test PRIOR expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import PriorExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = PriorExpression(column="employee_id")
        sql, params = expr.to_sql(dialect)
        assert "PRIOR" in sql
        assert "EMPLOYEE_ID" in sql
        assert params == []

    def test_siblings_expression(self):
        """Test ORDER SIBLINGS BY expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import SiblingsExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        expr = SiblingsExpression(columns=["name", "id"], ascending=[True, False])
        sql, params = expr.to_sql(dialect)
        assert "ORDER SIBLINGS BY" in sql
        assert "NAME" in sql
        assert "ID" in sql
        assert "ASC" in sql
        assert "DESC" in sql
        assert params == []


class TestPivotExpressions:
    """Test PIVOT and UNPIVOT expressions."""

    def test_import_pivot(self):
        """Test importing PIVOT expressions."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            PivotExpression, UnpivotExpression
        )

    def test_pivot_basic(self):
        """Test basic PIVOT expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import PivotExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        pivot = PivotExpression(
            aggregate_function="SUM",
            value_column="amount",
            pivot_column="month",
            values=["Jan", "Feb", "Mar"]
        )
        sql, params = pivot.to_sql(dialect)
        assert "PIVOT" in sql
        assert "SUM" in sql
        assert "AMOUNT" in sql  # Oracle uppercase
        assert "MONTH" in sql
        assert params == []

    def test_pivot_with_alias(self):
        """Test PIVOT with alias."""
        from rhosocial.activerecord.backend.impl.oracle.expression import PivotExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        pivot = PivotExpression(
            aggregate_function="COUNT",
            value_column="id",
            pivot_column="status",
            values=["A", "B"],
            alias="p"
        )
        sql, params = pivot.to_sql(dialect)
        assert "PIVOT" in sql
        assert "P" in sql.upper()  # Alias in uppercase

    def test_unpivot_basic(self):
        """Test basic UNPIVOT expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import UnpivotExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        unpivot = UnpivotExpression(
            value_column="amount",
            pivot_column="month",
            columns=["jan_sales", "feb_sales", "mar_sales"]
        )
        sql, params = unpivot.to_sql(dialect)
        assert "UNPIVOT" in sql
        assert "EXCLUDE NULLS" in sql
        assert params == []

    def test_unpivot_include_nulls(self):
        """Test UNPIVOT with INCLUDE NULLS."""
        from rhosocial.activerecord.backend.impl.oracle.expression import UnpivotExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        unpivot = UnpivotExpression(
            value_column="amount",
            pivot_column="month",
            columns=["jan_sales", "feb_sales"],
            include_nulls=True
        )
        sql, params = unpivot.to_sql(dialect)
        assert "UNPIVOT" in sql
        assert "INCLUDE NULLS" in sql


class TestHintExpressions:
    """Test query hint expressions."""

    def test_import_hints(self):
        """Test importing hint expressions."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            OracleHintExpression,
            index_hint, parallel_hint, leading_hint
        )

    def test_single_hint(self):
        """Test single hint expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleHintExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        hint = OracleHintExpression(hints=["FULL(users)"])
        sql, params = hint.to_sql(dialect)
        assert sql == "/*+ FULL(users) */"
        assert params == []

    def test_multiple_hints(self):
        """Test multiple hints."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleHintExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        hint = OracleHintExpression(
            hints=["INDEX(users idx_name)", "PARALLEL(4)"]
        )
        sql, params = hint.to_sql(dialect)
        assert "/*+" in sql
        assert "INDEX(users idx_name)" in sql
        assert "PARALLEL(4)" in sql
        assert "*/" in sql

    def test_hint_factories(self):
        """Test hint factory functions."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            index_hint, parallel_hint, leading_hint,
            full_hint, use_nl_hint, use_hash_hint,
            first_rows_hint, all_rows_hint
        )

        assert index_hint("users", "idx_name") == "INDEX(users idx_name)"
        assert parallel_hint("users", 4) == "PARALLEL(users 4)"
        assert leading_hint("users", "orders") == "LEADING(users orders)"
        assert full_hint("users") == "FULL(users)"
        assert use_nl_hint("orders", "line_items") == "USE_NL(orders line_items)"
        assert use_hash_hint("orders", "line_items") == "USE_HASH(orders line_items)"
        assert first_rows_hint(10) == "FIRST_ROWS(10)"
        assert all_rows_hint() == "ALL_ROWS"

    def test_empty_hint(self):
        """Test empty hint expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleHintExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        hint = OracleHintExpression(hints=[])
        sql, params = hint.to_sql(dialect)
        assert sql == ""
        assert params == []

    def test_hint_combination(self):
        """Test combining hints."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleHintExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        hint1 = OracleHintExpression(hints=["FULL(users)"])
        hint2 = OracleHintExpression(hints=["PARALLEL(4)"])
        combined = hint1 + hint2
        sql, params = combined.to_sql(dialect)
        assert "FULL(users)" in sql
        assert "PARALLEL(4)" in sql


class TestLockingExpressions:
    """Test enhanced locking expressions."""

    def test_import_locking(self):
        """Test importing locking expressions."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression

    def test_basic_for_update(self):
        """Test basic FOR UPDATE."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        lock = OracleForUpdateExpression()
        sql, params = lock.to_sql(dialect)
        assert sql == "FOR UPDATE"
        assert params == []

    def test_for_update_nowait(self):
        """Test FOR UPDATE NOWAIT."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        lock = OracleForUpdateExpression(nowait=True)
        sql, params = lock.to_sql(dialect)
        assert "FOR UPDATE" in sql
        assert "NOWAIT" in sql

    def test_for_update_wait(self):
        """Test FOR UPDATE WAIT n."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        lock = OracleForUpdateExpression(wait_seconds=10)
        sql, params = lock.to_sql(dialect)
        assert "FOR UPDATE" in sql
        assert "WAIT 10" in sql

    def test_for_update_skip_locked(self):
        """Test FOR UPDATE SKIP LOCKED."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        lock = OracleForUpdateExpression(skip_locked=True)
        sql, params = lock.to_sql(dialect)
        assert "FOR UPDATE" in sql
        assert "SKIP LOCKED" in sql

    def test_for_update_columns(self):
        """Test FOR UPDATE OF columns."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        lock = OracleForUpdateExpression(columns=["id", "name"])
        sql, params = lock.to_sql(dialect)
        assert "FOR UPDATE" in sql
        assert "OF" in sql
        assert "ID" in sql
        assert "NAME" in sql

    def test_invalid_lock_options(self):
        """Test that invalid option combinations raise error."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression

        with pytest.raises(ValueError):
            OracleForUpdateExpression(nowait=True, skip_locked=True)

        with pytest.raises(ValueError):
            OracleForUpdateExpression(nowait=True, wait_seconds=5)

        with pytest.raises(ValueError):
            OracleForUpdateExpression(wait_seconds=5, skip_locked=True)


class TestDialectSupport:
    """Test dialect support methods."""

    def test_supports_hierarchical(self):
        """Test hierarchical query support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_hierarchical_queries() is True

    def test_supports_connect_by(self):
        """Test CONNECT BY support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_connect_by() is True

    def test_supports_level(self):
        """Test LEVEL pseudo-column support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_level_pseudo_column() is True

    def test_supports_pivot(self):
        """Test PIVOT support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        # Default version is 19c, should support PIVOT (11g+)
        dialect = OracleDialect()
        assert dialect.supports_pivot() is True

        # 10g should not support PIVOT
        dialect_10g = OracleDialect(version=(10, 0, 0))
        assert dialect_10g.supports_pivot() is False

    def test_supports_unpivot(self):
        """Test UNPIVOT support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_unpivot() is True

    def test_supports_hints(self):
        """Test query hint support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_query_hints() is True

    def test_supports_parallel_hint(self):
        """Test PARALLEL hint support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_parallel_hint() is True

    def test_supports_skip_locked(self):
        """Test SKIP LOCKED support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        # Default version is 19c, should support SKIP LOCKED (11g+)
        dialect = OracleDialect()
        assert dialect.supports_for_update_skip_locked() is True

        # 10g should not support SKIP LOCKED
        dialect_10g = OracleDialect(version=(10, 0, 0))
        assert dialect_10g.supports_for_update_skip_locked() is False

    def test_supports_for_update_nowait(self):
        """Test FOR UPDATE NOWAIT support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_for_update_nowait() is True

    def test_supports_for_update_wait(self):
        """Test FOR UPDATE WAIT support check."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_for_update_wait() is True

    def test_format_methods_exist(self):
        """Test that dialect has format methods for expressions."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert hasattr(dialect, 'format_connect_by')
        assert hasattr(dialect, 'format_pivot')
        assert hasattr(dialect, 'format_unpivot')
        assert hasattr(dialect, 'format_hint')
        assert hasattr(dialect, 'format_for_update')
