# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase3_dialect.py
"""
Tests for Phase 3 dialect enhancements.

Tests verify that Oracle dialect properly supports new expressions
and protocols.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'src'))


class TestDialectHierarchicalSupport:
    """Test dialect hierarchical query support."""

    def test_supports_hierarchical_queries(self):
        """Test that dialect reports hierarchical query support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_hierarchical_queries() is True

    def test_supports_connect_by(self):
        """Test CONNECT BY support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_connect_by() is True

    def test_supports_level_pseudo_column(self):
        """Test LEVEL pseudo-column support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_level_pseudo_column() is True

    def test_supports_connect_by_root(self):
        """Test CONNECT_BY_ROOT support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_connect_by_root() is True

    def test_supports_sys_connect_by_path(self):
        """Test SYS_CONNECT_BY_PATH support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_sys_connect_by_path() is True


class TestDialectPivotSupport:
    """Test dialect PIVOT/UNPIVOT support."""

    def test_supports_pivot(self):
        """Test PIVOT support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect(version=(12, 0, 0))
        assert dialect.supports_pivot() is True

    def test_supports_unpivot(self):
        """Test UNPIVOT support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect(version=(12, 0, 0))
        assert dialect.supports_unpivot() is True

    def test_pivot_requires_11g(self):
        """Test that PIVOT requires 11g+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        old_dialect = OracleDialect(version=(10, 0, 0))
        assert old_dialect.supports_pivot() is False

        new_dialect = OracleDialect(version=(11, 0, 0))
        assert new_dialect.supports_pivot() is True


class TestDialectHintSupport:
    """Test dialect query hint support."""

    def test_supports_query_hints(self):
        """Test query hint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_query_hints() is True

    def test_supports_parallel_hint(self):
        """Test PARALLEL hint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_parallel_hint() is True

    def test_supports_index_hint(self):
        """Test INDEX hint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_index_hint() is True

    def test_supports_leading_hint(self):
        """Test LEADING hint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_leading_hint() is True


class TestDialectLockingSupport:
    """Test dialect enhanced locking support."""

    def test_supports_for_update_nowait(self):
        """Test NOWAIT support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_for_update_nowait() is True

    def test_supports_for_update_wait(self):
        """Test WAIT n support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_for_update_wait() is True

    def test_supports_for_update_of(self):
        """Test OF columns support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect()
        assert dialect.supports_for_update_of() is True


class TestDialectOracleSpecificSupport:
    """Test Oracle-specific feature support."""

    def test_supports_native_json_21c(self):
        """Test native JSON support requires 21c+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        old_dialect = OracleDialect(version=(19, 0, 0))
        assert old_dialect.supports_native_json() is False

        new_dialect = OracleDialect(version=(21, 0, 0))
        assert new_dialect.supports_native_json() is True

    def test_supports_boolean_type_23ai(self):
        """Test BOOLEAN type requires 23ai+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        old_dialect = OracleDialect(version=(21, 0, 0))
        assert old_dialect.supports_boolean_type() is False

        new_dialect = OracleDialect(version=(23, 0, 0))
        assert new_dialect.supports_boolean_type() is True

    def test_supports_vector_type_23ai(self):
        """Test VECTOR type requires 23ai+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        old_dialect = OracleDialect(version=(21, 0, 0))
        assert old_dialect.supports_vector_type() is False

        new_dialect = OracleDialect(version=(23, 0, 0))
        assert new_dialect.supports_vector_type() is True


class TestDialectExpressionFormatting:
    """Test dialect expression formatting methods."""

    def test_format_connect_by(self):
        """Test CONNECT BY formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.expression import ConnectByRootExpression

        dialect = OracleDialect()
        expr = ConnectByRootExpression(column="id")
        
        sql, params = dialect.format_connect_by(expr)
        assert "CONNECT_BY_ROOT" in sql

    def test_format_pivot(self):
        """Test PIVOT formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.expression import PivotExpression

        dialect = OracleDialect()
        pivot = PivotExpression(
            aggregate_function="SUM",
            value_column="amount",
            pivot_column="month",
            values=["Jan", "Feb"]
        )
        
        sql, params = dialect.format_pivot(pivot)
        assert "PIVOT" in sql
        assert "SUM" in sql

    def test_format_hint(self):
        """Test hint formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleHintExpression

        dialect = OracleDialect()
        hint = OracleHintExpression(hints=["FULL(users)"])
        
        sql, params = dialect.format_hint(hint)
        assert "/*+ FULL(users) */" in sql

    def test_format_for_update(self):
        """Test FOR UPDATE formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression

        dialect = OracleDialect()
        lock = OracleForUpdateExpression(nowait=True)
        
        sql, params = dialect.format_for_update(lock)
        assert "FOR UPDATE" in sql
        assert "NOWAIT" in sql


class TestProtocolCompliance:
    """Test that dialect implements all required protocols."""

    def test_hierarchical_query_protocol(self):
        """Test HierarchicalQuerySupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import HierarchicalQuerySupport

        dialect = OracleDialect()
        assert isinstance(dialect, HierarchicalQuerySupport)

    def test_pivot_protocol(self):
        """Test PivotSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import PivotSupport

        dialect = OracleDialect(version=(12, 0, 0))
        assert isinstance(dialect, PivotSupport)

    def test_hint_protocol(self):
        """Test QueryHintSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import QueryHintSupport

        dialect = OracleDialect()
        assert isinstance(dialect, QueryHintSupport)

    def test_locking_protocol(self):
        """Test OracleLockingSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import OracleLockingSupport

        dialect = OracleDialect()
        assert isinstance(dialect, OracleLockingSupport)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
