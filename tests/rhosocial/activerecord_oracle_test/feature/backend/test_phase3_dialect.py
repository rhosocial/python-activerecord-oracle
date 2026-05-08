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

    def test_set_operation_protocol(self):
        """Test SetOperationSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.dialect.protocols import SetOperationSupport

        dialect = OracleDialect()
        assert isinstance(dialect, SetOperationSupport)

    def test_truncate_protocol(self):
        """Test TruncateSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.dialect.protocols import TruncateSupport

        dialect = OracleDialect()
        assert isinstance(dialect, TruncateSupport)

    def test_constraint_protocol(self):
        """Test ConstraintSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.dialect.protocols import ConstraintSupport

        dialect = OracleDialect()
        assert isinstance(dialect, ConstraintSupport)

    def test_transaction_control_protocol(self):
        """Test TransactionControlSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.dialect.protocols import TransactionControlSupport

        dialect = OracleDialect()
        assert isinstance(dialect, TransactionControlSupport)

    def test_sql_function_protocol(self):
        """Test SQLFunctionSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.dialect.protocols import SQLFunctionSupport

        dialect = OracleDialect()
        assert isinstance(dialect, SQLFunctionSupport)

    def test_ora_native_json_protocol(self):
        """Test OracleNativeJSONSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import OracleNativeJSONSupport

        dialect = OracleDialect(version=(21, 0, 0))
        assert isinstance(dialect, OracleNativeJSONSupport)

    def test_ora_boolean_type_protocol(self):
        """Test OracleBooleanTypeSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import OracleBooleanTypeSupport

        dialect = OracleDialect(version=(23, 0, 0))
        assert isinstance(dialect, OracleBooleanTypeSupport)

    def test_ora_vector_type_protocol(self):
        """Test OracleVectorTypeSupport protocol compliance."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.protocols import OracleVectorTypeSupport

        dialect = OracleDialect(version=(23, 0, 0))
        assert isinstance(dialect, OracleVectorTypeSupport)


class TestConstraintCapabilities:
    """Test Oracle dialect constraint capabilities."""

    def test_supports_primary_key(self):
        """Test PRIMARY KEY constraint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_primary_key_constraint() is True

    def test_supports_foreign_key(self):
        """Test FOREIGN KEY constraint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_foreign_key_constraint() is True

    def test_supports_check_constraint(self):
        """Test CHECK constraint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_check_constraint() is True

    def test_supports_deferrable_constraint(self):
        """Test DEFERRABLE constraint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_deferrable_constraint() is True

    def test_no_fk_on_update(self):
        """Test Oracle does not support ON UPDATE."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_fk_on_update() is False

    def test_no_fk_match(self):
        """Test Oracle does not support MATCH PARTIAL/FULL."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_fk_match() is False

    def test_constraint_enforced_version_dependent(self):
        """Test ENFORCED/NOT ENFORCED support requires 12c+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        old_dialect = OracleDialect(version=(11, 0, 0))
        assert old_dialect.supports_constraint_enforced() is False

        new_dialect = OracleDialect(version=(12, 0, 0))
        assert new_dialect.supports_constraint_enforced() is True


class TestTransactionControl:
    """Test Oracle dialect transaction control capabilities."""

    def test_supports_savepoint(self):
        """Test savepoint support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_savepoint() is True

    def test_supports_read_only(self):
        """Test READ ONLY transaction support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_read_only_transaction() is True

    def test_supports_deferrable_transaction(self):
        """Test DEFERRABLE transaction support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_deferrable_transaction() is True

    def test_no_isolation_in_begin(self):
        """Test Oracle does not use isolation level in BEGIN."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_isolation_level_in_begin() is False

    def test_format_commit(self):
        """Test COMMIT formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.expression.transaction import CommitTransactionExpression
        dialect = OracleDialect()
        expr = CommitTransactionExpression(dialect)
        sql, params = dialect.format_commit_transaction(expr)
        assert sql == "COMMIT"

    def test_format_rollback(self):
        """Test ROLLBACK formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.expression.transaction import RollbackTransactionExpression
        dialect = OracleDialect()
        expr = RollbackTransactionExpression(dialect)
        sql, params = dialect.format_rollback_transaction(expr)
        assert sql == "ROLLBACK"

    def test_format_savepoint(self):
        """Test SAVEPOINT formatting."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.expression.transaction import SavepointExpression
        dialect = OracleDialect()
        expr = SavepointExpression(dialect, "sp1")
        sql, params = dialect.format_savepoint(expr)
        assert "SAVEPOINT" in sql
        assert "SP1" in sql


class TestTruncateCapabilities:
    """Test Oracle dialect TRUNCATE capabilities."""

    def test_supports_truncate(self):
        """Test TRUNCATE support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_truncate() is True

    def test_no_truncate_cascade(self):
        """Test Oracle does not support CASCADE on TRUNCATE."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_truncate_cascade() is False


class TestFunctionSupport:
    """Test Oracle dialect function support."""

    def test_supports_functions_exists(self):
        """Test supports_functions method exists."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        funcs = dialect.supports_functions()
        assert isinstance(funcs, dict)
        assert len(funcs) > 0

    def test_common_functions_available(self):
        """Test that common SQL functions are reported as available."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        funcs = dialect.supports_functions()
        assert funcs.get("count") is True
        assert funcs.get("sum") is True
        assert funcs.get("min") is True
        assert funcs.get("max") is True
        assert funcs.get("avg") is True

    def test_oracle_specific_functions(self):
        """Test Oracle-specific functions."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        funcs = dialect.supports_functions()
        assert funcs.get("nvl") is True
        assert funcs.get("decode") is True
        assert funcs.get("to_date") is True
        assert funcs.get("listagg") is True

    def test_json_functions_version_dependent(self):
        """Test JSON functions require 12c+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        old_dialect = OracleDialect(version=(9, 0, 0))
        old_funcs = old_dialect.supports_functions()
        assert old_funcs.get("json_value") is False

        new_dialect = OracleDialect(version=(12, 1, 0))
        new_funcs = new_dialect.supports_functions()
        assert new_funcs.get("json_value") is True

    def test_listagg_version_dependent(self):
        """Test LISTAGG requires 11g R2+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        old_dialect = OracleDialect(version=(10, 0, 0))
        old_funcs = old_dialect.supports_functions()
        assert old_funcs.get("listagg") is False

        new_dialect = OracleDialect(version=(11, 2, 0))
        new_funcs = new_dialect.supports_functions()
        assert new_funcs.get("listagg") is True

    def test_regexp_count_version_dependent(self):
        """Test REGEXP_COUNT requires 12c R2+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        old_dialect = OracleDialect(version=(11, 0, 0))
        old_funcs = old_dialect.supports_functions()
        assert old_funcs.get("regexp_count") is False

        new_dialect = OracleDialect(version=(12, 2, 0))
        new_funcs = new_dialect.supports_functions()
        assert new_funcs.get("regexp_count") is True


class TestSetOperationCapabilities:
    """Test Oracle dialect set operation capabilities."""

    def test_supports_union(self):
        """Test UNION support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_union() is True
        assert dialect.supports_union_all() is True

    def test_supports_intersect(self):
        """Test INTERSECT support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_intersect() is True

    def test_supports_except(self):
        """Test EXCEPT (MINUS) support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_except() is True

    def test_set_op_order_by(self):
        """Test set operation ORDER BY support."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        assert dialect.supports_set_operation_order_by() is True

    def test_set_op_limit_offset_version_dependent(self):
        """Test set operation LIMIT/OFFSET requires 12c+."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        old_dialect = OracleDialect(version=(10, 0, 0))
        assert old_dialect.supports_set_operation_limit_offset() is False

        new_dialect = OracleDialect(version=(12, 0, 0))
        assert new_dialect.supports_set_operation_limit_offset() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
