# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase1_standalone.py
"""
Standalone tests for Phase 1 directory restructuring.
Does not require database connection or test providers.
"""

import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'src'))


class TestExplainModule:
    """Test explain/ module imports and functionality."""
    
    def test_import_explain_types(self):
        """Test that explain types can be imported."""
        from rhosocial.activerecord.backend.impl.oracle.explain import (
            OracleExplainRow, OracleExplainResult
        )
        assert OracleExplainRow is not None
        assert OracleExplainResult is not None
    
    def test_explain_row_creation(self):
        """Test OracleExplainRow instantiation."""
        from rhosocial.activerecord.backend.impl.oracle.explain import OracleExplainRow
        row = OracleExplainRow(id=1, operation="TABLE ACCESS")
        assert row.id == 1
        assert row.operation == "TABLE ACCESS"
        assert row.children == []
    
    def test_explain_row_is_full_scan(self):
        """Test is_full_scan method."""
        from rhosocial.activerecord.backend.impl.oracle.explain import OracleExplainRow
        row = OracleExplainRow(id=1, operation="TABLE ACCESS", options="FULL")
        assert row.is_full_scan() is True
        
        row2 = OracleExplainRow(id=2, operation="TABLE ACCESS", options="BY INDEX ROWID")
        assert row2.is_full_scan() is False
    
    def test_explain_result_creation(self):
        """Test OracleExplainResult instantiation."""
        from rhosocial.activerecord.backend.impl.oracle.explain import (
            OracleExplainResult, OracleExplainRow
        )
        row1 = OracleExplainRow(id=1, operation="SELECT")
        row2 = OracleExplainRow(id=2, operation="TABLE ACCESS", parent_id=1)
        result = OracleExplainResult(statement_id="test", rows=[row1, row2])
        assert result.statement_id == "test"
        assert result.get_operation_count() == 2


class TestTypesModule:
    """Test types/ module imports and functionality."""
    
    def test_import_interval_types(self):
        """Test interval type imports."""
        from rhosocial.activerecord.backend.impl.oracle.types import (
            IntervalYearToMonth, IntervalDayToSecond
        )
        assert IntervalYearToMonth is not None
        assert IntervalDayToSecond is not None
    
    def test_interval_year_to_month(self):
        """Test IntervalYearToMonth creation."""
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalYearToMonth
        interval = IntervalYearToMonth(years=1, months=3)
        assert interval.years == 1
        assert interval.months == 3
        assert str(interval) == "01-03"
    
    def test_interval_year_to_month_validation(self):
        """Test interval validation."""
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalYearToMonth
        with pytest.raises(ValueError):
            IntervalYearToMonth(years=1, months=15)
    
    def test_import_rowid_types(self):
        """Test ROWID type imports."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleRowID
        rowid = OracleRowID("AAASdqAAEAAAAInAAA")
        assert rowid.data_object_number == "AAASdq"
    
    def test_import_xml_type(self):
        """Test XMLType import."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleXMLType
        xml = OracleXMLType("<root><child>value</child></root>")
        assert xml.is_valid
    
    def test_import_spatial_types(self):
        """Test spatial type imports."""
        from rhosocial.activerecord.backend.impl.oracle.types import (
            SDOGeometry, SDOPoint, SDOGeometryType
        )
        point = SDOPoint(x=1.0, y=2.0)
        assert point.x == 1.0
        
        geom = SDOGeometry(sdo_gtype=2001, sdo_point=point)
        assert geom.geometry_type == SDOGeometryType.POINT
    
    def test_import_vector_type(self):
        """Test VECTOR type import."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        assert vec.dimensions == 3


class TestExpressionModule:
    """Test expression/ module imports and functionality."""
    
    def test_import_hierarchical_expressions(self):
        """Test hierarchical expression imports."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            ConnectByExpression, PriorExpression,
            ConnectByRootExpression, SysConnectByPathExpression,
            LevelExpression
        )
        assert ConnectByExpression is not None
    
    def test_import_pivot_expressions(self):
        """Test pivot expression imports."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            PivotExpression, UnpivotExpression
        )
        assert PivotExpression is not None
    
    def test_import_hint_expressions(self):
        """Test hint expression imports."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            OracleHintExpression, index_hint, parallel_hint
        )
        assert OracleHintExpression is not None
        assert index_hint("t", "idx") == "INDEX(t idx)"
    
    def test_expression_to_sql(self):
        """Test expression SQL generation."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            ConnectByRootExpression, OracleForUpdateExpression
        )
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        
        dialect = OracleDialect()
        
        root_expr = ConnectByRootExpression(column="employee_id")
        sql, params = root_expr.to_sql(dialect)
        assert "CONNECT_BY_ROOT" in sql
        
        lock = OracleForUpdateExpression(nowait=True)
        sql, params = lock.to_sql(dialect)
        assert "NOWAIT" in sql


class TestFunctionsPackage:
    """Test functions/ package imports."""
    
    def test_import_json_functions(self):
        """Test JSON function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            json_value, json_query, json_exists
        )
        assert callable(json_value)
    
    def test_import_datetime_functions(self):
        """Test datetime function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            to_date, to_char, add_months
        )
        assert callable(to_date)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
