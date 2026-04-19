# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase1_structure.py
"""
Tests for Phase 1 directory restructuring.

Tests verify that all new modules and packages are importable
and provide the expected functionality.
"""

import pytest


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
    
    def test_explain_result_build_tree(self):
        """Test tree building."""
        from rhosocial.activerecord.backend.impl.oracle.explain import (
            OracleExplainResult, OracleExplainRow
        )
        row1 = OracleExplainRow(id=1, operation="SELECT")
        row2 = OracleExplainRow(id=2, operation="TABLE ACCESS", parent_id=1)
        result = OracleExplainResult(statement_id="test", rows=[row1, row2])
        result.build_tree()
        assert result.root == row1
        assert row2 in row1.children


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
    
    def test_interval_year_to_month_iso8601(self):
        """Test ISO 8601 conversion."""
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalYearToMonth
        interval = IntervalYearToMonth(years=2, months=3)
        assert interval.to_iso8601() == "P2Y3M"
    
    def test_interval_day_to_second(self):
        """Test IntervalDayToSecond creation."""
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalDayToSecond
        interval = IntervalDayToSecond(days=5, hours=12, minutes=30, seconds=45)
        assert interval.days == 5
        assert interval.hours == 12
        assert str(interval) == "5 12:30:45"
    
    def test_interval_to_timedelta(self):
        """Test timedelta conversion."""
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalDayToSecond
        from datetime import timedelta
        interval = IntervalDayToSecond(days=1, hours=2, minutes=30, seconds=0)
        td = interval.to_timedelta()
        assert td.days == 1
        assert td.seconds == 9000
    
    def test_import_rowid_types(self):
        """Test ROWID type imports."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleRowID
        rowid = OracleRowID("AAASdqAAEAAAAInAAA")
        assert rowid.data_object_number == "AAASdq"
        assert rowid.file_number == "AAE"
        assert rowid.block_number == "AAAAIn"
        assert rowid.row_number == "AAA"
    
    def test_rowid_validation(self):
        """Test ROWID validation."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleRowID
        with pytest.raises(ValueError):
            OracleRowID("invalid-rowid")
    
    def test_import_xml_type(self):
        """Test XMLType import."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleXMLType
        xml = OracleXMLType("<root><child>value</child></root>")
        assert xml.is_valid
        assert xml.root is not None
    
    def test_import_spatial_types(self):
        """Test spatial type imports."""
        from rhosocial.activerecord.backend.impl.oracle.types import (
            SDOGeometry, SDOPoint, SDOGeometryType
        )
        point = SDOPoint(x=1.0, y=2.0)
        assert point.x == 1.0
        
        geom = SDOGeometry(sdo_gtype=2001, sdo_point=point)
        assert geom.geometry_type == SDOGeometryType.POINT
        assert geom.dimension == 2
    
    def test_sdo_geometry_factories(self):
        """Test SDOGeometry factory methods."""
        from rhosocial.activerecord.backend.impl.oracle.types import SDOGeometry
        point = SDOGeometry.point(10.0, 20.0)
        assert point.is_point
        assert point.sdo_point.x == 10.0
        
        polygon = SDOGeometry.polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
        assert polygon.is_polygon
    
    def test_import_vector_type(self):
        """Test VECTOR type import."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        assert vec.dimensions == 3
        assert len(vec) == 3
    
    def test_vector_validation(self):
        """Test vector validation."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector
        with pytest.raises(ValueError):
            OracleVector(dimensions=3, values=[1.0, 2.0])
    
    def test_vector_similarity(self):
        """Test vector similarity methods."""
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector
        v1 = OracleVector(dimensions=3, values=[1.0, 0.0, 0.0])
        v2 = OracleVector(dimensions=3, values=[1.0, 0.0, 0.0])
        assert v1.cosine_similarity(v2) == 1.0
        
        v3 = OracleVector(dimensions=3, values=[0.0, 1.0, 0.0])
        assert abs(v1.cosine_similarity(v3)) < 0.0001


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
        assert PriorExpression is not None
    
    def test_import_pivot_expressions(self):
        """Test pivot expression imports."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            PivotExpression, UnpivotExpression
        )
        assert PivotExpression is not None
        assert UnpivotExpression is not None
    
    def test_import_hint_expressions(self):
        """Test hint expression imports."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            OracleHintExpression
        )
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            index_hint, parallel_hint
        )
        assert OracleHintExpression is not None
        assert index_hint("t", "idx") == "INDEX(t idx)"
    
    def test_connect_by_root(self):
        """Test CONNECT_BY_ROOT expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import ConnectByRootExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        expr = ConnectByRootExpression(column="employee_id")
        sql, params = expr.to_sql(dialect)
        assert "CONNECT_BY_ROOT" in sql
    
    def test_sys_connect_by_path(self):
        """Test SYS_CONNECT_BY_PATH expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import SysConnectByPathExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        expr = SysConnectByPathExpression(column="name", separator="/")
        sql, params = expr.to_sql(dialect)
        assert "SYS_CONNECT_BY_PATH" in sql
        assert "'/'" in sql
    
    def test_pivot_expression(self):
        """Test PIVOT expression."""
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
    
    def test_hint_expression(self):
        """Test hint expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleHintExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        hint = OracleHintExpression(hints=["FULL(users)", "PARALLEL(4)"])
        sql, params = hint.to_sql(dialect)
        assert "/*+ FULL(users) PARALLEL(4) */" in sql
    
    def test_for_update_expression(self):
        """Test FOR UPDATE expression."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect()
        
        lock = OracleForUpdateExpression()
        sql, params = lock.to_sql(dialect)
        assert sql == "FOR UPDATE"
        
        lock_nowait = OracleForUpdateExpression(nowait=True)
        sql, params = lock_nowait.to_sql(dialect)
        assert "NOWAIT" in sql
        
        lock_wait = OracleForUpdateExpression(wait_seconds=10)
        sql, params = lock_wait.to_sql(dialect)
        assert "WAIT 10" in sql
    
    def test_invalid_lock_options(self):
        """Test invalid lock option combination."""
        from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression
        with pytest.raises(ValueError):
            OracleForUpdateExpression(nowait=True, skip_locked=True)


class TestFunctionsPackage:
    """Test functions/ package imports and functionality."""
    
    def test_import_json_functions(self):
        """Test JSON function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            json_value, json_query, json_exists
        )
        assert callable(json_value)
        assert callable(json_query)
        assert callable(json_exists)
    
    def test_import_spatial_functions(self):
        """Test spatial function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            sdo_geom_distance, sdo_within_distance
        )
        assert callable(sdo_geom_distance)
        assert callable(sdo_within_distance)
    
    def test_import_datetime_functions(self):
        """Test datetime function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            to_date, to_char, add_months
        )
        assert callable(to_date)
        assert callable(to_char)
        assert callable(add_months)
    
    def test_import_null_functions(self):
        """Test null handling function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            nvl, nvl2, coalesce_oracle, nullif
        )
        assert callable(nvl)
        assert callable(nvl2)
        assert callable(coalesce_oracle)
        assert callable(nullif)
    
    def test_import_string_functions(self):
        """Test string function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            decode_expr, regexp_substr, regexp_like
        )
        assert callable(decode_expr)
        assert callable(regexp_substr)
        assert callable(regexp_like)
    
    def test_import_analytic_functions(self):
        """Test analytic function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            listagg, percentile_cont, percentile_disc
        )
        assert callable(listagg)
    
    def test_import_conversion_functions(self):
        """Test conversion function imports."""
        from rhosocial.activerecord.backend.impl.oracle.functions import (
            to_number, cast_expr
        )
        assert callable(to_number)
        assert callable(cast_expr)
