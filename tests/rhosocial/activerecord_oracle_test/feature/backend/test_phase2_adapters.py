# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase2_adapters.py
"""
Tests for Phase 2 type adapters.

Tests verify that new Oracle-specific type adapters work correctly.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'src'))


class TestIntervalAdapter:
    """Test IntervalAdapter functionality."""

    def test_adapter_imports(self):
        """Test that interval adapters can be imported."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        assert OracleIntervalAdapter is not None

    def test_interval_year_to_month_conversion(self):
        """Test YEAR TO MONTH interval conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalYearToMonth

        adapter = OracleIntervalAdapter()
        interval = IntervalYearToMonth(years=1, months=6)
        
        db_value = adapter.to_database(interval, str)
        assert db_value == "01-06"

    def test_interval_day_to_second_conversion(self):
        """Test DAY TO SECOND interval conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalDayToSecond

        adapter = OracleIntervalAdapter()
        interval = IntervalDayToSecond(days=5, hours=12, minutes=30, seconds=45)
        
        db_value = adapter.to_database(interval, str)
        assert "5" in db_value


class TestRowIDAdapter:
    """Test RowIDAdapter functionality."""

    def test_adapter_imports(self):
        """Test that ROWID adapter can be imported."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter
        assert OracleRowIDAdapter is not None

    def test_rowid_conversion(self):
        """Test ROWID conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleRowID

        adapter = OracleRowIDAdapter()
        rowid = OracleRowID("AAASdqAAEAAAAInAAA")
        
        db_value = adapter.to_database(rowid, str)
        assert db_value == "AAASdqAAEAAAAInAAA"

    def test_rowid_from_database(self):
        """Test ROWID from database conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter

        adapter = OracleRowIDAdapter()
        
        result = adapter.from_database("AAASdqAAEAAAAInAAA", str)
        assert hasattr(result, 'data_object_number')


class TestXMLAdapter:
    """Test XMLAdapter functionality."""

    def test_adapter_imports(self):
        """Test that XML adapter can be imported."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleXMLAdapter
        assert OracleXMLAdapter is not None

    def test_xml_conversion(self):
        """Test XMLType conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleXMLAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleXMLType

        adapter = OracleXMLAdapter()
        xml = OracleXMLType("<root><name>test</name></root>")
        
        db_value = adapter.to_database(xml, str)
        assert "<root>" in db_value


class TestSDOGeometryAdapter:
    """Test SDOGeometryAdapter functionality."""

    def test_adapter_imports(self):
        """Test that SDO_GEOMETRY adapter can be imported."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleSDOGeometryAdapter
        assert OracleSDOGeometryAdapter is not None

    def test_point_conversion(self):
        """Test point geometry conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleSDOGeometryAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import SDOGeometry

        adapter = OracleSDOGeometryAdapter()
        point = SDOGeometry.point(10.0, 20.0)
        
        db_value = adapter.to_database(point, str)
        assert "SDO_GEOMETRY" in db_value
        assert "SDO_POINT_TYPE" in db_value

    def test_geometry_from_dict(self):
        """Test geometry from database dict."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleSDOGeometryAdapter

        adapter = OracleSDOGeometryAdapter()
        
        geom_dict = {
            'SDO_GTYPE': 2001,
            'SDO_SRID': 4326,
            'SDO_POINT': {'X': 10.0, 'Y': 20.0},
            'SDO_ELEM_INFO': [],
            'SDO_ORDINATES': []
        }
        
        result = adapter.from_database(geom_dict, str)
        assert result.sdo_gtype == 2001


class TestVectorAdapter:
    """Test VectorAdapter functionality."""

    def test_adapter_imports(self):
        """Test that VECTOR adapter can be imported."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter
        assert OracleVectorAdapter is not None

    def test_vector_conversion(self):
        """Test vector conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector

        adapter = OracleVectorAdapter()
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        
        db_value = adapter.to_database(vec, str)
        assert db_value == "[1.0, 2.0, 3.0]"

    def test_vector_from_string(self):
        """Test vector from string."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter

        adapter = OracleVectorAdapter()
        
        result = adapter.from_database("[1.0, 2.0, 3.0]", str)
        assert result.dimensions == 3


class TestAdapterList:
    """Test that all adapters are properly listed."""

    def test_oracle_adapters_list(self):
        """Test that all adapters are in the list."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import oracle_adapters
        
        assert len(oracle_adapters) == 12
        adapter_names = [a.__name__ for a in oracle_adapters]
        
        assert 'OracleBooleanAdapter' in adapter_names
        assert 'OracleIntervalAdapter' in adapter_names
        assert 'OracleRowIDAdapter' in adapter_names
        assert 'OracleXMLAdapter' in adapter_names
        assert 'OracleSDOGeometryAdapter' in adapter_names
        assert 'OracleVectorAdapter' in adapter_names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
