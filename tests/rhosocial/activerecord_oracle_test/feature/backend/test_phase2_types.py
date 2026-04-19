# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase2_types.py
"""
Tests for Phase 2 type system and adapters.

Tests all Oracle-specific type adapters to ensure proper conversion
between Python and Oracle types.
"""

import pytest
from datetime import timedelta


class TestIntervalAdapter:
    """Test OracleIntervalAdapter functionality."""

    def test_interval_adapter_import(self):
        """Test importing interval adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        adapter = OracleIntervalAdapter()
        assert adapter is not None

    def test_interval_year_to_month_to_db(self):
        """Test converting IntervalYearToMonth to database format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalYearToMonth

        adapter = OracleIntervalAdapter()
        interval = IntervalYearToMonth(years=1, months=6)
        result = adapter.to_database(interval, str)
        assert result == "01-06"

    def test_interval_year_to_month_from_db(self):
        """Test converting database string to IntervalYearToMonth."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalYearToMonth

        adapter = OracleIntervalAdapter()
        result = adapter.from_database("01-06", IntervalYearToMonth)
        assert isinstance(result, IntervalYearToMonth)
        assert result.years == 1
        assert result.months == 6

    def test_interval_day_to_second_to_db(self):
        """Test converting IntervalDayToSecond to database format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalDayToSecond

        adapter = OracleIntervalAdapter()
        interval = IntervalDayToSecond(days=5, hours=12, minutes=30, seconds=45)
        result = adapter.to_database(interval, str)
        assert "5" in result
        assert "12:30:45" in result

    def test_interval_day_to_second_from_db(self):
        """Test converting database string to IntervalDayToSecond."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleIntervalAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import IntervalDayToSecond

        adapter = OracleIntervalAdapter()
        result = adapter.from_database("5 12:30:45", IntervalDayToSecond)
        assert isinstance(result, IntervalDayToSecond)
        assert result.days == 5
        assert result.hours == 12


class TestRowIDAdapter:
    """Test OracleRowIDAdapter functionality."""

    def test_rowid_adapter_import(self):
        """Test importing ROWID adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter
        adapter = OracleRowIDAdapter()
        assert adapter is not None

    def test_rowid_to_db(self):
        """Test converting OracleRowID to database format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleRowID

        adapter = OracleRowIDAdapter()
        rowid = OracleRowID("AAASdqAAEAAAAInAAA")
        result = adapter.to_database(rowid, str)
        assert result == "AAASdqAAEAAAAInAAA"

    def test_rowid_from_db_extended(self):
        """Test converting 18-char ROWID string to OracleRowID."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleRowID

        adapter = OracleRowIDAdapter()
        result = adapter.from_database("AAASdqAAEAAAAInAAA", OracleRowID)
        assert isinstance(result, OracleRowID)
        assert result.data_object_number == "AAASdq"

    def test_urowid_from_db(self):
        """Test converting UROWID string."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleRowIDAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleURowID

        adapter = OracleRowIDAdapter()
        result = adapter.from_database("some_urowid_value", OracleURowID)
        assert str(result) == "some_urowid_value"


class TestXMLAdapter:
    """Test OracleXMLAdapter functionality."""

    def test_xml_adapter_import(self):
        """Test importing XML adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleXMLAdapter
        adapter = OracleXMLAdapter()
        assert adapter is not None

    def test_xml_to_db(self):
        """Test converting OracleXMLType to database format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleXMLAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleXMLType

        adapter = OracleXMLAdapter()
        xml = OracleXMLType("<root><child>value</child></root>")
        result = adapter.to_database(xml, str)
        assert "<root>" in result
        assert "</root>" in result

    def test_xml_from_db(self):
        """Test converting database string to OracleXMLType."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleXMLAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleXMLType

        adapter = OracleXMLAdapter()
        result = adapter.from_database("<root><child>value</child></root>", OracleXMLType)
        assert isinstance(result, OracleXMLType)
        assert result.is_valid


class TestSDOGeometryAdapter:
    """Test OracleSDOGeometryAdapter functionality."""

    def test_sdo_adapter_import(self):
        """Test importing SDO_GEOMETRY adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleSDOGeometryAdapter
        adapter = OracleSDOGeometryAdapter()
        assert adapter is not None

    def test_sdo_point_to_db(self):
        """Test converting SDOGeometry point to SQL constructor."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleSDOGeometryAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import SDOGeometry

        adapter = OracleSDOGeometryAdapter()
        geom = SDOGeometry.point(10.0, 20.0)
        result = adapter.to_database(geom, str)
        assert "SDO_GEOMETRY" in result
        assert "SDO_POINT_TYPE" in result

    def test_sdo_from_dict(self):
        """Test converting dictionary to SDOGeometry."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleSDOGeometryAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import SDOGeometry

        adapter = OracleSDOGeometryAdapter()
        data = {
            'SDO_GTYPE': 2001,
            'SDO_SRID': 4326,
            'SDO_POINT': {'X': 10.0, 'Y': 20.0},
            'SDO_ELEM_INFO': [],
            'SDO_ORDINATES': []
        }
        result = adapter.from_database(data, SDOGeometry)
        assert isinstance(result, SDOGeometry)
        assert result.sdo_gtype == 2001
        assert result.sdo_point.x == 10.0


class TestVectorAdapter:
    """Test OracleVectorAdapter functionality."""

    def test_vector_adapter_import(self):
        """Test importing VECTOR adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter
        adapter = OracleVectorAdapter()
        assert adapter is not None

    def test_vector_to_db_string(self):
        """Test converting OracleVector to string format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector

        adapter = OracleVectorAdapter()
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        result = adapter.to_database(vec, str)
        assert result == "[1.0, 2.0, 3.0]"

    def test_vector_list_to_db(self):
        """Test converting Python list to vector string."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter

        adapter = OracleVectorAdapter()
        result = adapter.to_database([1.0, 2.0, 3.0], str)
        assert result == "[1.0, 2.0, 3.0]"

    def test_vector_from_db_string(self):
        """Test converting string to OracleVector."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector

        adapter = OracleVectorAdapter()
        result = adapter.from_database("[1.0, 2.0, 3.0]", OracleVector)
        assert isinstance(result, OracleVector)
        assert result.dimensions == 3
        assert result[0] == 1.0

    def test_vector_from_db_list(self):
        """Test converting list to OracleVector."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleVectorAdapter
        from rhosocial.activerecord.backend.impl.oracle.types import OracleVector

        adapter = OracleVectorAdapter()
        result = adapter.from_database([1.0, 2.0, 3.0], OracleVector)
        assert isinstance(result, OracleVector)
        assert result.dimensions == 3


class TestBooleanAdapter:
    """Test OracleBooleanAdapter functionality."""

    def test_boolean_adapter_import(self):
        """Test importing boolean adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter
        adapter = OracleBooleanAdapter()
        assert adapter is not None

    def test_true_to_db(self):
        """Test converting True to database format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter

        adapter = OracleBooleanAdapter()
        result = adapter.to_database(True, int)
        assert result == 1

    def test_false_to_db(self):
        """Test converting False to database format."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter

        adapter = OracleBooleanAdapter()
        result = adapter.to_database(False, int)
        assert result == 0

    def test_from_db_one(self):
        """Test converting 1 to True."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter

        adapter = OracleBooleanAdapter()
        result = adapter.from_database(1, bool)
        assert result is True

    def test_from_db_zero(self):
        """Test converting 0 to False."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter

        adapter = OracleBooleanAdapter()
        result = adapter.from_database(0, bool)
        assert result is False


class TestDateTimeAdapter:
    """Test OracleDateTimeAdapter functionality."""

    def test_datetime_adapter_import(self):
        """Test importing datetime adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleDateTimeAdapter
        adapter = OracleDateTimeAdapter()
        assert adapter is not None

    def test_datetime_roundtrip(self):
        """Test datetime roundtrip conversion."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleDateTimeAdapter
        from datetime import datetime, timezone

        adapter = OracleDateTimeAdapter()
        dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
        # Oracle thin client handles datetime directly
        result = adapter.to_database(dt, type(dt))
        assert result == dt


class TestJSONAdapter:
    """Test OracleJSONAdapter functionality."""

    def test_json_adapter_import(self):
        """Test importing JSON adapter."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleJSONAdapter
        adapter = OracleJSONAdapter()
        assert adapter is not None

    def test_dict_to_db(self):
        """Test converting dict to JSON string."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleJSONAdapter

        adapter = OracleJSONAdapter()
        data = {"key": "value", "number": 123}
        result = adapter.to_database(data, str)
        assert '"key"' in result
        assert '"value"' in result

    def test_list_to_db(self):
        """Test converting list to JSON string."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleJSONAdapter

        adapter = OracleJSONAdapter()
        data = [1, 2, 3]
        result = adapter.to_database(data, str)
        assert result == "[1, 2, 3]"

    def test_from_db_string(self):
        """Test converting JSON string to dict."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import OracleJSONAdapter

        adapter = OracleJSONAdapter()
        result = adapter.from_database('{"key": "value"}', dict)
        assert result == {"key": "value"}


class TestAdaptersList:
    """Test the oracle_adapters list."""

    def test_adapters_list_import(self):
        """Test that oracle_adapters list is available."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import oracle_adapters
        assert len(oracle_adapters) > 0

    def test_all_adapters_instantiable(self):
        """Test that all adapters can be instantiated."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import oracle_adapters
        for adapter_cls in oracle_adapters:
            adapter = adapter_cls()
            assert adapter is not None

    def test_adapters_count(self):
        """Test that expected number of adapters are defined."""
        from rhosocial.activerecord.backend.impl.oracle.adapters import oracle_adapters
        # Should have: Boolean, DateTime, Date, Time, Decimal, JSON, Bytes,
        # Interval, RowID, XML, SDOGeometry, Vector = 12 adapters
        assert len(oracle_adapters) == 12
