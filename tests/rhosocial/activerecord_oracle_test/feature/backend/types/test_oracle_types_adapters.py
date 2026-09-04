# tests/rhosocial/activerecord_oracle_test/feature/backend/types/test_oracle_types_adapters.py
"""Offline formatting tests for Oracle type rendering (mixins/types.py) and
the Python↔Oracle value adapters (adapters.py).

Every ``format_data_type_*`` handler is asserted through the dialect
dispatcher with the matching core ``DataType`` instance; every adapter is
exercised in both the python→bind-value and result-value→python directions,
including NULL / empty-string semantics.
"""
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Optional

import pytest

from rhosocial.activerecord.backend.expression.types import (
    BigIntType, BlobType, BooleanType, CharType, CustomType, DateType,
    DateTimeType, DecimalType, DoubleType, FloatType, IntegerType, JsonType,
    JsonBType, RealType, SmallIntType, TextType, TimeType, TimeTzType,
    TimestampType, TimestampTzType, TinyIntType, VarCharType,
)
from rhosocial.activerecord.backend.impl.oracle.adapters import (
    OracleBooleanAdapter, OracleBytesAdapter, OracleDateAdapter,
    OracleDateTimeAdapter, OracleDecimalAdapter, OracleEnumAdapter,
    OracleIntervalAdapter, OracleJSONAdapter, OracleRowIDAdapter,
    OracleSDOGeometryAdapter, OracleStringAdapter, OracleTimeAdapter,
    OracleUUIDAdapter, OracleVectorAdapter, OracleXMLAdapter,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.types import (
    OracleBigIntType, OracleBlobType, OracleCharType, OracleClobType,
    OracleIntegerType, OracleLongRawType, OracleLongType, OracleNClobType,
    OracleNVarChar2Type, OracleRawType, OracleSmallIntType, OracleVarChar2Type,
    OracleXmlType,
)
from rhosocial.activerecord.backend.impl.oracle.types import (
    IntervalDayToSecond, IntervalYearToMonth, OracleVector, OracleXMLType,
    SDOGeometry,
)


@pytest.fixture
def dialect() -> OracleDialect:
    return OracleDialect(version=(23, 0, 0))


class TestCoreDataTypeRendering:
    @pytest.mark.parametrize("factory,expected", [
        (lambda: IntegerType(), "NUMBER(10)"),
        (lambda: BigIntType(), "NUMBER(19)"),
        (lambda: SmallIntType(), "NUMBER(5)"),
        (lambda: TinyIntType(), "NUMBER(3)"),
        (lambda: FloatType(precision=10), "FLOAT(10)"),
        (lambda: FloatType(), "FLOAT"),
        (lambda: RealType(), "FLOAT(63)"),
        (lambda: DoubleType(), "FLOAT(126)"),
        (lambda: DecimalType(precision=10, scale=2), "NUMBER(10, 2)"),
        (lambda: DecimalType(precision=8), "NUMBER(8)"),
        (lambda: DecimalType(), "NUMBER"),
        (lambda: BooleanType(), "NUMBER(1)"),
        (lambda: VarCharType(length=100), "VARCHAR2(100)"),
        (lambda: VarCharType(None), "VARCHAR2(4000)"),
        (lambda: CharType(length=5), "CHAR(5)"),
        (lambda: CharType(None), "CHAR"),
        (lambda: TextType(), "CLOB"),
        (lambda: BlobType(), "BLOB"),
        (lambda: JsonType(), "VARCHAR2(4000)"),
        (lambda: DateTimeType(precision=6), "TIMESTAMP(6)"),
        (lambda: DateTimeType(None), "TIMESTAMP"),
        (lambda: DateType(), "DATE"),
        (lambda: TimeType(), "VARCHAR2(8)"),
        (lambda: TimestampType(precision=3), "TIMESTAMP(3)"),
        (lambda: TimestampType(None), "TIMESTAMP"),
        (lambda: TimeTzType(precision=3), "TIMESTAMP(3) WITH TIME ZONE"),
        (lambda: TimeTzType(None), "TIMESTAMP WITH TIME ZONE"),
        (lambda: TimestampTzType(precision=6), "TIMESTAMP(6) WITH TIME ZONE"),
        (lambda: TimestampTzType(None), "TIMESTAMP WITH TIME ZONE"),
    ])
    def test_rendering(self, dialect, factory, expected):
        sql, params = dialect.format_data_type(factory())
        assert sql == expected
        assert params == ()

    def test_unsupported_jsonb_raises(self, dialect):
        with pytest.raises(TypeError):
            dialect.format_data_type(JsonBType())


class TestOracleSpecificTypeRendering:
    @pytest.mark.parametrize("factory,expected", [
        (lambda: OracleIntegerType(), "NUMBER(10)"),
        (lambda: OracleSmallIntType(), "NUMBER(5)"),
        (lambda: OracleBigIntType(), "NUMBER(19)"),
        (lambda: OracleVarChar2Type(length=64), "VARCHAR2(64)"),
        (lambda: OracleVarChar2Type(None), "VARCHAR2(4000)"),
        (lambda: OracleNVarChar2Type(length=30), "NVARCHAR2(30)"),
        (lambda: OracleNVarChar2Type(None), "NVARCHAR2(2000)"),
        (lambda: OracleCharType(length=2), "CHAR(2)"),
        (lambda: OracleCharType(None), "CHAR"),
        (lambda: OracleClobType(), "CLOB"),
        (lambda: OracleNClobType(), "NCLOB"),
        (lambda: OracleLongType(), "LONG"),
        (lambda: OracleXmlType(), "XMLTYPE"),
        (lambda: OracleRawType(length=16), "RAW(16)"),
        (lambda: OracleRawType(None), "RAW(2000)"),
        (lambda: OracleLongRawType(), "LONG RAW"),
        (lambda: OracleBlobType(), "BLOB"),
    ])
    def test_rendering(self, dialect, factory, expected):
        sql, params = dialect.format_data_type(factory())
        assert sql == expected


class TestParseType:
    @pytest.mark.parametrize("raw,expected_cls", [
        ("NUMBER", DecimalType),
        ("NUMBER(7,3)", DecimalType),
        ("NUMBER(10)", OracleIntegerType),
        ("NUMBER(10,0)", OracleIntegerType),
        ("NUMBER(5)", OracleSmallIntType),
        ("NUMBER(19)", OracleBigIntType),
        ("BINARY_FLOAT", FloatType),
        ("BINARY_DOUBLE", DoubleType),
        ("RAW(2000)", OracleRawType),
        ("LONG RAW", OracleLongRawType),
        ("BLOB", OracleBlobType),
        ("VARCHAR2(128)", OracleVarChar2Type),
        ("NVARCHAR2(64)", OracleNVarChar2Type),
        ("CHAR(4)", OracleCharType),
        ("NCHAR(2)", OracleCharType),
        ("CHAR", OracleCharType),
        ("CLOB", OracleClobType),
        ("NCLOB", OracleNClobType),
        ("LONG", OracleLongType),
        ("DATE", DateType),
        ("TIMESTAMP(6)", TimestampType),
        ("TIMESTAMP", TimestampType),
        ("TIMESTAMP(3) WITH TIME ZONE", TimestampTzType),
        ("TIMESTAMP WITH LOCAL TIME ZONE", TimestampTzType),
        ("INTERVAL YEAR TO MONTH", TextType),
        ("INTERVAL DAY TO SECOND", TextType),
        ("XMLTYPE", OracleXmlType),
        ("SYS.XMLTYPE", OracleXmlType),
        ("MY_CUSTOM_TYPE", CustomType),
    ])
    def test_parsed_type_kind(self, dialect, raw, expected_cls):
        parsed = dialect.parse_type(raw)
        assert isinstance(parsed, expected_cls)

    def test_parse_preserves_precision_and_scale(self, dialect):
        parsed = dialect.parse_type("NUMBER(12, 4)")
        assert parsed.precision == 12
        assert parsed.scale == 4

    @pytest.mark.parametrize("raw,rendered", [
        ("NUMBER(10)", "NUMBER(10)"),
        ("VARCHAR2(100)", "VARCHAR2(100)"),
        ("TIMESTAMP(6)", "TIMESTAMP(6)"),
        ("XMLTYPE", "XMLTYPE"),
    ])
    def test_round_trip(self, dialect, raw, rendered):
        sql, _ = dialect.format_data_type(dialect.parse_type(raw))
        assert sql == rendered

    def test_raw_length_defaults(self, dialect):
        assert dialect.parse_type("RAW").length == 2000
        assert dialect.parse_type("VARCHAR2").length == 4000
        assert dialect.parse_type("NVARCHAR2").length == 2000

    def test_long_raw_wins_over_long_prefix(self, dialect):
        assert isinstance(dialect.parse_type("LONG RAW"), OracleLongRawType)
        assert isinstance(dialect.parse_type("LONG"), OracleLongType)


class TestBooleanDateTimeDecimalAdapters:
    def test_boolean_round_trip(self):
        adapter = OracleBooleanAdapter()
        assert adapter.to_database(True, int) == 1
        assert adapter.to_database(False, int) == 0
        assert adapter.from_database(1, bool) is True
        assert adapter.from_database(0, bool) is False

    def test_boolean_none_passthrough(self):
        assert OracleBooleanAdapter().to_database(None, int) is None

    def test_datetime_to_database_passes_object_through(self):
        value = datetime(2026, 8, 26, 12, 0, 0)
        assert OracleDateTimeAdapter().to_database(value, str) is value

    def test_datetime_from_numeric_timestamp(self):
        out = OracleDateTimeAdapter().from_database(1750000000, datetime)
        assert isinstance(out, datetime)

    def test_datetime_naive_gets_utc(self):
        naive = datetime(2026, 1, 2, 3, 4, 5)
        out = OracleDateTimeAdapter().from_database(naive, datetime)
        assert out.tzinfo is not None
        aware = datetime(2026, 1, 2, 3, 4, 5).replace(tzinfo=out.tzinfo)
        assert OracleDateTimeAdapter().from_database(aware, datetime).tzinfo is not None

    def test_datetime_iso_string(self):
        out = OracleDateTimeAdapter().from_database("2026-08-26T10:11:12", datetime)
        assert out == datetime(2026, 8, 26, 10, 11, 12)

    @pytest.mark.parametrize("raw,expected", [
        ("06-APR-26", datetime(2026, 4, 6)),
        ("06-apr-2026 14:33:36", datetime(2026, 4, 6, 14, 33, 36)),
        ("01-JAN-99", datetime(1999, 1, 1)),
    ])
    def test_datetime_oracle_format(self, raw, expected):
        out = OracleDateTimeAdapter().from_database(raw, datetime)
        assert out == expected

    def test_datetime_unparsed_string_passthrough(self):
        assert OracleDateTimeAdapter().from_database("not-a-date", datetime) == \
            "not-a-date"

    def test_date_from_datetime(self):
        out = OracleDateAdapter().from_database(datetime(2026, 8, 26, 5, 6, 7), date)
        assert out == date(2026, 8, 26)

    def test_time_round_trip(self):
        adapter = OracleTimeAdapter()
        assert adapter.to_database(time(1, 2, 3), str) == "01:02:03"
        assert adapter.from_database("01:02:03", time) == time(1, 2, 3)
        assert adapter.from_database("junk", time) == "junk"

    def test_decimal_adapter_directions(self):
        adapter = OracleDecimalAdapter()
        assert adapter.to_database(Decimal("1.5"), float) == 1.5
        assert adapter.to_database(2.25, float) == 2.25
        assert adapter.from_database(Decimal("2.50"), float) == 2.5
        assert adapter.from_database(7, float) == 7.0
        assert adapter.from_database("raw", float) == "raw"


class TestJsonBytesStringAdapters:
    def test_json_to_database_serialises(self):
        adapter = OracleJSONAdapter()
        assert adapter.to_database({"b": [1, 2]}, str) == '{"b": [1, 2]}'
        assert adapter.to_database([1, "x"], str) == '[1, "x"]'

    def test_json_parses_on_read(self):
        adapter = OracleJSONAdapter()
        assert adapter.from_database('{"k": 1}', dict) == {"k": 1}
        assert adapter.from_database("[1, 2]", list) == [1, 2]

    def test_json_keeps_string_for_str_target(self):
        adapter = OracleJSONAdapter()
        assert adapter.from_database('{"k": 1}', str) == '{"k": 1}'

    def test_json_reads_lob_like_objects(self):
        class Lob:
            def read(self) -> str:
                return '{"k": 1}'

        assert OracleJSONAdapter().from_database(Lob(), dict) == {"k": 1}

    def test_json_native_iterable_object(self):
        adapter = OracleJSONAdapter()
        assert adapter.from_database({"n": 1}, dict) == {"n": 1}
        assert adapter.from_database({"n": 1}, str) == '{"n": 1}'

    def test_bytes_round_trip_and_lob_read(self):
        adapter = OracleBytesAdapter()

        class Lob:
            def read(self) -> bytes:
                return b"zz"

        assert adapter.to_database(b"q", bytes) == b"q"
        assert adapter.from_database(b"w", bytes) == b"w"
        assert adapter.from_database(Lob(), bytes) == b"zz"

    def test_string_none_semantics(self):
        adapter = OracleStringAdapter()
        assert adapter.to_database("v", str) == "v"
        # Oracle '' is stored as NULL; non-optional str fields read back as ''
        assert adapter.from_database(None, str) == ""
        # Optional[str] preserves genuine NULL.
        assert adapter.from_database(None, Optional[str]) is None

    def test_string_json_wrapper_serialised_for_str_target(self):
        assert OracleStringAdapter().from_database(["x"], str) == '["x"]'


class TestUuidEnumAdapters:
    def test_uuid_round_trip_str_and_bytes(self):
        adapter = OracleUUIDAdapter()
        value = uuid.uuid4()
        assert adapter.to_database(value, str) == str(value)
        assert adapter.to_database(value, bytes) == value.bytes
        assert adapter.from_database(str(value), str) == value
        assert adapter.from_database(value.bytes, bytes) == value

    def test_uuid_accepts_non_uuid_input(self):
        adapter = OracleUUIDAdapter()
        value = uuid.uuid4()
        assert adapter.to_database(str(value), str) == str(value)

    def test_enum_name_storage_default(self):
        class Color(Enum):
            RED = "r"

        adapter = OracleEnumAdapter()
        assert adapter.to_database(Color.RED, str) == "RED"
        assert adapter.from_database("RED", Color) is Color.RED

    def test_enum_value_storage(self):
        class Color(Enum):
            RED = "r"

        adapter = OracleEnumAdapter(storage="value")
        assert adapter.to_database(Color.RED, str) == "r"
        assert adapter.from_database("RED", Color) is Color.RED

    def test_enum_int_target_stores_value(self):
        class Level(Enum):
            HIGH = 3

        adapter = OracleEnumAdapter()
        assert adapter.to_database(Level.HIGH, int) == 3

    def test_enum_rejects_invalid_storage(self):
        with pytest.raises(ValueError):
            OracleEnumAdapter(storage="bogus")

    def test_enum_unknown_value_passthrough(self):
        class Color(Enum):
            RED = "r"

        adapter = OracleEnumAdapter()
        assert adapter.from_database("PINK", Color) == "PINK"


class TestIntervalRowIdXmlSpatialVectorAdapters:
    def test_interval_year_to_month(self):
        adapter = OracleIntervalAdapter()
        parsed = adapter.from_database("01-06", str)
        assert isinstance(parsed, IntervalYearToMonth)
        assert (parsed.years, parsed.months) == (1, 6)
        assert adapter.to_database(parsed, str) == "01-06"

    def test_interval_day_to_second(self):
        adapter = OracleIntervalAdapter()
        parsed = adapter.from_database("5 12:30:45", str)
        assert isinstance(parsed, IntervalDayToSecond)
        assert parsed.days == 5 and parsed.hours == 12
        assert adapter.to_database(parsed, str).startswith("5 12:30:45")

    def test_rowid_routing_by_length(self):
        adapter = OracleRowIDAdapter()
        extended = adapter.from_database("AAASdqAAEAAAAInAAA", str)
        assert type(extended).__name__ == "OracleRowID"
        universal = adapter.from_database("*ABCD", str)
        assert type(universal).__name__ == "OracleURowID"

    def test_rowid_prefers_value_attribute(self):
        adapter = OracleRowIDAdapter()
        holder = SimpleNamespace(value="AAASdqAAEAAAAInAAA")
        assert adapter.to_database(holder, str) == "AAASdqAAEAAAAInAAA"
        assert adapter.to_database("plain", str) == "plain"

    def test_xml_adapter_directions(self):
        adapter = OracleXMLAdapter()
        doc = OracleXMLType("<a>1</a>")
        assert adapter.to_database(doc, str) == "<a>1</a>"
        read_back = adapter.from_database("<b/>", str)
        assert isinstance(read_back, OracleXMLType)
        assert read_back.content == "<b/>"

    def test_spatial_adapter_uses_constructor_sql(self):
        adapter = OracleSDOGeometryAdapter()
        geometry = SDOGeometry.point(10.0, 20.0)
        bound = adapter.to_database(geometry, str)
        assert bound.startswith("SDO_GEOMETRY(")
        restored = adapter.from_database(
            {
                "SDO_GTYPE": 2001,
                "SDO_SRID": None,
                "SDO_POINT": {"X": 10.0, "Y": 20.0, "Z": None},
            },
            object,
        )
        assert restored.is_point
        assert adapter.to_database("plain", str) == "plain"

    def test_vector_adapter_directions(self):
        adapter = OracleVectorAdapter()
        vector = OracleVector(dimensions=3, values=[1.0, 2.0, 3.5])
        assert adapter.to_database(vector, str) == "[1.0, 2.0, 3.5]"
        assert adapter.to_database([4, 5], str) == "[4, 5]"
        parsed = adapter.from_database("[1.0, 2.0]", str)
        assert parsed.values == [1.0, 2.0]
        assert adapter.from_database([7.0, 8.0], list).values == [7.0, 8.0]
