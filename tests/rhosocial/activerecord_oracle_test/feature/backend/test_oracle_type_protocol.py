# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_type_protocol.py
"""Oracle type-protocol compliance tests.

Covers:
- format_data_type_* / supports_data_type_* 1:1 correspondence
- supports_data_types() includes oracle_* and core types
- suggested_data_types() class types, disjoint keys
- dialect_options forwarding and equality
- Precision validation (NUMBER, FLOAT, TIMESTAMP)
"""
import re

import pytest

from rhosocial.activerecord.backend.expression.types import (
    BigIntType, BlobType, BooleanType, CharType, DateType, DateTimeType,
    DecimalType, DoubleType, FloatType, IntegerType, JsonBType, JsonType,
    RealType, SmallIntType, TextType, TimeTzType, TimestampTzType,
    TinyIntType, VarCharType,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.types import (
    OracleBigIntType, OracleBlobType, OracleCharType, OracleClobType,
    OracleIntegerType, OracleLongRawType, OracleLongType, OracleNClobType,
    OracleNVarChar2Type, OracleRawType, OracleSmallIntType, OracleVarChar2Type,
    OracleXmlType,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


# --- W2: format/supports 1:1 correspondence ---

class TestFormatSupportsOneToOne:
    """Every format_data_type_<name> must have a supports_data_type_<name>."""

    def test_all_format_methods_have_support_methods(self, dialect):
        fmt = set()
        sup = set()
        for attr in dir(type(dialect)):
            m = re.match(r"^format_data_type_([a-z][a-z0-9_]*)$", attr)
            if m:
                fmt.add(m.group(1))
            m = re.match(r"^supports_data_type_([a-z][a-z0-9_]*)$", attr)
            if m:
                sup.add(m.group(1))
        assert fmt == sup, (
            f"format-only: {fmt - sup}\n"
            f"supports-only: {sup - fmt}"
        )

    def test_no_always_false_supports(self, dialect):
        """No supports_data_type_* should unconditionally return False."""
        for attr in dir(type(dialect)):
            m = re.match(r"^supports_data_type_([a-z][a-z0-9_]*)$", attr)
            if m:
                method = getattr(dialect, attr)
                assert method() is True, f"{attr}() returned False"


# --- W2: supports_data_types() coverage ---

class TestSupportsDataTypes:
    def test_includes_core_types(self, dialect):
        supported = dialect.supports_data_types()
        expected_core = [
            "integer", "bigint", "smallint", "float", "real", "double",
            "decimal", "boolean", "varchar", "char", "text", "blob",
            "datetime", "date", "time", "timestamp", "json",
            "tinyint", "timetz", "timestamptz", "jsonb",
        ]
        for name in expected_core:
            assert name in supported, f"core type {name!r} missing from supports_data_types()"

    def test_includes_oracle_types(self, dialect):
        supported = dialect.supports_data_types()
        expected_oracle = [
            "oracle_integer", "oracle_smallint", "oracle_bigint",
            "oracle_varchar2", "oracle_nvarchar2", "oracle_char",
            "oracle_clob", "oracle_nclob", "oracle_long", "oracle_xml",
            "oracle_raw", "oracle_long_raw", "oracle_blob",
        ]
        for name in expected_oracle:
            assert name in supported, f"oracle type {name!r} missing from supports_data_types()"

    def test_values_are_data_type_classes(self, dialect):
        supported = dialect.supports_data_types()
        for name, cls in supported.items():
            assert isinstance(cls, type), f"{name}: value is not a class"
            assert hasattr(cls, "name"), f"{name}: class has no name attribute"


# --- W3: suggested_data_types() ---

class TestSuggestedDataTypes:
    def test_returns_dict(self, dialect):
        result = dialect.suggested_data_types()
        assert isinstance(result, dict)

    def test_keys_disjoint_from_supported(self, dialect):
        supported_keys = set(dialect.supports_data_types().keys())
        suggested_keys = set(dialect.suggested_data_types().keys())
        overlap = supported_keys & suggested_keys
        assert not overlap, f"overlap between supported and suggested: {overlap}"

    def test_values_are_classes(self, dialect):
        for name, cls in dialect.suggested_data_types().items():
            assert isinstance(cls, type), f"{name}: value is not a class"


# --- W1 + W4: dialect_options forwarding and equality ---

class TestDialectOptionsForwarding:
    def test_oracle_raw_type_forwards_dialect_options(self):
        opts = {"unsigned": True}
        t = OracleRawType(length=10, dialect_options=opts)
        assert t.dialect_options == {"unsigned": True}

    def test_oracle_raw_type_equality_ignores_dialect(self):
        t1 = OracleRawType(length=16)
        t2 = OracleRawType(length=16)
        assert t1 == t2

    def test_oracle_raw_type_inequality_on_length(self):
        t1 = OracleRawType(length=16)
        t2 = OracleRawType(length=32)
        assert t1 != t2

    def test_oracle_raw_type_hash_consistency(self):
        t1 = OracleRawType(length=16)
        t2 = OracleRawType(length=16)
        assert hash(t1) == hash(t2)

    def test_oracle_raw_type_equality_with_dialect_options(self):
        t1 = OracleRawType(length=16, dialect_options={"a": 1})
        t2 = OracleRawType(length=16, dialect_options={"a": 1})
        assert t1 == t2

    def test_oracle_raw_type_inequality_on_dialect_options(self):
        t1 = OracleRawType(length=16, dialect_options={"a": 1})
        t2 = OracleRawType(length=16, dialect_options={"a": 2})
        assert t1 != t2

    def test_oracle_varchar2_dialect_options_forwarded(self):
        t = VarCharType(dialect_options={"charset": "utf8"})
        assert t.dialect_options == {"charset": "utf8"}


# --- W4: Precision validation ---

class TestPrecisionValidation:
    def test_float_precision_valid(self, dialect):
        sql, _ = dialect.format_data_type(FloatType(precision=63))
        assert sql == "FLOAT(63)"

    def test_float_precision_too_low(self, dialect):
        with pytest.raises(ValueError, match="1-126"):
            dialect.format_data_type(FloatType(precision=0))

    def test_float_precision_too_high(self, dialect):
        with pytest.raises(ValueError, match="1-126"):
            dialect.format_data_type(FloatType(precision=127))

    def test_decimal_precision_valid(self, dialect):
        sql, _ = dialect.format_data_type(DecimalType(precision=38, scale=127))
        assert sql == "NUMBER(38, 127)"

    def test_decimal_precision_too_high(self, dialect):
        with pytest.raises(ValueError, match="1-38"):
            dialect.format_data_type(DecimalType(precision=39))

    def test_decimal_precision_too_low(self, dialect):
        with pytest.raises(ValueError, match="1-38"):
            dialect.format_data_type(DecimalType(precision=0))

    def test_decimal_scale_too_low(self, dialect):
        with pytest.raises(ValueError, match="-84 to 127"):
            dialect.format_data_type(DecimalType(precision=10, scale=-85))

    def test_decimal_scale_too_high(self, dialect):
        with pytest.raises(ValueError, match="-84 to 127"):
            dialect.format_data_type(DecimalType(precision=10, scale=128))

    def test_decimal_no_params_ok(self, dialect):
        sql, _ = dialect.format_data_type(DecimalType())
        assert sql == "NUMBER"

    def test_timestamp_precision_valid(self, dialect):
        sql, _ = dialect.format_data_type(DateTimeType(precision=9))
        assert sql == "TIMESTAMP(9)"

    def test_timestamp_precision_zero(self, dialect):
        sql, _ = dialect.format_data_type(DateTimeType(precision=0))
        assert sql == "TIMESTAMP(0)"

    def test_timestamp_precision_too_high(self, dialect):
        with pytest.raises(ValueError, match="0-9"):
            dialect.format_data_type(DateTimeType(precision=10))

    def test_timestamp_precision_negative(self, dialect):
        with pytest.raises(ValueError, match="0-9"):
            dialect.format_data_type(DateTimeType(precision=-1))

    def test_timestamptz_precision_valid(self, dialect):
        sql, _ = dialect.format_data_type(DateTimeType(precision=6))
        assert sql == "TIMESTAMP(6)"

    def test_timestamp_type_precision_valid(self, dialect):
        sql, _ = dialect.format_data_type(
            __import__(
                "rhosocial.activerecord.backend.expression.types",
                fromlist=["TimestampType"]
            ).TimestampType(precision=3)
        )
        assert sql == "TIMESTAMP(3)"
