# src/rhosocial/activerecord/backend/impl/oracle/mixins/types.py
"""Oracle DataType formatting mixin."""

from __future__ import annotations

import re
from typing import Tuple

from rhosocial.activerecord.backend.dialect.mixins import DDLTypeMixin
from rhosocial.activerecord.backend.dialect.protocols import DDLTypeSupport
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    DateType,
    DateTimeType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    JsonType,
    JsonBType,
    RealType,
    SmallIntType,
    TextType,
    TimeType,
    TimeTzType,
    TimestampType,
    TimestampTzType,
    TinyIntType,
    VarCharType,
    DataType,
    CustomType,
)
from ..expression.types import (
    OracleBigIntType,
    OracleBlobType,
    OracleCharType,
    OracleClobType,
    OracleIntegerType,
    OracleLongRawType,
    OracleLongType,
    OracleNClobType,
    OracleNVarChar2Type,
    OracleRawType,
    OracleSmallIntType,
    OracleVarChar2Type,
    OracleXmlType,
)


class OracleTypeSupportMixin(DDLTypeMixin, DDLTypeSupport):

    # --- Core type formatters (render standard types to Oracle SQL) ---

    @DDLTypeMixin.handles(IntegerType)
    def format_data_type_integer(self, data_type: IntegerType) -> Tuple[str, tuple]:
        return "NUMBER(10)", ()

    @DDLTypeMixin.handles(BigIntType)
    def format_data_type_bigint(self, data_type: BigIntType) -> Tuple[str, tuple]:
        return "NUMBER(19)", ()

    @DDLTypeMixin.handles(SmallIntType)
    def format_data_type_smallint(self, data_type: SmallIntType) -> Tuple[str, tuple]:
        return "NUMBER(5)", ()

    @DDLTypeMixin.handles(FloatType)
    def format_data_type_float(self, data_type: FloatType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"FLOAT({data_type.precision})", ()
        return "FLOAT", ()

    @DDLTypeMixin.handles(RealType)
    def format_data_type_real(self, data_type: RealType) -> Tuple[str, tuple]:
        return "FLOAT(63)", ()

    @DDLTypeMixin.handles(DoubleType)
    def format_data_type_double(self, data_type: DoubleType) -> Tuple[str, tuple]:
        return "FLOAT(126)", ()

    @DDLTypeMixin.handles(DecimalType)
    def format_data_type_decimal(self, data_type: DecimalType) -> Tuple[str, tuple]:
        if data_type.precision is not None and data_type.scale is not None:
            return f"NUMBER({data_type.precision}, {data_type.scale})", ()
        if data_type.precision is not None:
            return f"NUMBER({data_type.precision})", ()
        return "NUMBER", ()

    @DDLTypeMixin.handles(BooleanType)
    def format_data_type_boolean(self, data_type: BooleanType) -> Tuple[str, tuple]:
        return "NUMBER(1)", ()

    @DDLTypeMixin.handles(VarCharType)
    def format_data_type_varchar(self, data_type: VarCharType) -> Tuple[str, tuple]:
        return (f"VARCHAR2({data_type.length})" if data_type.length is not None else "VARCHAR2(4000)"), ()

    @DDLTypeMixin.handles(CharType)
    def format_data_type_char(self, data_type: CharType) -> Tuple[str, tuple]:
        return (f"CHAR({data_type.length})" if data_type.length is not None else "CHAR"), ()

    @DDLTypeMixin.handles(TextType)
    def format_data_type_text(self, data_type: TextType) -> Tuple[str, tuple]:
        return "CLOB", ()

    @DDLTypeMixin.handles(BlobType)
    def format_data_type_blob(self, data_type: BlobType) -> Tuple[str, tuple]:
        return "BLOB", ()

    @DDLTypeMixin.handles(DateTimeType)
    def format_data_type_datetime(self, data_type: DateTimeType) -> Tuple[str, tuple]:
        return (f"TIMESTAMP({data_type.precision})" if data_type.precision is not None else "TIMESTAMP"), ()

    @DDLTypeMixin.handles(DateType)
    def format_data_type_date(self, data_type: DateType) -> Tuple[str, tuple]:
        return "DATE", ()

    @DDLTypeMixin.handles(TimeType)
    def format_data_type_time(self, data_type: TimeType) -> Tuple[str, tuple]:
        return "VARCHAR2(8)", ()

    @DDLTypeMixin.handles(TimestampType)
    def format_data_type_timestamp(self, data_type: TimestampType) -> Tuple[str, tuple]:
        return (f"TIMESTAMP({data_type.precision})" if data_type.precision is not None else "TIMESTAMP"), ()

    @DDLTypeMixin.handles(JsonType)
    def format_data_type_json(self, data_type: JsonType) -> Tuple[str, tuple]:
        return "VARCHAR2(4000)", ()

    # --- Oracle-specific type formatters ---
    # These give precise round-trip rendering for Oracle-only types so that
    # introspection → DataType → SQL preserves the original Oracle spelling
    # (e.g. NVARCHAR2, NCLOB, LONG, RAW, XMLType) instead of collapsing to
    # the generic core rendering.

    @DDLTypeMixin.handles(OracleIntegerType)
    def format_data_type_oracle_integer(self, data_type: OracleIntegerType) -> Tuple[str, tuple]:
        return "NUMBER(10)", ()

    @DDLTypeMixin.handles(OracleSmallIntType)
    def format_data_type_oracle_smallint(self, data_type: OracleSmallIntType) -> Tuple[str, tuple]:
        return "NUMBER(5)", ()

    @DDLTypeMixin.handles(OracleBigIntType)
    def format_data_type_oracle_bigint(self, data_type: OracleBigIntType) -> Tuple[str, tuple]:
        return "NUMBER(19)", ()

    @DDLTypeMixin.handles(OracleVarChar2Type)
    def format_data_type_oracle_varchar2(self, data_type: OracleVarChar2Type) -> Tuple[str, tuple]:
        return (f"VARCHAR2({data_type.length})" if data_type.length is not None else "VARCHAR2(4000)"), ()

    @DDLTypeMixin.handles(OracleNVarChar2Type)
    def format_data_type_oracle_nvarchar2(self, data_type: OracleNVarChar2Type) -> Tuple[str, tuple]:
        return (f"NVARCHAR2({data_type.length})" if data_type.length is not None else "NVARCHAR2(2000)"), ()

    @DDLTypeMixin.handles(OracleCharType)
    def format_data_type_oracle_char(self, data_type: OracleCharType) -> Tuple[str, tuple]:
        return (f"CHAR({data_type.length})" if data_type.length is not None else "CHAR"), ()

    @DDLTypeMixin.handles(OracleClobType)
    def format_data_type_oracle_clob(self, data_type: OracleClobType) -> Tuple[str, tuple]:
        return "CLOB", ()

    @DDLTypeMixin.handles(OracleNClobType)
    def format_data_type_oracle_nclob(self, data_type: OracleNClobType) -> Tuple[str, tuple]:
        return "NCLOB", ()

    @DDLTypeMixin.handles(OracleLongType)
    def format_data_type_oracle_long(self, data_type: OracleLongType) -> Tuple[str, tuple]:
        return "LONG", ()

    @DDLTypeMixin.handles(OracleXmlType)
    def format_data_type_oracle_xml(self, data_type: OracleXmlType) -> Tuple[str, tuple]:
        return "XMLTYPE", ()

    @DDLTypeMixin.handles(OracleRawType)
    def format_data_type_oracle_raw(self, data_type: OracleRawType) -> Tuple[str, tuple]:
        return (f"RAW({data_type.length})" if data_type.length is not None else "RAW(2000)"), ()

    @DDLTypeMixin.handles(OracleLongRawType)
    def format_data_type_oracle_long_raw(self, data_type: OracleLongRawType) -> Tuple[str, tuple]:
        return "LONG RAW", ()

    @DDLTypeMixin.handles(OracleBlobType)
    def format_data_type_oracle_blob(self, data_type: OracleBlobType) -> Tuple[str, tuple]:
        return "BLOB", ()

    # --- Oracle core-comparable type handlers ---
    # These mirror MySQL/Postgres equivalents so that callers passing a core
    # DataType (e.g. TinyIntType, TimeTzType, JsonBType, TimestampTzType)
    # get an Oracle-specific rendering instead of falling back to the base
    # class' default SQL (which is often MySQL-flavored or undefined).

    @DDLTypeMixin.handles(TinyIntType)
    def format_data_type_tinyint(self, data_type: TinyIntType) -> Tuple[str, tuple]:
        # Oracle has no native TINYINT; mapped to NUMBER(3).
        return "NUMBER(3)", ()

    @DDLTypeMixin.handles(TimeTzType)
    def format_data_type_timetz(self, data_type: TimeTzType) -> Tuple[str, tuple]:
        # Oracle supports TIMESTAMP WITH TIME ZONE; precision optional.
        return (f"TIMESTAMP({data_type.precision}) WITH TIME ZONE"
                if getattr(data_type, 'precision', None) is not None
                else "TIMESTAMP WITH TIME ZONE"), ()

    @DDLTypeMixin.handles(TimestampTzType)
    def format_data_type_timestamptz(self, data_type: TimestampTzType) -> Tuple[str, tuple]:
        return (f"TIMESTAMP({data_type.precision}) WITH TIME ZONE"
                if getattr(data_type, 'precision', None) is not None
                else "TIMESTAMP WITH TIME ZONE"), ()

    @DDLTypeMixin.handles(JsonBType)
    def format_data_type_jsonb(self, data_type: JsonBType) -> Tuple[str, tuple]:
        # Oracle has no JSONB binary JSON; 21c+ uses native JSON, otherwise CLOB.
        # We render as CLOB (best-effort round-trip on all 12c+ versions).
        return "CLOB", ()

    # --- Parsing ---

    _ORACLE_NUMBER_TYPES = re.compile(r"^(?:NUMBER|FLOAT|BINARY_FLOAT|BINARY_DOUBLE)\b", re.IGNORECASE)
    # NOTE: LONG RAW must be matched before LONG; _ORACLE_BLOB_TYPES is
    # checked ahead of _ORACLE_STRING_TYPES in parse_type() for this reason.
    _ORACLE_STRING_TYPES = re.compile(r"^(?:VARCHAR2|NVARCHAR2|CHAR|NCHAR|CLOB|NCLOB|LONG)\b", re.IGNORECASE)
    _ORACLE_BLOB_TYPES = re.compile(r"^(?:BLOB|RAW|LONG\s+RAW)\b", re.IGNORECASE)
    _ORACLE_DATE_TYPES = re.compile(r"^(?:DATE|TIMESTAMP|INTERVAL)\b", re.IGNORECASE)
    _ORACLE_XML_TYPES = re.compile(r"^(?:XMLTYPE|SYS\.XMLTYPE)\b", re.IGNORECASE)

    def parse_type(self, raw: str) -> DataType:
        stripped = raw.strip()
        upper = stripped.upper()

        if self._ORACLE_NUMBER_TYPES.match(upper):
            if upper.startswith("BINARY_FLOAT"):
                return FloatType(63)
            if upper.startswith("BINARY_DOUBLE"):
                return DoubleType()
            if upper.startswith("FLOAT"):
                nums = re.findall(r"\d+", stripped)
                if nums:
                    return FloatType(int(nums[0]))
                return FloatType()
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                p, s = int(nums[0]), int(nums[1])
                # NUMBER(10) -> INTEGER, NUMBER(5) -> SMALLINT, NUMBER(19) -> BIGINT
                if s == 0 and p == 10:
                    return OracleIntegerType()
                if s == 0 and p == 5:
                    return OracleSmallIntType()
                if s == 0 and p == 19:
                    return OracleBigIntType()
                return DecimalType(p, s)
            if len(nums) == 1:
                p = int(nums[0])
                if p == 10:
                    return OracleIntegerType()
                if p == 5:
                    return OracleSmallIntType()
                if p == 19:
                    return OracleBigIntType()
                return DecimalType(p)
            return DecimalType()

        # BLOB types checked before string types so "LONG RAW" matches the
        # binary branch rather than being consumed by the "LONG" string rule.
        if self._ORACLE_BLOB_TYPES.match(upper):
            if "LONG RAW" in upper:
                return OracleLongRawType()
            if "RAW" in upper:
                length_match = re.search(r"\((\d+)", stripped)
                length = int(length_match.group(1)) if length_match else None
                return OracleRawType(length or 2000)
            return OracleBlobType()

        if self._ORACLE_STRING_TYPES.match(upper):
            if "NCLOB" in upper:
                return OracleNClobType()
            if "CLOB" in upper:
                return OracleClobType()
            if "NVARCHAR2" in upper:
                length_match = re.search(r"\((\d+)", stripped)
                length = int(length_match.group(1)) if length_match else None
                return OracleNVarChar2Type(length or 2000)
            if "VARCHAR2" in upper:
                length_match = re.search(r"\((\d+)", stripped)
                length = int(length_match.group(1)) if length_match else None
                return OracleVarChar2Type(length or 4000)
            if "NCHAR" in upper:
                length_match = re.search(r"\((\d+)", stripped)
                length = int(length_match.group(1)) if length_match else None
                return OracleCharType(length or 1)
            if upper.startswith("LONG"):
                return OracleLongType()
            # CHAR
            length_match = re.search(r"\((\d+)", stripped)
            length = int(length_match.group(1)) if length_match else None
            return OracleCharType(length or 1)

        if self._ORACLE_DATE_TYPES.match(upper):
            if "TIMESTAMP" in upper:
                with_tz = "WITH TIME ZONE" in upper or "WITH LOCAL TIME ZONE" in upper
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if with_tz:
                    return TimestampTzType(precision)
                return TimestampType(precision)
            if "INTERVAL" in upper:
                # INTERVAL YEAR TO MONTH / DAY TO SECOND - approximated as TEXT
                # until a typed IntervalType lands in the core expression layer.
                return TextType()
            return DateType()

        if self._ORACLE_XML_TYPES.match(upper):
            return OracleXmlType()

        return CustomType(stripped)