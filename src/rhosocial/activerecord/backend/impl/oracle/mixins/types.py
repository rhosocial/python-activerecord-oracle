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
    RealType,
    SmallIntType,
    TextType,
    TimeType,
    TimestampType,
    VarCharType,
    DataType,
    CustomType,
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

    # --- Parsing ---

    _ORACLE_NUMBER_TYPES = re.compile(r"^(?:NUMBER|FLOAT|BINARY_FLOAT|BINARY_DOUBLE)\b", re.IGNORECASE)
    _ORACLE_STRING_TYPES = re.compile(r"^(?:VARCHAR2|NVARCHAR2|CHAR|NCHAR|CLOB|NCLOB)\b", re.IGNORECASE)
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
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                return DecimalType(int(nums[0]), int(nums[1]))
            if len(nums) == 1:
                return DecimalType(int(nums[0]))
            return DecimalType()

        if self._ORACLE_STRING_TYPES.match(upper):
            if "CLOB" in upper or "NCLOB" in upper:
                return TextType()
            if "VARCHAR2" in upper or "NVARCHAR2" in upper:
                length_match = re.search(r"\((\d+)", stripped)
                length = int(length_match.group(1)) if length_match else None
                return VarCharType(length or 4000)
            length_match = re.search(r"\((\d+)", stripped)
            length = int(length_match.group(1)) if length_match else None
            return CharType(length or 1)

        if self._ORACLE_BLOB_TYPES.match(upper):
            return BlobType()

        if self._ORACLE_DATE_TYPES.match(upper):
            if "TIMESTAMP" in upper:
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                return TimestampType(precision)
            return DateType()

        if self._ORACLE_XML_TYPES.match(upper):
            return TextType()

        return CustomType(stripped)