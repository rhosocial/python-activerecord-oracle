# src/rhosocial/activerecord/backend/impl/oracle/introspection/type_parser.py
"""Oracle's type-spelling parser (read direction, introspection-only).

Parsing a server-reported type spelling back into a ``DataType`` is not an
expression-construction concern and is not a dialect protocol member. It lives
here, under introspection, and is used only when reading back a column's
declared type. There is deliberately no ``dialect.parse_type`` shim.
"""
from __future__ import annotations

import re
from typing import Optional

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    CustomType,
    DataType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    IntervalType,
    SmallIntType,
    TextType,
    TimestampTzType,
    TimestampType,
    VarCharType,
    XmlType,
)
from ..expression.types import (
    OracleBinaryDoubleType,
    OracleBinaryFloatType,
    OracleLongRawType,
    OracleLongType,
    OracleNClobType,
    OracleNVarChar2Type,
    OracleRawType,
    OracleTimestampLtzType,
)


class OracleTypeParser:
    """Parse a raw Oracle type spelling into a ``DataType``, bound to a dialect."""

    _ORACLE_NUMBER_TYPES = re.compile(
        r"^(?:NUMBER|FLOAT|BINARY_FLOAT|BINARY_DOUBLE)\b", re.IGNORECASE
    )
    # ``UNSIGNED`` as a whole word anywhere in a type string. Oracle's parser
    # rejects the string outright, so this can only fire on caller-supplied DDL;
    # the point is to refuse it rather than read the attribute off and return a
    # signed type. See ``_refuse_unsigned_type_string``.
    _UNSIGNED_ATTRIBUTE = re.compile(r"\bUNSIGNED\b", re.IGNORECASE)
    # NOTE: LONG RAW must be matched before LONG; _ORACLE_BLOB_TYPES is
    # checked ahead of _ORACLE_STRING_TYPES in parse() for this reason.
    _ORACLE_STRING_TYPES = re.compile(
        r"^(?:CHARACTER\s+VARYING|CHAR\s+VARYING|VARCHAR2|NVARCHAR2|VARCHAR|"
        r"CHARACTER|NCHAR|CHAR|CLOB|NCLOB|LONG)\b",
        re.IGNORECASE,
    )
    _ORACLE_BLOB_TYPES = re.compile(r"^(?:BLOB|RAW|LONG\s+RAW)\b", re.IGNORECASE)
    _ORACLE_DATE_TYPES = re.compile(r"^(?:DATE|TIMESTAMP|INTERVAL)\b", re.IGNORECASE)
    _ORACLE_XML_TYPES = re.compile(r"^(?:XMLTYPE|SYS\.XMLTYPE)\b", re.IGNORECASE)
    _ORACLE_BOOLEAN_TYPES = re.compile(r"^(?:BOOLEAN|BOOL)\b", re.IGNORECASE)

    #: Oracle's documented default size for a ``CHAR``/``NCHAR`` declared
    #: without one ("The default value is 1"), measured as ``DATA_LENGTH = 1``.
    _CHAR_DEFAULT_LENGTH = 1

    def __init__(self, dialect):
        self.dialect = dialect

    # --- helpers -------------------------------------------------------

    @staticmethod
    def _oracle_declared_length(stripped: str) -> Optional[int]:
        """The ``(n)`` Oracle wrote after a type name, or ``None``."""
        match = re.search(r"\((\d+)", stripped)
        return int(match.group(1)) if match else None

    def _require_declared_length(self, stripped: str, word: str, example: str) -> int:
        """The declared ``(n)``, or a refusal naming Oracle's requirement."""
        length = self._oracle_declared_length(stripped)
        if length is not None:
            return length
        raise ValueError(
            f"cannot parse an un-sized {word} (Oracle requires a size: "
            f"'You must specify size for a {word} value', and the server "
            f"rejects a bare {word} with ORA-00906 — a bare CHAR is different, "
            f"Oracle documents a default size of "
            f"{self._CHAR_DEFAULT_LENGTH} for it); pass the width, e.g. "
            f"'{example}'"
        )

    def _refuse_unsigned_type_string(self, raw: str) -> None:
        """Refuse a type string that carries an ``UNSIGNED`` attribute."""
        raise UnsupportedFeatureError(
            self.dialect.name,
            f"an UNSIGNED attribute in a type string ({raw!r}) "
            f"(Oracle has no unsigned numeric type, so no declaration and no "
            f"catalog row can carry UNSIGNED: CREATE TABLE's column_definition "
            f"admits no modifier after the type name, the reference chapter "
            f"says the numeric types 'store positive and negative fixed and "
            f"floating-point numbers', and the parser rejects the string at the "
            f"word itself -- ORA-00907: missing right parenthesis on 21c, "
            f"ORA-03062: missing comma or right parenthesis on 26ai Free. "
            f"Reading the attribute off and returning a signed type would compare "
            f"equal to the signed column, which is the silent loss this refuses.)",
            suggestion=(
                "Declare the column signed and enforce the range with a CHECK "
                "constraint; Oracle accepts 'c NUMBER(10,2) CHECK (c >= 0)'."
            ),
        )

    # --- parser ---------------------------------------------------------

    def parse(self, raw: str) -> DataType:
        d = self.dialect
        stripped = raw.strip()
        upper = stripped.upper()

        if self._UNSIGNED_ATTRIBUTE.search(stripped):
            self._refuse_unsigned_type_string(stripped)

        if self._ORACLE_NUMBER_TYPES.match(upper):
            if upper.startswith("BINARY_FLOAT"):
                return OracleBinaryFloatType(dialect=d)
            if upper.startswith("BINARY_DOUBLE"):
                return OracleBinaryDoubleType(dialect=d)
            if upper.startswith("FLOAT"):
                nums = re.findall(r"\d+", stripped)
                if nums:
                    return FloatType(dialect=d, precision=int(nums[0]))
                return FloatType(dialect=d)
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                p, s = int(nums[0]), int(nums[1])
                if s == 0 and p == 10:
                    return IntegerType(dialect=d)
                if s == 0 and p == 5:
                    return SmallIntType(dialect=d)
                if s == 0 and p == 19:
                    return BigIntType(dialect=d)
                return DecimalType(dialect=d, precision=p, scale=s)
            if len(nums) == 1:
                p = int(nums[0])
                if p == 10:
                    return IntegerType(dialect=d)
                if p == 5:
                    return SmallIntType(dialect=d)
                if p == 19:
                    return BigIntType(dialect=d)
                return DecimalType(dialect=d, precision=p)
            return DecimalType(dialect=d)

        # BLOB types checked before string types so "LONG RAW" matches the
        # binary branch rather than being consumed by the "LONG" string rule.
        if self._ORACLE_BLOB_TYPES.match(upper):
            if "LONG RAW" in upper:
                return OracleLongRawType(dialect=d)
            if "RAW" in upper:
                length = self._oracle_declared_length(stripped)
                if length is None:
                    raise ValueError(
                        "cannot parse an un-sized RAW (Oracle requires a size: "
                        "'You must specify size for a RAW value', and the "
                        "server rejects a bare RAW with ORA-00906); pass the "
                        "width, e.g. 'RAW(16)'"
                    )
                return OracleRawType(dialect=d, length=length)
            return BlobType(dialect=d)

        if self._ORACLE_STRING_TYPES.match(upper):
            if "NCLOB" in upper:
                return OracleNClobType(dialect=d)
            if "CLOB" in upper:
                return TextType(dialect=d, spelling="clob")
            if upper.startswith("NVARCHAR2"):
                return OracleNVarChar2Type(
                    dialect=d,
                    length=self._require_declared_length(
                        stripped, "NVARCHAR2", "NVARCHAR2(30)"
                    ),
                )
            if upper.startswith(("VARCHAR2", "VARCHAR", "CHARACTER VARYING", "CHAR VARYING")):
                return VarCharType(
                    dialect=d,
                    length=self._require_declared_length(
                        stripped, "VARCHAR2", "VARCHAR2(4000)"
                    ),
                )
            if upper.startswith("LONG"):
                return OracleLongType(dialect=d)
            return CharType(
                dialect=d,
                length=self._oracle_declared_length(stripped) or self._CHAR_DEFAULT_LENGTH,
            )

        if self._ORACLE_BOOLEAN_TYPES.match(upper):
            return BooleanType(dialect=d)

        if self._ORACLE_DATE_TYPES.match(upper):
            if "TIMESTAMP" in upper:
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH LOCAL TIME ZONE" in upper:
                    return OracleTimestampLtzType(dialect=d, precision=precision)
                if "WITH TIME ZONE" in upper:
                    return TimestampTzType(dialect=d, precision=precision)
                return TimestampType(dialect=d, precision=precision)
            if "INTERVAL" in upper:
                fields = re.sub(r"^INTERVAL\s+", "", stripped, flags=re.IGNORECASE).strip()
                return IntervalType(dialect=d, fields=fields or None)
            return DateType(dialect=d)

        if self._ORACLE_XML_TYPES.match(upper):
            return XmlType(dialect=d)

        return CustomType(dialect=d, raw=stripped)


__all__ = ["OracleTypeParser"]
