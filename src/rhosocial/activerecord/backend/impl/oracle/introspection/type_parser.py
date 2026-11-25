# src/rhosocial/activerecord/backend/impl/oracle/introspection/type_parser.py
"""Oracle's type-spelling parser (read direction, introspection-only).

Parsing a server-reported type spelling back into a ``DataType`` is not an
expression-construction concern and is not a dialect protocol member. It lives
here, under introspection, and is used only when reading back a column's
declared type. There is deliberately no ``dialect.parse_type`` shim.
"""
from __future__ import annotations

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
from .._scan import (
    contains_word,
    find_parenthesised_digit_run,
    iter_digit_runs,
    leading_word_in,
    starts_with_any_word_sequence,
    starts_with_word,
    strip_word_with_space,
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

    #: Numeric spellings Oracle reports at the head of a type string.
    _NUMBER_TYPE_WORDS = ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE")
    # ``UNSIGNED`` as a whole word anywhere in a type string. Oracle's parser
    # rejects the string outright, so this can only fire on caller-supplied DDL;
    # the point is to refuse it rather than read the attribute off and return a
    # signed type. See ``_refuse_unsigned_type_string``.
    _UNSIGNED_ATTRIBUTE_WORD = "UNSIGNED"
    # NOTE: LONG RAW must be matched before LONG; _ORACLE_BLOB_TYPES is
    # checked ahead of _ORACLE_STRING_TYPES in parse() for this reason.
    _STRING_TYPE_SEQUENCES = (("CHARACTER", "VARYING"), ("CHAR", "VARYING"))
    _STRING_TYPE_WORDS = (
        "VARCHAR2",
        "NVARCHAR2",
        "VARCHAR",
        "CHARACTER",
        "NCHAR",
        "CHAR",
        "CLOB",
        "NCLOB",
        "LONG",
    )
    _BLOB_TYPE_SEQUENCES = (("LONG", "RAW"),)
    _BLOB_TYPE_WORDS = ("BLOB", "RAW")
    _DATE_TYPE_WORDS = ("DATE", "TIMESTAMP", "INTERVAL")
    _XML_TYPE_SPELLINGS = ("XMLTYPE", "SYS.XMLTYPE")
    _BOOLEAN_TYPE_WORDS = ("BOOLEAN", "BOOL")

    #: Oracle's documented default size for a ``CHAR``/``NCHAR`` declared
    #: without one ("The default value is 1"), measured as ``DATA_LENGTH = 1``.
    _CHAR_DEFAULT_LENGTH = 1

    def __init__(self, dialect):
        self.dialect = dialect

    # --- helpers -------------------------------------------------------

    @staticmethod
    def _oracle_declared_length(stripped: str) -> Optional[int]:
        """The ``(n)`` Oracle wrote after a type name, or ``None``."""
        digits = find_parenthesised_digit_run(stripped)
        return int(digits) if digits else None

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

        if contains_word(stripped, self._UNSIGNED_ATTRIBUTE_WORD):
            self._refuse_unsigned_type_string(stripped)

        if leading_word_in(upper, self._NUMBER_TYPE_WORDS):
            if upper.startswith("BINARY_FLOAT"):
                return OracleBinaryFloatType(dialect=d)
            if upper.startswith("BINARY_DOUBLE"):
                return OracleBinaryDoubleType(dialect=d)
            if upper.startswith("FLOAT"):
                nums = iter_digit_runs(stripped)
                if nums:
                    return FloatType(dialect=d, precision=int(nums[0]))
                return FloatType(dialect=d)
            nums = iter_digit_runs(stripped)
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
        if starts_with_any_word_sequence(upper, self._BLOB_TYPE_SEQUENCES) or leading_word_in(
            upper, self._BLOB_TYPE_WORDS
        ):
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

        if starts_with_any_word_sequence(upper, self._STRING_TYPE_SEQUENCES) or leading_word_in(
            upper, self._STRING_TYPE_WORDS
        ):
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

        if leading_word_in(upper, self._BOOLEAN_TYPE_WORDS):
            return BooleanType(dialect=d)

        if leading_word_in(upper, self._DATE_TYPE_WORDS):
            if "TIMESTAMP" in upper:
                nums = iter_digit_runs(stripped)
                precision = int(nums[0]) if nums else None
                if "WITH LOCAL TIME ZONE" in upper:
                    return OracleTimestampLtzType(dialect=d, precision=precision)
                if "WITH TIME ZONE" in upper:
                    return TimestampTzType(dialect=d, precision=precision)
                return TimestampType(dialect=d, precision=precision)
            if "INTERVAL" in upper:
                stripped_interval = strip_word_with_space(stripped, "INTERVAL")
                fields = (stripped_interval if stripped_interval is not None else stripped).strip()
                return IntervalType(dialect=d, fields=fields or None)
            return DateType(dialect=d)

        if any(starts_with_word(upper, spelling) for spelling in self._XML_TYPE_SPELLINGS):
            return XmlType(dialect=d)

        return CustomType(dialect=d, raw=stripped)


__all__ = ["OracleTypeParser"]
