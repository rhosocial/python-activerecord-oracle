# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_types_adapters.py
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
    DateTimeType, DecimalType, DoubleType, FloatType, IntegerType,
    IntervalType, JsonType, JsonBType, RealType, SmallIntType, TextType,
    TimeType, TimeTzType, TimestampType, TimestampTzType, TinyIntType,
    VarBinaryType, VarCharType, XmlType,
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
    OracleBinaryDoubleType, OracleBinaryFloatType, OracleLongRawType,
    OracleLongType, OracleNClobType, OracleNVarChar2Type, OracleRawType,
    OracleTimestampLtzType,
)
from rhosocial.activerecord.backend.impl.oracle.type_values import (
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
        # A bare ``float`` states Oracle's documented default binary precision
        # rather than leaving the word alone, so the declaration matches the
        # catalog row (which reports DATA_PRECISION = 126). See
        # test_oracle_type_protocol.py::TestFloatSpellingsAgainstOraclesAnsiTable.
        (lambda: FloatType(), "FLOAT(126)"),
        (lambda: RealType(), "FLOAT(63)"),
        (lambda: DoubleType(), "FLOAT(126)"),
        (lambda: DecimalType(precision=10, scale=2), "NUMBER(10, 2)"),
        (lambda: DecimalType(precision=8), "NUMBER(8)"),
        (lambda: DecimalType(), "NUMBER"),
        # ``boolean`` is the one concept whose rendering is version-gated: the
        # native ``BOOLEAN`` column type arrived in 23ai and this fixture is a
        # 23.x dialect, so it renders BOOLEAN. See
        # TestNativeBooleanColumnType in test_oracle_type_protocol.py for both
        # sides and the measurement behind the boundary.
        (lambda: BooleanType(), "BOOLEAN"),
        (lambda: VarCharType(length=100), "VARCHAR2(100)"),
        # ``json`` and ``jsonb`` are the other two concepts whose rendering is
        # version-gated: the native ``JSON`` column type arrived in 21c and
        # this fixture is a 23.x dialect, so both render ``JSON``. Below 21c
        # they render ``VARCHAR2(4000)`` and ``CLOB`` — see
        # TestNativeJsonColumnType in test_oracle_type_protocol.py for both
        # sides and the measurement behind the boundary.
        (lambda: JsonType(), "JSON"),
        (lambda: JsonBType(), "JSON"),
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


class TestOracleSpecificTypeRendering:
    """The eight ``oracle_``-prefixed dispatch keys, and only those.

    A prefixed key is a claim that Oracle has a type the core hierarchy has no
    name for, so this list is short on purpose: ``NVARCHAR2`` (national
    character set), ``NCLOB`` (its unbounded form), the two deprecated ``LONG``
    forms, ``RAW(n)`` (a bounded byte string, which core ``varbinary`` is
    *substituted* for rather than rendered), the two IEEE floating-point words,
    and ``TIMESTAMP WITH LOCAL TIME ZONE`` (whose core sibling renders
    ``WITH TIME ZONE``). Six classes that rendered exactly what their core
    parent renders were deleted; the rendering they produced is asserted
    through the core classes in :class:`TestCoreDataTypeRendering` above.
    """

    @pytest.mark.parametrize("factory,expected", [
        (lambda: OracleNVarChar2Type(length=30), "NVARCHAR2(30)"),
        (lambda: OracleNVarChar2Type(None), "NVARCHAR2(2000)"),
        (lambda: OracleNClobType(), "NCLOB"),
        (lambda: OracleLongType(), "LONG"),
        (lambda: OracleRawType(length=16), "RAW(16)"),
        # No width declared: OracleRawType defaults to 16 (UUID()'s own width)
        # rather than inventing Oracle's 2000-byte STANDARD ceiling, which
        # made every suggested UUID column a 2 KB binary column.
        (lambda: OracleRawType(None), "RAW(16)"),
        (lambda: OracleRawType(length=64), "RAW(64)"),
        (lambda: OracleLongRawType(), "LONG RAW"),
        # The two IEEE words are parameterless and render themselves, not the
        # NUMBER-based FLOAT(63)/FLOAT(126) core float/double write (F.4-4).
        (lambda: OracleBinaryFloatType(), "BINARY_FLOAT"),
        (lambda: OracleBinaryDoubleType(), "BINARY_DOUBLE"),
        # WITH LOCAL TIME ZONE is its own word, not "WITH TIME ZONE": the
        # local form stores no offset (finding F.4-3).
        (lambda: OracleTimestampLtzType(None), "TIMESTAMP WITH LOCAL TIME ZONE"),
        (lambda: OracleTimestampLtzType(precision=6),
         "TIMESTAMP(6) WITH LOCAL TIME ZONE"),
    ])
    def test_rendering(self, dialect, factory, expected):
        sql, params = dialect.format_data_type(factory())
        assert sql == expected

    @pytest.mark.parametrize("raw,rendered", [
        # The four classes the datatype-hierarchy re-parenting touched. The
        # SQL they write must be byte-identical to what it was before: the
        # change was to what they *are*, not to what they say.
        ("CHAR(2)", "CHAR(2)"),
        ("NCLOB", "NCLOB"),
        ("LONG", "LONG"),
        ("RAW(16)", "RAW(16)"),
    ])
    def test_reparented_types_render_exactly_as_before(self, dialect, raw, rendered):
        parsed = dialect.parse_type(raw)
        assert dialect.format_data_type(parsed)[0] == rendered
        assert dialect.format_data_type(parsed)[1] == ()

    def test_clob_is_the_clob_spelling_of_text_not_a_class(self, dialect):
        """CLOB is the standard's name for unbounded text, so it is a
        *spelling* of TextType. It renders CLOB and there is no separate
        Oracle class for it."""
        parsed = dialect.parse_type("CLOB")
        assert type(parsed) is TextType
        assert parsed.spelling == "clob"
        assert dialect.format_data_type(parsed)[0] == "CLOB"

    def test_xmltype_is_the_xml_concept_not_text(self, dialect):
        """Oracle's XMLType stores an infoset, so it is the XML concept and
        explicitly *not* a TextType — the opposite of what the old
        OracleXmlType(TextType) claimed."""
        parsed = dialect.parse_type("XMLTYPE")
        assert type(parsed) is XmlType
        assert not isinstance(parsed, TextType)
        assert dialect.format_data_type(parsed)[0] == "XMLTYPE"


class TestOracleTimestampLtzIsNotTheCoreTzType:
    """``WITH LOCAL TIME ZONE`` and ``WITH TIME ZONE`` are different columns.

    ``WITH TIME ZONE`` stores the offset with each value; ``WITH LOCAL TIME
    ZONE`` stores none and normalises to the database time zone.  The catalog
    reports the two words separately on 18.4 XE, 21.3 XE and 26ai Free
    23.26.1 (``'TIMESTAMP(6) WITH LOCAL TIME ZONE'`` versus
    ``'TIMESTAMP(6) WITH TIME ZONE'``), so parse must keep them apart: before
    this class the LOCAL word parsed to ``TimestampTzType`` and re-rendered
    without ``LOCAL``, i.e. a write-back silently changed the column (finding
    F.4-3).  The bare declaration versus the ``TIMESTAMP(6) …`` catalog row is
    still unequal; that server-default question is F.4-5 and is not addressed
    here.
    """

    def test_parse_routes_each_word_to_its_own_class(self, dialect):
        assert type(dialect.parse_type(
            "TIMESTAMP WITH LOCAL TIME ZONE")) is OracleTimestampLtzType
        assert type(dialect.parse_type(
            "TIMESTAMP WITH TIME ZONE")) is TimestampTzType

    def test_the_local_word_round_trips_byte_identically(self, dialect):
        raw = "TIMESTAMP(6) WITH LOCAL TIME ZONE"
        assert dialect.format_data_type(dialect.parse_type(raw))[0] == raw

    def test_a_declared_local_column_equals_the_introspected_one(self, dialect):
        assert OracleTimestampLtzType(dialect, precision=6) == (
            dialect.parse_type("TIMESTAMP(6) WITH LOCAL TIME ZONE"))

    def test_local_and_non_local_are_not_equal(self, dialect):
        local = dialect.parse_type("TIMESTAMP(6) WITH LOCAL TIME ZONE")
        plain = dialect.parse_type("TIMESTAMP(6) WITH TIME ZONE")
        assert local != plain
        assert OracleTimestampLtzType(dialect, precision=6) != (
            TimestampTzType(dialect, precision=6))

    def test_local_is_a_timestamptz_concept(self, dialect):
        assert isinstance(OracleTimestampLtzType(dialect), TimestampTzType)


class TestOracleBinaryIeeeTypesAreNotTheNumberFamily:
    """``BINARY_FLOAT``/``BINARY_DOUBLE`` are IEEE storage, not ``NUMBER``.

    ``FLOAT(p)`` is "a subtype of the NUMBER data type", while the two
    ``BINARY_*`` words are 4/8-byte IEEE 754 values; the catalog reports them
    separately — ``DATA_TYPE='BINARY_FLOAT'`` with ``DATA_LENGTH=4`` versus
    ``'FLOAT'`` with ``DATA_PRECISION`` — measured on 18.4 XE, 21.3 XE and
    26ai Free 23.26.1.  Before these classes parse answered ``FloatType(63)``
    / ``DoubleType`` and re-rendered ``FLOAT(63)``/``FLOAT(126)``, i.e. a
    different storage type, and object equality made the differ unable to
    tell the two columns apart (finding F.4-4).
    """

    def test_parse_routes_the_ieee_words_to_their_own_classes(self, dialect):
        assert type(dialect.parse_type("BINARY_FLOAT")) is OracleBinaryFloatType
        assert type(dialect.parse_type("BINARY_DOUBLE")) is OracleBinaryDoubleType

    @pytest.mark.parametrize("word,klass", [
        ("BINARY_FLOAT", OracleBinaryFloatType),
        ("BINARY_DOUBLE", OracleBinaryDoubleType),
    ])
    def test_the_word_round_trips_byte_identically(self, dialect, word, klass):
        parsed = dialect.parse_type(word)
        assert type(parsed) is klass
        assert dialect.format_data_type(parsed)[0] == word

    def test_no_parameters_and_equality_by_class(self, dialect):
        assert OracleBinaryFloatType.PARAMETERS == ()
        assert OracleBinaryDoubleType.PARAMETERS == ()
        assert dialect.parse_type("BINARY_FLOAT") == OracleBinaryFloatType(dialect)
        assert dialect.parse_type("BINARY_DOUBLE") == OracleBinaryDoubleType(dialect)

    def test_the_ieee_classes_are_not_the_number_family(self, dialect):
        assert dialect.parse_type("BINARY_FLOAT") != FloatType(dialect, precision=63)
        assert dialect.parse_type("BINARY_FLOAT") != FloatType(dialect)
        assert dialect.parse_type("BINARY_DOUBLE") != DoubleType(dialect)
        assert OracleBinaryFloatType(dialect) != FloatType(dialect)
        assert OracleBinaryDoubleType(dialect) != DoubleType(dialect)


class TestOracleCharIsFixedLength:
    """Oracle ``CHAR`` is blank-padded and ``VARCHAR2`` is not, so the two are
    different concepts and the *core* classes say so (D6). The Oracle-namespaced
    subclasses that used to carry that distinction — ``OracleCharType`` /
    ``OracleVarChar2Type`` — are gone: both rendered byte for byte what
    ``format_data_type_char`` / ``format_data_type_varchar`` write, and because
    ``DataType.__eq__`` is class identity they made a declared column and the
    catalog row it produced compare unequal. The distinction itself is intact,
    because ``CharType`` and ``VarCharType`` are separate core concepts."""

    def test_char_renders_oracles_fixed_length_word(self, dialect):
        assert dialect.format_data_type(CharType(dialect, length=8))[0] == "CHAR(8)"

    def test_varchar_renders_oracles_variable_length_word(self, dialect):
        assert dialect.format_data_type(
            VarCharType(dialect, length=8))[0] == "VARCHAR2(8)"

    def test_the_two_core_concepts_are_not_interchangeable(self):
        assert not isinstance(CharType(length=8), VarCharType)
        assert CharType(dialect=OracleDialect(version=(23, 0, 0)),
                        length=8) != VarCharType(
            dialect=OracleDialect(version=(23, 0, 0)), length=8)

    def test_parse_keeps_fixed_and_variable_apart(self, dialect):
        assert type(dialect.parse_type("CHAR(10)")) is CharType
        assert type(dialect.parse_type("VARCHAR2(10)")) is VarCharType
        assert dialect.parse_type("CHAR(10)") != dialect.parse_type("VARCHAR2(10)")

    def test_a_declared_column_equals_the_introspected_one(self, dialect):
        for klass, raw in ((CharType, "CHAR(10)"), (VarCharType, "VARCHAR2(10)")):
            assert klass(dialect, length=10) == dialect.parse_type(raw), raw


class TestBlobIsTheCoreConcept:
    """``BLOB`` is the word ``format_data_type_blob`` writes, so core
    ``BlobType`` is what ``parse_type`` returns and what ``suggested_data_types``
    names for ``varbinary``. The ``OracleBlobType`` that used to sit between them
    rendered identical SQL under a prefixed name — and a prefixed name with
    nothing behind it is the same defect this file's other deletions removed."""

    def test_blob_parses_to_the_core_class(self, dialect):
        assert type(dialect.parse_type("BLOB")) is BlobType

    def test_a_declared_blob_equals_the_introspected_one(self, dialect):
        assert BlobType(dialect) == dialect.parse_type("BLOB")

    def test_long_raw_is_still_namespaced(self, dialect):
        """``LONG RAW`` is *not* a spelling of ``BLOB``: Oracle tells you to
        convert it, so a column declared that way has to be written back that
        way, and no core formatter writes ``LONG RAW``."""
        parsed = dialect.parse_type("LONG RAW")
        assert isinstance(parsed, OracleLongRawType)
        assert parsed != BlobType(dialect)
        assert dialect.format_data_type(parsed)[0] == "LONG RAW"


class TestOracleRawIsABoundedByteString:
    """``RAW(n)`` is Oracle's byte string with a declared maximum, so it is the
    core ``VarBinaryType`` — not the unbounded ``BlobType``, which lives outside
    the row behind a locator. Oracle's own words for the kind are worth quoting
    because they are the reason the two are not interchangeable here: "``RAW``
    is a variable-length data type like ``VARCHAR2``", and a ``BLOB`` "can store
    binary data up to (4 gigabytes - 1) * (the value of the CHUNK parameter of
    LOB storage)".

    ``n`` bounds the value without fixing it: ``RAW`` does not pad, and that is
    measured rather than assumed — ``DUMP`` of a ``RAW(16)`` holding eight bytes
    reports ``Typ=23 Len=8`` on both wired servers, not ``Len=16``.
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    """

    def test_raw_is_a_varbinary_type(self):
        assert isinstance(OracleRawType(length=16), VarBinaryType)

    def test_raw_is_not_a_blob_type(self):
        assert not isinstance(OracleRawType(length=16), BlobType)

    def test_blob_and_long_raw_stay_blobs(self):
        assert isinstance(OracleLongRawType(), BlobType)
        assert not isinstance(OracleLongRawType(), VarBinaryType)
        assert not isinstance(OracleRawType(length=16), BlobType)


class TestRawWidthIsReadNotGuessed:
    """``RAW(n)``'s width is the whole identity of the column — it is in
    ``OracleRawType.PARAMETERS``, so two ``RAW`` columns of different widths are
    different columns — and ``parse_type`` used to invent one.

    The gap it invented it for is closed: the introspector now spells the size
    out for ``RAW`` (``_parse_columns`` reads ``DATA_LENGTH``, which holds the
    declared width exactly — 16 for a ``RAW(16)``, 64 for a ``RAW(64)``, with
    ``DATA_PRECISION`` and ``DATA_SCALE`` both NULL; see
    ``test_introspector_deep.py`` for the live round trip). So the un-sized case
    is not "rare", it is a string Oracle cannot store, and it is refused.

    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    """

    @pytest.mark.parametrize("width", [1, 8, 16, 32, 64, 2000])
    def test_the_declared_width_is_what_comes_back(self, dialect, width):
        parsed = dialect.parse_type(f"RAW({width})")
        assert isinstance(parsed, OracleRawType)
        assert parsed.length == width

    def test_two_widths_are_two_different_types(self, dialect):
        assert dialect.parse_type("RAW(16)") != dialect.parse_type("RAW(64)")
        assert dialect.parse_type("RAW(16)") != dialect.parse_type("RAW(32)")

    def test_a_declared_column_equals_the_introspected_one(self, dialect):
        """The point of the whole refactor: a declaration and the catalog row it
        produced are the same column, so ``==`` (what the differ uses) says so."""
        assert OracleRawType(dialect, length=16) == dialect.parse_type("RAW(16)")
        assert OracleRawType(dialect, length=64) == dialect.parse_type("RAW(64)")
        assert OracleRawType(dialect, length=16) != dialect.parse_type("RAW(64)")

    @pytest.mark.parametrize("raw,rendered", [
        ("RAW(16)", "RAW(16)"),
        ("RAW(64)", "RAW(64)"),
        ("RAW(2000)", "RAW(2000)"),
        ("RAW (32)", "RAW(32)"),
    ])
    def test_round_trip_is_byte_identical(self, dialect, raw, rendered):
        sql, params = dialect.format_data_type(dialect.parse_type(raw))
        assert sql == rendered
        assert params == ()

    def test_an_unsized_raw_is_refused_rather_than_given_a_width(self, dialect):
        """Oracle's summary says "You must specify size for a ``RAW`` value" and
        the server agrees — ``CREATE TABLE t (a RAW)`` is
        ``ORA-00906: missing left parenthesis`` on 21c and 23c. So a bare
        ``RAW`` is not a column Oracle could have written, and every width this
        could have answered with would be one the catalog never reported.

        The value it used to answer with — 2000, Oracle's
        ``MAX_STRING_SIZE = STANDARD`` *ceiling* — made every introspected
        ``RAW(16)`` report 2000 bytes. A fallback that invents a width is still
        a false claim if it can be reached, so there is no fallback.
        """
        with pytest.raises(ValueError) as excinfo:
            dialect.parse_type("RAW")
        message = str(excinfo.value)
        assert "un-sized RAW" in message, message
        assert "RAW(16)" in message, message

    def test_long_raw_is_unaffected(self, dialect):
        """``LONG RAW`` needs no size, so the size rule does not touch it."""
        parsed = dialect.parse_type("LONG RAW")
        assert isinstance(parsed, OracleLongRawType)
        assert dialect.format_data_type(parsed)[0] == "LONG RAW"


class TestIntervalAndCustomConcepts:
    def test_interval_is_rendered_not_substituted(self, dialect):
        """Oracle has both interval types natively, so an INTERVAL column must
        not round-trip as CLOB."""
        assert "interval" in dialect.supports_data_types()
        assert "interval" not in dialect.suggested_data_types()

    @pytest.mark.parametrize("fields,expected", [
        ("YEAR TO MONTH", "INTERVAL YEAR TO MONTH"),
        ("DAY TO SECOND", "INTERVAL DAY TO SECOND"),
        ("DAY(2) TO SECOND(6)", "INTERVAL DAY(2) TO SECOND(6)"),
    ])
    def test_interval_renders_its_qualifier(self, dialect, fields, expected):
        assert dialect.format_data_type(IntervalType(fields=fields))[0] == expected

    def test_bare_interval_states_oracles_default(self, dialect):
        """Oracle means DAY(2) TO SECOND(6) by a bare INTERVAL; writing that
        out is what makes it round-trip."""
        sql, _ = dialect.format_data_type(IntervalType())
        assert sql == "INTERVAL DAY(2) TO SECOND(6)"
        assert dialect.format_data_type(dialect.parse_type(sql))[0] == sql

    def test_custom_is_rendered_not_substituted(self, dialect):
        """parse_type returns CustomType for every unmodelled name, so it has
        to be renderable or introspection could not be written back."""
        assert "custom" in dialect.supports_data_types()
        assert dialect.format_data_type(CustomType(raw="SDO_GEOMETRY")) == (
            "SDO_GEOMETRY", (),
        )

    def test_custom_renders_a_named_varray(self, dialect):
        """A column of a VARRAY or object type is written as the type's name,
        which is an identifier — precisely what CustomType carries."""
        assert dialect.format_data_type(
            CustomType(raw="order_ids_varray_t")
        ) == ("order_ids_varray_t", ())

    def test_custom_refuses_a_name_that_is_not_one(self):
        """The grammar is the defence: this position takes no parameter."""
        with pytest.raises(ValueError):
            CustomType(raw="NUMBER); DROP TABLE t --")


class TestParseType:
    @pytest.mark.parametrize("raw,expected_cls", [
        ("NUMBER", DecimalType),
        ("NUMBER(7,3)", DecimalType),
        # The three integer widths reach the *core* concepts: Oracle has one
        # integer type and this dialect already writes NUMBER(10)/(5)/(19) for
        # integer/smallint/bigint, so an Oracle-namespaced class would be a
        # second name for a concept that has one — and __eq__ is class identity,
        # which made a declaration and the catalog row it produced unequal.
        ("NUMBER(10)", IntegerType),
        ("NUMBER(10,0)", IntegerType),
        ("NUMBER(5)", SmallIntType),
        ("NUMBER(19)", BigIntType),
        # The two IEEE words reach their own classes: FLOAT(p) is a NUMBER
        # subtype, BINARY_* is IEEE storage, and the catalog keeps the words
        # apart (F.4-4).
        ("BINARY_FLOAT", OracleBinaryFloatType),
        ("BINARY_DOUBLE", OracleBinaryDoubleType),
        ("BOOLEAN", BooleanType),
        ("BOOL", BooleanType),
        ("RAW(2000)", OracleRawType),
        ("LONG RAW", OracleLongRawType),
        ("BLOB", BlobType),
        ("VARCHAR2(128)", VarCharType),
        ("NVARCHAR2(64)", OracleNVarChar2Type),
        ("CHAR(4)", CharType),
        ("CHARACTER(4)", CharType),
        ("NCHAR(2)", CharType),
        # A bare CHAR is legal — Oracle documents a default size of 1 — so this
        # one is a documented default rather than a guess.
        ("CHAR", CharType),
        ("CLOB", TextType),
        ("NCLOB", OracleNClobType),
        ("LONG", OracleLongType),
        ("DATE", DateType),
        ("TIMESTAMP(6)", TimestampType),
        ("TIMESTAMP", TimestampType),
        ("TIMESTAMP(3) WITH TIME ZONE", TimestampTzType),
        ("TIMESTAMP WITH TIME ZONE", TimestampTzType),
        # The local form is its own class: it stores no offset, and re-rendering
        # it as the core WITH TIME ZONE would write back a different column
        # (finding F.4-3).  The bare declaration versus the TIMESTAMP(6) ...
        # catalog row stays unequal — that is the server-defaults question
        # (F.4-5), deliberately out of this change's scope.
        ("TIMESTAMP WITH LOCAL TIME ZONE", OracleTimestampLtzType),
        ("TIMESTAMP(6) WITH LOCAL TIME ZONE", OracleTimestampLtzType),
        ("INTERVAL YEAR TO MONTH", IntervalType),
        ("INTERVAL DAY TO SECOND", IntervalType),
        ("XMLTYPE", XmlType),
        ("SYS.XMLTYPE", XmlType),
        ("MY_CUSTOM_TYPE", CustomType),
    ])
    def test_parsed_type_kind(self, dialect, raw, expected_cls):
        parsed = dialect.parse_type(raw)
        assert isinstance(parsed, expected_cls)

    @pytest.mark.parametrize("raw,expected_cls", [
        ("NUMBER(10)", IntegerType),
        ("NUMBER(5)", SmallIntType),
        ("NUMBER(19)", BigIntType),
        ("VARCHAR2(128)", VarCharType),
        ("CHAR(4)", CharType),
        ("BLOB", BlobType),
    ])
    def test_the_parsed_class_is_the_core_one_not_a_subclass(
            self, dialect, raw, expected_cls):
        """``isinstance`` would pass for a subclass, and a subclass is exactly
        what makes ``==`` fail: ``type(self) is type(other)``. Asserted on the
        concrete class, not the base."""
        assert type(dialect.parse_type(raw)) is expected_cls, raw

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


    def test_long_raw_wins_over_long_prefix(self, dialect):
        assert isinstance(dialect.parse_type("LONG RAW"), OracleLongRawType)
        assert isinstance(dialect.parse_type("LONG"), OracleLongType)

    def test_nclob_wins_over_clob_prefix(self, dialect):
        assert isinstance(dialect.parse_type("NCLOB"), OracleNClobType)
        assert type(dialect.parse_type("CLOB")) is TextType

    # --- every word that names a concept reaches the class that owns it ---

    @pytest.mark.parametrize("raw,expected_cls", [
        # CharType.SPELLINGS — Oracle's grammar admits the long form.
        ("CHAR(7)", CharType),
        ("CHARACTER(7)", CharType),
        # VarCharType.SPELLINGS
        ("VARCHAR2(7)", VarCharType),
        ("VARCHAR(7)", VarCharType),
        ("CHARACTER VARYING(7)", VarCharType),
        ("CHAR VARYING(7)", VarCharType),
        # TextType.SPELLINGS
        ("CLOB", TextType),
        # BooleanType.SPELLINGS — core's second spelling is not an Oracle word
        # for ``parse_type`` to see, but the native column type is.
        ("BOOLEAN", BooleanType),
        # IntervalType — both qualifiers, and both are the same concept with
        # different fields, which is exactly what `fields` is for.
        ("INTERVAL YEAR TO MONTH", IntervalType),
        ("INTERVAL YEAR(3) TO MONTH", IntervalType),
        ("INTERVAL DAY TO SECOND", IntervalType),
        ("INTERVAL DAY(3) TO SECOND(6)", IntervalType),
    ])
    def test_spelling_words_reach_their_owning_class(self, dialect, raw, expected_cls):
        assert isinstance(dialect.parse_type(raw), expected_cls)

    def test_character_varying_is_not_read_as_character(self, dialect):
        """'CHARACTER' is a spelling of CHAR, so the head of
        'CHARACTER VARYING' must be matched first — otherwise a
        variable-length column is introspected as fixed-length."""
        parsed = dialect.parse_type("CHARACTER VARYING(7)")
        assert type(parsed) is VarCharType
        assert not isinstance(parsed, CharType)
        assert parsed.length == 7

    @pytest.mark.parametrize("raw,fields", [
        ("INTERVAL YEAR TO MONTH", "YEAR TO MONTH"),
        ("INTERVAL YEAR(3) TO MONTH", "YEAR(3) TO MONTH"),
        ("INTERVAL DAY(3) TO SECOND(6)", "DAY(3) TO SECOND(6)"),
    ])
    def test_interval_qualifier_is_carried_not_dropped(self, dialect, raw, fields):
        parsed = dialect.parse_type(raw)
        assert parsed.fields == fields
        assert dialect.format_data_type(parsed)[0] == f"INTERVAL {fields}"


class TestUnSizedCharacterWidthsAreRefusedNotGuessed:
    """Three character types, three different answers, and the difference is
    whether Oracle *documents* a default.

    Oracle's built-in summary, verbatim, for each of the four sized character
    types:

    * ``VARCHAR2 (size [BYTE | CHAR])`` — "You **must specify size** for
      ``VARCHAR2``";
    * ``NVARCHAR2 (size)`` — "You **must specify size** for ``NVARCHAR2``";
    * ``RAW (size)`` — "You **must specify size** for a ``RAW`` value";
    * ``CHAR [(size [BYTE | CHAR])]`` — "**Default and minimum size** is 1 byte";
    * ``NCHAR [(size)]`` — "**Default and minimum size** is 1 character".

    The server agrees on every one of them, measured on 21c and 23ai:

    ====================================  ===================  =====================
    ``CREATE TABLE t (a …)``             21c                  23ai / 26ai
    ====================================  ===================  =====================
    ``VARCHAR2``                         ``ORA-00906``        ``ORA-00906``
    ``NVARCHAR2``                        ``ORA-00906``        ``ORA-00906``
    ``RAW``                              ``ORA-00906``        ``ORA-00906``
    ``CHAR``                             accepted, 1         accepted, 1
    ``NCHAR``                            accepted, 1         accepted, 1
    ====================================  ===================  =====================

    So ``CHAR``'s size of 1 is a *documented default* and stays; ``VARCHAR2``'s
    4000 and ``NVARCHAR2``'s 2000 were Oracle's ``MAX_STRING_SIZE = STANDARD``
    **ceilings**, handed back as if they were declared widths — the same defect
    ``RAW``'s 2000 was, and now refused rather than merely unreachable. The
    introspector spells the size out for every character type
    (``_parse_columns`` reads ``CHAR_LENGTH`` or ``DATA_LENGTH``), so an
    un-sized string cannot arrive from introspection in the first place.

    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    """

    @pytest.mark.parametrize("raw", ["VARCHAR2", "VARCHAR",
                                    "CHARACTER VARYING", "CHAR VARYING"])
    def test_an_unsized_varchar2_is_refused(self, dialect, raw):
        with pytest.raises(ValueError) as excinfo:
            dialect.parse_type(raw)
        message = str(excinfo.value)
        assert "un-sized VARCHAR2" in message, message
        assert "You must specify size" in message, message
        assert "ORA-00906" in message, message
        assert "VARCHAR2(4000)" in message, message

    def test_an_unsized_nvarchar2_is_refused(self, dialect):
        with pytest.raises(ValueError) as excinfo:
            dialect.parse_type("NVARCHAR2")
        message = str(excinfo.value)
        assert "un-sized NVARCHAR2" in message, message
        assert "You must specify size" in message, message
        assert "NVARCHAR2(30)" in message, message

    @pytest.mark.parametrize("raw,length", [
        ("CHAR", 1),
        ("NCHAR", 1),
        ("CHARACTER", 1),
    ])
    def test_char_keeps_its_documented_default_size(self, dialect, raw, length):
        """The one that genuinely has a default. Oracle states it twice, in the
        built-in summary and in the ``CHAR``/``NCHAR`` sections ("You can omit
        size from the column definition. The default value is 1"), and the
        server accepts a bare ``CHAR``/``NCHAR`` and stores one character — so
        answering ``CHAR(1)`` says what the column *would be* rather than
        guessing at it."""
        parsed = dialect.parse_type(raw)
        assert type(parsed) is CharType
        assert parsed.length == length
        assert dialect.format_data_type(parsed)[0] == f"CHAR({length})"

    @pytest.mark.parametrize("raw,rendered", [
        ("VARCHAR2(100)", "VARCHAR2(100)"),
        ("VARCHAR(100)", "VARCHAR2(100)"),
        ("CHARACTER VARYING(100)", "VARCHAR2(100)"),
        ("CHAR VARYING(100)", "VARCHAR2(100)"),
        ("NVARCHAR2(100)", "NVARCHAR2(100)"),
        ("CHAR(10)", "CHAR(10)"),
        ("NCHAR(10)", "CHAR(10)"),
    ])
    def test_a_sized_one_round_trips_byte_identically(self, dialect, raw, rendered):
        sql, params = dialect.format_data_type(dialect.parse_type(raw))
        assert sql == rendered
        assert params == ()

    @pytest.mark.parametrize("raw,length", [
        ("VARCHAR2(0)", 0),
        ("VARCHAR2(32767)", 32767),
    ])
    def test_a_declared_size_of_zero_is_not_taken_for_absent(self, dialect, raw, length):
        """``length=0`` is falsy, so an ``or``-style fallback would silently
        replace it with a default. Oracle accepts ``VARCHAR2(0)``, so the value
        has to survive as written."""
        parsed = dialect.parse_type(raw)
        assert parsed.length == length, raw

    def test_the_refusal_does_not_reach_the_formatters(self, dialect):
        """The guess lived on the **parse** side only. A declaration with no
        length still renders — ``VARCHAR2(4000)``, ``NVARCHAR2(2000)`` — because
        a concept's default form has to render *something* and the caller, not
        the parser, owns the width of a column it is inventing. Asserted so the
        distinction is on the record: refusing here would change DDL that
        already works."""
        assert dialect.format_data_type(VarCharType(dialect))[0] == "VARCHAR2(4000)"
        assert dialect.format_data_type(
            OracleNVarChar2Type(dialect))[0] == "NVARCHAR2(2000)"
        assert dialect.format_data_type(CharType(dialect))[0] == "CHAR"


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
