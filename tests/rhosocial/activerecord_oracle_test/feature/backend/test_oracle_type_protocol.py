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

from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BinaryType,
    BlobType,
    BooleanType,
    CharType,
    CustomType,
    DataType,
    DateTimeType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntervalType,
    JsonBType,
    JsonType,
    RealType,
    SmallIntType,
    TextType,
    TimestampTzType,
    TinyIntType,
    VarBinaryType,
    VarCharType,
    XmlType,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.types import (
    OracleRawType,
)
from rhosocial.activerecord.backend.impl.oracle.protocols import (
    OracleDataTypeSupport,
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

    def test_dialect_satisfies_the_data_type_protocol(self, dialect):
        """The family has one stated shape, not only a naming pattern."""
        assert isinstance(dialect, OracleDataTypeSupport)


# --- W2: supports_data_types() coverage ---

class TestSupportsDataTypes:
    def test_includes_core_types(self, dialect):
        supported = dialect.supports_data_types()
        expected_core = [
            "integer", "bigint", "smallint", "float", "real", "double",
            "decimal", "boolean", "varchar", "char", "text", "blob",
            "datetime", "date", "time", "timestamp", "json",
            "tinyint", "timetz", "timestamptz", "jsonb",
            # Native Oracle types, rendered rather than substituted:
            "interval", "xml", "custom",
        ]
        for name in expected_core:
            assert name in supported, f"core type {name!r} missing from supports_data_types()"

    def test_includes_oracle_types(self, dialect):
        supported = dialect.supports_data_types()
        # The eight words no core formatter here writes, and nothing else.
        expected_oracle = [
            "oracle_nvarchar2", "oracle_nclob", "oracle_long",
            "oracle_raw", "oracle_long_raw",
            "oracle_binary_float", "oracle_binary_double",
            "oracle_timestamp_ltz",
        ]
        for name in expected_oracle:
            assert name in supported, f"oracle type {name!r} missing from supports_data_types()"
        assert set(expected_oracle) == {
            name for name in supported if name.startswith("oracle_")
        }, supported

    def test_values_are_data_type_classes(self, dialect):
        supported = dialect.supports_data_types()
        for name, cls in supported.items():
            assert isinstance(cls, type), f"{name}: value is not a class"
            assert hasattr(cls, "name"), f"{name}: class has no name attribute"

    def test_collapsed_dispatch_keys_are_gone(self, dialect):
        """Every prefixed key that was a *second name* for a concept core already
        renders with the same word, plus the two that were synonym classes for
        concepts core models. One concept, one class.

        Each of the six removed from the Oracle-namespaced half rendered byte
        for byte what its core parent renders, so the class added a name and no
        Oracle type — and because ``DataType.__eq__`` is class identity, a
        declaration and the catalog row it produced compared unequal while
        rendering identical SQL. The remaining seven are the collapsed ones:
        ``oracle_clob`` (CLOB is TextType's ``clob`` spelling), ``oracle_xml``
        (XMLTYPE is the XML concept), and the six above.
        """
        supported = dialect.supports_data_types()
        for gone in (
            "oracle_clob", "oracle_xml",
            "oracle_integer", "oracle_smallint", "oracle_bigint",
            "oracle_varchar2", "oracle_char", "oracle_blob",
        ):
            assert gone not in supported, gone
            assert not hasattr(dialect, f"format_data_type_{gone}"), gone
            assert not hasattr(dialect, f"supports_data_type_{gone}"), gone

    def test_the_eight_remaining_keys_are_the_ones_oracle_has_and_core_does_not(self):
        """A prefixed key claims Oracle has a type the core hierarchy has no
        name for. Each of the eight does: the national character set
        (``NVARCHAR2``), its unbounded form (``NCLOB``), the two deprecated
        ``LONG`` forms, the bounded byte string (``RAW(n)``) — that one
        because core ``varbinary`` is *substituted* on this backend, not
        rendered, so there is no ``format_data_type_varbinary`` to reach
        ``RAW(n)`` through — the two IEEE words (whose core siblings render
        ``FLOAT(p)``, a NUMBER subtype), and ``TIMESTAMP WITH LOCAL TIME
        ZONE`` (whose core sibling renders ``WITH TIME ZONE``). The last
        three keep catalog words the core formatters would otherwise
        re-render as a different Oracle type (findings F.4-3 / F.4-4)."""
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            types as oracle_types,
        )
        remaining = {
            "oracle_nvarchar2": oracle_types.OracleNVarChar2Type,
            "oracle_nclob": oracle_types.OracleNClobType,
            "oracle_long": oracle_types.OracleLongType,
            "oracle_raw": oracle_types.OracleRawType,
            "oracle_long_raw": oracle_types.OracleLongRawType,
            "oracle_binary_float": oracle_types.OracleBinaryFloatType,
            "oracle_binary_double": oracle_types.OracleBinaryDoubleType,
            "oracle_timestamp_ltz": oracle_types.OracleTimestampLtzType,
        }
        for name, klass in remaining.items():
            assert klass.name == name, name
        # Every one of them renders a word its core parent does not.
        for name, klass in remaining.items():
            parent = {
                "oracle_nvarchar2": VarCharType,
                "oracle_nclob": TextType,
                "oracle_long": TextType,
                "oracle_raw": VarBinaryType,
                "oracle_long_raw": BlobType,
                "oracle_binary_float": FloatType,
                "oracle_binary_double": DoubleType,
                "oracle_timestamp_ltz": TimestampTzType,
            }[name]
            assert issubclass(klass, parent), name
        assert not hasattr(OracleDialect(version=(23, 0, 0)),
                           "format_data_type_varbinary")


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

    def test_values_are_data_type_subclasses(self, dialect):
        for name, cls in dialect.suggested_data_types().items():
            assert issubclass(cls, DataType), f"{name}: {cls} is not a DataType"

    def test_substitutes_are_themselves_rendered(self, dialect):
        """Suggesting a class this dialect cannot write would trade a clear
        "not supported" for one whose advice does not work."""
        supported = dialect.supports_data_types()
        for name, cls in dialect.suggested_data_types().items():
            assert cls.name in supported, (
                f"suggested[{name!r}] = {cls.__name__}, whose name "
                f"{cls.name!r} this dialect does not render"
            )

    def test_absence_of_a_data_type_names_what_to_use(self, dialect):
        """The substitute reaches the caller through the error message."""
        with pytest.raises(TypeError) as excinfo:
            dialect.format_data_type(VarBinaryType(dialect, length=16))
        assert "BlobType" in str(excinfo.value)
        with pytest.raises(TypeError) as excinfo:
            dialect.format_data_type(BinaryType(dialect, 16))
        assert "OracleRawType" in str(excinfo.value)

    def test_array_is_a_named_oracle_type_not_a_serialised_list(self, dialect):
        assert dialect.suggested_data_types()["array"] is CustomType

    def test_binary_is_a_bounded_raw_not_a_blob(self, dialect):
        """``binary`` is fixed-length, so its substitute must be the *bounded*
        byte string: ``RAW(n)``, with the width the caller declared.

        Oracle has no fixed-length byte string at all — ``BINARY(20)`` is
        rejected (``ORA-03060`` on 23c, ``ORA-00907`` on 21c) — so ``RAW(n)``
        is the closest column Oracle offers, not the same one: Oracle's own
        reference says "``RAW`` is a variable-length data type like
        ``VARCHAR2``" and it does not pad. ``BLOB`` would be further away
        still, because it also drops the declared bound.
        """
        assert dialect.suggested_data_types()["binary"] is OracleRawType

    @pytest.mark.parametrize("width", [1, 8, 16, 32, 64, 2000])
    def test_binary_renders_the_width_the_caller_declared(self, dialect, width):
        """The substitute is ``OracleRawType(length=n)``, and ``n`` is the
        caller's: ``length`` is in ``PARAMETERS``, so two byte-string columns
        of different widths are different columns and both must render."""
        assert dialect.format_data_type(
            OracleRawType(dialect, length=width))[0] == f"RAW({width})"

    def test_varbinary_is_a_blob_not_a_fixed_width_raw(self, dialect):
        """``varbinary`` is variable-length, and ``RAW(16)`` claims a column
        that stops growing at sixteen bytes — a false identity claim of exactly
        the kind this mapping exists to remove. Oracle's variable-length binary
        storage is ``BLOB``: "a binary large object", up to
        ``(4 gigabytes - 1) * (database block size)``, stored outside the row
        behind a locator.

        There is no third option: no ``VARCHAR2``-style type carries binary
        content here (``VARCHAR2`` is defined by the database character set and
        Oracle Net converts its contents — feeding a ``RAW`` value into a
        ``VARCHAR2(16)`` stores the four characters ``"00FF"``), and
        ``BLOB``/``RAW``/``LONG RAW`` are the only byte-string types in the
        built-in summary.

        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        assert dialect.suggested_data_types()["varbinary"] is BlobType
        assert dialect.format_data_type(
            dialect.suggested_data_types()["varbinary"](dialect))[0] == "BLOB"

    def test_the_two_byte_string_concepts_land_on_different_types(self, dialect):
        """``binary`` is bounded and ``varbinary`` is not; one substitute for
        both made them indistinguishable and gave the unbounded one a ceiling."""
        sub = dialect.suggested_data_types()
        assert sub["binary"] is not sub["varbinary"]
        assert sub["varbinary"] is not OracleRawType

    def test_the_varbinary_substitute_is_the_class_the_catalog_gives_back(self, dialect):
        """``BLOB`` needs no Oracle-namespaced class of its own: it is the word
        ``format_data_type_blob`` writes, so the substitute is core ``BlobType``
        and a declared ``blob`` and an introspected ``BLOB`` are the same object.
        A substitute class that rendered the same SQL under a second name would
        have made every ``blob`` column look changed."""
        assert dialect.suggested_data_types()["varbinary"](dialect) == \
            dialect.parse_type("BLOB")

    def test_uuid_is_raw_16(self, dialect):
        """Oracle has **no** ``UUID`` data type in any release — 23ai added the
        ``UUID()`` *function*, whose own reference page says it "returns a
        version 4 variant 1 UUID as a ``RAW(16)`` value". So the substitute is
        ``RAW(16)``, the width this backend already stores the 16 RFC 4122 bytes
        in."""
        assert dialect.suggested_data_types()["uuid"] is OracleRawType

    def test_the_uuid_substitute_renders_16_bytes_with_no_arguments(self, dialect):
        """The defect this replaces: the substitute used to render ``RAW(2000)``
        — a 2 KB binary column for a 128-bit identifier — because ``RAW`` took
        ``length=None`` and the formatter fell back to Oracle's
        ``MAX_STRING_SIZE = STANDARD`` ceiling. ``RAW(n)`` is *n bytes*, and a
        UUID is 16 of them, so the default is 16.

        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/uuid.html
        """
        substitute = dialect.suggested_data_types()["uuid"]
        assert dialect.format_data_type(substitute(dialect))[0] == "RAW(16)"

    def test_the_uuid_substitute_still_takes_an_explicit_width(self, dialect):
        """The default is the UUID width, not a hard-coded type: ``length`` is
        in ``PARAMETERS``, so two ``RAW`` columns of different widths are
        different columns and both must render."""
        assert dialect.format_data_type(OracleRawType(dialect, length=16))[0] == "RAW(16)"
        assert dialect.format_data_type(OracleRawType(dialect, length=32))[0] == "RAW(32)"

    def test_suggest_column_type_and_the_substitute_agree(self, dialect):
        """One width, one answer: the Python-type route and the substitution
        route must not drift apart."""
        import uuid as _uuid
        suggested = dialect.suggested_data_types()["uuid"]
        assert dialect.format_data_type(
            dialect.suggest_column_type(_uuid.UUID)
        )[0] == dialect.format_data_type(suggested(dialect))[0] == "RAW(16)"

    def test_enum_is_varchar(self, dialect):
        assert dialect.suggested_data_types()["enum"] is VarCharType


# --- unsigned is a field, so it must be honoured or refused ---

class TestUnsignedIsRefused:
    """Oracle has no unsigned integer type, and it has no ``UNSIGNED``
    attribute to write either — so ``unsigned=True`` is **refused** rather
    than silently dropped. Silently dropping it would render the signed
    column, report success, and produce a column that does not match what the
    caller declared.

    Evidence, all from the 26ai references:
      * the recognised data types, spelled out in full, contain no unsigned
        entry — https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlqr/Data-Types.html
      * ``column_definition`` admits no type modifier, and the words
        ``UNSIGNED``/``SIGNED`` appear nowhere in the CREATE TABLE reference —
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/CREATE-TABLE.html
    """

    #: Every formatter that renders an integer width on this backend: the four
    #: core concepts, and nothing else. Oracle has one integer type, so there is
    #: no Oracle-specific integer concept to add a ``oracle_``-prefixed key for.
    #: ``(dispatch name, class, signed SQL, the word the refusal names)``.
    _INTEGER_WIDTHS = [
        ("integer", IntegerType, "NUMBER(10)", "INTEGER"),
        ("smallint", SmallIntType, "NUMBER(5)", "SMALLINT"),
        ("bigint", BigIntType, "NUMBER(19)", "BIGINT"),
        ("tinyint", TinyIntType, "NUMBER(3)", "TINYINT"),
    ]
    _IDS = [row[0] for row in _INTEGER_WIDTHS]

    @pytest.mark.parametrize("name,klass,signed_sql,word",
                             _INTEGER_WIDTHS, ids=_IDS)
    def test_unsigned_is_refused(self, dialect, name, klass, signed_sql, word):
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            dialect.format_data_type(klass(dialect, unsigned=True))
        message = str(excinfo.value)
        assert "unsigned" in message, message
        assert word in message, message

    @pytest.mark.parametrize("name,klass,signed_sql,word",
                             _INTEGER_WIDTHS, ids=_IDS)
    def test_the_refusal_names_the_width_and_suggests_a_check(
            self, dialect, name, klass, signed_sql, word):
        """The message must say what *this* backend writes instead, so the
        caller can read the column it would have got, and point at the
        constraint that is where a per-column range belongs."""
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            dialect.format_data_type(klass(dialect, unsigned=True))
        message = str(excinfo.value)
        assert signed_sql in message, message
        assert "CHECK" in message, message
        assert excinfo.value.suggestion and "CHECK" in excinfo.value.suggestion

    @pytest.mark.parametrize("name,klass,signed_sql,word",
                             _INTEGER_WIDTHS, ids=_IDS)
    def test_the_signed_form_is_untouched(self, dialect, name, klass, signed_sql, word):
        """Refusing ``unsigned=True`` must not disturb the signed rendering,
        which stays byte-identical to what it was before."""
        assert dialect.format_data_type(klass(dialect))[0] == signed_sql
        assert dialect.format_data_type(klass(dialect, unsigned=False))[0] == signed_sql

    def test_unsigned_stays_in_identity_on_every_integer_concept(self):
        """The flag stays in ``PARAMETERS`` on all four core integer classes:
        two declarations differing only in it are different columns, so
        ``__eq__`` can still see the difference the refusal reports. Narrowing
        ``PARAMETERS`` would hide it instead."""
        for klass in (IntegerType, SmallIntType, BigIntType, TinyIntType):
            assert klass.PARAMETERS == ("unsigned",), klass.__name__
            assert klass() != klass(unsigned=True)

    def test_no_integer_formatter_drops_the_flag_silently(self, dialect):
        """The rule in one assertion, over every width this backend renders:
        either the flag changes the rendered SQL or it raises. Byte-identical
        SQL for both signs is the violation."""
        for name in ("integer", "smallint", "bigint", "tinyint"):
            klass = dialect.supports_data_types()[name]
            signed = dialect.format_data_type(klass(dialect))[0]
            try:
                unsigned = dialect.format_data_type(
                    klass(dialect, unsigned=True))[0]
            except UnsupportedFeatureError:
                continue  # refused — the other permitted answer
            assert unsigned != signed, (
                f"{name}: unsigned=True renders {unsigned!r}, the same as "
                f"unsigned=False — the flag is silently dropped"
            )

    def test_every_integer_rendering_is_a_core_dispatch_key(self, dialect):
        """No ``oracle_integer`` / ``oracle_smallint`` / ``oracle_bigint`` on this
        backend, and there is nothing for one to be: the four widths are four
        core concepts and the ``oracle_`` half has nothing left to say about
        them. Oracle's ANSI table gives ``{ INTEGER | INT | SMALLINT }`` a
        single row — ``NUMBER(38)`` — which is the whole of what it says about
        integers, and ``BIGINT`` is not an ANSI type at all."""
        for name in ("oracle_integer", "oracle_smallint", "oracle_bigint"):
            assert name not in dialect.supports_data_types()
            assert not hasattr(dialect, f"format_data_type_{name}")


# --- Oracle has one integer type; the four widths are precisions on it ---

class TestUnsignedNumericIsRefused:
    """The same rule as :class:`TestUnsignedIsRefused`, over the four
    exact/approximate numeric concepts rather than the four integer widths.

    ``DecimalType``, ``FloatType``, ``DoubleType`` and ``RealType`` each carry
    ``unsigned`` in ``PARAMETERS``, so two declarations differing only in it are
    different columns as far as the schema differ is concerned — and Oracle cannot
    express it, so every one of them refuses rather than writing the signed column
    and reporting success.

    Evidence, read from the 26ai references **and measured on both wired servers**
    (21c 21.0.0.0.0 and 26ai Free 23.26.1.0.0, which agree on all of it):

      * the reference chapter states the storages are signed by construction —
        "The Oracle database numeric data types store positive and negative fixed
        and floating-point numbers, zero, infinity, and values that are the
        undefined result of an operation" —
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
      * the recognised-type inventory has no unsigned row —
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlqr/Data-Types.html
      * ``CREATE TABLE``'s ``column_definition ::= ( datatype_domain ::= ,
        identity_clause ::= , inline_constraint ::= , inline_ref_constraint ::= ,
        annotations_clause ::= )`` — five clauses after the type, none of them a
        modifier, and the words ``UNSIGNED``/``SIGNED`` appear **zero** times on
        the page —
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/CREATE-TABLE.html
      * **measured**: every ``<type> ... UNSIGNED`` declaration is rejected by the
        *parser*, before any semantic check — ``ORA-00907: missing right
        parenthesis`` on 21c and ``ORA-03062: missing comma or right
        parenthesis`` on 26ai Free — so there is not even a spelling that would
        compile. The documented route forward was measured too:
        ``NUMBER(10,2) CHECK (c >= 0)`` is accepted on both and reports
        ``DATA_TYPE='NUMBER'``, ``DATA_PRECISION=10``, ``DATA_SCALE=2``.
    """

    #: ``(dispatch name, class, signed SQL, the word the refusal names)``. The
    #: signed SQL is what each concept renders here — note that ``float``,
    #: ``double`` and ``real`` all render a *binary* precision, and the first two
    #: render the same one, which is the ANSI conversion table's mapping of three
    #: concepts onto Oracle's single ``FLOAT``.
    _NUMERICS = [
        ("decimal", DecimalType, "NUMBER", "NUMBER"),
        ("float", FloatType, "FLOAT(126)", "FLOAT"),
        ("double", DoubleType, "FLOAT(126)", "FLOAT"),
        ("real", RealType, "FLOAT(63)", "FLOAT"),
    ]
    _IDS = [row[0] for row in _NUMERICS]

    @pytest.mark.parametrize("name,klass,signed_sql,word", _NUMERICS, ids=_IDS)
    def test_unsigned_is_refused(self, dialect, name, klass, signed_sql, word):
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            dialect.format_data_type(klass(dialect, unsigned=True))
        message = str(excinfo.value)
        assert "unsigned" in message, message
        assert word in message, message

    @pytest.mark.parametrize("name,klass,signed_sql,word", _NUMERICS, ids=_IDS)
    def test_the_refusal_cites_the_measured_parser_error_and_a_check(
            self, dialect, name, klass, signed_sql, word):
        """Both halves of the citation must survive into the message: the error
        *both* wired servers raise for this declaration, and the constraint that
        is where a per-column range belongs on this backend."""
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            dialect.format_data_type(klass(dialect, unsigned=True))
        message = str(excinfo.value)
        assert "ORA-00907" in message, message
        assert "ORA-03062" in message, message
        assert excinfo.value.suggestion and "CHECK" in excinfo.value.suggestion

    @pytest.mark.parametrize("name,klass,signed_sql,word", _NUMERICS, ids=_IDS)
    def test_the_signed_form_is_untouched(self, dialect, name, klass, signed_sql, word):
        """Refusing ``unsigned=True`` must not disturb the signed rendering, which
        stays byte-identical to what it was before the field arrived."""
        assert dialect.format_data_type(klass(dialect))[0] == signed_sql
        assert dialect.format_data_type(
            klass(dialect, unsigned=False))[0] == signed_sql

    @pytest.mark.parametrize("name,klass,signed_sql,word", _NUMERICS, ids=_IDS)
    def test_unsigned_stays_in_identity(self, dialect, name, klass, signed_sql, word):
        """The flag stays in ``PARAMETERS`` on all four classes, appended after the
        fields already there so ``__hash__`` keeps its meaning: two declarations
        differing only in it are different columns, and narrowing the tuple would
        hide the very difference the refusal reports."""
        assert "unsigned" in klass.PARAMETERS, klass.PARAMETERS
        assert klass.PARAMETERS[-1] == "unsigned", klass.PARAMETERS
        assert klass(dialect) != klass(dialect, unsigned=True)

    def test_no_numeric_formatter_drops_the_flag_silently(self, dialect):
        """The paradigm rule in one assertion over all four: either the flag
        changes the rendered SQL or it raises. Byte-identical SQL for both signs
        is the violation — and it is exactly what these four formatters used to do
        before the refusal was added."""
        for name, klass, signed_sql, word in self._NUMERICS:
            signed = dialect.format_data_type(klass(dialect))[0]
            try:
                unsigned = dialect.format_data_type(
                    klass(dialect, unsigned=True))[0]
            except UnsupportedFeatureError:
                continue  # refused — the other permitted answer
            assert unsigned != signed, (
                f"{name}: unsigned=True renders {unsigned!r}, the same as "
                f"unsigned=False — the flag is silently dropped"
            )

    @pytest.mark.parametrize("name,klass,signed_sql,word", _NUMERICS, ids=_IDS)
    def test_the_signedness_gate_runs_before_the_precision_and_scale_checks(
            self, dialect, name, klass, signed_sql, word):
        """A request wrong in two ways is told about the right one first.

        Signedness is a declaration this grammar cannot express **at all**, which
        is a stronger and less recoverable statement than an out-of-range number,
        so the refusal comes first. This ordering is not observable any other way:
        every test above and below passes with either order.
        """
        bad = {
            "decimal": DecimalType(dialect, precision=99, unsigned=True),
            "float": FloatType(dialect, precision=999, unsigned=True),
            "double": DoubleType(dialect, unsigned=True, spelling="float"),
            "real": RealType(dialect, unsigned=True),
        }[name]
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            dialect.format_data_type(bad)
        assert "unsigned" in str(excinfo.value)

    @pytest.mark.parametrize(
        "case,expected",
        [
            # every pre-existing ValueError, still firing for a *signed* declaration
            ("precision_out_of_range", ValueError),
            ("scale_out_of_range", ValueError),
            ("float_precision_out_of_range", ValueError),
            # ... and the refusals that are not ValueError
            ("decimal_bad_spelling", TypeError),
            ("double_bad_spelling", TypeError),
        ],
    )
    def test_every_pre_existing_check_still_fires_for_a_signed_declaration(
            self, dialect, case, expected):
        """The gate is additive: nothing it could mask may have stopped working.

        The cases are named rather than constructed in the ``parametrize`` list
        because a ``DataType`` needs a real dialect and ``dialect`` is a fixture
        at class-body time. Pinned by *type* rather than by message, because the
        point is that each of these still raises the exception it always raised.
        """
        declared = {
            "precision_out_of_range": DecimalType(dialect, precision=99),
            "scale_out_of_range": DecimalType(dialect, scale=999),
            "float_precision_out_of_range": FloatType(dialect, precision=999),
            "decimal_bad_spelling": DecimalType(dialect, precision=10,
                                                spelling="fixed"),
            "double_bad_spelling": DoubleType(dialect, spelling="float"),
        }[case]
        with pytest.raises(expected):
            declared.to_sql()

    def test_a_type_string_carrying_unsigned_is_refused_rather_than_read(
            self, dialect):
        """The same rule in the *reading* direction.

        ``parse_type`` reads Oracle's ``user_tab_columns`` rows and also
        caller-supplied DDL, and every branch below matches on the word in front
        of the attribute — so without a check here ``"NUMBER(10,2) UNSIGNED"``
        would come back as a *signed* ``DecimalType(10, 2)`` that ``==`` calls
        equal to the signed column, and the differ would report no change for the
        one change the string describes.
        """
        for raw in ("NUMBER(10,2) UNSIGNED", "FLOAT(126) UNSIGNED",
                    "NUMBER UNSIGNED", "BINARY_DOUBLE UNSIGNED"):
            with pytest.raises(UnsupportedFeatureError) as excinfo:
                dialect.parse_type(raw)
            assert "UNSIGNED" in str(excinfo.value), raw
        # ... and an unsigned-free string of the same shapes is untouched.
        assert dialect.parse_type("NUMBER(10,2)") == DecimalType(
            dialect, 10, 2)

    def test_the_refusal_is_not_a_value_error(self, dialect):
        """The cross-backend exception convention, stated as a test: a wrong
        value is ``ValueError``; a declaration this grammar cannot express at all
        is ``UnsupportedFeatureError``; the two do not share a base class."""
        assert not issubclass(UnsupportedFeatureError, ValueError)
        with pytest.raises(UnsupportedFeatureError):
            dialect.format_data_type(DecimalType(dialect, unsigned=True))


class TestIntegerWidthsAgainstOraclesAnsiTable:
    """Oracle's ANSI conversion table says ``{ INTEGER | INT | SMALLINT }`` →
    ``NUMBER(38)``, one row for three spellings, and a review of this backend
    read that as a defect in the ``NUMBER(5)`` it writes for ``smallint``. The
    table is not being misread — it says exactly that, and the server agrees.
    The conclusion drawn from it is what had to be checked, and it does not
    follow: the table's whole content is that the three words are *one*
    column, and Oracle has no narrower integer to render instead.

    Measured on both wired servers (21c and 23c), with
    ``CREATE TABLE t (a SMALLINT, b INT, c INTEGER)`` read back from
    ``USER_TAB_COLUMNS``:

    ==================================  ==========  ============  =========  =========
    declared                            DATA_TYPE   DATA_PREC.    DATA_SCALE  DATA_LEN
    ==================================  ==========  ============  =========  =========
    ``a SMALLINT`` / ``b INT`` /        NUMBER      ``NULL``      ``0``       22
    ``c INTEGER``
    ==================================  ==========  ============  =========  =========

    Three rows, one shape. A separately declared ``NUMBER(38)`` reports
    ``DATA_PRECISION = 38``, so the table's ``NUMBER(38)`` is its name for the
    maximum-range ``NUMBER``, not a bound the catalog stores.

    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlqr/Data-Types.html
    """

    #: Every width this backend renders, with the range that fixes it. The
    #: precision is the number of decimal digits in that range, which is the
    #: whole of the argument: ``NUMBER(p)`` is symmetric, so it holds the
    #: concept's whole signed range and nothing wider.
    _WIDTHS = [
        (TinyIntType, "NUMBER(3)", 127),
        (SmallIntType, "NUMBER(5)", 32767),
        (IntegerType, "NUMBER(10)", 2147483647),
        (BigIntType, "NUMBER(19)", 9223372036854775807),
    ]
    _IDS = [k.__name__ for k, _, _ in _WIDTHS]

    @pytest.mark.parametrize("klass,sql,widest", _WIDTHS, ids=_IDS)
    def test_the_width_is_pinned(self, dialect, klass, sql, widest):
        assert dialect.format_data_type(klass(dialect))[0] == sql

    @pytest.mark.parametrize("klass,sql,widest", _WIDTHS, ids=_IDS)
    def test_the_width_holds_the_whole_signed_range(self, dialect, klass, sql, widest):
        """``NUMBER(p)`` is symmetric: it reaches ``10**p - 1``. If the concept's
        widest signed value does not fit, the column is a false report of what
        fits — which is the only thing a wrong precision would be."""
        precision = int(sql[len("NUMBER("):-1])
        assert widest <= 10 ** precision - 1, (
            f"{klass.__name__}: {sql} cannot hold {widest}"
        )

    def test_no_two_integer_concepts_render_the_same_column(self, dialect):
        """Following the table literally would make all three of
        ``SmallIntType``/``IntegerType``/``BigIntType`` the same 38-digit
        column — which would both misreport a 2-byte smallint as a 38-digit
        number and erase the distinction a schema diff needs."""
        rendered = {klass: dialect.format_data_type(klass(dialect))[0]
                    for klass, _, _ in self._WIDTHS}
        assert len(set(rendered.values())) == len(self._WIDTHS), rendered

    def test_a_declared_integer_equals_the_introspected_one(self, dialect):
        """``parse_type`` reads ``NUMBER(5)``/``NUMBER(10)``/``NUMBER(19)`` back
        as the **core** concepts, so a declared column and the catalog row it
        produced are the same object. This is the assertion the round trip is
        worth anything for: ``DataType.__eq__`` is class identity, so returning
        an Oracle-namespaced subclass here made ``IntegerType() !=
        parse_type("NUMBER(10)")`` — the same column, reported as changed. The
        deletion of ``OracleIntegerType`` / ``OracleSmallIntType`` /
        ``OracleBigIntType`` is what makes this hold."""
        for klass, raw in (
            (IntegerType, "NUMBER(10)"),
            (SmallIntType, "NUMBER(5)"),
            (BigIntType, "NUMBER(19)"),
        ):
            declared = klass(dialect)
            parsed = dialect.parse_type(raw)
            assert type(parsed) is klass, raw
            assert declared == parsed, raw
            assert dialect.format_data_type(parsed)[0] == raw

    def test_number_3_is_not_read_back_as_a_tinyint(self, dialect):
        """An asymmetry that is *not* fixed here, recorded so it is visible.

        ``format_data_type_tinyint`` writes ``NUMBER(3)`` but ``parse_type``
        reads ``NUMBER(3)`` back as ``DecimalType(precision=3)``. Oracle's
        catalog cannot tell the two apart — ``NUMBER(p)`` is one type — so the
        forward map has to pick a side and the reverse map picks a side too, and
        today they pick differently for this one width.

        Extending the integer read to ``NUMBER(3)`` would be a new judgement
        call about which reading of an ambiguous catalog value is more likely,
        which is not one of the four items this round closes. It is also the
        *same* ambiguity ``NUMBER(10)`` already has (``INTEGER`` versus
        ``DECIMAL(10)``), resolved toward the integer concept; the honest place
        to record that resolution is a decision covering all four widths at
        once, not one width at a time. So this asserts the current behaviour.
        """
        assert type(dialect.parse_type("NUMBER(3)")) is DecimalType
        assert dialect.parse_type("NUMBER(3)").precision == 3

    def test_a_width_is_a_function_of_the_range_not_of_the_word(self, dialect):
        """Two ``NUMBER`` precisions, chosen by range: ``NUMBER(5)`` covers
        ±32767 and ``NUMBER(10)`` covers ±2147483647, so the two concepts are
        distinguishable in the catalog, which is what makes the reverse mapping
        possible at all."""
        assert dialect.format_data_type(SmallIntType(dialect))[0] == "NUMBER(5)"
        assert dialect.format_data_type(IntegerType(dialect))[0] == "NUMBER(10)"
        assert type(dialect.parse_type("NUMBER(5)")) is SmallIntType
        assert type(dialect.parse_type("NUMBER(10)")) is IntegerType


class TestIntegerSpellingsOracleDoesNotHave:
    """Words the backend accepts on a concept are normalised, but only because
    refusing them would refuse the concept. Two of them are not Oracle words at
    all, and the backend must not pretend otherwise in a way that reaches
    DDL."""

    @pytest.mark.parametrize("spelling", ["int2", "int8", "int1"])
    def test_oracle_rejects_these_and_we_normalise_them(self, dialect, spelling):
        """``CREATE TABLE t (a INT2)`` is ``ORA-00902: invalid datatype`` on both
        wired servers. Accepting the spelling is right — the concept has to be
        usable here — but the SQL written is Oracle's own ``NUMBER``, never the
        word Oracle refused."""
        factory = {"int1": lambda: TinyIntType(spelling="int1"),
                   "int2": lambda: SmallIntType(spelling="int2"),
                   "int8": lambda: BigIntType(spelling="int8")}[spelling]
        sql = dialect.format_data_type(factory())[0]
        assert sql.startswith("NUMBER("), sql
        assert spelling.upper() not in sql

    def test_bigint_is_not_an_ansi_type_and_oracle_rejects_it(self, dialect):
        """``BIGINT`` is absent from the ANSI names Oracle recognises and the
        server rejects it outright (``ORA-00902: invalid datatype`` on 21c and
        23ai) — so there is no Oracle ``BIGINT`` for a prefixed key to name, and
        ``format_data_type_bigint``'s docstring must not claim one. It writes
        ``NUMBER(19)`` for the concept, and the measurement is recorded there."""
        doc = type(dialect).format_data_type_bigint.__doc__ or ""
        assert "ORA-00902" in doc, doc
        assert "not an ANSI type" in doc, doc


class TestFloatSpellingsAgainstOraclesAnsiTable:
    """The same table's last row: ``FLOAT`` → ``FLOAT(126)``,
    ``DOUBLE PRECISION`` → ``FLOAT(126)``, ``REAL`` → ``FLOAT(63)``, with notes
    2–4 giving the binary precisions. ``FLOAT(126)`` looks odd because it is a
    ``NUMBER`` subtype rather than an IEEE double, but it is verbatim what
    Oracle's table says ``DOUBLE PRECISION`` is — and what the server reports
    back for it (measured: ``DATA_TYPE = 'FLOAT'``, ``DATA_PRECISION = 126``).
    """

    def test_double_renders_the_documented_binary_precision(self, dialect):
        assert dialect.format_data_type(DoubleType(dialect))[0] == "FLOAT(126)"
        assert dialect.format_data_type(
            DoubleType(dialect, spelling="double precision"))[0] == "FLOAT(126)"

    def test_real_renders_the_documented_binary_precision(self, dialect):
        assert dialect.format_data_type(RealType(dialect))[0] == "FLOAT(63)"

    def test_float_writes_the_documented_default_precision(self, dialect):
        """``FLOAT[(p)]`` is Oracle's own spelling of the concept, p from 1 to 126
        **binary** digits, and the default is stated by Oracle in both places it
        could be: the built-in summary ("The precision p can range from 1 to 126
        binary digits") and the ANSI conversion table's note 2 ("the default
        precision for this data type is 126 binary, or 38 decimal"). Measured:
        ``CREATE TABLE t (a FLOAT)`` reports ``DATA_PRECISION = 126`` on 21c and
        23ai, and the introspector reads it, so a bare ``FLOAT`` came back as
        ``FLOAT(126)``.

        So a bare ``FLOAT`` was a declaration that did not match the column it
        produced. Writing the documented default out is the same reason the
        sibling ``double`` writes ``FLOAT(126)`` and ``real`` writes
        ``FLOAT(63)`` rather than the bare words Oracle's table converts them
        from — and it is what makes the round trip byte-identical.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        assert dialect.format_data_type(FloatType(dialect))[0] == "FLOAT(126)"
        assert dialect.format_data_type(
            FloatType(dialect, precision=63))[0] == "FLOAT(63)"
        with pytest.raises(ValueError):
            dialect.format_data_type(FloatType(dialect, precision=127))

    def test_a_bare_float_round_trips_byte_identically(self, dialect):
        """The point of writing the default out: what Oracle stores for a bare
        ``FLOAT`` is read back as ``FLOAT(126)``, and rendering that again must
        produce the same word, not a shorter one."""
        parsed = dialect.parse_type("FLOAT(126)")
        assert type(parsed) is FloatType
        assert parsed.precision == 126
        assert dialect.format_data_type(parsed)[0] == "FLOAT(126)"

    def test_every_float_rendering_is_oracles_own_word(self, dialect):
        """Oracle recommends ``BINARY_FLOAT``/``BINARY_DOUBLE`` "as they are more
        robust" over ``FLOAT``, because ``FLOAT`` is a ``NUMBER`` whose digits
        happen to be counted in binary — so ``FLOAT(126)`` is not an IEEE double.
        That is a reason to ask for :class:`DoubleType` deliberately, and not a
        reason for this backend to write something else: ``float`` is the
        concept the caller named and ``FLOAT`` is Oracle's word for it."""
        doc = type(dialect).format_data_type_float.__doc__ or ""
        assert "more robust" in doc, doc
        assert "BINARY_DOUBLE" in doc, doc
        assert dialect.format_data_type(FloatType(dialect))[0].startswith("FLOAT")

    def test_every_double_spelling_round_trips_byte_identically(self, dialect):
        sql = dialect.format_data_type(DoubleType(dialect))[0]
        parsed = dialect.parse_type(sql)
        assert dialect.format_data_type(parsed)[0] == sql

    def test_a_double_column_introspects_as_a_float_of_that_precision(self, dialect):
        """Not ``DoubleType``, and deliberately: a column declared
        ``DOUBLE PRECISION`` comes back from ``USER_TAB_COLUMNS`` as
        ``DATA_TYPE = 'FLOAT'``, ``DATA_PRECISION = 126`` (measured on 21c and
        23c), so claiming it is the double concept would be a claim about the
        column the catalog does not support. It is Oracle's own ``FLOAT`` with
        Oracle's own precision, and that is what it parses to."""
        parsed = dialect.parse_type("FLOAT(126)")
        assert type(parsed) is FloatType
        assert parsed.precision == 126
        assert dialect.format_data_type(parsed)[0] == "FLOAT(126)"


class TestWhatOracleSuppliesForAnUndeclaredParameter:
    """``type_parameter_defaults()`` — one entry, and the three refusals beside it.

    The measurements behind this class were taken against **both** wired
    servers — 21c (21.0.0.0.0) and 26ai Free (23.26.1.0.0) — which agree on
    every row, so nothing here rests on one release:

    ==================================  =========================  =====================
    ``CREATE TABLE t (c …)``            reported                  declaration
    ==================================  =========================  =====================
    ``FLOAT``                           ``FLOAT``, prec **126**   **declared**
    ``FLOAT(126)``                      ``FLOAT``, prec 126        —
    ``REAL``                            ``FLOAT``, prec **63**    not declared
    ``DOUBLE PRECISION``                ``FLOAT``, prec **126**   not declared
    ``CHAR``                            ``CHAR``, len **1**       measured, not declared
    ``VARCHAR2``                        ``ORA-00906``             not declared
    ``INTERVAL``                        ``ORA-30089``             not a declaration at all
    ==================================  =========================  =====================

    The interesting content is the second column: three of the five unsized
    forms **do not exist** on this server, which is what makes silence the right
    answer for them and a documented number the right answer for ``FLOAT``.
    """

    def test_float_is_the_only_declared_entry(self, dialect):
        assert dialect.type_parameter_defaults() == {
            "float": {"precision": 126},
        }

    def test_the_declared_precision_is_what_the_catalog_reports(self, dialect):
        """126, and it is **measured**, not read off the rendered literal.

        The number the dialect already wrote as a literal is documented twice by
        Oracle — the built-in summary gives the range ("The precision ``p`` can
        range from 1 to 126 binary digits") and the ANSI conversion table's
        note 2 gives the default ("The default precision for this data type is
        126 binary, or 38 decimal"). Documented is not the same as *stored*,
        which is why the probe created the column and read ``DATA_PRECISION``
        back rather than assuming: on 21c and on 26ai a bare ``FLOAT`` reports
        ``DATA_TYPE='FLOAT'`` and ``DATA_PRECISION=126``. It agrees, and that is
        what licenses the entry.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        assert dialect.type_parameter_defaults()["float"]["precision"] == 126
        doc = (type(dialect).type_parameter_defaults.__doc__ or "").replace("\n", " ")
        assert "126 binary, or 38 decimal" in doc, doc

    def test_no_rendering_changed(self, dialect):
        """Before and after are byte-identical, which is why this step is safe.

        ``float`` and ``double`` both rendered ``FLOAT(126)`` and ``real``
        rendered ``FLOAT(63)`` before the declaration existed. Reading the number
        from the one place it is written down is what keeps the declaration and
        the formatters from drifting apart; it does not move any output.
        """
        assert dialect.format_data_type(FloatType(dialect))[0] == "FLOAT(126)"
        assert dialect.format_data_type(DoubleType(dialect))[0] == "FLOAT(126)"
        assert dialect.format_data_type(RealType(dialect))[0] == "FLOAT(63)"
        assert dialect.format_data_type(CharType(dialect))[0] == "CHAR"
        assert dialect.format_data_type(
            IntervalType(dialect))[0] == "INTERVAL DAY(2) TO SECOND(6)"

    def test_the_entry_is_live_and_closes_the_float_gap(self, dialect):
        """Core resolves ``precision`` the way it resolves ``length``, so the
        declaration now reaches the type.

        ``FloatType(d).precision`` is 126 rather than ``None``, which makes a
        bare declaration and the ``FloatType(d, precision=126)`` the catalog
        yields **the same value object** — exactly the gap this entry was
        written for.  The rendering did not move: ``FLOAT(126)`` is what the
        formatter wrote before and what it writes now, so this is equality and
        rendering becoming one story rather than a change to the DDL.
        """
        assert FloatType(dialect).precision == 126
        assert FloatType(dialect) == FloatType(dialect, precision=126)
        assert hash(FloatType(dialect)) == hash(FloatType(dialect, precision=126))
        doc = type(dialect).type_parameter_defaults.__doc__ or ""
        assert "no longer inert" in doc, doc

    def test_char_is_measured_and_still_not_declared(self, dialect):
        """Oracle documents 1 and the server stores it — and declaring it would
        change DDL that already works.

        ``CREATE TABLE t (c CHAR)`` is accepted on both servers and reports
        ``DATA_LENGTH = 1`` and ``CHAR_LENGTH = 1``, exactly what ``CHAR(1)``
        reports. So the declaration would be truthful. It is **not** written
        because resolving ``length`` to 1 makes ``format_data_type_char`` render
        ``CHAR(1)`` instead of the bare ``CHAR``: the same column in different
        bytes, which rule 6 of the cross-repo contract does not permit for its
        own sake. The asymmetry is recorded in that method's docstring and is one
        line to close if the maintainer wants it.
        """
        declared = dialect.type_parameter_defaults()
        assert "char" not in declared
        assert dialect.parse_type("CHAR").length == 1
        assert dialect.format_data_type(
            CharType(dialect))[0] != dialect.format_data_type(
            CharType(dialect, length=1))[0]
        doc = type(dialect).format_data_type_char.__doc__ or ""
        assert "one line to reverse" in doc, doc

    def test_varchar_is_refused_by_the_server_so_nothing_is_declared(self, dialect):
        """``VARCHAR2`` bare is ``ORA-00906`` on both servers.

        Oracle's own summary is "You must specify size for a ``VARCHAR2``" and
        the server agrees, so there is no bare ``VARCHAR2`` column for a catalog
        to report a width for. The ``VARCHAR2(4000)`` this dialect writes is its
        own ``MAX_STRING_SIZE = STANDARD`` ceiling, chosen so the concept still
        renders — a decision, not a fact read back — which is why it must not
        appear as a server default.
        """
        assert "varchar" not in dialect.type_parameter_defaults()
        assert dialect.format_data_type(
            VarCharType(dialect))[0] == "VARCHAR2(4000)"
        with pytest.raises(ValueError, match="ORA-00906"):
            dialect.parse_type("VARCHAR2")

    def test_interval_is_not_a_declaration_and_the_reason_is_the_shape(self,
                                                                       dialect):
        """Three measurements say it does not fit, and the third is the real one.

        * a bare ``INTERVAL`` is ``ORA-30089`` on both servers, so there is no
          unsized interval for Oracle to have supplied anything for;
        * ``INTERVAL DAY TO SECOND`` **is** accepted and reports
          ``DATA_PRECISION=2``, ``DATA_SCALE=6`` — so the two numbers are
          genuinely documented (``day_precision`` "The default is 2",
          ``fractional_seconds_precision`` "The default is 6"; NLSPG 4.2.2.2);
        * and yet the value is a whole qualifier ``DAY(2) TO SECOND(6)``, not a
          parameter. ``fields`` is a member of the closed SQL:2016
          :class:`IntervalQualifier` vocabulary, so a declaration would need a
          key standing for a qualifier and the two precisions inside it would
          have nowhere to land.

        Oracle also has two interval types and this dialect picks the wider one
        for an unqualified declaration — ``DAY TO SECOND`` over ``YEAR TO
        MONTH`` — which is a choice made here rather than a value read back. That
        is the difference between ``FLOAT`` (a server default, declarable) and
        ``INTERVAL`` (a backend decision, not declarable), and it is why this
        concept is documentation rather than an entry.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html
        """
        assert "interval" not in dialect.type_parameter_defaults()
        doc = type(dialect).format_data_type_interval.__doc__ or ""
        assert "ORA-30089" in doc, doc
        assert "ORA-00963" in doc, doc
        assert "day_precision" in doc and "The default is 2" in doc, doc
        assert "fractional_seconds_precision" in doc, doc
        assert "The default is 6" in doc, doc
        # ... and the choice is visible in the rendered SQL, which is unchanged.
        assert dialect.format_data_type(
            IntervalType(dialect))[0] == "INTERVAL DAY(2) TO SECOND(6)"
        assert dialect.format_data_type(
            IntervalType(dialect, fields="YEAR TO MONTH")
        )[0] == "INTERVAL YEAR TO MONTH"

    def test_real_declares_nothing_but_does_take_unsigned(self, dialect):
        """``REAL`` declares no **parameter**, and the one field it does carry —
        ``unsigned`` — is refused.

        Its ``FLOAT(63)`` is the ANSI conversion table's *mapping* of one concept
        onto Oracle's word (note 4), not a default for an absent parameter:
        ``type_parameter_defaults()`` has no ``real`` entry, so there is nothing
        for a declaration to fill in and nothing for a server default to resolve
        into. The server confirms the point, reporting a ``REAL`` column back as
        ``DATA_TYPE='FLOAT'``, ``DATA_PRECISION = 63`` (measured on 21c and on
        26ai Free, which agree).

        ``PARAMETERS`` is **not** empty, and saying otherwise would be a false
        claim about a class: core's ``numeric.py`` gives ``RealType`` exactly one
        field, ``unsigned``, because MySQL and MariaDB document ``UNSIGNED`` in
        this word's slot (``REAL[(M,D)] [SIGNED | UNSIGNED | ZEROFILL]``). Oracle
        does not, so the honest answer is a refusal rather than a bare
        ``FLOAT(63)`` that silently discards the declaration — see
        ``_refuse_unsigned_numeric``.
        """
        assert RealType.PARAMETERS == ("unsigned",)
        assert not hasattr(RealType(dialect), "precision")
        assert dialect.format_data_type(RealType(dialect))[0] == "FLOAT(63)"
        assert "real" not in dialect.type_parameter_defaults()

    def test_double_declares_nothing_though_it_takes_unsigned(self, dialect):
        """``DoubleType`` **does** carry a parameter — ``unsigned`` — and that is
        a reason not to declare it, not a licence to.

        ``DOUBLE PRECISION``'s ``FLOAT(126)`` is the ANSI conversion table's
        mapping of the concept (note 3), the same as ``float``'s, and the class
        has no precision field to resolve into. Declaring ``double`` would
        therefore assert something about a field it does not have, while
        ``double``'s actual identity field is ``unsigned`` — which Oracle has no
        way to store (``FLOAT`` takes no ``UNSIGNED`` modifier), so there is no
        server answer to declare for that one either.

        Worth pinning in this direction because the empty-``PARAMETERS`` claim
        would otherwise look like a good generalisation from ``RealType``, and it
        is false: core's ``numeric.py`` gives ``DoubleType`` an ``unsigned``.

        The second half is the paradigm rule: the field is in ``PARAMETERS``, so
        it is part of the type's identity, and this dialect must therefore either
        honour it or refuse it. Oracle cannot express it, so it is **refused**
        with a message naming it — writing ``FLOAT(126)`` for an unsigned request
        would hand back a signed column and report success, which is the silent
        loss the refusal exists to prevent. This assertion is what changed when
        the field arrived on ``double``; it used to assert the *same* rendered SQL
        either way, which was the violation.
        """
        assert "double" not in dialect.type_parameter_defaults()
        assert dialect.format_data_type(DoubleType(dialect))[0] == "FLOAT(126)"
        with pytest.raises(UnsupportedFeatureError) as caught:
            dialect.format_data_type(DoubleType(dialect, unsigned=True))
        message = str(caught.value)
        assert "unsigned" in message, message
        assert "ORA-00907" in message, message          # measured on 21c
        assert "ORA-03062" in message, message          # measured on 26ai
        assert caught.value.suggestion and "CHECK" in caught.value.suggestion

    def test_the_declaration_reads_the_same_number_the_range_check_bounds(
            self, dialect):
        """One fact, one home.

        The upper bound of the ``FLOAT`` range check and the value written for an
        absent precision are both 126 because both read
        :meth:`type_parameter_defaults`. A future edit to one and not the other
        would let this backend accept a precision its own fallback would never
        write, or write one it would refuse — which is exactly the drift this
        mechanism exists to remove.
        """
        sql, _ = dialect.format_data_type(FloatType(dialect))
        assert sql == f"FLOAT({dialect.type_parameter_defaults()['float']['precision']})"
        assert dialect.format_data_type(
            FloatType(dialect, precision=126))[0] == sql
        with pytest.raises(ValueError, match="1-126"):
            dialect.format_data_type(FloatType(dialect, precision=127))


# --- Oracle has no UUID data type, in any release ---

class TestOracleHasNoUUIDType:
    """A wrong statement about a vendor is worse than a missing one, because
    it gets relied upon. An earlier revision of this backend's
    ``suggested_data_types()`` docstring said "Oracle added a native ``UUID``
    data type only in 23ai". That was false: 23ai added the ``UUID()``
    *function*, and Oracle has no ``UUID`` data type through 26ai. What the
    backend needs from these tests is not that the sentence is gone but that
    the substitute is sized for what Oracle actually stores."""

    def test_the_uuid_substitute_is_not_a_native_uuid_type(self, dialect):
        """Oracle's substitute is ``RAW``, never a word Oracle does not have."""
        assert dialect.suggested_data_types()["uuid"].name == "oracle_raw"
        assert "uuid" not in dialect.supports_data_types()

    def test_uuid_comes_from_suggested_data_types_not_a_formatter(self, dialect):
        """No ``format_data_type_uuid``: writing it would emit DDL Oracle's
        grammar cannot parse."""
        assert not hasattr(dialect, "format_data_type_uuid")
        assert not hasattr(dialect, "supports_data_type_uuid")


# --- D9: every core concept is rendered or named ---

class TestCoreConceptsAreDeclared:
    def test_no_core_concept_goes_undeclared(self, dialect):
        import inspect
        core_classes, seen, stack = [], set(), [DataType]
        while stack:
            klass = stack.pop()
            if klass in seen:
                continue
            seen.add(klass)
            stack.extend(klass.__subclasses__())
            if klass is not DataType and not inspect.isabstract(klass) \
                    and klass.__module__.startswith(
                        "rhosocial.activerecord.backend.expression.types"):
                core_classes.append(klass)
        rendered = set(dialect.supports_data_types())
        suggested = set(dialect.suggested_data_types())
        undeclared = sorted(
            c.name for c in core_classes
            if c.name not in rendered and c.name not in suggested
        )
        assert not undeclared, f"undeclared core concepts: {undeclared}"

    def test_xml_is_rendered_not_substituted(self, dialect):
        """Oracle has a native XML type, so it renders the XML concept rather
        than substituting text for it."""
        assert "xml" in dialect.supports_data_types()
        assert "xml" not in dialect.suggested_data_types()
        assert dialect.format_data_type(XmlType(dialect))[0] == "XMLTYPE"

    def test_interval_is_rendered_not_substituted(self, dialect):
        assert "interval" in dialect.supports_data_types()
        assert "interval" not in dialect.suggested_data_types()
        assert dialect.format_data_type(
            IntervalType(dialect, fields="YEAR TO MONTH"))[0] == "INTERVAL YEAR TO MONTH"

    def test_custom_is_rendered(self, dialect):
        """parse_type yields CustomType for every unmodelled name, so it must
        be renderable for an introspected schema to be written back."""
        assert "custom" in dialect.supports_data_types()


# --- D9: spelling gates ---

class TestSpellingGates:
    #: Every concept that declares a closed SPELLINGS tuple.
    _SPELLED = [
        IntegerType, SmallIntType, BigIntType, BooleanType,
        CharType, VarCharType, TextType, BlobType,
    ]

    def test_default_spelling_always_renders(self, dialect):
        """A type built the ordinary way must work on this backend."""
        for klass in self._SPELLED:
            sql, _ = dialect.format_data_type(klass(dialect))
            assert sql, f"{klass.__name__} renders empty"

    def test_every_declared_spelling_renders(self, dialect):
        """Oracle normalises each concept to its own word rather than refusing
        a spelling, so all of them produce the same Oracle type."""
        for klass in self._SPELLED:
            for spelling in klass.SPELLINGS:
                sql, _ = dialect.format_data_type(klass(dialect, spelling=spelling))
                assert sql

    def test_a_spelling_outside_the_closed_list_is_refused_and_named(self, dialect):
        for klass in self._SPELLED:
            with pytest.raises(TypeError) as excinfo:
                dialect.format_data_type(klass(dialect, spelling="DROP TABLE t"))
            assert "'DROP TABLE t'" in str(excinfo.value), (
                f"{klass.__name__} must name the offending spelling, "
                f"got: {excinfo.value}"
            )

    @pytest.mark.parametrize("factory,expected", [
        (lambda: IntegerType(spelling="int"), "NUMBER(10)"),
        (lambda: SmallIntType(spelling="int2"), "NUMBER(5)"),
        (lambda: BigIntType(spelling="int8"), "NUMBER(19)"),
        (lambda: CharType(length=9, spelling="character"), "CHAR(9)"),
        (lambda: VarCharType(length=9, spelling="character varying"), "VARCHAR2(9)"),
        (lambda: TextType(spelling="clob"), "CLOB"),
        (lambda: BlobType(spelling="bytea"), "BLOB"),
    ])
    def test_normalisation_is_stable_across_spellings(self, dialect, factory, expected):
        assert dialect.format_data_type(factory())[0] == expected

    @pytest.mark.parametrize("spelling", ["boolean", "bool"])
    def test_the_boolean_spelling_still_normalises(self, dialect, spelling):
        """Both spellings reach the same Oracle column on both sides of the
        23ai gate — the gate changes *which* column, never whether the concept
        renders. See :class:`TestNativeBooleanColumnType`."""
        assert dialect.format_data_type(
            BooleanType(spelling=spelling))[0] == "BOOLEAN"
        pre = OracleDialect(version=(21, 0, 0))
        assert pre.format_data_type(
            BooleanType(spelling=spelling))[0] == "NUMBER(1)"

    def test_number_types_keep_their_width(self, dialect):
        """One concept is one storage: the spelling never changes the width."""
        assert dialect.format_data_type(IntegerType(spelling="int"))[0] == \
            dialect.format_data_type(IntegerType())[0]
        assert dialect.format_data_type(BigIntType(spelling="int8"))[0] == "NUMBER(19)"

    def test_char_and_varchar_stay_different_under_any_spelling(self, dialect):
        assert dialect.format_data_type(CharType(length=4, spelling="character"))[0] \
            == "CHAR(4)"
        assert dialect.format_data_type(
            VarCharType(length=4, spelling="character varying"))[0] == "VARCHAR2(4)"


# --- the re-parented identities, and the classes that are now redundant ---

class TestReParentedIdentities:
    def test_char_is_fixed_length_not_variable(self, dialect):
        """Oracle ``CHAR`` is blank-padded; ``VARCHAR2`` is not. The distinction
        survives the deletion of ``OracleCharType`` / ``OracleVarChar2Type``
        intact, because it is ``CharType`` versus ``VarCharType`` — two core
        concepts (D6) — and each already renders its own Oracle word."""
        assert dialect.format_data_type(CharType(dialect, length=8))[0] == "CHAR(8)"
        assert dialect.format_data_type(
            VarCharType(dialect, length=8))[0] == "VARCHAR2(8)"
        assert not isinstance(CharType(length=8), VarCharType)
        assert dialect.parse_type("CHAR(8)") != dialect.parse_type("VARCHAR2(8)")

    def test_a_declared_string_equals_the_introspected_one(self, dialect):
        """The reason ``OracleCharType`` / ``OracleVarChar2Type`` are gone. Both
        rendered byte for byte what ``format_data_type_char`` /
        ``format_data_type_varchar`` write, so ``__eq__`` — which is class
        identity — reported a fixed-length column as changed against the very
        declaration that produced it."""
        for klass, raw in ((CharType, "CHAR(10)"), (VarCharType, "VARCHAR2(10)")):
            assert klass(dialect, length=10) == dialect.parse_type(raw), raw

    def test_long_is_not_clob(self, dialect):
        """LONG has no LOB locator and a 2GB ceiling; it is the deprecated
        large-character type, not another word for CLOB."""
        from rhosocial.activerecord.backend.impl.oracle.expression.types import (
            OracleLongType,
        )
        assert isinstance(OracleLongType(), TextType)
        assert dialect.format_data_type(OracleLongType())[0] == "LONG"

    def test_raw_is_a_bounded_byte_string(self, dialect):
        assert isinstance(OracleRawType(length=16), VarBinaryType)
        assert not isinstance(OracleRawType(length=16), BlobType)

    def test_nvarchar2_is_a_varchar_with_a_charset(self, dialect):
        """The national character set is a field the framework does not model
        at the type layer, so the concept stays VarCharType."""
        from rhosocial.activerecord.backend.impl.oracle.expression.types import (
            OracleNVarChar2Type,
        )
        assert isinstance(OracleNVarChar2Type(length=30), VarCharType)
        assert dialect.format_data_type(OracleNVarChar2Type(length=30))[0] \
            == "NVARCHAR2(30)"

    def test_nclob_is_not_clob(self, dialect):
        """``NCLOB`` is the national character set's unbounded string, which is
        the reason it earns a prefixed name: ``format_data_type_text`` writes
        ``CLOB`` and there is no other way to say the national one."""
        from rhosocial.activerecord.backend.impl.oracle.expression.types import (
            OracleNClobType,
        )
        assert isinstance(OracleNClobType(), TextType)
        assert dialect.format_data_type(OracleNClobType())[0] == "NCLOB"
        assert dialect.format_data_type(OracleNClobType())[0] != \
            dialect.format_data_type(TextType(dialect))


# --- a native BOOLEAN column type arrived in 23ai ---

class TestNativeBooleanColumnType:
    """``BOOLEAN`` became a **column** type in 23ai, so the ``boolean`` concept
    renders a different column depending on the release.

    Evidence for the boundary, both halves measured on the wired servers:

    * ``CREATE TABLE t (a BOOLEAN)`` is ``ORA-00902: invalid datatype`` on 21c
      and reports ``DATA_TYPE = 'BOOLEAN'`` on 23ai/26ai (23.26.1.0.0);
    * Oracle's own release documentation puts it in 23ai — "Oracle Database 23ai
      introduces the new ``BOOLEAN`` data type", and "Some features (like SQL
      domains or BOOLEAN) only work in Oracle Database 23ai". The 26ai
      references list it among the built-in data types (code 252) and describe it
      as a new feature "in release 26ai", because 26ai is the branding of the
      23.0 line it arrived in;
    * ``PRODUCT_COMPONENT_VERSION`` reports ``VERSION = 23.0.0.0.0`` and
      ``VERSION_FULL = 23.26.1.0.0`` on that server, which is why the gate is
      stated against the version number rather than a product name — a product
      name is not a thing a tuple comparison can be made against.

    https://docs.oracle.com/en/learn/db23ai-sql-features/index.html
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    """

    @pytest.mark.parametrize("version,sql", [
        ((18, 0, 0), "NUMBER(1)"),
        ((19, 0, 0), "NUMBER(1)"),
        ((21, 0, 0), "NUMBER(1)"),
        ((23, 0, 0), "BOOLEAN"),
        ((26, 0, 0), "BOOLEAN"),
    ])
    def test_the_rendering_follows_the_release(self, version, sql):
        d = OracleDialect(version=version)
        assert d.format_data_type(BooleanType(d))[0] == sql
        assert d.format_data_type(BooleanType(d, spelling="bool"))[0] == sql

    def test_an_unadapted_dialect_renders_the_everywhere_legal_form(self):
        """``OracleDialect().version`` raises ``DialectNotAdaptedException`` and
        that behaviour is unchanged; DDL generation for a not-yet-adapted
        dialect must not start raising here because of a gate. ``NUMBER(1)`` is
        legal on every release — including every one with a native ``BOOLEAN`` —
        so it cannot mislead anything reading the emitted DDL."""
        from rhosocial.activerecord.backend.dialect.exceptions import (
            DialectNotAdaptedException,
        )
        d = OracleDialect()
        with pytest.raises(DialectNotAdaptedException):
            _ = d.version
        assert d.format_data_type(BooleanType(d))[0] == "NUMBER(1)"

    def test_the_two_renders_are_different_columns_not_two_spellings(self):
        """Stated rather than assumed: Oracle's ``BOOLEAN`` "comprises the
        distinct truth values True and False", and "unless prohibited by a
        ``NOT NULL`` constraint, the boolean data type also supports the truth
        value ``UNKNOWN`` as the null value" — the standard's three-valued
        logic by name, where ``NUMBER(1)`` is a number of which only 0 and 1 are
        valid. That is why the gate changes the *column* rather than
        normalising a word, and why ``supports_data_type_boolean`` is ``True``
        everywhere: the concept renders everywhere, as one of two columns."""
        pre, post = OracleDialect(version=(21, 0, 0)), OracleDialect(
            version=(23, 0, 0))
        assert pre.supports_data_type_boolean() is True
        assert post.supports_data_type_boolean() is True
        assert pre.supports_boolean_type() is False
        assert post.supports_boolean_type() is True

    @pytest.mark.parametrize("raw", ["BOOLEAN", "BOOL", "boolean"])
    def test_parse_reads_the_native_column_type(self, dialect, raw):
        """A ``BOOLEAN`` column introspects as ``DATA_TYPE = 'BOOLEAN'`` and
        ``parse_type`` has to reach the boolean concept from it — not
        ``CustomType``, which is where every unmodelled name lands and which
        would render the bare word back by accident rather than by
        understanding. ``BOOL`` is Oracle's documented abbreviation of
        ``BOOLEAN``."""
        parsed = dialect.parse_type(raw)
        assert type(parsed) is BooleanType, raw

    def test_a_native_boolean_column_round_trips(self, dialect):
        sql = dialect.format_data_type(BooleanType(dialect))[0]
        assert dialect.format_data_type(dialect.parse_type(sql))[0] == sql


class TestNativeJsonColumnType:
    """``JSON`` became a **column** type in 21c, so both JSON concepts render a
    different column depending on the release.

    Evidence for the boundary, both halves measured on the wired servers:

    * ``CREATE TABLE t (a JSON)`` is accepted on 21c and on 26ai, and reports
      ``DATA_TYPE = 'JSON'`` back from the catalog, where 18c rejects the word.
      On the servers measured the statement was only accepted in a **non-SYSTEM
      tablespace**: ``SYSTEM`` is not ASSM and raises ``ORA-43853`` for the same
      statement that succeeds under ``TABLESPACE USERS``;
    * Oracle's own documentation puts the type in 21c and describes it as the
      "new built-in data type" replacing the older ``CLOB``/``VARCHAR2`` +
      ``IS JSON`` idiom. The 26ai JSON overview calls it native support "with
      relational database features, including transactions, indexing,
      declarative querying, and views";
    * ``PRODUCT_COMPONENT_VERSION`` reports the same tuple on both servers the
      gate is compared against, so the boundary is stated as a version number —
      the same reasoning that makes the ``BOOLEAN`` gate a number rather than a
      product name.

    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    https://docs.oracle.com/en/database/oracle/oracle-database/26/adjsn/overview-json-oracle-ai-database.html
    """

    @pytest.mark.parametrize("version,sql", [
        ((12, 0, 0), "VARCHAR2(4000)"),
        ((18, 0, 0), "VARCHAR2(4000)"),
        ((19, 0, 0), "VARCHAR2(4000)"),
        ((20, 0, 0), "VARCHAR2(4000)"),
        ((21, 0, 0), "JSON"),
        ((23, 0, 0), "JSON"),
        ((26, 0, 0), "JSON"),
    ])
    def test_json_rendering_follows_the_release(self, version, sql):
        d = OracleDialect(version=version)
        assert d.format_data_type(JsonType(d))[0] == sql

    @pytest.mark.parametrize("version,sql", [
        ((12, 0, 0), "CLOB"),
        ((18, 0, 0), "CLOB"),
        ((19, 0, 0), "CLOB"),
        ((20, 0, 0), "CLOB"),
        ((21, 0, 0), "JSON"),
        ((23, 0, 0), "JSON"),
        ((26, 0, 0), "JSON"),
    ])
    def test_jsonb_rendering_follows_the_release(self, version, sql):
        """``jsonb`` is PostgreSQL's binary JSON and Oracle has no equivalent in
        any release — but from 21c it has ``JSON``, which is where a document
        belongs, so the same gate applies and ``CLOB`` stops being the answer.

        The method used to carry a comment saying exactly that ("21c+ uses
        native JSON, otherwise CLOB") while the code followed only the second
        half. A reader of the code and a reader of the running dialect
        disagreed about which was true, which is the shape of defect worth a
        test on both sides of the boundary rather than a corrected comment.
        """
        d = OracleDialect(version=version)
        assert d.format_data_type(JsonBType(d))[0] == sql

    def test_an_unadapted_dialect_renders_the_everywhere_legal_form(self):
        """``OracleDialect().version`` raises and that is unchanged.

        Below 21c the two words are both legal on every release, so an
        unadapted dialect keeps writing them and DDL generation cannot start
        raising because of a gate — the same reasoning the ``BOOLEAN`` gate
        uses, with the difference that there the pre-23ai form is also the
        legal one everywhere.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            DialectNotAdaptedException,
        )
        d = OracleDialect()
        with pytest.raises(DialectNotAdaptedException):
            _ = d.version
        assert d.format_data_type(JsonType(d))[0] == "VARCHAR2(4000)"
        assert d.format_data_type(JsonBType(d))[0] == "CLOB"

    def test_the_two_json_concepts_differ_below_the_boundary(self):
        """Stated rather than assumed: ``json`` and ``jsonb`` render *different*
        columns before 21c, because one has a stated ceiling and the other has
        none. ``VARCHAR2(4000)`` is Oracle's ``MAX_STRING_SIZE = STANDARD``
        maximum, which is a real limit the model author can read and declare
        around; ``CLOB`` is unbounded, and giving ``jsonb`` the 4000 form would
        invent a ceiling the concept does not have.

        Above the boundary they converge on ``JSON`` — the one native column
        Oracle has for a document, whatever the source dialect called it.
        """
        pre = OracleDialect(version=(18, 0, 0))
        assert pre.format_data_type(JsonType(pre))[0] == "VARCHAR2(4000)"
        assert pre.format_data_type(JsonBType(pre))[0] == "CLOB"

        post = OracleDialect(version=(21, 0, 0))
        assert post.format_data_type(JsonType(post))[0] == "JSON"
        assert post.format_data_type(JsonBType(post))[0] == "JSON"


# --- W4: type-param equality (dialect_options bag removed) ---

class TestDialectOptionsRemoved:

    def test_constructor_rejects_dialect_options(self):
        with pytest.raises(TypeError):
            OracleRawType(length=16, dialect_options={"a": 1})

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
