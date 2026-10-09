# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_column_suggestions.py
"""Oracle's column-type suggestion table, and the operations it narrows.

Three versions are exercised rather than one, because the whole content of
:meth:`OracleColumnSuggestionMixin.supports_column_operation` is two version
boundaries: 21c for the native ``JSON`` type and 23ai for the native ``BOOLEAN``
column type. The versions are the measured ones from the live sweep --
18.4 XE, 21.3 XE, 26ai Free 23.26.1 (the last of which reports
``version_full = 23.26.1.0.0``, which is why the boundary is written ``23.26``
here and ``(23, 0, 0)`` in the mixin).
"""

import datetime
import decimal
import enum
import typing
import uuid

import pytest
from rhosocial.activerecord.backend.expression.column_suggestions import (
    COLUMN_TYPE_ENTRIES,
    UNSUPPORTED,
    ColumnTypeResolutionError,
)
from rhosocial.activerecord.backend.expression.column_types import (
    ArrayColumn,
    BinaryColumn,
    BooleanColumn,
    ColumnBase,
    DateTimeColumn,
    DecimalColumn,
    FloatColumn,
    IntegerColumn,
    JSONColumn,
    NumericColumn,
    StringColumn,
    UUIDColumn,
)

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

pytestmark = [pytest.mark.feature, pytest.mark.backend]

#: The three measured servers, as ``(label, version)``.
ORACLE_18C = (18, 4, 0)
ORACLE_21C = (21, 3, 0)
ORACLE_23AI = (23, 26, 1)


@pytest.fixture
def dialect():
    return OracleDialect(version=ORACLE_21C)


class TestTableCompleteness:
    """The protocol's one non-negotiable: every entry, answered."""

    def test_answers_every_core_entry(self, dialect):
        table = dialect.suggested_column_types()
        assert set(table) == set(COLUMN_TYPE_ENTRIES)

    def test_no_entry_is_unsupported_or_none(self, dialect):
        # Oracle declines to use the sentinel anywhere. Asserted so that adding
        # one later is a deliberate act with a reason attached, rather than a
        # way to make a completeness test pass by asserting something weaker.
        for entry, column_class in dialect.suggested_column_types().items():
            assert column_class is not UNSUPPORTED, entry
            assert column_class is not None, entry

    def test_every_answer_is_a_column_class(self, dialect):
        # Also the guard against DataType-side facts leaking in: the Oracle
        # binary float classes and OracleTimestampLtzType are DataTypes, not
        # ColumnBase subclasses, so they cannot appear here.
        for entry, column_class in dialect.suggested_column_types().items():
            assert isinstance(column_class, type), entry
            assert issubclass(column_class, ColumnBase), entry

    def test_no_oracle_extension_entries(self, dialect):
        # The protocol allows extending the entry list; Oracle does not, and a
        # table that grew an undeclared key would silently change the answer.
        assert set(dialect.suggested_column_types()) == set(COLUMN_TYPE_ENTRIES)

    def test_the_table_is_a_copy(self, dialect):
        # Mutating the returned mapping must not reach the class attribute that
        # every later resolution reads.
        table = dialect.suggested_column_types()
        table[int] = StringColumn
        assert dialect.suggested_column_types()[int] is IntegerColumn


class TestBaselineEntries:
    """The eleven entries every backend agrees on."""

    @pytest.mark.parametrize(
        "annotation, expected",
        [
            (bool, BooleanColumn),
            (int, IntegerColumn),
            (float, FloatColumn),
            (decimal.Decimal, DecimalColumn),
            (str, StringColumn),
            (bytes, BinaryColumn),
            (bytearray, BinaryColumn),
            (datetime.date, DateTimeColumn),
            (datetime.time, DateTimeColumn),
            (datetime.datetime, DateTimeColumn),
            (uuid.UUID, UUIDColumn),
            (enum.Enum, StringColumn),
        ],
    )
    def test_entry(self, dialect, annotation, expected):
        assert dialect.suggested_column_types()[annotation] is expected

    def test_datetime_timedelta_is_numeric(self, dialect):
        # Core has no interval column class; Oracle *does* have native INTERVAL
        # DAY TO SECOND at the DDL layer. So this answer is limited by the
        # framework, not by Oracle -- which is why it is recorded as
        # NumericColumn rather than as UNSUPPORTED.
        assert dialect.suggested_column_types()[datetime.timedelta] is NumericColumn

    def test_float_keeps_the_core_class_not_the_ieee_types(self, dialect):
        # OracleBinaryFloatType / OracleBinaryDoubleType are DataType facts
        # (a storage width), not column facts. `float` stays on the core class.
        assert dialect.suggested_column_types()[float] is FloatColumn
        assert dialect.suggested_column_types()[decimal.Decimal] is DecimalColumn


class TestOracleDeviations:
    """The five entries where Oracle differs from the portable default."""

    @pytest.mark.parametrize("annotation", [dict, list, tuple, set, frozenset])
    def test_containers_are_json(self, dialect, annotation):
        # dict -> JSONColumn (CLOB below 21c, native JSON from 21c).
        # list/tuple/set/frozenset -> JSONColumn too: Oracle has no array column
        # type in any release, so ArrayColumn would offer array_length / unnest
        # on a server with nothing to unnest.
        assert dialect.suggested_column_types()[annotation] is JSONColumn

    def test_no_entry_answers_array_column(self, dialect):
        # The negative form of the same claim, so the reason it cannot change by
        # accident is on the test rather than only in the docstring.
        assert ArrayColumn not in dialect.suggested_column_types().values()


class TestResolution:
    """Resolution through the dialect, including the failures."""

    def test_resolves_an_entry(self, dialect):
        assert dialect.column_class_for(dict) is JSONColumn

    def test_peels_optional_and_annotated(self, dialect):
        assert dialect.column_class_for(typing.Optional[list]) is JSONColumn
        assert dialect.column_class_for(typing.Annotated[str, "note"]) is StringColumn

    def test_walks_subclasses(self, dialect):
        class Weekday(int, enum.Enum):
            MONDAY = 1

        class Tag(str):
            pass

        # An enum subclass answers the enum entry even though it also subclasses
        # int; a plain subclass answers its base.
        assert dialect.column_class_for(Weekday) is StringColumn
        assert dialect.column_class_for(Tag) is StringColumn

    def test_unregistered_annotation_fails_at_definition_time(self, dialect):
        with pytest.raises(ColumnTypeResolutionError):
            dialect.column_class_for(complex)

    def test_explicit_column_type_bypasses_the_table(self, dialect):
        # The escape hatch: naming a column class answers without consulting
        # the table at all.
        class UseColumnType:
            column_class = StringColumn

        assert dialect.column_class_for(complex, UseColumnType()) is StringColumn


class TestStringNarrowing:
    """``ILIKE`` does not exist on Oracle, on any release."""

    @pytest.mark.parametrize("version", [ORACLE_18C, ORACLE_21C, ORACLE_23AI])
    def test_ilike_narrowed_on_every_release(self, version):
        dialect = OracleDialect(version=version)
        assert dialect.supports_column_operation("StringColumn", "ilike") is False

    @pytest.mark.parametrize("version", [ORACLE_18C, ORACLE_21C, ORACLE_23AI])
    def test_like_still_available(self, version):
        # The narrowing must not swallow the operation Oracle does have:
        # ORA-00920 on 18c/21c and ORA-03049 on 23c are about ILIKE only.
        dialect = OracleDialect(version=version)
        assert dialect.supports_column_operation("StringColumn", "like") is True

    def test_the_narrowed_name_is_a_real_operation(self):
        from rhosocial.activerecord.backend.expression.mixins import (
            StringPatternPredicateMixin,
        )

        assert hasattr(StringColumn, "ilike")
        assert hasattr(StringPatternPredicateMixin, "ilike")


class TestJSONNarrowing:
    """Below 21c the JSON operations that need the JSON *type* are gone."""

    def test_whole_column_equality_and_concat_narrowed_on_18c(self):
        dialect = OracleDialect(version=ORACLE_18C)
        assert dialect.supports_column_operation("JSONColumn", "__eq__") is False
        assert dialect.supports_column_operation("JSONColumn", "concat") is False

    @pytest.mark.parametrize("version", [ORACLE_21C, ORACLE_23AI])
    def test_whole_column_equality_and_concat_available_from_21c(self, version):
        dialect = OracleDialect(version=version)
        assert dialect.supports_column_operation("JSONColumn", "__eq__") is True
        assert dialect.supports_column_operation("JSONColumn", "concat") is True

    @pytest.mark.parametrize("version", [ORACLE_18C, ORACLE_21C, ORACLE_23AI])
    def test_path_access_survives_the_clob_route(self, version):
        # The reason the 18c entry is JSONColumn and not UNSUPPORTED: this part
        # of the battery was measured working against a CLOB.
        dialect = OracleDialect(version=version)
        assert dialect.supports_column_operation("JSONColumn", "json_path") is True
        assert dialect.supports_column_operation("JSONColumn", "json_value") is True
        assert dialect.supports_column_operation("JSONColumn", "is_null") is True
        assert dialect.supports_column_operation("JSONColumn", "in_") is True

    def test_narrowing_is_scoped_to_the_json_column(self):
        # __eq__ on any other column class is untouched -- the narrowing names a
        # (column class, operation) pair, not an operation.
        dialect = OracleDialect(version=ORACLE_18C)
        assert dialect.supports_column_operation("StringColumn", "__eq__") is True
        assert dialect.supports_column_operation("IntegerColumn", "__eq__") is True


class TestBooleanNarrowing:
    """``IS TRUE`` needs the native BOOLEAN column type, which is 23ai+."""

    @pytest.mark.parametrize("version", [ORACLE_18C, ORACLE_21C])
    @pytest.mark.parametrize("op", ["is_true", "is_false"])
    def test_narrowed_before_23ai(self, version, op):
        # Storage below the boundary is NUMBER(1), where `flag IS TRUE` is a
        # syntax error. The `= 1` repair is a Phase 2b rendering decision.
        dialect = OracleDialect(version=version)
        assert dialect.supports_column_operation("BooleanColumn", op) is False

    @pytest.mark.parametrize("version", [ORACLE_23AI, (23, 0, 0)])
    @pytest.mark.parametrize("op", ["is_true", "is_false"])
    def test_available_from_23ai(self, version, op):
        dialect = OracleDialect(version=version)
        assert dialect.supports_column_operation("BooleanColumn", op) is True

    @pytest.mark.parametrize("op", ["is_true", "is_false"])
    def test_narrowing_does_not_touch_the_rest_of_the_boolean_surface(self, op):
        # & / | / ~ / comparison stay available: they are AND / OR / NOT / =,
        # which the NUMBER(1) column renders without help.
        dialect = OracleDialect(version=ORACLE_18C)
        assert dialect.supports_column_operation("BooleanColumn", op) is False
        for op_name in ("__and__", "__or__", "__invert__", "__eq__"):
            assert dialect.supports_column_operation("BooleanColumn", op_name) is True

    def test_the_narrowed_names_are_real_operations(self):
        for op in ("is_true", "is_false"):
            assert hasattr(BooleanColumn, op)

    def test_unadapted_dialect_answers_on_the_pre_23ai_side(self):
        # `OracleDialect()` has no version and `.version` raises. It is answered
        # as pre-23ai, the same branch format_data_type_boolean takes when
        # _version is None -- NUMBER(1) is legal on every release, so nothing
        # reading the emitted DDL can be misled.
        dialect = OracleDialect()
        assert dialect.supports_column_operation("BooleanColumn", "is_true") is False
        assert dialect.supports_column_operation("JSONColumn", "__eq__") is False
        # And it still answers the table, because the table has no branch in it.
        assert dialect.column_class_for(dict) is JSONColumn


class TestUnnarrowedByDefault:
    """The default is True, and the narrowing is the exception."""

    def test_operations_outside_the_narrowing_are_available(self, dialect):
        for column_name, op in [
            ("StringColumn", "like"),
            ("StringColumn", "concat"),
            ("IntegerColumn", "__add__"),
            ("DecimalColumn", "round"),
            ("DateTimeColumn", "date_trunc"),
            ("BinaryColumn", "__eq__"),
            ("UUIDColumn", "in_"),
            ("JSONColumn", "json_path"),
        ]:
            assert dialect.supports_column_operation(column_name, op) is True, (
                column_name,
                op,
            )

    def test_unknown_operation_name_is_not_a_claim_about_storage(self, dialect):
        # The method answers about the core operation set; a name it does not
        # know is not narrowed by the fact that it is unknown.
        assert dialect.supports_column_operation("StringColumn", "ilike") is False
        assert dialect.supports_column_operation("StringColumn", "nonesuch") is True
