# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/types/test_column_type.py
"""Oracle's column-type table: the class each common Python type means here.

The table was measured on the live sweep -- 18.4 XE, 21.3 XE, 26ai Free
23.26.1; see ``.claude/plan/2026-10-08/secondary-gaps-investigation.md`` and its
appendices. Those three versions appear here for one reason: to assert that the
table does **not** vary by version. On Oracle the version-dependent part of the
protocol is the storage behind a class -- the native ``JSON`` type from 21c
against a ``CLOB`` below it, ``BOOLEAN`` against ``NUMBER(1)`` from 23ai -- and
that belongs to the DDL layer, not to which class an annotation means.

No database is needed: every assertion is on the table, on the dialect's
composition, or on a never-connected dialect.
"""

import datetime
import decimal
import enum
import typing
import uuid

import pytest
from rhosocial.activerecord.backend.suggestion.protocols import ColumnTypeSuggestionSupport
from rhosocial.activerecord.backend.expression.column_types import (
    ArrayColumn,
    BinaryColumn,
    BitStringColumn,
    BooleanColumn,
    ColumnBase,
    GeometryColumn,
    IntegerColumn,
    IntervalColumn,
    IPAddressColumn,
    JSONColumn,
    MacAddressColumn,
    MultirangeColumn,
    NumericColumn,
    RangeColumn,
    StringColumn,
    TimestampColumn,
    UUIDColumn,
    VectorColumn,
)
from rhosocial.activerecord.backend.adapters import (
    AwareDatetime,
    BitString,
    Geometry,
    IPAddress,
    MacAddress,
    Multirange,
    Range,
    Vector,
)
from rhosocial.activerecord.base.field_proxy import ColumnTypeResolutionError
from rhosocial.activerecord.base.fields import UseColumnType
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.mixins.column_type import (
    ORACLE_COLUMN_TYPES,
    OracleColumnTypeMixin,
)
from rhosocial.activerecord.testsuite.feature.query.typed_column.column_helpers import (
    COMMON_TYPES,
    resolve_column_class,
)

pytestmark = [pytest.mark.feature, pytest.mark.backend]

#: The three measured servers, as ``(major, minor, patch)``.
ORACLE_18C = (18, 4, 0)
ORACLE_21C = (21, 3, 0)
ORACLE_23AI = (23, 26, 1)


@pytest.fixture
def dialect():
    return OracleDialect(version=ORACLE_21C)


class TestProtocolComposition:
    """The dialect must compose the new mixin, not the deleted one."""

    def test_the_dialect_composes_the_column_type_mixin(self, dialect):
        assert isinstance(dialect, OracleColumnTypeMixin)
        assert isinstance(dialect, ColumnTypeSuggestionSupport)
        assert issubclass(OracleDialect, ColumnTypeSuggestionSupport)

    def test_the_table_is_overridden_not_inherited(self, dialect):
        # The core mixin has no table to inherit: an un-overridden
        # suggest_column_types().table raises. What the dialect answers instead is
        # this backend's own measured table.
        assert dialect.suggest_column_types().table == ORACLE_COLUMN_TYPES
        assert dialect.suggest_column_types().table is not ORACLE_COLUMN_TYPES

    def test_the_mro_resolves_the_oracle_method(self):
        assert OracleDialect.suggest_column_types is (
            OracleColumnTypeMixin.suggest_column_types
        )


class TestTableCompleteness:
    """The protocol's one non-negotiable: every entry, answered."""

    def test_answers_every_common_entry(self, dialect):
        table = dialect.suggest_column_types().table
        assert set(table) == set(COMMON_TYPES)

    def test_no_entry_is_answered_with_none(self, dialect):
        # None is the protocol's last resort for a value family the backend
        # genuinely cannot express. Oracle declines to use it anywhere: every
        # entry has a measured pairing, 18c included. Asserted so that adding
        # one later is a deliberate act with a reason attached, rather than a
        # way to make a completeness test pass by asserting something weaker.
        for entry, column_class in dialect.suggest_column_types().table.items():
            assert column_class is not None, entry

    def test_every_answer_is_a_column_class(self, dialect):
        # Also the guard against DataType-side facts leaking in: the Oracle
        # binary float classes and OracleTimestampLtzType are DataTypes, not
        # ColumnBase subclasses, so they cannot appear here.
        for entry, column_class in dialect.suggest_column_types().table.items():
            assert isinstance(column_class, type), entry
            assert issubclass(column_class, ColumnBase), entry

    def test_no_oracle_extension_entries(self, dialect):
        # The protocol allows extending the entry list; Oracle does not, and a
        # table that grew an undeclared key would silently change the answer.
        assert set(dialect.suggest_column_types().table) == set(COMMON_TYPES)

    def test_the_table_is_a_copy(self, dialect):
        # Mutating the returned mapping must not reach the module-level table
        # that every later resolution reads.
        table = dialect.suggest_column_types().table
        table[int] = StringColumn
        assert ORACLE_COLUMN_TYPES[int] is IntegerColumn
        assert dialect.suggest_column_types().table[int] is IntegerColumn

    @pytest.mark.parametrize(
        "version", [ORACLE_18C, ORACLE_21C, ORACLE_23AI], ids=["18c", "21c", "23ai"]
    )
    def test_the_table_does_not_vary_by_version(self, version):
        # The class an annotation means is the same on every measured release;
        # what varies is the storage behind it, which the DDL layer spells.
        assert OracleDialect(version=version).suggest_column_types().table == (ORACLE_COLUMN_TYPES)


class TestBaselineEntries:
    """The portable entries every backend agrees on."""

    @pytest.mark.parametrize(
        "annotation, expected",
        [
            (bool, BooleanColumn),
            (int, IntegerColumn),
            (float, NumericColumn),
            (decimal.Decimal, NumericColumn),
            (str, StringColumn),
            (bytes, BinaryColumn),
            (bytearray, BinaryColumn),
            (datetime.date, TimestampColumn),
            (datetime.time, TimestampColumn),
            (AwareDatetime, TimestampColumn),
            (datetime.datetime, TimestampColumn),
            (uuid.UUID, UUIDColumn),
            (enum.Enum, StringColumn),
        ],
    )
    def test_entry(self, dialect, annotation, expected):
        assert dialect.suggest_column_types().table[annotation] is expected
        assert resolve_column_class(dialect, annotation) is expected

    def test_datetime_timedelta_is_an_interval(self, dialect):
        # Oracle has native INTERVAL DAY TO SECOND at the DDL layer, and core
        # now has the class that names the value. `IntervalColumn` promises no
        # arithmetic, so it is honest on both sides; Oracle's own interval
        # operations belong to an Oracle-named subclass.
        assert dialect.suggest_column_types().table[datetime.timedelta] is IntervalColumn

    def test_the_complex_families_answer_the_core_classes(self, dialect):
        table = dialect.suggest_column_types().table
        for entry, klass in (
            (Geometry, GeometryColumn),
            (Vector, VectorColumn),
            (BitString, BitStringColumn),
            (Range, RangeColumn),
            (Multirange, MultirangeColumn),
            (MacAddress, MacAddressColumn),
            (IPAddress, IPAddressColumn),
        ):
            assert table[entry] is klass

    def test_the_numeric_family_is_one_class(self, dialect):
        # OracleBinaryFloatType / OracleBinaryDoubleType are DataType facts (a
        # storage width), not column facts: the operations on a float and on a
        # Decimal are the operations of the one numeric class, and the choice
        # between NUMBER, BINARY_FLOAT and BINARY_DOUBLE is the DDL layer's.
        assert dialect.suggest_column_types().table[float] is NumericColumn
        assert dialect.suggest_column_types().table[decimal.Decimal] is NumericColumn
        assert dialect.suggest_column_types().table[int] is IntegerColumn


class TestOracleDeviations:
    """The five entries where Oracle differs from the portable default."""

    @pytest.mark.parametrize("annotation", [dict, list, tuple, set, frozenset])
    def test_containers_are_json(self, dialect, annotation):
        # dict -> JSONColumn (CLOB below 21c, native JSON from 21c).
        # list/tuple/set/frozenset -> JSONColumn too: Oracle has no array column
        # type in any release, so ArrayColumn would offer array_length / unnest
        # on a server with nothing to unnest.
        assert dialect.suggest_column_types().table[annotation] is JSONColumn
        assert resolve_column_class(dialect, annotation) is JSONColumn

    def test_no_entry_answers_array_column(self, dialect):
        # The negative form of the same claim, so the reason it cannot change by
        # accident is on the test rather than only in the docstring.
        assert ArrayColumn not in dialect.suggest_column_types().table.values()


class TestResolution:
    """Resolution through the field accessor's selection step."""

    def test_resolves_an_entry(self, dialect):
        assert resolve_column_class(dialect, dict) is JSONColumn

    def test_peels_optional_and_annotated(self, dialect):
        assert resolve_column_class(dialect, typing.Optional[list]) is JSONColumn
        assert resolve_column_class(dialect, typing.Annotated[str, "note"]) is (StringColumn)

    def test_walks_subclasses(self, dialect):
        class Weekday(int, enum.Enum):
            MONDAY = 1

        class Tag(str):
            pass

        # An enum subclass answers the enum entry even though it also subclasses
        # int; a plain subclass answers its base.
        assert resolve_column_class(dialect, Weekday) is StringColumn
        assert resolve_column_class(dialect, Tag) is StringColumn

    def test_unregistered_annotation_fails_at_definition_time(self, dialect):
        with pytest.raises(ColumnTypeResolutionError):
            resolve_column_class(dialect, complex)

    def test_explicit_column_type_bypasses_the_table(self, dialect):
        # The escape hatch: declaring a column class answers without consulting
        # the table at all.
        declared = UseColumnType(StringColumn)
        assert resolve_column_class(dialect, complex, declared) is StringColumn

    def test_an_unadapted_dialect_still_answers_the_table(self):
        # `OracleDialect()` has no version and `.version` raises. The table has
        # no version branch in it, so it answers exactly as an adapted one does;
        # the version boundaries live in the DDL layer, not here.
        dialect = OracleDialect()
        assert resolve_column_class(dialect, dict) is JSONColumn
        assert resolve_column_class(dialect, bool) is BooleanColumn


# ---------------------------------------------------------------------------
# Oracle's own value classes: the extra table and its column classes
# ---------------------------------------------------------------------------


class TestExtraColumnTypes:
    """Every value class Oracle models has a column class a bare field gets."""

    def test_the_extra_table_mirrors_the_value_classes(self):
        from rhosocial.activerecord.backend.impl.oracle.mixins.column_type import (
            ORACLE_EXTRA_COLUMN_TYPES,
        )
        from rhosocial.activerecord.backend.impl.oracle.type_values import (
            IntervalDayToSecond,
            IntervalYearToMonth,
            OracleRowID,
            OracleURowID,
            OracleVector,
            OracleXMLType,
            SDOGeometry,
        )

        assert set(ORACLE_EXTRA_COLUMN_TYPES) == {
            IntervalDayToSecond,
            IntervalYearToMonth,
            OracleRowID,
            OracleURowID,
            OracleXMLType,
            SDOGeometry,
            OracleVector,
        }

    def test_every_extra_answer_is_a_column_class(self):
        for version in (ORACLE_18C, ORACLE_21C, ORACLE_23AI):
            extra = OracleDialect(version=version).suggest_extra_column_types().table
            for entry, column_class in extra.items():
                assert isinstance(column_class, type), entry
                assert issubclass(column_class, ColumnBase), entry

    def test_every_extra_class_builds_and_renders(self):
        from rhosocial.activerecord.backend.expression.column_types import ColumnBase

        dialect = OracleDialect(version=ORACLE_23AI)
        for column_class in dialect.suggest_extra_column_types().table.values():
            column = column_class(dialect, "c")
            assert isinstance(column, ColumnBase)
            # Oracle folds unquoted identifiers to upper case.
            assert column.to_sql() == ('"C"', ())

    def test_the_extra_table_does_not_overlap_the_common_one(self):
        dialect = OracleDialect(version=ORACLE_23AI)
        assert (
            set(dialect.suggest_column_types().table) & set(dialect.suggest_extra_column_types().table)
            == set()
        )

    def test_vector_is_gated_on_23ai_like_its_adapter(self):
        from rhosocial.activerecord.backend.impl.oracle.type_values import OracleVector

        assert OracleVector in OracleDialect(version=ORACLE_23AI).suggest_extra_column_types().table
        assert OracleVector not in OracleDialect(version=ORACLE_21C).suggest_extra_column_types().table
        # No version at all: the adapter is not registered either, so the entry
        # is absent rather than naming a class for a type the server may not
        # have.
        assert OracleVector not in OracleDialect().suggest_extra_column_types().table

    def test_an_oracle_value_class_resolves_without_a_declaration(self, dialect):
        from rhosocial.activerecord.backend.expression.column_types import (
            GeometryColumn,
            XMLColumn,
        )
        from rhosocial.activerecord.backend.impl.oracle.column_types import (
            OracleIntervalColumn,
            OracleRowIdColumn,
            OracleSdoGeometryColumn,
            OracleXmlColumn,
        )
        from rhosocial.activerecord.backend.impl.oracle.type_values import (
            IntervalDayToSecond,
            OracleRowID,
            OracleXMLType,
            SDOGeometry,
        )

        assert resolve_column_class(dialect, SDOGeometry) is OracleSdoGeometryColumn
        assert resolve_column_class(dialect, IntervalDayToSecond) is OracleIntervalColumn
        assert resolve_column_class(dialect, OracleRowID) is OracleRowIdColumn
        assert resolve_column_class(dialect, OracleXMLType) is OracleXmlColumn
        # The names inherit the core floor they belong to.
        assert issubclass(OracleSdoGeometryColumn, GeometryColumn)
        assert issubclass(OracleXmlColumn, XMLColumn)

    def test_use_column_type_selects_the_named_class(self, dialect):
        from rhosocial.activerecord.backend.impl.oracle.column_types import (
            OracleSdoGeometryColumn,
            OracleVectorColumn,
        )
        from rhosocial.activerecord.backend.impl.oracle.type_values import (
            OracleVector,
            SDOGeometry,
        )

        assert (
            resolve_column_class(dialect, SDOGeometry, UseColumnType(OracleSdoGeometryColumn))
            is OracleSdoGeometryColumn
        )
        assert (
            resolve_column_class(dialect, OracleVector, UseColumnType(OracleVectorColumn))
            is OracleVectorColumn
        )


# ---------------------------------------------------------------------------
# Oracle's own column classes: the SDO operations
# ---------------------------------------------------------------------------


class TestSdoColumnOperations:
    def test_distance_to(self, dialect):
        from rhosocial.activerecord.backend.impl.oracle.column_types import (
            OracleSdoGeometryColumn,
        )

        column = OracleSdoGeometryColumn(dialect, "g")
        sql, params = column.distance_to(column).to_sql()
        assert sql == 'SDO_GEOM.SDO_DISTANCE("G", "G", ?)'
        assert params == (0.005,)

    @pytest.mark.parametrize(
        "method, name, call_param",
        [
            ("within_distance", "SDO_WITHIN_DISTANCE", "distance=5"),
            ("contains", "SDO_CONTAINS", None),
            ("inside", "SDO_INSIDE", None),
            ("relate", "SDO_RELATE", "mask=ANYINTERACT"),
        ],
    )
    def test_spatial_predicates(self, dialect, method, name, call_param):
        from rhosocial.activerecord.backend.impl.oracle.column_types import (
            OracleSdoGeometryColumn,
        )

        column = OracleSdoGeometryColumn(dialect, "g")
        args = (column, 5) if method == "within_distance" else (column,)
        sql, params = getattr(column, method)(*args).to_sql()
        expected = f'{name}("G", "G"' + (", ?" if call_param else "") + ") = ?"
        assert sql == expected
        assert params[-1] == "TRUE"
        if call_param:
            assert params[0] == call_param

    @pytest.mark.parametrize(
        "method", ["distance_to", "within_distance", "contains", "inside", "relate"]
    )
    def test_ops_fail_without_the_capability(self, dialect, method, monkeypatch):
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )
        from rhosocial.activerecord.backend.impl.oracle.column_types import (
            OracleSdoGeometryColumn,
        )

        monkeypatch.setattr(dialect, "supports_spatial_type", lambda: False)
        column = OracleSdoGeometryColumn(dialect, "g")
        args = (column, 5) if method == "within_distance" else (column,)
        with pytest.raises(UnsupportedFeatureError, match="supports_spatial_type"):
            getattr(column, method)(*args)
