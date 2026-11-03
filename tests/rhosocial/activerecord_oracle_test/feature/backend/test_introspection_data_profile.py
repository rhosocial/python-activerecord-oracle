# tests/rhosocial/activerecord_oracle_test/feature/backend/test_introspection_data_profile.py
"""The data-profile family, measured against a real server.

Two halves, as in the rest of this directory.

Offline: the three probes and the parser's conversions are driven with
synthetic ``ALL_TAB_COLUMNS`` rows through a stub collaborator. No connection
is opened. What is pinned there is the arithmetic (``NUM_NULLS`` over
``SAMPLE_SIZE``) and the one deliberate absence -- ``min_value``/``max_value``
stay ``None`` even when the RAW extremes are in the row.

Live: a table is built so every figure is arithmetic rather than guesswork::

    id      NUMBER          i, unique            -> 1000 distinct, no nulls
    code    VARCHAR2(10)    "a"/"b"/"c" by i % 3-> 3 distinct, no nulls
    amount  NUMBER          i, unique            -> 1000 distinct, no nulls
    note    VARCHAR2(10)    "x" when i % 5 == 0, "y" when i % 5 == 1,
                            NULL otherwise       -> 2 distinct, 600 nulls
                                                 -> 60% null

The statistics are gathered explicitly, because the family reads them and a
column with no statistics is precisely the case where the answer must be
``None`` rather than zero. The live assertions are the ones that must hold on
any server: the ratio, the low-cardinality counts, the ordering of the value
listing. Anything that depends on how much of the table the optimizer chose to
sample is asserted in the direction it must hold and says why.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
    OracleIntrospector,
)
from rhosocial.activerecord.backend.introspection.types import (
    ColumnDataProfile,
    ColumnValueFrequency,
)

TABLE = "INTRO_PROFILE_ORDERS"
UNANALYSED_TABLE = "INTRO_PROFILE_UNANALYSED"
CODES = ("a", "b", "c")
ROW_COUNT = 1000


# --------------------------------------------------------------------- #
# Offline collaborators
# --------------------------------------------------------------------- #


class StubBackend:
    """The two backend attributes the introspector mixin actually reads."""

    def __init__(self, username: str = "ar_crm") -> None:
        self.config = SimpleNamespace(username=username)
        self._version = (23, 0, 0)
        self.dialect = OracleDialect(version=(23, 0, 0))


class StubExecutor:
    """An executor that answers nothing: the parsers are pure functions."""

    def execute(self, sql, params=()):
        return []


def _introspector() -> OracleIntrospector:
    return OracleIntrospector(StubBackend(), StubExecutor())


#: An analysed column, exactly as ``ALL_TAB_COLUMNS`` reports one. The RAW
#: extremes are present on purpose: the parser must ignore them.
ANALYSED_ROW = {
    "OWNER": "AR_CRM",
    "TABLE_NAME": TABLE,
    "COLUMN_NAME": "NOTE",
    "DATA_TYPE": "VARCHAR2",
    "NUM_DISTINCT": 2,
    "NUM_NULLS": 600,
    "SAMPLE_SIZE": 1000,
    "LAST_ANALYZED": datetime(2026, 1, 2, 3, 4, 5),
    "HISTOGRAM": "FREQUENCY",
    "LOW_VALUE": b"\x78",
    "HIGH_VALUE": b"\x79",
}

#: A column the statistics have never seen: every figure is NULL.
UNANALYSED_ROW = {
    "OWNER": "AR_CRM",
    "TABLE_NAME": TABLE,
    "COLUMN_NAME": "FRESH",
    "DATA_TYPE": "NUMBER",
    "NUM_DISTINCT": None,
    "NUM_NULLS": None,
    "SAMPLE_SIZE": None,
    "LAST_ANALYZED": None,
    "HISTOGRAM": None,
    "LOW_VALUE": None,
    "HIGH_VALUE": None,
}

#: An analysed but empty table: the sample is zero, so the fraction is
#: undefined rather than zero.
EMPTY_ROW = {
    "OWNER": "AR_CRM",
    "TABLE_NAME": TABLE,
    "COLUMN_NAME": "NOTHING",
    "DATA_TYPE": "NUMBER",
    "NUM_DISTINCT": 0,
    "NUM_NULLS": 0,
    "SAMPLE_SIZE": 0,
    "LAST_ANALYZED": datetime(2026, 1, 2, 3, 4, 5),
    "HISTOGRAM": "NONE",
    "LOW_VALUE": None,
    "HIGH_VALUE": None,
}


class TestProbes:
    """What the dialect promises, without a server to ask."""

    def test_the_profile_family_is_declared(self):
        dialect = OracleDialect(version=(23, 0, 0))
        assert dialect.supports_column_data_profile() is True
        assert dialect.supports_column_value_sample() is True

    def test_join_coverage_stays_unclaimed(self):
        """The statement is not written, and it is a double scan.

        Answering ``True`` would promise a formatter that does not exist; the
        roundtrip walk rejects exactly that shape, so the probe is ``False``
        until the statement is written.
        """
        assert OracleDialect(version=(23, 0, 0)).supports_join_coverage() is False


class TestParserOffline:
    """The conversions the dictionary cannot do for itself."""

    def test_the_fraction_is_nulls_over_the_sample(self):
        (profile,) = _introspector().parse_column_data_profiles(
            [ANALYSED_ROW], "AR_CRM", TABLE
        )
        assert isinstance(profile, ColumnDataProfile)
        assert profile.null_fraction == pytest.approx(0.6)
        assert profile.distinct_count == 2
        assert profile.row_count == 1000
        assert profile.sample_size == 1000
        assert profile.histogram == "FREQUENCY"
        assert profile.data_type == "VARCHAR2"

    def test_an_analysed_column_carries_its_vintage(self):
        (profile,) = _introspector().parse_column_data_profiles(
            [ANALYSED_ROW], "AR_CRM", TABLE
        )
        assert profile.source == "estimate"
        assert profile.collected_at == "2026-01-02 03:04:05"

    def test_the_raw_extremes_are_not_projected_into_min_and_max(self):
        """LOW_VALUE/HIGH_VALUE are RAW; a decode needs PL/SQL, so None.

        The row carries both, so this is "cannot read", not "was not there".
        """
        (profile,) = _introspector().parse_column_data_profiles(
            [ANALYSED_ROW], "AR_CRM", TABLE
        )
        assert profile.min_value is None
        assert profile.max_value is None

    def test_a_column_without_statistics_reports_none_not_zero(self):
        (profile,) = _introspector().parse_column_data_profiles(
            [UNANALYSED_ROW], "AR_CRM", TABLE
        )
        assert profile.source == "unknown"
        assert profile.collected_at is None
        assert profile.null_fraction is None
        assert profile.distinct_count is None
        assert profile.row_count is None
        assert profile.sample_size is None

    def test_a_zero_sample_leaves_the_fraction_undefined(self):
        (profile,) = _introspector().parse_column_data_profiles(
            [EMPTY_ROW], "AR_CRM", TABLE
        )
        assert profile.null_fraction is None
        assert profile.row_count == 0

    def test_the_listing_pairs_each_value_with_its_count(self):
        found = _introspector().parse_column_value_samples(
            [{"VALUE": "a", "COUNT": 3}, {"VALUE": None, "COUNT": 7}]
        )
        assert [item.value for item in found] == ["a", None]
        assert [item.count for item in found] == [3, 7]
        # No share is computed: the denominator is the caller's to supply.
        assert all(item.frequency is None for item in found)
        assert isinstance(found[0], ColumnValueFrequency)


# --------------------------------------------------------------------- #
# Live half
# --------------------------------------------------------------------- #


def _ddl():
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    return ExecutionOptions(stmt_type=StatementType.DDL)


def _select():
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    return ExecutionOptions(stmt_type=StatementType.SELECT)


def _drop(backend, table):
    """Drop a table if it exists, without caring whether it did."""
    backend.execute(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE'; "
        "EXCEPTION WHEN OTHERS THEN NULL; END;",
        options=_ddl(),
    )


def _rows():
    rows = []
    for i in range(1, ROW_COUNT + 1):
        if i % 5 == 0:
            note = "x"
        elif i % 5 == 1:
            note = "y"
        else:
            note = None
        rows.append((i, CODES[i % 3], i, note))
    return rows


def _gather(backend, table):
    """Gather statistics, or skip when the connected user may not.

    ``estimate_percent`` is left at its default (``AUTO_SAMPLE_SIZE``): naming
    a percentage would pin how much of the table Oracle reads, and that
    parameter is deprecated on recent releases. The consequence is that
    ``SAMPLE_SIZE`` may be below the table's row count, which is why the
    assertions that touch it are one-sided.
    """
    try:
        backend.execute(
            f"BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '{table}'); END;",
            options=_ddl(),
        )
    except DatabaseError as exc:  # pragma: no cover - privilege dependent
        pytest.skip(f"connected user may not gather statistics: {exc}")


def _setup(backend):
    _drop(backend, TABLE)
    backend.execute(
        f"CREATE TABLE {TABLE} ("
        "id NUMBER PRIMARY KEY, code VARCHAR2(10), amount NUMBER, "
        "note VARCHAR2(10))",
        options=_ddl(),
    )
    # ``execute_many`` does not auto-commit, and the statistics gathering that
    # follows is a separate call that must see the rows. An explicit
    # transaction makes the visibility a decision rather than a side effect of
    # whichever connection the pool handed out.
    with backend.transaction():
        backend.execute_many(
            f"INSERT INTO {TABLE} (id, code, amount, note) VALUES (?, ?, ?, ?)",
            _rows(),
        )
    _gather(backend, TABLE)


def _teardown(backend):
    _drop(backend, TABLE)


def _dictionary_stats(backend, table):
    """The statistics columns themselves, for a cross-check.

    Read through the backend rather than the introspector so the comparison
    is between the profile and the dictionary, not between the profile and
    itself.
    """
    result = backend.execute(
        "SELECT column_name, num_distinct, num_nulls, sample_size, histogram, "
        "low_value, high_value FROM user_tab_columns WHERE table_name = :1",
        (table,),
        options=_select(),
    )
    return {row["column_name"]: row for row in result.data}


class TestColumnDataProfile:
    """What the dictionary says the rows hold."""

    def test_profile_matches_a_hand_count(self, oracle_backend_single):
        backend = oracle_backend_single
        _setup(backend)
        try:
            profiles = {
                profile.column_name: profile
                for profile in backend.introspector.list_column_data_profiles(
                    TABLE
                )
            }
            assert set(profiles) == {"ID", "CODE", "AMOUNT", "NOTE"}

            assert profiles["ID"].null_fraction == pytest.approx(0.0)
            # Unique, so the estimate must land on (or very near) 1000. It is
            # one-sided because AUTO_SAMPLE_SIZE may read only part of the
            # table, in which case Oracle extrapolates rather than counts.
            assert 900 <= profiles["ID"].distinct_count <= 1000

            # Three values, none null: sampling cannot miss them at this size,
            # so this count is exact on any server.
            assert profiles["CODE"].null_fraction == pytest.approx(0.0)
            assert profiles["CODE"].distinct_count == 3

            assert profiles["AMOUNT"].null_fraction == pytest.approx(0.0)
            assert 900 <= profiles["AMOUNT"].distinct_count <= 1000

            # 600 of 1000 rows are NULL. A profile that clamped the figure, or
            # returned the count instead of the share, would fail here. The
            # tolerance is loose on purpose: under AUTO_SAMPLE_SIZE the share
            # is estimated from however many rows the optimizer chose to read,
            # and even a small sample lands near the true 0.6.
            assert profiles["NOTE"].null_fraction == pytest.approx(0.6, abs=0.05)
            assert profiles["NOTE"].distinct_count == 2
        finally:
            _teardown(backend)

    def test_the_fraction_is_the_dictionarys_own_arithmetic(
        self, oracle_backend_single
    ):
        """``null_fraction`` is exactly ``NUM_NULLS / SAMPLE_SIZE``.

        The point is the pairing: whatever Oracle sampled, the two figures the
        profile reports are the two the dictionary holds, and the share is
        their ratio rather than an independently measured number.
        """
        backend = oracle_backend_single
        _setup(backend)
        try:
            profiles = {
                profile.column_name: profile
                for profile in backend.introspector.list_column_data_profiles(
                    TABLE
                )
            }
            for name, row in _dictionary_stats(backend, TABLE).items():
                profile = profiles[name]
                sample_size = int(row["sample_size"])
                assert profile.sample_size == sample_size
                assert profile.row_count == sample_size
                assert 0 < sample_size <= ROW_COUNT
                assert profile.distinct_count == int(row["num_distinct"])
                # The dictionary's own ratio. It is compared as a float, with
                # a tolerance of one row: a sampled NUM_NULLS is scaled to the
                # table size and may carry a fraction, while the profile
                # stores a count.
                assert profile.null_fraction == pytest.approx(
                    float(row["num_nulls"]) / sample_size, abs=1 / sample_size
                )
        finally:
            _teardown(backend)

    def test_every_profile_carries_its_vintage(self, oracle_backend_single):
        """An estimate must be able to say when it was collected."""
        backend = oracle_backend_single
        _setup(backend)
        try:
            profiles = backend.introspector.list_column_data_profiles(TABLE)
            assert profiles
            for profile in profiles:
                assert profile.source == "estimate"
                assert profile.collected_at is not None
        finally:
            _teardown(backend)

    def test_extremes_stay_none_although_the_raw_is_there(
        self, oracle_backend_single
    ):
        """min/max are None because the encoding needs PL/SQL, not because
        the statistics are missing.

        ``LOW_VALUE``/``HIGH_VALUE`` are RAW; decoding one needs
        ``DBMS_STATS.CONVERT_RAW_VALUE``, whose result type is the column's
        own. Asserting both halves together is what keeps a future shortcut
        honest: the slot is empty while the fact behind it is present.
        """
        backend = oracle_backend_single
        _setup(backend)
        try:
            profiles = backend.introspector.list_column_data_profiles(TABLE)
            assert profiles
            for profile in profiles:
                assert profile.min_value is None
                assert profile.max_value is None

            dictionary = _dictionary_stats(backend, TABLE)
            assert all(row["low_value"] is not None for row in dictionary.values())
            assert all(row["high_value"] is not None for row in dictionary.values())
        finally:
            _teardown(backend)

    def test_a_column_without_statistics_answers_none_not_zero(
        self, oracle_backend_single
    ):
        """No statistics is "not measured", never a zero-filled profile.

        The columns are still listed -- they exist -- so the answer is one
        profile per column with every figure ``None``, which is a different
        answer from PostgreSQL's empty list and deliberately so.
        """
        backend = oracle_backend_single
        _drop(backend, UNANALYSED_TABLE)
        backend.execute(
            f"CREATE TABLE {UNANALYSED_TABLE} (id NUMBER, note VARCHAR2(10))",
            options=_ddl(),
        )
        try:
            try:
                # A table may have been analysed by the automatic statistics
                # task; deleting is what makes "never measured" certain rather
                # than probable.
                backend.execute(
                    f"BEGIN DBMS_STATS.DELETE_TABLE_STATS"
                    f"(USER, '{UNANALYSED_TABLE}'); END;",
                    options=_ddl(),
                )
            except DatabaseError as exc:  # pragma: no cover
                pytest.skip(f"connected user may not delete statistics: {exc}")

            profiles = backend.introspector.list_column_data_profiles(
                UNANALYSED_TABLE
            )
            assert {profile.column_name for profile in profiles} == {"ID", "NOTE"}
            for profile in profiles:
                assert profile.source == "unknown"
                assert profile.collected_at is None
                assert profile.null_fraction is None
                assert profile.distinct_count is None
                assert profile.row_count is None
                assert profile.sample_size is None
                assert profile.min_value is None
                assert profile.max_value is None
        finally:
            _drop(backend, UNANALYSED_TABLE)


class TestColumnValueSample:
    """The grouped listing, which is a scan rather than a catalogue read."""

    def test_listing_is_ordered_by_frequency(self, oracle_backend_single):
        """b occurs 334 times, a and c 333 each; ties break by value."""
        backend = oracle_backend_single
        _setup(backend)
        try:
            samples = backend.introspector.sample_column_values(
                TABLE, "code", limit=10
            )
            assert [item.value for item in samples] == ["b", "a", "c"]
            assert [item.count for item in samples] == [334, 333, 333]
        finally:
            _teardown(backend)

    def test_limit_bounds_the_listing(self, oracle_backend_single):
        backend = oracle_backend_single
        _setup(backend)
        try:
            assert (
                len(
                    backend.introspector.sample_column_values(
                        TABLE, "code", limit=2
                    )
                )
                == 2
            )
        finally:
            _teardown(backend)

    def test_null_is_a_value_of_its_own(self, oracle_backend_single):
        """A ``GROUP BY`` groups the nulls, so NULL is the commonest value.

        Worth pinning because the catalogue-based implementation of this
        family never reports one: PostgreSQL keeps NULLs in ``null_frac``
        instead. Same question, two shapes -- which is why the caller reads
        ``null_fraction`` rather than looking for a NULL entry.
        """
        backend = oracle_backend_single
        _setup(backend)
        try:
            samples = backend.introspector.sample_column_values(
                TABLE, "note", limit=10
            )
            assert samples[0].value is None
            assert samples[0].count == 600
            assert {
                (item.value, item.count) for item in samples if item.value
            } == {("x", 200), ("y", 200)}
        finally:
            _teardown(backend)
