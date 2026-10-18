# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_typed_values.py
"""Oracle's spellings for the typed value literals.

Core's value nodes render through ``inline_params`` -- a class-level
strategy, never a per-instance flag -- so the DDL-position assertions use a
test-local DDL-position class per kind, the same lift core itself performs
when it renders a ``DEFAULT`` / ``CHECK`` tree (``Literal`` -> ``DdlLiteral``).
The data-position assertions run the node classes as they ship, which bind.

Every literal form asserted here was measured, not transcribed. Measured on
the live server behind scenario ``oracle_23c`` (127.0.0.1:11523, which
answers ``v$version`` with "Oracle AI Database 26ai Free Release
23.26.1.0.0"; session ``NLS_DATE_FORMAT=DD-MON-RR``,
``NLS_TIMESTAMP_FORMAT=DD-MON-RR HH.MI.SSXFF AM``). The 18c (11521) and 21c
(11522) scenarios refuse connections at the time of writing, so the
pre-23ai spellings are pinned by the version gates the dialect already
carries rather than by a live answer:

* ``RAW(4) DEFAULT HEXTORAW('0102')`` fills the column with ``b'\\x01\\x02'``
  on insert. ``HEXTORAW('')`` is accepted but reads back ``None`` -- Oracle
  cannot hold a zero-length RAW, it becomes NULL.
* ``VARCHAR2(36) DEFAULT '<canonical uuid>'`` reads the hyphenated text
  back verbatim -- no UUID type, so the bare quoted text is the spelling.
* ``INTERVAL '1 02:03:04.123456' DAY(1) TO SECOND(6)`` round-trips a
  ``timedelta`` exactly; the literal's default leading precision is two
  digits, so three-digit days need ``DAY(3)`` (``ORA-01873`` otherwise), and
  the sign is the literal's leading character:
  ``'-0 00:00:01.000000' DAY(1) TO SECOND(6)`` reads back as exactly
  ``timedelta(seconds=-1)``.
* ``DATE DEFAULT '2026-10-18'`` is refused by the measured session
  (NLS-dependent); ``TO_DATE('2026-10-18','YYYY-MM-DD')`` reads back
  2026-10-18 00:00.
* ``TIMESTAMP DEFAULT TO_TIMESTAMP('2026-10-18 12:34:56.123456',
  'YYYY-MM-DD HH24:MI:SS.FF6')`` reads back exact;
  ``TO_TIMESTAMP_TZ('2026-10-18 12:34:56 +08:00', 'YYYY-MM-DD HH24:MI:SS
  TZH:TZM')`` carries the offset.
* ``TO_DATE('14:30:00','HH24:MI:SS')`` is accepted but invents a date the
  caller never supplied -- the read-back is the first day of the current
  month (2026-10-01, measured on 2026-10-18), which is why the DDL position
  for a time of day is refused.
* ``NUMBER(1) DEFAULT TRUE`` reads back ``1`` and ``DEFAULT FALSE`` reads
  back ``0`` on 23.26.1; a ``BOOLEAN DEFAULT TRUE`` column round-trips a
  real boolean. Below 23ai the words are not SQL, so the DDL position
  refuses through the ``supports_boolean_type`` gate.

The live-server class at the bottom re-measures the answers end to end
through the first scenario that answers, raw SQL over oracledb, because the
literal forms are this dialect's own product and the expression layer has
no DDL-strategy value class to build a ``DEFAULT`` tree from yet (core's own
lift, ``DdlLiteral``, covers ``Literal`` only).
"""

import datetime
import decimal
import pathlib
import uuid

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.values import (
    BinaryValue,
    BooleanValue,
    DateValue,
    DecimalValue,
    FloatValue,
    IntegerValue,
    IntervalValue,
    NullValue,
    StringValue,
    TimeValue,
    TimestampValue,
    UUIDValue,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


@pytest.fixture
def pre23ai_dialect():
    return OracleDialect(version=(19, 0, 0))


# -- the DDL-position lift ------------------------------------------------
#
# One class per kind, each declaring the strategy its position requires,
# exactly as core's own DdlLiteral does for Literal. The subclass adds no
# field and no kind: it only owns the position, which is the whole of the
# class-level inline strategy.

class DdlBinaryValue(BinaryValue):
    inline_params = True


class DdlUUIDValue(UUIDValue):
    inline_params = True


class DdlIntervalValue(IntervalValue):
    inline_params = True


class DdlNullValue(NullValue):
    inline_params = True


class DdlBooleanValue(BooleanValue):
    inline_params = True


class DdlIntegerValue(IntegerValue):
    inline_params = True


class DdlFloatValue(FloatValue):
    inline_params = True


class DdlDecimalValue(DecimalValue):
    inline_params = True


class DdlStringValue(StringValue):
    inline_params = True


class DdlDateValue(DateValue):
    inline_params = True


class DdlTimeValue(TimeValue):
    inline_params = True


class DdlTimestampValue(TimestampValue):
    inline_params = True


class TestBinaryValue:
    """``HEXTORAW('0102')`` -- RAW has no literal form of its own."""

    def test_data_position_binds_the_bytes(self, dialect):
        sql, params = BinaryValue(dialect, b"\x01\x02").to_sql()
        assert sql == "?"
        assert params == (b"\x01\x02",)

    def test_ddl_position_spells_hex(self, dialect):
        sql, params = DdlBinaryValue(dialect, b"\x01\x02\xff").to_sql()
        assert sql == "HEXTORAW('0102FF')"
        assert params == ()

    def test_the_digits_are_uppercase(self, dialect):
        sql, _ = DdlBinaryValue(dialect, b"\xab\xcd\xef").to_sql()
        assert sql == "HEXTORAW('ABCDEF')"

    def test_an_empty_value_is_the_empty_literal(self, dialect):
        sql, _ = DdlBinaryValue(dialect, b"").to_sql()
        assert sql == "HEXTORAW('')"

    def test_bytearray_is_normalised_before_rendering(self, dialect):
        sql, _ = DdlBinaryValue(dialect, bytearray(b"\x01")).to_sql()
        assert sql == "HEXTORAW('01')"


class TestUUIDValue:
    """The bare quoted canonical text -- all Oracle offers is a text home."""

    CANONICAL = "c73d9175-0a1d-48c6-8d30-df165461328b"

    def test_data_position_binds_the_uuid(self, dialect):
        value = uuid.UUID(self.CANONICAL)
        sql, params = UUIDValue(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_ddl_position_is_the_quoted_canonical_text(self, dialect):
        sql, params = DdlUUIDValue(dialect, uuid.UUID(self.CANONICAL)).to_sql()
        assert sql == f"'{self.CANONICAL}'"
        assert params == ()


class TestIntervalValue:
    """``INTERVAL '<d> HH:MM:SS.ffffff' DAY(p) TO SECOND(6)``."""

    def test_data_position_binds_the_timedelta(self, dialect):
        span = datetime.timedelta(days=2, hours=3)
        sql, params = IntervalValue(dialect, span).to_sql()
        assert sql == "?"
        assert params == (span,)

    def test_ddl_position_spells_the_uniform_form(self, dialect):
        span = datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5)
        sql, params = DdlIntervalValue(dialect, span).to_sql()
        assert sql == "INTERVAL '1 02:03:04.000005' DAY(1) TO SECOND(6)"
        assert params == ()

    def test_a_whole_second_span_still_carries_the_fraction(self, dialect):
        sql, _ = DdlIntervalValue(dialect, datetime.timedelta(days=1, seconds=7384)).to_sql()
        assert sql == "INTERVAL '1 02:03:04.000000' DAY(1) TO SECOND(6)"

    def test_zero_is_day_precision_one(self, dialect):
        sql, _ = DdlIntervalValue(dialect, datetime.timedelta(0)).to_sql()
        assert sql == "INTERVAL '0 00:00:00.000000' DAY(1) TO SECOND(6)"

    def test_three_digit_days_size_the_leading_precision(self, dialect):
        # The literal's default leading precision is two digits; three-digit
        # days without DAY(3) are ORA-01873 (measured), so the precision is
        # sized to the day count.
        span = datetime.timedelta(days=150, seconds=7384, microseconds=123456)
        sql, _ = DdlIntervalValue(dialect, span).to_sql()
        assert sql == "INTERVAL '150 02:03:04.123456' DAY(3) TO SECOND(6)"

    def test_the_sign_leads_and_applies_to_the_whole_value(self, dialect):
        # timedelta(seconds=-1) is days=-1, seconds=86399 after
        # normalisation; spelling those components with a leading minus
        # would mean -(1 day 23:59:59), a different value. The rendering
        # spells the absolute span and lets the literal's sign carry it.
        sql, _ = DdlIntervalValue(dialect, datetime.timedelta(seconds=-1)).to_sql()
        assert sql == "INTERVAL '-0 00:00:01.000000' DAY(1) TO SECOND(6)"

    def test_a_negative_multi_day_span_spells_its_absolute_days(self, dialect):
        span = -datetime.timedelta(days=150, microseconds=500000)
        sql, _ = DdlIntervalValue(dialect, span).to_sql()
        assert sql == "INTERVAL '-150 00:00:00.500000' DAY(3) TO SECOND(6)"


class TestBooleanValue:
    """``TRUE`` / ``FALSE`` from 23ai; refused before it."""

    def test_data_position_binds_the_bool(self, dialect):
        sql, params = BooleanValue(dialect, True).to_sql()
        assert sql == "?"
        assert params == (True,)

    def test_ddl_position_spells_the_keywords_on_23ai(self, dialect):
        assert DdlBooleanValue(dialect, True).to_sql() == ("TRUE", ())
        assert DdlBooleanValue(dialect, False).to_sql() == ("FALSE", ())

    def test_ddl_position_refuses_below_23ai(self, pre23ai_dialect):
        with pytest.raises(UnsupportedFeatureError, match="TRUE literal"):
            DdlBooleanValue(pre23ai_dialect, True).to_sql()
        with pytest.raises(UnsupportedFeatureError, match="FALSE literal"):
            DdlBooleanValue(pre23ai_dialect, False).to_sql()


class TestDateValue:
    """``TO_DATE('2026-10-18','YYYY-MM-DD')`` -- the quoted form is NLS-dependent."""

    def test_data_position_binds_the_date(self, dialect):
        value = datetime.date(2026, 10, 18)
        sql, params = DateValue(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_ddl_position_spells_the_parse_call(self, dialect):
        sql, params = DdlDateValue(dialect, datetime.date(2026, 10, 18)).to_sql()
        assert sql == "TO_DATE('2026-10-18', 'YYYY-MM-DD')"
        assert params == ()


class TestTimeValue:
    """Refused in a DDL position: Oracle has no time-of-day literal."""

    def test_data_position_binds_the_time(self, dialect):
        value = datetime.time(14, 30)
        sql, params = TimeValue(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_ddl_position_refuses_rather_than_inventing_a_date(self, dialect):
        with pytest.raises(UnsupportedFeatureError, match="time-of-day literal"):
            DdlTimeValue(dialect, datetime.time(14, 30)).to_sql()


class TestTimestampValue:
    """``TO_TIMESTAMP`` / ``TO_TIMESTAMP_TZ`` with explicit masks."""

    def test_data_position_binds_the_datetime(self, dialect):
        value = datetime.datetime(2026, 10, 18, 12, 34, 56)
        sql, params = TimestampValue(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_ddl_position_spells_the_parse_call(self, dialect):
        sql, params = DdlTimestampValue(
            dialect, datetime.datetime(2026, 10, 18, 12, 34, 56)
        ).to_sql()
        assert sql == "TO_TIMESTAMP('2026-10-18 12:34:56', 'YYYY-MM-DD HH24:MI:SS')"
        assert params == ()

    def test_microseconds_add_the_ff6_mask(self, dialect):
        sql, _ = DdlTimestampValue(
            dialect, datetime.datetime(2026, 10, 18, 12, 34, 56, 123456)
        ).to_sql()
        assert sql == (
            "TO_TIMESTAMP('2026-10-18 12:34:56.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        )

    def test_an_aware_value_spells_the_offset_zone(self, dialect):
        value = datetime.datetime(
            2026, 10, 18, 12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=8))
        )
        sql, _ = DdlTimestampValue(dialect, value).to_sql()
        assert sql == (
            "TO_TIMESTAMP_TZ('2026-10-18 12:34:56 +08:00', "
            "'YYYY-MM-DD HH24:MI:SS TZH:TZM')"
        )

    def test_a_second_granular_offset_is_refused(self, dialect):
        class PlusOneSecond(datetime.tzinfo):
            # TZH:TZM spells whole minutes; an offset carrying seconds
            # cannot be written, so the rendering refuses rather than
            # truncating it.
            def utcoffset(self, dt):
                return datetime.timedelta(hours=8, seconds=1)

            def tzname(self, dt):
                return "+08:00:01"

            def dst(self, dt):
                return None

        value = datetime.datetime(2026, 10, 18, 12, 34, 56, tzinfo=PlusOneSecond())
        with pytest.raises(ValueError, match="minute-granular"):
            DdlTimestampValue(dialect, value).to_sql()


class TestTierOneAgreesWithTheBase:
    """The audit: nothing overridden for the kinds Oracle spells like core.

    Each assertion is the base's spelling reached through this dialect's
    ``format_literal``, pinned here so a base change that breaks Oracle's
    grammar is caught by this file rather than in the matrix, where the row
    would only say "renders".
    """

    def test_null_binds_then_renders_as_the_keyword(self, dialect):
        assert NullValue(dialect).to_sql() == ("?", (None,))
        assert DdlNullValue(dialect).to_sql() == ("NULL", ())

    def test_integers_render_as_numeric_text(self, dialect):
        assert DdlIntegerValue(dialect, 18).to_sql() == ("18", ())
        assert DdlIntegerValue(dialect, -7).to_sql() == ("-7", ())

    def test_floats_render_as_their_repr(self, dialect):
        assert DdlFloatValue(dialect, 0.5).to_sql() == ("0.5", ())
        assert DdlFloatValue(dialect, 15e-03).to_sql() == ("0.015", ())

    def test_decimals_render_exact(self, dialect):
        assert DdlDecimalValue(dialect, decimal.Decimal("12.34")).to_sql() == ("12.34", ())

    def test_strings_double_the_quotes(self, dialect):
        sql, _ = DdlStringValue(dialect, "today's").to_sql()
        assert sql == "'today''s'"

    def test_strings_keep_a_single_backslash(self, dialect):
        # Oracle text literals have no backslash escapes, so the base's
        # quote-doubling is the whole escaping and 'a\b' stays as written.
        sql, _ = DdlStringValue(dialect, "a\\b").to_sql()
        assert sql == "'a\\b'"


class TestMROOrder:
    """The composed order is what resolves the overrides."""

    def test_the_local_mixin_wins_over_the_base(self, dialect):
        for name in (
            "format_binary_value",
            "format_uuid_value",
            "format_interval_value",
            "format_boolean_value",
            "format_date_value",
            "format_time_value",
            "format_timestamp_value",
        ):
            method = getattr(dialect, name)
            assert method.__func__.__qualname__.startswith("OracleTypedValueMixin"), name

    def test_the_base_answers_the_kinds_the_local_mixin_leaves_alone(self, dialect):
        for name in (
            "format_null_value",
            "format_integer_value",
            "format_float_value",
            "format_decimal_value",
            "format_string_value",
        ):
            method = getattr(dialect, name)
            assert method.__func__.__qualname__.startswith("TypedValueMixin"), name


class TestLiveServerReadsTheLiteralsBack:
    """The measured answers re-verified end to end against the running server.

    Raw SQL over oracledb against the first scenario that answers (skip when
    none does), matching the repo's existing live-server precedent. What is
    verified is what the mixin claims: the server accepts the spelling and
    hands the same value back.
    """

    @pytest.fixture
    def live(self):
        import oracledb
        import yaml

        cfg = pathlib.Path(__file__).resolve()
        scenarios = {}
        for parent in cfg.parents:
            candidate = parent / "config" / "oracle_scenarios.yaml"
            if candidate.exists():
                scenarios = yaml.safe_load(
                    candidate.read_text()).get("scenarios", {})
                break
        conn = None
        for _name, s in scenarios.items():
            try:
                conn = oracledb.connect(
                    user=s["username"], password=s["password"],
                    dsn=f"{s['host']}:{s['port']}/{s['service_name']}",
                )
                break
            except Exception:
                continue
        if conn is None:  # pragma: no cover
            pytest.skip("no reachable Oracle scenario")
        cur = conn.cursor()
        cur.execute(
            "SELECT banner_full FROM v$version WHERE banner_full LIKE 'Oracle%'"
        )
        banner = cur.fetchone()[0]
        version = tuple(int(part) for part in banner.split("Release", 1)[1].split(".")[:3])
        yield cur, version
        conn.close()

    def _drop(self, cur, conn, name):
        try:
            cur.execute(f"DROP TABLE {name}")
            conn.commit()
        except Exception:
            conn.rollback()

    def test_binary_default_roundtrips(self, live):
        cur, _version = live
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_bin")
        try:
            cur.execute(
                "CREATE TABLE ar_ora_tv_bin (id INT, v RAW(4) DEFAULT HEXTORAW('0102'))"
            )
            cur.execute("INSERT INTO ar_ora_tv_bin (id) VALUES (1)")
            cur.execute("SELECT v FROM ar_ora_tv_bin WHERE id = 1")
            assert cur.fetchone()[0] == b"\x01\x02"
        finally:
            self._drop(cur, conn, "ar_ora_tv_bin")

    def test_uuid_default_roundtrips(self, live):
        canonical = "3f2a8c1e-9b4d-4c6a-a1b2-c3d4e5f60718"
        cur, _version = live
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_uuid")
        try:
            cur.execute(
                f"CREATE TABLE ar_ora_tv_uuid "
                f"(id INT, v VARCHAR2(36) DEFAULT '{canonical}')"
            )
            cur.execute("INSERT INTO ar_ora_tv_uuid (id) VALUES (1)")
            cur.execute("SELECT v FROM ar_ora_tv_uuid WHERE id = 1")
            assert cur.fetchone()[0].lower() == canonical
        finally:
            self._drop(cur, conn, "ar_ora_tv_uuid")

    def test_interval_default_roundtrips_positive_and_negative(self, live):
        cur, _version = live
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_iv")
        try:
            cur.execute(
                "CREATE TABLE ar_ora_tv_iv (id INT, "
                "p INTERVAL DAY(9) TO SECOND(9) "
                "DEFAULT INTERVAL '1 02:03:04.123456' DAY(1) TO SECOND(6), "
                "n INTERVAL DAY(9) TO SECOND(9) "
                "DEFAULT INTERVAL '-0 00:00:01.000000' DAY(1) TO SECOND(6))"
            )
            cur.execute("INSERT INTO ar_ora_tv_iv (id) VALUES (1)")
            cur.execute("SELECT p, n FROM ar_ora_tv_iv WHERE id = 1")
            p, n = cur.fetchone()
            assert p == datetime.timedelta(
                days=1, hours=2, minutes=3, seconds=4, microseconds=123456
            )
            assert n == datetime.timedelta(seconds=-1)
        finally:
            self._drop(cur, conn, "ar_ora_tv_iv")

    def test_date_default_roundtrips(self, live):
        cur, _version = live
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_date")
        try:
            cur.execute(
                "CREATE TABLE ar_ora_tv_date "
                "(id INT, d DATE DEFAULT TO_DATE('2026-10-18','YYYY-MM-DD'))"
            )
            cur.execute("INSERT INTO ar_ora_tv_date (id) VALUES (1)")
            cur.execute("SELECT d FROM ar_ora_tv_date WHERE id = 1")
            assert cur.fetchone()[0].date() == datetime.date(2026, 10, 18)
        finally:
            self._drop(cur, conn, "ar_ora_tv_date")

    def test_timestamp_default_roundtrips(self, live):
        cur, _version = live
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_ts")
        try:
            cur.execute(
                "CREATE TABLE ar_ora_tv_ts (id INT, ts TIMESTAMP(6) "
                "DEFAULT TO_TIMESTAMP('2026-10-18 12:34:56.123456',"
                "'YYYY-MM-DD HH24:MI:SS.FF6'))"
            )
            cur.execute("INSERT INTO ar_ora_tv_ts (id) VALUES (1)")
            cur.execute("SELECT ts FROM ar_ora_tv_ts WHERE id = 1")
            assert cur.fetchone()[0] == datetime.datetime(
                2026, 10, 18, 12, 34, 56, 123456
            )
        finally:
            self._drop(cur, conn, "ar_ora_tv_ts")

    def test_boolean_default_roundtrips_on_23ai(self, live):
        cur, version = live
        if version < (23, 0, 0):
            pytest.skip(f"TRUE/FALSE literals need 23ai, server is {version}")
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_bool")
        try:
            cur.execute(
                "CREATE TABLE ar_ora_tv_bool (id INT, b NUMBER(1) DEFAULT TRUE)"
            )
            cur.execute("INSERT INTO ar_ora_tv_bool (id) VALUES (1)")
            cur.execute("SELECT b FROM ar_ora_tv_bool WHERE id = 1")
            assert cur.fetchone()[0] == 1
        finally:
            self._drop(cur, conn, "ar_ora_tv_bool")

    def test_the_quoted_iso_date_default_is_nls_dependent(self, live):
        # The reason format_date_value does not delegate to the base: under
        # the measured session format the quoted ISO form is refused, so a
        # DEFAULT written with it depends on the session and breaks. What
        # this asserts is the failure itself -- pinning that the dialect's
        # TO_DATE spelling is not a stylistic choice.
        import oracledb

        cur, _version = live
        conn = cur.connection
        self._drop(cur, conn, "ar_ora_tv_nls")
        try:
            with pytest.raises(oracledb.DatabaseError):
                cur.execute(
                    "CREATE TABLE ar_ora_tv_nls (id INT, d DATE DEFAULT '2026-10-18')"
                )
        finally:
            self._drop(cur, conn, "ar_ora_tv_nls")
