# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_literals.py
"""Oracle's spellings for inline value literals.

Core's single ``Literal`` node carries every value and answers one question
per position -- ``inline=True`` writes SQL text through the dialect's
``format_literal``; the default binds behind a placeholder. The twelve
per-kind classes and their ``format_<kind>_value`` entry points are gone
(core 239678e5), so what this file pins is what remains: the placeholder at
a data position, the exact text at an inline position, and the branches of
this dialect's ``format_literal`` override.

Every literal form asserted here was measured, not transcribed. Measured on
the live server behind scenario ``oracle_23c`` (127.0.0.1:11523, which
answers ``v$version`` with "Oracle AI Database 26ai Free Release
23.26.1.0.0"; session ``NLS_DATE_FORMAT=DD-MON-RR``,
``NLS_TIMESTAMP_FORMAT=DD-MON-RR HH.MI.SSXFF AM``, ``NLS_LANGUAGE=
AMERICAN``). The 18c (11521) and 21c (11522) scenarios refuse connections
at the time of writing, so the pre-23ai spellings are pinned by the version
gates the dialect already carries rather than by a live answer:

* ``RAW(4) DEFAULT HEXTORAW('0102')`` fills the column with ``b'\\x01\\x02'``
  on insert. ``HEXTORAW('')`` is accepted but reads back ``None`` -- Oracle
  cannot hold a zero-length RAW, it becomes NULL.
* ``VARCHAR2(36) DEFAULT '<canonical uuid>'`` reads the hyphenated text
  back verbatim -- no UUID type, so the bare quoted text is the spelling.
* ``INTERVAL '1 02:03:04.123456' DAY(1) TO SECOND(6)`` round-trips a
  ``timedelta`` exactly; three-digit days need ``DAY(3)`` (``ORA-01873``
  otherwise, the literal's default leading precision being two digits), and
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
  month (2026-10-01, measured on 2026-10-18), which is why a time of day is
  refused inline.
* ``NUMBER(1) DEFAULT TRUE`` reads back ``1`` and ``DEFAULT FALSE`` reads
  back ``0`` on 23.26.1; a ``BOOLEAN DEFAULT TRUE`` column round-trips a
  real boolean. Below 23ai the words are not SQL, so the inline form
  refuses through the ``supports_boolean_type`` gate.

The live-server class at the bottom re-measures the answers end to end
through the first scenario that answers, raw SQL over oracledb, because the
literal forms are this dialect's own product.
"""

import datetime
import decimal
import pathlib
import uuid

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


@pytest.fixture
def pre23ai_dialect():
    return OracleDialect(version=(19, 0, 0))


class TestBinary:
    """``HEXTORAW('0102')`` -- RAW has no literal form of its own."""

    def test_data_position_binds_the_bytes(self, dialect):
        sql, params = Literal(dialect, b"\x01\x02").to_sql()
        assert sql == "?"
        assert params == (b"\x01\x02",)

    def test_inline_position_spells_hex(self, dialect):
        sql, params = Literal(dialect, b"\x01\x02\xff", inline=True).to_sql()
        assert sql == "HEXTORAW('0102FF')"
        assert params == ()

    def test_the_digits_are_uppercase(self, dialect):
        assert dialect.format_literal(b"\xab\xcd\xef") == "HEXTORAW('ABCDEF')"

    def test_an_empty_value_is_the_empty_literal(self, dialect):
        # The server accepts HEXTORAW('') and reads it back as NULL:
        # Oracle cannot hold a zero-length RAW (measured, see the class
        # docstring of the mixin).
        assert dialect.format_literal(b"") == "HEXTORAW('')"

    def test_bytearray_is_normalised_before_rendering(self, dialect):
        assert dialect.format_literal(bytearray(b"\x01")) == "HEXTORAW('01')"


class TestUUID:
    """The bare quoted canonical text -- all Oracle offers is a text home."""

    CANONICAL = "c73d9175-0a1d-48c6-8d30-df165461328b"

    def test_data_position_binds_the_uuid(self, dialect):
        value = uuid.UUID(self.CANONICAL)
        sql, params = Literal(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_inline_position_is_the_quoted_canonical_text(self, dialect):
        sql, params = Literal(dialect, uuid.UUID(self.CANONICAL), inline=True).to_sql()
        assert sql == f"'{self.CANONICAL}'"
        assert params == ()


class TestInterval:
    """``INTERVAL '<d> HH:MM:SS.ffffff' DAY(p) TO SECOND(6)``."""

    def test_data_position_binds_the_timedelta(self, dialect):
        span = datetime.timedelta(days=2, hours=3)
        sql, params = Literal(dialect, span).to_sql()
        assert sql == "?"
        assert params == (span,)

    def test_inline_position_spells_the_uniform_form(self, dialect):
        span = datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5)
        sql, params = Literal(dialect, span, inline=True).to_sql()
        assert sql == "INTERVAL '1 02:03:04.000005' DAY(1) TO SECOND(6)"
        assert params == ()

    def test_a_whole_second_span_still_carries_the_fraction(self, dialect):
        span = datetime.timedelta(days=1, seconds=7384)
        assert dialect.format_literal(span) == "INTERVAL '1 02:03:04.000000' DAY(1) TO SECOND(6)"

    def test_zero_is_day_precision_one(self, dialect):
        assert dialect.format_literal(datetime.timedelta(0)) == (
            "INTERVAL '0 00:00:00.000000' DAY(1) TO SECOND(6)"
        )

    def test_three_digit_days_size_the_leading_precision(self, dialect):
        # The literal's default leading precision is two digits; three-digit
        # days without DAY(3) are ORA-01873 (measured), so the precision is
        # sized to the day count.
        span = datetime.timedelta(days=150, seconds=7384, microseconds=123456)
        assert dialect.format_literal(span) == (
            "INTERVAL '150 02:03:04.123456' DAY(3) TO SECOND(6)"
        )

    def test_the_sign_leads_and_applies_to_the_whole_value(self, dialect):
        # timedelta(seconds=-1) is days=-1, seconds=86399 after
        # normalisation; spelling those components with a leading minus
        # would mean -(1 day 23:59:59), a different value. The rendering
        # spells the absolute span and lets the literal's sign carry it.
        assert dialect.format_literal(datetime.timedelta(seconds=-1)) == (
            "INTERVAL '-0 00:00:01.000000' DAY(1) TO SECOND(6)"
        )

    def test_a_negative_multi_day_span_spells_its_absolute_days(self, dialect):
        span = -datetime.timedelta(days=150, microseconds=500000)
        assert dialect.format_literal(span) == (
            "INTERVAL '-150 00:00:00.500000' DAY(3) TO SECOND(6)"
        )


class TestBoolean:
    """``TRUE`` / ``FALSE`` from 23ai; refused before it."""

    def test_data_position_binds_the_bool(self, dialect):
        sql, params = Literal(dialect, True).to_sql()
        assert sql == "?"
        assert params == (True,)

    def test_inline_position_spells_the_keywords_on_23ai(self, dialect):
        assert dialect.format_literal(True) == "TRUE"
        assert dialect.format_literal(False) == "FALSE"

    def test_inline_position_refuses_below_23ai(self, pre23ai_dialect):
        with pytest.raises(UnsupportedFeatureError, match="TRUE literal"):
            pre23ai_dialect.format_literal(True)
        with pytest.raises(UnsupportedFeatureError, match="FALSE literal"):
            pre23ai_dialect.format_literal(False)


class TestDate:
    """``TO_DATE('2026-10-18','YYYY-MM-DD')`` -- the quoted form is NLS-dependent."""

    def test_data_position_binds_the_date(self, dialect):
        value = datetime.date(2026, 10, 18)
        sql, params = Literal(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_inline_position_spells_the_parse_call(self, dialect):
        sql, params = Literal(dialect, datetime.date(2026, 10, 18), inline=True).to_sql()
        assert sql == "TO_DATE('2026-10-18', 'YYYY-MM-DD')"
        assert params == ()


class TestTime:
    """Refused inline: Oracle has no time-of-day literal."""

    def test_data_position_binds_the_time(self, dialect):
        value = datetime.time(14, 30)
        sql, params = Literal(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_inline_position_refuses_rather_than_inventing_a_date(self, dialect):
        with pytest.raises(UnsupportedFeatureError, match="time-of-day literal"):
            Literal(dialect, datetime.time(14, 30), inline=True).to_sql()


class TestTimestamp:
    """``TO_TIMESTAMP`` / ``TO_TIMESTAMP_TZ`` with explicit masks."""

    def test_data_position_binds_the_datetime(self, dialect):
        value = datetime.datetime(2026, 10, 18, 12, 34, 56)
        sql, params = Literal(dialect, value).to_sql()
        assert sql == "?"
        assert params == (value,)

    def test_inline_position_spells_the_parse_call(self, dialect):
        value = datetime.datetime(2026, 10, 18, 12, 34, 56)
        sql, params = Literal(dialect, value, inline=True).to_sql()
        assert sql == "TO_TIMESTAMP('2026-10-18 12:34:56', 'YYYY-MM-DD HH24:MI:SS')"
        assert params == ()

    def test_microseconds_add_the_ff6_mask(self, dialect):
        value = datetime.datetime(2026, 10, 18, 12, 34, 56, 123456)
        assert dialect.format_literal(value) == (
            "TO_TIMESTAMP('2026-10-18 12:34:56.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        )

    def test_an_aware_value_spells_the_offset_zone(self, dialect):
        value = datetime.datetime(
            2026, 10, 18, 12, 34, 56,
            tzinfo=datetime.timezone(datetime.timedelta(hours=8)),
        )
        assert dialect.format_literal(value) == (
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
            dialect.format_literal(value)


class TestTierOneAgreesWithTheBase:
    """The audit: nothing overridden for the kinds Oracle spells like core.

    Each assertion is the base's spelling reached through this dialect's
    ``format_literal``, pinned here so a base change that breaks Oracle's
    grammar is caught by this file rather than in the matrix, where the row
    would only say "renders".
    """

    def test_null_binds_then_renders_as_the_keyword(self, dialect):
        assert Literal(dialect, None).to_sql() == ("?", (None,))
        assert dialect.format_literal(None) == "NULL"

    def test_integers_render_as_numeric_text(self, dialect):
        assert dialect.format_literal(18) == "18"
        assert dialect.format_literal(-7) == "-7"

    def test_floats_render_as_their_repr(self, dialect):
        assert dialect.format_literal(0.5) == "0.5"
        assert dialect.format_literal(15e-03) == "0.015"

    def test_decimals_render_exact(self, dialect):
        assert dialect.format_literal(decimal.Decimal("12.34")) == "12.34"

    def test_strings_double_the_quotes(self, dialect):
        assert dialect.format_literal("today's") == "'today''s'"

    def test_strings_keep_a_single_backslash(self, dialect):
        # Oracle text literals have no backslash escapes, so the base's
        # quote-doubling is the whole escaping and 'a\b' stays as written.
        assert dialect.format_literal("a\\b") == "'a\\b'"


class TestMROOrder:
    """The composed order is what resolves the override."""

    def test_the_literal_mixin_wins_over_the_base(self, dialect):
        # The mixin sits ahead of SQLDialectBase in the base list, because
        # it overrides format_literal and a mixin placed after the class it
        # overrides never wins (core 239678e5, sqlite's LiteralValueMixin
        # being the reference placement).
        assert dialect.format_literal.__func__.__qualname__.startswith(
            "OracleLiteralValueMixin"
        )


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
        # The reason format_literal does not keep the base's quoted form for
        # dates: under the measured session format the quoted ISO form is
        # refused, so a DEFAULT written with it depends on the session and
        # breaks. What this asserts is the failure itself -- pinning that
        # the dialect's TO_DATE spelling is not a stylistic choice.
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
