# tests/rhosocial/activerecord_oracle_test/feature/backend/expression/test_seconds_between.py
"""Oracle's spelling for signed, fractional seconds between two instants.

``seconds_between`` is a duration, so the cases that matter are the ones a
calendar-component sum gets wrong: the sign, the zero, the fraction, and the
crossing of a month boundary. The rendering tests pin the SQL text and the
bind order; the live tests execute the same expression against Oracle 26ai
Free and check the values it actually answers, including DATE columns, where
Oracle's ``-`` is a number of days rather than an interval.
"""

import datetime as dt
import os

import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    Literal,
    QueryStatement,
    TimestampColumn,
    seconds_between,
)
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

try:
    from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
except ImportError:  # pragma: no cover - the backend is the package under test
    OracleBackend = None


@pytest.fixture
def dialect():
    from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

    return OracleDialect()


def _diff_sql(end: str, start: str) -> str:
    """The subtraction Oracle is told to take, as SQL text."""
    return (
        f"(CAST({end} AS TIMESTAMP(9))"
        f" - CAST({start} AS TIMESTAMP(9)))"
    )


def _expected_sql(end: str, start: str) -> str:
    """The full rendering for one pair of operands."""
    difference = _diff_sql(end, start)
    return (
        f"(EXTRACT(DAY FROM {difference}) * 86400"
        f" + EXTRACT(HOUR FROM {difference}) * 3600"
        f" + EXTRACT(MINUTE FROM {difference}) * 60"
        f" + EXTRACT(SECOND FROM {difference}))"
    )


class TestSecondsBetweenRendering:
    """What the Oracle formatter writes, before any server sees it."""

    def test_renders_interval_components_in_seconds(self, dialect):
        started = TimestampColumn(dialect, "started_at", table="t")
        ended = TimestampColumn(dialect, "ended_at", table="t")

        sql, params = seconds_between(dialect, started, ended).to_sql()

        assert sql == _expected_sql('"T"."ENDED_AT"', '"T"."STARTED_AT"')
        assert params == ()

    def test_casts_to_timestamp_not_date(self, dialect):
        """A DATE cast would lose the fraction and change the interval."""
        started = TimestampColumn(dialect, "started_at", table="t")
        ended = TimestampColumn(dialect, "ended_at", table="t")

        sql, _ = seconds_between(dialect, started, ended).to_sql()

        assert "AS TIMESTAMP(9)" in sql
        assert "AS DATE" not in sql
        assert "EXTRACT(SECOND FROM" in sql
        assert "* 86400" in sql

    def test_alias_belongs_to_the_whole_sum(self, dialect):
        started = TimestampColumn(dialect, "started_at", table="t")
        ended = TimestampColumn(dialect, "ended_at", table="t")

        sql, _ = seconds_between(dialect, started, ended).as_("elapsed").to_sql()

        assert sql == (
            _expected_sql('"T"."ENDED_AT"', '"T"."STARTED_AT"')
            + ' AS "ELAPSED"'
        )

    def test_bind_parameters_follow_the_written_operands(self, dialect):
        """``end`` is written first in each extraction, so its bind comes first.

        The four component extractions each re-render the subtraction, so each
        operand contributes four binds and the order repeats with the SQL.
        """
        expr = seconds_between(
            dialect,
            Literal(dialect, "2026-01-01 00:00:00"),
            Literal(dialect, "2026-01-02 00:00:00"),
        )

        sql, params = expr.to_sql()

        assert sql == _expected_sql("?", "?")
        assert params == (
            "2026-01-02 00:00:00",
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
            "2026-01-01 00:00:00",
        )

    def test_column_method_matches_the_factory(self, dialect):
        started = TimestampColumn(dialect, "started_at", table="t")
        ended = TimestampColumn(dialect, "ended_at", table="t")

        method_sql, method_params = started.seconds_between(ended).to_sql()
        factory_sql, factory_params = seconds_between(
            dialect, started, ended
        ).to_sql()

        assert method_sql == factory_sql
        assert method_params == factory_params

    def test_factory_coerces_a_bare_name_to_a_column(self, dialect):
        sql, params = seconds_between(dialect, "started_at", "ended_at").to_sql()

        assert sql == _expected_sql('"ENDED_AT"', '"STARTED_AT"')
        assert params == ()


# ---------------------------------------------------------------------------
# Live execution
# ---------------------------------------------------------------------------

_HOST = os.environ.get("ORACLE_HOST", "localhost")
_PORT = int(os.environ.get("ORACLE_PORT", "11523"))
_USER = os.environ.get("ORACLE_USER", "testuser")
_PASSWORD = os.environ.get("ORACLE_PASSWORD", "Password1!")
_SERVICE = os.environ.get("ORACLE_SERVICE", "FREEPDB1")

_DROP_BLOCK = (
    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {name} CASCADE CONSTRAINTS'; "
    "EXCEPTION WHEN OTHERS THEN NULL; END;"
)


@pytest.fixture(scope="module")
def live_backend():
    """A connected Oracle backend, or a skip when no server is reachable."""
    if OracleBackend is None:  # pragma: no cover
        pytest.skip("Oracle backend is not importable")
    backend = OracleBackend(
        host=_HOST,
        port=_PORT,
        username=_USER,
        password=_PASSWORD,
        service_name=_SERVICE,
    )
    try:
        backend.connect()
        backend.introspect_and_adapt()
    except Exception as exc:  # pragma: no cover - environment dependent
        try:
            backend.disconnect()
        except Exception:
            pass
        pytest.skip(f"Oracle server {_HOST}:{_PORT}/{_SERVICE} unavailable: {exc}")
    try:
        yield backend
    finally:
        backend.disconnect()


def _drop(backend, name: str) -> None:
    backend.executescript(_DROP_BLOCK.format(name=name))


class TestSecondsBetweenExecution:
    """The values the Oracle spelling actually answers."""

    def test_bound_literals_are_signed_and_fractional(self, live_backend):
        """One round trip, one expression per case.

        Each expression binds its own operands, so this also exercises the
        repeated bind order the formatter writes: a wrong order would answer
        a different duration for every case below.
        """
        backend = live_backend
        dialect = backend.dialect
        cases = [
            ("zero", dt.datetime(2026, 6, 4, 10, 0, 0),
             dt.datetime(2026, 6, 4, 10, 0, 0)),
            ("hour", dt.datetime(2026, 6, 4, 10, 0, 0),
             dt.datetime(2026, 6, 4, 11, 0, 0)),
            ("negative", dt.datetime(2026, 6, 4, 11, 0, 0),
             dt.datetime(2026, 6, 4, 10, 0, 0)),
            ("cross_month", dt.datetime(2026, 1, 31, 23, 59, 59),
             dt.datetime(2026, 2, 1, 0, 0, 0)),
            ("positive_fraction", dt.datetime(2026, 1, 1, 0, 0, 0),
             dt.datetime(2026, 1, 3, 3, 4, 5, 250000)),
            ("negative_fraction", dt.datetime(2026, 1, 3, 3, 4, 5, 250000),
             dt.datetime(2026, 1, 1, 0, 0, 0)),
            ("subsecond_negative", dt.datetime(2026, 6, 4, 10, 0, 0, 250000),
             dt.datetime(2026, 6, 4, 10, 0, 0)),
        ]
        select = [
            seconds_between(
                dialect, Literal(dialect, start), Literal(dialect, end)
            ).as_(name)
            for name, start, end in cases
        ]
        sql, params = QueryStatement(dialect, select=select).to_sql()
        result = backend.execute(
            sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL)
        )
        row = result.data[0]

        assert row["zero"] == pytest.approx(0.0, abs=1e-6)
        assert row["hour"] == pytest.approx(3600.0, abs=1e-6)
        assert row["negative"] == pytest.approx(-3600.0, abs=1e-6)
        assert row["cross_month"] == pytest.approx(1.0, abs=1e-6)
        assert row["positive_fraction"] == pytest.approx(183845.25, abs=1e-6)
        assert row["negative_fraction"] == pytest.approx(-183845.25, abs=1e-6)
        assert row["subsecond_negative"] == pytest.approx(-0.25, abs=1e-6)

    def test_timestamp_columns_in_a_row(self, live_backend):
        backend = live_backend
        table = "SECONDS_BETWEEN_TS"

        _drop(backend, table)
        backend.executescript(
            f"""
            CREATE TABLE {table} (
                id NUMBER(4),
                started_at TIMESTAMP(6),
                ended_at TIMESTAMP(6)
            )
            """
        )
        rows = [
            (1, dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 3, 3, 4, 5, 250000)),
            (2, dt.datetime(2026, 1, 3, 3, 4, 5, 250000), dt.datetime(2026, 1, 1)),
            (3, dt.datetime(2026, 1, 1, 0, 0, 0, 500000), dt.datetime(2026, 1, 1, 0, 0, 0, 250000)),
            (4, dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 1)),
        ]
        try:
            for row in rows:
                backend.execute(
                    f"INSERT INTO {table} VALUES (?, ?, ?)", row
                )
            dialect = backend.dialect
            expr = seconds_between(
                dialect,
                TimestampColumn(dialect, "started_at"),
                TimestampColumn(dialect, "ended_at"),
            ).as_("seconds")
            sql, params = QueryStatement(
                dialect,
                select=[expr, Column(dialect, "id")],
                from_=table,
            ).to_sql()
            result = backend.execute(
                sql,
                params,
                options=ExecutionOptions(stmt_type=StatementType.DQL),
            )
            values = {row["id"]: row["seconds"] for row in result.data}
        finally:
            _drop(backend, table)

        assert values[1] == pytest.approx(183845.25, abs=1e-6)
        assert values[2] == pytest.approx(-183845.25, abs=1e-6)
        assert values[3] == pytest.approx(-0.25, abs=1e-6)
        assert values[4] == pytest.approx(0.0, abs=1e-6)

    def test_date_columns_subtract_through_the_timestamp_cast(self, live_backend):
        """``DATE - DATE`` is a day number; the cast makes it an interval."""
        backend = live_backend
        table = "SECONDS_BETWEEN_DATE"

        _drop(backend, table)
        backend.executescript(
            f"""
            CREATE TABLE {table} (
                id NUMBER(4),
                started_at DATE,
                ended_at DATE
            )
            """
        )
        try:
            backend.execute(
                f"INSERT INTO {table} VALUES (1, "
                "TO_DATE('2026-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'), "
                "TO_DATE('2026-01-02 01:02:03','YYYY-MM-DD HH24:MI:SS'))"
            )
            dialect = backend.dialect
            expr = seconds_between(
                dialect,
                Column(dialect, "started_at"),
                Column(dialect, "ended_at"),
            ).as_("seconds")
            sql, params = QueryStatement(
                dialect,
                select=[expr, Column(dialect, "id")],
                from_=table,
            ).to_sql()
            result = backend.execute(
                sql,
                params,
                options=ExecutionOptions(stmt_type=StatementType.DQL),
            )
        finally:
            _drop(backend, table)

        assert result.data[0]["seconds"] == pytest.approx(90123.0, abs=1e-6)
