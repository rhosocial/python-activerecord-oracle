# tests/rhosocial/activerecord_oracle_test/feature/backend/expression/test_datetime_operations.py

"""Oracle's date arithmetic, rendered.

Two things were wrong here and neither raised anything useful. The formatter
called a method that does not exist, so every date operation failed with an
AttributeError naming a private helper -- a name no reader could act on. And
the difference between two dates was written the way PostgreSQL writes it,
which Oracle rejects: ``-`` is defined for numbers and intervals, so
``end - start`` on two DATE columns is an ORA-00932.

There is no local Oracle to run against, so these assert on the SQL. What they
can catch is the failure mode that actually happened: an operation that
renders at all is not evidence that it is valid, and ``(end - start) * 1``
renders perfectly happily on a dialect that cannot execute it.
"""

import pytest

from rhosocial.activerecord.backend.expression import Column, DateTimeColumn


@pytest.fixture
def dialect():
    from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

    return OracleDialect()


@pytest.fixture
def started_at(dialect):
    return DateTimeColumn(dialect, "started_at", table="t")


@pytest.fixture
def ended_at(dialect):
    return DateTimeColumn(dialect, "ended_at", table="t")


class TestDateArithmeticRenders:
    """Every one of these used to raise AttributeError."""

    def test_trunc(self, started_at):
        sql, _ = started_at.date_trunc("day").to_sql()
        assert sql.startswith("TRUNC(")
        assert "'DD'" in sql

    def test_trunc_month_uses_the_oracle_format(self, started_at):
        sql, _ = started_at.date_trunc("month").to_sql()
        assert "'MM'" in sql

    def test_add_uses_an_interval(self, started_at):
        sql, params = started_at.date_add(3, "day").to_sql()
        assert "NUMTOYMINTERVAL" in sql or "NUMTODSINTERVAL" in sql
        assert 3 in params

    def test_subtract_uses_an_interval(self, started_at):
        sql, params = started_at.date_sub(2, "hour").to_sql()
        assert "NUMTODSINTERVAL" in sql
        assert 2 in params

    def test_year_and_week_intervals(self, started_at):
        year, _ = started_at.date_add(1, "year").to_sql()
        week, _ = started_at.date_add(1, "week").to_sql()
        assert "NUMTOYMINTERVAL" in year
        assert "NUMTODSINTERVAL" in week

    def test_the_operand_is_bound(self, started_at):
        """The interval length came from the caller, so it is data."""
        sql, params = started_at.date_add(7, "day").to_sql()
        assert "?" in sql
        assert params == (7,)


class TestDateDifferenceIsOracleSyntax:
    """``end - start`` is PostgreSQL. Oracle has to go through an interval."""

    @pytest.mark.parametrize("unit", ["day", "hour", "minute", "second", "week"])
    def test_no_bare_date_subtraction(self, started_at, ended_at, unit):
        sql, _ = started_at.date_diff(unit, ended_at).to_sql()
        assert "CAST" in sql and "AS DATE" in sql

    def test_day_is_the_default_unit(self, started_at, ended_at):
        sql, _ = started_at.date_diff("day", ended_at).to_sql()
        assert "86400" in sql

    def test_finer_units_divide_the_second_count(self, started_at, ended_at):
        hour, _ = started_at.date_diff("hour", ended_at).to_sql()
        week, _ = started_at.date_diff("week", ended_at).to_sql()
        assert "3600" in hour
        assert "604800" in week

    def test_month_uses_months_between(self, started_at, ended_at):
        sql, _ = started_at.date_diff("month", ended_at).to_sql()
        assert "MONTHS_BETWEEN" in sql

    def test_year_divides_months(self, started_at, ended_at):
        sql, _ = started_at.date_diff("year", ended_at).to_sql()
        assert "MONTHS_BETWEEN" in sql
        assert "/ 12" in sql

    def test_both_operands_are_rendered(self, started_at, ended_at):
        sql, _ = started_at.date_diff("day", ended_at).to_sql()
        assert "STARTED_AT" in sql
        assert "ENDED_AT" in sql


class TestAliasing:
    """The alias belongs outside the expression, whichever way it was built."""

    def test_alias_after_the_operation(self, started_at):
        sql, _ = started_at.date_trunc("day").as_("d").to_sql()
        assert sql.upper().endswith('AS "D"')

    def test_alias_before_the_operation(self, started_at):
        sql, _ = started_at.as_("s").date_trunc("day").to_sql()
        assert 'AS "D"' not in sql.upper()
        assert "TRUNC(" in sql