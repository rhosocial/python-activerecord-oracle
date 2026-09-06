# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_oracle_datetime_functions.py
"""Tests for the Oracle date/time function factories.

Covers ``functions/datetime.py``: ``to_date``, ``to_char``,
``to_timestamp``, ``to_timestamp_tz``, ``trunc_date``, ``add_months``,
``months_between``, ``last_day``, ``next_day`` and ``extract_date``. Each
factory is asserted for the emitted SQL/params through the assembled
``OracleDialect``, for string vs. expression inputs, and for the
optional-format argument forms.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.functions.datetime import (
    add_months,
    extract_date,
    last_day,
    months_between,
    next_day,
    to_char,
    to_date,
    to_timestamp,
    to_timestamp_tz,
    trunc_date,
)


@pytest.fixture
def dialect():
    """Return a 23ai Oracle dialect."""
    return OracleDialect(version=(23, 4, 0))


class TestToDate:
    """The TO_DATE factory."""

    def test_without_format(self, dialect):
        """A bare string argument renders as a plain TO_DATE(column)."""
        sql, params = to_date(dialect, "hire_date").to_sql()
        assert sql == "TO_DATE(hire_date)", "no format means a single argument"
        assert params == (), "no parameters are expected"

    def test_with_format(self, dialect):
        """A format mask becomes a bound literal argument."""
        sql, params = to_date(dialect, "hire_date", "YYYY-MM-DD").to_sql()
        assert sql == "TO_DATE(hire_date, ?)", "format must be the second argument"
        assert params == ("YYYY-MM-DD",), "format mask must be bound as a parameter"

    def test_with_numeric_input(self, dialect):
        """A numeric input is converted to a bound literal."""
        sql, params = to_date(dialect, 20240101).to_sql()
        assert sql == "TO_DATE(?)", "numeric input must become a literal placeholder"
        assert params == (20240101,), "numeric value must be bound"

    def test_with_expression_input(self, dialect):
        """A BaseExpression input passes through unchanged."""
        expr = core.Column(dialect, "hire_date")
        sql, params = to_date(dialect, expr, "YYYY-MM-DD").to_sql()
        assert sql == "TO_DATE(hire_date, ?)", "expression input must be preserved"
        assert params == ("YYYY-MM-DD",), "format mask must be bound"


class TestToChar:
    """The TO_CHAR factory."""

    def test_without_format(self, dialect):
        """A bare argument renders as TO_CHAR(column)."""
        sql, params = to_char(dialect, "salary").to_sql()
        assert sql == "TO_CHAR(salary)", "no format means a single argument"
        assert params == (), "no parameters are expected"

    def test_with_format(self, dialect):
        """A format mask becomes a bound literal argument."""
        sql, params = to_char(dialect, "hire_date", "YYYY").to_sql()
        assert sql == "TO_CHAR(hire_date, ?)", "format must be the second argument"
        assert params == ("YYYY",), "format mask must be bound as a parameter"


class TestToTimestamp:
    """The TO_TIMESTAMP factory."""

    def test_without_format(self, dialect):
        """A bare argument renders as TO_TIMESTAMP(column)."""
        sql, params = to_timestamp(dialect, "event_ts").to_sql()
        assert sql == "TO_TIMESTAMP(event_ts)", "no format means a single argument"
        assert params == (), "no parameters are expected"

    def test_with_format(self, dialect):
        """A format mask becomes a bound literal argument."""
        sql, params = to_timestamp(dialect, "event_ts", "YYYY-MM-DD HH24:MI:SS").to_sql()
        assert sql == "TO_TIMESTAMP(event_ts, ?)", "format must be the second argument"
        assert params == ("YYYY-MM-DD HH24:MI:SS",), "format mask must be bound"


class TestToTimestampTz:
    """The TO_TIMESTAMP_TZ factory."""

    def test_without_format(self, dialect):
        """A bare argument renders as TO_TIMESTAMP_TZ(column)."""
        sql, params = to_timestamp_tz(dialect, "event_ts").to_sql()
        assert sql == "TO_TIMESTAMP_TZ(event_ts)", "no format means a single argument"
        assert params == (), "no parameters are expected"

    def test_with_format(self, dialect):
        """A format mask becomes a bound literal argument."""
        sql, params = to_timestamp_tz(dialect, "event_ts", "YYYY-MM-DD TZH:TZM").to_sql()
        assert sql == "TO_TIMESTAMP_TZ(event_ts, ?)", "format must be the second argument"
        assert params == ("YYYY-MM-DD TZH:TZM",), "format mask must be bound"


class TestTruncDate:
    """The TRUNC(date) factory."""

    def test_without_format(self, dialect):
        """A bare argument renders as TRUNC(column)."""
        sql, params = trunc_date(dialect, "hire_date").to_sql()
        assert sql == "TRUNC(hire_date)", "no format means a single argument"
        assert params == (), "no parameters are expected"

    def test_with_format(self, dialect):
        """A format mask becomes a bound literal argument."""
        sql, params = trunc_date(dialect, "hire_date", "MM").to_sql()
        assert sql == "TRUNC(hire_date, ?)", "format must be the second argument"
        assert params == ("MM",), "format mask must be bound"


class TestAddMonths:
    """The ADD_MONTHS factory."""

    def test_string_date(self, dialect):
        """A string date renders as ADD_MONTHS(column, ?)."""
        sql, params = add_months(dialect, "hire_date", 3).to_sql()
        assert sql == "ADD_MONTHS(hire_date, ?)", "months must be a bound literal"
        assert params == (3,), "month count must be bound"

    def test_numeric_date(self, dialect):
        """A numeric date becomes a bound literal for the date operand."""
        sql, params = add_months(dialect, 20240101, 1).to_sql()
        assert sql == "ADD_MONTHS(?, ?)", "numeric date must become a literal"
        assert params == (20240101, 1), "both operands must be bound"


class TestMonthsBetween:
    """The MONTHS_BETWEEN factory."""

    def test_string_dates(self, dialect):
        """String dates render as MONTHS_BETWEEN(a, b)."""
        sql, params = months_between(dialect, "start_date", "end_date").to_sql()
        assert sql == "MONTHS_BETWEEN(start_date, end_date)", "both columns must be inlined"
        assert params == (), "no parameters are expected"

    def test_numeric_dates(self, dialect):
        """Numeric dates become bound literals."""
        sql, params = months_between(dialect, 20240101, 20230101).to_sql()
        assert sql == "MONTHS_BETWEEN(?, ?)", "numeric dates must become literals"
        assert params == (20240101, 20230101), "both numeric dates must be bound"


class TestLastDay:
    """The LAST_DAY factory."""

    def test_string_date(self, dialect):
        """A string date renders as LAST_DAY(column)."""
        sql, params = last_day(dialect, "hire_date").to_sql()
        assert sql == "LAST_DAY(hire_date)", "single column argument expected"
        assert params == (), "no parameters are expected"


class TestNextDay:
    """The NEXT_DAY factory."""

    def test_string_date(self, dialect):
        """A weekday name becomes a bound literal argument."""
        sql, params = next_day(dialect, "hire_date", "MON").to_sql()
        assert sql == "NEXT_DAY(hire_date, ?)", "weekday must be the second argument"
        assert params == ("MON",), "weekday must be bound as a parameter"


class TestExtractDate:
    """The EXTRACT factory."""

    def test_string_source(self, dialect):
        """A string source renders as EXTRACT(component FROM column)."""
        sql, params = extract_date(dialect, "YEAR", "hire_date").to_sql()
        assert sql == "EXTRACT(YEAR FROM hire_date)", "component and source must be combined"
        assert params == (), "no parameters are expected"

    def test_component_uppercased(self, dialect):
        """A lowercase component is normalised to uppercase."""
        sql, params = extract_date(dialect, "month", "hire_date").to_sql()
        assert sql == "EXTRACT(MONTH FROM hire_date)", "component must be uppercased"
        assert params == (), "no parameters are expected"

    def test_numeric_source_is_not_literal(self, dialect):
        """Numeric sources stay raw (never converted to bound literals)."""
        sql, params = extract_date(dialect, "YEAR", 2024).to_sql()
        assert sql == "EXTRACT(YEAR FROM 2024)", "numeric source must be inlined verbatim"
        assert params == (), "no parameters are expected"

    def test_expression_source(self, dialect):
        """A BaseExpression source passes through unchanged."""
        expr = core.Column(dialect, "hire_date")
        sql, params = extract_date(dialect, "YEAR", expr).to_sql()
        assert sql == "EXTRACT(YEAR FROM hire_date)", "expression source must be preserved"
        assert params == (), "no parameters are expected"