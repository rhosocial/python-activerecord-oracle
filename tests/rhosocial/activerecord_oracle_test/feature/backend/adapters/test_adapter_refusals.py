# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/test_adapter_refusals.py
"""The Oracle scalar adapters' pass-throughs and refusals.

The neutral-family adapters have their own file; this one covers the
per-scalar adapters: what they hand back untouched, and what the interval
adapter refuses when a value cannot be expressed in the shape declared.

The interval rules are worth stating plainly, because they are real Oracle
semantics rather than adapter preferences:

* a ``DAY TO SECOND`` value can always become a ``timedelta`` (it has a fixed
  length);
* a ``YEAR TO MONTH`` value cannot, because a month has no fixed length;
* going the other way, a ``timedelta`` cannot become ``YEAR TO MONTH`` for the
  same reason.
"""

import datetime
from types import SimpleNamespace

import pytest

from rhosocial.activerecord.backend.impl.oracle.adapters import (
    OracleBooleanAdapter,
    OracleBytesAdapter,
    OracleDateAdapter,
    OracleIntervalAdapter,
)


class TestBooleanAdapterRefusals:
    def test_a_bool_target_keeps_the_value_a_bool(self):
        adapter = OracleBooleanAdapter()

        assert adapter.to_database(True, bool) is True
        assert adapter.to_database(1, int) == 1


class TestDateAdapterRefusals:
    def test_a_date_is_written_untouched(self):
        """Oracle's DATE column takes the Python object through the driver."""
        adapter = OracleDateAdapter()
        value = datetime.date(2024, 1, 1)

        assert adapter.to_database(value, str) is value

    def test_a_value_that_is_not_a_datetime_is_carried_back_unchanged(self):
        adapter = OracleDateAdapter()

        assert adapter.from_database("2024-01-01", datetime.date) == "2024-01-01"

    def test_a_datetime_read_is_narrowed_to_its_date(self):
        adapter = OracleDateAdapter()

        assert adapter.from_database(datetime.datetime(2024, 1, 1, 12, 0), datetime.date) == (
            datetime.date(2024, 1, 1)
        )


class TestBytesAdapterRefusals:
    def test_a_non_bytes_value_is_carried_back_unchanged(self):
        adapter = OracleBytesAdapter()

        assert adapter.from_database("text", str) == "text"


class TestIntervalAdapterRefusals:
    def test_a_timedelta_round_trips_as_day_to_second(self):
        adapter = OracleIntervalAdapter()
        value = datetime.timedelta(days=1, hours=2)

        text = adapter.to_database(value, str)

        assert isinstance(text, str)
        assert adapter.from_database(text, datetime.timedelta) == value

    def test_a_timedelta_can_be_returned_as_its_declared_shape(self):
        adapter = OracleIntervalAdapter()
        value = datetime.timedelta(days=1)

        assert adapter.from_database(value, datetime.timedelta) is value
        assert isinstance(
            adapter.from_database(value, adapter.IntervalDayToSecond),
            adapter.IntervalDayToSecond,
        )

    def test_a_day_to_second_value_cannot_become_year_to_month(self):
        adapter = OracleIntervalAdapter()

        with pytest.raises(ValueError, match="cannot be expressed as YEAR TO MONTH"):
            adapter.from_database(datetime.timedelta(days=1), adapter.IntervalYearToMonth)

    def test_a_year_to_month_object_is_rebuilt(self):
        """oracledb hands over ``IntervalYM(years=..., months=...)``."""
        adapter = OracleIntervalAdapter()
        native = SimpleNamespace(years=1, months=2)

        rebuilt = adapter.from_database(native, adapter.IntervalYearToMonth)
        assert rebuilt.years == 1 and rebuilt.months == 2

    def test_a_year_to_month_object_cannot_become_a_timedelta(self):
        adapter = OracleIntervalAdapter()

        with pytest.raises(ValueError, match="months have no fixed length"):
            adapter.from_database(SimpleNamespace(years=1, months=2), datetime.timedelta)

    def test_year_to_month_text_cannot_become_a_timedelta(self):
        adapter = OracleIntervalAdapter()

        with pytest.raises(ValueError, match="months have no fixed length"):
            adapter.from_database("1-2", datetime.timedelta)

    def test_an_unrecognised_interval_text_is_named(self):
        adapter = OracleIntervalAdapter()

        with pytest.raises(ValueError, match="Unrecognised Oracle interval text"):
            adapter.from_database("abc", str)
