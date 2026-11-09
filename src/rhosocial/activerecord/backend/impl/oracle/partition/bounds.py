# src/rhosocial/activerecord/backend/impl/oracle/partition/bounds.py
"""Boundary arithmetic for Oracle range partition policies.

Oracle RANGE partitioning speaks in upper bounds: ``VALUES LESS THAN (x)``.
This module converts the half-open ``[start, end)`` vocabulary used by the
plan into that upper bound, and owns the calendar arithmetic every generated
range policy needs:

* how a named interval advances a ``date``/``datetime`` value, and
* how the start of an interval becomes a partition-name label.

Both are pure functions, so the policy objects and the maintenance executor
agree on boundaries without either owning a calendar.
"""

import calendar
from datetime import date, datetime, timedelta
from typing import Any, Iterator, Optional, Tuple, Union

__all__ = [
    "Interval",
    "add_interval",
    "default_label",
    "iter_range_bounds",
    "normalize_interval",
]

#: A named unit or an ``(amount, unit)`` pair, e.g. ``"month"`` or ``(15, "day")``.
Interval = Union[str, Tuple[int, str]]

_NAMED_UNITS = ("day", "week", "month", "quarter", "year")
_UNIT_MONTHS = {"month": 1, "quarter": 3, "year": 12}
_UNIT_DAYS = {"day": 1, "week": 7}


def normalize_interval(interval: Interval) -> Tuple[int, str]:
    """Return ``(amount, unit)`` for a policy interval.

    Accepted spellings are a unit name (``day``, ``week``, ``month``,
    ``quarter`` or ``year``), a countable spelling such as ``"15 day"`` /
    ``"1 month"``, and an ``(amount, unit)`` pair. The amount must be a
    positive integer.

    Raises:
        ValueError: if the interval is not one of these spellings.
        TypeError: if the interval is not a string or a pair.
    """
    if isinstance(interval, tuple):
        if len(interval) != 2:
            raise ValueError("interval pairs must be (amount, unit)")
        amount, unit = interval
        if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            raise ValueError("interval amount must be a positive integer")
    elif isinstance(interval, str):
        parts = interval.strip().lower().split()
        if len(parts) == 1:
            amount, unit = 1, parts[0]
        elif len(parts) == 2:
            try:
                amount = int(parts[0])
            except ValueError as exc:
                raise ValueError(
                    "interval amount must be a positive integer, got %r" % parts[0]
                ) from exc
            unit = parts[1]
        else:
            raise ValueError("interval must be a unit name or '<amount> <unit>'")
        if amount <= 0:
            raise ValueError("interval amount must be a positive integer")
    else:
        raise TypeError("interval must be a string or an (amount, unit) pair")

    if unit not in _NAMED_UNITS:
        raise ValueError(
            "interval unit must be one of %s, got %r" % (", ".join(_NAMED_UNITS), unit)
        )
    return amount, unit


def _shift_months(value: Any, months: int) -> Any:
    """Advance a date or datetime by *months*, clamping the day of month."""
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def add_interval(value: Any, interval: Interval) -> Any:
    """Advance *value* by one step of *interval*.

    ``date`` and ``datetime`` values are supported; a ``datetime`` keeps its
    time and tzinfo, which is what keeps generated Oracle boundaries stable
    regardless of the session time zone.

    Raises:
        TypeError: if *value* is not a date or datetime.
    """
    if not isinstance(value, (date, datetime)):
        raise TypeError("bound values must be date or datetime, got %r" % type(value))
    amount, unit = normalize_interval(interval)
    if unit in _UNIT_DAYS:
        return value + timedelta(days=_UNIT_DAYS[unit] * amount)
    return _shift_months(value, _UNIT_MONTHS[unit] * amount)


def default_label(start: Any, interval: Interval) -> str:
    """Derive the default partition-name label from an interval start.

    Monthly plans read ``2026_01``, quarterly ones ``2026_q1``, yearly ones
    ``2026``, and day/week-granular plans use ``2026_01_01``. The label is the
    suffix of a generated partition name, so it contains no separator beyond
    ``_``.
    """
    _amount, unit = normalize_interval(interval)
    if unit == "year":
        return start.strftime("%Y")
    if unit == "quarter":
        return "%s_q%d" % (start.strftime("%Y"), (start.month - 1) // 3 + 1)
    if unit == "month":
        return start.strftime("%Y_%m")
    return start.strftime("%Y_%m_%d")


def iter_range_bounds(
    start: Any,
    interval: Interval = "month",
    count: Optional[int] = None,
    through: Optional[Any] = None,
) -> Iterator[Tuple[Any, Any]]:
    """Yield ``(start, end)`` half-open ranges from *start*.

    Generation stops at the first of two limits: *count* boundaries have been
    produced, or the next start would not be before *through*. Both limits may
    be supplied; at least one is required.

    Args:
        start: First boundary, as ``date`` or ``datetime``.
        interval: Step between boundaries.
        count: Maximum number of ranges to yield.
        through: Exclusive upper limit. A range whose start is at or beyond
            this value is not produced.

    Raises:
        TypeError: if *start* is not a date or datetime.
        ValueError: if neither limit is given, *count* is not positive, or
            *through* is not after *start*.
    """
    if not isinstance(start, (date, datetime)):
        raise TypeError("start must be date or datetime, got %r" % type(start))
    if count is None and through is None:
        raise ValueError("iter_range_bounds requires count or through")
    if count is not None:
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ValueError("count must be a positive integer")
    if through is not None and through <= start:
        raise ValueError("through must be after start")

    current = start
    produced = 0
    while True:
        if count is not None and produced >= count:
            return
        if through is not None and current >= through:
            return
        end = add_interval(current, interval)
        yield current, end
        current = end
        produced += 1
