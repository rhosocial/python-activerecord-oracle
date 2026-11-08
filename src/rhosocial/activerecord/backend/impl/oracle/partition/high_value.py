# src/rhosocial/activerecord/backend/impl/oracle/partition/high_value.py
"""Conservative parsing of Oracle's ``HIGH_VALUE`` catalogue text.

``ALL_TAB_PARTITIONS.HIGH_VALUE`` and ``ALL_TAB_SUBPARTITIONS.HIGH_VALUE`` are
SQL expression *text*, not values. Oracle spells a date bound as
``TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')``, a string LIST
bound as ``'EAST', 'NORTH'``, and a terminally unbound partition as
``MAXVALUE``. The text is useful for comparison, but only when it is parsed
strictly: a bound this module cannot recognize must be reported as unparsed
rather than treated as a match.

The parser accepts only a small closed grammar:

* ``MAXVALUE`` and ``DEFAULT``;
* quoted string literals;
* integer and decimal numeric literals;
* ``DATE '...'``, ``TIMESTAMP '...'`` and ``TO_DATE(...)`` /
  ``TO_TIMESTAMP(...)`` literals in common Oracle date formats;
* comma-separated values, including one parenthesized row group.

Anything else -- a function expression, an interval, a format this module does
not know -- raises :class:`ValueError` and the caller records an issue.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple

__all__ = [
    "ParsedHighValue",
    "parse_high_value",
]

_MAXVALUE = "MAXVALUE"
_DEFAULT = "DEFAULT"
_NULL = "NULL"

_DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S.%f %z",
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%d %H:%M %z",
)

_TO_DATE_RE = re.compile(
    r"(?is)^(?:to_date|to_timestamp|to_timestamp_tz)\s*\(\s*"
    r"'(?P<value>(?:[^']|'')*)'\s*"
    r"(?:,\s*'(?P<format>(?:[^']|'')*)'\s*"
    r"(?:,\s*'(?:[^']|'')*'\s*)*)?\)\s*$"
)
_TIMESTAMP_RE = re.compile(r"(?is)^timestamp\s*'(?P<value>(?:[^']|'')*)'\s*$")
_DATE_LITERAL_RE = re.compile(r"(?is)^date\s*'(?P<value>(?:[^']|'')*)'\s*$")
_INTEGER_RE = re.compile(r"^[+-]?\d+$")
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+\.\d*|\.\d+|\d+[eE][+-]?\d+)$")


@dataclass(frozen=True)
class ParsedHighValue:
    """The outcome of parsing one catalogue bound.

    Attributes:
        values: Parsed scalars, or one tuple per parenthesized row group.
        parsed: Whether *values* is a trustworthy representation of the text.
        error: Why parsing failed, when ``parsed`` is false.
        text: The original catalogue text, preserved verbatim.
    """

    text: Optional[str]
    values: Tuple[Any, ...] = ()
    parsed: bool = False
    error: Optional[str] = None


def parse_high_value(text: Optional[str], method: Optional[str]) -> ParsedHighValue:
    """Parse one Oracle ``HIGH_VALUE`` string for *method*.

    Args:
        text: Catalogue text exactly as Oracle reports it.
        method: ``RANGE``, ``LIST`` or ``HASH``.

    Returns:
        A :class:`ParsedHighValue`. An empty bound is treated as "nothing to
        parse" (true for HASH); any non-empty text this module cannot recognize
        is returned with ``parsed=False`` and an ``error``.
    """
    original = text
    if text is None or not str(text).strip():
        return ParsedHighValue(original, (), True, None)
    stripped = str(text).strip()
    normalized = (method or "").strip().upper()
    if stripped.upper() == _MAXVALUE:
        return ParsedHighValue(original, (_MAXVALUE,), True, None)
    try:
        if normalized == "RANGE":
            values = _parse_range(stripped)
        elif normalized == "LIST":
            values = _parse_list(stripped)
        elif normalized == "HASH":
            raise ValueError("HASH partitions carry no bound")
        else:
            raise ValueError("unknown partition method %r" % method)
    except ValueError as exc:
        return ParsedHighValue(original, (), False, str(exc))
    return ParsedHighValue(original, values, True, None)


def _parse_range(text: str) -> Tuple[Any, ...]:
    inner, wrapped = _unwrap_parentheses(text)
    if wrapped:
        return tuple(_parse_scalar(token) for token in _split_top_level(inner))
    return (_parse_scalar(text),)


def _parse_list(text: str) -> Tuple[Any, ...]:
    if text.strip().upper() == _DEFAULT:
        return (_DEFAULT,)
    return tuple(
        _parse_scalar(token) for token in _split_top_level(text)
    )


def _parse_scalar(text: str) -> Any:
    token = text.strip()
    if not token:
        raise ValueError("empty value")
    inner, wrapped = _unwrap_parentheses(token)
    if wrapped:
        return tuple(_parse_scalar(part) for part in _split_top_level(inner))

    upper = token.upper()
    if upper == _MAXVALUE:
        return _MAXVALUE
    if upper == _DEFAULT:
        return _DEFAULT
    if upper == _NULL:
        return None

    if token[0] == "'" and token[-1] == "'" and len(token) >= 2:
        return token[1:-1].replace("''", "'")

    match = _TO_DATE_RE.match(token)
    if match:
        return _parse_date_text(match.group("value"))
    match = _TIMESTAMP_RE.match(token)
    if match:
        return _parse_date_text(match.group("value"))
    match = _DATE_LITERAL_RE.match(token)
    if match:
        return _parse_date_text(match.group("value"))

    if _INTEGER_RE.match(token):
        return int(token)
    if _DECIMAL_RE.match(token):
        return float(token)

    raise ValueError("cannot parse bound token %r" % token)


def _parse_date_text(value: str) -> Any:
    text = value.strip()
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if _is_midnight(parsed):
            return parsed.date()
        return parsed
    raise ValueError("unknown date bound %r" % text)


def _is_midnight(value: datetime) -> bool:
    return (
        value.hour == 0
        and value.minute == 0
        and value.second == 0
        and value.microsecond == 0
    )


def _split_top_level(text: str) -> Tuple[str, ...]:
    """Split *text* on commas that are outside quotes and parentheses."""
    parts = []
    start = 0
    depth = 0
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "'":
            index += 1
            while index < length:
                if text[index] == "'":
                    if index + 1 < length and text[index + 1] == "'":
                        index += 2
                        continue
                    break
                index += 1
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
        index += 1
    tail = text[start:]
    if tail.strip() or not parts:
        parts.append(tail)
    return tuple(part for part in parts if part.strip())


def _unwrap_parentheses(text: str) -> Tuple[str, bool]:
    """Return the inside of one whole parenthesized group, if that is the shape."""
    stripped = text.strip()
    if not stripped.startswith("(") or not stripped.endswith(")"):
        return stripped, False
    depth = 0
    index = 0
    length = len(stripped)
    while index < length:
        char = stripped[index]
        if char == "'":
            index += 1
            while index < length:
                if stripped[index] == "'":
                    if index + 1 < length and stripped[index + 1] == "'":
                        index += 2
                        continue
                    break
                index += 1
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0 and index != length - 1:
                return stripped, False
        index += 1
    if depth != 0:
        raise ValueError("unbalanced parentheses in bound %r" % stripped)
    return stripped[1:-1].strip(), True
