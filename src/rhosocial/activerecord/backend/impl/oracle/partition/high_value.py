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

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple

from .._scan import (
    parse_signed_decimal,
    parse_signed_integer,
    scan_single_quoted,
    skip_space,
    starts_with_word,
)

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

#: Function names whose first argument is the date text to read.
_DATE_FUNCTION_NAMES = ("TO_DATE", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ")


def _scan_date_function(token: str) -> Optional[str]:
    """The value literal of ``TO_DATE(...)``-style text, or ``None``.

    The accepted shape is a listed function name, an open parenthesis, one
    quoted value, an optional quoted format and any further quoted arguments,
    a close parenthesis and nothing but whitespace after it.
    """
    name = None
    for candidate in _DATE_FUNCTION_NAMES:
        if starts_with_word(token, candidate):
            name = candidate
            break
    if name is None:
        return None
    index = skip_space(token, len(name))
    length = len(token)
    if index >= length or token[index] != "(":
        return None
    index = skip_space(token, index + 1)
    scanned = scan_single_quoted(token, index)
    if scanned is None:
        return None
    value, index = scanned
    index = skip_space(token, index)
    if index < length and token[index] == ",":
        while index < length and token[index] == ",":
            index = skip_space(token, index + 1)
            scanned = scan_single_quoted(token, index)
            if scanned is None:
                return None
            index = skip_space(token, scanned[1])
    if index >= length or token[index] != ")":
        return None
    index = skip_space(token, index + 1)
    if index != length:
        return None
    return value


def _scan_word_date_literal(token: str, word: str) -> Optional[str]:
    """The value literal of ``WORD 'text'``, or ``None``."""
    if not starts_with_word(token, word):
        return None
    index = skip_space(token, len(word))
    scanned = scan_single_quoted(token, index)
    if scanned is None:
        return None
    value, index = scanned
    index = skip_space(token, index)
    if index != len(token):
        return None
    return value


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

    match = _scan_date_function(token)
    if match is not None:
        return _parse_date_text(match)
    match = _scan_word_date_literal(token, "TIMESTAMP")
    if match is not None:
        return _parse_date_text(match)
    match = _scan_word_date_literal(token, "DATE")
    if match is not None:
        return _parse_date_text(match)

    integer = parse_signed_integer(token)
    if integer is not None:
        return integer
    decimal = parse_signed_decimal(token)
    if decimal is not None:
        return decimal

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
