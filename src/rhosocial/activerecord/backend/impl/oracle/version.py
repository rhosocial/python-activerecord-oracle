# src/rhosocial/activerecord/backend/impl/oracle/version.py
"""Oracle version parsing helpers."""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional, Tuple


VERSION_FULL_QUERY = (
    "SELECT VERSION, VERSION_FULL FROM PRODUCT_COMPONENT_VERSION "
    "WHERE PRODUCT LIKE 'Oracle Database%'"
)
VERSION_QUERY = "SELECT VERSION FROM PRODUCT_COMPONENT_VERSION WHERE PRODUCT LIKE 'Oracle Database%'"
_VERSION_PART_RE = re.compile(r"^\d+")


def parse_version(value: Any) -> Tuple[int, ...]:
    """Parse an Oracle dotted version into integer components."""
    if value is None:
        return ()
    if isinstance(value, (tuple, list)):
        parts = list(value)
    else:
        text = str(value).strip()
        if not text:
            return ()
        parts = text.split(".")
    result = []
    for part in parts:
        text = str(part).strip()
        match = _VERSION_PART_RE.match(text)
        if match is None:
            break
        result.append(int(match.group(0)))
    return tuple(result)


def parse_version_row(row: Any) -> Tuple[Tuple[int, ...], Optional[Tuple[int, ...]]]:
    """Parse a PRODUCT_COMPONENT_VERSION row into base and full versions."""
    if row is None:
        return (), None
    if isinstance(row, Mapping):
        lowered = {str(key).lower(): value for key, value in row.items()}
        version = lowered.get("version")
        version_full = lowered.get("version_full")
    else:
        version = row[0] if len(row) > 0 else None
        version_full = row[1] if len(row) > 1 else None
    base = parse_version(version)
    full = parse_version(version_full)
    return base, full or None


def normalize_version(value: Any, minimum_length: int = 3) -> Tuple[int, ...]:
    """Normalize a version to at least the requested component count."""
    parsed = parse_version(value)
    if not parsed:
        return ()
    if len(parsed) < minimum_length:
        return parsed + (0,) * (minimum_length - len(parsed))
    return parsed


def ru_from_version_full(value: Any) -> Optional[int]:
    """Return the RU component from an Oracle VERSION_FULL value."""
    parsed = parse_version(value)
    if len(parsed) < 2:
        return None
    return parsed[1]


__all__ = [
    "VERSION_FULL_QUERY",
    "VERSION_QUERY",
    "parse_version",
    "parse_version_row",
    "normalize_version",
    "ru_from_version_full",
]
