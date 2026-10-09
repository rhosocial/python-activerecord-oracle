# src/rhosocial/activerecord/backend/impl/oracle/version.py
"""Oracle version parsing, and how the server's own version is found.

The version number is what every gate in this backend compares, so the job of
this module is to produce it from something the server cannot rename. It used to
select ``... FROM PRODUCT_COMPONENT_VERSION WHERE PRODUCT LIKE 'Oracle Database
%'`` and parse the first row, and the rename below broke it.

The rename, measured
--------------------
``PRODUCT`` is free text, Oracle has used it as a brand, and on the current
23.0 line it changed under this query's feet. Read directly off both wired
servers as ``system``:

===================================  ==============================  ==============
server                              ``PRODUCT``                     ``VERSION``
===================================  ==============================  ==============
21c Express Edition (``21.3.0.0.0``) ``Oracle Database 21c Express   ``21.0.0.0.0``
                                     ``Edition `` (note the trailing
                                     space, 36 characters)
26ai Free (``23.26.1.0.0``)         ``Oracle AI Database 26ai Free``  ``23.0.0.0.0``
                                     (28 characters)
===================================  ==============================  ==============

``'Oracle Database%'`` matches the first and not the second — ``Oracle AI
Database`` does not start with ``Oracle Database`` — so both queries returned no
rows, ``get_server_version`` logged *"Could not determine Oracle version …
defaulting to 19.0.0"*, and every ``self.version >= …`` comparison in the
backend answered against ``(19, 0, 0)``. Oracle documents the same product name
from the other side: ``V$INSTANCE.EDITION`` spells the 26ai edition
``FREE: Oracle AI Database Free``, where 21c's is ``XE``.

Why not match the new name
--------------------------
``'Oracle AI Database%'`` would fix this server and break the next rename, so it
is not the fix. The two names differ in a place Oracle chose to differ — the
brand — and the gates do not compare the brand. They compare numbers. The number
is also the part that is *stable across the rename*: ``26ai`` is the branding of
the 23.0 line, so both products report ``VERSION = '23.0.0.0.0'`` and the
feature that arrived in "23ai" and the feature the 26ai references call "new in
26ai" are the same feature on the same code line. Oracle states the numbering
rule outright: "The first number of the release stays the same since Oracle AI
Database 26ai simply replaces Oracle Database 23ai – it remains '23'. The second
number of the release indicates the year of the release update, e.g. 26 for
2026", and the measured ``VERSION_FULL`` ``23.26.1.0.0`` follows it (23 = the
line, 26 = 2026, 1 = the January RU).

So this module matches on **the shape of the version**, and never on a product
name:

* a row qualifies when its ``VERSION`` is a plain dotted number — measured on
  both wired servers, which is the database row and, on an edition with bundled
  components, is also the shape that keeps component rows out;
* among qualifying rows the one carrying a ``VERSION_FULL`` wins, because the
  database row carries the release update and a component row pinned to the same
  release does not;
* ties go to the highest leading major.

:func:`select_version_row` is that rule, as a pure function, because it is the
part worth pinning. The SQL is deliberately no cleverer than it has to be: it
returns candidates and lets the rule choose.

23ai versus 26ai
----------------
If the gates ever need to tell those two apart — and the reference manuals
routinely call one feature "new in 23ai" and another "new in 26ai" — the number
that separates them is **already measured and already exposed**: it is the second
component of ``VERSION_FULL``, which this module already returns as
``version_full`` and the dialect already exposes as ``ru_version``. The measured
26ai server reports ``23.26.1.0.0``, so ``ru_version`` is ``26``; the 23ai
release updates Oracle lists for the same line are ``23.4.0.25.04``,
``23.8.0.25.04``, ``23.9.0.25.07`` and ``23.10.0.25.10``, so a 23ai server's
second component is below 26. That is the whole distinction, and it is a number
rather than a brand. No gate in this backend may pretend to distinguish them any
other way: the server exposes no other difference (measured — ``PRODUCT``,
``VERSION``, ``VERSION_FULL`` and ``STATUS`` are the only columns
``PRODUCT_COMPONENT_VERSION`` has on either wired server; there is no
``PRODUCT_FULL``, ``VERSION_START``, ``VERSION_END``, ``BANNER`` or
``DESCRIPTION``, and ``V$INSTANCE``, which does have ``VERSION`` /
``VERSION_FULL`` / ``VERSION_LEGACY``, repeats exactly those numbers).

What is left when detection fails
---------------------------------
Nothing that looks like a version. The caller is told ``None`` and the backend's
own ``_version`` is cleared, which leaves the dialect unadapted — and an
unadapted dialect's ``.version`` raises ``DialectNotAdaptedException``, so a
gate cannot answer on a number nobody measured. That is the framework's existing
"no version" state, used here rather than a new sentinel.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Optional, Tuple


#: Ask for every row whose ``VERSION`` is a plain dotted number, carrying the
#: product name purely so it can be **logged** — not so it can be matched. The
#: trailing ``PRODUCT`` column is why :func:`parse_version_row` still reads
#: indices 0 and 1, and :func:`product_name_from_row` reads index 2.
#:
#: ``REGEXP_LIKE`` is here to reject component rows whose ``VERSION`` is not a
#: number (measured: every row on both wired servers passes it, and
#: ``VERSION IS NULL`` never occurs — ``COUNT(*) WHERE VERSION IS NULL`` is 0).
VERSION_FULL_QUERY = (
    "SELECT VERSION, VERSION_FULL, PRODUCT FROM PRODUCT_COMPONENT_VERSION "
    "WHERE REGEXP_LIKE(VERSION, '^[0-9]+(\\.[0-9]+)*$')"
)

#: The same query for a server whose ``PRODUCT_COMPONENT_VERSION`` predates
#: ``VERSION_FULL`` (the column arrived in 12.1), where naming it is
#: ``ORA-00904``. It returns fewer columns, so a row read from it has no product
#: name — which costs a log line, not the version.
VERSION_QUERY = (
    "SELECT VERSION FROM PRODUCT_COMPONENT_VERSION "
    "WHERE REGEXP_LIKE(VERSION, '^[0-9]+(\\.[0-9]+)*$')"
)

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


def _row_get(row: Any, key: str, index: int) -> Any:
    """One column of a ``PRODUCT_COMPONENT_VERSION`` row, by name or position.

    ``oracledb`` hands back a tuple in column order; the testsuite and the
    executors hand back a dict. Both are supported because both are what this
    function is handed in the two places it is called from.
    """
    if isinstance(row, Mapping):
        for candidate in (key, key.lower(), key.upper()):
            if candidate in row:
                return row[candidate]
        lowered = {str(k).lower(): v for k, v in row.items()}
        return lowered.get(key.lower())
    try:
        length = len(row)
    except TypeError:
        return None
    return row[index] if index < length else None


def product_name_from_row(row: Any) -> Optional[str]:
    """The ``PRODUCT`` string of a row, or ``None`` when the query did not ask
    for it (the :data:`VERSION_QUERY` fallback) or the server had none.

    Returned for **logging only**. Nothing in this backend branches on it, and
    that is the point: the string is the part Oracle renames.
    """
    if row is None:
        return None
    value = _row_get(row, "PRODUCT", 2)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _version_full_of(row: Any) -> Tuple[int, ...]:
    return parse_version(_row_get(row, "VERSION_FULL", 1))


def select_version_row(rows: Optional[Iterable[Any]]) -> Any:
    """Pick the database's own row out of everything ``PRODUCT_COMPONENT_VERSION``
    holds, or ``None`` when none of them qualifies.

    The rule, in order, and it is the whole of the rename fix:

    1. **the row must carry a parseable version.** A component row whose
       ``VERSION`` is prose is not the database and cannot be allowed to answer
       a version question;
    2. **a row carrying a ``VERSION_FULL`` beats one that does not.** This is
       what separates the database from a bundled component pinned to the same
       release: measured, the database row is the one holding the release update
       (``23.26.1.0.0``), and both wired servers have exactly one
       ``VERSION_FULL`` in the view;
    3. **otherwise the highest leading major wins**, so an edition listing a
       component at a different version cannot displace the database with a
       higher or lower one;
    4. **otherwise the server's own row order decides** — a tie the rule cannot
       break is resolved by the server rather than by a guess.

    ``rows`` is always a **sequence of rows**, never a bare row: an earlier
    revision sniffed for a single row, and a one-element ``fetchall()`` result
    read as one row whose first column was the row tuple — which parsed as
    ``(23, 23)`` from ``'23.0.0.0.0'``. The rule only has a sequence to apply.

    Pinned by ``test_oracle_version_detection.py`` against recorded row sets,
    including the multi-row sets no wired server here produces.
    """
    if rows is None:
        return None
    if isinstance(rows, (str, bytes)) or not hasattr(rows, "__iter__"):
        return None
    candidates = []
    for position, row in enumerate(rows):
        if row is None:
            continue
        parsed = parse_version(_row_get(row, "VERSION", 0))
        if not parsed:
            continue
        candidates.append((0 if _version_full_of(row) else 1, -parsed[0], position, row))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[:3])
    return candidates[0][3]


def parse_version_row(row: Any) -> Tuple[Tuple[int, ...], Optional[Tuple[int, ...]]]:
    """Parse a ``PRODUCT_COMPONENT_VERSION`` row into base and full versions."""
    if row is None:
        return (), None
    base = parse_version(_row_get(row, "VERSION", 0))
    full = parse_version(_row_get(row, "VERSION_FULL", 1))
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
    """Return the RU component from an Oracle VERSION_FULL value.

    Under the numbering scheme Oracle introduced with 26ai this is the *release
    update* component, and it is the number that separates a 23ai server from a
    26ai one: the 23ai release updates Oracle lists for the same line are
    ``23.4.0.25.04`` / ``23.8.0.25.04`` / ``23.9.0.25.07`` /
    ``23.10.0.25.10``, and 26ai begins at ``23.26`` — the second component
    *is* the year of the update, and the "26" in the product name is that same
    number. The measured 26ai server reports ``23.26.1.0.0``, so this returns
    ``26``.
    """
    parsed = parse_version(value)
    if len(parsed) < 2:
        return None
    return parsed[1]


__all__ = [
    "VERSION_FULL_QUERY",
    "VERSION_QUERY",
    "parse_version",
    "parse_version_row",
    "product_name_from_row",
    "select_version_row",
    "normalize_version",
    "ru_from_version_full",
]
