# src/rhosocial/activerecord/backend/impl/oracle/mixins/literal.py
"""Oracle's own branches of :meth:`format_literal`.

``SQLDialectBase.format_literal`` is the single entry point for writing a
value as SQL text, and it resolves in three tiers: the portable scalars the
base knows, the types a backend spells for itself, and the
literal-construction protocol. This module is Oracle's second tier plus the
tier-1 branches Oracle's grammar refuses or re-spells.

Every literal form below was measured on the live server behind scenario
``oracle_23c`` (127.0.0.1:11523, ``v$version`` answers "Oracle AI Database
26ai Free Release 23.26.1.0.0"; session ``NLS_DATE_FORMAT=DD-MON-RR``,
``NLS_TIMESTAMP_FORMAT=DD-MON-RR HH.MI.SSXFF AM``, ``NLS_LANGUAGE=AMERICAN``):

* **binary** -- RAW has no literal form. ``RAW(4) DEFAULT HEXTORAW('0102')``
  fills the column with ``b'\\x01\\x02'`` on insert (measured), so
  ``HEXTORAW`` is the spelling. An empty value reads back ``None``:
  ``HEXTORAW('')`` is accepted but Oracle cannot hold a zero-length RAW --
  it becomes NULL, the same fact as zero-length VARCHAR being NULL.
* **UUID** -- Oracle has no UUID type; the bare quoted canonical text in a
  ``VARCHAR2(36) DEFAULT`` reads back verbatim (measured). A ``RAW(16)``
  storage convention is a type mapping, not a literal, so no hex
  re-spelling happens here.
* **timedelta** -- ``INTERVAL '1 02:03:04.123456' DAY(1) TO SECOND(6)``
  round-trips exactly (measured). The literal's default leading precision is
  two digits -- ``INTERVAL '150 ...' DAY TO SECOND`` is ``ORA-01873`` --
  so the rendering sizes ``DAY(p)`` to the day count. The sign is the
  literal's leading character and applies to the whole value:
  ``timedelta(seconds=-1)`` is ``'-0 00:00:01.000000' DAY(1) TO SECOND(6)``
  and reads back as exactly minus one second. A year and a month have no
  fixed length, so a calendar span cannot be derived from a ``timedelta``
  at all; the ``YEAR TO MONTH`` family is out of reach by construction.
* **datetime** -- the base's quoted ISO form is session-format dependent and
  the measured session refuses it, so the parse call carries its own mask:
  ``TO_TIMESTAMP('2026-10-18 12:34:56', 'YYYY-MM-DD HH24:MI:SS')`` plus
  ``.FF6`` when the value holds microseconds; an aware value spells
  ``TO_TIMESTAMP_TZ`` with a ``TZH:TZM`` offset (``+08:00`` measured).
  Second-granular offsets are refused rather than truncated -- ``TZH:TZM``
  cannot spell them.
* **date** -- same session dependence;
  ``TO_DATE('2026-10-18','YYYY-MM-DD')`` is accepted and reads back the
  exact date (measured), which is the spelling core's documentation names
  for Oracle.
* **time of day** -- Oracle has no time-of-day literal.
  ``TO_DATE('14:30:00','HH24:MI:SS')`` is accepted but invents a date the
  caller never supplied: the measured read-back is the first day of the
  current month (2026-10-01 on 2026-10-18). Inline text is refused rather
  than inventing the date; a data position binds the host value and works.
* **bool** -- ``TRUE`` / ``FALSE`` are SQL keywords from 23ai only.
  Measured on 23.26.1: ``NUMBER(1) DEFAULT TRUE`` reads back ``1``,
  ``DEFAULT FALSE`` reads back ``0``, and a ``BOOLEAN DEFAULT TRUE`` column
  round-trips a real boolean. Below 23ai the words are not SQL (the
  ``BOOLEAN`` column type is ``ORA-00902`` on 21c, see mixins/types.py), so
  the inline form is refused through :meth:`supports_boolean_type` rather
  than emitting a keyword the parser rejects.

NULL, numbers and strings keep the base's spellings -- Oracle's text
escaping is the base's quote-doubling, because Oracle SQL text literals have
no backslash escapes.
"""

import datetime as _dt
import uuid as _uuid
from typing import Any

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class OracleLiteralValueMixin:
    """Oracle's branches of :meth:`format_literal`.

    Stands ahead of :class:`SQLDialectBase` in the dialect's base list,
    because it overrides a method the base defines and a mixin placed after
    the class it overrides never wins. Every type Oracle spells differently
    is branched on here; everything else falls through to the base
    implementation, which is where the portable forms and the
    literal-construction protocol live.
    """

    def format_literal(self, value: Any) -> str:
        """Render *value* as inline SQL text, Oracle's spelling where it has one.

        The dispatch order is load-bearing in two places, both because a
        Python type is a subclass of another: ``bool`` is an ``int``, so the
        boolean gate runs before anything numeric could claim it, and
        ``datetime`` is a ``date``, so the timestamp branch runs first and
        the date branch never sees a value carrying a time part.

        Args:
            value: The host value to write into the statement.

        Returns:
            The escaped literal text, without a bind parameter.

        Raises:
            UnsupportedFeatureError: A boolean below 23ai, where ``TRUE`` /
                ``FALSE`` are not SQL keywords; or a time of day, which has
                no Oracle literal at all.
            ValueError: A timestamp whose UTC offset carries seconds, which
                ``TZH:TZM`` cannot spell.
        """
        if isinstance(value, bool):
            if not self.supports_boolean_type():
                keyword = "TRUE" if value else "FALSE"
                raise UnsupportedFeatureError(
                    self.name,
                    f"{keyword} literal in a DDL position",
                    suggestion=(
                        f"Oracle's SQL has no {keyword} literal before 23ai; bind "
                        "the boolean in a data position, or spell the pre-23ai "
                        "NUMBER(1) convention (1/0) with an Integer value."
                    ),
                )
            return super().format_literal(value)
        if isinstance(value, (bytes, bytearray)):
            return f"HEXTORAW('{bytes(value).hex().upper()}')"
        if isinstance(value, _uuid.UUID):
            return self.format_literal(str(value))
        if isinstance(value, _dt.timedelta):
            return self._format_oracle_interval(value)
        if isinstance(value, _dt.datetime):
            return self._format_oracle_timestamp(value)
        if isinstance(value, _dt.date):
            return f"TO_DATE({self.format_literal(value.isoformat())}, 'YYYY-MM-DD')"
        if isinstance(value, _dt.time):
            raise UnsupportedFeatureError(
                self.name,
                f"time-of-day literal ({value.isoformat()}) in a DDL position",
                suggestion=(
                    "Oracle has no time-of-day literal: TO_DATE with only time "
                    "fields invents the first day of the current month, a date "
                    "the caller never supplied (measured on 23.26.1). Bind the "
                    "value in a data position, or carry the full moment as a "
                    "datetime."
                ),
            )
        return super().format_literal(value)

    def _format_oracle_interval(self, span: _dt.timedelta) -> str:
        """``INTERVAL '<d> HH:MM:SS.ffffff' DAY(p) TO SECOND(6)``.

        The sign is the literal's leading character and applies to the whole
        value, so a negative span is spelled from its absolute components --
        a ``timedelta`` normalizes ``-1 s`` to ``days=-1, seconds=86399``,
        and spelling those with a leading minus would mean
        ``-(1 day 23:59:59)``, a different value. The leading precision is
        sized to the day count because the literal's default is two digits
        and three-digit days are ``ORA-01873`` (measured).
        """
        sign = ""
        if span < _dt.timedelta(0):
            sign = "-"
            span = -span
        days = span.days
        seconds = span.seconds
        micros = span.microseconds
        text = (
            f"{days} {seconds // 3600:02d}:{seconds % 3600 // 60:02d}:"
            f"{seconds % 60:02d}.{micros:06d}"
        )
        precision = max(1, len(str(days)))
        return f"INTERVAL '{sign}{text}' DAY({precision}) TO SECOND(6)"

    def _format_oracle_timestamp(self, value: _dt.datetime) -> str:
        """``TO_TIMESTAMP(...)`` / ``TO_TIMESTAMP_TZ(...)`` with explicit masks.

        The quoted ISO form is session-format dependent (the measured
        ``NLS_TIMESTAMP_FORMAT`` refuses it), so the parse call carries its
        own mask: ``YYYY-MM-DD HH24:MI:SS`` plus ``.FF6`` when the value
        holds microseconds, and ``TZH:TZM`` with ``TO_TIMESTAMP_TZ`` for an
        aware value (offset measured as ``+08:00``).
        """
        mask = "YYYY-MM-DD HH24:MI:SS"
        if value.microsecond:
            mask += ".FF6"
        offset = value.utcoffset() if value.tzinfo is not None else None
        if offset is None:
            body = value.isoformat(sep=" ")
            return f"TO_TIMESTAMP({self.format_literal(body)}, '{mask}')"
        total_seconds = int(offset.total_seconds())
        if total_seconds % 60:
            raise ValueError(
                f"Timestamp offset must be minute-granular for TZH:TZM; "
                f"{offset!r} carries seconds."
            )
        sign = "-" if total_seconds < 0 else "+"
        total_seconds = abs(total_seconds)
        offset_text = (
            f"{sign}{total_seconds // 3600:02d}:{total_seconds % 3600 // 60:02d}"
        )
        body = value.replace(tzinfo=None).isoformat(sep=" ")
        quoted = self.format_literal(f"{body} {offset_text}")
        return f"TO_TIMESTAMP_TZ({quoted}, '{mask} TZH:TZM')"
