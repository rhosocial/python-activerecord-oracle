# src/rhosocial/activerecord/backend/impl/oracle/mixins/typed_value.py
"""Oracle's spellings for the value types whose literal form is its own.

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
  storage convention is a type mapping, not a literal, so no hex re-spelling
  happens here.
* **INTERVAL** -- ``INTERVAL '1 02:03:04' DAY TO SECOND`` round-trips a
  ``timedelta`` exactly, fractions included (measured down to the
  microsecond). The literal's default leading precision is two digits:
  ``INTERVAL '150 ...' DAY TO SECOND`` is ``ORA-01873`` (measured), so the
  rendering sizes ``DAY(p)`` to the day count and always spells six
  fractional digits with ``TO SECOND(6)`` -- the uniform shape measured
  end to end. The sign is the literal's leading character and applies to
  the whole value: ``timedelta(seconds=-1)`` is
  ``INTERVAL '-0 00:00:01.000000' DAY(1) TO SECOND(6)`` and reads back as
  exactly minus one second (measured). A year and a month have no fixed
  length, so a calendar span cannot be derived from a ``timedelta`` at all;
  the ``YEAR TO MONTH`` family is out of this node's reach by construction,
  not by omission.
* **DATE** -- the tier-1 quoted ISO form is session-format dependent and the
  measured session refuses it (``DATE DEFAULT '2026-10-18'`` fails under
  ``NLS_DATE_FORMAT=DD-MON-RR``); ``TO_DATE('2026-10-18','YYYY-MM-DD')`` is
  accepted and reads back 2026-10-18 00:00 (measured). Overridden to the
  parse call, which is the spelling core's typed_value.py names for Oracle.
* **TIMESTAMP** -- same session dependence; ``TO_TIMESTAMP('2026-10-18
  12:34:56','YYYY-MM-DD HH24:MI:SS')`` and its ``.FF6`` fractional form
  read back exact (measured). An aware value spells ``TO_TIMESTAMP_TZ``
  with a ``TZH:TZM`` offset (``+08:00`` measured). Overridden.
* **time of day** -- Oracle has no time-of-day literal.
  ``TO_DATE('14:30:00','HH24:MI:SS')`` is accepted but invents a date the
  caller never supplied: the measured read-back is the first day of the
  current month (2026-10-01 on 2026-10-18). The DDL position is refused
  rather than inventing the date; a data position binds the host value.
* **BOOLEAN** -- ``TRUE`` / ``FALSE`` are SQL keywords from 23ai only.
  Measured on 23.26.1: ``NUMBER(1) DEFAULT TRUE`` reads back ``1``,
  ``DEFAULT FALSE`` reads back ``0``, and a ``BOOLEAN DEFAULT TRUE`` column
  round-trips a real boolean. Below 23ai the words are not SQL (the
  ``BOOLEAN`` column type is ``ORA-00902`` on 21c, see mixins/types.py), so
  the DDL position is refused through :meth:`supports_boolean_type` rather
  than emitting a keyword the parser rejects.
* **NULL** / numbers / strings agree with the base, and Oracle's text
  escaping is the base's quote-doubling (Oracle SQL text literals have no
  backslash escapes), so no method is overridden for them.
"""

import datetime as _dt
from typing import TYPE_CHECKING, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.values import TypedValue


class OracleTypedValueMixin:
    """Oracle's tier-1 disagreements and tier-2 value literals.

    Stands alone rather than inheriting :class:`TypedValueMixin`, like every
    other Oracle mixin: the dialect composes this class ahead of the generic
    one, and the overrides win by that position. Inheriting would put the
    base ahead of the other generic mixins and make the ordering the
    dialect has to reason about depend on this class instead.
    """

    def format_boolean_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """``TRUE`` / ``FALSE`` -- SQL keywords from 23ai, refused before it.

        Measured on 23.26.1: ``DEFAULT TRUE`` on ``NUMBER(1)`` reads back
        ``1``, ``DEFAULT FALSE`` reads back ``0``, and a ``BOOLEAN`` column
        round-trips the boolean itself. Below 23ai the words are not SQL, so
        the DDL position refuses through the version gate instead of
        emitting a keyword the parser rejects.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        if not self.supports_boolean_type():
            keyword = "TRUE" if expr.value else "FALSE"
            raise UnsupportedFeatureError(
                self.name,
                f"{keyword} literal in a DDL position",
                suggestion=(
                    f"Oracle's SQL has no {keyword} literal before 23ai; bind "
                    "the boolean in a data position, or spell the pre-23ai "
                    "NUMBER(1) convention (1/0) with an IntegerValue."
                ),
            )
        return self.format_literal(expr.value), ()

    def format_date_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """``TO_DATE('2026-10-18','YYYY-MM-DD')``.

        The quoted ISO form is session-format dependent: the measured
        session (``NLS_DATE_FORMAT=DD-MON-RR``) refuses
        ``DEFAULT '2026-10-18'`` on a DATE column, while the parse call
        with an explicit format is accepted and reads back the exact date.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        quoted = self.format_literal(expr.value.isoformat())
        return f"TO_DATE({quoted}, 'YYYY-MM-DD')", ()

    def format_time_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """Refused in a DDL position: Oracle has no time-of-day literal.

        ``TO_DATE('14:30:00','HH24:MI:SS')`` is accepted but invents a date
        the caller never supplied -- the measured read-back is the first
        day of the current month. A data position binds the host value and
        works; only the inline spelling is refused.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        raise UnsupportedFeatureError(
            self.name,
            f"time-of-day literal ({expr.value.isoformat()}) in a DDL position",
            suggestion=(
                "Oracle has no time-of-day literal: TO_DATE with only time "
                "fields invents the first day of the current month, a date "
                "the caller never supplied (measured on 23.26.1). Bind the "
                "value in a data position, or carry the full moment with a "
                "TimestampValue."
            ),
        )

    def format_timestamp_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """``TO_TIMESTAMP(...)`` / ``TO_TIMESTAMP_TZ(...)`` with explicit masks.

        The quoted ISO form is session-format dependent (the measured
        ``NLS_TIMESTAMP_FORMAT`` refuses it), so the parse call carries its
        own mask: ``YYYY-MM-DD HH24:MI:SS`` plus ``.FF6`` when the value
        holds microseconds, and ``TZH:TZM`` with ``TO_TIMESTAMP_TZ`` for an
        aware value (offset measured as ``+08:00``). Offsets are minute
        granular -- ``TZH:TZM`` cannot spell a second-granularity offset, so
        one is refused rather than truncated.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        value = expr.value
        mask = "YYYY-MM-DD HH24:MI:SS"
        if value.microsecond:
            mask += ".FF6"
        offset = value.utcoffset() if value.tzinfo is not None else None
        if offset is None:
            body = value.isoformat(sep=" ")
            return f"TO_TIMESTAMP({self.format_literal(body)}, '{mask}')", ()
        total_seconds = int(offset.total_seconds())
        if total_seconds % 60:
            raise ValueError(
                f"TimestampValue offset must be minute-granular for "
                f"TZH:TZM; {offset!r} carries seconds."
            )
        sign = "-" if total_seconds < 0 else "+"
        total_seconds = abs(total_seconds)
        offset_text = f"{sign}{total_seconds // 3600:02d}:{total_seconds % 3600 // 60:02d}"
        body = value.replace(tzinfo=None).isoformat(sep=" ")
        quoted = self.format_literal(f"{body} {offset_text}")
        return f"TO_TIMESTAMP_TZ({quoted}, '{mask} TZH:TZM')", ()

    def format_binary_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """``HEXTORAW('0102')`` -- RAW has no literal form of its own.

        Measured on 23.26.1: a ``RAW(4) DEFAULT HEXTORAW('0102')`` fills the
        column with ``b'\\x01\\x02'`` on insert. An empty value is
        ``HEXTORAW('')``, which the server accepts but reads back as NULL --
        Oracle cannot hold a zero-length RAW.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        return f"HEXTORAW('{bytes(expr.value).hex().upper()}')", ()

    def format_uuid_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """A UUID as the bare quoted canonical text.

        Oracle has no UUID type; a ``VARCHAR2(36) DEFAULT '<canonical>'``
        reads the text back verbatim (measured). The ``RAW(16)`` storage
        convention belongs to a type mapping, not to the literal.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        return self.format_literal(str(expr.value)), ()

    def format_interval_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """``INTERVAL '<d> HH:MM:SS.ffffff' DAY(p) TO SECOND(6)``.

        The sign is the literal's leading character and applies to the whole
        value (``timedelta(seconds=-1)`` is ``'-0 00:00:01.000000'`` and
        reads back exact -- measured), so a negative span is spelled from
        its absolute components. The leading precision is sized to the day
        count because the literal's default is two digits and three-digit
        days are ``ORA-01873`` (measured). Years and months have no fixed
        length and cannot be derived from a ``timedelta`` at all, so the
        ``YEAR TO MONTH`` family is out of this node's reach by construction.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        span = expr.value
        if not isinstance(span, _dt.timedelta):
            raise TypeError(
                f"IntervalValue takes a timedelta, got {type(span).__name__}"
            )
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
        return f"INTERVAL '{sign}{text}' DAY({precision}) TO SECOND(6)", ()
