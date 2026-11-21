# src/rhosocial/activerecord/backend/impl/oracle/suggestion/data_type_suggestions.py
"""Oracle's **suggested data types**: what a bare field is declared as here.

The DDL half of the type answer, separate from :mod:`.column_type` for the reason
that file already states: a column class says what a value can *do*, this says
what a column *is*.

Four facts about this server shape the answers:

* **there is no ``BOOLEAN`` column type below 23ai.** ``BOOLEAN`` is native from
  23ai only; before it the value is carried by ``NUMBER(1)``. The suggestion is
  core ``BooleanType``, whose formatter owns that gate -- a table per version
  would duplicate the fact where the SQL is chosen.
* **``DATE`` carries a time component.** Oracle's ``DATE`` is a datetime with
  second resolution, so a ``datetime.date`` field is declared ``DATE`` and reads
  back as a ``datetime``. There is no separate pure-date column type.
* **no document, interval or spatial column through a bare annotation.** The
  native ``JSON`` word exists from 21c only and below that a document is a
  ``CLOB`` with ``IS JSON``; ``INTERVAL DAY TO SECOND`` and ``MDSYS.SDO_GEOMETRY``
  are native but carry a qualifier/complex constructor, and ``VECTOR`` needs a
  dimension and 23ai. A bare annotation says none of those, so the suggestion
  names the carrier this backend's adapters already write -- text -- and the
  field that wants the native column declares it with ``UseSqlType``.
* **a UUID is 16 raw bytes.** Oracle has no ``UUID`` data type in any release
  (23ai added the ``UUID()`` *function*, which returns a ``RAW(16)`` value), so
  ``OracleRawType()`` -- whose ``DEFAULT_LENGTH`` is 16 -- is what the
  backend's adapter writes and what a bare ``uuid.UUID`` gets.

No key answers ``None``. The twenty-four entries are ``FRAMEWORK_PYTHON_TYPES``,
and every one is answered: a suggestion is what a bare field gets, so taking
the table away from the caller is not an answer.

Each call returns a **fresh dict** whose values are this module's read-only
templates; the derivation binds a dialect on a copy of the single entry it uses.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import uuid
from typing import Any, Dict, Optional

from rhosocial.activerecord.backend.adapters import (
    BitString,
    Geometry,
    MacAddress,
    Multirange,
    Range,
    Vector,
)
from rhosocial.activerecord.backend.expression.types import (
    BlobType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntervalType,
    IntegerType,
    TextType,
    TimeType,
    XmlType,
    TimestampType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.oracle.expression.types import OracleRawType
from rhosocial.activerecord.backend.impl.oracle.type_values import (
    IntervalDayToSecond,
    IntervalYearToMonth,
    OracleRowID,
    OracleURowID,
    OracleVector,
    OracleXMLType,
    SDOGeometry,
)

#: Oracle's suggested declaration per framework-modelled Python type.
ORACLE_DATA_TYPE_SUGGESTIONS: Dict[Any, Optional[Any]] = {
    # ``NUMBER(1)`` below 23ai and native ``BOOLEAN`` from it; the formatter
    # owns the gate, so one entry covers every release.
    bool: BooleanType(),
    # Oracle has one integer type; 10 digits hold the 4-byte range.
    int: IntegerType(),
    # ``FLOAT(126)`` is Oracle's ``DOUBLE PRECISION``. ``BINARY_DOUBLE`` is the
    # native 8-byte IEEE value, and a field that wants those bytes declares it.
    float: DoubleType(),
    decimal.Decimal: DecimalType(),
    # A bare ``str`` is a bounded, comparable value in every other backend
    # (SQLite/PG/MySQL/MariaDB all suggest a TEXT/VARCHAR that works in a WHERE
    # ``=``, GROUP BY and a UNIQUE index). Oracle's unbounded character type is
    # CLOB, which cannot be a comparison key at all (ORA-22848) or an ordinary
    # B-tree index key. Use VARCHAR2(4000) -- Oracle's maximum in a standard
    # (non-EXTENDED) strings setup -- so plain string fields stay queryable and
    # indexable. A field that really needs unbounded text declares TextType()
    # explicitly and gets CLOB; this suggestion is only the bare-field default.
    str: VarCharType(length=4000),
    bytes: BlobType(),
    bytearray: BlobType(),
    # ``DATE`` includes a time of day; there is no pure-date column type.
    datetime.date: DateType(),
    # Oracle has no ``TIME`` type; the dialect writes the value as text.
    datetime.time: TimeType(),
    # Oracle's native timestamp resolution is microseconds (TIMESTAMP(6), the
    # default when no precision is written) and the dialect advertises
    # microsecond support, so a bare ``datetime`` must keep all six fractional
    # digits. TIMESTAMP(3) silently truncated writes and broke round-trips for
    # values carrying microseconds.
    datetime.datetime: TimestampType(precision=6),
    # ``INTERVAL DAY TO SECOND`` is native and renders here; the adapter writes
    # its text form, and this is the declaration a bare duration gets. (MySQL
    # and MariaDB have no interval type and name a text carrier instead.)
    datetime.timedelta: IntervalType(),
    # ``RAW(16)``: the RFC 4122 bytes, and ``OracleRawType``'s default width.
    uuid.UUID: OracleRawType(),
    # A document has no dimension-free column: ``JSON`` exists from 21c only
    # and below it the document is text with ``IS JSON``, so the bare answer is
    # the text carrier. Declare ``JsonType`` for the native column.
    dict: TextType(),
    list: TextType(),
    tuple: TextType(),
    set: TextType(),
    frozenset: TextType(),
    # ``VARCHAR2(255)`` plus the ``CHECK`` the type-level enum discipline uses;
    # Oracle has no ``ENUM`` type in any release.
    enum.Enum: VarCharType(length=255),
    # -- Complex families ------------------------------------------------
    # The neutral adapter encodes WKT/EWKT text by default and binds to
    # ``VARCHAR2``/``CLOB``; a native ``SDO_GEOMETRY`` column is declared.
    Geometry: TextType(),
    # A dimension is column policy; the neutral adapter's ``[1.0, 2.0]`` text
    # round-trips through a text column.
    Vector: TextType(),
    # A width is not in the annotation; the adapter's bit text keeps the
    # bit length the bytes would lose.
    BitString: TextType(),
    # The element type is column policy; the adapter writes JSON text.
    Range: TextType(),
    Multirange: TextType(),
    # The canonical 17-character form (six pairs plus five separators).
    MacAddress: VarCharType(length=17),
}


#: Declarations for Oracle's **own** value classes -- the types the
#: framework does not model. An identifier has no length of its own here, so
#: the text forms are the carriers; ``INTERVAL DAY TO SECOND`` and ``XMLTYPE``
#: are native and get their declaration.
ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS: Dict[Any, Optional[Any]] = {
    IntervalDayToSecond: IntervalType(),
    # ``INTERVAL YEAR TO MONTH`` has no core DataType; its literal is the
    # carrier this adapter writes.
    IntervalYearToMonth: TextType(),
    OracleRowID: VarCharType(length=18),
    OracleURowID: VarCharType(length=4000),
    OracleXMLType: XmlType(),
    # SDO_GEOMETRY and VECTOR need a column-level shape/dimension; the
    # adapters write text, which is the carrier a bare field round-trips.
    SDOGeometry: TextType(),
    OracleVector: TextType(),
}


class OracleDataTypeSuggestionsMixin:
    """Oracle's answer to "what declaration does a bare field get here"."""

    def suggested_data_types(self) -> Dict[Any, Optional[Any]]:
        table = dict(ORACLE_DATA_TYPE_SUGGESTIONS)
        return table

    def suggested_extra_data_types(self) -> Dict[Any, Optional[Any]]:
        table = dict(ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS)
        # `VECTOR` exists on 23ai and later only, and the adapter that carries
        # the value is registered on the same condition. An unadapted dialect,
        # or one on an older server, therefore has no adapter for it -- and a
        # suggestion without an adapter builds a column nothing can write.
        version = self.version_or_none
        if version is None or tuple(version) < (23, 0, 0):
            table.pop(OracleVector, None)
        return table


__all__ = [
    "ORACLE_DATA_TYPE_SUGGESTIONS",
    "ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS",
    "OracleDataTypeSuggestionsMixin",
]
