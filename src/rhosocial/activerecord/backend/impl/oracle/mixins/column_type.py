# src/rhosocial/activerecord/backend/impl/oracle/mixins/column_type.py
"""Oracle's answer to "which column class does this Python type mean here".

The column half of the suggestion protocol, and deliberately separate from
:meth:`OracleTypeSuggestionMixin.suggest_column_type` next door. That one is the
DataType side -- *how Oracle stores it* (``VARCHAR2(255)``, ``RAW(16)``) -- while
this is the column-type side: *what the value can do*. Oracle keeps them apart
because the two are separate decisions and neither implies the other, and the
case that proves it on this backend is ``float``: the storage question has two
real answers here (``BINARY_FLOAT`` and ``BINARY_DOUBLE``, which
:class:`~...expression.types.OracleBinaryFloatType` and
:class:`~...expression.types.OracleBinaryDoubleType` exist to spell), while
the operation question has one -- arithmetic on a number -- and
:class:`~...expression.column_types.NumericColumn` is that answer. Putting
either binary type into this table would say a ``float`` column on Oracle can
do something a ``float`` column cannot, which is false: they differ in width,
not in operations.

Nothing here answers ``None``
-----------------------------
All eighteen of the common Python types get a column class on every Oracle
release this backend targets, including 18c. That is a measured claim rather
than an optimistic one: the release whose storage genuinely lacks something is
18c's JSON -- there is no ``JSON`` type before 21c and the value goes in a
``CLOB`` -- and the probe run against an 18c ``CLOB`` column passed path
extraction, ``JSON_EXISTS``, ``JSON_VALUE(col, '$.arr.size()')``, ``doc IS
JSON`` and ``JSON_TABLE``, i.e. most of the battery. Refusing the entry would
refuse ``dict`` and ``list`` fields on every 18c database over a gap that the
value layer covers. ``None`` is the protocol's last resort for a value family a
backend genuinely cannot express, and it is deliberately not used anywhere
here: every entry has a measured pairing. The measurements come from three live
servers (18.4 XE, 21.3 XE, 26ai Free 23.26.1) rather than from a release note;
see ``.claude/plan/2026-10-08/secondary-gaps-investigation.md`` appendix F and
the framework's ``suggested-pairing-json.md`` / ``suggested-pairing-array.md``.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import uuid
from typing import Any, Dict, Type

from rhosocial.activerecord.backend.dialect.mixins import ColumnTypeMixin
from rhosocial.activerecord.backend.expression.column_types import (
    BinaryColumn,
    BooleanColumn,
    ColumnBase,
    DateTimeColumn,
    IntegerColumn,
    JSONColumn,
    NumericColumn,
    StringColumn,
    UUIDColumn,
)

#: Oracle's full table: ``{common Python type: ColumnBase subclass}``.
#:
#: One answer per common Python type, and every one answered -- see this
#: module's docstring for why Oracle declines to answer ``None`` anywhere.
#:
#: The entries that differ from the portable baseline, and why:
#:
#: ``float`` / ``decimal.Decimal``
#:     :class:`~...expression.column_types.NumericColumn`, which is the class
#:     every numeric width answers. Oracle makes the split unavoidable from the
#:     storage side -- ``NUMBER`` is the only exact type and the only
#:     approximate ones are the two IEEE words -- but that is a width
#:     difference the DDL layer spells, not an operation difference the column
#:     layer carries (see the module docstring).
#:
#: ``list`` / ``tuple`` / ``set`` / ``frozenset``
#:     :class:`~...expression.column_types.JSONColumn`. **Oracle has no array
#:     column type in any release.** ``T[]`` is not in its DDL grammar at all;
#:     the only array-valued storage it has is a ``VARRAY`` or a nested table,
#:     and both must be a *named type* created first (``CREATE TYPE order_ids_t
#:     AS VARRAY(10) OF NUMBER``) and then referred to by name -- so the
#:     substitute for an array concept is the named-type door that
#:     :meth:`~...types.OracleTypeSupportMixin.suggested_data_types` already
#:     declares under ``"array"``, not a serialised list. This table answers
#:     the other half of that same decision: the column a Python list means is
#:     a JSON document, and the operations available on it are the JSON ones.
#:     Answering ``ArrayColumn`` here would offer ``array_length`` /
#:     ``unnest`` -- ``ArrayMixin`` -- on a server with no array type at all.
#:
#: ``dict``
#:     :class:`~...expression.column_types.JSONColumn` on every release, but
#:     the *storage* behind it branches: the native ``JSON`` type from 21c
#:     (``_ORACLE_JSON_TYPE_MIN_VERSION`` in :mod:`.types`) and a ``CLOB``
#:     below it. The class does not change, the storage does.
#:
#: ``datetime.timedelta``
#:     :class:`~...expression.column_types.NumericColumn` -- **core has no
#:     interval column class**, and this entry is limited by that rather than
#:     by Oracle. Oracle has had ``INTERVAL DAY TO SECOND`` natively since 8i
#:     and this backend renders it at the DDL layer without complaint
#:     (:meth:`~...types.OracleTypeSupportMixin.format_data_type_interval`);
#:     what is missing is the column class that would carry the interval
#:     operations. So this is a core gap recorded in a backend table, and it
#:     is listed in appendix D of the investigation as one of the classes
#:     this backend has storage for and core has no wrapper for. It is
#:     deliberately **not** answered ``None``: a numeric answer is what every
#:     backend gives today, and refusing it would break every ``timedelta``
#:     field on Oracle rather than describe the gap.
#:
#: ``datetime.date`` / ``datetime.time``
#:     :class:`~...expression.column_types.DateTimeColumn`, and this one is
#:     Oracle-specific rather than a deviation from the baseline: **Oracle's
#:     ``DATE`` carries a time component.** A ``date`` mapped here is stored
#:     as ``DATE`` or ``TIMESTAMP`` and reads back as a ``datetime``, so the
#:     answer is the datetime family even though the annotation is not one.
#:     Appendix C flags the corollary -- when a pure-date column class lands,
#:     this entry has to be re-decided against that semantics rather than
#:     copied from a backend whose ``DATE`` is a date.
#:
#: ``uuid.UUID``
#:     :class:`~...expression.column_types.UUIDColumn`, carrying no UUID
#:     operator, because portable SQL has none. Oracle has no ``UUID``
#:     **type** in any release either -- 23ai added the ``UUID()`` *function*
#:     returning ``RAW(16)`` -- so the ``RAW(16)`` storage is the DDL layer's
#:     answer
#:     (:meth:`~...types.OracleTypeSupportMixin.suggested_data_types`,
#:     ``"uuid"``), and the column class says only that the value is a
#:     comparable one.
#:
#: ``bool``
#:     :class:`~...expression.column_types.BooleanColumn` on every release,
#:     with a storage that branches hard: ``BOOLEAN`` is native from 23ai
#:     (``_ORACLE_BOOLEAN_TYPE_MIN_VERSION`` in :mod:`.types`) and
#:     ``NUMBER(1)`` below it. The class is the same either way.
ORACLE_COLUMN_TYPES: Dict[Any, Type[ColumnBase]] = {
    bool: BooleanColumn,
    int: IntegerColumn,
    float: NumericColumn,
    decimal.Decimal: NumericColumn,
    str: StringColumn,
    bytes: BinaryColumn,
    bytearray: BinaryColumn,
    datetime.date: DateTimeColumn,
    datetime.time: DateTimeColumn,
    datetime.datetime: DateTimeColumn,
    datetime.timedelta: NumericColumn,
    uuid.UUID: UUIDColumn,
    dict: JSONColumn,
    # Oracle has no array column type in any release, so the four sequence
    # entries take the document column class rather than `ArrayColumn`.
    list: JSONColumn,
    tuple: JSONColumn,
    set: JSONColumn,
    frozenset: JSONColumn,
    enum.Enum: StringColumn,
}


class OracleColumnTypeMixin(ColumnTypeMixin):
    """Oracle's 18-entry column-type table.

    The table does not vary by version, which is a finding rather than a
    shortcut: on Oracle the version-dependent part of this protocol is *not*
    which class a type means, it is which storage and operations behind it --
    a ``dict`` means :class:`~...expression.column_types.JSONColumn` on 18c and
    on 26ai, and the two differ in what the DDL layer writes. Putting the
    branch in the class table would make the answer depend on a version the
    caller has to supply before asking a question that does not have a version
    in it.
    """

    def suggested_column_types(self) -> Dict[Any, Type[ColumnBase]]:
        """Oracle's answer for each of the common Python types.

        Every entry is answered with a :class:`ColumnBase` subclass; no entry
        is answered ``None``. See :data:`ORACLE_COLUMN_TYPES` for the reasons.

        Returns:
            A fresh copy of the table, so a caller mutating the answer cannot
            reach the table every later resolution reads.
        """
        return dict(ORACLE_COLUMN_TYPES)


__all__ = ["ORACLE_COLUMN_TYPES", "OracleColumnTypeMixin"]
