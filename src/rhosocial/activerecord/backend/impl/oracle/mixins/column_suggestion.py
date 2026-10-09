# src/rhosocial/activerecord/backend/impl/oracle/mixins/column_suggestion.py
"""Oracle's answer to "which column class does this Python type mean here".

The column half of the suggestion protocol, and deliberately separate from
:meth:`OracleTypeSuggestionMixin.suggest_column_type` next door. That one is the
DataType side -- *how Oracle stores it* (``VARCHAR2(255)``, ``RAW(16)``) -- while
this is the column side: *what the value can do*. Oracle keeps them apart
because the two are separate decisions and neither implies the other, and the
case that proves it on this backend is ``float``: the storage question has two
real answers here (``BINARY_FLOAT`` and ``BINARY_DOUBLE``, which
:class:`~...expression.types.OracleBinaryFloatType` and
:class:`~...expression.types.OracleBinaryDoubleType` now exist to spell), while
the operation question has one -- approximate arithmetic -- and
:class:`~...expression.column_types.FloatColumn` is already that answer. Putting
the two binary types into this table would say a ``float`` column on Oracle can
do something a ``float`` column cannot, which is false: they differ in width,
not in operations.

Two version boundaries carry the whole content of
:meth:`OracleColumnSuggestionMixin.supports_column_operation`, and both were
measured on three live servers (18.4 XE, 21.3 XE, 26ai Free 23.26.1) rather than
read off a release note; see
``..claude/plan/2026-10-08/secondary-gaps-investigation.md`` appendix F and
the framework's ``suggested-pairing-json.md`` / ``suggested-pairing-array.md``.

Nothing here answers ``UNSUPPORTED``
-------------------------------------
All eighteen entries of
:data:`~...expression.column_suggestions.COLUMN_TYPE_ENTRIES` get a column class
on every Oracle release this backend targets, including 18c. That is a measured
claim rather than an optimistic one: the release whose storage genuinely lacks
something is 18c's JSON -- there is no ``JSON`` type before 21c and the value
goes in a ``CLOB`` -- and the probe run against an 18c ``CLOB`` column passed
path extraction, ``JSON_EXISTS``, ``JSON_VALUE(col, '$.arr.size()')``,
``doc IS JSON`` and ``JSON_TABLE``, i.e. most of the battery. Refusing the entry
would refuse ``dict`` and ``list`` fields on every 18c database over a gap that
the value layer covers. What that substitute cannot do is *declared*, in the
same table rather than in a docstring nobody reads: see
:meth:`OracleColumnSuggestionMixin.supports_column_operation`.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import uuid
from typing import Any, Dict, Tuple, Type

from rhosocial.activerecord.backend.dialect.mixins import ColumnSuggestionMixin
from rhosocial.activerecord.backend.expression.column_types import (
    BinaryColumn,
    BooleanColumn,
    ColumnBase,
    DateTimeColumn,
    DecimalColumn,
    FloatColumn,
    IntegerColumn,
    JSONColumn,
    NumericColumn,
    StringColumn,
    UUIDColumn,
)

from .types import _ORACLE_BOOLEAN_TYPE_MIN_VERSION

#: The release that introduced Oracle's native SQL ``JSON`` **column type**.
#:
#: Measured on all three wired servers, creating and reading the catalog row
#: back: 18.4 XE rejects it (``ORA-00902: invalid datatype``), 21.3 XE accepts
#: it and reports ``DATA_TYPE='JSON'`` with ``DATA_LENGTH=8200``, and 26ai Free
#: 23.26.1 does the same. Two environment facts travel with that measurement and
#: are why the boundary is stated against the version number rather than the
#: product name: the server's own ``version_full`` for the 26ai release is
#: ``23.26.1.0.0`` while ``v$instance.version`` still reports ``23.0.0.0.0``,
#: and the DDL only succeeds in an **ASSM** tablespace -- ``CREATE TABLE t (v
#: JSON)`` under ``TABLESPACE USERS`` succeeds on 21c and 26ai while under the
#: default ``SYSTEM`` (which reports ``segment_space_management=MANUAL`` on all
#: three) it is ``ORA-43853``. Both are the DDL layer's business, not this
#: table's; this constant exists so the *operation* gate below has one place
#: that states the same boundary
#: (:meth:`OracleTypeSuggestionMixin.suggest_column_type` inlines ``(21, 0, 0)``
#: for the same reason it must ask for a known version first).
_ORACLE_JSON_TYPE_MIN_VERSION: Tuple[int, int, int] = (21, 0, 0)


class OracleColumnSuggestionMixin(ColumnSuggestionMixin):
    """Oracle's 18-entry column suggestion table, and its narrowed operations.

    The table itself does not vary by version, so it is a class attribute and
    :meth:`~...dialect.mixins.column_suggestion.ColumnSuggestionMixin.suggested_column_types`
    needs no override here. That is a finding rather than a shortcut: on Oracle
    the version-dependent part of this protocol is *not* which class a type means,
    it is which operations that class can carry -- a ``dict`` means
    :class:`~...expression.column_types.JSONColumn` on 18c and on 26ai, and the
    two differ in what may be asked of it. Putting the branch in the class table
    would make the answer depend on a version the caller has to supply before
    asking a question that does not have a version in it.
    """

    #: Oracle's full table: one answer per entry of
    #: :data:`~...expression.column_suggestions.COLUMN_TYPE_ENTRIES`, and every
    #: entry answered -- see this module's docstring for why Oracle declines to
    #: use :data:`~...expression.column_suggestions.UNSUPPORTED` anywhere.
    #:
    #: The entries that differ from core's neutral table, and why:
    #:
    #: ``float`` / ``decimal.Decimal``
    #:     :class:`~...expression.column_types.FloatColumn` /
    #:     :class:`~...expression.column_types.DecimalColumn`, where core's
    #:     neutral table still answers ``NumericColumn`` for both. Oracle makes
    #:     the split unavoidable from the storage side -- ``NUMBER`` is the only
    #:     exact type and the only approximate ones are the two IEEE words --
    #:     and core's note on the neutral table says the classes that tell them
    #:     apart "are declared but not yet wired here". Wiring them here is
    #:     exactly that, for this backend.
    #:
    #: ``list`` / ``tuple`` / ``set`` / ``frozenset``
    #:     :class:`~...expression.column_types.JSONColumn`, where core answers
    #:     ``ArrayColumn``. **Oracle has no array column type in any release.**
    #:     ``T[]`` is not in its DDL grammar at all; the only array-valued
    #:     storage it has is a ``VARRAY`` or a nested table, and both must be a
    #:     *named type* created first (``CREATE TYPE order_ids_t AS VARRAY(10)
    #:     OF NUMBER``) and then referred to by name -- so the substitute for an
    #:     array concept is the named-type door that
    #:     :meth:`~...types.OracleTypeSupportMixin.suggested_data_types` already
    #:     declares under ``"array"``, not a serialised list. This table answers
    #:     the other half of that same decision: the column a Python list means
    #:     is a JSON document, and the operations available on it are the JSON
    #:     ones, version-gated in
    #:     :meth:`supports_column_operation`. Answering ``ArrayColumn`` here would
    #:     offer ``array_length`` / ``unnest`` -- ``ArrayMixin`` -- on a server
    #:     with no array type at all.
    #:
    #: ``dict``
    #:     :class:`~...expression.column_types.JSONColumn` on every release, but
    #:     the *storage* behind it branches: the native ``JSON`` type from 21c
    #:     (:data:`_ORACLE_JSON_TYPE_MIN_VERSION`) and a ``CLOB`` below it. The
    #:     class does not change, the operations do.
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
    #:     deliberately **not** answered ``UNSUPPORTED``: a numeric answer is
    #:     what every backend gives today, and refusing it would break every
    #:     ``timedelta`` field on Oracle rather than describe the gap.
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
    #:     (:data:`_ORACLE_BOOLEAN_TYPE_MIN_VERSION`) and ``NUMBER(1)`` below it.
    #:     The class is the same either way; ``is_true`` / ``is_false`` are not,
    #:     and that is declared in :meth:`supports_column_operation`.
    COLUMN_TYPE_SUGGESTIONS: Dict[Any, Type[ColumnBase]] = {
        bool: BooleanColumn,
        int: IntegerColumn,
        float: FloatColumn,
        decimal.Decimal: DecimalColumn,
        str: StringColumn,
        bytes: BinaryColumn,
        bytearray: BinaryColumn,
        datetime.date: DateTimeColumn,
        datetime.time: DateTimeColumn,
        datetime.datetime: DateTimeColumn,
        datetime.timedelta: NumericColumn,
        uuid.UUID: UUIDColumn,
        dict: JSONColumn,
        list: JSONColumn,
        tuple: JSONColumn,
        set: JSONColumn,
        frozenset: JSONColumn,
        enum.Enum: StringColumn,
    }

    def supports_column_operation(self, column_name: str, op: str) -> bool:
        """What Oracle cannot do, per (column class, operation, version).

        Everything not named below is available, which is core's default and
        also the direction the burden of proof runs: a backend that over-declares
        hides an incompatibility, one that forgot to narrow refuses a query that
        would have worked. Every ``False`` below is a measurement.

        Three narrowings, on three different grounds:

        ``StringColumn.ilike``
            **No release.** Oracle has no ``ILIKE`` operator and does not parse
            one: measured on all three servers, 18.4 and 21c reject it with
            ``ORA-00920: invalid relational operator`` and 23c/26ai with
            ``ORA-03049: invalid SQL identifier`` -- the second because the
            parser takes ``ILIKE`` for an alias and then finds
            ``non-boolean type`` where the pattern should be. The portable
            spelling is ``UPPER(col) LIKE UPPER(:pat)`` (or the ``'i'`` argument
            of ``REGEXP_LIKE``, which *is* available on every release), and
            which of those a caller wants is a rendering decision that belongs
            to Phase 2b with the rest of
            ``suggested-mappings.md`` section 8.4. Declared narrowed, not
            implemented.

        ``JSONColumn`` whole-column equality and document concat
            **Below 21c only**, because the operations that make them are the
            ones that arrived with the ``JSON`` type:
            ``JSON_EQUAL`` (the semantic equality Oracle documents --
            a direct ``=`` on a JSON column is ``ORA-40796: invalid comparison
            ... JSON type`` on 21c and worse than an error on 26ai, where it
            runs and **matches nothing**, which is the dangerous kind) and
            ``JSON_TRANSFORM(doc, APPEND '$', ...)``. Measured on 18.4 against a
            real ``CLOB`` column: a direct document comparison is ``ORA-00932:
            inconsistent datatypes: got CLOB``, and ``JSON_TRANSFORM`` is not a
            function the 18c grammar has at all (``ORA-00907``).
            ``DBMS_LOB.COMPARE`` does compare two ``CLOB``s on 18c, so an
            emulation exists -- but it is a byte comparison of the serialized
            text, not JSON-semantic equality, and it is exactly the kind of
            silent-degradation answer the protocol forbids; so 18c is told the
            operation is unavailable rather than handed a weaker one.

            What 18c *keeps* is the rest of the battery, which is why the entry
            above is ``JSONColumn`` and not ``UNSUPPORTED``: ``JSON_VALUE``
            (scalar path), ``JSON_EXISTS`` (has-key),
            ``JSON_VALUE(doc, '$.arr.size()')`` (array length), ``doc IS JSON``
            (validity) and ``JSON_TABLE`` (unnest) were each measured working
            against a ``CLOB`` on 18.4. ``json_path`` / ``json_value`` /
            ``is_null`` / ``in_`` stay available at every version, which is what
            the default ``True`` says.

        ``BooleanColumn.is_true`` / ``is_false``
            **Below 23ai**, and this one is a rendering hazard rather than a
            missing operation, which is why it is declared *now* and repaired in
            Phase 2b.

            Oracle's SQL ``BOOLEAN`` **column type** arrived in 23ai: measured,
            ``CREATE TABLE t (a BOOLEAN)`` is ``ORA-00902: invalid datatype`` on
            18.4 and on 21.3, and on 26ai Free it succeeds with
            ``DATA_TYPE='BOOLEAN'``. Below that boundary the storage this
            backend writes is ``NUMBER(1)`` (see
            :meth:`~...types.OracleTypeSupportMixin.format_data_type_boolean`),
            and ``IS TRUE`` does not exist for it -- ``WHERE flag IS TRUE`` on a
            ``NUMBER(1)`` column is a syntax error, not a comparison. So the
            operation is unavailable *as the core class spells it*, and saying
            so is what keeps ``Model.c.flag.is_true()`` from rendering SQL that
            fails at the database with an error pointing at the column.

            The repair is a rendering decision deliberately left to Phase 2b:
            ``flag = 1`` is legal on a ``NUMBER(1)`` column, and it is also the
            spelling ``suggested-mappings.md`` section 7 forbids as a
            cross-backend default because PostgreSQL rejects ``boolean =
            integer`` outright. Choosing between ``= 1`` and a bound parameter
            is therefore an Oracle decision that must not leak into the
            portable rendering, and this method is where it will be recorded --
            declaring ``False`` now keeps the pairing check from claiming a
            seamless default that does not exist.

            An unadapted dialect (``OracleDialect()``, whose ``.version``
            raises) is answered on the **pre-23ai** side, which is the same
            branch :meth:`~...types.OracleTypeSupportMixin.format_data_type_boolean`
            takes when ``_version`` is ``None`` and the reason it gives: the
            emitted DDL stays legal on every release, so nothing reading it can
            be misled about whether the statement will run. Guessing the newer
            side for an unknown server would be the opposite and worse guess.

        Args:
            column_name: The column class name (``"JSONColumn"``).
            op: The public method that provides the operation, named after
                itself.

        Returns:
            False where Oracle narrows, True everywhere else -- which includes
            every operation on every class not mentioned above.
        """
        # `ilike` is the one narrowing with no version in it: Oracle has never
        # had the operator, on any release this backend targets or any release
        # at all that the probe covered.
        if op == "ilike":
            return False

        if op in ("is_true", "is_false"):
            return self._has_native_boolean_type()

        if column_name == "JSONColumn" and op in ("__eq__", "concat"):
            return self._has_native_json_type()

        return True

    def _has_native_boolean_type(self) -> bool:
        """Whether this server has Oracle's native SQL ``BOOLEAN`` column type.

        23ai and later. Below it -- and on an unadapted dialect, whose
        ``.version`` raises ``DialectNotAdaptedException`` -- the answer is
        ``False``, and the column this backend creates for a ``bool`` is
        ``NUMBER(1)``. An unset ``_version`` is read as "not 23ai" rather than
        as an error because that is the branch
        :meth:`~...types.OracleTypeSupportMixin.format_data_type_boolean` already
        takes for it, and :meth:`supports_column_operation` has to answer on an
        unadapted dialect too.
        """
        version = getattr(self, "_version", None)
        if version is None:
            return False
        return tuple(version) >= _ORACLE_BOOLEAN_TYPE_MIN_VERSION

    def _has_native_json_type(self) -> bool:
        """Whether this server has Oracle's native SQL ``JSON`` column type.

        21c and later (:data:`_ORACLE_JSON_TYPE_MIN_VERSION`). Below it the value
        goes in a ``CLOB``, which still answers most JSON operations and none of
        the two that need the type -- see
        :meth:`supports_column_operation`.

        An unset ``_version`` is read as "not 21c", for the same reason as in
        :meth:`_has_native_boolean_type`: it is the branch the DDL layer already
        takes, and :meth:`OracleTypeSuggestionMixin.suggest_column_type` refuses
        to guess upward for an unknown version when it has to choose between
        ``JsonType`` and ``TextType(spelling="clob")``.
        """
        version = getattr(self, "_version", None)
        if version is None:
            return False
        return tuple(version) >= _ORACLE_JSON_TYPE_MIN_VERSION
