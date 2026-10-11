# src/rhosocial/activerecord/backend/impl/oracle/expression/types.py
"""Oracle-specific DDL DataType subclasses.

What earns an ``oracle_``-prefixed ``name``
-------------------------------------------
Every ``DataType`` subclass here sits under a **core concept** and is named
``oracle_<something>``. The prefix is not decoration: core enforces it, and it is
a claim. It says *this backend has a type of its own that the core hierarchy has
no name for*. A prefixed name that marks nothing is a false claim about what the
backend has, and it is the same class of defect as a type definition that
disagrees with the server.

Concretely, a class earns the prefix only when **its rendering is not what the
core concept would render**, which is the one situation in which a second class
is the only way to say what Oracle says:

=============================================== ============================== ======
class                                           Oracle type                    kept?
=============================================== ============================== ======
:class:`OracleNVarChar2Type`                    ``NVARCHAR2(n)``               yes
:class:`OracleNClobType`                        ``NCLOB``                      yes
:class:`OracleLongType`                          ``LONG``                      yes
:class:`OracleRawType`                          ``RAW(n)``                     yes
:class:`OracleLongRawType`                      ``LONG RAW``                   yes
:class:`OracleBinaryFloatType`                  ``BINARY_FLOAT``               yes
:class:`OracleBinaryDoubleType`                 ``BINARY_DOUBLE``              yes
:class:`OracleTimestampLtzType`                 ``TIMESTAMP(…) WITH LOCAL TIME ZONE`` yes
=============================================== ============================== ======

The first five are types Oracle has and the core concepts do not model under
*any* name: the national character set, the two deprecated ``LONG`` forms, and
Oracle's bounded byte string. Each renders a word no core formatter here can
produce.

The other three are the cases where the core concept exists but **Oracle's
word for the concept is not what the core formatter writes**, and the
difference is storage rather than spelling: ``BINARY_FLOAT``/``BINARY_DOUBLE``
are 4/8-byte IEEE 754 values, while ``format_data_type_float``/``_double`` write
``FLOAT(p)`` — "a subtype of the ``NUMBER`` data type"; and ``WITH LOCAL TIME
ZONE`` stores no offset while the core ``TimestampTzType`` renders ``WITH TIME
ZONE``. Collapsing either pair into one class made a declaration and the
catalog row it produced compare equal while naming a different Oracle column,
so the schema differ could not see the change and a write-back changed the
column. All three were measured on the three wired servers (18.4 XE, 21.3 XE,
26ai Free 23.26.1) and recorded as findings F.4-3 / F.4-4 in the appendix of
``.claude/plan/2026-10-08/secondary-gaps-investigation.md``.

Six other classes used to sit here and have been **deleted**, because each
rendered exactly what its own core parent renders — a second name for a concept
that already had one, with no Oracle type of its own behind it:

* ``OracleIntegerType`` / ``OracleSmallIntType`` / ``OracleBigIntType``. Oracle
  has **one** integer type. Its ANSI table gives ``{ INTEGER | INT | SMALLINT }``
  a single row, ``NUMBER(38)``, and ``BIGINT`` is not an ANSI type at all (the
  server rejects it, ``ORA-00902``). This backend writes ``NUMBER(10)`` /
  ``NUMBER(5)`` / ``NUMBER(19)`` — one concept, one width, one ``NUMBER`` — and
  core ``IntegerType`` / ``SmallIntType`` / ``BigIntType`` already render exactly
  that, so the prefixed key added a name and nothing else.
* ``OracleVarChar2Type`` / ``OracleCharType``. ``VARCHAR2`` and ``CHAR`` *are*
  Oracle types, but they are the types core's ``varchar`` and ``char`` already
  name, and ``format_data_type_varchar`` / ``format_data_type_char`` already
  write those two words. (The fixed-length/variable-length distinction survives
  the deletion intact, because it is ``CharType`` versus ``VarCharType`` —
  separate core concepts.)
* ``OracleBlobType``. ``BLOB`` is what ``format_data_type_blob`` writes.

Deleting them was not cosmetic. ``DataType.__eq__`` is
``type(self) is type(other)``, so a class whose only difference is its name makes
a declaration and the catalog row it produced compare **unequal** — measured on
this backend before the deletion:

===================== =========================== =================== ========
declared              what ``parse_type`` returned ``==``                SQL equal
===================== =========================== =================== ========
``IntegerType()``     ``OracleIntegerType()``      ``False``            ``True``
``SmallIntType()``    ``OracleSmallIntType()``     ``False``            ``True``
``BigIntType()``      ``OracleBigIntType()``       ``False``            ``True``
``VarCharType(10)``   ``OracleVarChar2Type(10)``   ``False``            ``True``
``CharType(10)``      ``OracleCharType(10)``       ``False``            ``True``
``BlobType()``        ``OracleBlobType()``         ``False``            ``True``
===================== =========================== =================== ========

Six columns that render identical SQL and are the same column, reported by the
schema differ as changed. ``parse_type`` now returns the core class, the
declaration and the introspection agree, and the round trip is byte-identical as
well as equal.

Usage scope
-----------
These types are used **only** for Oracle backend DDL column definitions,
introspection result parsing, and schema comparison.  They should **not**
be used by application code directly — always use the core types for
DDL definition expressions (``ColumnDefinition.data_type``).

Backend registration
--------------------
Every Oracle-specific ``DataType`` subclass carries an ``oracle_``-prefixed
``name`` attribute so that ``format_data_type()`` dispatches to the matching
``format_data_type_<name>`` formatter declared in ``OracleTypeSupportMixin``
(see ``mixins/types.py``).
"""

from __future__ import annotations

from typing import Optional

from rhosocial.activerecord.backend.expression.types import (
    BlobType,
    DoubleType,
    FloatType,
    TextType,
    TimestampTzType,
    VarBinaryType,
    VarCharType,
)


# ---------------------------------------------------------------------------
# Character string variants
# ---------------------------------------------------------------------------

class OracleNVarChar2Type(VarCharType):
    """Oracle ``NVARCHAR2(n)`` — variable-length string in the *national*
    character set.

    The concept is the same variable-length character data as
    :class:`~...expression.types.VarCharType`, and the only difference is the
    character set the characters are encoded in — which this framework
    deliberately treats as a **field**, not a type (the position
    ``MariaDBEnumType.charset`` already takes). So ``NVARCHAR2`` is a
    ``VarCharType`` and the national-charset difference is not expressed in
    the type hierarchy at all; promoting ``charset`` onto the core character
    types would be a change to core, decided separately. Same reasoning as
    :class:`OracleNClobType` in the large-object family.

    It keeps a prefixed ``name`` because it is not a spelling: ``format_data_type_varchar``
    writes ``VARCHAR2(n)``, and this is the only way to say the national
    character set.
    """

    name = "oracle_nvarchar2"


# ---------------------------------------------------------------------------
# Large object / long string variants
# ---------------------------------------------------------------------------

class OracleNClobType(TextType):
    """Oracle ``NCLOB`` — character large object in the *national* character
    set.

    The concept is unbounded character data (:class:`TextType`); the
    national character set is a field the framework does not model at the
    type layer — see :class:`OracleNVarChar2Type` for why.

    Prefixed because it is not a spelling: ``format_data_type_text`` writes
    ``CLOB``, which is the database character set, and this is the national one.
    """

    name = "oracle_nclob"


class OracleLongType(TextType):
    """Oracle ``LONG`` — the deprecated large-character type.

    Deprecated in favour of ``CLOB``, and not the same thing as one: the
    limit is 2GB rather than the CLOB's 4GB, ``LONG`` cannot be stored in
    a LOB locator so it cannot be read in pieces or written through the
    LOB API, and it cannot appear in most contexts a ``CLOB`` can. It is
    the same *concept* — unbounded character data — so it derives from
    :class:`TextType` and keeps its own name because the DDL says ``LONG``
    and a dump of an old schema will still contain it.

    Prefixed because it is not a spelling: ``format_data_type_text`` writes
    ``CLOB``, and Oracle says "Do not create tables with LONG columns. Use
    LOB columns (CLOB, NCLOB, BLOB) instead" — a column declared ``LONG``
    must be written back as ``LONG`` to stay that column.

    Plain ``CLOB`` is **not** modelled as a class here: it is a spelling of
    :class:`TextType` (``TextType(spelling="clob")``), since it is the
    standard's name for the same unbounded string rather than a variant of
    it.
    """

    name = "oracle_long"


# ---------------------------------------------------------------------------
# Binary variants
# ---------------------------------------------------------------------------

class OracleRawType(VarBinaryType):
    """Oracle ``RAW(n)`` — a byte string of at most ``n`` bytes.

    Oracle's byte-string type, and the concept core calls ``VarBinaryType``
    (a byte string with a declared maximum). Oracle says so in the same words
    the character type uses: "``RAW`` is a variable-length data type like
    ``VARCHAR2``", the difference being that Oracle Net and the import/export
    utilities do not character-convert a ``RAW``. So ``RAW(n)`` holds up to
    ``n`` bytes and stores **fewer** if that is what was written — it does not
    pad, which is measured rather than assumed: ``DUMP`` of a ``RAW(16)``
    holding eight bytes reports ``Typ=23 Len=8`` on 21c and 23c, not
    ``Len=16``.

    That is also why this derives from ``VarBinaryType`` and not from
    ``BlobType``: a ``BLOB`` is unbounded and lives outside the row behind a
    locator, while ``RAW(n)`` is bounded and stored in the row, so claiming
    they are one type would be false about storage.

    Prefixed because core ``varbinary`` is **substituted** on this backend
    rather than rendered (``suggested_data_types()`` names this class for it):
    there is no ``format_data_type_varbinary``, so this class is the only way
    to write a bounded byte string at all.

    ``n`` is in **bytes**, not characters: the built-in summary says ``RAW
    (size)`` is "raw binary data of length size bytes", and 16 is the width a
    UUID needs — ``UUID()`` "returns a version 4 variant 1 UUID as a
    ``RAW(16)`` value". The same row caps ``n`` at "32767 bytes if
    MAX_STRING_SIZE = EXTENDED" and "2000 bytes if MAX_STRING_SIZE =
    STANDARD"; beyond that Oracle wants a ``BLOB``, and this dialect does not
    police the cap here because it is a property of the instance's
    ``MAX_STRING_SIZE``, not of the type.
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/uuid.html

    Args:
        dialect: The dialect that renders the type. First positional
            parameter, as for every ``DataType`` — with ``length`` first, a
            caller following the documented convention would have their
            dialect silently stored as the byte count.
        length: Maximum number of bytes. **Defaults to 16** — Oracle's own
            ``UUID()`` width, and the width this backend uses for every UUID
            it stores (``OracleUUIDAdapter``'s bytes path,
            ``suggest_column_type(uuid.UUID)``). The default is that width and
            not the alternative reading of "unspecified", Oracle's 2000-byte
            ``MAX_STRING_SIZE = STANDARD`` ceiling: a substitute that renders
            a 2 KB column for a 128-bit identifier is a column that does not
            match what the caller declared, and this class is what
            ``suggested_data_types()`` points a caller at for ``uuid``.

            It is **not** a default for ``binary``, the other concept this
            class substitutes for: 16 is the width a UUID needs and nothing
            states it for a fixed-length byte string, so that substitution has
            to be read as ``OracleRawType(length=n)`` with the ``n`` the
            caller declared. ``varbinary`` is not this class at all — a
            variable-length byte string with no ceiling is core
            :class:`~...expression.types.BlobType` (``BLOB``). See
            ``mixins/types.py``'s ``suggested_data_types`` for the reasoning
            and the measurements.

            A byte string of any other width is spelled out —
            ``OracleRawType(length=n)`` — and the bound is genuinely part of
            the declaration, because ``length`` is in :attr:`PARAMETERS` and
            two ``RAW`` columns of different widths are different columns.
    """

    # ``RAW(n)`` is bounded and the bound is the declaration; two RAW columns
    #     of different widths are different columns.

    PARAMETERS = ("length",)

    name = "oracle_raw"

    #: Bytes written when the caller declares no width. See :attr:`length`.
    DEFAULT_LENGTH = 16

    def __init__(self, dialect=None, *, length: Optional[int] = None):
        super().__init__(dialect, self.DEFAULT_LENGTH if length is None else length)


class OracleLongRawType(BlobType):
    """Oracle ``LONG RAW`` — the deprecated large binary type.

    Unbounded binary data like ``BLOB``, and kept as the deprecated type it
    is: like ``LONG`` it has no LOB locator, its limit is 2GB, and it cannot
    appear where a ``BLOB`` can. It is still the binary *large object*
    concept — bytes with no declared maximum — so it derives from
    :class:`BlobType`.

    Prefixed because it is not a spelling: ``format_data_type_blob`` writes
    ``BLOB``, and Oracle tells you to convert ``LONG RAW`` *to* ``BLOB`` — so
    a column declared ``LONG RAW`` has to be written back as ``LONG RAW`` to
    stay that column.
    """

    name = "oracle_long_raw"


# ---------------------------------------------------------------------------
# Numeric variants
# ---------------------------------------------------------------------------

class OracleBinaryFloatType(FloatType):
    """Oracle ``BINARY_FLOAT`` — 32-bit IEEE 754 binary floating point.

    Oracle's two binary floating-point types are **not** the ``NUMBER``
    family. ``BINARY_FLOAT`` is a true 4-byte IEEE binary32 value (single
    precision, with ``Inf`` and ``NaN`` among its values), while ``FLOAT(p)``
    — what core :class:`FloatType` renders — is "a subtype of the ``NUMBER``
    data type", a decimal-value type whose digits happen to be counted in
    binary. The catalog reports the two words separately and the server
    stores different bytes for them: measured on all three wired servers
    (18.4 XE, 21.3 XE, 26ai Free 23.26.1), ``CREATE TABLE t (c
    BINARY_FLOAT)`` reports ``DATA_TYPE = 'BINARY_FLOAT'`` with
    ``DATA_LENGTH = 4``, while a ``FLOAT(63)`` column reports
    ``DATA_TYPE = 'FLOAT'`` with ``DATA_PRECISION = 63``.

    Core ``FloatType`` renders ``FLOAT(p)`` (126 by default), so before this
    class ``parse_type`` answered the catalog word with
    ``FloatType(precision=63)`` and re-rendered it as ``FLOAT(63)`` — a
    write-back named a different storage type, and object equality made the
    schema differ treat the 4-byte IEEE column and the NUMBER-based one as
    the same column. That is finding F.4-4 in the appendix of
    ``.claude/plan/2026-10-08/secondary-gaps-investigation.md``.

    The word is the whole declaration and takes no parameters, so
    :attr:`PARAMETERS` is empty: two ``BINARY_FLOAT`` columns are one type,
    the class is never equal to any ``FloatType``, and the 24-bit mantissa is
    a property of the type rather than a declarable precision.
    """

    name = "oracle_binary_float"

    PARAMETERS = ()

    def __init__(self, dialect=None):
        super().__init__(dialect)


class OracleBinaryDoubleType(DoubleType):
    """Oracle ``BINARY_DOUBLE`` — 64-bit IEEE 754 binary floating point.

    The 8-byte sibling of :class:`OracleBinaryFloatType`, and the same
    collapse was measured for it: the catalog reports ``DATA_TYPE =
    'BINARY_DOUBLE'`` with ``DATA_LENGTH = 8`` on all three wired servers,
    while a ``DOUBLE PRECISION`` column reports ``DATA_TYPE = 'FLOAT'`` with
    ``DATA_PRECISION = 126``. Before this class ``parse_type`` answered
    ``BINARY_DOUBLE`` with core :class:`DoubleType` and re-rendered
    ``FLOAT(126)``, so the differ could not distinguish the IEEE column from
    the NUMBER-based one and a write-back named the other type (finding
    F.4-4).

    No parameters either: :attr:`PARAMETERS` is empty, the class equals
    itself and never equals any ``DoubleType``, and the 53-bit mantissa is a
    property of the type rather than a declarable precision.
    """

    name = "oracle_binary_double"

    PARAMETERS = ()

    def __init__(self, dialect=None):
        super().__init__(dialect)


# ---------------------------------------------------------------------------
# Datetime variants
# ---------------------------------------------------------------------------

class OracleTimestampLtzType(TimestampTzType):
    """Oracle ``TIMESTAMP[(n)] WITH LOCAL TIME ZONE`` — the zone-less
    timestamp-with-time-zone column.

    Oracle has two timestamp-with-time-zone forms and they are **not
    interchangeable columns**: ``WITH TIME ZONE`` stores the offset with each
    value, while ``WITH LOCAL TIME ZONE`` stores none and normalises the value
    to the database time zone. The catalog keeps the whole word too — measured
    on all three wired servers (18.4 XE, 21.3 XE, 26ai Free 23.26.1),
    ``CREATE TABLE t (c TIMESTAMP WITH LOCAL TIME ZONE)`` reports
    ``DATA_TYPE = 'TIMESTAMP(6) WITH LOCAL TIME ZONE'`` with
    ``DATA_SCALE = 6``, while the non-local sibling reports
    ``'TIMESTAMP(6) WITH TIME ZONE'``.

    Core :class:`TimestampTzType` renders ``TIMESTAMP(n) WITH TIME ZONE``, so
    before this class ``parse_type`` answered the LOCAL word with the core one
    and re-rendered it **without** ``LOCAL``: a write-back silently changed the
    column's storage semantics, and object equality made the schema differ
    treat the two columns as one. That is finding F.4-3 of the plan appendix
    (``.claude/plan/2026-10-08/secondary-gaps-investigation.md``), and it is
    what earns this word its own class under the module's prefix rule.

    ``precision`` is the core sibling's field, carried unchanged: the class
    renders ``TIMESTAMP(n) WITH LOCAL TIME ZONE`` and the bare word when none
    is declared. The bare declaration versus the ``TIMESTAMP(6) …`` catalog
    row is left unequal on purpose — that is the server-default-precision
    question (finding F.4-5) tracked separately, not something this class
    resolves.
    """

    name = "oracle_timestamp_ltz"


__all__ = [
    "OracleNVarChar2Type",
    "OracleNClobType",
    "OracleLongType",
    "OracleRawType",
    "OracleLongRawType",
    "OracleBinaryFloatType",
    "OracleBinaryDoubleType",
    "OracleTimestampLtzType",
]