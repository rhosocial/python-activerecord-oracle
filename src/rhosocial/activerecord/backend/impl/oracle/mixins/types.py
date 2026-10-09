# src/rhosocial/activerecord/backend/impl/oracle/mixins/types.py
"""Oracle DataType formatting and parsing mixin.

Two halves of one contract, both keyed on ``DataType.name``:

* **rendering** — ``format_data_type_<name>`` for every concept this dialect
  writes, whether the name is a core one (``varchar``, ``text``,
  ``interval``, ``xml``, ``custom``) or an Oracle-namespaced one
  (``oracle_nvarchar2``, ``oracle_raw``, …). The second group is short on
  purpose: a prefixed key is a claim that Oracle has a type the core hierarchy
  has no name for, and it only holds when the formatter writes a word no core
  formatter here writes — see the class docstring;
* **stance** — ``supports_data_type_<name>`` for each of those, plus
  ``suggested_data_types()`` for the core concepts Oracle stores some other
  way. Nothing is left undeclared: a concept this dialect cannot spell still
  names its substitute, because "unsupported" on its own tells the caller
  nothing they can act on.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.dialect.mixins import (
    DDLTypeMixin,
)
from rhosocial.activerecord.backend.dialect.protocols import DDLTypeSupport
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    DateType,
    DateTimeType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntervalType,
    JsonType,
    JsonBType,
    RealType,
    SmallIntType,
    TextType,
    TimeType,
    TimeTzType,
    TimestampType,
    TimestampTzType,
    TinyIntType,
    VarCharType,
    XmlType,
    DataType,
    CustomType,
)
from ..expression.types import (
    OracleBinaryDoubleType,
    OracleBinaryFloatType,
    OracleLongRawType,
    OracleLongType,
    OracleNClobType,
    OracleNVarChar2Type,
    OracleRawType,
    OracleTimestampLtzType,
)


#: The release that introduced Oracle's native SQL ``BOOLEAN`` **column type** —
#: not the PL/SQL ``BOOLEAN`` that has always existed, but the one you can write
#: in ``CREATE TABLE``. Oracle states it for 23ai: "Oracle Database 23ai
#: introduces the new BOOLEAN data type", and "Some features (like SQL domains
#: or BOOLEAN) only work in Oracle Database 23ai". The 26ai references list it
#: among the built-in data types (code 252) and call it a new feature of 26ai,
#: because 26ai is the branding of the 23.0 line it arrived in; the server's own
#: ``PRODUCT_COMPONENT_VERSION`` reports ``23.0.0.0.0`` / ``VERSION_FULL``
#: ``23.26.1.0.0`` for that release, which is why the boundary is stated against
#: the version number rather than a product name. Measured: ``CREATE TABLE t (a
#: BOOLEAN)`` is ``ORA-00902`` on 21c and reports ``DATA_TYPE='BOOLEAN'`` on
#: 23ai.
#: https://docs.oracle.com/en/learn/db23ai-sql-features/index.html
_ORACLE_BOOLEAN_TYPE_MIN_VERSION: Tuple[int, int, int] = (23, 0, 0)

#: The release that introduced Oracle's native SQL ``JSON`` **column type** —
#: not the ``IS JSON`` constraint that has existed since 12.1, and not the
#: ``SYS.JSON`` functions, but a ``JSON`` keyword you can write in
#: ``CREATE TABLE``. Oracle states it for 21c: the SQL language reference lists
#: ``JSON`` among the data types from that release, and the JSON overview page
#: describes it as "a new built-in data type" replacing the older
#: ``CLOB``/``VARCHAR2`` + ``IS JSON`` idiom. Measured: ``CREATE TABLE t (a
#: JSON)`` is accepted on 21c and on 26ai, and reports
#: ``DATA_TYPE='JSON'`` back from the catalog, while 18c rejects the word.
#:
#: One caveat belongs with the boundary rather than in a bug report: on the
#: servers measured, a ``JSON`` column was only accepted in a **non-SYSTEM
#: tablespace**. The ``SYSTEM`` segment is not ASSM, and Oracle raises
#: ``ORA-43853`` there for the same statement that succeeds under
#: ``TABLESPACE USERS``. The gate therefore decides which *word* is written and
#: not which tablespace the caller is in — that is the model author's, and
#: this backend has no way to read it.
#: https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
#: https://docs.oracle.com/en/database/oracle/oracle-database/26/adjsn/overview-json-oracle-ai-database.html
_ORACLE_JSON_TYPE_MIN_VERSION: Tuple[int, int, int] = (21, 0, 0)

#: Oracle's two documented default precisions inside ``INTERVAL DAY TO SECOND``,
#: quoted from NLSPG 4.2.2.2: "``day_precision`` is the number of digits in the
#: ``DAY`` datetime field. Accepted values are 0 to 9. **The default is 2.**"
#: and "``fractional_seconds_precision`` is the number of digits in the
#: fractional part of the ``SECOND`` datetime field. Accepted values are 0 to 9.
#: **The default is 6.**"
#:
#: They live here rather than inside the SQL string because they are two
#: *different* facts about two *different* fields, and neither is a
#: ``type_parameter_defaults`` entry — see
#: :meth:`OracleTypeSupportMixin.type_parameter_defaults` and
#: :meth:`OracleTypeSupportMixin.format_data_type_interval` for why a whole
#: ``DAY(2) TO SECOND(6)`` qualifier is not a per-parameter declaration. Measured:
#: ``CREATE TABLE t (c INTERVAL DAY TO SECOND)`` reports ``DATA_PRECISION = 2``
#: and ``DATA_SCALE = 6`` on both 21c and 26ai.
#: https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html
_ORACLE_INTERVAL_DAY_PRECISION = 2
_ORACLE_INTERVAL_SECOND_PRECISION = 6


class OracleTypeSupportMixin(DDLTypeMixin, DDLTypeSupport):
    """Oracle data-type rendering and parsing.

    Two families of formatter live here, and they are the same family:
    ``format_data_type_<name>`` for a **core** concept (``varchar``,
    ``text``, ``interval``, …) and ``format_data_type_oracle_<name>`` for
    an Oracle-namespaced subclass of one. The naming convention is what
    makes them one family — ``DataType.name`` is the dispatch key — so a
    caller holding an ``OracleRawType`` and a caller holding a core
    ``VarBinaryType`` reach the same answer by different routes.

    What earns the ``oracle_`` half
    -------------------------------
    A prefixed dispatch key is a claim that this backend has a type the core
    hierarchy has no name for, and it only earns that claim when **its SQL is
    not the SQL the core concept already renders here**. Eight classes hold up
    on that test — ``NVARCHAR2``, ``NCLOB``, ``LONG``, ``RAW(n)``,
    ``LONG RAW``, ``BINARY_FLOAT``, ``BINARY_DOUBLE`` and ``TIMESTAMP WITH
    LOCAL TIME ZONE`` (the last three: the core siblings render ``FLOAT(p)``
    and ``WITH TIME ZONE``, different Oracle columns) — and they are exactly
    the eight left in ``expression/types.py``.

    Six classes used to be here and are gone, each of which rendered byte for
    byte what its core parent renders: ``OracleIntegerType`` /
    ``OracleSmallIntType`` / ``OracleBigIntType`` (Oracle has one integer type
    and this dialect already writes ``NUMBER(p)`` for the three core concepts),
    ``OracleVarChar2Type`` / ``OracleCharType`` (``format_data_type_varchar``
    writes ``VARCHAR2`` and ``format_data_type_char`` writes ``CHAR``) and
    ``OracleBlobType`` (``format_data_type_blob`` writes ``BLOB``). A second
    name for a concept that already has one is not a type, and because
    ``DataType.__eq__`` is ``type(self) is type(other)`` it was worse than
    useless: a declaration and the catalog row it produced compared **unequal**
    while rendering identical SQL, so the schema differ reported six kinds of
    change that do not exist. See ``expression/types.py``'s module docstring
    for the measurement and the list.

    The integer widths, and why they are not Oracle's own
    -----------------------------------------------------
    This dialect writes ``NUMBER(3)`` / ``NUMBER(5)`` / ``NUMBER(10)`` /
    ``NUMBER(19)`` for ``tinyint`` / ``smallint`` / ``integer`` / ``bigint``,
    and Oracle's ANSI conversion table says something else: it lists
    ``{ INTEGER | INT | SMALLINT }`` → ``NUMBER(38)`` — one row, one target,
    three spellings. The table is not being misread, and it is the reason
    the widths are what they are rather than an argument against them.

    **Oracle has exactly one integer type.** That single row is the whole
    content of the table for integers, and it says the three words are the
    *same* column. Measured on both wired servers,
    ``CREATE TABLE t (a SMALLINT, b INT, c INTEGER)`` produces three catalog
    rows identical in every respect:

    ==================================  ==========  ============  =========
    declared                            DATA_TYPE   DATA_PREC.    DATA_SCALE
    ==================================  ==========  ============  =========
    ``a SMALLINT`` / ``b INT`` /        ``NUMBER``  ``NULL``      ``0``
    ``c INTEGER``
    ==================================  ==========  ============  =========

    A separately declared ``NUMBER(38)`` reports ``DATA_PRECISION = 38``, so
    the table's ``NUMBER(38)`` is its name for the maximum-range ``NUMBER``,
    not a bound the catalog stores. There is no 2-byte integer and no 4-byte
    integer in Oracle: one symmetric ``NUMBER``, and no unsigned row to
    select.
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlqr/Data-Types.html

    **So the divergence is deliberate**, and the width is the smallest
    precision holding the whole range of the concept being written — the
    number of decimal digits in it, which ``NUMBER(p)`` reaches because it is
    symmetric:

    ===========  ================  ==========  =============================
    concept      signed range     written    digits of that range
    ===========  ================  ==========  =============================
    ``tinyint``  ±127             NUMBER(3)  3
    ``smallint`` ±32767           NUMBER(5)  5
    ``integer``  ±2147483647      NUMBER(10) 10
    ``bigint``   ±9223372036854…  NUMBER(19) 19
    ===========  ================  ==========  =============================

    Three reasons this is the honest answer and ``NUMBER(38)`` is not:

    * Following the table would render ``SmallIntType``, ``IntegerType`` and
      ``BigIntType`` as the *same* 38-digit column. A 2-byte smallint
      reported as a 38-digit number is a far larger false claim than a 2-byte
      smallint reported as the 5-digit number that holds its range.
    * It would erase the distinction the framework exists to model: a schema
      diff could no longer tell a smallint column from an integer one,
      because on the server they would be one column.
    * The reverse mapping is load-bearing. :meth:`parse_type` reads
      ``NUMBER(5)`` / ``NUMBER(10)`` / ``NUMBER(19)`` back as the three
      concepts that write them, which is what makes a declared column and an
      introspected column compare equal. Under ``NUMBER(38)`` all three
      concepts would parse back as ``DecimalType(precision=38)`` and no
      integer column would round-trip at all.

    ``BIGINT`` deserves a note of its own: it is **not** an ANSI type at all,
    so it is in neither the table nor Oracle's grammar, and the server
    rejects it outright (``CREATE TABLE t (a BIGINT)`` → ``ORA-00902: invalid
    datatype`` on 21c and 23c). The ``bigint`` *concept* is still rendered
    here — as ``NUMBER(19)``, the smallest precision holding its whole signed
    range — because refusing it would refuse the concept rather than inform the
    caller, but it is a core concept and it keeps the core ``bigint`` dispatch
    key: there is no Oracle ``BIGINT`` for a prefixed key to name.
    """

    # --- Core type formatters (render standard types to Oracle SQL) ---

    def suggested_data_types(self) -> Dict[str, type]:
        """Core concepts Oracle stores some other way.

        Oracle renders most core concepts natively — ``NUMBER`` for the
        numerics, ``CLOB`` for unbounded text, ``XMLTYPE`` for an XML
        document, ``INTERVAL DAY TO SECOND`` for a span. These are the ones
        whose *storage* Oracle genuinely does not have, so each names what
        it stores instead rather than leaving a caller with "unsupported":

        ``array``
            Oracle has **no** array column type: ``T[]`` is not in its DDL
            grammar at all. The only array-valued storage it offers is a
            ``VARRAY`` or a nested table, and both must be a *named type*
            created first (``CREATE TYPE order_ids_t AS VARRAY(10) OF
            NUMBER``) and then referred to by name in the column definition.
            A name is an identifier, which is exactly what
            :class:`CustomType` carries and what the grammar in
            ``expression/type_name.py`` already admits — and this backend
            has the ``CREATE TYPE`` half in
            ``OracleVarrayTypeDefinition`` / ``OracleNestedTableTypeDefinition``.
            So the substitute is the named-type door, not a serialised list.

        ``binary``
            Oracle has **no** fixed-length byte string at all. ``BINARY(n)``
            is not a word its grammar has — measured on 21c and 23c,
            ``CREATE TABLE t (a BINARY(20))`` is rejected
            (``ORA-03060: Data type BINARY is invalid`` on 23c,
            ``ORA-00907`` on 21c) — and the one bounded byte string it does
            have, ``RAW(n)``, is not fixed-length: Oracle's own reference says
            "``RAW`` is a variable-length data type like ``VARCHAR2``", and it
            does not pad (``DUMP`` of a ``RAW(16)`` holding eight bytes reports
            ``Len=8``, not 16). So ``RAW(n)`` is the *closest* column Oracle
            can offer a fixed-length concept, not the same column: it accepts
            every ``n``-byte value a fixed-length one would, and shorter ones
            too. That difference is a constraint on the column rather than a
            property of the type, so the caller enforces it.

            Which makes the width the caller's to state, and this entry is not
            a licence to leave it out:
            :attr:`OracleRawType.DEFAULT_LENGTH` is 16 because that is the
            width a **UUID** needs, and 16 is not a defensible default for an
            arbitrary fixed-length byte string. Read the suggestion as
            ``OracleRawType(length=n)`` with the ``n`` the caller declared.

        ``varbinary``
            Oracle's variable-length binary storage is ``BLOB`` — "a binary
            large object", up to ``(4 gigabytes - 1) * (database block size)``,
            stored outside the row behind a locator and read in pieces
            through the LOB API. That is the concept ``varbinary`` is: a byte
            string with no declared maximum. ``RAW(n)`` is *not* it, because
            ``n`` is a ceiling: rendering ``varbinary`` as ``RAW(16)`` claims a
            column that stops growing at sixteen bytes, which is precisely the
            kind of false identity claim this mapping exists to remove.

            So the two byte-string concepts land on two different Oracle types
            — ``binary`` on the bounded ``RAW(n)``, ``varbinary`` on the
            unbounded ``BLOB`` — and neither of them lands on a width it did
            not choose. ``BLOB`` needs no Oracle-namespaced class of its own:
            it is the word :meth:`format_data_type_blob` already writes for core
            ``BlobType``, and a substitute that rendered the same SQL under a
            second class name would make a declared ``blob`` and an introspected
            ``BLOB`` compare unequal for no reason.

            There is no third option. No ``VARCHAR2``-style type carries binary
            content, because a ``VARCHAR2`` is defined by the database
            character set and Oracle Net converts its contents: feeding a
            ``RAW`` value into a ``VARCHAR2(16)`` stores the four *characters*
            ``"00FF"`` (measured), which is the opposite of preserving bytes.
            Of the byte-string types in the built-in summary, ``RAW(n)`` and
            ``BLOB`` are the two that live inside the database; ``LONG RAW`` is
            the deprecated form Oracle tells you to convert to ``BLOB`` ("Do not
            create tables with LONG columns. Use LOB columns (CLOB, NCLOB,
            BLOB) instead"), and ``BFILE`` "stores a locator to a large binary
            file stored *outside* the database".
            https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html

        ``enum``
            Oracle has **no** ``ENUM`` type in any release, and the substitute
            is therefore not version-dependent. This entry used to read "Oracle
            has no enum type before 23c", which implies there is one after it;
            there is not. What 23ai added is ``BOOLEAN`` and ``VECTOR`` —
            Oracle's own announcement names exactly those two — and 21c added
            ``JSON``. A reader who trusted the old wording would have gone
            looking for an ``ENUM`` keyword in the 23ai grammar and found
            nothing, which is the kind of wild goose chase this docstring is
            supposed to prevent.

            So the value goes in a ``VARCHAR2`` constrained by a ``CHECK`` on
            every release, and VARCHAR is what the type means here. The check is
            the constraint, not the storage: it rejects an illegal member at
            ``INSERT`` rather than silently storing it, which is what a native
            ``ENUM`` would have done, and it is the same discipline the other
            backends' χ (surrogate) enum columns follow.

        ``json`` / ``jsonb``
            **Not here, and that is the point.** Both concepts are *rendered*,
            by ``format_data_type_json`` and ``format_data_type_jsonb``, each
            version-gated at 21c — the release that introduced the native
            ``JSON`` column type. A concept that renders needs no substitute,
            and listing one here as well would put the name in both sets with
            one of the two lying. The version gate lives in the formatter,
            where the SQL is actually chosen, rather than in a second table a
            reader would have to reconcile with it.

        ``uuid``
            **Oracle has no ``UUID`` data type in any release**, 26ai
            included: the built-in summary and the datatype grammar both list
            every type Oracle has, and ``UUID`` is not among them. What 23ai
            added is the ``UUID()`` *function*, which "returns a version 4
            variant 1 UUID as a ``RAW(16)`` value" — a function, not a type,
            and it says so in its own reference page.
            https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
            https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/uuid.html

            So a UUID is 16 bytes in ``RAW(16)``, and that is what this
            substitute produces **with no arguments at all**, because
            :attr:`OracleRawType.DEFAULT_LENGTH` is 16 — the same 16 bytes
            ``suggest_column_type(uuid.UUID)`` returns and the same bytes
            ``OracleUUIDAdapter`` reads and writes. An earlier revision of
            this docstring said Oracle "added a native ``UUID`` data type only
            in 23ai"; that was false, and false in the way that matters,
            because it would have had somebody writing ``UUID`` DDL against a
            server that has no such type.
        """
        return {
            "array": CustomType,
            "binary": OracleRawType,
            "varbinary": BlobType,
            "enum": VarCharType,
            "uuid": OracleRawType,
        }

    def format_data_type_integer(self, data_type: IntegerType) -> Tuple[str, tuple]:
        """``INTEGER`` and ``INT`` are one 4-byte signed integer, written as
        Oracle's ``NUMBER(10)``.

        The spelling is normalised rather than honoured — ``INT`` is an ANSI
        word Oracle converts away rather than a type it has, and refusing the
        concept's own shorthand would refuse the concept for no gain.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`, which is also where the reason for
        the refusal is written down. For why the width is ten digits — and why
        it is not the ``NUMBER(38)`` Oracle's ANSI table gives the word
        ``INTEGER`` — see this class's docstring."""
        self._check_spelling(data_type, IntegerType)
        self._refuse_unsigned_integer(data_type, "INTEGER", "NUMBER(10)")
        return "NUMBER(10)", ()

    def supports_data_type_integer(self) -> bool:
        return True

    def format_data_type_bigint(self, data_type: BigIntType) -> Tuple[str, tuple]:
        """``BIGINT`` and ``INT8`` are the same 8-byte signed integer, written
        as Oracle's ``NUMBER(19)``.

        Neither word is Oracle's: ``BIGINT`` is not an ANSI type at all and the
        server rejects it (``ORA-00902: invalid datatype`` on 21c and 23c), as
        it does ``INT8``. Both spellings are still accepted and normalised,
        because refusing them would refuse the concept rather than inform the
        caller.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`, and this class's docstring for the
        width."""
        self._check_spelling(data_type, BigIntType)
        self._refuse_unsigned_integer(data_type, "BIGINT", "NUMBER(19)")
        return "NUMBER(19)", ()

    def supports_data_type_bigint(self) -> bool:
        return True

    def format_data_type_smallint(self, data_type: SmallIntType) -> Tuple[str, tuple]:
        """``SMALLINT`` and ``INT2`` are the same 2-byte signed integer,
        written as Oracle's ``NUMBER(5)``.

        This is the width a review of this backend questioned, because Oracle's
        ANSI table maps ``SMALLINT`` to ``NUMBER(38)``. It does — and that table
        is precisely *why* the answer is five digits and not thirty-eight:
        Oracle has one integer column, so concepts of different widths have to
        be different precisions on it. See this class's docstring for the
        measurements and the rest of the argument.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`."""
        self._check_spelling(data_type, SmallIntType)
        self._refuse_unsigned_integer(data_type, "SMALLINT", "NUMBER(5)")
        return "NUMBER(5)", ()

    def supports_data_type_smallint(self) -> bool:
        return True

    def _refuse_unsigned_integer(
        self,
        data_type: "TinyIntType | SmallIntType | IntegerType | BigIntType",
        oracle_word: str,
        oracle_number: str,
    ) -> None:
        """Refuse ``unsigned=True``, because Oracle has no unsigned integer.

        The core integer concepts carry signedness as a **field** rather than as
        a class, so ``BigIntType(unsigned=True)`` is constructible, the flag is
        listed in ``PARAMETERS`` — hence it reaches ``__eq__``/``__hash__``,
        hence two declarations differing only in it are different columns — and
        it reaches the formatter.  Every width has to do something honest with
        it, and this backend's honest answer is a refusal, because Oracle's
        inventory of numeric types contains no unsigned entry at all:

        * The types Oracle recognises, spelled out in full: the ANSI names are
          ``{ CHARACTER [VARYING] (size) | { CHAR | NCHAR } VARYING (size) |
          VARCHAR (size) | NATIONAL { CHARACTER | CHAR } [VARYING] (size) |
          { NUMERIC | DECIMAL | DEC } [ (precision [, scale ]) ] |
          { INTEGER | INT | SMALLINT } | FLOAT [ (size) ] |
          DOUBLE PRECISION | REAL }`` and the built-in numerics are
          ``{ NUMBER [ (precision [, scale ]) ] | FLOAT [ (precision) ] |
          BINARY_FLOAT | BINARY_DOUBLE }``.  One symmetric ``NUMBER`` and two
          IEEE binary floats — there is no unsigned row to select.
          https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlqr/Data-Types.html
        * The reference chapter says the same in prose: the numeric data types
          "store positive and negative fixed and floating-point numbers", and
          ``NUMBER(p)`` is "a fixed-point number with precision p and scale 0".
          Signedness is not a property ``NUMBER`` has, only a range the caller
          may or may not want to use.
          https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        * There is not even a spelling that would parse.  ``column_definition``
          admits ``datatype_domain``, ``identity_clause``,
          ``inline_constraint``, ``inline_ref_constraint`` and
          ``annotations_clause`` after the type — never a type modifier — and
          the words ``UNSIGNED`` and ``SIGNED`` do not occur anywhere in the
          ``CREATE TABLE`` statement reference.
          https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/CREATE-TABLE.html

        The signed range does cover every width's unsigned range — ``NUMBER(10)``
        holds 9999999999, so it holds the whole of an unsigned 32-bit integer
        without loss, and ``NUMBER(19)`` holds 9999999999999999999 so it holds
        the whole of an unsigned 64-bit one — and that is the argument for
        dropping the *concept* on this backend.  It is not the argument for
        dropping the *flag*: what the flag says is that this particular column
        holds no negative value, and writing the signed column for an unsigned
        declaration produces a column that does not match what the caller
        declared while reporting success.  That is the same silent loss as
        accepting the flag and discarding it, so the flag is refused and the
        range is stated with a ``CHECK`` constraint, which is where a per-column
        range belongs anyway.

        Oracle's ANSI conversion table is no help here either: it maps
        ``NUMERIC``/``DECIMAL`` to ``NUMBER(p,s)`` and gives no unsigned row
        either, which is the same conclusion this class's docstring reaches
        about widths for the other half of the same table.
        """
        if not data_type.unsigned:
            return
        raise UnsupportedFeatureError(
            self.name,
            f"an unsigned {oracle_word} column "
            f"(Oracle has no unsigned integer type; {oracle_word} is "
            f"{oracle_number}, a symmetric NUMBER, and there is no UNSIGNED "
            f"attribute to write after it)",
            suggestion=(
                "Declare the column signed and enforce the range with a CHECK "
                "constraint if negatives must be rejected."
            ),
        )

    def _refuse_unsigned_numeric(
        self,
        data_type: "FloatType | RealType | DoubleType | DecimalType",
        oracle_word: str,
    ) -> None:
        """Refuse ``unsigned=True`` on ``NUMBER``, ``FLOAT`` or ``REAL``.

        The same field reaches the four exact/approximate numeric concepts that
        it reaches the four integer widths — ``DecimalType``, ``FloatType``,
        ``DoubleType`` and ``RealType`` each carry ``unsigned`` in
        ``PARAMETERS``, so two declarations differing only in it are different
        columns as far as the schema differ is concerned — and Oracle's answer is
        the same refusal as :meth:`_refuse_unsigned_integer`, for its own reasons,
        all of which were measured on both wired servers as well as read:

        * **The reference chapter says the storages are signed by construction.**
          "The Oracle database numeric data types store positive and negative
          fixed and floating-point numbers, zero, infinity, and values that are
          the undefined result of an operation—'not a number' or NAN."
          ``NUMBER(p)`` is "a fixed-point number with precision p and scale 0",
          and a type whose documented values include the negatives and ``-Inf``
          is not a type with an unsigned form to select.
          https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        * **The type inventory is closed and carries no signedness.**  Oracle's
          SQL Quick Reference lists the ANSI names and then the built-in
          numerics — ``NUMBER [ (precision [, scale ]) ] | FLOAT [ (precision) ]
          | BINARY_FLOAT | BINARY_DOUBLE`` — and there is no unsigned row in
          either list.
          https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlqr/Data-Types.html
        * **There is not even a spelling that would parse, which was measured
          rather than inferred.**  On 21c (21.0.0.0.0) and 26ai Free
          (23.26.1.0.0), every one of these declarations is rejected by the
          *parser* — it never reaches a semantic check, because ``UNSIGNED`` is
          simply an unexpected token where the type name ends::

              NUMBER UNSIGNED          ORA-00907 (21c) / ORA-03062 (26ai)
              NUMBER(10,2) UNSIGNED   ORA-00907 (21c) / ORA-03062 (26ai)
              FLOAT(126) UNSIGNED     ORA-00907 (21c) / ORA-03062 (26ai)
              FLOAT(63) UNSIGNED      ORA-00907 (21c) / ORA-03062 (26ai)
              BINARY_DOUBLE UNSIGNED  ORA-00907 (21c) / ORA-03062 (26ai)

          Both codes say the same thing: a missing right parenthesis, i.e. the
          parser was still inside the type when it hit the word.  ``CREATE
          TABLE``'s own grammar agrees — ``column_definition ::= ( datatype_domain
          ::= , identity_clause ::= , inline_constraint ::= , inline_ref_constraint
          ::= , annotations_clause ::= )``, five clauses after the type and not
          one of them a type modifier, with the words ``UNSIGNED`` and ``SIGNED``
          appearing **zero** times on the page.
          https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/CREATE-TABLE.html
        * **The documented way to say it works, and was measured.**  ``CREATE
          TABLE t (c NUMBER(10,2) CHECK (c >= 0))`` is accepted on both servers
          and reports ``DATA_TYPE='NUMBER'``, ``DATA_PRECISION=10``,
          ``DATA_SCALE=2`` — which is the ``suggestion`` below, not a courtesy.

        Writing a bare ``NUMBER`` or ``FLOAT(126)`` for an unsigned request would
        create a column that accepts the negatives the caller declared it would
        not, and report success: the same silent loss as accepting the flag and
        discarding it, which is what this replaces.

        ``UnsupportedFeatureError``, not ``ValueError``: this is a declaration
        the grammar cannot express at all rather than a wrong value, and the two
        exceptions do not share a base class.

        **Where this is called matters as much as what it raises.**  It runs
        *before* ``_check_spelling`` and before every precision and scale check,
        so a request that is wrong in two ways is told about this one first — the
        caller asked for something this grammar cannot express at all, which is
        a stronger and less recoverable statement than a misspelled word or an
        out-of-range number.  Every one of those existing checks still fires
        unchanged for a signed declaration, which is what
        ``test_oracle_type_protocol.py`` pins.
        """
        if not data_type.unsigned:
            return
        raise UnsupportedFeatureError(
            self.name,
            f"an unsigned {oracle_word} column "
            f"(Oracle has no unsigned numeric type; {oracle_word} is Oracle's own "
            f"word for a NUMBER, whose documented values are positive *and* "
            f"negative, Oracle's numeric type inventory has no unsigned row, "
            f"CREATE TABLE's column_definition admits no modifier after the type "
            f"name, and every '{oracle_word} ... UNSIGNED' declaration is "
            f"rejected by the parser -- ORA-00907 on 21c, ORA-03062 on 26ai)",
            suggestion=(
                "Declare the column signed and enforce the range with a CHECK "
                "constraint if negatives must be rejected -- Oracle accepts "
                "'c NUMBER(10,2) CHECK (c >= 0)'."
            ),
        )

    def format_data_type_float(self, data_type: FloatType) -> Tuple[str, tuple]:
        """``FLOAT(p)`` — a subtype of ``NUMBER`` with a **binary** precision, and
        the precision is written out even when the caller declared none.

        ``unsigned`` is refused rather than ignored, before the precision check
        below; see :meth:`_refuse_unsigned_numeric`, which is where the reason
        and the measurements are written down.

        Oracle's built-in summary has the whole of it: "``FLOAT [(p)]`` — a
        subtype of the ``NUMBER`` data type having precision ``p``. A ``FLOAT``
        value is represented internally as ``NUMBER``. The precision ``p`` can
        range from 1 to 126 binary digits", and the ANSI conversion table's note
        2 gives the other half — "the default precision for this data type is
        126 binary, or 38 decimal". The number is **measured and identical**:
        ``CREATE TABLE t (c FLOAT)`` reports ``DATA_TYPE = 'FLOAT'`` and
        ``DATA_PRECISION = 126`` on both wired servers — 21c (21.0.0.0.0) and
        26ai Free (23.26.1.0.0) — and the introspector reads that number, so a
        bare ``FLOAT`` comes back as ``FLOAT(126)``.

        That measurement is what licenses the fallback below, and it is why the
        number is read from :meth:`type_parameter_defaults` rather than written
        here: it is one fact about this server, and the declaration and this
        formatter must not be able to drift apart on it.  ``FloatType.precision``
        resolves against that declaration at read time, exactly as
        ``VarCharType.length`` does, so a bare bound declaration carries 126
        before this method runs and takes the first branch below; the fallback
        remains for a type with no dialect to resolve through, and reads the
        same declared number so the two paths cannot disagree.

        So a bare ``FLOAT`` was already a false claim about the round trip: the
        declaration said one word and the catalog said another, and
        :meth:`parse_type` gave the two different objects. Writing the documented
        default out makes the declaration and the catalog byte-identical, which
        is the same reason the sibling ``double`` writes ``FLOAT(126)`` and
        ``real`` writes ``FLOAT(63)`` instead of the bare words Oracle's table
        converts them from — the same table, note 3 and note 4.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html

        One honest caveat, stated rather than hidden: Oracle's own reference
        recommends against this type — "Oracle ``FLOAT`` is available for you to
        use, but Oracle recommends that you use the ``BINARY_FLOAT`` and
        ``BINARY_DOUBLE`` data types instead, as they are more robust" — and the
        reason is that ``FLOAT`` is *not* an IEEE binary float. It is a
        ``NUMBER`` whose digits are counted in binary, which is why a 64-bit
        value can come back as a 38-decimal-digit number: 126 binary digits
        "roughly equivalent to 38 digits of decimal precision". That is a
        reason to reach for the dedicated IEEE classes
        (:class:`OracleBinaryFloatType` / :class:`OracleBinaryDoubleType`,
        which render the two words the catalog reports for them) when IEEE
        storage is what is wanted — not a reason for this method to render
        something other than ``FLOAT``: ``float`` is the concept the caller
        asked for, and Oracle's word for it is ``FLOAT``.
        """
        self._refuse_unsigned_numeric(data_type, "FLOAT")
        if data_type.precision is not None:
            if not (1 <= data_type.precision <= self._FLOAT_DEFAULT_PRECISION):
                raise ValueError(
                    f"Oracle FLOAT binary precision must be 1-"
                    f"{self._FLOAT_DEFAULT_PRECISION}, "
                    f"got {data_type.precision}"
                )
            return f"FLOAT({data_type.precision})", ()
        return f"FLOAT({self.type_parameter_defaults()['float']['precision']})", ()

    def supports_data_type_float(self) -> bool:
        return True

    def format_data_type_real(self, data_type: RealType) -> Tuple[str, tuple]:
        """Oracle has no ``REAL`` type — ``REAL`` is an alias it accepts for
        ``FLOAT(63)``, per the ANSI conversion table's note 4 — and the precision
        is written out because that is what the server reports back for a column
        declared ``REAL``: ``DATA_TYPE='FLOAT'``, ``DATA_PRECISION = 63``
        (measured on 21c and on 26ai Free, which agree).

        **63 is this concept's rendering, not a declared default.** ``RealType``
        carries exactly one field — ``unsigned``, and it is
        :meth:`_refuse_unsigned_numeric`'s to answer — so 24 bits of mantissa is
        the whole of the concept and there is no field for a server default to
        resolve into. It is named in :attr:`_REAL_PRECISION` rather than written
        as a literal so the two ``FLOAT`` renderers cannot drift apart on the two
        numbers they must differ by.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_numeric`."""
        self._refuse_unsigned_numeric(data_type, "FLOAT")
        return f"FLOAT({self._REAL_PRECISION})", ()

    def supports_data_type_real(self) -> bool:
        return True

    def format_data_type_double(self, data_type: DoubleType) -> Tuple[str, tuple]:
        """``double`` and ``double precision`` are one 64-bit float, and Oracle's
        own name for it is ``FLOAT`` carrying a binary precision. Both
        spellings render ``FLOAT(126)``, stated in full rather than as a bare
        ``FLOAT``: that is exactly what the server reports for
        ``DOUBLE PRECISION`` *and* for a bare ``FLOAT`` — ``DATA_PRECISION =
        126`` for both, measured on 21c and 26ai Free — so writing it in full is
        what keeps the round trip byte-identical.

        The 126 is read from the one place this server's number is written down,
        :meth:`type_parameter_defaults`, for the same reason
        :meth:`format_data_type_float` reads it there: the ANSI conversion table
        maps ``FLOAT``, ``DOUBLE PRECISION`` and ``REAL`` to three *different*
        precisions (126 / 126 / 63) from one table, so the numbers have to be
        traceable to the note that says each.

        ``unsigned`` is refused rather than ignored, **before** the spelling
        check; see :meth:`_refuse_unsigned_numeric`."""
        self._refuse_unsigned_numeric(data_type, "FLOAT")
        self._check_spelling(data_type, DoubleType)
        return f"FLOAT({self.type_parameter_defaults()['float']['precision']})", ()

    def supports_data_type_double(self) -> bool:
        return True

    def format_data_type_decimal(self, data_type: DecimalType) -> Tuple[str, tuple]:
        """``DECIMAL``, ``NUMERIC`` and ``DEC`` are one exact-numeric concept
        and Oracle calls it ``NUMBER``; all three are accepted and all three
        render as ``NUMBER``.

        ``unsigned`` is refused rather than ignored, **before** ``_check_spelling``
        and before both ``ValueError`` range checks below; see
        :meth:`_refuse_unsigned_numeric`, which is where the reason and the
        measurements live.  Those two checks, and the ``-84..127`` scale bound,
        are untouched and still fire for a signed declaration."""
        self._refuse_unsigned_numeric(data_type, "NUMBER")
        self._check_spelling(data_type, DecimalType)
        if data_type.precision is not None:
            if not (1 <= data_type.precision <= 38):
                raise ValueError(
                    f"Oracle NUMBER precision must be 1-38, "
                    f"got {data_type.precision}"
                )
        if data_type.scale is not None:
            if not (-84 <= data_type.scale <= 127):
                raise ValueError(
                    f"Oracle NUMBER scale must be -84 to 127, "
                    f"got {data_type.scale}"
                )
        if data_type.precision is not None and data_type.scale is not None:
            return f"NUMBER({data_type.precision}, {data_type.scale})", ()
        if data_type.precision is not None:
            return f"NUMBER({data_type.precision})", ()
        return "NUMBER", ()

    def supports_data_type_decimal(self) -> bool:
        return True

    def format_data_type_boolean(self, data_type: BooleanType) -> Tuple[str, tuple]:
        """``boolean`` and ``bool`` are one concept, and which Oracle column type
        carries it depends on the release: **``BOOLEAN`` from 23ai**, and
        ``NUMBER(1)`` before that.

        The gate is the dialect's ``_version`` against ``(23, 0, 0)`` — the same
        fact :meth:`~...features.OracleFeaturesMixin.supports_boolean_type`
        reports, and the version ``PRODUCT_COMPONENT_VERSION.VERSION`` returns
        for the 23ai line. Measured on both wired servers, which is the whole of
        the evidence for the boundary:

        ================================================  ==================== ==========================
        statement                                         21c                 23ai / 26ai (23.26.1.0.0)
        ================================================  ==================== ==========================
        ``CREATE TABLE t (a BOOLEAN)``                    ``ORA-00902``       ``DATA_TYPE='BOOLEAN'``
        ================================================  ==================== ==========================

        and Oracle's own release documentation puts it in 23ai — "Oracle Database
        23ai introduces the new ``BOOLEAN`` data type. This leverages the use of
        true boolean columns/variables, instead of simulating them with a
        numeric value or Varchar", and "Some features (like SQL domains or
        BOOLEAN) only work in Oracle Database 23ai". The 26ai references list it
        among the built-in data types (code 252) and describe it as a new feature
        "in release 26ai", because 26ai is the branding of the 23.0 line the
        feature arrived in; ``PRODUCT_COMPONENT_VERSION`` reports ``23.0.0.0.0``
        and ``VERSION_FULL`` ``23.26.1.0.0`` on the wired 23c/23ai server, so
        the gate is stated against the *version number* the server reports rather
        than against a product name, which is the thing a comparison can be made
        against at all.
        https://docs.oracle.com/en/learn/db23ai-sql-features/index.html
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html

        **Why gate rather than pick one.** ``NUMBER(1)`` is legal everywhere and
        ``BOOLEAN`` is illegal below 23ai, so a single unconditional choice is
        either wrong on old releases or wrong on new ones. The two columns are
        also genuinely different and the choice is worth making per server:
        ``BOOLEAN`` "comprises the distinct truth values True and False", and
        "unless prohibited by a ``NOT NULL`` constraint, the boolean data type
        also supports the truth value ``UNKNOWN`` as the null value" — the
        standard's three-valued logic by name, where ``NUMBER(1)`` is a number in
        which only 0 and 1 are valid and ``NULL`` means "unknown" only by
        convention. A 23ai server told to store a ``boolean`` should store a
        ``boolean``.

        Neither spelling is refused: core's invariant is that a concept's default
        spelling always renders, and here *both* forms of it render — one of
        them per release. A caller who needs the other one says so by building
        the type it wants to store, which on a release that cannot store it is a
        statement about the column rather than a silent substitution.

        An **unadapted** dialect (``OracleDialect()`` with no version, whose
        ``.version`` raises ``DialectNotAdaptedException``) also renders
        ``NUMBER(1)``, and that is not a guess of the same kind the un-sized
        ``RAW`` used to be: ``NUMBER(1)`` is legal on *every* release including
        every one that has a native ``BOOLEAN``, so nothing reading the emitted
        DDL can be misled about whether the statement will run. Raising here
        instead would break DDL generation for a dialect that has not been
        adapted yet, which is a regression this gate has no reason to cause.

        One caveat that belongs with the gate rather than in a bug report, and
        which is now **fixed**: this backend's version detection used to filter on
        ``PRODUCT LIKE 'Oracle Database%'``, a name Oracle no longer uses — the
        23ai/26ai server reports ``Oracle AI Database 26ai Free`` — so both
        version queries returned nothing, the fallback reported ``(19, 0, 0)``,
        and **this gate was inert on the live server**: it wrote ``NUMBER(1)``
        where the server has a native ``BOOLEAN``. Detection now selects on the
        shape of the version rather than on any product name (see
        ``version.py``), the measured 26ai server yields ``(23, 0, 0)`` and this
        method writes ``BOOLEAN`` there, and
        ``tests/.../test_introspector_deep.py::TestDialectCarriesTheServersOwnVersion``
        pins the agreement per server so it cannot go back to being inert.
        """
        self._check_spelling(data_type, BooleanType)
        version = getattr(self, "_version", None)
        if version is not None and version >= _ORACLE_BOOLEAN_TYPE_MIN_VERSION:
            return "BOOLEAN", ()
        return "NUMBER(1)", ()

    def supports_data_type_boolean(self) -> bool:
        """``True`` on every release.

        Not a gate: the ``boolean`` concept renders on all of them, as ``BOOLEAN``
        from 23ai and ``NUMBER(1)`` below it. A version-dependent answer would
        have to mean "the server can store it natively", which is
        :meth:`~...features.OracleFeaturesMixin.supports_boolean_type`'s
        question and not this one's."""
        return True

    def format_data_type_varchar(self, data_type: VarCharType) -> Tuple[str, tuple]:
        """``varchar`` and ``character varying`` are the same variable-length
        string, and Oracle's word for it is ``VARCHAR2``. Both are accepted —
        Oracle's own grammar admits ``CHARACTER VARYING`` as a synonym for
        ``VARCHAR2`` — and both render ``VARCHAR2``, because writing
        ``VARCHAR2`` back is what Oracle reports from the catalog.

        A caller who states **no** length gets ``VARCHAR2(4000)``, Oracle's
        ``MAX_STRING_SIZE = STANDARD`` ceiling — a default this dialect
        chooses, not one Oracle documents (``"You must specify size for
        VARCHAR2"``). It stays on this side only: on the *parse* side the same
        absence is refused, because there the width would be a claim about a
        catalog row that never existed
        (:meth:`_require_declared_length`). The consequence is recorded rather
        than hidden: ``VarCharType(d)`` and ``parse_type("VARCHAR2(4000)")``
        render identical SQL and are **not** ``==``, because ``length`` is in
        :attr:`~VarCharType.PARAMETERS` and ``None`` is not ``4000``. That
        asymmetry is a property of "no width stated" being unrepresentable in a
        type's identity, and it is a core question rather than an Oracle one —
        the same shape appears for ``char`` (1), ``float`` (126) and
        ``nvarchar2`` (2000).
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        self._check_spelling(data_type, VarCharType)
        return (f"VARCHAR2({data_type.length})" if data_type.length is not None else "VARCHAR2(4000)"), ()

    def supports_data_type_varchar(self) -> bool:
        return True

    def format_data_type_char(self, data_type: CharType) -> Tuple[str, tuple]:
        """``char`` and ``character`` are the same fixed-length,
        blank-padded string, and Oracle's word for it is ``CHAR``. Both are
        accepted (``CHARACTER`` is a documented Oracle synonym) and both
        render ``CHAR(n)``.

        A caller who states no length gets the bare word ``CHAR``, which Oracle
        resolves to ``CHAR(1)`` — its own documented default, so this is a
        spelling of the column rather than an invented width.

        **The bare word is deliberate, and it is the one case where a
        ``type_parameter_defaults`` entry was measured, written down and then
        not made.** Oracle documents the 1 twice ("You can omit size from the
        column definition. The default value is 1"; "Default and minimum size is
        1 byte") and the server honours it: ``CREATE TABLE t (c CHAR)`` is
        accepted on both wired servers and reports ``DATA_LENGTH = 1`` and
        ``CHAR_LENGTH = 1``, byte-for-byte what ``CHAR(1)`` reports. So the
        declaration would be true and it would close a real asymmetry —
        ``CharType(d)`` currently renders ``CHAR`` while
        ``parse_type("CHAR")`` yields ``CharType(length=1)``, and those two are
        not ``==`` even though they render the same SQL.

        Declaring it would change that SQL, because resolving ``length`` to 1
        makes this formatter write ``CHAR(1)``. Same column, different bytes,
        and the cross-repo contract's rule 6 — rendered SQL does not change for
        an existing rendering — is not worth spending on it. So the entry is
        absent on purpose, :attr:`_CHAR_DEFAULT_LENGTH` holds the number for the
        two places that genuinely need it (:meth:`parse_type` and
        :meth:`_require_declared_length`'s message), and the asymmetry is written
        down here instead. **It is one line to reverse** if the maintainer wants
        the identity closed: add ``"char": {"length": 1}`` to
        :meth:`type_parameter_defaults` and drop the bare-word branch below.
        """
        self._check_spelling(data_type, CharType)
        return (f"CHAR({data_type.length})" if data_type.length is not None else "CHAR"), ()

    def supports_data_type_char(self) -> bool:
        return True

    def format_data_type_text(self, data_type: TextType) -> Tuple[str, tuple]:
        """``text`` and ``clob`` are one unbounded string, and Oracle's word
        for it is ``CLOB`` — so both render ``CLOB`` and neither is refused.
        Refusing ``text`` here would refuse the concept's *default* spelling,
        which is what every plain ``TextType(d)`` carries; and refusing
        ``clob`` would refuse the one word Oracle does write. The two arms
        collapse, so there is nothing left to distinguish between them."""
        self._check_spelling(data_type, TextType)
        return "CLOB", ()

    def supports_data_type_text(self) -> bool:
        return True

    def format_data_type_blob(self, data_type: BlobType) -> Tuple[str, tuple]:
        """``blob`` and ``bytea`` are the same unbounded byte storage; Oracle
        calls it ``BLOB``. ``BYTEA`` is not an Oracle word at all, but it is
        a spelling of the concept and the column Oracle produces is a
        ``BLOB``, so it is accepted and normalised rather than refused."""
        self._check_spelling(data_type, BlobType)
        return "BLOB", ()

    def supports_data_type_blob(self) -> bool:
        return True

    def format_data_type_datetime(self, data_type: DateTimeType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 9):
                raise ValueError(
                    f"Oracle TIMESTAMP fractional seconds precision must be 0-9, "
                    f"got {data_type.precision}"
                )
            return f"TIMESTAMP({data_type.precision})", ()
        return "TIMESTAMP", ()

    def supports_data_type_datetime(self) -> bool:
        return True

    def format_data_type_date(self, data_type: DateType) -> Tuple[str, tuple]:
        return "DATE", ()

    def supports_data_type_date(self) -> bool:
        return True

    def format_data_type_time(self, data_type: TimeType) -> Tuple[str, tuple]:
        return "VARCHAR2(8)", ()

    def supports_data_type_time(self) -> bool:
        return True

    def format_data_type_timestamp(self, data_type: TimestampType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 9):
                raise ValueError(
                    f"Oracle TIMESTAMP fractional seconds precision must be 0-9, "
                    f"got {data_type.precision}"
                )
            return f"TIMESTAMP({data_type.precision})", ()
        return "TIMESTAMP", ()

    def supports_data_type_timestamp(self) -> bool:
        return True

    def format_data_type_json(self, data_type: JsonType) -> Tuple[str, tuple]:
        """``JSON`` is a native Oracle **column type** from 21c, and text below it.

        The version boundary is Oracle's, not this backend's: 21c introduced the
        ``JSON`` keyword in ``CREATE TABLE`` (measured — the catalog reports
        ``DATA_TYPE='JSON'``), where 18c and earlier have no such word and the
        only way to hold a document is a ``CLOB`` carrying an ``IS JSON``
        constraint, or text with no validation at all.

        Below 21c the word written stays ``VARCHAR2(4000)`` — the choice this
        method has always made, and one that is *bounded*: an unbounded
        ``CLOB`` would claim a width the concept does not have, while 4000 is
        Oracle's own ``MAX_STRING_SIZE = STANDARD`` ceiling. The caller who
        needs more declares it.

        The 21c+ branch is not free of caveats and they are recorded rather
        than discovered: a ``JSON`` column was accepted only in a non-SYSTEM
        tablespace on the servers measured (``ORA-43853`` in ``SYSTEM``, the
        same statement succeeding under ``TABLESPACE USERS``). The gate picks
        the word; the tablespace is the model author's, and this backend cannot
        read which one a model is created in.
        """
        version = getattr(self, "_version", None)
        if version is not None and version >= _ORACLE_JSON_TYPE_MIN_VERSION:
            return "JSON", ()
        return "VARCHAR2(4000)", ()

    def supports_data_type_json(self) -> bool:
        return True

    def format_data_type_interval(self, data_type: IntervalType) -> Tuple[str, tuple]:
        """Oracle has **both** interval types natively, so neither is
        substituted for text.

        ``fields`` is Oracle's own qualifier — ``YEAR TO MONTH`` or
        ``DAY TO SECOND``, each with an optional leading precision — and an
        unqualified ``IntervalType()`` renders one stated in full, so that what is
        written is what Oracle reports back.

        **Which qualifier, and why it is not a
        :meth:`type_parameter_defaults` entry.** Measured on both wired servers
        (21c 21.0.0.0.0 and 26ai Free 23.26.1.0.0), which agree on every row::

            CREATE TABLE t (c INTERVAL)               -> ORA-30089:
                                                       missing or invalid
                                                       <datetime field>
            CREATE TABLE t (c INTERVAL DAY TO SECOND)  -> DATA_TYPE=
                'INTERVAL DAY(2) TO SECOND(6)'        DATA_PRECISION=2
                                                       DATA_SCALE=6
            CREATE TABLE t (c INTERVAL YEAR TO MONTH)  -> DATA_TYPE=
                'INTERVAL YEAR(2) TO MONTH'           DATA_PRECISION=2
            CREATE TABLE t (c INTERVAL DAY(4))        -> ORA-00963:
                                                       unsupported interval
                                                       type

        ... and ``HOUR``, ``MINUTE``, ``SECOND``, ``MONTH``, ``YEAR`` and
        ``DAY TO MINUTE`` are all ``ORA-00963`` the same way. Three things
        follow, and together they are why this concept does not belong in a
        mechanism whose entries answer one parameter at a time:

        * **There is no bare ``INTERVAL`` on Oracle.** The server rejects it, so
          unlike ``FLOAT`` and ``CHAR`` there is no declaration of nothing for
          the catalog to have supplied a value for — and unlike ``CHAR`` there
          is no stored width to read back. A server that refuses the
          declaration supplies no default for it.
        * **``fields`` is a qualifier, not a parameter.** It is a member of the
          closed SQL:2016 vocabulary
          (:class:`~...expression.types.IntervalQualifier`), and the value here
          is a *whole* ``DAY(2) TO SECOND(6)`` — two documented precisions
          (``day_precision`` "The default is 2", ``fractional_seconds_precision``
          "The default is 6"; NLSPG 4.2.2.2) inside one qualifier string. The
          mechanism is keyed ``{concept: {parameter: value}}``, so declaring it
          would need a key standing for a *qualifier*, and the two precisions
          inside it would still have nowhere to go.
        * **Oracle has two, and the choice between them is this backend's.**
          ``INTERVAL YEAR(2) TO MONTH`` counts years and months and nothing
          smaller; ``INTERVAL DAY(2) TO SECOND(6)`` counts days, hours, minutes
          and fractional seconds and nothing larger. ``DAY TO SECOND`` is the
          wider of the two and so is the one a caller who said nothing gets —
          a choice made here, not a value read back from a server.

        The rendering is therefore unchanged and is right to stay so: writing the
        full qualifier means the column Oracle creates is exactly the column this
        declared, and :meth:`parse_type` reads that same string back, so the
        rendered round trip is already byte-identical. What is *not* closed —
        and cannot be closed by any number — is the object identity:
        ``IntervalType(d)`` carries ``fields=None`` and the introspected
        ``IntervalType(d, fields="DAY(2) TO SECOND(6)")`` does not compare equal
        to it. That is the same ``None``-versus-a-value asymmetry
        :meth:`format_data_type_varchar` records, and here it is the honest end
        of the road: there is no single parameter whose absence could be
        resolved to close it, because what is absent is a whole qualifier.

        A caller who wants the other interval, or a different resolution, builds
        the type that asks for it — ``IntervalType(d, fields="YEAR TO MONTH")`` —
        and this method renders it verbatim.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html
        """
        if data_type.fields:
            return f"INTERVAL {data_type.fields.upper()}", ()
        # The two numbers are named at module scope so they are traceable to the
        # sentences that document them rather than buried inside a SQL string.
        return (
            f"INTERVAL DAY({_ORACLE_INTERVAL_DAY_PRECISION})"
            f" TO SECOND({_ORACLE_INTERVAL_SECOND_PRECISION})",
            (),
        )

    def supports_data_type_interval(self) -> bool:
        return True

    def format_data_type_xml(self, data_type: XmlType) -> Tuple[str, tuple]:
        """``XMLTYPE`` — Oracle's native XML type, available on every release
        this backend targets (``XMLType`` is the 21c spelling of the same
        type and renders identically).

        The value is an XML **infoset**, not a document written as text: the
        server parses and can validate it, and querying it goes through
        ``XMLQuery``/``XMLExists`` rather than string functions. That is why
        this is the XML concept and not ``TextType`` — a ``CLOB`` holding the
        same markup cannot be queried that way at all, and calling the column
        text would say otherwise.

        An associated XML Schema is a column attribute and does not enter the
        type's identity.
        """
        return "XMLTYPE", ()

    def supports_data_type_xml(self) -> bool:
        return True

    def format_data_type_custom(self, data_type: CustomType) -> Tuple[str, tuple]:
        """Write a type name the framework has no class for, verbatim.

        ``CustomType`` is what :meth:`parse_type` returns for every name the
        framework does not model, so without this formatter a column
        introspected as, say, ``SDO_GEOMETRY`` could not be turned back into
        DDL at all — the round trip would stop at the parse. It is also how a
        column referring to a *named* Oracle type is declared: a ``VARRAY`` or
        object type is written as its name, and a name is an identifier.

        The name has already been through
        ``expression/type_name.py::validate_type_name``, which admits an
        identifier, an optional schema qualifier, an optional precision and
        array markers — and nothing that could end the statement or open a
        subquery. This position cannot take a bound parameter, so the grammar
        is the whole defence.
        """
        return data_type.raw, ()

    def supports_data_type_custom(self) -> bool:
        return True

    # --- Oracle-namespaced type formatters ---
    # Each of these renders a word that the core formatters above do not, which
    # is the only reason a prefixed dispatch key exists on this backend: a
    # concept the core hierarchy already names under a *different* Oracle word,
    # or no core rendering at all. There is deliberately no `oracle_integer`,
    # `oracle_smallint`, `oracle_bigint`, `oracle_varchar2`, `oracle_char` or
    # `oracle_blob` — see this class's docstring for the measurement that
    # removed them.

    def format_data_type_oracle_nvarchar2(self, data_type: OracleNVarChar2Type) -> Tuple[str, tuple]:
        return (f"NVARCHAR2({data_type.length})" if data_type.length is not None else "NVARCHAR2(2000)"), ()

    def supports_data_type_oracle_nvarchar2(self) -> bool:
        return True

    def format_data_type_oracle_nclob(self, data_type: OracleNClobType) -> Tuple[str, tuple]:
        return "NCLOB", ()

    def supports_data_type_oracle_nclob(self) -> bool:
        return True

    def format_data_type_oracle_long(self, data_type: OracleLongType) -> Tuple[str, tuple]:
        return "LONG", ()

    def supports_data_type_oracle_long(self) -> bool:
        return True

    def format_data_type_oracle_raw(self, data_type: OracleRawType) -> Tuple[str, tuple]:
        """``RAW(n)`` — Oracle requires the size, so there is no bare form.

        Oracle's summary says "You must specify size for a ``RAW`` value", and
        the server agrees: ``CREATE TABLE t (a RAW)`` is
        ``ORA-00906: missing left parenthesis`` on 21c and 23c. So
        :attr:`OracleRawType.length` cannot be ``None``: the class defaults it
        to :attr:`OracleRawType.DEFAULT_LENGTH` (16, the width ``UUID()``
        returns) rather than leaving the dialect to invent a width here. An
        earlier revision invented Oracle's 2000-byte
        ``MAX_STRING_SIZE = STANDARD`` *ceiling* for the un-sized case, which
        turned every suggested UUID column into a 2 KB binary column — and,
        through :meth:`parse_type`, turned every introspected ``RAW(16)`` into a
        2000-byte column too.

        ``n`` is a ceiling and not a fixed width: Oracle calls ``RAW`` "a
        variable-length data type like ``VARCHAR2``" and it does not pad, which
        is why :class:`OracleRawType` is the core ``varbinary`` concept and not
        a fixed-length one.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        return f"RAW({data_type.length})", ()

    def supports_data_type_oracle_raw(self) -> bool:
        return True

    def format_data_type_oracle_long_raw(self, data_type: OracleLongRawType) -> Tuple[str, tuple]:
        return "LONG RAW", ()

    def supports_data_type_oracle_long_raw(self) -> bool:
        return True

    def format_data_type_oracle_binary_float(self, data_type: OracleBinaryFloatType) -> Tuple[str, tuple]:
        """``BINARY_FLOAT`` — 32-bit IEEE, written as the catalog writes it.

        The word is the whole declaration and takes no parameters, so nothing
        from the instance is rendered. That is what lets the catalog word
        round-trip: measured on 18.4 XE, 21.3 XE and 26ai Free 23.26.1,
        ``CREATE TABLE t (c BINARY_FLOAT)`` reports
        ``DATA_TYPE='BINARY_FLOAT'`` with ``DATA_LENGTH=4``, while a ``FLOAT``
        column reports ``'FLOAT'`` with ``DATA_PRECISION`` — two different
        storages that used to share ``FloatType`` and its ``FLOAT(63)``
        rendering (finding F.4-4).
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        return "BINARY_FLOAT", ()

    def supports_data_type_oracle_binary_float(self) -> bool:
        return True

    def format_data_type_oracle_binary_double(self, data_type: OracleBinaryDoubleType) -> Tuple[str, tuple]:
        """``BINARY_DOUBLE`` — 64-bit IEEE, written as the catalog writes it.

        The 8-byte sibling of :meth:`format_data_type_oracle_binary_float`,
        parameterless for the same reason. The catalog reports
        ``DATA_TYPE='BINARY_DOUBLE'`` with ``DATA_LENGTH=8`` on all three
        wired servers, where ``DOUBLE PRECISION`` reports ``'FLOAT'`` with
        ``DATA_PRECISION=126`` (finding F.4-4).
        """
        return "BINARY_DOUBLE", ()

    def supports_data_type_oracle_binary_double(self) -> bool:
        return True

    # --- Oracle core-comparable type handlers ---
    # These mirror MySQL/Postgres equivalents so that callers passing a core
    # DataType (e.g. TinyIntType, TimeTzType, JsonBType, TimestampTzType)
    # get an Oracle-specific rendering instead of falling back to the base
    # class' default SQL (which is often MySQL-flavored or undefined).

    def format_data_type_tinyint(self, data_type: TinyIntType) -> Tuple[str, tuple]:
        """Oracle has no 1-byte integer type at all — ``TINYINT`` and ``INT1``
        are absent from the ANSI names it recognises and the server rejects both
        (``ORA-00902``) — so the concept is *widened* to the smallest ``NUMBER``
        that still holds its whole range, three decimal digits. Both ``tinyint``
        and ``int1`` are accepted: the caller asked for one concept, and
        refusing would leave the concept unusable here while saying nothing
        a widened column does not already say.

        The same reasoning picks the other widths; see this class's docstring.
        Widening the column does not make it unsigned, so ``unsigned`` is still
        refused rather than ignored; see :meth:`_refuse_unsigned_integer`."""
        self._check_spelling(data_type, TinyIntType)
        self._refuse_unsigned_integer(data_type, "TINYINT", "NUMBER(3)")
        return "NUMBER(3)", ()

    def supports_data_type_tinyint(self) -> bool:
        return True

    def format_data_type_timetz(self, data_type: TimeTzType) -> Tuple[str, tuple]:
        # Oracle supports TIMESTAMP WITH TIME ZONE; precision optional.
        return (f"TIMESTAMP({data_type.precision}) WITH TIME ZONE"
                if getattr(data_type, 'precision', None) is not None
                else "TIMESTAMP WITH TIME ZONE"), ()

    def supports_data_type_timetz(self) -> bool:
        return True

    def format_data_type_timestamptz(self, data_type: TimestampTzType) -> Tuple[str, tuple]:
        return (f"TIMESTAMP({data_type.precision}) WITH TIME ZONE"
                if getattr(data_type, 'precision', None) is not None
                else "TIMESTAMP WITH TIME ZONE"), ()

    def supports_data_type_timestamptz(self) -> bool:
        return True

    def format_data_type_oracle_timestamp_ltz(self, data_type: OracleTimestampLtzType) -> Tuple[str, tuple]:
        """``TIMESTAMP(n) WITH LOCAL TIME ZONE`` — the local-time-zone spelling.

        Oracle's two timestamp-with-time-zone forms are different columns:
        ``WITH TIME ZONE`` stores the offset in the row, ``WITH LOCAL TIME
        ZONE`` stores none and normalises to the database time zone. The
        catalog says which one a column is — measured on 18.4 XE, 21.3 XE and
        26ai Free 23.26.1, a ``WITH LOCAL TIME ZONE`` column reports
        ``DATA_TYPE='TIMESTAMP(6) WITH LOCAL TIME ZONE'`` — so re-rendering
        the word as the core ``WITH TIME ZONE`` would write back a column that
        is not the one introspected (finding F.4-3 of the plan appendix).
        ``precision`` is written when declared, exactly as the sibling
        formatter does; the bare word is left bare because the server default
        (6) is the separate question tracked as F.4-5.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        return (f"TIMESTAMP({data_type.precision}) WITH LOCAL TIME ZONE"
                if getattr(data_type, 'precision', None) is not None
                else "TIMESTAMP WITH LOCAL TIME ZONE"), ()

    def supports_data_type_oracle_timestamp_ltz(self) -> bool:
        return True

    def format_data_type_jsonb(self, data_type: JsonBType) -> Tuple[str, tuple]:
        """Oracle has no ``JSONB`` — but from 21c it has ``JSON``, which is the answer.

        ``JSONB`` is PostgreSQL's binary JSON, and Oracle has no binary-JSON
        column type in any release. What it does have, from **21c**, is the
        native ``JSON`` word — the same 21c boundary :meth:`format_data_type_json`
        documents — and that is what a JSON document should be stored in here,
        not the ``CLOB`` this method has always written.

        The comment this method used to carry said as much — "21c+ uses native
        JSON, otherwise CLOB" — while the code followed only the second half.
        That is the shape of bug worth naming: the fact was known and recorded,
        and simply never reached the branch. A reader of the code and a reader
        of the running dialect disagreed about which one was true.

        Below 21c it stays ``CLOB``: unbounded, so it does not claim a width the
        concept lacks, and readable by the ``IS JSON`` constraint this backend
        writes for it. ``VARCHAR2(4000)`` would be the wrong choice here in a way
        it is not for ``json`` — a *binary* document has no stated ceiling, and
        4000 would be an invented one.
        """
        version = getattr(self, "_version", None)
        if version is not None and version >= _ORACLE_JSON_TYPE_MIN_VERSION:
            return "JSON", ()
        return "CLOB", ()

    def supports_data_type_jsonb(self) -> bool:
        return True

    # --- Parsing ---
    #
    # Canonical in the sense the hierarchy requires: one name in, one concept
    # out. Where a word can be read two ways it is disambiguated here rather
    # than by a second class — LONG RAW is matched as binary before the LONG
    # string rule can take it, NCLOB before CLOB, and ``CHARACTER VARYING``
    # / ``CHAR VARYING`` before ``CHARACTER``/``CHAR`` — and every spelling a
    # concept declares (``CHARACTER`` for CHAR, ``CHARACTER VARYING`` for
    # VARCHAR2, ``CLOB`` for TEXT, ``YEAR TO MONTH`` / ``DAY TO SECOND`` for
    # INTERVAL) reaches the class that owns it.
    #
    # **The classes it returns are the core ones wherever the core formatter
    # already writes the same word.** ``NUMBER(10)``/``NUMBER(5)``/``NUMBER(19)``
    # come back as ``IntegerType``/``SmallIntType``/``BigIntType``,
    # ``VARCHAR2(n)`` as ``VarCharType``, ``CHAR(n)`` as ``CharType`` and ``BLOB``
    # as ``BlobType``, because a declared column and the catalog row it produced
    # must be ``==`` and ``DataType.__eq__`` is class identity. Only the eight
    # words no core formatter here writes — ``NVARCHAR2``, ``NCLOB``, ``LONG``,
    # ``RAW(n)``, ``LONG RAW``, ``BINARY_FLOAT``, ``BINARY_DOUBLE``,
    # ``TIMESTAMP WITH LOCAL TIME ZONE`` — get an ``oracle_``-named class, plus
    # ``BOOLEAN`` from 23ai, which is this backend's own rendering of the core
    # ``boolean`` concept and not a class of its own.

    _ORACLE_NUMBER_TYPES = re.compile(r"^(?:NUMBER|FLOAT|BINARY_FLOAT|BINARY_DOUBLE)\b", re.IGNORECASE)
    # ``UNSIGNED`` as a whole word anywhere in a type string.  Oracle's parser
    # rejects the string outright, so this can only fire on caller-supplied DDL --
    # which ``parse_type`` is a documented reader of -- and the point is to refuse
    # it rather than read the attribute off and return a signed type.  See
    # ``_refuse_unsigned_type_string``.
    _UNSIGNED_ATTRIBUTE = re.compile(r"\bUNSIGNED\b", re.IGNORECASE)
    # NOTE: LONG RAW must be matched before LONG; _ORACLE_BLOB_TYPES is
    # checked ahead of _ORACLE_STRING_TYPES in parse_type() for this reason.
    # The longer forms come first for the same reason: ``CHARACTER`` would
    # otherwise be read as a spelling of CHAR when it is really the head of
    # ``CHARACTER VARYING``. The list is the set of words Oracle's own
    # grammar accepts (verified against 21c), not the concepts' full
    # SPELLINGS: ``TEXT`` and ``BYTEA`` are rejected by the server as invalid
    # datatypes, so there is nothing for parse_type to read. (``BOOL`` *is* a
    # word Oracle accepts — from 23ai, and reported back as ``BOOLEAN`` — so it
    # is read by ``_ORACLE_BOOLEAN_TYPES`` below rather than being in this list.)
    _ORACLE_STRING_TYPES = re.compile(
        r"^(?:CHARACTER\s+VARYING|CHAR\s+VARYING|VARCHAR2|NVARCHAR2|VARCHAR|"
        r"CHARACTER|NCHAR|CHAR|CLOB|NCLOB|LONG)\b",
        re.IGNORECASE,
    )
    _ORACLE_BLOB_TYPES = re.compile(r"^(?:BLOB|RAW|LONG\s+RAW)\b", re.IGNORECASE)
    _ORACLE_DATE_TYPES = re.compile(r"^(?:DATE|TIMESTAMP|INTERVAL)\b", re.IGNORECASE)
    _ORACLE_XML_TYPES = re.compile(r"^(?:XMLTYPE|SYS\.XMLTYPE)\b", re.IGNORECASE)
    _ORACLE_BOOLEAN_TYPES = re.compile(r"^(?:BOOLEAN|BOOL)\b", re.IGNORECASE)

    #: Oracle's documented size for a ``CHAR``/``NCHAR`` declared without one:
    #: "You can omit size from the column definition. The default value is 1"
    #: (stated for both ``CHAR`` and ``NCHAR``, and the built-in summary repeats
    #: it — "Default and minimum size is 1 byte" for ``CHAR``). Measured: a bare
    #: ``CREATE TABLE t (a CHAR)`` is accepted on 21c and 23ai and reports
    #: ``DATA_LENGTH = 1``, ``CHAR_LENGTH = 1``; ``NCHAR`` reports
    #: ``CHAR_LENGTH = 1`` and ``DATA_LENGTH = 2`` (AL16UTF16). So one is a
    #: documented default here, not an invented width — which is exactly what
    #: ``VARCHAR2`` and ``NVARCHAR2`` do **not** have.
    _CHAR_DEFAULT_LENGTH = 1

    #: Oracle's binary precision for the bare ``FLOAT``, and the number
    #: ``format_data_type_float`` writes for a declaration that named none.
    #:
    #: Documented twice over. The built-in summary gives the range — ``FLOAT
    #: [(p)]`` — "a subtype of the ``NUMBER`` data type having precision ``p``.
    #: A ``FLOAT`` value is represented internally as ``NUMBER``. The precision
    #: ``p`` can range from 1 to 126 binary digits" — and the ANSI conversion
    #: table's note 2 says what the default is: "The ``FLOAT`` data type is a
    #: floating-point number with a binary precision ``b``. **The default
    #: precision for this data type is 126 binary, or 38 decimal.**" That table
    #: is also where ``DOUBLE PRECISION`` → ``FLOAT(126)`` and ``REAL`` →
    #: ``FLOAT(63)`` come from, which is why the two siblings write the two
    #: numbers and not one shared "float default".
    #:
    #: Measured rather than assumed, on **both** wired servers — 21c
    #: (21.0.0.0.0) and the 26ai Free server (23.26.1.0.0), which agree on
    #: every row::
    #:
    #:     CREATE TABLE t (c FLOAT)     -> DATA_TYPE='FLOAT'  DATA_PRECISION=126
    #:     CREATE TABLE t (c FLOAT(126))-> DATA_TYPE='FLOAT'  DATA_PRECISION=126
    #:     CREATE TABLE t (c REAL)      -> DATA_TYPE='FLOAT'  DATA_PRECISION=63
    #:     CREATE TABLE t (c DOUBLE PRECISION)
    #:                                 -> DATA_TYPE='FLOAT'  DATA_PRECISION=126
    #:
    #: So the number the catalog reports for a bare ``FLOAT`` **is** 126, the
    #: same figure this dialect renders. That was worth measuring instead of
    #: assuming: the renderer had been writing a literal that the vendor
    #: documents, and a literal that is documented is still not the same thing
    #: as a literal the server stores. The two agree here, so
    #: ``format_data_type_float`` keeps rendering ``FLOAT(126)`` and no existing
    #: rendering changes.
    #: https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    _FLOAT_DEFAULT_PRECISION = 126

    #: The ``FLOAT`` binary precision the ANSI conversion table maps ``REAL``
    #: to, which is what ``format_data_type_real`` writes. It is **not**
    #: ``BINARY_FLOAT``'s mantissa: ``FLOAT(p)`` is a ``NUMBER`` subtype,
    #: while ``BINARY_FLOAT`` is a separate 32-bit IEEE type with its own
    #: class (:class:`OracleBinaryFloatType`) and no precision parameter at
    #: all. The older comment here conflated the two, which is the same
    #: collapse finding F.4-4 of the plan appendix records.
    #: https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    _REAL_PRECISION = 63

    def type_parameter_defaults(self) -> Dict[str, Dict[str, object]]:
        """What Oracle supplies for a parameter a declaration left undeclared.

        **One entry, and ``float`` rather than ``char`` is the deliberate
        choice.** Both are documented defaults and both are stored and reported
        back, so both *could* be declared; what separates them is what Oracle
        does when the declaration is missing.

        ``float`` → ``precision`` 126. Oracle documents it (ANSI conversion
        table note 2: "The default precision for this data type is 126 binary, or
        38 decimal") and both wired servers store and report it:
        ``CREATE TABLE t (c FLOAT)`` gives ``DATA_TYPE='FLOAT'`` and
        ``DATA_PRECISION = 126`` on 21c (21.0.0.0.0) and on 26ai Free
        (23.26.1.0.0), which agree on every row of the probe. The introspector
        reads ``DATA_PRECISION`` for a ``FLOAT`` (``introspection/
        introspector.py``), so a bare ``FloatType(d)`` and the
        ``FloatType(d, precision=126)`` the catalog yields are the same column
        rather than two that render identically and compare unequal.

        **This is no longer inert:** ``FloatType.precision`` resolves against
        this declaration at read time, exactly as ``CharType.length`` resolves
        against its entry, so ``FloatType(d)`` carries 126 and compares equal to
        the ``FloatType(d, precision=126)`` the catalog yields.  The declaration
        and the property read the same number from the same place, which is what
        makes a bare ``FLOAT`` column and its own declaration one value object
        rather than two that render identically and compare unequal.

        ``char`` → **not declared, and that is a finding rather than an
        oversight.** Oracle *does* document 1 ("You can omit size from the
        column definition. The default value is 1"; "Default and minimum size is
        1 byte") and the server *does* accept a bare ``CHAR`` and store one
        character — measured, ``DATA_LENGTH = 1`` and ``CHAR_LENGTH = 1`` on both
        servers. So the declaration would be truthful and it is deliberately
        **not** written, because on Oracle it would change DDL that already
        works: :meth:`format_data_type_char` renders the bare word ``CHAR``, and
        resolving ``length`` to 1 makes it render ``CHAR(1)``. Those are the same
        column — the server reports the same two numbers for both — so the change
        is cosmetic, and rule 6 of the cross-repo contract ("rendered SQL must
        not change for any existing rendering") is not worth paying for. The
        asymmetry it would close is stated instead in
        :meth:`format_data_type_char`: ``CharType(d)`` and ``parse_type("CHAR")``
        render the same SQL and are not ``==``, which is a property of an absent
        width being unrepresentable in a type's identity rather than an Oracle
        fact. If the maintainer wants it, the entry is one line and the one
        rendering it changes is one.

        What is deliberately **not** declared, and why the three differ:

        ``varchar`` / ``nvarchar2`` — Oracle has no default for these at all.
        ``"You must specify size for a ``VARCHAR2``"``, and the server agrees
        with the summary on every release: ``CREATE TABLE t (c VARCHAR2)`` is
        ``ORA-00906: missing left parenthesis`` on both servers. The 4000 the
        formatter writes is this backend's ``MAX_STRING_SIZE = STANDARD`` ceiling
        chosen so the concept still renders — a decision, not a fact read back,
        and :meth:`_require_declared_length` refuses the same absence on the
        parse side for the mirror-image reason.

        ``interval`` — **does not belong in this mechanism at all**, and the
        reason is in the shape of the field rather than in the number. ``fields``
        is not a parameter but a member of a closed SQL:2016 qualifier
        vocabulary (:class:`~...expression.types.IntervalQualifier`), and there
        is no bare ``INTERVAL`` for Oracle to supply one for: ``CREATE TABLE t
        (c INTERVAL)`` is ``ORA-30089: missing or invalid <datetime field>`` on
        both servers. Oracle's two interval types must be spelled —
        ``INTERVAL YEAR TO MONTH`` and ``INTERVAL DAY TO SECOND`` — and each
        carries its own *two* documented precisions (``day_precision`` "The
        default is 2", ``fractional_seconds_precision`` "The default is 6"; NLSPG
        4.2.2.2), which the catalog confirms as ``INTERVAL DAY(2) TO SECOND(6)``
        and ``INTERVAL YEAR(2) TO MONTH``. So the value the renderer writes is a
        **whole qualifier** this backend chose, not a default a server filled in,
        and folding it into a mechanism whose entries are per-parameter would
        pretend one parameter answers it. See :meth:`format_data_type_interval`,
        which now says so with the measurements attached.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html
        """
        return {
            "float": {"precision": self._FLOAT_DEFAULT_PRECISION},
        }

    @staticmethod
    def _oracle_declared_length(stripped: str) -> Optional[int]:
        """The ``(n)`` Oracle wrote after a type name, or ``None``.

        Deliberately loose about what follows the digits: the catalog spells
        the size ``VARCHAR2(4000 BYTE)`` as well as ``VARCHAR2(4000)``, and
        the semantics suffix is not part of the length.
        """
        match = re.search(r"\((\d+)", stripped)
        return int(match.group(1)) if match else None

    def _require_declared_length(
        self, stripped: str, word: str, example: str
    ) -> int:
        """The declared ``(n)``, or a refusal naming Oracle's requirement.

        The standard this applies is the one ``RAW`` already meets. Oracle's
        built-in summary says, in its own words for each type:

        * ``RAW (size)`` — "You must specify size for a ``RAW`` value";
        * ``VARCHAR2 (size [BYTE | CHAR])`` — "You must specify size for
          ``VARCHAR2``";
        * ``NVARCHAR2 (size)`` — "You must specify size for ``NVARCHAR2``";
        * ``CHAR [(size [BYTE | CHAR])]`` — "Default and minimum size is 1 byte";
        * ``NCHAR [(size)]`` — "Default and minimum size is 1 character".

        The server agrees with the summary on every one of them, measured on 21c
        and 23ai:

        ====================================  ==========================  ===================
        ``CREATE TABLE t (a …)``             21c                         23ai / 26ai
        ====================================  ==========================  ===================
        ``VARCHAR2``                         ``ORA-00906``               ``ORA-00906``
        ``NVARCHAR2``                        ``ORA-00906``               ``ORA-00906``
        ``RAW``                              ``ORA-00906``               ``ORA-00906``
        ``CHAR``                             accepted, ``LENGTH 1``     accepted, ``LENGTH 1``
        ``NCHAR``                            accepted, ``CHAR_LEN 1``   accepted, ``CHAR_LEN 1``
        ====================================  ==========================  ===================

        So three of the five have **no** default and a fallback there is a
        width the catalog never reported — the same defect the ``RAW`` branch
        above used to have, where Oracle's 2000-byte
        ``MAX_STRING_SIZE = STANDARD`` *ceiling* was handed back as if it were a
        declared ``RAW(16)``'s width. The fallback is gone rather than merely
        unreachable: ``_parse_columns`` spells the size out for every character
        type (``introspection/introspector.py`` reads ``CHAR_LENGTH`` or
        ``DATA_LENGTH``), so an un-sized string cannot arrive from
        introspection, and a caller who writes one by hand has typed a column
        Oracle will not create.

        The fourth — ``CHAR``/``NCHAR`` — is different in kind rather than in
        degree, and keeps its size: one is Oracle's *documented default*, so
        returning it states what the column would be (``CHAR(1)``) instead of
        guessing. That is why the same answer is right for ``CHAR`` and wrong
        for ``VARCHAR2``.
        https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
        """
        length = self._oracle_declared_length(stripped)
        if length is not None:
            return length
        raise ValueError(
            f"cannot parse an un-sized {word} (Oracle requires a size: "
            f"'You must specify size for a {word} value', and the server "
            f"rejects a bare {word} with ORA-00906 — a bare CHAR is different, "
            f"Oracle documents a default size of "
            f"{self._CHAR_DEFAULT_LENGTH} for it); pass the width, e.g. "
            f"'{example}'"
        )

    def _refuse_unsigned_type_string(self, raw: str) -> None:
        """Refuse a type string that carries an ``UNSIGNED`` attribute.

        :meth:`_refuse_unsigned_numeric` closes the door on the way *in* — a
        declaration.  This closes the door on the way *out*: a string.  The same
        field, the same loss, in the other direction, and it is a real hole
        because ``parse_type`` is the reader for three sources, not one — Oracle's
        own ``user_tab_columns`` rows, **and** caller-supplied DDL such as a dump
        or a hand-written migration that nobody has run against a server.

        Dropping the attribute here would hand back a *signed* value object for a
        declaration that says ``UNSIGNED``, and ``==`` would call it equal to the
        signed one — the differ would report no change for the exact change the
        string describes.  So it is refused by name, on the same reasoning as
        :meth:`_refuse_unsigned_numeric`, which is where the vendor citation
        lives.

        Note what is *not* claimed here: no Oracle server can have written this
        string. It was measured that the parser rejects it — ``CREATE TABLE t (c
        NUMBER(10,2) UNSIGNED)`` is ``ORA-00907: missing right parenthesis`` on
        21c and ``ORA-03062: missing comma or right parenthesis`` on 26ai Free —
        so this branch guards the *caller-supplied* path, and says so rather than
        pretending a catalog row is at stake.
        """
        raise UnsupportedFeatureError(
            self.name,
            f"an UNSIGNED attribute in a type string ({raw!r}) "
            f"(Oracle has no unsigned numeric type, so no declaration and no "
            f"catalog row can carry UNSIGNED: CREATE TABLE's column_definition "
            f"admits no modifier after the type name, the reference chapter "
            f"says the numeric types 'store positive and negative fixed and "
            f"floating-point numbers', and the parser rejects the string at the "
            f"word itself -- ORA-00907: missing right parenthesis on 21c, "
            f"ORA-03062: missing comma or right parenthesis on 26ai Free. "
            f"Reading the attribute off and returning a signed type would compare "
            f"equal to the signed column, which is the silent loss this refuses.)",
            suggestion=(
                "Declare the column signed and enforce the range with a CHECK "
                "constraint; Oracle accepts 'c NUMBER(10,2) CHECK (c >= 0)'."
            ),
        )

    def parse_type(self, raw: str) -> DataType:
        stripped = raw.strip()
        upper = stripped.upper()

        if self._UNSIGNED_ATTRIBUTE.search(stripped):
            self._refuse_unsigned_type_string(stripped)

        if self._ORACLE_NUMBER_TYPES.match(upper):
            if upper.startswith("BINARY_FLOAT"):
                # Oracle's two IEEE types are not the NUMBER family, so they
                # are classes of their own rather than FloatType/DoubleType:
                # those render FLOAT(p), the catalog keeps BINARY_FLOAT /
                # BINARY_DOUBLE apart, and collapsing them made a write-back
                # name different storage (finding F.4-4).
                return OracleBinaryFloatType(dialect=self)
            if upper.startswith("BINARY_DOUBLE"):
                return OracleBinaryDoubleType(dialect=self)
            if upper.startswith("FLOAT"):
                nums = re.findall(r"\d+", stripped)
                if nums:
                    return FloatType(dialect=self, precision=int(nums[0]))
                return FloatType(dialect=self)
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                p, s = int(nums[0]), int(nums[1])
                # NUMBER(10) -> integer, NUMBER(5) -> smallint,
                # NUMBER(19) -> bigint. The *core* concepts, deliberately:
                # ``format_data_type_integer`` / ``_smallint`` / ``_bigint``
                # already write these three words, so an Oracle-namespaced class
                # would render identical SQL under a second name — and
                # ``DataType.__eq__`` is ``type(self) is type(other)``, so the
                # declaration and the catalog row would compare unequal. See
                # this class's docstring.
                if s == 0 and p == 10:
                    return IntegerType(dialect=self)
                if s == 0 and p == 5:
                    return SmallIntType(dialect=self)
                if s == 0 and p == 19:
                    return BigIntType(dialect=self)
                return DecimalType(dialect=self, precision=p, scale=s)
            if len(nums) == 1:
                p = int(nums[0])
                if p == 10:
                    return IntegerType(dialect=self)
                if p == 5:
                    return SmallIntType(dialect=self)
                if p == 19:
                    return BigIntType(dialect=self)
                return DecimalType(dialect=self, precision=p)
            return DecimalType(dialect=self)

        # BLOB types checked before string types so "LONG RAW" matches the
        # binary branch rather than being consumed by the "LONG" string rule.
        if self._ORACLE_BLOB_TYPES.match(upper):
            if "LONG RAW" in upper:
                return OracleLongRawType(dialect=self)
            if "RAW" in upper:
                # Oracle has no un-sized RAW at all: the built-in summary says
                # "You must specify size for a RAW value", and the server agrees
                # — ``CREATE TABLE t (a RAW)`` is ``ORA-00906: missing left
                # parenthesis`` on 21c and 23c. So a bare ``RAW`` here is not a
                # column Oracle could have stored, and every width this method
                # could return would be one the catalog never reported.
                #
                # That used to be papered over with a guess. The introspector
                # spelled out the length for NUMBER/FLOAT and the character
                # types and not for RAW, so every RAW column arrived here
                # unsized; the guess was Oracle's 2000-byte
                # ``MAX_STRING_SIZE = STANDARD`` ceiling, which made an
                # introspected ``RAW(16)`` report 2000 bytes. ``DATA_LENGTH`` is
                # now read for RAW (``_parse_columns`` in
                # ``introspection/introspector.py``; it holds the declared width
                # exactly — 16 for a ``RAW(16)``, 64 for a ``RAW(64)``), so the
                # guess is gone rather than merely unreachable, and what is left
                # is a refusal. A width that cannot be read must not be claimed.
                length = self._oracle_declared_length(stripped)
                if length is None:
                    raise ValueError(
                        "cannot parse an un-sized RAW (Oracle requires a size: "
                        "'You must specify size for a RAW value', and the "
                        "server rejects a bare RAW with ORA-00906); pass the "
                        "width, e.g. 'RAW(16)'"
                    )
                return OracleRawType(dialect=self, length=length)
            # BLOB is the word ``format_data_type_blob`` writes for core
            # ``BlobType``, so the core class is what comes back. A second class
            # for it rendered the same SQL under a prefixed name and made a
            # declared ``blob`` and an introspected ``BLOB`` compare unequal.
            return BlobType(dialect=self)

        if self._ORACLE_STRING_TYPES.match(upper):
            if "NCLOB" in upper:
                return OracleNClobType(dialect=self)
            if "CLOB" in upper:
                # CLOB is the unbounded-string concept under Oracle's own
                # name, i.e. the ``clob`` spelling of TextType — not a class
                # of its own. Round-trips back to ``CLOB``.
                return TextType(dialect=self, spelling="clob")
            if upper.startswith("NVARCHAR2"):
                return OracleNVarChar2Type(
                    dialect=self,
                    length=self._require_declared_length(
                        stripped, "NVARCHAR2", "NVARCHAR2(30)"),
                )
            if upper.startswith(("VARCHAR2", "VARCHAR",
                                "CHARACTER VARYING", "CHAR VARYING")):
                return VarCharType(
                    dialect=self,
                    length=self._require_declared_length(
                        stripped, "VARCHAR2", "VARCHAR2(4000)"),
                )
            if upper.startswith("LONG"):
                return OracleLongType(dialect=self)
            # NCHAR, CHARACTER and CHAR are all Oracle's fixed-length,
            # blank-padded type — including the standard's long form, which
            # Oracle's own grammar accepts as a synonym for CHAR. Unlike
            # VARCHAR2/NVARCHAR2 the size here is genuinely optional: see
            # ``_CHAR_DEFAULT_LENGTH``.
            return CharType(
                dialect=self,
                length=self._oracle_declared_length(stripped)
                or self._CHAR_DEFAULT_LENGTH,
            )

        if self._ORACLE_BOOLEAN_TYPES.match(upper):
            # BOOLEAN arrived as a SQL column type in 23ai, so a catalog that
            # says it is a 23ai-or-later catalog and the column is the boolean
            # concept in full — not a number standing in for one. Both words
            # Oracle accepts are read: ``CREATE TABLE t (a BOOL)`` and
            # ``CREATE TABLE t (a BOOLEAN)`` both report ``DATA_TYPE =
            # 'BOOLEAN'`` on 23ai/26ai (measured), and ``BOOL`` is documented
            # as an abbreviation of ``BOOLEAN``. Both are ``ORA-00902`` on 21c,
            # so neither can arrive from a pre-23ai catalog either.
            # https://docs.oracle.com/en/learn/db23ai-sql-features/index.html
            return BooleanType(dialect=self)

        if self._ORACLE_DATE_TYPES.match(upper):
            if "TIMESTAMP" in upper:
                # The two time-zone forms are different columns — WITH TIME
                # ZONE stores the offset with each value, WITH LOCAL TIME ZONE
                # stores none and normalises to the database time zone — so
                # they do not share a class: LOCAL renders through its own
                # class (finding F.4-3) and the plain form stays on the core
                # TimestampTzType.  "WITH LOCAL TIME ZONE" is checked first so
                # a future edit cannot let the shorter phrase claim its word.
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH LOCAL TIME ZONE" in upper:
                    return OracleTimestampLtzType(dialect=self, precision=precision)
                if "WITH TIME ZONE" in upper:
                    return TimestampTzType(dialect=self, precision=precision)
                return TimestampType(dialect=self, precision=precision)
            if "INTERVAL" in upper:
                # Oracle has two of them and they are different types:
                # INTERVAL YEAR TO MONTH counts years and months and nothing
                # smaller, INTERVAL DAY TO SECOND counts days, hours,
                # minutes and fractional seconds and nothing larger. The
                # qualifier is what says which, so it is carried into the
                # core IntervalType rather than dropped — and it round-trips,
                # so a column introspected as INTERVAL DAY(2) TO SECOND(6)
                # renders back as exactly that.
                fields = re.sub(r"^INTERVAL\s+", "", stripped,
                                flags=re.IGNORECASE).strip()
                return IntervalType(dialect=self, fields=fields or None)
            return DateType(dialect=self)

        if self._ORACLE_XML_TYPES.match(upper):
            # Oracle's XML type is the XML concept, not text: the value is an
            # infoset the server parses and can validate. It has exactly one
            # spelling here, so there is no Oracle-namespaced class for it —
            # core XmlType rendered by format_data_type_xml is the whole of it.
            return XmlType(dialect=self)

        return CustomType(dialect=self, raw=stripped)


class OracleTypeSuggestionMixin:
    """Oracle-native ``suggest_column_type()``.

    Oracle has no ``UUID`` **data type** at all — not on the 18c/21c/23c
    servers this backend targets, and not on 26ai either; 23ai added the
    ``UUID()`` *function*, and its own reference page says that function
    "returns a version 4 variant 1 UUID as a ``RAW(16)`` value". So UUIDs
    default to ``OracleRawType(length=16)`` — the 16 RFC 4122 bytes, matching
    the ``OracleUUIDAdapter`` bytes path and the same convention as
    MySQL/MariaDB ``BINARY(16)``. That is also
    :attr:`OracleRawType.DEFAULT_LENGTH`, so the same answer comes back
    whether a caller spells the length out or not, and it is what
    ``suggested_data_types()`` gives for the generic ``UUIDType``.
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/uuid.html

    ``dict``/``list`` are version-gated: native JSON requires Oracle 21c+
    (``JsonType``); older servers fall back to ``TextType(spelling="clob")``
    (CLOB, no size limit). Version-unknown returns ``None`` (no guessing).
    """


    def suggest_column_type(
        self, python_type: type, version: "Optional[Tuple[int, int, int]]" = None
    ) -> "Optional[DataType]":
        import datetime as _dt
        import decimal as _dec
        import enum as _enum
        import uuid as _uuid

        if version is None:
            version = getattr(self, "_version", None)

        mapping = {
            # Core concepts, because the core formatter already writes the
            # Oracle word for each of them: ``VARCHAR2(n)``, ``NUMBER(10)``,
            # ``BLOB``. Naming an Oracle-namespaced class here would render the
            # same SQL under a second name, and a suggestion is something a
            # caller builds — so the class it hands back has to be the one the
            # catalog will hand back too.
            str: VarCharType,
            int: IntegerType,
            bool: BooleanType,
            float: DoubleType,
            bytes: BlobType,
            _dt.datetime: DateTimeType,
            _dt.date: DateType,
            _dt.time: TimeType,
            _dec.Decimal: DecimalType,
            _uuid.UUID: OracleRawType,
            _enum.Enum: VarCharType,
        }
        if python_type is _uuid.UUID:
            return OracleRawType(dialect=self, length=16)
        factory = mapping.get(python_type)
        if factory is not None:
            if python_type is _enum.Enum:
                return VarCharType(dialect=self, length=64)
            if python_type is str:
                return VarCharType(dialect=self, length=255)
            return factory()

        if python_type in (dict, list):
            if version is None:
                return None
            if version >= (21, 0, 0):
                return JsonType(dialect=self)
            return TextType(dialect=self, spelling="clob")

        return None
