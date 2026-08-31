# src/rhosocial/activerecord/backend/impl/oracle/expression/types.py
"""Oracle-specific DDL DataType subclasses.

Naming convention
-----------------
Oracle-specific types use the ``Oracle`` prefix to distinguish them from
the core types (which have no prefix).  This avoids ambiguity when both
core and backend types are used together.

Usage scope
-----------
These types are used **only** for Oracle backend DDL column definitions,
introspection result parsing, and schema comparison.  They should **not**
be used by application code directly — always use the core types for
DDL definition expressions (``ColumnDefinition.data_type``).

Backend registration
--------------------
Every Oracle-specific ``DataType`` subclass is registered with
``backend="oracle"`` so that ``dialect.format_data_type()`` dispatches to
the matching ``format_data_type_*`` formatter declared in
``OracleTypeSupportMixin`` (see ``mixins/types.py``).
"""

from __future__ import annotations

from typing import Optional, Set

from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BlobType,
    IntegerType,
    SmallIntType,
    TextType,
    VarCharType,
)


# ---------------------------------------------------------------------------
# Integer variants
# ---------------------------------------------------------------------------

class OracleIntegerType(IntegerType):
    """Oracle ``INTEGER`` — mapped to ``NUMBER(10)``."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'IntegerType'}


class OracleSmallIntType(SmallIntType):
    """Oracle ``SMALLINT`` — mapped to ``NUMBER(5)``."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SmallIntType'}


class OracleBigIntType(BigIntType):
    """Oracle ``BIGINT`` — mapped to ``NUMBER(19)``."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BigIntType'}


# ---------------------------------------------------------------------------
# Character string variants
# ---------------------------------------------------------------------------

class OracleVarChar2Type(VarCharType):
    """Oracle ``VARCHAR2(n)`` — variable-length byte/char string."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'VarCharType'}


class OracleNVarChar2Type(VarCharType):
    """Oracle ``NVARCHAR2(n)`` — Unicode variable-length string."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'VarCharType'}


class OracleCharType(VarCharType):
    """Oracle ``CHAR(n)`` — fixed-length string.

    Inherits from ``VarCharType`` to reuse the ``length`` parameter; the
    dedicated formatter renders ``CHAR(n)``.
    """

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'CharType'}


# ---------------------------------------------------------------------------
# Large object / long string variants
# ---------------------------------------------------------------------------

class OracleClobType(TextType):
    """Oracle ``CLOB`` — character large object."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'TextType'}


class OracleNClobType(TextType):
    """Oracle ``NCLOB`` — national character large object."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'TextType'}


class OracleLongType(TextType):
    """Oracle ``LONG`` — deprecated large string (use CLOB)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'TextType'}


class OracleXmlType(TextType):
    """Oracle ``XMLType`` — XML document storage."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'TextType'}


# ---------------------------------------------------------------------------
# Binary variants
# ---------------------------------------------------------------------------

class OracleRawType(BlobType):
    """Oracle ``RAW(n)`` — variable-length binary."""

    length: Optional[int] = None

    def __init__(self, dialect=None, *, length: Optional[int] = None):
        super().__init__(dialect)
        self.length = length

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.length == other.length

    def __hash__(self) -> int:
        return hash((type(self), self.length))

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}


class OracleLongRawType(BlobType):
    """Oracle ``LONG RAW`` — deprecated large binary (use BLOB)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}


class OracleBlobType(BlobType):
    """Oracle ``BLOB`` — binary large object."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}


__all__ = [
    "OracleIntegerType",
    "OracleSmallIntType",
    "OracleBigIntType",
    "OracleVarChar2Type",
    "OracleNVarChar2Type",
    "OracleCharType",
    "OracleClobType",
    "OracleNClobType",
    "OracleLongType",
    "OracleXmlType",
    "OracleRawType",
    "OracleLongRawType",
    "OracleBlobType",
]
