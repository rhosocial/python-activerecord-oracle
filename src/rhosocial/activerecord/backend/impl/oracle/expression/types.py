# src/rhosocial/activerecord/backend/impl/oracle/expression/types.py
"""Oracle-specific DDL DataType subclasses for DDL operations."""

from rhosocial.activerecord.backend.expression.types import IntegerType, VarCharType


class OracleIntegerType(IntegerType):
    """Oracle INTEGER — mapped to NUMBER(10)."""
    pass


class OracleVarChar2Type(VarCharType):
    """Oracle VARCHAR2 type."""
    pass


class OracleClobType(VarCharType):
    """Oracle CLOB for large text storage."""
    pass


class OracleNClobType(VarCharType):
    """Oracle NCLOB for Unicode large text storage."""
    pass


class OracleNVarChar2Type(VarCharType):
    """Oracle NVARCHAR2 type."""
    pass


class OracleRawType(VarCharType):
    """Oracle RAW type for binary data."""
    pass


class OracleLongType(VarCharType):
    """Oracle LONG type (deprecated, use CLOB)."""
    pass


class OracleLongRawType(VarCharType):
    """Oracle LONG RAW type (deprecated, use BLOB)."""
    pass


class OracleXmlType(VarCharType):
    """Oracle XMLType."""
    pass


__all__ = [
    "OracleIntegerType",
    "OracleVarChar2Type",
    "OracleClobType",
    "OracleNClobType",
    "OracleNVarChar2Type",
    "OracleRawType",
    "OracleLongType",
    "OracleLongRawType",
    "OracleXmlType",
]