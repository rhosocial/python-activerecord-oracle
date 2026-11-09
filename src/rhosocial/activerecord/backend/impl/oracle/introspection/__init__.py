# src/rhosocial/activerecord/backend/impl/oracle/introspection/__init__.py
"""
Oracle introspection package.

Provides:
  OracleIntrospector   — synchronous introspector for Oracle databases
  AsyncOracleIntrospector  — asynchronous introspector for Oracle databases
  OracleSequenceInfo       — value object for one ALL_SEQUENCES row
  OracleSynonymInfo        — value object for one ALL_SYNONYMS row
  OraclePackageInfo        — value object for one ALL_OBJECTS package row
  OraclePartitionInfo      — value object for one ALL_TAB_PARTITIONS row
  OracleRoutineInfo        — value object for one standalone procedure/function
  OracleTypeInfo           — value object for one named type
  OracleConstraintInfo     — value object for one ALL_CONSTRAINTS row
  OracleColumnDataProfile  — one column's profile, from ALL_TAB_COLUMNS
  OracleStatusIntrospector  — synchronous Oracle status introspector
  AsyncOracleStatusIntrospector — asynchronous Oracle status introspector
"""

from .introspector import (
    OracleIntrospector,
    AsyncOracleIntrospector,
)
from .types import (
    OracleColumnDataProfile,
    OracleConstraintInfo,
    OraclePackageInfo,
    OraclePartitionInfo,
    OraclePartitionStrategy,
    OracleRoutineInfo,
    OracleRoutineParameter,
    OracleSequenceInfo,
    OracleSynonymInfo,
    OracleTypeAttributeInfo,
    OracleTypeInfo,
)
from .status_introspector import (
    OracleStatusIntrospector,
    AsyncOracleStatusIntrospector,
)
from .executor import (
    OracleIntrospectorExecutor,
    AsyncOracleIntrospectorExecutor,
)

__all__ = [
    "OracleIntrospector",
    "AsyncOracleIntrospector",
    "OracleColumnDataProfile",
    "OracleConstraintInfo",
    "OraclePackageInfo",
    "OraclePartitionInfo",
    "OraclePartitionStrategy",
    "OracleRoutineInfo",
    "OracleRoutineParameter",
    "OracleSequenceInfo",
    "OracleSynonymInfo",
    "OracleTypeAttributeInfo",
    "OracleTypeInfo",
    "OracleStatusIntrospector",
    "AsyncOracleStatusIntrospector",
    "OracleIntrospectorExecutor",
    "AsyncOracleIntrospectorExecutor",
]
