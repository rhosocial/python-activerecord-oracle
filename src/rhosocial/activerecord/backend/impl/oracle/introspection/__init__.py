# src/rhosocial/activerecord/backend/impl/oracle/introspection/__init__.py
"""
Oracle introspection package.

Provides:
  SyncOracleIntrospector   — synchronous introspector for Oracle databases
  AsyncOracleIntrospector  — asynchronous introspector for Oracle databases
  OracleSequenceInfo       — value object for one ALL_SEQUENCES row
  OracleSynonymInfo        — value object for one ALL_SYNONYMS row
  OraclePackageInfo        — value object for one ALL_OBJECTS package row
  SyncOracleStatusIntrospector  — synchronous Oracle status introspector
  AsyncOracleStatusIntrospector — asynchronous Oracle status introspector
"""

from .introspector import (
    SyncOracleIntrospector,
    AsyncOracleIntrospector,
)
from .types import (
    OraclePackageInfo,
    OracleSequenceInfo,
    OracleSynonymInfo,
)
from .status_introspector import (
    SyncOracleStatusIntrospector,
    AsyncOracleStatusIntrospector,
)
from .executor import (
    SyncOracleIntrospectorExecutor,
    AsyncOracleIntrospectorExecutor,
)

__all__ = [
    "SyncOracleIntrospector",
    "AsyncOracleIntrospector",
    "OraclePackageInfo",
    "OracleSequenceInfo",
    "OracleSynonymInfo",
    "SyncOracleStatusIntrospector",
    "AsyncOracleStatusIntrospector",
]
