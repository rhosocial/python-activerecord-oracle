# src/rhosocial/activerecord/backend/impl/oracle/show/__init__.py
"""
Oracle data dictionary introspection module.

Mirrors the structure of the MySQL `show/` package but emits SELECT
statements against Oracle's data dictionary views
(`USER_*`, `ALL_*`, `DBA_*`, `V$*`, `NLS_*`) instead of MySQL `SHOW`
commands.

Usage:
    result = backend.show().sessions(active_only=True)
    info = backend.show().database_info()
    objs = backend.show().objects(object_type='TABLE')
"""

from .types import (
    OracleSessionInfo,
    OracleProcessInfo,
    OracleSqlInfo,
    OracleDatabaseInfo,
    OracleInstanceInfo,
    OracleObjectInfo,
    OracleTableInfo,
    OracleColumnInfo,
    OracleIndexInfo,
    OracleConstraintInfo,
    OracleLockInfo,
    OracleWaitEventInfo,
    OracleNlsParameter,
    OracleIntrospectionResult,
)
from .expressions import (
    OracleQueryExpression,
    OracleQuerySessionsExpression,
    OracleQueryRunningSQLExpression,
    OracleQueryDatabaseInfoExpression,
    OracleQueryInstanceInfoExpression,
    OracleQueryObjectsExpression,
    OracleQueryLocksExpression,
    OracleQueryWaitEventsExpression,
    OracleQueryNlsParametersExpression,
)
from .dialect import OracleShowDialectMixin
from .functionality import (
    OracleShowFunctionality,
    AsyncOracleShowFunctionality,
    QUERY_LAMBDAS,
)
from .backend_mixin import OracleShowMixin, AsyncOracleShowMixin

__all__ = [
    # Row dataclasses
    "OracleSessionInfo",
    "OracleProcessInfo",
    "OracleSqlInfo",
    "OracleDatabaseInfo",
    "OracleInstanceInfo",
    "OracleObjectInfo",
    "OracleTableInfo",
    "OracleColumnInfo",
    "OracleIndexInfo",
    "OracleConstraintInfo",
    "OracleLockInfo",
    "OracleWaitEventInfo",
    "OracleNlsParameter",
    "OracleIntrospectionResult",
    # Expression classes
    "OracleQueryExpression",
    "OracleQuerySessionsExpression",
    "OracleQueryRunningSQLExpression",
    "OracleQueryDatabaseInfoExpression",
    "OracleQueryInstanceInfoExpression",
    "OracleQueryObjectsExpression",
    "OracleQueryLocksExpression",
    "OracleQueryWaitEventsExpression",
    "OracleQueryNlsParametersExpression",
    # Dialect mixin
    "OracleShowDialectMixin",
    # Functionality wrappers
    "OracleShowFunctionality",
    "AsyncOracleShowFunctionality",
    "QUERY_LAMBDAS",
    # Backend mixins
    "OracleShowMixin",
    "AsyncOracleShowMixin",
]
