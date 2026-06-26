# src/rhosocial/activerecord/backend/impl/oracle/__init__.py
"""
Oracle backend implementation for the Python ORM.

This module provides:
- Oracle synchronous backend with connection management and query execution
- Oracle asynchronous backend with async/await support (via oracledb thin mode)
- Oracle-specific connection configuration
- Type mapping and value conversion
- Transaction management with savepoint support (sync and async)
- Oracle dialect and expression handling

Architecture:
- OracleBackend: Synchronous implementation using oracledb
- AsyncOracleBackend: Asynchronous implementation using oracledb (thin mode)
- Independent from ORM frameworks - uses only native drivers

Subpackages:
- explain: EXPLAIN result types
- types: Oracle-specific type definitions
- expression: Oracle-specific SQL expressions (including DDL DataType subclasses)
- functions: Oracle function factories
- mixins: Feature-specific mixin classes
- schema: Schema differ support
"""

from .backend import OracleBackend
from .async_backend import AsyncOracleBackend
from .config import OracleConnectionConfig
from .collation import OracleCollation
from .dialect import OracleDialect
from .transaction import OracleTransactionManager
from .async_transaction import AsyncOracleTransactionManager

from .adapters import (
    OracleBooleanAdapter,
    OracleDateTimeAdapter,
    OracleDateAdapter,
    OracleTimeAdapter,
    OracleDecimalAdapter,
    OracleJSONAdapter,
    OracleBytesAdapter,
)

from .explain import OracleExplainResult, OracleExplainRow

from .mixins import (
    OracleTransactionMixin,
    OracleBackendMixin,
    OracleConcurrencyMixin,
    OracleTypeSupportMixin,
)

from .expression.types import (
    OracleIntegerType,
    OracleVarChar2Type,
    OracleClobType,
    OracleNClobType,
    OracleNVarChar2Type,
    OracleRawType,
    OracleLongType,
    OracleLongRawType,
    OracleXmlType,
)

from .schema import OracleSchemaDiffer

from .type_compatibility import (
    DIRECT_COMPATIBLE_CASTS,
    check_cast_compatibility,
    get_compatible_types,
)

__all__ = [
    # Backend classes
    "OracleBackend",
    "AsyncOracleBackend",

    # Configuration
    "OracleConnectionConfig",

    # Dialect
    "OracleDialect",
    "OracleCollation",

    # Transaction management
    "OracleTransactionManager",
    "AsyncOracleTransactionManager",

    # Type adapters
    "OracleBooleanAdapter",
    "OracleDateTimeAdapter",
    "OracleDateAdapter",
    "OracleTimeAdapter",
    "OracleDecimalAdapter",
    "OracleJSONAdapter",
    "OracleBytesAdapter",

    # EXPLAIN
    "OracleExplainResult",
    "OracleExplainRow",

    # Mixins
    "OracleTransactionMixin",
    "OracleBackendMixin",
    "OracleConcurrencyMixin",
    "OracleTypeSupportMixin",

    # DDL DataType subclasses
    "OracleIntegerType",
    "OracleVarChar2Type",
    "OracleClobType",
    "OracleNClobType",
    "OracleNVarChar2Type",
    "OracleRawType",
    "OracleLongType",
    "OracleLongRawType",
    "OracleXmlType",

    # Schema differ
    "OracleSchemaDiffer",

    # Type compatibility
    "DIRECT_COMPATIBLE_CASTS",
    "check_cast_compatibility",
    "get_compatible_types",
]