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
- expression: Oracle-specific SQL expressions
- functions: Oracle function factories
"""

from .backend import OracleBackend
from .async_backend import AsyncOracleBackend
from .config import OracleConnectionConfig
from .dialect import OracleDialect
from .transaction import OracleTransactionManager
from .async_transaction import AsyncOracleTransactionManager

# Type adapters
from .adapters import (
    OracleBooleanAdapter,
    OracleDateTimeAdapter,
    OracleDateAdapter,
    OracleTimeAdapter,
    OracleDecimalAdapter,
    OracleJSONAdapter,
    OracleBytesAdapter,
)

__all__ = [
    # Backend classes
    'OracleBackend',
    'AsyncOracleBackend',
    
    # Configuration
    'OracleConnectionConfig',
    
    # Dialect
    'OracleDialect',
    
    # Transaction management
    'OracleTransactionManager',
    'AsyncOracleTransactionManager',
    
    # Type adapters
    'OracleBooleanAdapter',
    'OracleDateTimeAdapter',
    'OracleDateAdapter',
    'OracleTimeAdapter',
    'OracleDecimalAdapter',
    'OracleJSONAdapter',
    'OracleBytesAdapter',
]
