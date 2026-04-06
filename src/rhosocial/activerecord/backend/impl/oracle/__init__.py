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
"""

from .backend import OracleBackend
from .async_backend import AsyncOracleBackend
from .config import OracleConnectionConfig
from .dialect import OracleDialect
from .transaction import OracleTransactionManager
from .async_transaction import AsyncOracleTransactionManager

__all__ = [
    # Synchronous Backend
    'OracleBackend',

    # Asynchronous Backend
    'AsyncOracleBackend',

    # Configuration
    'OracleConnectionConfig',

    # Dialect related
    'OracleDialect',

    # Transaction - Sync and Async
    'OracleTransactionManager',
    'AsyncOracleTransactionManager',
]
