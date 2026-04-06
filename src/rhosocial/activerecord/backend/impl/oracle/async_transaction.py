# src/rhosocial/activerecord/backend/impl/oracle/async_transaction.py
"""Oracle asynchronous transaction manager implementation."""
import logging

from rhosocial.activerecord.backend.errors import TransactionError
from rhosocial.activerecord.backend.transaction import (
    IsolationLevel,
    AsyncTransactionManager,
    TransactionMode,
    TransactionState,
)
from .mixins import OracleTransactionMixin


class AsyncOracleTransactionManager(OracleTransactionMixin, AsyncTransactionManager):
    """Oracle asynchronous transaction manager implementation."""

    _ISOLATION_LEVELS = OracleTransactionMixin._ISOLATION_LEVELS

    def __init__(self, connection, logger=None):
        """Initialize async Oracle transaction manager."""
        super().__init__(connection, logger)
        self._isolation_level = IsolationLevel.READ_COMMITTED
        self._state = TransactionState.INACTIVE

    async def _ensure_connection_ready(self):
        """Ensure connection is ready for async transaction operations."""
        if not self._connection:
            error_msg = "No valid connection for transaction"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg)

    async def _do_begin(self) -> None:
        """Begin Oracle transaction asynchronously."""
        await self._ensure_connection_ready()

        try:
            # Oracle starts transactions implicitly on first DML
            # Set isolation level if needed (only for SERIALIZABLE)
            # READ_ONLY is handled separately as a transaction mode

            # Handle isolation level
            if self._isolation_level == IsolationLevel.SERIALIZABLE:
                cursor = self._connection.cursor()
                await cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                await cursor.close()

            # Handle transaction mode (READ_ONLY vs READ_WRITE)
            if self._transaction_mode == TransactionMode.READ_ONLY:
                cursor = self._connection.cursor()
                await cursor.execute("SET TRANSACTION READ ONLY")
                await cursor.close()
            # READ_WRITE is the default, no need to set explicitly

            self._state = TransactionState.ACTIVE

            isolation_str = self._ISOLATION_LEVELS.get(self._isolation_level, "READ COMMITTED")
            mode_str = "READ ONLY" if self._transaction_mode == TransactionMode.READ_ONLY else "READ WRITE"
            self.log(logging.DEBUG, f"Started async Oracle transaction with isolation level {isolation_str}, mode {mode_str}")
        except Exception as e:
            error_msg = f"Failed to begin async transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    async def _do_commit(self) -> None:
        """Commit Oracle transaction asynchronously."""
        await self._ensure_connection_ready()

        try:
            await self._connection.commit()
            self._state = TransactionState.COMMITTED
            self.log(logging.DEBUG, "Committed async Oracle transaction")
        except Exception as e:
            error_msg = f"Failed to commit async transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    async def _do_rollback(self) -> None:
        """Rollback Oracle transaction asynchronously."""
        await self._ensure_connection_ready()

        try:
            await self._connection.rollback()
            self._state = TransactionState.ROLLED_BACK
            self.log(logging.DEBUG, "Rolled back async Oracle transaction")
        except Exception as e:
            error_msg = f"Failed to rollback async transaction: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    async def _do_create_savepoint(self, name: str) -> None:
        """Create Oracle savepoint asynchronously."""
        await self._ensure_connection_ready()

        try:
            cursor = self._connection.cursor()
            await cursor.execute(f"SAVEPOINT {name}")
            await cursor.close()
            self.log(logging.DEBUG, f"Created savepoint (async): {name}")
        except Exception as e:
            error_msg = f"Failed to create savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    async def _do_release_savepoint(self, name: str) -> None:
        """Release Oracle savepoint (not supported, ignore)."""
        self.log(logging.DEBUG, f"Savepoint {name} will be released at transaction end")

    async def _do_rollback_savepoint(self, name: str) -> None:
        """Rollback to Oracle savepoint asynchronously."""
        await self._ensure_connection_ready()

        try:
            cursor = self._connection.cursor()
            await cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
            await cursor.close()
            self.log(logging.DEBUG, f"Rolled back to savepoint (async): {name}")
        except Exception as e:
            error_msg = f"Failed to rollback to savepoint {name}: {str(e)}"
            self.log(logging.ERROR, error_msg)
            raise TransactionError(error_msg) from e

    def supports_savepoint(self) -> bool:
        """Check if savepoints are supported."""
        return True
