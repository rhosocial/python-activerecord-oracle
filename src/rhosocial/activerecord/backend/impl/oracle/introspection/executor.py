# src/rhosocial/activerecord/backend/impl/oracle/introspection/executor.py
"""Oracle-specific introspector executors.

The generic :class:`SyncIntrospectorExecutor` executes SQL directly via the
DB-API cursor.  Oracle's driver (oracledb) requires native ``:1``, ``:2``, …
positional bind markers rather than the generic ``?`` placeholder used by the
rest of the framework.

These subclasses intercept ``execute()`` and run the SQL through
``OracleBackend._convert_placeholders_to_oracle()`` before handing it to
the cursor.
"""

from typing import Any, Dict, List, Optional, Tuple

from rhosocial.activerecord.backend.introspection.executor import (
    AsyncIntrospectorExecutor,
    SyncIntrospectorExecutor,
)


class SyncOracleIntrospectorExecutor(SyncIntrospectorExecutor):
    """Sync executor that converts ``?`` → ``:N`` for Oracle."""

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        oracle_sql, oracle_params = self._backend._convert_placeholders_to_oracle(
            sql, params or None
        )
        cursor = self._backend._get_cursor()
        try:
            cursor.execute(oracle_sql, oracle_params or ())
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()


class AsyncOracleIntrospectorExecutor(AsyncIntrospectorExecutor):
    """Async executor that converts ``?`` → ``:N`` for Oracle."""

    async def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        oracle_sql, oracle_params = self._backend._convert_placeholders_to_oracle(
            sql, params or None
        )
        cursor = await self._backend._get_cursor()
        try:
            await cursor.execute(oracle_sql, oracle_params or ())
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            await cursor.close()
