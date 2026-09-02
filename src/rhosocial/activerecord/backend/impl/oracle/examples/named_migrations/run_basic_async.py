# src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/run_basic_async.py
"""
Basic async migration example — single migration UP then DOWN (Oracle).

Async counterpart of :mod:`run_basic`. This script demonstrates:

  1. Creating an ``AsyncOracleBackend``
  2. Dry-running an ``AsyncNamedMigration`` UP (no actual changes)
  3. Applying the migration UP (creates ``users`` table)
  4. Verifying the table was created via the async introspector
  5. Duplicate execution protection
  6. Rolling the migration back DOWN (drops ``users`` table)
  7. JSON record store persistence

Usage::

    PYTHONPATH=src python -m rhosocial.activerecord.backend.impl.oracle.examples.named_migrations.run_basic_async

Connection info is read from ``ORACLE_*`` environment variables with
safe defaults (localhost / empty password).
"""

import asyncio
import os
from pathlib import Path
import tempfile

from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.migration import (
    AsyncMigrationRunner,
    MigrationDirection,
    JSONFileMigrationRecordStore,
    MigrationAlreadyAppliedError,
)

_EXPRESSIONS_FQN = (
    "rhosocial.activerecord.backend.impl.oracle.examples"
    ".named_migrations.async_migrations.V001CreateUsersAsync"
)


async def _table_exists(backend: AsyncOracleBackend, table: str) -> bool:
    """Return True if ``table`` exists in the current schema.

    Oracle's dialect does not yet implement the introspection ``table list``
    query, so we check ``USER_TABLES`` directly with a bound parameter.
    """
    from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
    result = await backend.execute(
        "SELECT 1 FROM user_tables WHERE table_name = ?",
        (table.upper(),),
        options=ExecutionOptions(stmt_type=StatementType.DQL),
    )
    return bool(getattr(result, "data", None))


async def main() -> None:
    print("=" * 60)
    print("Async Named Migration Demo — Basic (Oracle)")
    print("=" * 60)

    config = OracleConnectionConfig(
        host=os.getenv("ORACLE_HOST", "localhost"),
        port=int(os.getenv("ORACLE_PORT", "1521")),
        service_name=os.getenv("ORACLE_SERVICE", "XEPDB1"),
        username=os.getenv("ORACLE_USER", "system"),
        password=os.getenv("ORACLE_PASSWORD", ""),
    )
    backend = AsyncOracleBackend(connection_config=config)
    await backend.connect()
    await backend.introspect_and_adapt()
    print("\n[1] AsyncOracle backend connected.")

    store_path = Path(tempfile.gettempdir()) / "mig_oracle_basic_async.json"
    if store_path.exists():
        store_path.unlink()
    store = JSONFileMigrationRecordStore(store_path)
    print(f"[2] Record store: {store_path}")

    runner = AsyncMigrationRunner(_EXPRESSIONS_FQN)

    print("\n[3] Async dry-run (UP) — no actual changes …")
    result = await runner.run(backend, MigrationDirection.UP, dry_run=True)
    print(f"    Result: version={result.version}, success={result.success}")
    exists = await _table_exists(backend, "users")
    assert not exists, "dry-run must not create the table"
    print("    ✓ Dry-run completed (table not created).")

    print("\n[4] Applying v001_create_users async (UP) …")
    result = await runner.run(backend, MigrationDirection.UP, record_store=store)
    print(f"    Result: version={result.version}, success={result.success}")
    exists = await _table_exists(backend, "users")
    assert exists, "users table should exist after UP"
    print("    ✓ Table 'users' created.")

    print("\n[5] Duplicate async UP (should be rejected) …")
    try:
        await runner.run(backend, MigrationDirection.UP, record_store=store)
        print("    ✗ ERROR: should have raised!")
    except MigrationAlreadyAppliedError as e:
        print(f"    ✓ {e}")

    print("\n[6] Rolling back v001_create_users async (DOWN) …")
    result = await runner.run(backend, MigrationDirection.DOWN, record_store=store)
    print(f"    Result: version={result.version}, success={result.success}")
    exists = await _table_exists(backend, "users")
    assert not exists, "users table should be gone after DOWN"
    print("    ✓ Table 'users' dropped.")

    applied = store.get_applied()
    print(f"\n[7] Applied migrations: {len(applied)} (should be 0)")

    await backend.disconnect()
    if store_path.exists():
        store_path.unlink()
    print("\n=== Oracle async basic migration demo completed ===")


if __name__ == "__main__":
    asyncio.run(main())
