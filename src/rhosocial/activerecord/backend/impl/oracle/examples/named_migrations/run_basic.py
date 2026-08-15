# src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/run_basic.py
"""
Basic migration example — single migration UP then DOWN (Oracle).

This script demonstrates:
  1. Creating an Oracle backend
  2. Running a single ``NamedMigration`` UP (creates ``users`` table)
  3. Verifying the table was created
  4. Running the same migration DOWN (drops ``users`` table)
  5. Showing JSON record store persistence
  6. Dry-run mode (no actual changes)
  7. Duplicate execution protection

Usage:
    PYTHONPATH=src python -m rhosocial.activerecord.backend.impl.oracle.examples.named_migrations.run_basic

Connection info is read from ``ORACLE_*`` environment variables with
safe defaults (localhost / empty password).
"""

from pathlib import Path
import tempfile
import os

from rhosocial.activerecord.backend.impl.oracle import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.migration import (
    MigrationRunner,
    MigrationDirection,
    JSONFileMigrationRecordStore,
    MigrationAlreadyAppliedError,
)


def main():
    print("=" * 60)
    print("Named Migration Demo — Basic (Oracle)")
    print("=" * 60)

    config = OracleConnectionConfig(
        host=os.getenv("ORACLE_HOST", "localhost"),
        port=int(os.getenv("ORACLE_PORT", "1521")),
        service_name=os.getenv("ORACLE_SERVICE", "XEPDB1"),
        username=os.getenv("ORACLE_USER", "system"),
        password=os.getenv("ORACLE_PASSWORD", ""),
    )
    backend = OracleBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    print("\n[1] Oracle backend connected.")

    store_path = Path(tempfile.gettempdir()) / "mig_oracle_basic.json"
    if store_path.exists():
        store_path.unlink()
    store = JSONFileMigrationRecordStore(store_path)
    print(f"[2] Record store: {store_path}")

    fqn = (
        "rhosocial.activerecord.backend.impl.oracle.examples"
        ".named_migrations.migrations.V001CreateUsers"
    )
    runner = MigrationRunner(fqn)

    print("\n[3] Dry-run (UP) — no actual changes …")
    result = runner.run(backend, MigrationDirection.UP, dry_run=True)
    print(f"    Result: version={result.version}, success={result.success}")
    print("    ✓ Dry-run completed (table not created).")

    print("\n[4] Applying v001_create_users (UP) …")
    result = runner.run(backend, MigrationDirection.UP, record_store=store)
    print(f"    Result: version={result.version}, success={result.success}")
    print("    ✓ Table 'users' created.")

    print("\n[5] Duplicate UP (should be rejected) …")
    try:
        runner.run(backend, MigrationDirection.UP, record_store=store)
        print("    ✗ ERROR: should have raised!")
    except MigrationAlreadyAppliedError as e:
        print(f"    ✓ {e}")

    print("\n[6] Rolling back v001_create_users (DOWN) …")
    result = runner.run(backend, MigrationDirection.DOWN, record_store=store)
    print(f"    Result: version={result.version}, success={result.success}")
    print("    ✓ Table 'users' dropped.")

    applied = store.get_applied()
    print(f"\n[7] Applied migrations: {len(applied)} (should be 0)")

    backend.disconnect()
    if store_path.exists():
        store_path.unlink()
    print("\n=== Oracle basic migration demo completed ===")


if __name__ == "__main__":
    main()
