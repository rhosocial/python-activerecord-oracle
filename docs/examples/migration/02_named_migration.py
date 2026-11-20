# docs/examples/migration/02_named_migration.py
"""Class-style Oracle migration: NamedMigration + MigrationRunner + dry run.

Requires a reachable Oracle server; connection values come from ``ORACLE_*``
environment variables. This is the small local counterpart of the longer demos
under ``src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/``.

The DDL is emitted through named expressions (dotted FQNs) in
``migration_expressions.py``; the Oracle dialect renders version-aware SQL.
"""

import os
import sys
import tempfile
from pathlib import Path

# Make the sibling migration_expressions module resolvable by its dotted FQN.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend  # noqa: E402
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig  # noqa: E402
from rhosocial.activerecord.backend.migration import (  # noqa: E402
    JSONFileMigrationRecordStore,
    MigrationContext,
    MigrationDirection,
    MigrationRunner,
    NamedMigration,
)

EXPRESSIONS_MODULE = "migration_expressions"


class CreateAuditTable(NamedMigration):
    """Create the ``audit_log`` table (one DDL statement per migration)."""

    version = "v001_create_audit_table"

    def up(self, ctx: MigrationContext) -> None:
        ctx.execute(f"{EXPRESSIONS_MODULE}.create_audit_table")

    def down(self, ctx: MigrationContext) -> None:
        # IF EXISTS rendering is gated by the connected server version.
        ctx.execute(f"{EXPRESSIONS_MODULE}.drop_audit_table")


def build_backend() -> OracleBackend:
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
    return backend


def main() -> None:
    backend = build_backend()
    record_path = Path(tempfile.gettempdir()) / "oracle_named_migration_records.json"
    store = JSONFileMigrationRecordStore(record_path)
    # A class FQN uses dots: module.Class. Under `python <script>` this is __main__.
    runner = MigrationRunner(f"{__name__}.CreateAuditTable")
    try:
        print("== dry run (UP) ==")
        result = runner.run(backend, MigrationDirection.UP, dry_run=True)
        print("  version:", result.version, "success:", result.success)

        print("\n== UP ==")
        result = runner.run(backend, MigrationDirection.UP, record_store=store)
        print("  version:", result.version, "success:", result.success)

        print("\n== DOWN ==")
        result = runner.run(backend, MigrationDirection.DOWN, record_store=store)
        print("  version:", result.version, "success:", result.success)
    finally:
        if record_path.exists():
            record_path.unlink()
        backend.disconnect()


if __name__ == "__main__":
    main()
