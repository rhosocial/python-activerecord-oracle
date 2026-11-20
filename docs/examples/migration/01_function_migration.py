# docs/examples/migration/01_function_migration.py
"""Function-style Oracle migration: dry run -> UP -> DOWN, with version records.

Requires a reachable Oracle server; connection values come from ``ORACLE_*``
environment variables. Demonstrates the API from the core migration chapter,
but with this repository's ``OracleBackend`` / ``OracleConnectionConfig``.

Oracle DDL is NOT transactional (each DDL statement implicitly commits). Read
``docs/en_US/migration/README.md`` before adapting this for a real deployment.
"""

import os
import tempfile
from pathlib import Path
from typing import Annotated, Optional

from rhosocial.activerecord.base import UseSqlType
from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.expression.types import IntegerType, VarCharType
from rhosocial.activerecord.backend.migration import (
    MigrationDirection,
    migration,
    run_migration_group,
)
from rhosocial.activerecord.backend.migration.record import JSONFileMigrationRecordStore
from rhosocial.activerecord.model import ActiveRecord


class AppUser(ActiveRecord):
    @classmethod
    def table_name(cls) -> str:
        return "app_users"

    id: Annotated[Optional[int], UseSqlType(IntegerType())] = None
    name: Annotated[str, UseSqlType(VarCharType(length=64))]


@migration("v001_create_app_users", description="create the initial app_users table")
def create_app_users(ctx):
    if ctx.direction == MigrationDirection.UP:
        ctx.apply(AppUser.create_table_statement(backend=ctx.backend))
    else:
        ctx.apply(AppUser.drop_table_statement(if_exists=True, backend=ctx.backend))


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
    backend.introspect_and_adapt()  # teach the dialect the server version BEFORE DDL
    return backend


def main() -> None:
    backend = build_backend()
    record_path = Path(tempfile.gettempdir()) / "oracle_migration_records.json"
    store = JSONFileMigrationRecordStore(record_path)
    fqn = f"{__name__}:create_app_users"
    try:
        preview = run_migration_group(backend, [fqn], dry_run=True)
        print("== dry run ==")
        print("  statements:", preview.dry_run_total)
        for label, sql, params in preview.dry_run_sql:
            print(f"  [{label}] {sql}")
        print("  records written:", preview.records_written)

        up = run_migration_group(backend, [fqn], record_store=store)
        print("\n== UP ==")
        print("  applied:", up.applied, "versions:", up.versions)

        down = run_migration_group(
            backend, [fqn], direction=MigrationDirection.DOWN, record_store=store
        )
        print("\n== DOWN ==")
        print("  rolled back:", down.rolled_back)
    finally:
        if record_path.exists():
            record_path.unlink()
        backend.disconnect()


if __name__ == "__main__":
    main()
