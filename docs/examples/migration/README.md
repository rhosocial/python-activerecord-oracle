<!-- docs/examples/migration/README.md -->
# Oracle Migration Examples

Small, runnable examples for the migration system **as used with this Oracle backend**. They do not
re-explain the migration system itself — see the
[core migration chapter](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md)
and this repository's [Oracle migration guide](../../en_US/migration/README.md)
([中文](../../zh_CN/migration/README.md)).

These scripts need a reachable Oracle server (connection values come from `ORACLE_*` environment
variables) and are intended to be read and adapted. Run them from the repository root:

```bash
python docs/examples/migration/01_function_migration.py
python docs/examples/migration/02_named_migration.py
```

| Script | Demonstrates |
|---|---|
| `01_function_migration.py` | `@migration` + `ctx.apply()` on an `OracleBackend`; `run_migration_group` with dry run → UP → DOWN and a `JSONFileMigrationRecordStore` |
| `02_named_migration.py` | `NamedMigration` + `MigrationRunner`, class FQN, dry run, UP and DOWN against an `OracleBackend` |
| `migration_expressions.py` | version-aware DDL statement objects used by `02_named_migration.py` via `ctx.execute(...)` |

For the longer class-style demos shipped with the backend, see
`src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/`.

> Reminder: Oracle DDL implicitly commits and is **not** transactional. A multi-statement migration
> cannot be rolled back as a unit; keep migrations small and preview with a dry run first.
