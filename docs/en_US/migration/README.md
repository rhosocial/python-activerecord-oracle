<!-- docs/en_US/migration/README.md -->
# Migrations on the Oracle Backend

This page covers **only what is specific to running migrations against Oracle**. The migration
system itself — the two authoring APIs, vocabulary, ordering and dependency rules, version records,
dry-run capture, the exception hierarchy, and the `migrate` CLI — lives in the core chapter:

- [Migrations — The Backend Runtime System](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md)
- [Runnable core examples](https://github.com/rhosocial/python-activerecord/tree/main/docs/examples/migration)

Everything in that chapter applies here. Read it first; this page does not repeat it.

Oracle changes one thing that matters more than any other: **DDL is not transactional**. Read
[§2](#2-oracle-ddl-is-not-transactional-implicit-commit) before running a multi-statement migration.

## 1. Configure an Oracle backend for migrations

Use this repository's own config and backend classes. Both sync and async paths are supported:

```python
import os

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig

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
```

`introspect_and_adapt()` is the important call: it teaches the dialect the server's real version and
capabilities **before** any DDL is rendered. Never skip it in a migration runner — an unadapted
dialect cannot answer version gates (see [§3](#3-version-gated-features-and-types)).

`OracleConnectionConfig` also carries the generic `version` field (`Optional[Tuple[int, ...]]`).
Setting it pins the version the dialect renders against, which makes generated DDL reproducible in CI
or when the live server cannot be reached; leaving it `None` lets detection fill it in. The async
counterpart is `AsyncOracleBackend` from
`rhosocial.activerecord.backend.impl.oracle.backend.async_backend` — it has the same
`connect()` / `introspect_and_adapt()` contract.

For the **class-style CLI**, the Oracle adapter (`named-migration`) reads the same values from
`--host` / `--port` / `--service` / `--user` / `--password` (with `ORACLE_*` environment variables as
fallbacks), or from a saved `--named-connection`. No Python config object is needed on the command line.

## 2. Oracle DDL is not transactional (implicit commit)

Oracle issues an **implicit `COMMIT` before and after each DDL statement** (`CREATE TABLE`,
`ALTER TABLE`, `DROP TABLE`, and so on). DDL cannot be wrapped in a transaction and cannot be rolled
back. This has consequences that the core chapter's generic guarantees do not otherwise show:

- **A multi-statement migration group is not atomic.** If statement 3 fails, statements 1 and 2 are
  already committed. There is no rollback that takes them back.
- **Failure leaves schema drift.** `run_migration_group()` writes version records only on success —
  on failure it writes none. After a mid-group failure the schema is partly changed but no version is
  recorded, so a retry starts from the beginning and may hit errors such as `ORA-00955` (name already
  used by an existing object).
- **`transaction_mode` cannot fix this.** `TransactionMode.AUTO` / `STEP` / `NONE` control how the
  core `Procedure` runner wraps work; they do not stop Oracle's implicit commit.
- **Do not trust the `transactional_ddl` flag on a failure.** The core sets
  `MigrationFunctionExecutionError.transactional_ddl` by calling
  `dialect.supports_transactional_ddl()` and defaulting to `True` when the dialect has no such
  method. The Oracle dialect does not currently override it, so the flag may read `True` even though
  Oracle DDL is not transactional. Treat **all** Oracle DDL as non-transactional regardless.

Practical rules for Oracle migrations:

1. Keep one DDL statement per migration where you can; each one commits on its own and is easy to
   reason about.
2. Make `DOWN` migrations tolerant of missing objects (render `IF EXISTS` where the server version
   supports it — see [§3](#3-version-gated-features-and-types)) so a partial failure can be cleaned
   up by hand.
3. Preview with `dry_run=True` (or `--dry-run`) before a real run, and read the rendered statements
   as a committed sequence, not as a single transaction.
4. After a failed run, reconcile the schema manually before retrying; never assume the group
   "rolled back".

Version records are still written on a clean success and still drive duplicate/applied checks exactly
as described in the core chapter — the caveat is only that a *failed* group cannot be undone.

## 3. Version-gated features and types

The Oracle dialect adapts itself to the connected server. `get_server_version()`, `version_full`, and
`ru_version` (the release-update component of `VERSION_FULL`) are what the feature gates compare —
the gates deliberately compare numbers, not Oracle's product name. Examples of gates that affect DDL:

- `supports_identity_column()` — `GENERATED ... AS IDENTITY` is rendered from **12c** onward.
- `FETCH FIRST ... WITH TIES` — gated at **12c**.
- Native `BOOLEAN` / `JSON` types and `DROP TABLE IF EXISTS` — gated on newer releases / release
  updates.
- `ru_version` distinguishes a 23ai line from the 26ai rebrand (both report major `23`; the release
  update is the number that differs).

So the *same migration function can render different SQL on different Oracle versions*. To keep a
migration reproducible, either pin `version=(...)` in `OracleConnectionConfig` or record the target
version in your release checklist. If version detection fails the dialect stays unadapted and a gate
cannot answer — check the server view with the Oracle CLI's `info` subcommand before a migration run.

Because gates are applied while rendering, **do not assemble raw SQL strings**. Emit statement
objects through `ctx.apply(...)` (model statement getters such as
`Model.create_table_statement(backend=ctx.backend)`), or named expressions via `ctx.execute(FQN, ...)`.
That is what lets the dialect apply the correct, version-aware Oracle spelling.

## 4. Both API styles on Oracle

The choice between APIs is explained in the core chapter; it is unchanged here. In short: function
style is recommended for new work; class style is kept for existing migration classes.

### 4.1 Function style (`@migration` + `run_migration_group`)

Author the function exactly as the core chapter describes; only the backend/config objects are
Oracle's. A complete runnable script is in
[`docs/examples/migration/01_function_migration.py`](../../examples/migration/01_function_migration.py).

```python
from rhosocial.activerecord.backend.migration import migration, MigrationDirection

@migration("v001_create_app_users", description="create the initial app_users table")
def create_app_users(ctx):
    if ctx.direction == MigrationDirection.UP:
        ctx.apply(AppUser.create_table_statement(backend=ctx.backend))
    else:
        ctx.apply(AppUser.drop_table_statement(if_exists=True, backend=ctx.backend))
```

Run it against Oracle in-process with `run_migration_group()` (dry run → UP → DOWN), or through the
core `migrate` CLI, which resolves connections generically via `--backend-class` / `--config-class` /
`--config`:

```bash
python -m rhosocial.activerecord.cli migrate myapp.migrations:create_users \
    --backend-class rhosocial.activerecord.backend.impl.oracle.backend:OracleBackend \
    --config-class rhosocial.activerecord.backend.impl.oracle.config:OracleConnectionConfig \
    --config host=localhost --config service_name=XEPDB1 --config username=system \
    --record-store records.json --dry-run
```

Pass `--config password=...` as well for a real run. See the core chapter for `--list`, `--plan`,
`--direction down`, `-o json`, dependency rules, and the async variant.

### 4.2 Class style (`NamedMigration` + `named-migration` CLI)

Write `NamedMigration` subclasses whose `up()` / `down()` call `ctx.execute(FQN, ...)`, then drive
them with this repository's CLI. The CLI adapter is
`src/rhosocial/activerecord/backend/impl/oracle/cli/named_migration.py`; it delegates to the shared
helper and builds an `OracleBackend` (or `AsyncOracleBackend` with `--async`) from the connection
arguments.

```bash
rhosocial-activerecord-oracle named-migration myapp.migrations.v001.CreateUsersTable \
    --host localhost --service XEPDB1 --user system --direction up --record-store ./migrations.json
rhosocial-activerecord-oracle named-migration myapp.migrations.v001.CreateUsersTable \
    --service XEPDB1 --direction down --dry-run
rhosocial-activerecord-oracle named-migration myapp.migrations --list
```

The same commands work with `python -m rhosocial.activerecord.backend.impl.oracle` in place of the
console script. Other flags (`--describe`, `--param KEY=VALUE`, `--all`, `--single-transaction`) are
documented in the core chapter. Existing class-style demos live in
`src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/`; a small local demo is in
[`docs/examples/migration/02_named_migration.py`](../../examples/migration/02_named_migration.py).

## 5. Local examples

| Script | Demonstrates |
|---|---|
| [`01_function_migration.py`](../../examples/migration/01_function_migration.py) | `@migration` + `ctx.apply()` on an Oracle backend; `run_migration_group` dry run → UP → DOWN |
| [`02_named_migration.py`](../../examples/migration/02_named_migration.py) | `NamedMigration` classes + `MigrationRunner` with Oracle config and named-expression DDL |
| [`migration_expressions.py`](../../examples/migration/migration_expressions.py) | the version-aware DDL expressions used by the class-style demo |

These scripts require a reachable Oracle server and are provided for reading/adaptation; connection
values come from `ORACLE_*` environment variables. For the full explanation of the migration system,
return to the [core migration chapter](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md).
