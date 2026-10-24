# `introspect` — Oracle metadata, from the command line

The `introspect` subcommand asks a live Oracle server what it holds, and prints
the answer. Every question runs through the backend's introspector, so what the
CLI reports is what the library sees — the same objects, the same types, the
same parsing — rather than a second implementation that can drift from it.

```bash
python -m rhosocial.activerecord.backend.impl.oracle introspect <TYPE> [NAME] [OPTIONS]
```

## What it can ask about

| Type | Reads | Notes |
| :--- | :--- | :--- |
| `tables` | `all_tables`, `all_tab_comments` | Base tables in the schema asked about. |
| `views` | `all_views`, `all_objects` | Views, with their definitions. |
| `table` | several | One table by name, with its columns, indexes and foreign keys filled in. |
| `columns` | `all_tab_columns` | One table's columns: name, type, nullability, default. |
| `indexes` | `all_ind_columns`, `all_indexes` | One table's indexes, including the primary key. |
| `foreign-keys` | `all_constraints`, `all_cons_columns` | One table's constraints and the columns each side names. |
| `sequences` | `all_sequences` | One owner's sequences. `last_number` is the **next cached value**, not the last one handed out — the dictionary does not record the latter. |
| `synonyms` | `all_synonyms` | Private synonyms, or `--owner PUBLIC` for the global ones. They are never mixed into a schema's list. |
| `packages` | `all_objects` | PL/SQL package specifications, so a specification and its body are not counted twice. |
| `database` | `v$nls_parameters`, `v$version` | Server version and the NLS settings this session uses. |

`NAME` is required for `table`, `columns`, `indexes` and `foreign-keys`, and
selects one object for `sequences`, `synonyms` and `packages`.

## Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `-o, --output` | `table` | `table` (rich), `json`, `csv`, `tsv`. |
| `--owner` | the connected user | The schema whose objects are listed. |
| `--mode` | _None_ | Connection mode. |
| `--host`, `--port`, `--database` / `--service`, `--user`, `--password` | env or defaults | Connection. `ORACLE_*` env vars are honoured, and arguments override them. |
| `--ssl` | off | Use SSL for the connection. |
| `--async` | off | Drive the asynchronous backend. |
| `--named-connection` | _None_ | A connection saved by the `named-connection` subcommand. |
| `--conn-param` | _None_ | Extra `key=value` connection parameters; repeatable. |
| `--rich-ascii` | off | ASCII characters for table borders. |

## Notes

*   **Read-only.** `introspect` never writes. It does not create, drop, or
    alter anything, so it is safe to run against a database whose schema
    matters.
*   **The dictionary is upper case.** Oracle folds unquoted identifiers to
    upper case, so a sequence created as `order_id_seq` is reported as
    `ORDER_ID_SEQ` and asked about by that spelling unless it was created with
    a quoted name.
*   **The owner is explicit.** `--owner` defaults to the connecting user rather
    than to what the session's `CURRENT_SCHEMA` happens to be, so the same
    command gives the same answer from any role that can see the schema.
*   **One question per invocation.** A CLI process is one connection that
    opens, answers, and closes — there is no cache carried between runs, so
    the answer reflects the server as it is *now*.

## Examples

```bash
# The three schema-object families Oracle has that the core catalogue does not
python -m rhosocial.activerecord.backend.impl.oracle introspect sequences --owner HR
python -m rhosocial.activerecord.backend.impl.oracle introspect synonyms --owner HR
python -m rhosocial.activerecord.backend.impl.oracle introspect packages --owner HR

# Public synonyms live under their own owner
python -m rhosocial.activerecord.backend.impl.oracle introspect synonyms --owner PUBLIC

# One table in full
python -m rhosocial.activerecord.backend.impl.oracle introspect table orders --owner HR -o json
```
