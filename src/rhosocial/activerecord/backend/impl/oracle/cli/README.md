# `introspect` — Oracle metadata, from the command line

The `introspect` subcommand asks a live Oracle server what it holds, and prints
the answer. Every question runs through the backend's introspector, so what the
CLI reports is what the library sees — the same objects, the same types, the
same parsing — rather than a second implementation that can drift from it.

```bash
python -m rhosocial.activerecord.backend.impl.oracle introspect <TYPE> [NAME] [OPTIONS]
```

## What it can ask about

| Type | `NAME` | Reads | Notes |
| :--- | :--- | :--- | :--- |
| `tables` | none | `all_tables`, `all_tab_comments` | Base tables in the schema asked about. |
| `views` | view, optional | `all_views`, `all_objects` | Views, with their definitions. A named view is described on its own, columns included. |
| `columns` | table, required | `all_tab_columns` | One table's columns: name, type, nullability, default. |
| `indexes` | table, required | `all_ind_columns`, `all_indexes` | One table's indexes, including the primary key. |
| `foreign-keys` | table, required | `all_constraints`, `all_cons_columns` | One table's constraints and the columns each side names. |
| `triggers` | table, optional | `all_triggers` | The triggers on one table, or every trigger in the schema. |
| `sequences` | sequence, optional | `all_sequences` | One owner's sequences. `last_number` is the **next cached value**, not the last one handed out — the dictionary does not record the latter. |
| `synonyms` | synonym, optional | `all_synonyms` | The connecting user's private synonyms. The global ones live under the owner `PUBLIC` and are never mixed into a schema's list. |
| `packages` | none | `all_objects` | PL/SQL package specifications, so a specification and its body are not counted twice. |
| `database` | none | `v$nls_parameters`, `v$version` | Server version and the NLS settings this session uses. |

`NAME` is a **table** for `columns`, `indexes`, `foreign-keys` and `triggers`,
and an **object** for `views`, `sequences` and `synonyms`. The two readings
point in opposite directions — `triggers users` asks for the triggers *on*
users, while `views users` asks for the view *called* users — so:

*   `columns`, `indexes` and `foreign-keys` require a table name;
*   `triggers` takes an optional table name and lists that table's triggers
    without one;
*   `views`, `sequences` and `synonyms` take an optional object name and
    answer with that one object, columns included for a view. A named object
    that does not exist exits 1;
*   `tables`, `packages` and `database` take no name. Passing one is refused
    with exit 2 rather than ignored: a dropped name reads as a filter that
    matched everything. `packages` has no single-object answer to give, and
    `tables NAME` is deliberately absent — it would bring back the aggregate
    (columns, indexes and foreign keys in one answer) that was removed.

Exit status follows the same split: **1** for a runtime failure (connection,
query, anything unexpected) or a missing object, **2** for a request this
server cannot serve (a type its dialect does not support, or a `NAME` the type
cannot use).

## Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `-o, --output` | `table` | `table` (rich), `json`, `csv`, `tsv`. |
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
*   **`triggers` takes a table, not a trigger.** `triggers orders` lists the
    triggers defined on `orders`, and `triggers` alone lists every trigger in
    the schema. The table name is the only filter `all_triggers` is asked for
    — an Oracle trigger belongs to one table — so there is no "describe one
    trigger" form here, and the name is never mistaken for the trigger's own.
*   **The dictionary is upper case.** Oracle folds unquoted identifiers to
    upper case, so a sequence created as `order_id_seq` is reported as
    `ORDER_ID_SEQ` and asked about by that spelling unless it was created with
    a quoted name.
*   **The owner is the connection's user.** The schema asked about is the one
    the connection authenticates as (`--user`), not what the session's
    `CURRENT_SCHEMA` happens to be and not a second argument on the command
    line, so the same command gives the same answer from any role that can see
    the schema. PUBLIC synonyms are not listed: Oracle records them under the
    owner `PUBLIC`, which is a name in `all_synonyms` rather than a schema a
    run connects as.
*   **One question per invocation.** A CLI process is one connection that
    opens, answers, and closes — there is no cache carried between runs, so
    the answer reflects the server as it is *now*.

## Examples

```bash
# The three schema-object families Oracle has that the core catalogue does not
python -m rhosocial.activerecord.backend.impl.oracle introspect sequences
python -m rhosocial.activerecord.backend.impl.oracle introspect synonyms
python -m rhosocial.activerecord.backend.impl.oracle introspect packages

# One sequence by name, and one view with its columns
python -m rhosocial.activerecord.backend.impl.oracle introspect sequences orders_seq
python -m rhosocial.activerecord.backend.impl.oracle introspect views active_users

# The triggers on one table (NAME is the table), or every trigger in the schema
python -m rhosocial.activerecord.backend.impl.oracle introspect triggers orders
python -m rhosocial.activerecord.backend.impl.oracle introspect triggers

# A table's columns, indexes and foreign keys, which need the table name
python -m rhosocial.activerecord.backend.impl.oracle introspect columns orders -o json
python -m rhosocial.activerecord.backend.impl.oracle introspect foreign-keys orders
```
