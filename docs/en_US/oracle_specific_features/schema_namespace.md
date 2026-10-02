# docs/en_US/oracle_specific_features/schema_namespace.md

# Oracle Schema Namespaces

> This page covers what is specific to this backend: what a `schema_name` names
> here, why identifiers come out upper case, how a qualified reference is built
> from three parts instead of two, why Oracle emits no `AS` before a table
> alias, which namespace an unqualified name resolves against, what a synonym
> qualifies, and where the dialect's `format_table` adds clauses of its own.
>
> The model-level API — declaring `__schema_name__`, when the schema reaches
> the SQL, the DDL boundary, the cross-backend support matrix — is documented in
> the core library guide, which lives in the `python-activerecord` repository as
> [`docs/en_US/modeling/schema_namespace.md`][core-en].

[core-en]: https://github.com/rhosocial/python-activerecord/tree/main/docs/en_US/modeling/schema_namespace.md

## How this page was verified

Every SQL fragment below was rendered by the expression layer with
`OracleDialect(version=(19, 0, 0))` and no live server:

```
PYTHONPATH=src .venv3.14-ubuntu26.04/bin/python
```

Measured against `rhosocial-activerecord` 1.0.0.dev30 and
`rhosocial-activerecord-oracle` 1.0.0.dev3.

Every statement comes out on a single line. Long ones are wrapped below for
reading; the line breaks are not part of the output.

Statements that describe the server rather than the renderer — `CURRENT_SCHEMA`
against `SESSION_USER`, the requirement of a `FROM` clause, the synonym target —
are Oracle's own documented behaviour and were not exercised against a live
instance in this repository. Each is marked where it appears, and every point
left open by the absence of an instance says so in place.

## What a `schema_name` names here

`OracleDialect` implements the core `SchemaSupport` protocol, so a
`schema_name` is accepted everywhere the core expects one. What it *names* is
Oracle's own: **a schema, which is the owning user**. Every schema has exactly
one owner and is named after it. `AR_XCRM."USERS"` is a table called `USERS`
belonging to the database user `AR_XCRM`, and there is no way to name a schema
that is not a user.

This is the difference from PostgreSQL that matters most when porting. On
PostgreSQL a schema is an independent object inside one database, created by
`CREATE SCHEMA`, and several users may own objects in it. On Oracle the two
concepts are one: creating a schema means creating a user, and dropping one
means `DROP USER ... CASCADE`.

The rows below are the two servers' documented behaviour, not framework
behaviour; the framework behaviour is what follows them.

| | PostgreSQL | Oracle |
|---|---|---|
| What a schema is | an independent object inside a database | the owning user |
| Created by | `CREATE SCHEMA ar_crm` | `CREATE USER ar_crm IDENTIFIED BY ...` |
| Dropped by | `DROP SCHEMA ar_crm` | `DROP USER ar_crm CASCADE` |
| One schema, many users | yes — users may be granted rights on it | no — one schema, one user |

The capability flags follow from that, and most of them are `False`:

```python
dialect.supports_schema()                # True
dialect.supports_create_schema()         # False
dialect.supports_drop_schema()           # False
dialect.supports_schema_if_not_exists()  # False
dialect.supports_schema_if_exists()      # False
dialect.supports_schema_cascade()        # False
dialect.supports_schema_authorization()  # False
```

`supports_schema()` answers whether a name can be *qualified* with a namespace,
and on Oracle it can. The remaining flags answer whether a namespace can be
created and dropped by schema DDL, and it cannot: both statements raise rather
than render.

```python
CreateSchemaExpression(d, "ar_crm").to_sql()[0]
# UnsupportedFeatureError: 'Oracle' dialect does not support CREATE SCHEMA.
#   Suggestion: Oracle does not support CREATE SCHEMA.

DropSchemaExpression(d, "ar_crm").to_sql()[0]
# UnsupportedFeatureError: 'Oracle' dialect does not support DROP SCHEMA.
#   Suggestion: Oracle does not support DROP SCHEMA.
```

### How a qualified name is rendered

Two double-quoted identifiers separated by a dot:

| Expression | SQL |
|---|---|
| `TableExpression(d, "users", schema_name="APP")` | `"APP"."USERS"` |
| `TableExpression(d, "users")` | `"USERS"` |
| `TableExpression(d, "orders", schema_name="APP", alias="o")` | `"APP"."ORDERS" "O"` |

Note the third row: there is no `AS` before the alias. Oracle's `SELECT`
reference notes that the `AS` keyword is optional for a *column* alias and says
nothing of the kind for a *table* correlation name, which is why the dialect
emits a bare space. Hand-written SQL in a migration or a view definition follows
the same rule.

### Quoting

`format_identifier` upper-cases the value, doubles any embedded double quote, and
wraps the result in double quotes. A name that would otherwise terminate the
reference therefore stays inside one identifier:

```python
TableExpression(d, "orders", schema_name='a"b').to_sql()[0]
# "A""B"."ORDERS"

TableExpression(d, "my orders", schema_name="app").to_sql()[0]
# "APP"."MY ORDERS"
```

A reserved word is quoted like any other name:

```python
TableExpression(d, "user", schema_name="app").to_sql()[0]
# "APP"."USER"
```

Each segment is quoted on its own, so a dot inside a value is not a separator.
See [Common mistakes](#common-mistakes).

## Declaring one on a model

```python
from typing import ClassVar, Optional

class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "app"                # -> "APP"."ORDERS"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    customer_id: Optional[int] = None
    total: Optional[float] = None
```

`__schema_name__` is optional and defaults to `None`, which means unqualified and
leaves the choice to the connection. A model without it renders bare:

```python
class PlainOrder(ActiveRecord):
    __table_name__ = "plain_orders"

PlainOrder.query().select(PlainOrder.c.id).to_sql()[0]
# SELECT "PLAIN_ORDERS"."ID" FROM "PLAIN_ORDERS"
```

When it is set, every statement the model builds carries the namespace, and the
column references inside carry it too — see
[Columns take three parts](#columns-take-three-parts).

The namespace is read once, through `schema_name()`, and reaches each column
expression as it is built. Changing `__schema_name__` afterwards does not rewrite
an expression that already exists. The core guide describes the binding in full.

`__table_name__` and `__schema_name__` are separate on purpose, and folding is
another reason not to fold the two together by hand: writing `__table_name__` as
`"app.orders"` quotes the dot into a single identifier. See
[Common mistakes](#common-mistakes).

## Identifiers fold to upper case

This is the behaviour with the widest reach, and the one most likely to look
like a bug in generated SQL. `format_identifier` upper-cases before quoting, so
a lower-case declaration and an upper-case declaration render identically:

```python
class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "ar_xcrm"     # declared in lower case

Order.query().select(Order.c.id).to_sql()[0]
# SELECT "AR_XCRM"."ORDERS"."ID" FROM "AR_XCRM"."ORDERS"
```

The rendered SQL does not contain the string `ar_xcrm`. It contains `AR_XCRM`.
The same applies to the table name, to the alias, and to every other identifier
this dialect quotes.

**What this costs you.** Declaring lower case works, and the statement runs,
because Oracle stores unquoted `CREATE USER ar_xcrm` under the upper-case name
`AR_XCRM` — an unquoted name on the server folds the same way the renderer
folds it. The folding is therefore self-consistent and round-trips.

**What to watch for.** The generated SQL no longer matches the text you wrote,
which matters in three places:

- **Reading logs and plans.** Every line shows upper case. Comparing a log
  against the model definition needs the folding rule, not an exact match.
- **Asserting on SQL.** A test that looks for `"ar_xcrm"` fails against correct
  output. Match the folded form.
- **Comparing across backends.** The same model on PostgreSQL renders
  `"ar_xcrm"."orders"`; here it renders `"AR_XCRM"."ORDERS"`. A test shared
  between the two backends has to be written against the shape each one
  produces.

**What breaks it.** A name created with quotes, or in mixed case, does not fold.
`CREATE USER "Ar_Xcrm"` stores the name `Ar_Xcrm` exactly as written, and
`"AR_XCRM"` in the generated SQL does not name it. The statement then fails with
ORA-00942, table or view does not exist.

`format_identifier` takes a `need_quote` argument, and passing `False` returns
the value verbatim — no upper-casing, no quotes. Each segment of a
`TableExpression` carries its own flag, so turning both off renders the name as
written:

```python
TableExpression(
    d, "orders", schema_name="App",
    name_need_quote=False, schema_need_quote=False,
).to_sql()[0]
# App.orders
```

A value that is a reserved word then goes out unquoted, and the dialect emits an
`IdentifierQuotingWarning` rather than failing quietly. Use this only for a name
that genuinely exists with mixed case.

## Columns take three parts

Oracle accepts `schema.table.column` and this backend emits it. An unaliased
range keeps the schema all the way through the column reference:

```python
Order.query().select(Order.c.id).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS"

Order.query().where(Order.c.id > 1).to_sql()[0]
# SELECT * FROM "SHOP"."ORDERS" WHERE "SHOP"."ORDERS"."ID" > ?

Order.query().group_by(Order.c.customer_id).having(Order.c.total > 10).to_sql()[0]
# SELECT * FROM "SHOP"."ORDERS" GROUP BY "SHOP"."ORDERS"."CUSTOMER_ID" HAVING "SHOP"."ORDERS"."TOTAL" > ?
```

`ORDER BY`, `GROUP BY` and `HAVING` follow the same rule as `SELECT`: the schema
is on the range and on every reference to it.

This is where Oracle differs from SQL Server most visibly: that backend's
dialect drops the schema and renders `[orders].[id]`. Neither form is wrong for
its own server, and a page written for one backend must not quote the other's
examples.

Oracle's own constraint on the three-part form: a column may be qualified with a
schema **only when the range in the `FROM` clause is qualified with the same
schema**. The renderer satisfies that by construction for any query the query
layer builds, because one value reaches both. A hand-built query can break the
pairing, and this backend does not repair it:

```python
QueryExpression(
    dialect=d,
    select=[Column(d, "id", table="orders", schema_name="app")],
    from_=TableExpression(d, "orders"),            # no schema on the range
).to_sql()[0]
# SELECT "APP"."ORDERS"."ID" FROM "ORDERS"       -- mismatched
```

Two further boundaries hold. A column carrying a schema but no table is
refused, because there is nothing to resolve the prefix against:

```
ValueError: Oracle: cannot qualify column 'id' with schema 'APP' because no table
was given; a column reference needs a table (or an alias) to be
schema-qualified
```

And a hand-built wildcard still renders three-part, because
`WildcardExpression` is formatted by the core renderer and this dialect does not
override it:

```python
WildcardExpression(d, table="orders", schema_name="app").to_sql()[0]
# "APP"."ORDERS".*
```

No query the framework builds reaches that shape — every wildcard the query layer
creates is bare. Treat the rendering as measured and the server's answer as
untested; this repository has no live Oracle instance.

## Aliases

An aliased range is addressed by its alias alone, and the alias is quoted and
folded like any other identifier. No `AS` precedes it:

```python
Order.query().join(
    Customer, on=Order.c.customer_id == Customer.c.with_table_alias("u").id, alias="u"
).select(Order.c.id, Customer.c.with_table_alias("u").name).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID", "U"."NAME" FROM "SHOP"."ORDERS"
#   JOIN "CRM"."CUSTOMERS" "U" ON "SHOP"."ORDERS"."CUSTOMER_ID" = "U"."ID"
```

The range keeps its schema; only the column prefix changes. The drop happens in
the query layer, when `FieldProxy` builds the column expression: a table alias
in effect sets the column's `schema_name` to `None`.

Because that guard lives in `FieldProxy` and not in the renderer, a
hand-built `Column` bypasses it and produces SQL Oracle rejects:

```python
QueryExpression(
    dialect=d,
    select=[Column(d, "id", table="orders", schema_name="app")],
    from_=TableExpression(d, "orders", schema_name="app", alias="o"),
).to_sql()[0]
# SELECT "APP"."ORDERS"."ID" FROM "APP"."ORDERS" "O"   -- "O" hides the range
```

Build column expressions through `Model.c.<field>` rather than `Column` when the
range carries an alias.

### Two ranges that share a table name

Two models bound to the same table name in different owners render distinct
three-part prefixes, and Oracle tells them apart:

```python
ShopOrder.query().join(CrmOrder, on=ShopOrder.c.user_id == CrmOrder.c.id).to_sql()[0]
# SELECT * FROM "SHOP"."ORDERS" JOIN "CRM"."ORDERS"
#   ON "SHOP"."ORDERS"."USER_ID" = "CRM"."ORDERS"."ID"
```

Oracle resolves `"SHOP"."ORDERS"` and `"CRM"."ORDERS"` to two different owners,
so the two prefixes name two different columns and the framework has nothing to
flag. No correlation name is required. Give one anyway when the statement is read
often enough that `"SHOP"."ORDERS"."USER_ID"` costs more attention than
`"S"."USER_ID"`.

## Set operations

`UNION`, `INTERSECT` and `MINUS` name no object of their own, so there is
nothing for them to qualify. Each branch keeps its own owner. Note that
`except_()` renders as `MINUS`, which is Oracle's spelling of the operator:

```python
Order.query().select(Order.c.id).intersect(Customer.query().select(Customer.c.id)).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS"
#   INTERSECT SELECT "CRM"."CUSTOMERS"."ID" FROM "CRM"."CUSTOMERS"

Order.query().select(Order.c.id).except_(Customer.query().select(Customer.c.id)).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS"
#   MINUS SELECT "CRM"."CUSTOMERS"."ID" FROM "CRM"."CUSTOMERS"
```

## CTEs

A CTE is named for the rest of the query, not for the database, so its own name
is bare and unprefixed. The query inside it still carries the model's schema:

```python
from rhosocial.activerecord.query.cte_query import CTEQuery

CTEQuery(backend).with_cte(
    "recent_orders", Order.query().select(Order.c.id)
).from_cte("recent_orders").select("id").to_sql()[0]
# WITH "RECENT_ORDERS" AS (SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS")
#   SELECT "ID" FROM "RECENT_ORDERS"
```

The CTE's name is folded to upper case like any other identifier. Qualifying it
would make Oracle look for a table called `RECENT_ORDERS` in the schema, which
does not exist.

## DDL takes a schema of its own

`__schema_name__` selects the read/write namespace. It is **not** consulted when
DDL is built — a migration has to name the schema it means — but every statement
that names a schema-bearing object accepts a `schema_name` of its own, so
qualification no longer has to be assembled by hand. Every name below is folded
like any other identifier.

```python
DropTableExpression(d, TableExpression(d, "orders", schema_name="app")).to_sql()[0]
# DROP TABLE "APP"."ORDERS"

TruncateExpression(d, "orders", schema_name="app").to_sql()[0]
# TRUNCATE TABLE "APP"."ORDERS"

CreateIndexExpression(d, "idx_orders_id", "orders", ["id"], schema_name="app").to_sql()[0]
# CREATE INDEX "APP"."IDX_ORDERS_ID" ON "APP"."ORDERS" ("ID")

DropIndexExpression(d, "idx_orders_id", schema_name="app").to_sql()[0]
# DROP INDEX "APP"."IDX_ORDERS_ID"

CreateSequenceExpression(d, "seq_orders", schema_name="app").to_sql()[0]
# CREATE SEQUENCE "APP"."SEQ_ORDERS" NOCYCLE NOORDER

DropSequenceExpression(d, "seq_orders", schema_name="app").to_sql()[0]
# DROP SEQUENCE "APP"."SEQ_ORDERS"

DropTypeExpression(d, "t_addr", schema_name="app").to_sql()[0]
# DROP TYPE "APP"."T_ADDR"
```

`CREATE INDEX` has one `schema_name` and it covers both names: the index and the
table it is built on land in the same owner. That is required on Oracle, because
an index must live in the same schema as its table.

`DROP TABLE` has a quirk worth knowing before the first migration runs on this
backend. Passing `if_exists=True` does not raise and does not render the guard;
the statement comes out bare:

```python
dialect.supports_if_exists_table()                                  # False
DropTableExpression(d, TableExpression(d, "o", schema_name="app"), if_exists=True).to_sql()[0]
# DROP TABLE "APP"."O"
```

`DROP INDEX` treats the same flag differently and raises
`UnsupportedFeatureError`. To drop a table only when it is there, test
`ALL_TABLES` in the migration, or issue the drop inside a PL/SQL block that
swallows ORA-00942 — the pattern this backend's own cross-schema tests use in
`tests/rhosocial/activerecord_oracle_test/feature/backend/test_cross_schema.py`.

DML takes a qualified `TableExpression`:

```python
InsertExpression(d, TableExpression(d, "users", schema_name="app"), source,
                 columns=["id", "name"]).to_sql()[0]
# INSERT INTO "APP"."USERS" ("ID", "NAME") VALUES (?, ?)

UpdateExpression(d, TableExpression(d, "users", schema_name="app"),
                 {"name": value}).to_sql()[0]
# UPDATE "APP"."USERS" SET "NAME" = ?

DeleteExpression(d, TableExpression(d, "users", schema_name="app")).to_sql()[0]
# DELETE FROM "APP"."USERS"
```

Because soft delete rebuilds an `UPDATE` against the model's range, `restore()`
carries the namespace down the same way `delete()` does. Without it, a restore
would clear `deleted_at` on the same-named table in the connected user's schema
and leave the intended row soft-deleted.

### A synonym qualifies its target, not itself

`OracleCreateSynonymExpression` takes the synonym's name and the target's name as
two separate arguments, and its `schema_name` qualifies the **target**:

```python
OracleCreateSynonymExpression(d, "s_users", "users", schema_name="app").to_sql()[0]
# CREATE SYNONYM "S_USERS" FOR "APP"."USERS"

OracleCreateSynonymExpression(d, "s_users", "users").to_sql()[0]
# CREATE SYNONYM "S_USERS" FOR "USERS"

OracleCreateSynonymExpression(d, "s_users", "users", schema_name="app", public=True).to_sql()[0]
# CREATE PUBLIC SYNONYM "S_USERS" FOR "APP"."USERS"
```

`schema_name` is not rendered before the synonym's own name, and there is no
expression that produces `CREATE SYNONYM "APP"."S_USERS"`. That is not a gap:
a synonym is created in the connected user's schema unless `PUBLIC` is given,
and its name is not something a `schema_name` can select. It reaches the object
the synonym resolves to, not the synonym.

### `@dblink` and flashback on a table reference

The dialect overrides `format_table` to append two clauses that the core
expression does not carry. Both hang off attributes on the table expression:

```python
from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
    OracleAsOfClause, OracleAsOfMode,
)

t = TableExpression(d, "orders", schema_name="app", alias="o")
t.dblink = "remotedb"
t.to_sql()[0]
# "APP"."ORDERS"@"REMOTEDB" "O"

t2 = TableExpression(d, "orders", schema_name="app")
t2.flashback = OracleAsOfClause(d, OracleAsOfMode.TIMESTAMP,
                                "SYSTIMESTAMP - INTERVAL '1' DAY")
t2.to_sql()[0]
# "APP"."ORDERS" AS OF TIMESTAMP SYSTIMESTAMP - INTERVAL '1' DAY
```

With all three carried at once, the order holds: schema and name, `@dblink`,
flashback clause, alias.

```python
t3 = TableExpression(d, "orders", schema_name="app", alias="o")
t3.dblink = "dl"
t3.flashback = OracleAsOfClause(d, OracleAsOfMode.SCN, 12345)
t3.to_sql()[0]
# "APP"."ORDERS"@"DL" AS OF SCN 12345 "O"
```

The `dblink` name is folded like any other identifier, so `dl` becomes `"DL"`.

Two cautions. `TableExpression` has no `dblink` or `flashback` constructor
argument — the attributes have to be assigned after construction, and an
expression that lacks them renders without the clauses. And the auto-generated
capability protocol declares `format_table` as taking `dblink` and `flashback`
keyword arguments, while the implementation takes a single expression; calling
it with those keywords raises `TypeError`.

## Which schema an unqualified name resolves against

Oracle resolves an unqualified name against the session's *current schema*, which
starts as the user the session authenticated as and can be changed for the rest
of the session:

```sql
ALTER SESSION SET CURRENT_SCHEMA = ar_xcrm
```

Oracle's own documentation is explicit that this changes the current schema but
**not** the session user or the current user, and grants the session no
additional system or object privileges. The two values therefore come apart:
after that statement, `SYS_CONTEXT('USERENV','CURRENT_SCHEMA')` reads `AR_XCRM`
while `SYS_CONTEXT('USERENV','SESSION_USER')` still reads whoever logged in.
Both are read together by `get_session_info()`, so the divergence is visible
there. This is documented Oracle behaviour, not framework behaviour, and it was
not exercised against a live instance here.

The backend reads the current schema through:

```python
backend.get_current_schema()      # AsyncOracleBackend: await ...
```

which renders

```
SELECT SYS_CONTEXT(?, ?) FROM "DUAL"
```

with the two function arguments carried as parameters rather than written into
the string. `OracleBackend._convert_placeholders_to_oracle` renumbers the `?`
markers to `:1, :2` on the way to the driver, so the statement the server
receives is the familiar

```sql
SELECT SYS_CONTEXT(:1, :2) FROM "DUAL"
```

with `'USERENV'` and `'CURRENT_SCHEMA'` as the bound values.

`FROM DUAL` is what makes the statement a query block. `DUAL` is the one-row
table that gives a bare value expression somewhere to select from — the same
reason `SELECT SYSDATE FROM DUAL` is the canonical form in Oracle's own
documentation. Oracle Database 23ai made the `FROM` clause optional for simple
expressions, but only for that release and only for statements that involve no
tables, joins or subqueries; the backend keeps emitting `FROM DUAL` because it
has to work on every supported version. A statement of just
`SELECT SYS_CONTEXT(...)` is rejected before 23ai, and that is why the backend
builds a `QueryExpression` over `TableExpression(dialect, "DUAL")` rather than
concatenating a string.

There is nothing on the connection to set it. `OracleConnectionConfig` has no
schema field, no `search_path` counterpart and no session-statement hook; the
`ALTER SESSION` above would have to be issued by hand through
`backend.execute(...)`, and it applies to the one connection `connect()` opened.
A namespace is therefore chosen by declaring `__schema_name__` on the models
that deviate from the connected user.

Introspection is scoped the same way, and folds the owner it binds:

```sql
SELECT column_name, data_type, ... FROM all_tab_columns
WHERE owner = ? AND table_name = ? ORDER BY column_id
```

`format_column_info_query` upper-cases both bind values before sending them, so
an owner given as `ar_xcrm` is compared against the stored `AR_XCRM`.

## The empty string, and when it is caught

`""` is a mistake, not a way of saying "unqualified" — that is what `None`
means. It is rejected, but **not when the expression is built**. An expression
only collects parameters at that point — its dialect may not even be settled
yet — so strict validation happens while the statement is rendered, where the
statement is known to be whole. The failure therefore arrives later than you
would expect:

```python
class Bad(ActiveRecord):
    __table_name__ = "empties"
    __schema_name__ = ""

Bad.schema_name()                        # ''           -- no error
Bad.c.id                                 # Column       -- no error
Bad.query()                              # ActiveQuery  -- no error
Bad.query().select(Bad.c.id)             # ActiveQuery  -- no error
Bad.query().select(Bad.c.id).to_sql()    # ValueError   -- here
```

The message names the expression at fault:

```
ValueError: Column.schema_name must be a non-empty string; use None for an
unqualified reference
```

```
ValueError: TableExpression.schema_name must be a non-empty string; use None for
an unqualified reference
```

A blank string is rejected the same way as an empty one — the check strips
whitespace first, so `"   "` is refused too. A non-string is rejected with its
own message:

```
ValueError: TableExpression.schema_name must be a string or None, not int
```

The reason to reject rather than treat `""` as absent is that `format_table`
decides whether to qualify from `bool(expr.schema_name)`, which is false for
`""`, and takes the unqualified branch. A caller who asked for `"APP"."ORDERS"`
would get `"ORDERS"` with no error, no warning and no affected-row count to
notice it by. On Oracle that is the table in the connected user's schema, so the
statement runs and writes to the wrong place.

## Common mistakes

**A dot in `__table_name__` is not a namespace.** The whole string is quoted as
one identifier, and then upper-cased:

```python
class User(ActiveRecord):
    __table_name__ = "app.users"

User.query().select(User.c.id).to_sql()[0]
# SELECT "APP.USERS"."ID" FROM "APP.USERS"    -- a table named literally APP.USERS
```

Use `__schema_name__`, or pass a qualified `TableExpression` to the statement
that needs one.

**A dot in `__schema_name__` is not a namespace either.** Each segment is quoted
separately, so a dot inside one stays inside that one:

```python
TableExpression(d, "orders", schema_name="app.public").to_sql()[0]
# "APP.PUBLIC"."ORDERS"     -- a user literally named APP.PUBLIC
```

**Reading `"ar_xcrm"` in a log or an assertion.** The rendered SQL says
`"AR_XCRM"`. This is the folding rule at work, not a change of namespace. See
[Identifiers fold to upper case](#identifiers-fold-to-upper-case).

**Creating the user with mixed case.** `CREATE USER "Ar_Xcrm"` stores a name the
renderer cannot produce, because every quoted identifier is upper-cased. Create
users unquoted, or accept that the framework will not address them.

**Writing `AS` in a table alias.** The dialect emits a bare space, and Oracle's
`SELECT` reference grants the optional `AS` to column aliases only. Hand-written
SQL in a migration or a view definition follows the same rule.

**Hand-building a `Column` against an aliased or unqualified range.** The drop
of the schema on an aliased range happens in `FieldProxy`, not in the renderer,
so `Column(d, "id", table="orders", schema_name="app")` produces SQL Oracle
rejects under `FROM "APP"."ORDERS" "O"`. Go through `Model.c.<field>`.

**Expecting construction to raise.** Nothing rejects a bad `schema_name` until
the statement renders. A model-level mistake survives every step up to and
including query building and fails when the SQL is assembled.

**Reaching for `CREATE SCHEMA` or `DROP SCHEMA`.** Both raise
`UnsupportedFeatureError`. Provision a namespace with `CREATE USER` and retire it
with `DROP USER ... CASCADE`.

**Reading `SESSION_USER` as the schema.** They agree on a fresh connection and
come apart after `ALTER SESSION SET CURRENT_SCHEMA`. `get_session_info()` reports
both; `get_current_schema()` reports only the one that resolves names.

**Expecting `if_exists` on `DROP TABLE` to render a guard.** It is accepted and
then dropped. `DROP INDEX` raises instead. See
[DDL takes a schema of its own](#ddl-takes-a-schema-of-its-own).

## Recommended layering

Let the connected user carry the common case and reserve `__schema_name__` for
the exception:

- **One owner** — set no `__schema_name__` at all. Unqualified names keep DML,
  DDL and introspection consistent with one another, and there is no qualified
  range to reason about.
- **Several owners** — set `__schema_name__` only on the models that deviate
  from the connected user. The smaller the exceptional surface, the fewer
  chances of hitting the mistakes above.
- **Cross-owner joins** — each side qualifies its own range, so this works
  without extra configuration:

  ```python
  Order.query().join(
      Customer, on=Order.c.customer_id == Customer.c.id
  ).select(Order.c.id, Customer.c.name)
  # SELECT "SHOP"."ORDERS"."ID", "CRM"."CUSTOMERS"."NAME" FROM "SHOP"."ORDERS"
  #   JOIN "CRM"."CUSTOMERS" ON "SHOP"."ORDERS"."CUSTOMER_ID" = "CRM"."CUSTOMERS"."ID"
  ```

  Since a schema is a user, cross-owner joins need `SELECT` rights on both sides'
  tables, granted by the owning user. `__schema_name__` qualifies the name; it
  grants nothing.