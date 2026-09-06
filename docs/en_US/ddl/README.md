# DDL Operations

## Overview

This section covers DDL (Data Definition Language) operations for the Oracle backend. DDL defines your database schema — tables, indexes, views, and other objects.

**Important**: All DDL in rhosocial-activerecord is **expression-based**. You define your schema in Python, the framework generates the SQL, and you execute it via the backend. The examples below use `ActiveRecord` for brevity, but `AsyncActiveRecord` works identically — the DDL generation is pure computation with no I/O involved.

The DDL chapter is divided into two parts:

1. **Backend DDL Capabilities** — The complete set of DDL operations the Oracle backend supports, expressed through backend-specific expression classes. This is the backend's full power.
2. **ActiveRecord DDL Derivation** — What the framework can automatically generate from model class declarations. This is a convenient subset that covers most common use cases.

---

# Part 1: Backend DDL Capabilities

The Oracle backend supports the following DDL operations. Each operation is expressed through a corresponding expression class — you construct the expression, then execute it via the backend.

## Supported Operations

| Operation | Oracle Support | Expression Class |
|-----------|---------------|-----------------|
| CREATE TABLE | ✅ | `CreateTableExpression` |
| ALTER TABLE | ✅ | `AlterTableExpression` |
| DROP TABLE | ✅ | `DropTableExpression` |
| CREATE INDEX | ✅ | `CreateIndexExpression` |
| DROP INDEX | ✅ | `DropIndexExpression` |
| CREATE VIEW | ✅ | `CreateViewExpression` |
| DROP VIEW | ✅ | `DropViewExpression` |
| TRUNCATE | ✅ | `TruncateExpression` |
| CREATE SCHEMA | ✅ | `CreateSchemaExpression` |
| CREATE SEQUENCE | ✅ | `OracleCreateSequenceExpression` |
| CREATE MATERIALIZED VIEW | ✅ | `OracleCreateMaterializedViewExpression` |
| CREATE SYNONYM | ✅ | `OracleCreateSynonymExpression` |
| PARTITION DDL | ✅ | `OraclePartitionExpression` |

## CREATE TABLE

### Basic Usage

```python
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)

# Construct a CREATE TABLE expression
# Backend-specific DDL expression
```

### Oracle-Specific Table Options

Oracle supports several table-level options that are not available in other backends:

| Option | Description |
|--------|-------------|
| `TABLESPACE` | Storage tablespace for the table |
| `ORGANIZATION` | Table organization: `HEAP`, `INDEX`, `EXTERNAL` |
| `CLUSTER` | Cluster table with other tables |
| `PARTITION BY` | Table partitioning strategy |
| `ROWDEPENDENCIES` | Row-level dependency tracking |

### IF NOT EXISTS

| Backend | IF NOT EXISTS |
|---------|--------------|
| SQLite | Yes |
| MySQL | Yes |
| PostgreSQL | Yes |
| **Oracle** | **No** (use exception handling) |

Oracle does not support `IF NOT EXISTS` syntax. Use PL/SQL exception handling or check existence first.

### Temporary Tables

| Backend | Temporary Table Support |
|---------|----------------------|
| SQLite | Yes (but limited) |
| MySQL | Yes |
| PostgreSQL | Yes |
| **Oracle** | **Yes** (`ON COMMIT PRESERVE/DELETE ROWS`) |

Oracle temporary tables persist for the session or transaction:

```sql
CREATE GLOBAL TEMPORARY TABLE temp_results (
    id NUMBER,
    name VARCHAR2(100)
) ON COMMIT PRESERVE ROWS;
```

## ALTER TABLE

### Adding Columns

| Backend | IF NOT EXISTS on ADD COLUMN |
|---------|--------------------------|
| SQLite | No (before 3.35.0) |
| MySQL | No |
| PostgreSQL | Yes |
| **Oracle** | **No** (use exception handling) |

### Dropping Columns

| Backend | IF EXISTS on DROP COLUMN | Minimum Version |
|---------|------------------------|----------------|
| SQLite | Yes | 3.35.0+ |
| MySQL | No | — |
| PostgreSQL | Yes | — |
| **Oracle** | **No** (use exception handling) | — |

### Renaming Columns

| Backend | RENAME COLUMN |
|---------|--------------|
| SQLite | Yes (3.25.0+) |
| MySQL | No (use `CHANGE COLUMN`) |
| PostgreSQL | Yes |
| **Oracle** | **Yes** (`RENAME COLUMN ... TO`) |

### Changing Column Types

| Backend | Syntax |
|---------|--------|
| MySQL | `MODIFY COLUMN` or `CHANGE COLUMN` |
| PostgreSQL | `ALTER COLUMN ... TYPE` |
| SQLite | Limited |
| **Oracle** | **`MODIFY (col TYPE ...)`** |

Oracle uses `MODIFY` for column type changes:

```sql
ALTER TABLE users MODIFY (email VARCHAR2(500));
```

## DROP TABLE

| Backend | IF EXISTS | CASCADE/RESTRICT |
|---------|----------|-----------------|
| SQLite | Yes | No |
| MySQL | Yes | Parsed but ignored |
| PostgreSQL | Yes | Yes (default: `RESTRICT`) |
| **Oracle** | **No** | **CASCADE CONSTRAINTS** |

Oracle supports `CASCADE CONSTRAINTS` for dropping tables with foreign key dependencies.

## CREATE INDEX

### Index Types

| Backend | Supported Index Types |
|---------|---------------------|
| SQLite | B-tree only |
| MySQL | BTREE, HASH |
| PostgreSQL | BTREE, HASH, GIN, GiST, SP-GiST, BRIN |
| **Oracle** | **BTREE, BITMAP, DOMAIN** |

### Partial Indexes

| Backend | Partial Index Support |
|---------|---------------------|
| SQLite | Yes (since 3.8.0) |
| MySQL | No |
| PostgreSQL | Yes |
| **Oracle** | **No** (use function-based indexes) |

### Functional Indexes

| Backend | Functional Index Support |
|---------|------------------------|
| SQLite | Yes |
| MySQL | No |
| PostgreSQL | Yes |
| **Oracle** | **Yes** (function-based indexes) |

### Concurrent Index Creation

| Backend | CONCURRENTLY Support |
|---------|---------------------|
| SQLite | No |
| MySQL | No |
| PostgreSQL | Yes |
| **Oracle** | **No** (all index creation is blocking) |

### Full-Text Indexes

| Backend | Full-Text Syntax |
|---------|-----------------|
| SQLite | FTS5 virtual table |
| MySQL | `FULLTEXT INDEX` (InnoDB, MySQL 5.6+) |
| PostgreSQL | GIN index on `tsvector` column |
| **Oracle** | **Oracle Text** (`CREATE INDEX ... INDEXTYPE IS CTXSYS.CONTEXT`) |

## CREATE VIEW

| Backend | OR REPLACE | TEMPORARY | Materialized | WITH CHECK OPTION |
|---------|-----------|----------|-------------|------------------|
| SQLite | No | No | No | No |
| MySQL | Yes | Yes | No | Yes |
| PostgreSQL | Yes | Yes | Yes | Yes |
| **Oracle** | **Yes** | **No** | **Yes** | **Yes** |

## TRUNCATE

| Backend | Supported | RESTART IDENTITY | CASCADE |
|---------|----------|-----------------|---------|
| SQLite | No (use `DELETE FROM`) | N/A | N/A |
| MySQL | Yes | No | No |
| PostgreSQL | Yes | Yes | Yes |
| **Oracle** | **Yes** | **No** | **CASCADE** (Oracle uses different syntax) |

## Schema Support

| Backend | Schemas | CREATE/DROP SCHEMA |
|---------|---------|-------------------|
| SQLite | No | No |
| MySQL | No (schema = database) | `CREATE DATABASE` (synonym) |
| PostgreSQL | Yes (true namespaces) | Yes |
| **Oracle** | **Yes** (users/schemas) | **Yes** (CREATE USER grants schema) |

## Sequences

| Backend | Sequences | AUTO_INCREMENT Mechanism |
|---------|----------|------------------------|
| SQLite | No | `AUTOINCREMENT` on `INTEGER PRIMARY KEY` |
| MySQL | No | `AUTO_INCREMENT` column attribute |
| PostgreSQL | Yes (`SERIAL`, `IDENTITY`) | `SERIAL` or `GENERATED ... AS IDENTITY` |
| **Oracle** | **Yes** (first-class citizens) | **`CREATE SEQUENCE` + `NEXTVAL/CURRVAL`** |

Oracle uses sequences as the primary auto-increment mechanism:

```python
# Create a sequence
OracleCreateSequenceExpression(
    name="users_seq",
    start_with=1,
    increment_by=1,
    nocycle=True,
    cache=20
)

# Use in a table
CREATE TABLE users (
    id NUMBER DEFAULT users_seq.NEXTVAL PRIMARY KEY,
    name VARCHAR2(100)
);
```

## Partitioning

| Backend | Partitioning | Strategies |
|---------|-------------|------------|
| SQLite | **No** | — |
| MySQL | Yes (5.1+) | RANGE, LIST, HASH, KEY, COLUMNS, subpartitioning |
| PostgreSQL | Yes (PG 10+) | RANGE, LIST, HASH |
| **Oracle** | **Yes** (comprehensive) | **RANGE, LIST, HASH, COMPOSITE, INTERVAL** |

Oracle has the most comprehensive partitioning support:

```python
# Oracle partitioning examples
OraclePartitionExpression(
    partition_by="RANGE",
    columns=["created_at"],
    interval="NUMTOYMINTERVAL(1, 'MONTH')",  # Interval partitioning
    partitions=[
        {"name": "p_old", "values_less_than": ("2024-01-01",)},
        {"name": "p_2024", "values_less_than": ("2025-01-01",)},
    ]
)
```

> **Note**: Partition DDL is not integrated into the model declaration layer. You must use backend-specific expression classes directly. See [Partitioning](../backend_specific_features/partition.md) for details.

## Backend-Specific Expression Classes

### Oracle

| Expression | Purpose |
|-----------|---------|
| `OracleCreateSequenceExpression` | CREATE SEQUENCE with Oracle-specific options |
| `OracleCreateMaterializedViewExpression` | Materialized view with refresh strategies |
| `OracleCreateSynonymExpression` | CREATE PUBLIC/PRIVATE SYNONYM |
| `OraclePartitionExpression` | Partition DDL (RANGE, LIST, HASH, INTERVAL) |
| `OracleAnalyzeTableExpression` | ANALYZE TABLE statistics |
| `OracleFlashbackTableExpression` | FLASHBACK TABLE to point in time |
| `OraclePivotExpression` | PIVOT row-to-column transformation |
| `OracleHintExpression` | Oracle optimizer hints |

## Checking Feature Support

Use the protocol system to check if a feature is available:

```python
from rhosocial.activerecord.backend.dialect.protocols import (
    TableSupport,
    IndexSupport,
    SchemaSupport,
    PartitionSupport,
    SequenceSupport,
)

dialect = backend.dialect

if isinstance(dialect, SequenceSupport):
    if dialect.supports_sequences():
        # Use sequence DDL expression classes
        ...

if isinstance(dialect, PartitionSupport):
    if dialect.supports_table_partitioning():
        # Use partition DDL expression classes
        ...
```

## What Happens with Unsupported Features

When you use a feature on a backend that does not support it:

| Feature | Oracle Behavior |
|---------|----------------|
| `AUTO_INCREMENT` | Error — use sequences |
| `SERIAL` type | Error — use NUMBER with sequence |
| `GIN` index type | Error — use Oracle Text |
| `CONCURRENTLY` | Error — all index creation is blocking |
| `IF NOT EXISTS` | Error — use exception handling |
| Partial indexes | Error — use function-based indexes |

**Rule of thumb**: Options that are structural (like `ENGINE`) are silently ignored if unsupported. Options that change SQL syntax (like `GIN` index type) raise errors if unsupported.

---

# Part 2: ActiveRecord DDL Derivation

`ModelSchemaGenerator` derives DDL from your ActiveRecord model declarations. You define fields, table names, indexes, and constraints on the model class — the framework generates the SQL.

This is a convenient subset of the backend's full DDL capabilities. For features not covered here (partitioning, sequences, triggers, stored procedures), use the backend's expression classes directly (Part 1).

## What Can Be Derived from Models

| Feature | Model-Integrated | How to Use |
|---------|-----------------|------------|
| Table creation | Yes | `ModelSchemaGenerator.generate_create_table()` |
| Column definitions | Yes | Declare fields on the model class |
| Indexes | Yes | `indexes()` class method |
| Constraints | Yes | `UseConstraint` annotations |
| Table comment | Yes | `comment()` class method |
| Schema | Yes | `schema()` class method |
| Partitioning | **No** | Backend-specific expression classes only |
| Sequences | **No** | Backend-specific expression classes only |
| Triggers | **No** | Backend-specific expression classes only |
| Stored procedures | **No** | Backend-specific expression classes only |

## Creating a Table

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base import FieldProxy
from typing import ClassVar

class User(ActiveRecord):
    id: int | None = None
    username: str
    email: str
    age: int
    is_active: bool = True

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'users'
```

The generated SQL differs per backend:

| Backend | Generated SQL |
|---------|--------------|
| SQLite | `CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, ...)` |
| MySQL | ``CREATE TABLE IF NOT EXISTS `users` (`id` INT NOT NULL AUTO_INCREMENT, ...) ENGINE=InnoDB ...`` |
| PostgreSQL | `CREATE TABLE IF NOT EXISTS "users" ("id" SERIAL PRIMARY KEY, ...)` |
| **Oracle** | `CREATE TABLE "USERS" ("ID" NUMBER GENERATED AS IDENTITY PRIMARY KEY, ...)` |

Notice Oracle differences:
- **Identifier case**: Unquoted identifiers fold to uppercase
- **No IF NOT EXISTS**: Oracle does not support this syntax
- **IDENTITY columns**: Oracle 12c+ supports `GENERATED AS IDENTITY`
- **NUMBER type**: Oracle uses `NUMBER` instead of `INTEGER`/`BIGINT`

## Backend-Specific Table Options

### Oracle: TABLESPACE, STORAGE, PARTITIONING

```python
class User(ActiveRecord):
    @classmethod
    def tablespace(cls) -> str:
        return 'users_ts'

    @classmethod
    def partition_key(cls) -> str:
        return 'created_at'
```

**Note**: `TABLESPACE` and partitioning are Oracle-specific. If you use these on MySQL or PostgreSQL, they are silently ignored.

## Indexes

### Basic Index

```python
class User(ActiveRecord):
    email: str

    @classmethod
    def indexes(cls) -> list:
        return [
            {'columns': ['email']},
        ]
```

### Unique Index

```python
{'columns': ['email'], 'unique': True}
```

### Function-Based Index (Oracle-specific)

```python
{'columns': ['UPPER(email)'], 'type': 'FUNCTION-BASED'}
```

### Bitmap Index (Oracle-specific)

```python
{'columns': ['status'], 'type': 'BITMAP'}
```

| Backend | Partial Index Support |
|---------|---------------------|
| SQLite | Yes |
| MySQL | No |
| PostgreSQL | Yes |
| **Oracle** | **No** (use function-based indexes) |

## Schema

```python
class User(ActiveRecord):
    @classmethod
    def schema(cls) -> str:
        return 'HR'  # Oracle schema (user)
```

**Note**: On Oracle, `schema()` maps to the schema owner. On MySQL, it maps to `USE database`. On PostgreSQL, it produces schema-qualified table names. On SQLite, it is silently ignored.

## Generating DDL SQL

You can generate DDL SQL without executing it:

```python
from rhosocial.activerecord.base.ddl_generator import DDLGenerator

class User(ActiveRecord):
    id: int | None = None
    username: str
    email: str

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'users'

# Generate CREATE TABLE SQL
create_sql = DDLGenerator.generate_create_table(User)
print(create_sql)
```

This generates the SQL for the currently configured backend's dialect.

## Running DDL

To execute DDL, use the backend directly:

```python
# Sync
with User.connection() as conn:
    conn.execute(create_sql)

# Async
async with User.connection() as conn:
    await conn.execute(create_sql)
```

Or use the DDL generator's built-in execution:

```python
# This generates and executes in one step
DDLGenerator.create_table(User)
```

---

## See Also

- [Field Types](../backend_specific_features/field_types.md) — DataType hierarchy and backend-specific types
- [Indexing](../backend_specific_features/indexing.md) — index types and optimization
- [Partitioning](../backend_specific_features/partition.md) — table partitioning strategies
- [Dialect Expressions](../backend_specific_features/dialect.md) — feature detection and protocol system
- [Core: DDL](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/modeling/ddl)

💡 *AI Prompt:* "How does DDL generation differ between Oracle and PostgreSQL?"
