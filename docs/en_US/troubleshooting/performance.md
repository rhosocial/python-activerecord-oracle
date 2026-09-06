# Performance Issues

## Overview

This section covers Oracle performance issues and optimization methods.

## Slow Query Analysis

### Enabling Statistics

Oracle's cost-based optimizer (CBO) uses table statistics to choose execution plans. Keep statistics fresh:

```sql
-- Gather statistics for a table
EXEC DBMS_STATS.GATHER_TABLE_STATS('APP', 'USERS');
```

### Using EXPLAIN PLAN to Analyze Queries

Use the `ExplainExpression` class to generate an `EXPLAIN PLAN` statement. **Do not execute raw EXPLAIN SQL** — use the expression system instead:

```python
from rhosocial.activerecord.backend.expression.statements.explain import (
    ExplainExpression,
    ExplainOptions,
)

# Build a query expression
query = User.query().where(User.c.name == "Tom").select(User.c.id, User.c.name)

# Basic EXPLAIN PLAN
explain = ExplainExpression(dialect, statement=query)
sql, params = explain.to_sql()
# Output: EXPLAIN PLAN FOR SELECT "id", "name" FROM "users" WHERE "name" = :1
```

> **Note**: Oracle's `EXPLAIN PLAN` only **estimates** the plan — it does not execute the statement. Use `DBMS_XPLAN.DISPLAY` to display the plan and actual statistics.

## Common Performance Issues

### 1. Missing Index

```sql
CREATE INDEX idx_users_name ON users(name);
```

### 2. SELECT *

```python
# Avoid SELECT *, only query required columns
users = User.query().select(User.c.id, User.c.name).all()
```

### 3. N+1 Query Problem

```python
# Use with_() to eagerly load related data and avoid N+1 queries
users = User.query().with_('posts').all()

# Load nested relations
users = User.query().with_('posts.comments').all()

# Load with query modifier
users = User.query().with_(('posts', lambda q: q.limit(5))).all()
```

### 4. Pagination Performance

Oracle pagination differs by version:

| Oracle Version | Pagination Approach | Notes |
|----------------|---------------------|-------|
| 12c+ | `FETCH FIRST` / `OFFSET` | ANSI standard, supports offset |
| Pre-12c | `ROWNUM` | No offset support; use subquery or analytic `ROW_NUMBER()` |

### 5. NVL / COALESCE in WHERE Clauses

Avoid wrapping indexed columns in functions unless function-based indexes exist — it prevents index usage:

```python
# ❌ Prevents index usage
users = User.query().where(FunctionCall(dialect, "NVL", User.c.name, Literal(dialect, "")) == "Tom").all()

# ✅ Use COALESCE in SELECT, or use a function-based index
```

## Connection Timeouts and Pooling

For high-concurrency scenarios, use `oracledb` connection pooling via `OracleConnectionConfig`:

```python
config = OracleConnectionConfig(
    host='localhost',
    database='ORCLPDB1',
    pool_min=1,
    pool_max=20,
    pool_increment=1,
    pool_get_timeout=30,
)
```

## Oracle-Specific Tuning

| Area | Recommendation |
|------|----------------|
| Use bind variables | Prevents hard parsing; the backend uses parameterized queries |
| Gather statistics | Keeps the CBO plan optimal |
| VARCHAR2 length | EXTENDED (32k) vs standard 4000 bytes affects storage |
| Number of sessions | Monitor `v$session` for connection exhaustion |
| Automatic memory | Use `SGA_TARGET`/`PGA_AGGREGATE_TARGET` auto-tuning |

💡 *AI Prompt:* "How to optimize Oracle query performance?"

