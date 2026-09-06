# Index Types

## Overview

Oracle supports various index types for query optimization.

## Index Types

### B-Tree Index (Default)

```python
from rhosocial.activerecord.backend.expression import CreateIndexExpression

# Standard B-tree index
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_name",
    table_name="users",
    columns=["name"],
)
sql, params = create_idx.to_sql()
# sql: CREATE INDEX idx_users_name ON users (name)
```

### Bitmap Index

```python
# Bitmap index (for low-cardinality columns)
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_status",
    table_name="users",
    columns=["status"],
    index_type="BITMAP",
)
```

### Function-Based Index

```sql
-- Function-based index
CREATE INDEX idx_users_upper_name ON users(UPPER(name));
```

### Reverse Key Index

```python
# Reverse key index (for distributed systems)
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_id_rev",
    table_name="users",
    columns=["id"],
    index_type="REVERSE",
)
```

## Index Organization

### Index-Organized Table (IOT)

```sql
-- Create index-organized table
CREATE TABLE users (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100)
) ORGANIZATION INDEX;
```

## Index Optimization

```sql
-- Rebuild index
ALTER INDEX idx_users_name REBUILD;

-- Analyze index
ANALYZE INDEX idx_users_name VALIDATE STRUCTURE;
```

## See Also

- [EXPLAIN](./explain.md) — Query execution plans

💡 *AI Prompt:* "When should I use bitmap vs B-tree indexes in Oracle?"
