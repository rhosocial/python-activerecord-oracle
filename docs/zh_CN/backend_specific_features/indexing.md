# 索引类型

## 概述

Oracle 支持多种索引类型用于查询优化。

## 索引类型

### B-Tree 索引（默认）

```python
from rhosocial.activerecord.backend.expression import CreateIndexExpression

# 标准 B-tree 索引
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_name",
    table_name="users",
    columns=["name"],
)
sql, params = create_idx.to_sql()
# sql: CREATE INDEX idx_users_name ON users (name)
```

### 位图索引

```python
# 位图索引（用于低基数列）
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_status",
    table_name="users",
    columns=["status"],
    index_type="BITMAP",
)
```

### 基于函数的索引

```sql
-- 基于函数的索引
CREATE INDEX idx_users_upper_name ON users(UPPER(name));
```

### 反向键索引

```python
# 反向键索引（用于分布式系统）
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_id_rev",
    table_name="users",
    columns=["id"],
    index_type="REVERSE",
)
```

## 索引组织

### 索引组织表 (IOT)

```sql
-- 创建索引组织表
CREATE TABLE users (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100)
) ORGANIZATION INDEX;
```

## 索引优化

```sql
-- 重建索引
ALTER INDEX idx_users_name REBUILD;

-- 分析索引
ANALYZE INDEX idx_users_name VALIDATE STRUCTURE;
```

## 另请参阅

- [EXPLAIN](./explain.md) — 查询执行计划

💡 *AI 提示：* "Oracle 中何时应该使用位图索引而不是 B-Tree 索引？"
