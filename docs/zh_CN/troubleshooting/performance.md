# 性能问题

## 概述

本节介绍 Oracle 的性能问题和优化方法。

## 慢查询分析

### 启用统计信息

Oracle 基于成本的优化器（CBO）使用表统计信息来选择执行计划。请保持统计信息新鲜：

```sql
-- 收集表的统计信息
EXEC DBMS_STATS.GATHER_TABLE_STATS('APP', 'USERS');
```

### 使用 EXPLAIN PLAN 分析查询

使用 `ExplainExpression` 类生成 `EXPLAIN PLAN` 语句。**不要执行原始 EXPLAIN SQL**——请使用表达式系统：

```python
from rhosocial.activerecord.backend.expression.statements.explain import (
    ExplainExpression,
    ExplainOptions,
)

# 构建查询表达式
query = User.query().where(User.c.name == "Tom").select(User.c.id, User.c.name)

# 基本 EXPLAIN PLAN
explain = ExplainExpression(dialect, statement=query)
sql, params = explain.to_sql()
# 输出：EXPLAIN PLAN FOR SELECT "id", "name" FROM "users" WHERE "name" = :1
```

> **注意**：Oracle 的 `EXPLAIN PLAN` 只**估算**执行计划——它不执行语句。请使用 `DBMS_XPLAN.DISPLAY` 显示执行计划和实际统计信息。

## 常见性能问题

### 1. 缺少索引

```sql
CREATE INDEX idx_users_name ON users(name);
```

### 2. SELECT *

```python
# 避免 SELECT *，只查询所需列
users = User.query().select(User.c.id, User.c.name).all()
```

### 3. N+1 查询问题

```python
# 使用 with_() 预加载关联数据，避免 N+1 查询
users = User.query().with_('posts').all()

# 加载嵌套关联
users = User.query().with_('posts.comments').all()

# 带查询修饰符加载
users = User.query().with_(('posts', lambda q: q.limit(5))).all()
```

### 4. 分页性能

Oracle 分页因版本而异：

| Oracle 版本 | 分页方式 | 说明 |
|----------------|---------------------|-------|
| 12c+ | `FETCH FIRST` / `OFFSET` | ANSI 标准，支持偏移量 |
| 12c 之前 | `ROWNUM` | 不支持偏移量；使用子查询或分析函数 `ROW_NUMBER()` |

### 5. WHERE 子句中的 NVL / COALESCE

除非存在基于函数的索引，否则避免在索引列上包裹函数——这会阻止索引使用：

```python
# ❌ 阻止索引使用
users = User.query().where(FunctionCall(dialect, "NVL", User.c.name, Literal(dialect, "")) == "Tom").all()

# ✅ 在 SELECT 中使用 COALESCE，或使用基于函数的索引
```

## 连接超时与连接池

对于高并发场景，请通过 `OracleConnectionConfig` 使用 `oracledb` 连接池：

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

## Oracle 特定调优

| 领域 | 建议 |
|------|----------------|
| 使用绑定变量 | 避免硬解析；后端使用参数化查询 |
| 收集统计信息 | 保持 CBO 执行计划最优 |
| VARCHAR2 长度 | EXTENDED（32k）与标准 4000 字节影响存储 |
| 会话数量 | 监控 `v$session` 防止连接耗尽 |
| 自动内存管理 | 使用 `SGA_TARGET`/`PGA_AGGREGATE_TARGET` 自动调优 |

💡 *AI Prompt:* "如何优化 Oracle 查询性能？"