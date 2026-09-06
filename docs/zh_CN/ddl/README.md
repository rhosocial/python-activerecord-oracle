# DDL 操作

## 概述

本节介绍 Oracle 后端的 DDL（数据定义语言）操作。DDL 定义您的数据库模式——表、索引、视图和其他对象。

**重要**: rhosocial-activerecord 中的所有 DDL 都是**基于表达式的**。您在 Python 中定义模式，框架生成 SQL，然后通过后端执行。下面的示例使用 `ActiveRecord` 以简洁起见，但 `AsyncActiveRecord` 工作方式完全相同——DDL 生成是纯计算，不涉及 I/O。

DDL 章节分为两部分：

1. **后端 DDL 功能** — Oracle 后端支持的完整 DDL 操作集，通过后端特定表达式类表示。这是后端的全部功能。
2. **ActiveRecord DDL 派生** — 框架可以从模型类声明自动生成的内容。这是一个便捷子集，涵盖大多数常见用例。

---

# 第 1 部分：后端 DDL 功能

Oracle 后端支持以下 DDL 操作。每个操作通过相应的表达式类表示——您构建表达式，然后通过后端执行。

## 支持的操作

| 操作 | Oracle 支持 | 表达式类 |
|------|------------|---------|
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

## IF NOT EXISTS

| 后端 | IF NOT EXISTS |
|------|--------------|
| SQLite | 是 |
| MySQL | 是 |
| PostgreSQL | 是 |
| **Oracle** | **否**（使用异常处理） |

Oracle 不支持 `IF NOT EXISTS` 语法。请使用 PL/SQL 异常处理或先检查是否存在。

## 临时表

| 后端 | 临时表支持 |
|------|-----------|
| SQLite | 是（但有限） |
| MySQL | 是 |
| PostgreSQL | 是 |
| **Oracle** | **是**（`ON COMMIT PRESERVE/DELETE ROWS`） |

Oracle 临时表在会话或事务期间持续存在：

```sql
CREATE GLOBAL TEMPORARY TABLE temp_results (
    id NUMBER,
    name VARCHAR2(100)
) ON COMMIT PRESERVE ROWS;
```

## DROP TABLE

| 后端 | IF EXISTS | CASCADE/RESTRICT |
|------|----------|-----------------|
| SQLite | 是 | 否 |
| MySQL | 是 | 解析但忽略 |
| PostgreSQL | 是 | 是（默认：`RESTRICT`） |
| **Oracle** | **否** | **CASCADE CONSTRAINTS** |

Oracle 支持 `CASCADE CONSTRAINTS` 用于删除具有外键依赖关系的表。

## CREATE INDEX

### 索引类型

| 后端 | 支持的索引类型 |
|------|---------------|
| SQLite | 仅 B-tree |
| MySQL | BTREE、HASH |
| PostgreSQL | BTREE、HASH、GIN、GiST、SP-GiST、BRIN |
| **Oracle** | **BTREE、BITMAP、DOMAIN** |

### 部分索引

| 后端 | 部分索引支持 |
|------|-------------|
| SQLite | 是（3.8.0+） |
| MySQL | 否 |
| PostgreSQL | 是 |
| **Oracle** | **否**（使用函数索引） |

### 函数索引

| 后端 | 函数索引支持 |
|------|-------------|
| SQLite | 是 |
| MySQL | 否 |
| PostgreSQL | 是 |
| **Oracle** | **是**（函数索引） |

### 并发索引创建

| 后端 | CONCURRENTLY 支持 |
|------|------------------|
| SQLite | 否 |
| MySQL | 否 |
| PostgreSQL | 是 |
| **Oracle** | **否**（所有索引创建都是阻塞的） |

## CREATE VIEW

| 后端 | OR REPLACE | 临时 | 物化 | WITH CHECK OPTION |
|------|-----------|------|------|------------------|
| SQLite | 否 | 否 | 否 | 否 |
| MySQL | 是 | 是 | 否 | 是 |
| PostgreSQL | 是 | 是 | 是 | 是 |
| **Oracle** | **是** | **否** | **是** | **是** |

## 序列

| 后端 | 序列 | AUTO_INCREMENT 机制 |
|------|------|-------------------|
| SQLite | 否 | `INTEGER PRIMARY KEY` 上的 `AUTOINCREMENT` |
| MySQL | 否 | `AUTO_INCREMENT` 列属性 |
| PostgreSQL | 是（`SERIAL`、`IDENTITY`） | `SERIAL` 或 `GENERATED ... AS IDENTITY` |
| **Oracle** | **是**（一等公民） | **`CREATE SEQUENCE` + `NEXTVAL/CURRVAL`** |

Oracle 使用序列作为主要的自增机制：

```python
# 创建序列
OracleCreateSequenceExpression(
    name="users_seq",
    start_with=1,
    increment_by=1,
    nocycle=True,
    cache=20
)

# 在表中使用
CREATE TABLE users (
    id NUMBER DEFAULT users_seq.NEXTVAL PRIMARY KEY,
    name VARCHAR2(100)
);
```

## 分区

| 后端 | 分区 | 策略 |
|------|------|------|
| SQLite | **否** | — |
| MySQL | 是（5.1+） | RANGE、LIST、HASH、KEY、COLUMNS、子分区 |
| PostgreSQL | 是（PG 10+） | RANGE、LIST、HASH |
| **Oracle** | **是**（全面） | **RANGE、LIST、HASH、COMPOSITE、INTERVAL** |

Oracle 拥有最全面的分区支持：

```python
# Oracle 分区示例
OraclePartitionExpression(
    partition_by="RANGE",
    columns=["created_at"],
    interval="NUMTOYMINTERVAL(1, 'MONTH')",  # 间隔分区
    partitions=[
        {"name": "p_old", "values_less_than": ("2024-01-01",)},
        {"name": "p_2024", "values_less_than": ("2025-01-01",)},
    ]
)
```

> **注意**: 分区 DDL 未集成到模型声明层中。您必须直接使用后端特定表达式类。详情请参阅[分区](../backend_specific_features/partition.md)。

## 后端特定表达式类

### Oracle

| 表达式 | 用途 |
|--------|------|
| `OracleCreateSequenceExpression` | CREATE SEQUENCE（Oracle 特定选项） |
| `OracleCreateMaterializedViewExpression` | 物化视图（刷新策略） |
| `OracleCreateSynonymExpression` | CREATE PUBLIC/PRIVATE SYNONYM |
| `OraclePartitionExpression` | 分区 DDL（RANGE、LIST、HASH、INTERVAL） |
| `OracleAnalyzeTableExpression` | ANALYZE TABLE 统计信息 |
| `OracleFlashbackTableExpression` | FLASHBACK TABLE 到时间点 |
| `OraclePivotExpression` | PIVOT 行到列转换 |
| `OracleHintExpression` | Oracle 优化器提示 |

## 检查功能支持

使用协议系统检查功能是否可用：

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
        # 使用序列 DDL 表达式类
        ...

if isinstance(dialect, PartitionSupport):
    if dialect.supports_table_partitioning():
        # 使用分区 DDL 表达式类
        ...
```

## 不支持功能的处理

当您在不支持该功能的后端上使用某个功能时：

| 功能 | Oracle 行为 |
|------|------------|
| `AUTO_INCREMENT` | 错误——使用序列 |
| `SERIAL` 类型 | 错误——使用带序列的 NUMBER |
| `GIN` 索引类型 | 错误——使用 Oracle Text |
| `CONCURRENTLY` | 错误——所有索引创建都是阻塞的 |
| `IF NOT EXISTS` | 错误——使用异常处理 |
| 部分索引 | 错误——使用函数索引 |

**经验法则**: 结构性选项（如 `ENGINE`）如果不支持会被静默忽略。更改 SQL 语法的选项（如 `GIN` 索引类型）如果不支持会引发错误。

---

# 第 2 部分：ActiveRecord DDL 派生

`ModelSchemaGenerator` 从您的 ActiveRecord 模型声明中派生 DDL。您在模型类上定义字段、表名、索引和约束——框架生成 SQL。

这是后端完整 DDL 功能的便捷子集。对于此处未涵盖的功能（分区、序列、触发器、存储过程），请直接使用后端的表达式类（第 1 部分）。

## 可以从模型派生的功能

| 功能 | 模型集成 | 使用方式 |
|------|---------|---------|
| 表创建 | 是 | `ModelSchemaGenerator.generate_create_table()` |
| 列定义 | 是 | 在模型类上声明字段 |
| 索引 | 是 | `indexes()` 类方法 |
| 约束 | 是 | `UseConstraint` 注解 |
| 表注释 | 是 | `comment()` 类方法 |
| 模式 | 是 | `schema()` 类方法 |
| 分区 | **否** | 仅后端特定表达式类 |
| 序列 | **否** | 仅后端特定表达式类 |
| 触发器 | **否** | 仅后端特定表达式类 |
| 存储过程 | **否** | 仅后端特定表达式类 |

## 创建表

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

生成的 SQL 因后端而异：

| 后端 | 生成的 SQL |
|------|-----------|
| SQLite | `CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, ...)` |
| MySQL | ``CREATE TABLE IF NOT EXISTS `users` (`id` INT NOT NULL AUTO_INCREMENT, ...) ENGINE=InnoDB ...`` |
| PostgreSQL | `CREATE TABLE IF NOT EXISTS "users" ("id" SERIAL PRIMARY KEY, ...)` |
| **Oracle** | `CREATE TABLE "USERS" ("ID" NUMBER GENERATED AS IDENTITY PRIMARY KEY, ...)` |

注意 Oracle 的差异：
- **标识符大小写**: 未加引号的标识符折叠为大写
- **无 IF NOT EXISTS**: Oracle 不支持此语法
- **IDENTITY 列**: Oracle 12c+ 支持 `GENERATED AS IDENTITY`
- **NUMBER 类型**: Oracle 使用 `NUMBER` 而不是 `INTEGER`/`BIGINT`

## 生成 DDL SQL

您可以不执行而生成 DDL SQL：

```python
from rhosocial.activerecord.base.ddl_generator import DDLGenerator

# 生成 CREATE TABLE SQL
create_sql = DDLGenerator.generate_create_table(User)
print(create_sql)
```

这将为当前配置的后端方言生成 SQL。

## 运行 DDL

要执行 DDL，请直接使用后端：

```python
# 同步
with User.connection() as conn:
    conn.execute(create_sql)

# 异步
async with User.connection() as conn:
    await conn.execute(create_sql)
```

或使用 DDL 生成器的内置执行：

```python
# 这会在一步中生成并执行
DDLGenerator.create_table(User)
```

---

## 另请参阅

- [字段类型](../backend_specific_features/field_types.md) — DataType 层次结构和后端特定类型
- [索引](../backend_specific_features/indexing.md) — 索引类型和优化
- [分区](../backend_specific_features/partition.md) — 表分区策略
- [方言表达式](../backend_specific_features/dialect.md) — 功能检测和协议系统
- [核心：DDL](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/modeling/ddl)

💡 *AI Prompt:* "Oracle 和 PostgreSQL 的 DDL 生成有何不同？"
