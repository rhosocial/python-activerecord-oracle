# 保存点支持

## 概述

保存点允许在事务内创建中间检查点，实现部分回滚。Oracle 完全支持保存点。

## 使用保存点

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

config = OracleConnectionConfig(
    host='localhost',
    database='ORCLPDB1',
    username='user',
    password='password',
)

backend = OracleBackend(connection_config=config)
backend.connect()

try:
    with backend.transaction_manager() as tx:
        # 操作 1
        cursor = backend.execute("INSERT INTO users (name) VALUES (:1)", ("Alice",))

        # 创建保存点
        tx.create_savepoint("sp1")

        try:
            # 操作 2（可能失败）
            backend.execute("INSERT INTO users (name) VALUES (:1)", ("Bob",))
        except Exception:
            # 回滚到保存点
            tx.rollback_savepoint("sp1")
            # 继续事务的其余部分
finally:
    backend.disconnect()
```

## Oracle 中的保存点行为

| 特性 | 行为 |
|---------|----------|
| 创建 | `SAVEPOINT <name>` |
| 回滚到 | `ROLLBACK TO SAVEPOINT <name>` |
| 释放 | 不支持——当事务提交或回滚时，Oracle 自动移除保存点；创建同名的新保存点也会替换旧保存点 |
| 嵌套保存点 | 支持——保存点可以嵌套 |

## 命名保存点

```python
# 创建保存点
tx.create_savepoint("savepoint_name")

# 回滚到保存点（部分回滚）
tx.rollback_savepoint("savepoint_name")

# 检查支持情况
tx.supports_savepoint()  # Oracle 返回 True
```

## 保存点与 DDL

> **重要**：在 Oracle 中，**DDL 语句会隐式提交事务**，因此会**清除所有保存点**。不要在 DDL 边界上创建保存点——应在 DDL 完成之后再创建。

```python
with User.transaction() as tx:
    # DDL 提交事务，清除之前的所有保存点
    User.backend().executescript("CREATE TABLE temp_t (id NUMBER)")

    # 在 DDL 之后创建保存点
    tx.create_savepoint("sp1")
    ...
```

## 另请参阅

- [事务支持概述](./README.md) — 事务管理器 API
- [隔离级别](./isolation_level.md) — Oracle 隔离语义
- [死锁处理](./deadlock.md) — ORA-00060 与重试策略

💡 *AI Prompt:* "什么是数据库保存点？它与完全回滚有何不同？"