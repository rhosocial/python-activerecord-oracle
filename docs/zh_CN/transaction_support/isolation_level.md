# 事务隔离级别

## 概述

Oracle 支持两种 SQL 标准隔离级别：**READ COMMITTED**（默认）和 **SERIALIZABLE**。与 MySQL 不同，Oracle **不支持** READ UNCOMMITTED 和 REPEATABLE READ。

## 隔离级别比较

| 隔离级别 | 脏读 | 不可重复读 | 幻读 | Oracle 支持 |
|-----------------|------------|---------------------|--------------|----------------|
| READ UNCOMMITTED | 可能 | 可能 | 可能 | ❌ 不支持 |
| READ COMMITTED（默认） | 不可能 | 可能 | 可能 | ✅ 支持 |
| REPEATABLE READ | 不可能 | 不可能 | 可能 | ⚠️ 通过 SERIALIZABLE 模拟 |
| SERIALIZABLE | 不可能 | 不可能 | 不可能 | ✅ 支持 |

Oracle 的默认隔离级别是 **READ COMMITTED**。它使用 **MVCC**（多版本并发控制），因此读不阻塞写，写也不阻塞读。

## 设置隔离级别

事务管理器仅在请求 `SERIALIZABLE` 时才执行 `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`。READ COMMITTED 是默认值，无需显式设置：

```python
from rhosocial.activerecord.backend.transaction import IsolationLevel
from rhosocial.activerecord.backend.impl.oracle import OracleTransactionManager

# SERIALIZABLE
with Account.transaction(isolation_level=IsolationLevel.SERIALIZABLE):
    # 所有读取都看到一致的快照
    ...

# READ COMMITTED（默认）——每条语句看到最新的已提交数据
with Account.transaction():
    ...
```

## 隔离级别详解

### READ COMMITTED（默认）

每条语句只能看到已提交的数据。这是 Oracle 的默认值，适合大多数应用场景，在并发性和数据一致性之间取得平衡。

```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

### SERIALIZABLE

最高隔离级别。事务看到的是事务开始时的一致快照，可防止不可重复读和幻读。

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

**注意事项**：

- 当并发事务修改了可串行化事务要修改的数据时，可能出现 `ORA-08177: can't serialize access for this transaction` 错误
- 这不是真正的串行执行——它依赖于快照隔离

### 事务模式（READ ONLY / READ WRITE）

Oracle 还支持事务访问模式，事务管理器将其单独暴露：

```python
from rhosocial.activerecord.backend.transaction import TransactionMode

# READ ONLY——不允许 DML，只允许查询
with Account.transaction(mode=TransactionMode.READ_ONLY):
    total = Account.sum('balance')

# READ WRITE（默认）
with Account.transaction(mode=TransactionMode.READ_WRITE):
    ...
```

## 从其他数据库映射

| 数据库默认值 | Oracle 对应 |
|------------------|-------------------|
| MySQL REPEATABLE READ | 不可直接使用；如需严格的快照语义，请使用 SERIALIZABLE |
| PostgreSQL READ COMMITTED | 与 Oracle READ COMMITTED 相同 |

💡 *AI Prompt:* "什么是脏读、不可重复读和幻读？"