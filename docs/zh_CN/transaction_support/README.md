# 事务支持

## 概述

rhosocial-activerecord 提供事务管理器来封装 Oracle 的事务语义。本节介绍 Oracle 后端的隔离级别、保存点和死锁处理。

## 事务管理器

### 同步

```python
# 使用事务管理器
with User.transaction():
    user = User(username='alice')
    user.save()
    # 事务在成功退出时提交
    # 事务在异常时回滚
```

### 异步

```python
# 使用异步事务管理器
async with User.transaction():
    user = User(username='alice')
    await user.save()
    # 事务在成功退出时提交
    # 事务在异常时回滚
```

事务管理器有同步和异步两种变体：
- `OracleTransactionManager` — 同步
- `AsyncOracleTransactionManager` — 异步

API 是相同的——唯一的区别是 `async with` 和 `with`。

## Oracle 事务行为

Oracle 使用**隐式事务**——DML 语句自动开始事务。标准 Oracle SQL 中没有显式 `BEGIN TRANSACTION` 语句（尽管 PL/SQL 块使用 `BEGIN...END`）。

```python
# Oracle 隐式启动事务
user = User(username='alice')
user.save()  # 隐式启动事务

# 事务在以下情况下提交：
# 1. 显式提交
# 2. 连接关闭
# 3. 新 DDL 语句（DDL 在 Oracle 中自动提交）
```

## 内容

- [隔离级别](isolation_level.md): Oracle 特定隔离语义
- [保存点](savepoint.md): 嵌套事务和条件回滚
- [死锁处理](deadlock.md): Oracle 死锁检测、错误代码和重试策略

## 另请参阅

- [核心：并行工作器模式](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/scenarios/parallel_workers) — 死锁预防原则

💡 *AI Prompt:* "隔离级别如何影响 Oracle 中的并发事务？"
