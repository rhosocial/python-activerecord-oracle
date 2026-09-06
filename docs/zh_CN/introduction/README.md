# 简介

## Oracle 后端概述

`rhosocial-activerecord-oracle` 是 rhosocial-activerecord 核心库的 Oracle 数据库后端实现。它提供完整的 ActiveRecord 模式支持，专门针对 Oracle 数据库特性进行优化，包括 PL/SQL 语法、序列、闪回查询、层次查询、空间数据等。

## 同步与异步

Oracle 后端提供功能等效的同步和异步 API。文档将使用同步示例，但异步 API 用法相同——只需将方法调用替换为其异步等效项。

### 命名约定

框架在所有后端使用一致的命名约定：

| 组件 | 同步 | 异步 |
|------|------|------|
| 后端类 | `OracleBackend` | `AsyncOracleBackend` |
| 事务管理器 | `OracleTransactionManager` | `AsyncOracleTransactionManager` |
| 连接配置 | `OracleConnectionConfig` | `OracleConnectionConfig`（共享） |
| 方言 | `OracleDialect` | `OracleDialect`（共享） |

连接配置和方言在同步和异步之间共享——它们是纯数据对象，不是活动连接。

### 模型层

模型层提供两个基类，具有相同的方法名但不同的调用约定：

| 操作 | `ActiveRecord`（同步） | `AsyncActiveRecord`（异步） |
|------|----------------------|----------------------------|
| 查找一个 | `find_one()` | `async find_one()` |
| 查找全部 | `find_all()` | `async find_all()` |
| 保存 | `save()` | `async save()` |
| 删除 | `delete()` | `async delete()` |
| 查询构建器 | `.query()` → `ActiveQuery` | `.query()` → `AsyncActiveQuery` |

方法名是**相同的**——没有 `a` 前缀约定。区别在类级别，而不是方法级别。

### 配置

```python
# 同步
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

class User(ActiveRecord):
    ...

User.configure(OracleConnectionConfig(...), OracleBackend)
user = User.find_one(1)

# 异步
from rhosocial.activerecord.model import AsyncActiveRecord
from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend, OracleConnectionConfig

class User(AsyncActiveRecord):
    ...

User.configure(OracleConnectionConfig(...), AsyncOracleBackend)
user = await User.find_one(1)
```

### 异步驱动要求

Oracle 使用 `oracledb` 库同时进行同步和异步操作。异步模式使用 `oracledb` thin 模式（无需安装 Oracle Client）：

| 后端 | 同步驱动 | 异步驱动 | 备注 |
|------|----------|----------|------|
| Oracle | `oracledb` | `oracledb`（thin 模式） | 同一库，原生异步 |

**重要**: 异步后端延迟加载其组件，以便在仅使用同步 API 时不需要异步驱动包。如果您导入 `AsyncOracleBackend` 但异步驱动未安装，将在导入时收到 `ImportError`。

## 快速链接

- **[与核心库的关系](./relationship.md)**: 了解 Oracle 后端如何与核心库协同工作
- **[支持的版本](./supported_versions.md)**: 查看支持的 Oracle、Python 和依赖版本

## 已知限制和怪癖

每个数据库都有自己的行为，与 SQL 标准不同。本节记录可能让您感到意外的 Oracle 特定怪癖。

| 怪癖 | 描述 |
|------|------|
| ROWNUM 分页 | 12c 之前使用 ROWNUM（不支持偏移量）；12c+ 使用 FETCH FIRST/OFFSET |
| 序列自增 | 使用 NEXTVAL/CURRVAL；序列不属于表列 |
| RETURNING INTO 语法 | 需要输出绑定变量，而不是简单的 RETURNING |
| 空字符串 = NULL | Oracle 将空字符串视为 NULL |
| 无原生 BOOLEAN | 通过适配器模拟（通常为 1/0） |
| CURRVAL 需要 NEXTVAL | 在会话中必须先调用 NEXTVAL 再调用 CURRVAL |
| 标识符大小写折叠 | 未加引号的标识符折叠为大写 |
| 无 `EXPLAIN ANALYZE` | EXPLAIN PLAN 仅估算；不执行语句 |
| 无部分索引 | Oracle 不支持索引上的 WHERE 子句 |
| 无 `CONCURRENTLY` 索引创建 | 所有索引创建都是阻塞的 |
| VARRAY 而非原生数组 | Oracle 使用 VARRAY 集合，而非原生数组类型 |

💡 *AI Prompt:* "什么是 ActiveRecord 模式？它与 DataMapper 模式有何不同？"
