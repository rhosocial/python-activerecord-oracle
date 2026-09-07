# Oracle 特定功能

本节介绍与其他后端不同的 Oracle 特定功能。rhosocial-activerecord 对许多功能使用两层架构：**核心层**提供通用接口和默认实现，而后端的**方言层**覆盖格式化并添加后端特定的功能。

当您遇到本节中的功能时，请检查它是后端特定的扩展还是具有后端特定格式化的核心功能——文档将指示适用的层。

## 内容

- [方言表达式](dialect.md): 两层表达式系统——通用核心和 Oracle 特定覆盖
- [字段类型](field_types.md): 核心 DataType 层次结构和 Oracle 特定类型扩展
- [索引](indexing.md): Oracle 特定索引类型和优化策略
- [EXPLAIN](explain.md): 查询执行计划分析（Oracle 特定语法）
- [内省](introspection.md): 数据库元数据查询和模式检查
- [分区](partition.md): 表分区（Oracle 特定，可选）
- [DDL 特征 Spec](ddl_spec.md)：Oracle 方言认领的声明式 DDL Spec

## 功能亮点

| 功能 | 通用层 | Oracle 特定层 |
|------|--------|--------------|
| 表达式 | 核心表达式类（Column、Literal、FunctionCall 等） | 方言覆盖和 Oracle 特定表达式类 |
| 类型系统 | 核心 DataType 层次结构（IntegerType、VarCharType 等） | Oracle 特定 DataType 子类和类型适配器 |
| EXPLAIN | ExplainExpression 接口 | Oracle 特定 EXPLAIN PLAN 语法和结果解析 |
| 内省 | Introspector 接口 | Oracle 特定元数据查询（ALL_TABLES、USER_TAB_COLUMNS 等） |

## Oracle 独有功能

| 功能 | 描述 |
|------|------|
| **序列** | Oracle 原生序列对象，用于自增（NEXTVAL/CURRVAL） |
| **闪回查询** | 使用 `AS OF TIMESTAMP` 语法查询历史数据 |
| **层次查询** | 使用 `CONNECT BY` / `START WITH` 进行树遍历 |
| **空间数据** | SDO_GEOMETRY 类型，用于地理和几何数据 |
| **物化视图** | 可刷新物化视图，支持多种刷新策略 |
| **PIVOT/UNPIVOT** | 行到列和列到行转换 |
| **同义词** | 数据库对象别名，提供位置透明性 |
| **PL/SQL** | 存储过程、函数、包和触发器 |
| **高级队列** | Oracle AQ，用于基于消息的工作流 |
| **向量搜索** | AI/ML 向量类型，用于相似性搜索（23ai+） |

## 相关主题

- [类型适配器](../type_adapters/README.md) — 类型映射和自定义适配器
- [DDL 操作](../ddl/README.md) — 模式管理
- [核心：表达式系统](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/expression)
- [核心：后端系统](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend)
