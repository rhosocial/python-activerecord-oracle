# 自定义

## 概述

rhosocial-activerecord 设计为可扩展的。您可以在多个级别自定义框架：

1. **自定义表达式** — 为 Oracle 特定 SQL 语法创建新的表达式类
2. **自定义数据类型** — 为自定义列类型定义新的 DataType 子类
3. **自定义类型适配器** — 注册 Python 对象和 Oracle 值之间的转换器
4. **自定义方言扩展** — 向方言添加新的 `format_*()` 方法

## 自定义架构

框架使用**委托 + 组合**模式：

- **表达式**通过委托给其绑定方言的 `format_*()` 方法来生成 SQL
- **DataType**通过委托给 `dialect.format_data_type()` 来生成 DDL SQL
- **方言**由小型、专注的 mixin 组合而成（每个 mixin 为一个功能区域提供 `format_*()` 方法）
- **类型适配器**在 `TypeRegistry` 中注册 Python 值和数据库值之间的转换

这意味着您可以在不修改核心库的情况下扩展任何层。

## 何时自定义

| 需求 | 方法 |
|------|------|
| Oracle 有函数不在表达式库中 | 自定义表达式 |
| Oracle 有列类型不在类型系统中 | 自定义 DataType |
| Python 对象需要自定义序列化 | 自定义类型适配器 |
| 新 SQL 语法需要 Oracle 特定格式化 | 自定义方言 Mixin |

## 内容

- [自定义表达式](custom_expressions.md): 创建新的表达式类
- [自定义数据类型](custom_types.md): 定义新的 DataType 子类
- [自定义类型适配器](custom_adapters.md): 注册自定义转换器

## 另请参阅

- [方言表达式](../backend_specific_features/dialect.md) — 表达式系统架构
- [字段类型](../backend_specific_features/field_types.md) — DataType 层次结构
- [类型适配器](../type_adapters/README.md) — 类型转换系统

💡 *AI Prompt:* "如何为不在表达式库中的 Oracle 特定函数添加支持？"
