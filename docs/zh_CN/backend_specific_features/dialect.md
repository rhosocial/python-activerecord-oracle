# Oracle 方言表达式

## 概述

Oracle 提供了自己的 SQL 方言，包含 PL/SQL 扩展。

## DUAL 表

Oracle 在没有 FROM 子句的查询中需要 DUAL 表：

```python
# 使用 DUAL 表的查询表达式
# SELECT 1 FROM DUAL
```

## NULLS FIRST/LAST

Oracle 支持 ORDER BY 中的 NULLS FIRST 和 NULLS LAST：

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause

# 使用 NULLS LAST 排序
order = OrderByClause(dialect, expressions=[Column(dialect, "name")], nulls="LAST")
```

## 字符串函数

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall

# CONCAT 函数
func = FunctionCall(dialect, "CONCAT", Column(dialect, "first_name"), Column(dialect, "last_name"))
sql, params = func.to_sql()
# sql: CONCAT(first_name, last_name)
# params: ()
```

## FETCH FIRST 子句

Oracle 12c+ 支持 ANSI FETCH FIRST：

```python
# SELECT * FROM users FETCH FIRST 10 ROWS ONLY
```

## 另请参阅

- [字段类型](./field_types.md) — Oracle 数据类型
- [索引](./indexing.md) — 索引类型

💡 *AI 提示：* "Oracle 的 SQL 方言与标准 SQL 有何不同？"
