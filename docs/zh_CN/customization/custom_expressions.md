# 自定义表达式

## 概述

Oracle 后端扩展了核心表达式类，支持 Oracle 特定的 SQL 语法。您可以创建新的表达式类型，以添加对 Oracle 独有 SQL 结构的支持。

## 表达式设计原则

表达式是**声明式**的——它们收集所有参数并将 SQL 生成委托给方言：

```python
class MyExpression(BaseExpression):
    def __init__(self, dialect, **params):
        self.dialect = dialect
        self.params = params

    def to_sql(self, dialect):
        # 委托给方言生成 SQL
        return dialect.format_my_expression(**self.params)
```

## 创建自定义表达式

### 第 1 步：定义表达式类

```python
from rhosocial.activerecord.backend.expression.base import BaseExpression
from rhosocial.activerecord.backend.expression import Column

class OracleNVLExpression(BaseExpression):
    """用于 NULL 替换的 Oracle NVL 表达式。"""

    def __init__(self, dialect, column, default):
        self.dialect = dialect
        self.column = column
        self.default = default

    def to_sql(self, dialect):
        return f"NVL({self.column.to_sql(dialect)}, {self.default})"
```

### 第 2 步：注册到方言

```python
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

class CustomOracleDialect(OracleDialect):
    def format_nvl(self, column, default):
        return f"NVL({column}, {default})"
```

### 第 3 步：在代码中使用

```python
# 构建表达式并检查生成的 SQL 和参数
column = Column(dialect, "name")
expr = BinaryExpression(dialect, column, "=", Literal(dialect, "Tom"))
sql, params = expr.to_sql()
# sql: name = :1
# params: ('Tom',)
```

> **注意**：请使用表达式类（`BinaryExpression`、`Column`、`Literal`、`FunctionCall`），而不是原始字符串查询。构建表达式后，调用 `expr.to_sql()` 以获取 `sql` 和 `params`。

## Oracle 特定函数示例

使用 `FunctionCall` 调用 Oracle 函数，如 `NVL`、`COALESCE`、`DECODE` 和 `TO_CHAR`：

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall

# NVL 函数
func = FunctionCall(dialect, "NVL", Column(dialect, "name"), Literal(dialect, "unknown"))
sql, params = func.to_sql()
# sql: NVL(name, :1)
# params: ('unknown',)
```

## 运算符混入

使用运算符混入来获得常见的比较和算术操作：

```python
from rhosocial.activerecord.backend.expression.operators import ComparisonMixin, ArithmeticMixin

class MyExpression(ComparisonMixin, ArithmeticMixin, BaseExpression):
    pass

# 现在支持 ==、!=、<、>、+、-、*、/ 等
expr = MyExpression(dialect, Column(dialect, "amount")) > 100
```

## 序列化

表达式支持序列化/反序列化，用于缓存和日志记录：

```python
# 序列化
data = expr.serialize()

# 反序列化
expr = BaseExpression.deserialize(data)
```

## 另请参阅

- [核心表达式系统](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/expression) — 表达式基类和运算符
- [Oracle 方言](../backend_specific_features/dialect.md) — Oracle 特定 SQL 函数

💡 *AI Prompt:* "如何为 NVL 之类的 Oracle 函数创建自定义表达式？"

