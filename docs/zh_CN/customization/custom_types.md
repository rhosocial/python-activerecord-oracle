# 自定义数据类型

## 概述

Oracle 后端提供了一个类型系统，用于在 Python 类型和 Oracle 列类型之间进行转换。您可以通过创建自定义 DataType 子类来为新的 Python 类型添加支持。

## 类型系统架构

类型系统有两层：

1. **核心 DataType 层次结构** — 类型转换的基类
2. **后端特定的 DataType** — Oracle 特定类型处理

## 创建自定义数据类型

### 第 1 步：定义 DataType 子类

```python
from rhosocial.activerecord.backend.impl.oracle.expression.types import OracleVarChar2Type

class PhoneNumberType(OracleVarChar2Type):
    """用于规范化电话号码的自定义数据类型。"""

    @staticmethod
    def to_sql(value, dialect):
        """将 Python PhoneNumber 转换为 Oracle VARCHAR2 字面量。"""
        if value is None:
            return None
        return value.normalized

    @staticmethod
    def from_sql(value, dialect):
        """将 Oracle VARCHAR2 转换为 Python PhoneNumber。"""
        if value is None:
            return None
        return PhoneNumber(value)
```

### 第 2 步：使用 @handles 装饰器注册

```python
from rhosocial.activerecord.backend.impl.oracle.expression.types import OracleVarChar2Type

@OracleVarChar2Type.handles(PhoneNumber)
class PhoneNumberAdapter:
    @staticmethod
    def to_sql(value, dialect):
        return value.normalized

    @staticmethod
    def from_sql(value, dialect):
        return PhoneNumber(value)
```

### 第 3 步：在模型中使用

```python
from rhosocial.activerecord import Model

class User(Model):
    __tablename__ = "users"
    id: int
    name: str
    phone: PhoneNumber  # 使用自定义类型
```

## 类型参数

定义自定义类型时，请考虑以下参数：

| 参数 | 描述 | 示例 |
|-----------|-------------|---------|
| `precision` | NUMBER 的总位数 | `NUMBER(10, 2)` |
| `scale` | 小数点后的位数 | `NUMBER(10, 2)` |
| `length` | VARCHAR2 的最大长度 | `VARCHAR2(255)` |

## Oracle 特定类型

Oracle 后端提供以下内置 DataType 子类：

| Oracle 类型 | DataType 类 | 说明 |
|-------------|----------------|------|
| `NUMBER` | `OracleIntegerType`、`OracleSmallIntType` | 数值 |
| `NUMBER(38)` | `OracleBigIntType` | 大数值 |
| `VARCHAR2` | `OracleVarChar2Type` | 变长字符串（4000 / EXTENDED 32767） |
| `NVARCHAR2` | `OracleNVarChar2Type` | 国家字符集字符串 |
| `CHAR` | `OracleCharType` | 定长字符串 |
| `CLOB`/`NCLOB` | `OracleClobType`、`OracleNClobType` | 字符大对象 |
| `BLOB` | `OracleBlobType` | 二进制大对象 |
| `RAW`/`LONG RAW` | `OracleRawType`、`OracleLongRawType` | 原始二进制 |
| `LONG` | `OracleLongType` | 长文本（已弃用） |
| `XMLType` | `OracleXmlType` | 原生 XML 存储 |

> **注意**：Oracle 没有原生的 `BOOLEAN` 或 `ENUM` 类型。布尔值映射到 `NUMBER(1)`，枚举通过适配器建模为带 `CHECK` 约束的 `VARCHAR2`。

## 另请参阅

- [Oracle 字段类型](../backend_specific_features/field_types.md) — Oracle 特定数据类型
- [类型映射](../type_adapters/mapping.md) — Oracle 到 Python 的类型转换表
- [核心自定义类型指南](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/template/customization/custom_types.md) — 通用自定义模式

💡 *AI Prompt:* "如何为 Oracle 中的自定义 Python 类型添加支持？"

