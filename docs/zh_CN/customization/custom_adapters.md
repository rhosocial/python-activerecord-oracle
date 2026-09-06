# 自定义类型适配器

## 概述

类型适配器用于在 Python 值与 Oracle 数据库值之间进行转换。Oracle 后端提供了一个类型注册表，用于注册自定义适配器。

## 类型注册表

类型注册表管理所有类型转换：

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend

backend = OracleBackend(...)
registry = backend.type_registry
```

## 注册自定义适配器

### 使用 @handles 装饰器

```python
from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter

@OracleBooleanAdapter.handles(MyClass)
class MyClassAdapter:
    @staticmethod
    def to_sql(value, dialect):
        """将 Python MyClass 转换为 Oracle 值。"""
        return json.dumps(value.__dict__)

    @staticmethod
    def from_sql(value, dialect):
        """将 Oracle 值转换为 Python MyClass。"""
        return MyClass(**json.loads(value))
```

### 手动注册

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend

class MyAdapter:
    @staticmethod
    def to_sql(value, dialect):
        return str(value)

    @staticmethod
    def from_sql(value, dialect):
        return MyClass(value)

backend = OracleBackend(...)
backend.type_registry.register(MyClass, MyAdapter)
```

## SQLTypeAdapter 协议

自定义适配器必须实现 `SQLTypeAdapter` 协议：

```python
class SQLTypeAdapter(Protocol):
    @staticmethod
    def to_sql(value: Any, dialect: Any) -> Any:
        """将 Python 值转换为 SQL 值。"""
        ...

    @staticmethod
    def from_sql(value: Any, dialect: Any) -> Any:
        """将 SQL 值转换为 Python 值。"""
        ...
```

## BaseSQLTypeAdapter

对于常见模式，可以继承 `BaseSQLTypeAdapter`：

```python
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter

class MyDateAdapter(BaseSQLTypeAdapter):
    def _do_to_database(self, value, target_type, options):
        if value is None:
            return None
        return value.isoformat()

    def _do_from_database(self, value, target_type, options):
        if value is None:
            return None
        return date.fromisoformat(value)
```

## Oracle 特定适配器说明

Oracle 后端内置了一套遵循 Oracle 约定的适配器：

| 适配器 | Oracle 类型 | 说明 |
|---------|-------------|------|
| `OracleBooleanAdapter` | `NUMBER(1)` | 布尔值存储为 `1`/`0` |
| `OracleDateTimeAdapter` | `TIMESTAMP` | 处理 `TIMESTAMP WITH TIME ZONE`（假定 UTC） |
| `OracleDecimalAdapter` | `NUMBER` | 金额以 `float`/`Decimal` 表示 |
| `OracleJSONAdapter` | `VARCHAR2`/`CLOB`/`JSON` | 21c 之前为字符串，21c+ 为原生 JSON |
| `OracleUUIDAdapter` | `CHAR(36)`/`RAW(16)` | 无原生 UUID；以字符串或字节存储 |
| `OracleEnumAdapter` | `VARCHAR2` + CHECK | 无原生 ENUM |
| `OracleIntervalAdapter` | `INTERVAL` | YEAR TO MONTH / DAY TO SECOND |
| `OracleRowIDAdapter` | `ROWID`/`UROWID` | 扩展 ROWID（18 字符） |
| `OracleSDOGeometryAdapter` | `SDO_GEOMETRY` | 空间数据 |
| `OracleVectorAdapter` | `VECTOR` | AI/ML 向量（23ai+） |
| `OracleXMLAdapter` | `XMLType` | 原生 XML 存储 |

## 类型转换流程

```
Python 值
    │
    ▼
to_sql(value, dialect)
    │
    ▼
Oracle 数据库值
    │
    ▼
from_sql(value, dialect)
    │
    ▼
Python 值
```

## 另请参阅

- [类型映射](../type_adapters/mapping.md) — Oracle 到 Python 的类型转换表
- [自定义类型适配器](../type_adapters/custom.md) — 扩展类型支持
- [核心自定义适配器指南](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/template/customization/custom_adapters.md) — 通用自定义模式

💡 *AI Prompt:* "如何为 Oracle 注册自定义类型适配器？"

