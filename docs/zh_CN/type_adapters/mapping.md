# Oracle 到 Python 的类型映射

## 概述

Oracle 后端负责将 Oracle 数据库数据类型转换为 Python 对象，并将 Python 对象转换回 Oracle 可识别的格式。

## 类型映射表

### 数值类型

| Oracle 类型 | Python 类型 | 描述 |
|-------------|-------------|-------------|
| NUMBER | int | 整数 |
| NUMBER(p, s) | Decimal / float | 带精度/小数位的精确数值 |
| NUMBER(38) | int | 大整数 |
| FLOAT | float | 带精度的浮点数 |
| BINARY_FLOAT | float | 32 位浮点数 |
| BINARY_DOUBLE | float | 64 位浮点数 |

### 字符串类型

| Oracle 类型 | Python 类型 | 描述 |
|-------------|-------------|-------------|
| CHAR | str | 定长字符串 |
| VARCHAR2 | str | 变长字符串（4000 字节，EXTENDED：32767） |
| NCHAR | str | 国家字符集定长字符串 |
| NVARCHAR2 | str | 国家字符集变长字符串 |
| CLOB | str | 字符大对象 |
| NCLOB | str | 国家字符集大对象 |
| LONG | str | 长文本（已弃用） |
| JSON | dict/list | JSON 文档（21c+ 原生；21c 之前为 VARCHAR2/CLOB） |

> **注意**：Oracle 将空字符串（`''`）视为 `NULL`。`OracleStringAdapter` 会将非 `Optional[str]` 字段的 `NULL` 转换回 `''`。

### 日期和时间类型

| Oracle 类型 | Python 类型 | 描述 |
|-------------|-------------|-------------|
| DATE | date / datetime | 日期和时间（精确到秒） |
| TIMESTAMP | datetime | 日期和时间（小数秒） |
| TIMESTAMP WITH TIME ZONE | datetime | 带时区（以 UTC 存储，返回时带 UTC tzinfo） |
| TIMESTAMP WITH LOCAL TIME ZONE | datetime | 本地时区 |
| INTERVAL YEAR TO MONTH | IntervalYearToMonth | 年月间隔 |
| INTERVAL DAY TO SECOND | IntervalDayToSecond | 天秒间隔 |

### 二进制类型

| Oracle 类型 | Python 类型 | 描述 |
|-------------|-------------|-------------|
| RAW | bytes | 变长二进制 |
| LONG RAW | bytes | 长二进制（已弃用） |
| BLOB | bytes | 二进制大对象（4GB） |
| BFILE | 文件句柄 | 外部二进制文件 |

### 特殊类型

| Oracle 类型 | Python 类型 | 描述 |
|-------------|-------------|-------------|
| BOOLEAN（通过 NUMBER(1)） | bool | 通过 `OracleBooleanAdapter` 模拟（1/0） |
| ENUM（通过 VARCHAR2 + CHECK） | enum.Enum | 通过 `OracleEnumAdapter` 模拟 |
| ROWID | OracleRowID | 18 字符扩展 ROWID |
| UROWID | OracleURowID | 通用行标识符 |
| XMLType | OracleXMLType | 原生 XML 存储 |
| SDO_GEOMETRY | SDOGeometry | 空间数据 |
| VECTOR | OracleVector | AI/ML 向量（23ai+） |

## 使用示例

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import UUIDMixin, DefaultTimestampMixin
from typing import ClassVar
from decimal import Decimal


class Product(UUIDMixin, DefaultTimestampMixin, ActiveRecord):
    name: str           # 自动映射到 VARCHAR2
    price: Decimal      # 自动映射到 NUMBER(10, 2)
    description: str    # 自动映射到 VARCHAR2/CLOB
    metadata: dict      # 自动映射到 JSON（21c+）或 VARCHAR2/CLOB

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'products'
```

## 另请参阅

- [字段类型](../backend_specific_features/field_types.md) — Oracle 数据类型分类
- [自定义适配器](./custom.md) — 扩展类型支持
- [时区处理](./timezone.md) — 时间戳和时区配置

💡 *AI Prompt:* "为什么存储货币值推荐使用 DECIMAL 而不是 FLOAT？"