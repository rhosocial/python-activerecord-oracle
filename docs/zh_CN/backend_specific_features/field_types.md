# Oracle 字段类型

## 概述

Oracle 提供多种数据类型用于不同场景。

## 数据类型分类

### 数值类型

| 类型 | 说明 |
|------|------|
| NUMBER | 可变精度（1-38 位） |
| BINARY_FLOAT | 32 位浮点 |
| BINARY_DOUBLE | 64 位浮点 |
| FLOAT | 带精度的 NUMBER |

### 字符类型

| 类型 | 最大长度 |
|------|----------|
| CHAR | 2000 字节 |
| VARCHAR2 | 4000 字节（扩展：32767 字节） |
| NCHAR | 2000 字节 |
| NVARCHAR2 | 4000 字节 |
| CLOB | 4GB |
| NCLOB | 4GB |
| LONG | 2GB（已弃用） |

### 时间类型

| 类型 | 说明 |
|------|------|
| DATE | 日期和时间（到秒） |
| TIMESTAMP | 日期和时间（小数秒） |
| TIMESTAMP WITH TIME ZONE | 带时区 |
| TIMESTAMP WITH LOCAL TIME ZONE | 本地时区 |
| INTERVAL YEAR TO MONTH | 年月间隔 |
| INTERVAL DAY TO SECOND | 天秒间隔 |

### 二进制类型

| 类型 | 最大长度 |
|------|----------|
| BLOB | 4GB |
| BFILE | 外部文件 |
| RAW | 2000 字节 |
| LONG RAW | 2GB（已弃用） |

### 大对象类型

| 类型 | 说明 |
|------|------|
| CLOB | 字符大对象（4GB） |
| NCLOB | 国家字符大对象（4GB） |
| BLOB | 二进制大对象（4GB） |

## JSON 类型

Oracle 21c+ 提供原生 JSON 类型：

```python
class Product(ActiveRecord):
    __table_name__ = "products"
    name: str
    attributes: dict    # JSON
```

## 另请参阅

- [类型适配器](../type_adapters/README.md) — 类型转换

💡 *AI 提示：* "Oracle 中 VARCHAR2 和 NVARCHAR2 有什么区别？"
