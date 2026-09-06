# Oracle Field Types

## Overview

Oracle provides various data types for different use cases.

## Data Type Categories

### Numeric Types

| Type | Description |
|------|-------------|
| NUMBER | Variable precision (1-38 digits) |
| BINARY_FLOAT | 32-bit float |
| BINARY_DOUBLE | 64-bit float |
| FLOAT | NUMBER with precision |

### Character Types

| Type | Max Length |
|------|------------|
| CHAR | 2000 bytes |
| VARCHAR2 | 4000 bytes (EXTENDED: 32767 bytes) |
| NCHAR | 2000 bytes |
| NVARCHAR2 | 4000 bytes |
| CLOB | 4GB |
| NCLOB | 4GB |
| LONG | 2GB (deprecated) |

### Temporal Types

| Type | Description |
|------|-------------|
| DATE | Date and time (to second) |
| TIMESTAMP | Date and time (fractional seconds) |
| TIMESTAMP WITH TIME ZONE | With time zone |
| TIMESTAMP WITH LOCAL TIME ZONE | Local time zone |
| INTERVAL YEAR TO MONTH | Year-month interval |
| INTERVAL DAY TO SECOND | Day-second interval |

### Binary Types

| Type | Max Length |
|------|------------|
| BLOB | 4GB |
| BFILE | External file |
| RAW | 2000 bytes |
| LONG RAW | 2GB (deprecated) |

### Large Object Types

| Type | Description |
|------|-------------|
| CLOB | Character large object (4GB) |
| NCLOB | National character large object (4GB) |
| BLOB | Binary large object (4GB) |

## JSON Type

Oracle 21c+ provides native JSON type:

```python
class Product(ActiveRecord):
    __table_name__ = "products"
    name: str
    attributes: dict    # JSON
```

## See Also

- [Type Adapters](../type_adapters/README.md) — Type conversion

💡 *AI Prompt:* "What are the differences between VARCHAR2 and NVARCHAR2 in Oracle?"
