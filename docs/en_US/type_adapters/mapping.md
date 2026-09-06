# Oracle to Python Type Mapping

## Overview

The Oracle backend is responsible for converting Oracle database data types to Python objects, and converting Python objects back to Oracle-recognized formats.

## Type Mapping Table

### Numeric Types

| Oracle Type | Python Type | Description |
|-------------|-------------|-------------|
| NUMBER | int | Integer |
| NUMBER(p, s) | Decimal / float | Exact numeric with precision/scale |
| NUMBER(38) | int | Large integer |
| FLOAT | float | Float with precision |
| BINARY_FLOAT | float | 32-bit float |
| BINARY_DOUBLE | float | 64-bit float |

### String Types

| Oracle Type | Python Type | Description |
|-------------|-------------|-------------|
| CHAR | str | Fixed-length string |
| VARCHAR2 | str | Variable-length string (4000 bytes, EXTENDED: 32767) |
| NCHAR | str | National fixed-length string |
| NVARCHAR2 | str | National variable-length string |
| CLOB | str | Character large object |
| NCLOB | str | National character large object |
| LONG | str | Long text (deprecated) |
| JSON | dict/list | JSON document (21c+ native; pre-21c as VARCHAR2/CLOB) |

> **Note**: Oracle treats empty strings (`''`) as `NULL`. The `OracleStringAdapter` converts `NULL` back to `''` for non-`Optional[str]` fields.

### Date and Time Types

| Oracle Type | Python Type | Description |
|-------------|-------------|-------------|
| DATE | date / datetime | Date and time (to second) |
| TIMESTAMP | datetime | Date and time (fractional seconds) |
| TIMESTAMP WITH TIME ZONE | datetime | With time zone (stored as UTC, returned with UTC tzinfo) |
| TIMESTAMP WITH LOCAL TIME ZONE | datetime | Local time zone |
| INTERVAL YEAR TO MONTH | IntervalYearToMonth | Year-month interval |
| INTERVAL DAY TO SECOND | IntervalDayToSecond | Day-second interval |

### Binary Types

| Oracle Type | Python Type | Description |
|-------------|-------------|-------------|
| RAW | bytes | Variable-length binary |
| LONG RAW | bytes | Long binary (deprecated) |
| BLOB | bytes | Binary large object (4GB) |
| BFILE | file handle | External binary file |

### Special Types

| Oracle Type | Python Type | Description |
|-------------|-------------|-------------|
| BOOLEAN (via NUMBER(1)) | bool | Emulated via `OracleBooleanAdapter` (1/0) |
| ENUM (via VARCHAR2 + CHECK) | enum.Enum | Emulated via `OracleEnumAdapter` |
| ROWID | OracleRowID | 18-char extended ROWID |
| UROWID | OracleURowID | Universal row identifier |
| XMLType | OracleXMLType | Native XML storage |
| SDO_GEOMETRY | SDOGeometry | Spatial data |
| VECTOR | OracleVector | AI/ML vector (23ai+) |

## Usage Example

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import UUIDMixin, DefaultTimestampMixin
from typing import ClassVar
from decimal import Decimal


class Product(UUIDMixin, DefaultTimestampMixin, ActiveRecord):
    name: str           # Automatically maps to VARCHAR2
    price: Decimal      # Automatically maps to NUMBER(10, 2)
    description: str    # Automatically maps to VARCHAR2/CLOB
    metadata: dict      # Automatically maps to JSON (21c+) or VARCHAR2/CLOB

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'products'
```

## See Also

- [Field Types](../backend_specific_features/field_types.md) — Oracle data type categories
- [Custom Adapters](./custom.md) — extending type support
- [Timezone Handling](./timezone.md) — timestamp and timezone configuration

💡 *AI Prompt:* "Why is DECIMAL recommended over FLOAT for storing monetary values?"

