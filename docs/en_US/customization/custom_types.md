# Custom Data Types

## Overview

The Oracle backend provides a type system for converting between Python types and Oracle column types. You can add support for new Python types by creating custom DataType subclasses.

## Type System Architecture

The type system has two layers:

1. **Core DataType hierarchy** — base classes for type conversion
2. **Backend-specific DataType** — Oracle-specific type handling

## Creating Custom Data Types

### Step 1: Define the DataType Subclass

```python
from rhosocial.activerecord.backend.impl.oracle.expression.types import OracleVarChar2Type

class PhoneNumberType(OracleVarChar2Type):
    """Custom data type for normalized phone numbers."""

    @staticmethod
    def to_sql(value, dialect):
        """Convert Python PhoneNumber to Oracle VARCHAR2 literal."""
        if value is None:
            return None
        return value.normalized

    @staticmethod
    def from_sql(value, dialect):
        """Convert Oracle VARCHAR2 to Python PhoneNumber."""
        if value is None:
            return None
        return PhoneNumber(value)
```

### Step 2: Register with @handles Decorator

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

### Step 3: Use in Models

```python
from rhosocial.activerecord import Model

class User(Model):
    __tablename__ = "users"
    id: int
    name: str
    phone: PhoneNumber  # Uses custom type
```

## Type Parameters

When defining custom types, consider these parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `precision` | Total digits for NUMBER | `NUMBER(10, 2)` |
| `scale` | Digits after decimal point | `NUMBER(10, 2)` |
| `length` | Maximum length for VARCHAR2 | `VARCHAR2(255)` |

## Oracle-Specific Types

The Oracle backend provides these built-in DataType subclasses:

| Oracle Type | DataType Class | Notes |
|-------------|----------------|-------|
| `NUMBER` | `OracleIntegerType`, `OracleSmallIntType` | Numeric |
| `NUMBER(38)` | `OracleBigIntType` | Large numeric |
| `VARCHAR2` | `OracleVarChar2Type` | Variable-length string (4000 / 32767 EXTENDED) |
| `NVARCHAR2` | `OracleNVarChar2Type` | National character string |
| `CHAR` | `OracleCharType` | Fixed-length string |
| `CLOB`/`NCLOB` | `OracleClobType`, `OracleNClobType` | Character large objects |
| `BLOB` | `OracleBlobType` | Binary large object |
| `RAW`/`LONG RAW` | `OracleRawType`, `OracleLongRawType` | Raw binary |
| `LONG` | `OracleLongType` | Long text (deprecated) |
| `XMLType` | `OracleXmlType` | Native XML storage |

> **Note**: Oracle has no native `BOOLEAN` or `ENUM` types. Booleans map to `NUMBER(1)` and enums are modeled as `VARCHAR2` with a `CHECK` constraint via adapters.

## See Also

- [Oracle Field Types](../backend_specific_features/field_types.md) — Oracle-specific data types
- [Type Mapping](../type_adapters/mapping.md) — Oracle to Python type conversion table
- [Core Custom Types Guide](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/template/customization/custom_types.md) — general customization patterns

💡 *AI Prompt:* "How do I add support for a custom Python type in Oracle?"

