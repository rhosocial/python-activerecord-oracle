# Custom Type Adapters

## Overview

Type adapters convert between Python values and Oracle database values. The Oracle backend provides a type registry for registering custom adapters.

## Type Registry

The type registry manages all type conversions:

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend

backend = OracleBackend(...)
registry = backend.type_registry
```

## Registering Custom Adapters

### Using the @handles Decorator

```python
from rhosocial.activerecord.backend.impl.oracle.adapters import OracleBooleanAdapter

@OracleBooleanAdapter.handles(MyClass)
class MyClassAdapter:
    @staticmethod
    def to_sql(value, dialect):
        """Convert Python MyClass to an Oracle value."""
        return json.dumps(value.__dict__)

    @staticmethod
    def from_sql(value, dialect):
        """Convert an Oracle value to Python MyClass."""
        return MyClass(**json.loads(value))
```

### Manual Registration

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

## SQLTypeAdapter Protocol

Custom adapters must implement the `SQLTypeAdapter` protocol:

```python
class SQLTypeAdapter(Protocol):
    @staticmethod
    def to_sql(value: Any, dialect: Any) -> Any:
        """Convert Python value to SQL value."""
        ...

    @staticmethod
    def from_sql(value: Any, dialect: Any) -> Any:
        """Convert SQL value to Python value."""
        ...
```

## BaseSQLTypeAdapter

For common patterns, extend `BaseSQLTypeAdapter`:

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

## Oracle-Specific Adapter Notes

The Oracle backend ships with a set of built-in adapters that follow Oracle conventions:

| Adapter | Oracle Type | Notes |
|---------|-------------|-------|
| `OracleBooleanAdapter` | `NUMBER(1)` | Boolean stored as `1`/`0` |
| `OracleDateTimeAdapter` | `TIMESTAMP` | Handles `TIMESTAMP WITH TIME ZONE` (UTC assumption) |
| `OracleDecimalAdapter` | `NUMBER` | Money as `float`/`Decimal` |
| `OracleJSONAdapter` | `VARCHAR2`/`CLOB`/`JSON` | Pre-21c as string, 21c+ native JSON |
| `OracleUUIDAdapter` | `CHAR(36)`/`RAW(16)` | No native UUID; string or bytes storage |
| `OracleEnumAdapter` | `VARCHAR2` + CHECK | No native ENUM |
| `OracleIntervalAdapter` | `INTERVAL` | YEAR TO MONTH / DAY TO SECOND |
| `OracleRowIDAdapter` | `ROWID`/`UROWID` | Extended ROWID (18-char) |
| `OracleSDOGeometryAdapter` | `SDO_GEOMETRY` | Spatial data |
| `OracleVectorAdapter` | `VECTOR` | AI/ML vector (23ai+) |
| `OracleXMLAdapter` | `XMLType` | Native XML storage |

## Type Conversion Flow

```
Python Value
    │
    ▼
to_sql(value, dialect)
    │
    ▼
Oracle Database Value
    │
    ▼
from_sql(value, dialect)
    │
    ▼
Python Value
```

## See Also

- [Type Mapping](../type_adapters/mapping.md) — Oracle to Python type conversion table
- [Custom Type Adapters](../type_adapters/custom.md) — extending type support
- [Core Custom Adapters Guide](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/template/customization/custom_adapters.md) — general customization patterns

💡 *AI Prompt:* "How do I register a custom type adapter for Oracle?"

