# Introduction

## Oracle Backend Overview

`rhosocial-activerecord-oracle` is the Oracle database backend implementation for the rhosocial-activerecord core library. It provides complete ActiveRecord pattern support, optimized specifically for Oracle database features including PL/SQL syntax, sequences, flashback queries, hierarchical queries, spatial data, and more.

## Synchronous and Asynchronous

The Oracle backend provides both synchronous and asynchronous APIs that are functionally equivalent. The documentation will use synchronous examples throughout, but the asynchronous API usage is identical — just replace method calls with their async equivalents.

### Naming Convention

The framework uses a consistent naming convention across all backends:

| Component | Sync | Async |
|-----------|------|-------|
| Backend class | `OracleBackend` | `AsyncOracleBackend` |
| Transaction manager | `OracleTransactionManager` | `AsyncOracleTransactionManager` |
| Connection config | `OracleConnectionConfig` | `OracleConnectionConfig` (shared) |
| Dialect | `OracleDialect` | `OracleDialect` (shared) |

The connection config and dialect are shared between sync and async — they are pure data objects, not active connections.

### Model Layer

The model layer provides two base classes with identical method names but different calling conventions:

| Operation | `ActiveRecord` (sync) | `AsyncActiveRecord` (async) |
|-----------|----------------------|----------------------------|
| Find one | `find_one()` | `async find_one()` |
| Find all | `find_all()` | `async find_all()` |
| Save | `save()` | `async save()` |
| Delete | `delete()` | `async delete()` |
| Query builder | `.query()` → `ActiveQuery` | `.query()` → `AsyncActiveQuery` |

The method names are **identical** — there is no `a` prefix convention. The distinction is at the class level, not the method level.

### Configuration

```python
# Synchronous
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

class User(ActiveRecord):
    ...

User.configure(OracleConnectionConfig(...), OracleBackend)
user = User.find_one(1)

# Asynchronous
from rhosocial.activerecord.model import AsyncActiveRecord
from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend, OracleConnectionConfig

class User(AsyncActiveRecord):
    ...

User.configure(OracleConnectionConfig(...), AsyncOracleBackend)
user = await User.find_one(1)
```

### Async Driver Requirements

Oracle uses the `oracledb` library for both sync and async operations. The async mode uses `oracledb` thin mode (no Oracle Client installation required):

| Backend | Sync Driver | Async Driver | Notes |
|---------|-------------|--------------|-------|
| Oracle | `oracledb` | `oracledb` (thin mode) | Same library, native async |

**Important**: The async backend lazy-loads its components to avoid requiring async driver packages when only the sync API is used. If you import `AsyncOracleBackend` and the async driver is not installed, you will get an `ImportError` at import time.

## Quick Links

- **[Relationship with Core Library](./relationship.md)**: Learn how the Oracle backend works with the core library
- **[Supported Versions](./supported_versions.md)**: View supported Oracle, Python, and dependency versions

## Known Limitations and Quirks

Every database has its own behavior that differs from the SQL standard. This section documents Oracle-specific quirks that may surprise you.

| Quirk | Description |
|-------|-------------|
| ROWNUM for pagination | Pre-12c uses ROWNUM (no offset support); 12c+ uses FETCH FIRST/OFFSET |
| Sequences for auto-increment | Use NEXTVAL/CURRVAL; sequences are not owned by table columns |
| RETURNING INTO syntax | Requires output bind variables, not simple RETURNING |
| Empty string = NULL | Oracle treats empty strings as NULL |
| No native BOOLEAN | Emulated via adapter (typically 1/0) |
| CURRVAL requires NEXTVAL | Must call NEXTVAL before CURRVAL in a session |
| Identifier case folding | Unquoted identifiers are folded to uppercase |
| No `EXPLAIN ANALYZE` | EXPLAIN PLAN only estimates; does not execute the statement |
| No partial indexes | Oracle does not support WHERE clauses on indexes |
| No `CONCURRENTLY` index creation | All index creation is blocking |
| VARRAY instead of native arrays | Oracle uses VARRAY collections, not native array types |

💡 *AI Prompt:* "What is the ActiveRecord pattern? How does it differ from DataMapper pattern?"
