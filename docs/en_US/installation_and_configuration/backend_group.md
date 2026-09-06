# BackendGroup and BackendManager (Oracle)

This document describes how to use `BackendGroup` and `BackendManager` with the Oracle backend. For detailed API documentation, refer to the [core library documentation](../../../rhosocial-activerecord/docs/en_US/connection/connection_management.md).

## Quick Example

```python
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig
from rhosocial.activerecord.model import ActiveRecord


class User(ActiveRecord):
    name: str
    email: str


# Using context manager
with BackendGroup(
    name="main",
    models=[User],
    config=OracleConnectionConfig(
        host="localhost",
        port=1521,
        database="ORCLPDB1",
        username="app",
        password="secret",
    ),
    backend_class=OracleBackend,
) as group:
    user = User(name="John", email="john@example.com")
    user.save()

# Using with multiple groups via BackendManager
from rhosocial.activerecord.connection import BackendManager

manager = BackendManager()
manager.create_group(
    name="main",
    models=[User],
    config=OracleConnectionConfig(host="localhost", database="main_db"),
    backend_class=OracleBackend,
)
manager.create_group(
    name="stats",
    config=OracleConnectionConfig(host="localhost", database="stats_db"),
    backend_class=OracleBackend,
)

main_backend = manager.get_group("main").get_backend()
stats_backend = manager.get_group("stats").get_backend()
```

## Oracle-Specific Features

### Connection Pool Configuration

The Oracle backend supports `oracledb` connection pooling via `OracleConnectionConfig`:

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="app",
    password="secret",
    pool_min=1,
    pool_max=10,
    pool_increment=1,
    pool_get_timeout=30,
)
```

### DSN Configuration

```python
# Using service name (preferred)
config = OracleConnectionConfig(
    service_name="ORCLPDB1",
    username="app",
    password="secret",
)

# Using SID
config = OracleConnectionConfig(
    sid="ORCL",
    username="app",
    password="secret",
)

# Using a full DSN
config = OracleConnectionConfig(
    dsn="localhost:1521/ORCLPDB1",
    username="app",
    password="secret",
)
```

### SSL/TLS Configuration

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="app",
    password="secret",
    ssl_verify_cert=True,
    ssl_ca="/path/to/ca.pem",
)
```

## Thread Safety Note

Like the core single-connection model, a single `ActiveRecord` class's `__backend__` remains one connection. For multi-threaded parallel worker scenarios, prefer **multi-process** so each process establishes its own independent connection.

## Example Code

See the [core library connection management documentation](../../../rhosocial-activerecord/docs/en_US/connection/connection_management.md) for a multi-worker FastAPI example using backend connection management.

