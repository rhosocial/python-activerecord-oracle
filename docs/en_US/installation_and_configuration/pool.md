# Connection Management

## Overview

The Oracle backend uses a single-connection lifecycle by default. Connection pooling can be configured through the `OracleConnectionConfig` pool parameters.

## Single Connection

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

class User(ActiveRecord):
    __table_name__ = "users"

config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password"
)
User.configure(config, OracleBackend)

# Connection is established on first use
user = User.find_one(1)
```

## Connection Pooling

Configure pool parameters for production use:

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password",
    pool_min=2,
    pool_max=10,
    pool_increment=1,
    pool_get_timeout=30
)
```

## FastAPI Integration

```python
from fastapi import FastAPI
from rhosocial.activerecord.model import AsyncActiveRecord
from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend, OracleConnectionConfig

app = FastAPI()

class User(AsyncActiveRecord):
    __table_name__ = "users"

@app.on_event("startup")
async def startup():
    config = OracleConnectionConfig(
        host="localhost",
        port=1521,
        database="ORCLPDB1",
        username="system",
        password="password"
    )
    User.configure(config, AsyncOracleBackend)

@app.on_event("shutdown")
async def shutdown():
    await User.close_connection()
```

## See Also

- [Connection Configuration](configuration.md) — connection parameters
- [Installation Guide](installation.md) — setup instructions
