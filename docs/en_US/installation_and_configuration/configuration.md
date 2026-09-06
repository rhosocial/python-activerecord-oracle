# Connection Configuration

## Basic Configuration

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

config = OracleConnectionConfig(
    host="localhost",       # Database server hostname
    port=1521,              # Database server port (default: 1521)
    database="ORCLPDB1",    # Oracle service name or SID
    username="system",      # Authentication username
    password="password"     # Authentication password
)
```

## Oracle-Specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `service_name` | `str` | `None` | Oracle service name (alternative to `database`) |
| `sid` | `str` | `None` | Oracle SID (alternative to `service_name`) |
| `dsn` | `str` | `None` | Full Data Source Name (overrides host/port/database) |
| `mode` | `str` | `None` | Connection mode: `SYSDBA`, `SYSOPER`, `thin`, `thick` |
| `encoding` | `str` | `"UTF-8"` | Character encoding |
| `nencoding` | `str` | `None` | National character encoding |
| `edition` | `str` | `None` | Edition name for Edition-Based Redefinition |

## DSN Format

The DSN (Data Source Name) is constructed automatically from `host`, `port`, and `database`:

- If `service_name` is set: `host:port/service_name`
- If `sid` is set: `host:port:sid`
- Otherwise: `host:port/database`

You can also provide a full DSN string directly:

```python
config = OracleConnectionConfig(
    dsn="localhost:1521/ORCLPDB1",
    username="system",
    password="password"
)
```

## Pool Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pool_min` | `int` | `None` | Minimum pool connections |
| `pool_max` | `int` | `None` | Maximum pool connections |
| `pool_increment` | `int` | `None` | Connection increment |
| `pool_get_timeout` | `int` | `None` | Timeout for getting connection from pool |

## Session Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stmtcachesize` | `int` | `20` | Statement cache size |
| `prefetchrows` | `int` | `None` | Number of rows to prefetch |
| `arraysize` | `int` | `100` | Array fetch size |
| `threaded` | `bool` | `True` | Use threaded connections |
| `events` | `bool` | `False` | Enable Oracle events |

## Named Connections

You can define named connections in Python modules for reuse:

```python
# myapp/connections/oracle_prod.py
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

def get_config():
    return OracleConnectionConfig(
        host="prod-db.example.com",
        port=1521,
        database="ORCLPDB1",
        username="app_user",
        password="secret"
    )

def get_backend():
    return OracleBackend
```

```bash
# Use via CLI
rhosocial-activerecord-oracle query \
    --named-connection myapp.connections.oracle_prod \
    "SELECT * FROM users"
```

## See Also

- [SSL/TLS Configuration](ssl.md) — secure connections
- [Connection Management](pool.md) — connection pooling
