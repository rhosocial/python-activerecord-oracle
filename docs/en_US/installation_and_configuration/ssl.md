# SSL/TLS Configuration

## Overview

Oracle supports TLS encryption for client-server connections. The `oracledb` driver provides SSL/TLS configuration through connection parameters.

## Basic SSL Configuration

```python
config = OracleConnectionConfig(
    host="secure-db.example.com",
    port=2484,
    database="ORCLPDB1",
    username="system",
    password="password",
    # SSL parameters passed via conn_param
)
```

## SSL Connection Parameters

| Parameter | Description |
|-----------|-------------|
| `ssl_server_cert_dn` | Distinguished name of the server certificate |
| `ssl_allow_weak_dn` | Allow weak distinguished name matching |
| `wallet_location` | Path to Oracle wallet directory |
| `wallet_password` | Password for the Oracle wallet |

## See Also

- [Connection Configuration](configuration.md) — all connection parameters
