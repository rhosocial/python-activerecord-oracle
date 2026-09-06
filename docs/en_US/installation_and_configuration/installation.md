# Installation Guide

## Prerequisites

- Python 3.8+ (including 3.13t/3.14t free-threaded builds)
- Oracle Database 12c+ (recommended: 18c+)
- Oracle Instant Client (for thick mode) or network access (for thin mode)

## Installation

### Basic Installation

```bash
pip install rhosocial-activerecord-oracle
```

This installs:
- `rhosocial-activerecord` (core library)
- `oracledb` (Oracle database driver)

### Development Installation

```bash
pip install rhosocial-activerecord-oracle[dev]
```

### With Test Dependencies

```bash
pip install rhosocial-activerecord-oracle[test]
```

## Driver Modes

The `oracledb` driver supports two modes:

### Thin Mode (No Oracle Client Required)

The default mode. No Oracle Instant Client installation needed. Connects directly to Oracle Database over TCP/IP.

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password"
)
```

### Thick Mode (Oracle Instant Client Required)

Requires Oracle Instant Client to be installed. Provides additional features like Oracle Call Interface (OCI) and support for some Oracle-specific protocols.

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password",
    mode="thick"  # Use thick mode
)
```

## Verifying Installation

```python
import oracledb
print(f"oracledb version: {oracledb.__version__}")

from rhosocial.activerecord.backend.impl.oracle import OracleBackend
print("Oracle backend imported successfully")
```

## See Also

- [Connection Configuration](configuration.md) — connection parameters
- [SSL/TLS Configuration](ssl.md) — secure connections
- [Connection Management](pool.md) — connection pooling
