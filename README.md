# rhosocial-activerecord-oracle ($\rho_{\mathbf{AR}\text{-oracle}}$)

[![PyPI version](https://badge.fury.io/py/rhosocial-activerecord-oracle.svg)](https://badge.fury.io/py/rhosocial-activerecord-oracle)
[![Python](https://img.shields.io/pypi/pyversions/rhosocial-activerecord-oracle.svg)](https://pypi.org/project/rhosocial-activerecord-oracle/)
[![Tests](https://github.com/rhosocial/python-activerecord-oracle/actions/workflows/test.yml/badge.svg)](https://github.com/rhosocial/python-activerecord-oracle/actions)
[![Coverage Status](https://codecov.io/gh/rhosocial/python-activerecord-oracle/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rhosocial/python-activerecord-oracle/tree/main)
[![Apache 2.0 License](https://img.shields.io/github/license/rhosocial/python-activerecord-oracle.svg)](https://github.com/rhosocial/python-activerecord-oracle/blob/main/LICENSE)
[![Powered by vistart](https://img.shields.io/badge/Powered_by-vistart-blue.svg)](https://github.com/vistart)

<div align="center">
    <img src="https://raw.githubusercontent.com/rhosocial/python-activerecord/main/docs/images/logo.svg" alt="rhosocial ActiveRecord Logo" width="200"/>
    <h3>Oracle Backend for rhosocial-activerecord</h3>
    <p><b>Enterprise PL/SQL Support · Flashback & Spatial · Sync & Async</b></p>
</div>

> **Note**: This is a backend implementation for [rhosocial-activerecord](https://github.com/rhosocial/python-activerecord). It cannot be used standalone.

## Why This Backend?

### 1. Oracle-Specific Optimizations

| Feature | This Backend | Generic Solutions |
|---------|-------------|-------------------|
| **RETURNING** | Native `RETURNING ... INTO` | Manual SELECT-after-write |
| **Flashback** | `AS OF TIMESTAMP`, `FLASHBACK TABLE` | Point-in-time recovery tooling |
| **Materialized Views** | Native MV support | Application-level caches |
| **Spatial** | `SDO_GEOMETRY` | External GIS systems |
| **Hierarchical Queries** | `CONNECT BY` | Application-level recursion |

### 2. True Sync-Async Parity

Same API surface for both sync and async operations:

```python
# Sync
users = User.query().where(User.c.age >= 18).all()

# Async - just add await
users = await User.query().where(User.c.age >= 18).all()
```

### 3. Built for Production

- **Connection pooling** with configurable pool sizes
- **Transaction support** with proper isolation levels
- **Error mapping** from Oracle error codes to Python exceptions
- **Type adapters** for Oracle-specific data types

## Quick Start

### Installation

```bash
pip install rhosocial-activerecord-oracle
```

### Basic Usage

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.oracle import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from typing import Optional

class User(ActiveRecord):
    __table_name__ = "users"
    id: Optional[int] = None
    name: str
    email: str

# Configure
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="YourPassword"
)
User.configure(config, OracleBackend)

# Use
user = User(name="Alice", email="alice@example.com")
user.save()

# Query with parameter binding
results = User.query().where("email = ?", ("alice@example.com",)).all()
```

> 💡 **AI Prompt**: "How do I configure connection pooling for Oracle?"

## Oracle-Specific Features

### RETURNING INTO

Retrieve inserted or updated rows in a single round-trip:

```python
# INSERT with RETURNING
user = User(name="Alice", email="alice@example.com")
user.save()
print(user.id)  # Populated automatically via RETURNING INTO
```

### Flashback Queries

Query historical state without restore:

```python
# As of a past timestamp
past_users = User.query().where(
    "id = ? AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '1' HOUR)",
    (1,),
).all()
```

### Hierarchical Queries

Oracle's `CONNECT BY` for tree-structured data:

```python
# Org hierarchy traversal
Employee.query().where(
    "START WITH manager_id IS NULL CONNECT BY PRIOR emp_id = manager_id"
).all()
```

### Materialized Views

Native materialized view support:

```python
# Refreshable MV DDL
User.query().where(
    "MATERIALIZED VIEW mv_users REFRESH FAST ON COMMIT"
).all()
```

## Requirements

- **Python**: 3.9+ (including 3.13t/3.14t free-threaded builds)
- **Core**: `rhosocial-activerecord>=1.0.0`
- **Driver**: `oracledb>=3.0.0`

## Oracle Version Compatibility

| Feature | Min Version | Notes |
|---------|-------------|-------|
| Basic operations | 18c+ | Core functionality |
| JSON | 18c+ | JSON data type, JSON_TABLE |
| RETURNING INTO | 8.0+ | DML returning |
| Flashback queries | 9.0+ | AS OF TIMESTAMP |
| Hierarchical queries | 8.0+ | CONNECT BY |
| Materialized views | 8.0+ | MV refresh strategies |
| Sequences | 8.0+ | CREATE SEQUENCE |
| Spatial | 8.1+ | SDO_GEOMETRY |
| PIVOT | 11.2+ | Row-to-column transforms |
| Identity columns | 12.1+ | GENERATED AS IDENTITY |
| PL/SQL procedures | 8.0+ | Stored procedures |
| Vector search | 23ai+ | AI Vector Search |

**Recommended**: Oracle 18c+ for optimal feature support.

## Get Started with AI Code Agents

This project supports AI-assisted development. Clone and open in your preferred tool:

```bash
git clone https://github.com/rhosocial/python-activerecord-oracle.git
cd python-activerecord-oracle
```

### Example AI Prompts

- "How do I configure connection pooling for Oracle?"
- "Show me how to use flashback queries"
- "How do I use RETURNING INTO with INSERT?"
- "Create a model with a hierarchical query"

### For Any LLM

Feed the documentation files in `docs/` to your preferred LLM for context-aware assistance.

## Testing

> ⚠️ **CRITICAL**: Tests MUST run serially. Do NOT use `pytest -n auto` or parallel execution.

```bash
# Run all tests
PYTHONPATH=src pytest tests/

# Run specific feature tests
PYTHONPATH=src pytest tests/rhosocial/activerecord_oracle_test/feature/basic/
PYTHONPATH=src pytest tests/rhosocial/activerecord_oracle_test/feature/query/
```

See the [Testing Documentation](https://github.com/rhosocial/python-activerecord/blob/main/.claude/testing.md) for details.

## Documentation

- **[Getting Started](docs/en_US/getting_started/)** — Installation and configuration
- **[Oracle Features](docs/en_US/oracle_specific_features/)** — Oracle-specific capabilities
- **[Type Adapters](docs/en_US/type_adapters/)** — Data type handling
- **[Transaction Support](docs/en_US/transaction_support/)** — Transaction management

## Comparison with Other Backends

| Feature | Oracle | PostgreSQL | SQLite |
|---------|--------|------------|--------|
| **RETURNING** | ✅ RETURNING INTO | ✅ RETURNING | ✅ RETURNING |
| **Flashback** | ✅ | ❌ | ❌ |
| **Hierarchical Queries** | ✅ CONNECT BY | ⚠️ WITH RECURSIVE | ⚠️ WITH RECURSIVE |
| **Materialized Views** | ✅ | ✅ | ❌ |
| **Arrays** | ⚠️ VARRAY | ✅ Native | ❌ |

> 💡 **AI Prompt**: "When should I choose Oracle over PostgreSQL for my project?"

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

[Apache License 2.0](LICENSE) — Copyright © 2026 [vistart](https://github.com/vistart)

---

<div align="center">
    <p><b>Built with ❤️ by the rhosocial team</b></p>
    <p><a href="https://github.com/rhosocial/python-activerecord-oracle">GitHub</a> · <a href="https://docs.python-activerecord.dev.rho.social/backends/oracle.html">Documentation</a> · <a href="https://pypi.org/project/rhosocial-activerecord-oracle/">PyPI</a></p>
</div>