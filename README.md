<!-- README.md -->
# rhosocial-activerecord-oracle ($\rho_{\mathbf{AR}\text{-oracle}}$)

[![PyPI version](https://badge.fury.io/py/rhosocial-activerecord-oracle.svg)](https://badge.fury.io/py/rhosocial-activerecord-oracle)
[![Python](https://img.shields.io/pypi/pyversions/rhosocial-activerecord-oracle.svg)](https://pypi.org/project/rhosocial-activerecord-oracle/)
[![Tests](https://github.com/rhosocial/python-activerecord-oracle/actions/workflows/test.yml/badge.svg)](https://github.com/rhosocial/python-activerecord-oracle/actions)
[![Coverage Status](https://codecov.io/gh/rhosocial/python-activerecord-oracle/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rhosocial/python-activerecord-oracle/tree/main)
[![Apache 2.0 License](https://img.shields.io/github/license/rhosocial/python-activerecord-oracle.svg)](https://github.com/rhosocial/python-activerecord-oracle/blob/main/LICENSE)
[![Supported Oracle versions](https://img.shields.io/badge/Oracle-18c%7C21c%7C23-blue?logo=oracle)](docs/en_US/README.md)

<div align="center">
    <img src="https://raw.githubusercontent.com/rhosocial/python-activerecord/main/docs/images/logo.svg" alt="rhosocial ActiveRecord Logo" width="200"/>
    <h3>Oracle Backend for rhosocial-activerecord</h3>
    <p><b>RETURNING INTO · Flashback · CONNECT BY · Spatial · Sync &amp; async</b></p>
</div>

An Oracle database backend for [rhosocial-activerecord](https://github.com/rhosocial/python-activerecord) — use your Oracle data through the same ActiveRecord models, queries, and lifecycle API. Cannot be used standalone.

**Keywords:** oracle, orm, activerecord, pydantic, python, oracledb, async, type-safe

## Why This Project?

The core `rhosocial-activerecord` library intentionally stays free of database-specific code. This package supplies the Oracle half: install it alongside the core to point your models at an Oracle Database, with no changes to how you declare models or write queries.

Where Oracle has something extra to offer, this backend exposes it through the familiar API rather than hiding it:

- **`RETURNING INTO`** on `INSERT`/`UPDATE`/`DELETE` — generated keys are available immediately after `save()`, with no follow-up `SELECT`.
- **Flashback** and **hierarchical (`CONNECT BY`) queries** through the query builder.
- **Spatial (`SDO_GEOMETRY`) columns** and spatial indexes, plus native `JSON`, `JSON Duality`, and `vector` types on newer releases.
- **Sync and async** with identical method names — add `await` for the async model.

You get native Oracle behavior without leaking SQL dialect details into your models.

## Quick Start

### Installation

```bash
pip install rhosocial-activerecord-oracle
```

Requires Python 3.8+ and the core `rhosocial-activerecord` library. The example below needs a reachable Oracle instance (18c or newer).

### End-to-End Example

```python
"""Save as demo.py and run against a reachable Oracle instance."""
from typing import Annotated, ClassVar

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.expression.types import VarCharType
from rhosocial.activerecord.base import FieldProxy, UseSqlType
from rhosocial.activerecord.field import DefaultTimestampMixin, IntegerPKMixin
from rhosocial.activerecord.model import ActiveRecord


class User(IntegerPKMixin, DefaultTimestampMixin, ActiveRecord):
    name: Annotated[str, UseSqlType(VarCharType(length=100))]
    email: Annotated[str, UseSqlType(VarCharType(length=255))]
    age: int = 0

    c: ClassVar[FieldProxy] = FieldProxy()  # enables User.c.<field> expressions

    @classmethod
    def table_name(cls) -> str:
        return "users"


# Configure the backend and create the table from the model declaration
User.configure(
    OracleConnectionConfig(
        host="localhost",
        port=1521,
        service_name="ORCLPDB1",
        username="system",
        password="password",
    ),
    OracleBackend,
)
User.create_table()

# Create
alice = User(name="Alice", email="alice@example.com", age=30)
alice.save()
print(alice.id)  # database-assigned integer primary key

# Query with type-safe expressions
adults = User.query().where(User.c.age >= 18).all()
print([user.name for user in adults])

# Inspect the generated SQL without executing it
sql, params = User.query().where(User.c.age >= 18).to_sql()
print(sql, params)
```

> **Async parity:** use `AsyncActiveRecord` with `AsyncOracleBackend` — same method names, same patterns, just add `await`.

## Project Architecture

```
Your Code
   ↓
Interface        models, field proxies, mixins, query builders
   ↓
ActiveRecord     lifecycle, persistence, relations
   ↓
Expressions      backend-agnostic declarations → generic statements
   ↓
Dialect          binds expressions to Oracle SQL
   ↓
Driver           Oracle backend (python-oracledb) → Oracle Database
```

For configuration, introspection, partitioning, and date/time handling, start with the [Backend Documentation](docs/en_US/README.md).

**Oracle highlights.** DDL and DML are derived from the same model declarations as the core; the dialect renders Oracle syntax (`VARCHAR2`, `NUMBER`, identity columns, `RETURNING INTO`) and the driver is [`oracledb`](https://pypi.org/project/oracledb/). Oracle-specific capabilities are version-gated against the connected server.

## Backend Support & Progress

This package is a **separate distribution from the core** `rhosocial-activerecord` library; install both together. It is CI-tested against **Oracle Database 18c, 21c and 23** (XE / Free editions) and supports Oracle 18c or newer as the practical baseline.

> **⚠️ Development Stage:** This project is under active development. APIs may change, and some features are not yet production-ready.

## Contributing, License, Links

### Contributing

We welcome contributions! Please see [CONTRIBUTING.md](https://github.com/rhosocial/python-activerecord/blob/main/CONTRIBUTING.md) for guidelines.

### License

[Apache License 2.0](LICENSE) — Copyright © 2026 [vistart](https://github.com/vistart)

### Documentation & Links

* **[Backend Documentation](docs/en_US/README.md)** — Oracle backend documentation home
* **[Database Introspection](docs/introspection.md)**
* **[Date/Time Expressions](docs/datetime-expressions.md)**
* **[Partitioning and Partition Maintenance](docs/partition.md)**
* **[Architecture notes](.claude/architecture.md)** · **[Testing notes](.claude/testing.md)**
* **[Framework documentation](https://github.com/rhosocial/python-activerecord/tree/main/docs)**
* **[GitHub](https://github.com/rhosocial/python-activerecord-oracle)** · **[PyPI](https://pypi.org/project/rhosocial-activerecord-oracle/)**

---

<div align="center">
    <p><b>Built with ❤️ by the rhosocial team</b></p>
    <p><a href="https://github.com/rhosocial/python-activerecord-oracle">GitHub</a> · <a href="https://github.com/rhosocial/python-activerecord/tree/main/docs">Documentation</a> · <a href="https://pypi.org/project/rhosocial-activerecord-oracle/">PyPI</a></p>
</div>
