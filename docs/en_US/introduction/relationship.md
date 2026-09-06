# Relationship with Core Library

## Architecture Overview

rhosocial-activerecord uses a modular design where the core library (`rhosocial-activerecord`) provides database-agnostic ActiveRecord implementations, and database backends exist as separate extension packages.

The Oracle backend's namespace is located under `rhosocial.activerecord.backend.impl.oracle`, at the same level as other backends (such as `sqlite`, `dummy`). This means:

- Backends do not participate in ActiveRecord layer changes
- Backends strictly follow backend interface protocols
- Backend updates are decoupled from the core library's ActiveRecord functionality

```
rhosocial.activerecord
├── backend.impl.sqlite   # SQLite backend
├── backend.impl.dummy   # Dummy backend for testing
└── backend.impl.oracle   # Oracle backend (this package)
    ├── OracleBackend
    ├── AsyncOracleBackend
    └── ...
```

## Backend Responsibilities

The Oracle backend is responsible for the following:

### 1. SQL Dialect Generation

Converts generic query builders into Oracle-specific SQL statements:

```python
# Core library: generic query building
query = User.query().where(User.c.age >= 18).order_by(User.c.created_at)

# Oracle backend: converted to Oracle SQL
# SELECT * FROM users WHERE age >= 18 ORDER BY created_at
```

### 2. Data Type Mapping

Handles Oracle-specific data types, including:

- NUMBER, NUMBER(38), FLOAT, BINARY_FLOAT, BINARY_DOUBLE
- CHAR, VARCHAR2, NVARCHAR2, NCHAR, CLOB, NCLOB, LONG
- DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE
- RAW, LONG RAW, BLOB, BFILE
- XMLType, SDO_GEOMETRY, VECTOR (23ai+)

### 3. Connection Management

Provides Oracle connection establishment, disconnection, and other low-level operations via `oracledb`.

### 4. Transaction Control

Implements Oracle transaction COMMIT, ROLLBACK, and savepoint logic. Oracle uses **implicit transactions** — DML automatically starts a transaction, and DDL statements implicitly commit the current transaction.

## Quick Start

### 1. Installation

```bash
pip install rhosocial-activerecord
pip install rhosocial-activerecord-oracle
```

### 2. Define Models

```python
import uuid
from typing import ClassVar
from pydantic import Field
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import UUIDMixin, DefaultTimestampMixin


class User(UUIDMixin, DefaultTimestampMixin, ActiveRecord):
    username: str = Field(..., max_length=50)
    email: str

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'users'
```

### 3. Configure Backend

```python
from rhosocial.activerecord.backend.impl.oracle import (
    OracleBackend,
    OracleConnectionConfig,
)

# Configure Oracle connection
config = OracleConnectionConfig(
    host='localhost',
    port=1521,
    database='ORCLPDB1',
    username='user',
    password='password',
)

# Configure backend for the model
User.configure(config, OracleBackend)
```

### 4. CRUD Operations

```python
# Create
user = User(username='tom', email='tom@example.com')
user.save()

# Read
user = User.query().where(User.c.username == 'tom').one()

# Update
user.email = 'tom.new@example.com'
user.save()

# Delete
user.delete()
```

> **Note**: Oracle auto-increment uses **sequences** rather than `AUTO_INCREMENT`. The backend handles `NEXTVAL`/`CURRVAL` generation for identity columns.

💡 *AI Prompt:* "What is the ActiveRecord pattern? What are its advantages and disadvantages?"

