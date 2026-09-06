# Savepoint Support

## Overview

Savepoints allow creating intermediate checkpoints within a transaction, enabling partial rollbacks. Oracle fully supports savepoints.

## Using Savepoints

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

config = OracleConnectionConfig(
    host='localhost',
    database='ORCLPDB1',
    username='user',
    password='password',
)

backend = OracleBackend(connection_config=config)
backend.connect()

try:
    with backend.transaction_manager() as tx:
        # Operation 1
        cursor = backend.execute("INSERT INTO users (name) VALUES (:1)", ("Alice",))

        # Create savepoint
        tx.create_savepoint("sp1")

        try:
            # Operation 2 (may fail)
            backend.execute("INSERT INTO users (name) VALUES (:1)", ("Bob",))
        except Exception:
            # Rollback to savepoint
            tx.rollback_savepoint("sp1")
            # Continue with the rest of the transaction
finally:
    backend.disconnect()
```

## Savepoint Behavior in Oracle

| Feature | Behavior |
|---------|----------|
| Create | `SAVEPOINT <name>` |
| Rollback to | `ROLLBACK TO SAVEPOINT <name>` |
| Release | Not supported — Oracle automatically removes a savepoint when the transaction commits or rolls back, or when a later savepoint with the same name is created |
| Nested savepoints | Supported — savepoints can be nested |

## Named Savepoints

```python
# Create a savepoint
tx.create_savepoint("savepoint_name")

# Rollback to a savepoint (partial rollback)
tx.rollback_savepoint("savepoint_name")

# Check support
tx.supports_savepoint()  # True for Oracle
```

## Savepoints and DDL

> **Important**: In Oracle, **DDL statements implicitly commit the transaction** and therefore **erase all savepoints**. Do not create savepoints across DDL boundaries — create them after DDL completes.

```python
with User.transaction() as tx:
    # DDL commits the transaction, erasing any prior savepoints
    User.backend().executescript("CREATE TABLE temp_t (id NUMBER)")

    # Create savepoint AFTER DDL
    tx.create_savepoint("sp1")
    ...
```

## See Also

- [Transaction Support Overview](./README.md) — transaction manager API
- [Isolation Levels](./isolation_level.md) — Oracle isolation semantics
- [Deadlock Handling](./deadlock.md) — ORA-00060 and retry strategies

💡 *AI Prompt:* "What is a database savepoint? How does it differ from a full rollback?"

