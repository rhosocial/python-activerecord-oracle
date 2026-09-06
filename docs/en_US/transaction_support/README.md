# Transaction Support

## Overview

rhosocial-activerecord provides a transaction manager that wraps Oracle's transaction semantics. This section covers isolation levels, savepoints, and deadlock handling for the Oracle backend.

## Transaction Manager

### Synchronous

```python
# Using the transaction manager
with User.transaction():
    user = User(username='alice')
    user.save()
    # Transaction commits on successful exit
    # Transaction rolls back on exception
```

### Asynchronous

```python
# Using the async transaction manager
async with User.transaction():
    user = User(username='alice')
    await user.save()
    # Transaction commits on successful exit
    # Transaction rolls back on exception
```

The transaction manager has both sync and async variants:
- `OracleTransactionManager` — synchronous
- `AsyncOracleTransactionManager` — asynchronous

The API is identical — the only difference is `async with` vs `with`.

## Oracle Transaction Behavior

Oracle uses **implicit transactions** — DML statements automatically start a transaction. There is no explicit `BEGIN TRANSACTION` statement in standard Oracle SQL (though PL/SQL blocks use `BEGIN...END`).

```python
# Oracle starts transactions implicitly
user = User(username='alice')
user.save()  # Implicitly starts a transaction

# The transaction is committed on:
# 1. Explicit commit
# 2. Connection close
# 3. New DDL statement (DDL auto-commits in Oracle)
```

## Contents

- [Isolation Levels](isolation_level.md): Oracle-specific isolation semantics
- [Savepoint](savepoint.md): Nested transactions and conditional rollback
- [Deadlock Handling](deadlock.md): Oracle deadlock detection, error codes, and retry strategies

## See Also

- [Core: Parallel Worker Patterns](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/scenarios/parallel_workers) — deadlock prevention principles

💡 *AI Prompt:* "How do isolation levels affect concurrent transactions in Oracle?"
