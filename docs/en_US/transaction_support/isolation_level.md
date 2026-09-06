# Transaction Isolation Levels

## Overview

Oracle supports two SQL standard isolation levels: **READ COMMITTED** (default) and **SERIALIZABLE**. Unlike MySQL, Oracle does **not** support READ UNCOMMITTED or REPEATABLE READ — both are emulated or unsupported.

## Isolation Level Comparison

| Isolation Level | Dirty Read | Non-Repeatable Read | Phantom Read | Oracle Support |
|-----------------|------------|---------------------|--------------|----------------|
| READ UNCOMMITTED | Possible | Possible | Possible | ❌ Not supported |
| READ COMMITTED (Default) | Impossible | Possible | Possible | ✅ Supported |
| REPEATABLE READ | Impossible | Impossible | Possible | ⚠️ Emulated via SERIALIZABLE |
| SERIALIZABLE | Impossible | Impossible | Impossible | ✅ Supported |

Oracle's default isolation level is **READ COMMITTED**. It uses **MVCC** (Multi-Version Concurrency Control), so readers never block writers and writers never block readers.

## Setting Isolation Level

The transaction manager only issues `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` when `SERIALIZABLE` is requested. READ COMMITTED is the default and requires no explicit setting:

```python
from rhosocial.activerecord.backend.transaction import IsolationLevel
from rhosocial.activerecord.backend.impl.oracle import OracleTransactionManager

# SERIALIZABLE
with Account.transaction(isolation_level=IsolationLevel.SERIALIZABLE):
    # All reads see a consistent snapshot
    ...

# READ COMMITTED (default) — each statement sees the latest committed data
with Account.transaction():
    ...
```

## Isolation Level Details

### READ COMMITTED (Default)

Each statement sees only committed data. This is Oracle's default and is suitable for most applications, balancing concurrency and consistency.

```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

### SERIALIZABLE

The highest isolation level. The transaction sees a consistent snapshot as of the time the transaction began, preventing non-repeatable and phantom reads.

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

**Caveats**:

- A `ORA-08177: can't serialize access for this transaction` error may occur when a concurrent transaction modifies data the serializable transaction tries to modify
- This is not true serial execution — it relies on snapshot isolation

### Transaction Mode (READ ONLY / READ WRITE)

Oracle also supports transaction access modes, which the transaction manager exposes separately:

```python
from rhosocial.activerecord.backend.transaction import TransactionMode

# READ ONLY — no DML allowed, only queries
with Account.transaction(mode=TransactionMode.READ_ONLY):
    total = Account.sum('balance')

# READ WRITE (default)
with Account.transaction(mode=TransactionMode.READ_WRITE):
    ...
```

## Mapping from Other Databases

| Database Default | Oracle Equivalent |
|------------------|-------------------|
| MySQL REPEATABLE READ | Not directly available; use SERIALIZABLE for strict snapshot semantics |
| PostgreSQL READ COMMITTED | Same as Oracle READ COMMITTED |

💡 *AI Prompt:* "What are dirty reads, non-repeatable reads, and phantom reads?"

