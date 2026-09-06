# Deadlock Handling

## Overview

An Oracle deadlock is a situation where two or more transactions are waiting for each other to release locks. Oracle detects deadlocks automatically, rolls back one of the conflicting transactions, and returns the error **ORA-00060**. The backend maps this to a `DeadlockError`.

## Deadlock Detection in Oracle

When Oracle detects a deadlock:

1. One transaction is automatically rolled back
2. The rolled-back session receives `ORA-00060: deadlock detected while waiting for resource`
3. A deadlock trace file is written to the Oracle diagnostic directory
4. The other transaction continues normally

The backend maps `ORA-00060` (and self-deadlock `ORA-04020`) to a `DeadlockError`:

```python
from rhosocial.activerecord.backend.errors import DeadlockError

try:
    with Account.transaction():
        # ... locking operations
        ...
except DeadlockError as e:
    # Oracle detected a deadlock; retry the operation
    print("Deadlock occurred, will retry")
```

## Deadlock Trace

The Oracle alert log and trace directory record the deadlock. Typical locations:

| Item | Location |
|------|----------|
| Alert log | `$ORACLE_BASE/diag/rdbms/<db>/<sid>/trace/alert_<sid>.log` |
| Deadlock trace | `.../trace/<sid>_ora_<spid>.trc` (contains the `DEADLOCK DETECTED` block) |

The trace shows which sessions, SQL statements, and row locks were involved, which helps identify the lock-order conflict.

## Auto-Retry Strategy

Because Oracle rolls back one transaction, you can **let deadlocks happen and retry**:

```python
import time
from rhosocial.activerecord.backend.errors import DeadlockError


def _is_deadlock(exc: Exception) -> bool:
    """Check whether the exception is an Oracle deadlock."""
    msg = str(exc)
    return "ORA-00060" in msg or "deadlock" in msg.lower()


def retry_on_deadlock(max_retries=3, delay=0.1):
    def decorator(func):
        from functools import wraps

        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except DeadlockError as e:
                    last_exception = e
                    time.sleep(delay * (attempt + 1))  # exponential back-off
                    continue
            raise last_exception
        return wrapper
    return decorator


@retry_on_deadlock(max_retries=3)
def transfer_money(from_account, to_account, amount):
    with Account.transaction():
        debit = Account.find_one(from_account)
        credit = Account.find_one(to_account)
        debit.balance -= amount
        credit.balance += amount
        debit.save()
        credit.save()
```

## Recommendations for Avoiding Deadlocks

1. **Access resources in a fixed order**: Always lock rows in the same order (e.g., ascending primary key)
2. **Use indexes whenever possible**: Reduce the number of rows locked
3. **Keep transactions small**: Reduce lock duration
4. **Commit promptly**: Do not hold locks longer than necessary
5. **Use `FOR UPDATE` selectively**: Only lock rows that genuinely need locking

> **Oracle-specific note**: Oracle uses **pessimistic** locking by default for DML — `UPDATE` statements acquire row locks immediately. Unlike MySQL's `innodb_lock_wait_timeout`, Oracle waits indefinitely (controlled by `DISTRIBUTED_LOCK_TIMEOUT` / `LOCK_TIMEOUT` session setting) before ORA-00060 detection.

💡 *AI Prompt:* "What is a database deadlock? How can deadlocks be avoided?"

