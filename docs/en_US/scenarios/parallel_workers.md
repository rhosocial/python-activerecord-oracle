# Parallel Workers: Best Practices (Oracle)

In data processing, task queues, and bulk import scenarios, developers often run multiple workers in parallel to improve throughput. This chapter focuses on parallel worker patterns for Oracle.

For general patterns (multi-process lifecycle, async behavior, deadlock prevention principles, application separation), see [Core Parallel Worker Patterns](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/scenarios/parallel_workers.md).

> **Design principle throughout this chapter**: The synchronous `BaseActiveRecord` and asynchronous `AsyncBaseActiveRecord` have **identical method names** — `configure()`, `backend()`, `transaction()`, `save()`, and so on. The async version simply requires `await` or `async with`. All examples in this chapter provide both versions.

## Table of Contents

1. [Oracle Concurrency Overview](#1-oracle-concurrency-overview)
2. [Multi-process: The Recommended Approach](#2-multi-process-the-recommended-approach)
3. [Oracle Async Backend Characteristics](#3-oracle-async-backend-characteristics)
4. [Deadlocks: Oracle Detection and Prevention](#4-deadlocks-oracle-detection-and-prevention)
5. [Application Separation Principle](#5-application-separation-principle)
6. [Example Code](#6-example-code)

---

## 1. Oracle Concurrency Overview

### 1.1 Fundamental Differences from SQLite

`rhosocial-activerecord` follows the core design principle of **one ActiveRecord class bound to one connection**:

- **Sync**: `Post.configure(config, OracleBackend)` → writes to `Post.__backend__`
- **Async**: `await Post.configure(config, AsyncOracleBackend)` → writes to `Post.__backend__`

Oracle differs fundamentally from SQLite:

| Feature | SQLite | Oracle |
| --- | --- | --- |
| Lock granularity | File-level lock | Row-level lock / block-level |
| Concurrent writes | Requires WAL mode; writes still serialized | MVCC (multi-version) — readers don't block writers |
| Deadlock handling | Timeout wait, raises `database is locked` | ORA-00060 — one transaction is rolled back automatically |
| Connection type | File path (local) | TCP network connection (host:port) |

### 1.2 The Immutability of the Single-Connection Model

Regardless of Oracle's concurrency advantages, **a single `ActiveRecord` class's `__backend__` remains a single connection**. In a multi-threaded environment, concurrent access to the same `__backend__` corrupts cursor state.

> **Do not share an ActiveRecord configuration across multiple threads.** Multi-process is the correct choice for parallel worker scenarios.

---

## 2. Multi-process: The Recommended Approach

Multi-process is the recommended approach for parallel worker scenarios. Each process has its own isolated memory space; `configure()` executes independently within each process, establishing a separate TCP connection.

### 2.1 Correct Lifecycle

**Sync (multiprocessing)**:

```python
import multiprocessing
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig
from models import Comment, Post, User

def worker(post_ids: list[int]):
    # 1. Configure the connection inside the process.
    config = OracleConnectionConfig(
        host="localhost",
        port=1521,
        database="ORCLPDB1",
        username="app",
        password="secret",
    )
    User.configure(config, OracleBackend)
    Post.__backend__ = User.backend()
    Comment.__backend__ = User.backend()

    try:
        for post_id in post_ids:
            post = Post.find_one(post_id)
            if post is None:
                continue
            author = post.author()          # BelongsTo relation
            approved = len([c for c in post.comments() if c.is_approved])
            post.view_count = 1 + approved
            post.save()
    finally:
        # 2. Disconnect before the process exits
        User.backend().disconnect()


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(worker, chunks)
```

**Async (asyncio + multi-process)**:

```python
import asyncio
import multiprocessing
from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend, OracleConnectionConfig
from models import AsyncComment, AsyncPost, AsyncUser

async def async_worker_main(post_ids: list[int]):
    config = OracleConnectionConfig(
        host="localhost", port=1521,
        database="ORCLPDB1", username="app", password="secret",
    )
    await AsyncUser.configure(config, AsyncOracleBackend)
    AsyncPost.__backend__ = AsyncUser.backend()
    AsyncComment.__backend__ = AsyncUser.backend()

    try:
        async def process_post(post_id: int):
            post = await AsyncPost.find_one(post_id)
            if post is None:
                return
            author = await post.author()
            approved = len([c for c in await post.comments() if c.is_approved])
            post.view_count = 1 + approved
            await post.save()

        # Single connection: sequential execution within one process
        for pid in post_ids:
            await process_post(pid)
    finally:
        await AsyncUser.backend().disconnect()


def run_async_worker(post_ids: list[int]):
    asyncio.run(async_worker_main(post_ids))


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(run_async_worker, chunks)
```

**Key rules**:

- `configure()` must be called inside the child process, never before `fork`
- Oracle connections are TCP connections; inheriting file descriptors after `fork` is dangerous
- Within a single process, coroutines naturally access the database serially (event loop single-threaded scheduling)

---

## 3. Oracle Async Backend Characteristics

The async Oracle backend (`AsyncOracleBackend`) is built on `oracledb` **thin mode**. Each ActiveRecord class is bound to **one connection** — this differs from a connection-pool approach:

| Feature | Single-connection ORM (this project) | Connection pool approach |
| --- | --- | --- |
| asyncio.gather in one process | ❌ Not supported — raises RuntimeError | ✅ Supported — each coroutine uses a different connection |
| Configuration complexity | Low — `configure()` in one line | High — manual pool management |
| Multi-process concurrency | ✅ Independent connection per process | ✅ Also supported |
| Best use case | Batch processing, task queues, data pipelines | High-concurrency web services |

```text
Single-connection async execution:
  coroutine A → send SQL → await → Oracle response → coroutine A resumes
  coroutine B → must wait until A finishes (sequential)

Connection-pool async execution:
  coroutine A → acquire conn-1 → send SQL → await (concurrent)
  coroutine B → acquire conn-2 → send SQL → await (concurrent)
```

### 3.1 Multi-process Is the Right Pattern for Concurrency

Even though coroutines within a single process must execute sequentially, **across processes** each process holds its own independent connection — true parallelism:

```python
# Single connection: sequential (cannot use asyncio.gather)
for pid in post_ids:
    await update_one(pid)
```

---

## 4. Deadlocks: Oracle Detection and Prevention

Oracle detects deadlocks automatically. When a deadlock is detected, Oracle rolls back one of the transactions and returns **ORA-00060**. The backend maps this to a `DeadlockError`.

### 4.1 Root Cause: Inconsistent Lock Order

```python
# ❌ Wrong: Different workers lock rows in opposite order
def worker_a():
    with Post.transaction():
        post1 = Post.find_one(1)  # Worker A locks id=1 first
        post2 = Post.find_one(2)  # Worker A requests id=2 (B holds it)
        # → Oracle detects deadlock and raises ORA-00060

def worker_b():
    with Post.transaction():
        post2 = Post.find_one(2)  # Worker B locks id=2 first
        post1 = Post.find_one(1)  # Worker B requests id=1 (A holds it)
```

### 4.2 Prevention Approach 1: Consistent Lock Order

```python
# ✅ Correct: Always lock resources in ascending primary key order
def transfer_safe(from_id: int, to_id: int, amount: float):
    first_id, second_id = min(from_id, to_id), max(from_id, to_id)
    with Account.transaction():
        first  = Account.find_one(first_id)
        second = Account.find_one(second_id)
        debit, credit = (first, second) if from_id < to_id else (second, first)
        debit.balance  -= amount
        credit.balance += amount
        debit.save()
        credit.save()

# Async version (same method names, add await)
async def transfer_safe_async(from_id: int, to_id: int, amount: float):
    first_id, second_id = min(from_id, to_id), max(from_id, to_id)
    async with Account.transaction():
        first  = await Account.find_one(first_id)
        second = await Account.find_one(second_id)
        debit, credit = (first, second) if from_id < to_id else (second, first)
        debit.balance  -= amount
        credit.balance += amount
        await debit.save()
        await credit.save()
```

### 4.3 Prevention Approach 2: Atomic Claim (Query + Update in One Transaction)

```python
# ✅ Correct: Atomic claim inside a transaction;
#    Oracle row-level locking guarantees no duplicates.
def claim_posts(batch_size: int = 5) -> list:
    with Post.transaction():
        pending = (
            Post.query()
                .where(Post.c.status == "draft")
                .order_by(Post.c.id)
                .limit(batch_size)
                .for_update()  # Oracle supports FOR UPDATE
                .all()
        )
        if not pending:
            return []
        for post in pending:
            post.status = "processing"
            post.save()
        return pending
```

### 4.4 Oracle-Specific Approach: Catch Deadlock and Retry

Because Oracle raises **ORA-00060** on deadlock, you can **let deadlocks happen and retry**. This is the recommended pattern for Oracle production environments:

```python
import time

def _is_deadlock(exc: Exception) -> bool:
    """Check whether the exception is an Oracle deadlock (ORA-00060)."""
    msg = str(exc)
    return "ORA-00060" in msg or "deadlock" in msg.lower()


def claim_posts_with_retry(batch_size: int = 5, max_retry: int = 3) -> list:
    """Atomic claim with automatic deadlock retry."""
    for attempt in range(max_retry):
        try:
            with Post.transaction():
                pending = (
                    Post.query()
                        .where(Post.c.status == "draft")
                        .order_by(Post.c.id)
                        .limit(batch_size)
                        .all()
                )
                if not pending:
                    return []
                for post in pending:
                    post.status = "processing"
                    post.save()
                return pending
        except Exception as e:
            if _is_deadlock(e) and attempt < max_retry - 1:
                time.sleep(0.05 * (attempt + 1))  # exponential back-off
                continue
            raise
    return []
```

### 4.5 Five Prevention Principles (with Oracle-Specific Notes)

| Principle | Description |
| --- | --- |
| **Data partitioning** | Assign data by ID range or hash to each worker so they never touch the same rows |
| **Consistent lock order** | Always request resources in a fixed order (e.g., ascending primary key) |
| **Short transactions** | Keep only necessary operations in a transaction; avoid I/O waits or expensive computations |
| **Atomic claim** | Query and update task status inside one transaction |
| **Deadlock retry** (Oracle-specific) | Catch `DeadlockError` (ORA-00060) and retry |

> **Oracle vs SQLite**: SQLite uses `pragmas={"busy_timeout": 5000}` for lock-wait timeout. Oracle detects deadlocks automatically (ORA-00060) and rolls back one transaction; the backend raises a `DeadlockError`.

---

## 5. Application Separation Principle

When a system contains two very different kinds of workloads, deploy them as separate applications.

| Workload type | Characteristics | Suitable deployment |
| --- | --- | --- |
| Web API service | Short requests, high concurrency, latency-sensitive | FastAPI / Django + asyncio + connection pools |
| Data analytics batch | Long-running, large datasets, CPU-intensive | Standalone script + multiprocessing + OracleBackend |
| Task queue consumer | Periodic polling, independent tasks, horizontally scalable | Celery + Oracle or custom + multiprocessing |

```text
User request ──→ Web app (asyncio + async connection pool)
                    │
                    └──→ Task queue (Oracle table / Redis)
                                │
                                └──→ Background worker pool
                                      (each process: independent OracleBackend sync connection)
```

---

## 6. Example Code

Runnable examples for parallel workers follow the structure in the [MySQL backend examples](../../../python-activerecord-mysql/docs/examples/chapter_12_scenarios/parallel_workers/), adapted to `OracleBackend` / `AsyncOracleBackend` with `OracleConnectionConfig`.

| File | Contents |
| --- | --- |
| `config_loader.py` | Connection config loader |
| `models.py` | Shared model definitions (`User`, `Post`, `Comment`, sync + async) |
| `setup_db.py` | Database initialization script |
| `exp1_basic_multiprocess.py` | Correct multi-process usage |
| `exp2_deadlock_wrong.py` | Row lock order conflict causing ORA-00060 (anti-pattern) |
| `exp3_partition_correct.py` | Data partitioning + atomic claim + deadlock retry |
| `exp4_multithread_warning.py` | Dangers of sharing an Oracle connection across threads (anti-pattern) |

---

## 7. Test Verification Conclusions

### 7.1 Known Limitations of Async Worker Testing

When running async tests in a multi-process environment, the event loop created by the test framework in the main process is isolated from child process event loops:

```text
Main Process (pytest):
  └── Event Loop A (created by pytest-asyncio)
      └── Async backend instance bound to Loop A

Child Process (Worker):
  └── Event Loop B (created by asyncio.run())
      └── Task tries to use async backend bound to Loop A
          └── Error: Task got Future attached to a different loop
```

**Correct Approach**: Only pass serializable connection parameters, create new async backend instances in child processes:

```python
async def async_worker_task(user_id, conn_params):
    config = conn_params['config_kwargs']
    await Model.configure(config, AsyncOracleBackend)
    user = await Model.find_one(user_id)
    await Model.backend().disconnect()
```

### 7.2 Production Recommendations

1. **Sync Workers Preferred**: In multi-process Worker scenarios, sync backend (`OracleBackend`) is the more stable choice
2. **Async for Single Process**: Async backend (`AsyncOracleBackend`) works well in single-process sequential execution scenarios
3. **Avoid Cross-Process Async Instance Passing**: Only pass serializable connection parameters, create new async backend instances in child processes

### 7.3 FOR UPDATE Capability Detection

Oracle supports `FOR UPDATE`. Use `dialect.supports_for_update()` to check capability before applying it in cross-backend code:

```python
def transfer_task(from_id: int, to_id: int, amount: float, conn_params: dict):
    supports_for_update = backend.dialect.supports_for_update()

    with Model.transaction():
        if supports_for_update:
            # Oracle: Use FOR UPDATE to lock rows
            first = Model.query().where(Model.c.id == first_id).for_update().one()
        else:
            # SQLite: Use regular query (relies on file locks)
            first = Model.find_one({'id': first_id})
```

