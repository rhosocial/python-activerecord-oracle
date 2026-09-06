# 并行工作器：最佳实践（Oracle）

在数据处理、任务队列和批量导入场景中，开发人员经常并行运行多个工作器以提高吞吐量。本章重点介绍 Oracle 的并行工作器模式。

有关通用模式（多进程生命周期、异步行为、死锁预防原则、应用分离），请参阅[核心并行工作器模式](https://github.com/Rhosocial/python-activerecord/tree/main/docs/zh_CN/scenarios/parallel_workers.md)。

> **本章设计原则**：同步 `BaseActiveRecord` 和异步 `AsyncBaseActiveRecord` 具有**完全相同的方法名**——`configure()`、`backend()`、`transaction()`、`save()` 等。异步版本只需添加 `await` 或 `async with`。本章所有示例均提供同步和异步两种版本。

## 目录

1. [Oracle 并发概述](#1-oracle-并发概述)
2. [多进程：推荐的方法](#2-多进程推荐的方法)
3. [Oracle 异步后端特性](#3-oracle-异步后端特性)
4. [死锁：Oracle 检测与预防](#4-死锁oracle-检测与预防)
5. [应用分离原则](#5-应用分离原则)
6. [示例代码](#6-示例代码)

---

## 1. Oracle 并发概述

### 1.1 与 SQLite 的根本区别

`rhosocial-activerecord` 遵循核心设计原则——**一个 ActiveRecord 类绑定一个连接**：

- **同步**：`Post.configure(config, OracleBackend)` → 写入 `Post.__backend__`
- **异步**：`await Post.configure(config, AsyncOracleBackend)` → 写入 `Post.__backend__`

Oracle 与 SQLite 有几个根本性差异：

| 特性 | SQLite | Oracle |
| --- | --- | --- |
| 锁粒度 | 文件级锁 | 行级锁 / 块级锁 |
| 并发写入 | 需要 WAL 模式；写入仍被串行化 | MVCC（多版本）——读不阻塞写 |
| 死锁处理 | 超时等待，抛出 `database is locked` | ORA-00060——自动回滚其中一个事务 |
| 连接类型 | 文件路径（本地） | TCP 网络连接（host:port） |

### 1.2 单连接模型的不可变性

无论 Oracle 的并发优势如何，**单个 `ActiveRecord` 类的 `__backend__` 仍然是一个连接**。在多线程环境中，并发访问同一个 `__backend__` 会破坏游标状态。

> **不要跨线程共享 ActiveRecord 配置。** 对于并行工作器场景，多进程才是正确的选择。

---

## 2. 多进程：推荐的方法

多进程是并行工作器场景的推荐方法。每个进程有独立的内存空间；`configure()` 在每个进程内独立执行，建立各自的 TCP 连接。

### 2.1 正确的生命周期

**同步（multiprocessing）**：

```python
import multiprocessing
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig
from models import Comment, Post, User

def worker(post_ids: list[int]):
    # 1. 在进程内部配置连接。
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
            author = post.author()          # BelongsTo 关系
            approved = len([c for c in post.comments() if c.is_approved])
            post.view_count = 1 + approved
            post.save()
    finally:
        # 2. 进程退出前断开连接
        User.backend().disconnect()


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(worker, chunks)
```

**异步（asyncio + 多进程）**：

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

        # 单连接：在同一进程内顺序执行
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

**关键规则**：

- `configure()` 必须在子进程内部调用，绝不能在 `fork` 之前调用
- Oracle 连接是 TCP 连接；`fork` 后继承文件描述符很危险
- 在单个进程内，协程自然串行访问数据库（事件循环单线程调度）

---

## 3. Oracle 异步后端特性

Oracle 异步后端（`AsyncOracleBackend`）基于 `oracledb` **thin 模式**构建。每个 ActiveRecord 类绑定**一个连接**——这与连接池方法不同：

| 特性 | 单连接 ORM（本项目） | 连接池方法 |
| --- | --- | --- |
| 单进程内 asyncio.gather | ❌ 不支持——抛出 RuntimeError | ✅ 支持——每个协程使用不同连接 |
| 配置复杂度 | 低——一行 `configure()` | 高——手动管理连接池 |
| 多进程并发 | ✅ 每进程独立连接 | ✅ 也支持 |
| 最佳使用场景 | 批处理、任务队列、数据管道 | 高并发 Web 服务 |

```text
单连接异步执行：
  coroutine A → 发送 SQL → await → Oracle 响应 → coroutine A 恢复
  coroutine B → 必须等待 A 完成（顺序执行）

连接池异步执行：
  coroutine A → 获取 conn-1 → 发送 SQL → await（并发）
  coroutine B → 获取 conn-2 → 发送 SQL → await（并发）
```

### 3.1 多进程是并发的正确模式

尽管单进程内的协程必须顺序执行，但**跨进程**时每个进程持有自己独立的连接——实现真正的并行：

```python
# 单连接：顺序执行（不能使用 asyncio.gather）
for pid in post_ids:
    await update_one(pid)
```

---

## 4. 死锁：Oracle 检测与预防

Oracle 自动检测死锁。检测到死锁时，Oracle 会回滚其中一个事务并返回 **ORA-00060**。后端将其映射为 `DeadlockError`。

### 4.1 根本原因：不一致的行锁顺序

```python
# ❌ 错误：不同工作器以相反的顺序锁行
def worker_a():
    with Post.transaction():
        post1 = Post.find_one(1)  # 工作器 A 先锁定 id=1
        post2 = Post.find_one(2)  # 工作器 A 请求 id=2（B 持有它）
        # → Oracle 检测到死锁并抛出 ORA-00060

def worker_b():
    with Post.transaction():
        post2 = Post.find_one(2)  # 工作器 B 先锁定 id=2
        post1 = Post.find_one(1)  # 工作器 B 请求 id=1（A 持有它）
```

### 4.2 预防方法 1：一致的锁顺序

```python
# ✅ 正确：始终按主键升序锁定资源
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

# 异步版本（方法名相同，添加 await）
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

### 4.3 预防方法 2：原子认领（查询 + 更新在同一事务中）

```python
# ✅ 正确：在事务内进行原子认领；
#    Oracle 行级锁保证不会重复。
def claim_posts(batch_size: int = 5) -> list:
    with Post.transaction():
        pending = (
            Post.query()
                .where(Post.c.status == "draft")
                .order_by(Post.c.id)
                .limit(batch_size)
                .for_update()  # Oracle 支持 FOR UPDATE
                .all()
        )
        if not pending:
            return []
        for post in pending:
            post.status = "processing"
            post.save()
        return pending
```

### 4.4 Oracle 特定方法：捕获死锁并重试

由于 Oracle 在死锁时抛出 **ORA-00060**，您可以让死锁发生然后**重试**。这是 Oracle 生产环境的推荐模式：

```python
import time

def _is_deadlock(exc: Exception) -> bool:
    """检查异常是否为 Oracle 死锁（ORA-00060）。"""
    msg = str(exc)
    return "ORA-00060" in msg or "deadlock" in msg.lower()


def claim_posts_with_retry(batch_size: int = 5, max_retry: int = 3) -> list:
    """带自动死锁重试的原子认领。"""
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
                time.sleep(0.05 * (attempt + 1))  # 指数退避
                continue
            raise
    return []
```

### 4.5 五个预防原则（附 Oracle 特定说明）

| 原则 | 描述 |
| --- | --- |
| **数据分区** | 按 ID 范围或哈希将数据分配给每个工作器，使它们永远不会接触相同的行 |
| **一致的锁顺序** | 锁定多个资源时，始终按固定顺序请求（如主键升序） |
| **短事务** | 事务中只保留必要的操作；避免 I/O 等待或昂贵的计算 |
| **原子认领** | 在同一事务内查询和更新任务状态 |
| **死锁重试**（Oracle 特定） | 捕获 `DeadlockError`（ORA-00060）并重试 |

> **Oracle vs SQLite**：SQLite 使用 `pragmas={"busy_timeout": 5000}` 进行锁等待超时。Oracle 自动检测死锁（ORA-00060）并回滚其中一个事务；后端抛出 `DeadlockError`。

---

## 5. 应用分离原则

当系统包含两种截然不同的工作负载时，应将它们部署为独立的应用程序。

| 工作负载类型 | 特性 | 适合的部署 |
| --- | --- | --- |
| Web API 服务 | 短请求、高并发、延迟敏感 | FastAPI / Django + asyncio + 连接池 |
| 数据分析批处理 | 长时运行、大数据集、CPU 密集 | 独立脚本 + multiprocessing + OracleBackend |
| 任务队列消费者 | 周期轮询、独立任务、可水平扩展 | Celery + Oracle 或自定义 + multiprocessing |

```text
用户请求 ──→ Web 应用（asyncio + 异步连接池）
                    │
                    └──→ 任务队列（Oracle 表 / Redis）
                                │
                                └──→ 后台工作器池
                                      （每个进程：独立的 OracleBackend 同步连接）
```

---

## 6. 示例代码

并行工作器的可运行示例遵循 [MySQL 后端示例](../../../python-activerecord-mysql/docs/examples/chapter_12_scenarios/parallel_workers/) 的结构，并适配为 `OracleBackend` / `AsyncOracleBackend` 和 `OracleConnectionConfig`。

| 文件 | 内容 |
| --- | --- |
| `config_loader.py` | 连接配置加载器 |
| `models.py` | 共享模型定义（`User`、`Post`、`Comment`，同步 + 异步） |
| `setup_db.py` | 数据库初始化脚本 |
| `exp1_basic_multiprocess.py` | 正确的多进程用法 |
| `exp2_deadlock_wrong.py` | 行锁顺序冲突导致 ORA-00060（反模式） |
| `exp3_partition_correct.py` | 数据分区 + 原子认领 + 死锁重试 |
| `exp4_multithread_warning.py` | 跨线程共享 Oracle 连接的危险（反模式） |

---

## 7. 测试验证结论

### 7.1 异步工作器测试的已知限制

在多进程环境中运行异步测试时，测试框架在主进程中创建的事件循环与子进程的事件循环相互隔离：

```text
主进程（pytest）：
  └── 事件循环 A（由 pytest-asyncio 创建）
      └── 绑定到循环 A 的异步后端实例

子进程（工作器）：
  └── 事件循环 B（由 asyncio.run() 创建）
      └── 任务尝试使用绑定到循环 A 的异步后端
          └── 错误：Task got Future attached to a different loop
```

**正确方法**：只传递可序列化的连接参数，在子进程中创建新的异步后端实例：

```python
async def async_worker_task(user_id, conn_params):
    config = conn_params['config_kwargs']
    await Model.configure(config, AsyncOracleBackend)
    user = await Model.find_one(user_id)
    await Model.backend().disconnect()
```

### 7.2 生产环境建议

1. **优先使用同步工作器**：在多进程工作器场景中，同步后端（`OracleBackend`）更稳定
2. **异步用于单进程**：异步后端（`AsyncOracleBackend`）适合单进程顺序执行场景
3. **避免跨进程传递异步实例**：只传递可序列化的连接参数，在子进程中创建新的异步后端实例

### 7.3 FOR UPDATE 能力检测

Oracle 支持 `FOR UPDATE`。在跨后端代码中应用之前，请使用 `dialect.supports_for_update()` 检查能力：

```python
def transfer_task(from_id: int, to_id: int, amount: float, conn_params: dict):
    supports_for_update = backend.dialect.supports_for_update()

    with Model.transaction():
        if supports_for_update:
            # Oracle：使用 FOR UPDATE 锁行
            first = Model.query().where(Model.c.id == first_id).for_update().one()
        else:
            # SQLite：使用普通查询（依赖文件锁）
            first = Model.find_one({'id': first_id})
```