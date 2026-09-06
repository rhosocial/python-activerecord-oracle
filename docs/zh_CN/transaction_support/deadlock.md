# 死锁处理

## 概述

Oracle 死锁是指两个或多个事务互相等待对方释放锁的情况。Oracle 自动检测死锁，回滚其中一个冲突事务，并返回错误 **ORA-00060**。后端将其映射为 `DeadlockError`。

## Oracle 中的死锁检测

当 Oracle 检测到死锁时：

1. 其中一个事务被自动回滚
2. 被回滚的会话收到 `ORA-00060: deadlock detected while waiting for resource`
3. 死锁跟踪文件写入 Oracle 诊断目录
4. 另一个事务继续正常运行

后端将 `ORA-00060`（以及自死锁 `ORA-04020`）映射为 `DeadlockError`：

```python
from rhosocial.activerecord.backend.errors import DeadlockError

try:
    with Account.transaction():
        # ... 锁操作
        ...
except DeadlockError as e:
    # Oracle 检测到死锁；重试该操作
    print("发生死锁，将重试")
```

## 死锁跟踪文件

Oracle 告警日志和跟踪目录记录死锁。典型位置：

| 项目 | 位置 |
|------|----------|
| 告警日志 | `$ORACLE_BASE/diag/rdbms/<db>/<sid>/trace/alert_<sid>.log` |
| 死锁跟踪 | `.../trace/<sid>_ora_<spid>.trc`（包含 `DEADLOCK DETECTED` 块） |

跟踪文件显示了涉及哪些会话、SQL 语句和行锁，有助于识别锁顺序冲突。

## 自动重试策略

由于 Oracle 会回滚其中一个事务，您可以**让死锁发生然后重试**：

```python
import time
from rhosocial.activerecord.backend.errors import DeadlockError


def _is_deadlock(exc: Exception) -> bool:
    """检查异常是否为 Oracle 死锁。"""
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
                    time.sleep(delay * (attempt + 1))  # 指数退避
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

## 避免死锁的建议

1. **以固定顺序访问资源**：始终以相同顺序锁定行（如主键升序）
2. **尽可能使用索引**：减少锁定的行数
3. **保持事务短小**：减少锁的持续时间
4. **及时提交**：不要持有锁超过必要时间
5. **谨慎使用 `FOR UPDATE`**：只锁定真正需要锁定的行

> **Oracle 特定说明**：Oracle 对 DML 默认采用**悲观**锁定——`UPDATE` 语句会立即获取行锁。与 MySQL 的 `innodb_lock_wait_timeout` 不同，Oracle 在 ORA-00060 检测之前会无限期等待（由 `DISTRIBUTED_LOCK_TIMEOUT` / `LOCK_TIMEOUT` 会话设置控制）。

💡 *AI Prompt:* "什么是数据库死锁？如何避免死锁？"