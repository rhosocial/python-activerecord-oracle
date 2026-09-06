# 时区处理

## 概述

Oracle 后端保持数据库返回的原始形式，并对 Oracle 的时区感知类型进行特定处理。

## DATE 与 TIMESTAMP 的区别

Oracle 有几种时区行为不同的时间类型：

- **DATE**：存储日期和时间（精确到秒）**不带**时区信息
- **TIMESTAMP**：存储带小数秒的日期和时间，**不带**时区
- **TIMESTAMP WITH TIME ZONE**：与时间戳一起存储时区；内部以 UTC 存储
- **TIMESTAMP WITH LOCAL TIME ZONE**：以数据库时区存储，以会话时区返回

```sql
CREATE TABLE events (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(255),
    created_at DATE,                         -- 无时区
    updated_at TIMESTAMP,                    -- 无时区
    tz_ts TIMESTAMP WITH TIME ZONE,          -- 带时区
    local_ts TIMESTAMP WITH LOCAL TIME ZONE  -- 本地时区
);
```

## Oracle 服务器时区

数据库和会话时区设置会影响时区感知类型的存储和检索：

```sql
-- 查看会话时区
SELECT SESSIONTIMEZONE FROM DUAL;

-- 查看数据库时区
SELECT DBTIMEZONE FROM DUAL;

-- 设置会话时区
ALTER SESSION SET TIME_ZONE = '+08:00';
```

## Python 端处理

`OracleDateTimeAdapter` 处理时区感知类型。对于 `TIMESTAMP WITH TIME ZONE`，如果返回的 datetime 没有时区，适配器会假定为 UTC：

```python
from datetime import datetime, timezone, timedelta


def to_utc(dt: datetime) -> datetime:
    """转换为 UTC 时间"""
    if dt.tzinfo is None:
        # 假定本地时区
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
    return dt.astimezone(timezone.utc)


def to_local(dt: datetime) -> datetime:
    """转换为本地时间"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo
    return dt.astimezone(local_tz)
```

## 最佳实践

1. **以 `TIMESTAMP WITH TIME ZONE` 存储 UTC**——它在内部以 UTC 存储并一致地转换
2. **在应用层转换**——在 Python 或前端进行时区转换
3. **避免混用**——不要在同一个列中混用不同时区的时间
4. **警惕 `DATE`**——它会丢失亚秒精度且没有时区；对于全球化应用，建议使用 `TIMESTAMP WITH TIME ZONE`

```python
from datetime import datetime, timezone


class Event(ActiveRecord):
    name: str
    created_at: datetime

    @property
    def created_at_utc(self) -> datetime:
        if self.created_at.tzinfo is None:
            return self.created_at.replace(tzinfo=timezone.utc)
        return self.created_at.astimezone(timezone.utc)
```

## Oracle 与 MySQL 对比

| 方面 | Oracle | MySQL |
|--------|--------|-------|
| 时区感知类型 | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP`（与服务器时区相互转换） |
| 无时区类型 | `DATE`、`TIMESTAMP` | `DATETIME` |
| 默认返回值 | 取决于类型；时区感知类型以 UTC 返回 | 取决于服务器 `time_zone` |

💡 *AI Prompt:* "为什么建议以 UTC 而不是本地时区存储时间？"