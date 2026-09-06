# Timezone Handling

## Overview

The Oracle backend maintains the original form returned by the database, with specific handling for Oracle's timezone-aware types.

## Difference Between DATE and TIMESTAMP

Oracle has several temporal types with different timezone behavior:

- **DATE**: Stores date and time (to the second) **without** timezone information
- **TIMESTAMP**: Stores date and time with fractional seconds, **without** timezone
- **TIMESTAMP WITH TIME ZONE**: Stores the timezone alongside the timestamp; stored internally in UTC
- **TIMESTAMP WITH LOCAL TIME ZONE**: Stored in the database timezone, returned in the session timezone

```sql
CREATE TABLE events (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(255),
    created_at DATE,                         -- No timezone
    updated_at TIMESTAMP,                    -- No timezone
    tz_ts TIMESTAMP WITH TIME ZONE,          -- With time zone
    local_ts TIMESTAMP WITH LOCAL TIME ZONE  -- Local time zone
);
```

## Oracle Server Timezone

The database and session timezone settings affect how timezone-aware types are stored and retrieved:

```sql
-- View session timezone
SELECT SESSIONTIMEZONE FROM DUAL;

-- View database timezone
SELECT DBTIMEZONE FROM DUAL;

-- Set session timezone
ALTER SESSION SET TIME_ZONE = '+08:00';
```

## Python Side Handling

The `OracleDateTimeAdapter` handles timezone-aware types. For `TIMESTAMP WITH TIME ZONE`, if the returned datetime has no timezone, the adapter assumes UTC:

```python
from datetime import datetime, timezone, timedelta


def to_utc(dt: datetime) -> datetime:
    """Convert to UTC time"""
    if dt.tzinfo is None:
        # Assume local timezone
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
    return dt.astimezone(timezone.utc)


def to_local(dt: datetime) -> datetime:
    """Convert to local time"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo
    return dt.astimezone(local_tz)
```

## Best Practices

1. **Store UTC in `TIMESTAMP WITH TIME ZONE`** — it is stored in UTC internally and converts consistently
2. **Convert at the application layer** — perform timezone conversion in Python or the frontend
3. **Avoid mixing** — do not mix times from different timezones in the same column
4. **Beware of `DATE`** — it drops sub-second precision and has no timezone; prefer `TIMESTAMP WITH TIME ZONE` for global applications

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

## Oracle vs MySQL Comparison

| Aspect | Oracle | MySQL |
|--------|--------|-------|
| Timezone-aware types | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` (converts to/from server timezone) |
| Naive type | `DATE`, `TIMESTAMP` | `DATETIME` |
| Default return | Depends on type; tz-aware returned as UTC | Depends on server `time_zone` |

💡 *AI Prompt:* "Why is it recommended to store time in UTC instead of local timezone?"

