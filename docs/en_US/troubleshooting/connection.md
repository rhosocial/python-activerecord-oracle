# Common Connection Errors

## Overview

This section covers common Oracle connection errors and their solutions.

## Connection Refused

### Error Message
```
ORA-12541: TNS:no listener
```

### Causes
- Oracle listener is not running
- Incorrect host or port
- Firewall blocking port 1521

### Solutions
```bash
# Check if the Oracle listener is running
lsnrctl status

# Check port
telnet localhost 1521
```

## Authentication Failed

### Error Message
```
ORA-01017: invalid username/password; logon denied
```

### Causes
- Incorrect username or password
- User lacks access to the target service

### Solutions
```sql
-- Execute as a DBA (SYSDBA)
ALTER USER app_user IDENTIFIED BY new_password;
GRANT CONNECT, RESOURCE TO app_user;
```

## Service Not Found

### Error Message
```
ORA-12514: TNS:listener does not currently know of service requested
```

### Causes
- Incorrect service name or SID
- The service is registered under a different name

### Solutions
```python
# Verify the correct service name (e.g., FREEPDB1, ORCLPDB1)
config = OracleConnectionConfig(
    host='localhost',
    port=1521,
    database='FREEPDB1',   # Correct service name
    username='user',
    password='password',
)
```

## Connection Timeout

### Error Message
```
ORA-12535: TNS:operation timed out
```

### Causes
- Network issues
- Long connection setup time

### Solutions
```python
config = OracleConnectionConfig(
    host='remote.host.com',
    database='ORCLPDB1',
    username='user',
    password='password',
    # oracledb thin mode connection timeout
)
```

## Connection Loss and Automatic Recovery

### Overview

In long-running applications, database connections may be dropped. The Oracle backend implements a recovery mechanism to reconnect automatically.

### Common Connection Loss Scenarios

| Scenario | Cause |
|----------|-------|
| Idle timeout | Connection idle too long |
| Server restart | Oracle instance restart |
| Network instability | TCP connection drop |

### Automatic Recovery Mechanism

The backend checks connection status before each query and reconnects when needed:

```python
def _get_cursor(self):
    """Get a database cursor, ensuring connection is active."""
    if not self._connection:
        # No connection, establish new one
        self.connect()
    elif not self._connection.is_healthy():
        # Connection lost, reconnect
        self.disconnect()
        self.connect()
    return self._connection.cursor()
```

### Manual Keep-Alive Mechanism

Use the `ping()` method for proactive connection maintenance:

```python
# Check connection status without auto-reconnect
is_alive = backend.ping(reconnect=False)

# Check connection status with auto-reconnect if disconnected
is_alive = backend.ping(reconnect=True)
```

### Error Codes Reference

| ORA Error | Description |
|-----------|-------------|
| ORA-12541 | TNS: no listener |
| ORA-01017 | invalid username/password |
| ORA-12514 | service not known |
| ORA-12535 | TNS: operation timed out |
| ORA-12170 | TNS: connect timeout occurred |
| ORA-00028 | session has been killed |
| ORA-03113 | end-of-file on communication channel |

## Best Practices

1. **Verify the service name** — the most common Oracle connection error is an incorrect service name or SID
2. **Use `ping()` for keep-alive** in long-running worker processes
3. **Multi-process workers** — each process should have its own backend instance
4. **Thin mode** — the default requires no Oracle Client; thick mode requires Oracle Instant Client installed

💡 *AI Prompt:* "How to troubleshoot Oracle connection errors? How does the backend automatically recover connections?"

