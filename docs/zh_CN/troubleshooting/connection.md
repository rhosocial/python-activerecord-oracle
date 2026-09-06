# 常见连接错误

## 概述

本节介绍常见的 Oracle 连接错误及其解决方法。

## 连接被拒绝

### 错误信息
```
ORA-12541: TNS:no listener
```

### 原因
- Oracle 监听器未运行
- 主机或端口不正确
- 防火墙阻止了 1521 端口

### 解决方法
```bash
# 检查 Oracle 监听器是否运行
lsnrctl status

# 检查端口
telnet localhost 1521
```

## 身份验证失败

### 错误信息
```
ORA-01017: invalid username/password; logon denied
```

### 原因
- 用户名或密码错误
- 用户没有访问目标服务的权限

### 解决方法
```sql
-- 以 DBA（SYSDBA）身份执行
ALTER USER app_user IDENTIFIED BY new_password;
GRANT CONNECT, RESOURCE TO app_user;
```

## 服务未找到

### 错误信息
```
ORA-12514: TNS:listener does not currently know of service requested
```

### 原因
- 服务名或 SID 错误
- 服务以不同的名称注册

### 解决方法
```python
# 确认正确的服务名（如 FREEPDB1、ORCLPDB1）
config = OracleConnectionConfig(
    host='localhost',
    port=1521,
    database='FREEPDB1',   # 正确的服务名
    username='user',
    password='password',
)
```

## 连接超时

### 错误信息
```
ORA-12535: TNS:operation timed out
```

### 原因
- 网络问题
- 连接建立时间过长

### 解决方法
```python
config = OracleConnectionConfig(
    host='remote.host.com',
    database='ORCLPDB1',
    username='user',
    password='password',
    # oracledb thin 模式连接超时
)
```

## 连接丢失与自动恢复

### 概述

在长时间运行的应用程序中，数据库连接可能因各种原因断开。Oracle 后端实现了自动重连机制。

### 常见连接丢失场景

| 场景 | 原因 |
|----------|-------|
| 空闲超时 | 连接空闲时间过长 |
| 服务器重启 | Oracle 实例重启 |
| 网络不稳定 | TCP 连接断开 |

### 自动恢复机制

后端在每次查询前检查连接状态，并在需要时重连：

```python
def _get_cursor(self):
    """获取数据库游标，确保连接处于活动状态。"""
    if not self._connection:
        # 没有连接，建立新连接
        self.connect()
    elif not self._connection.is_healthy():
        # 连接丢失，重新连接
        self.disconnect()
        self.connect()
    return self._connection.cursor()
```

### 手动保活机制

使用 `ping()` 方法进行主动连接维护：

```python
# 检查连接状态，不自动重连
is_alive = backend.ping(reconnect=False)

# 检查连接状态，断开时自动重连
is_alive = backend.ping(reconnect=True)
```

### 错误码参考

| ORA 错误 | 描述 |
|-----------|-------------|
| ORA-12541 | TNS：无监听器 |
| ORA-01017 | 用户名/密码无效 |
| ORA-12514 | 服务未知 |
| ORA-12535 | TNS：操作超时 |
| ORA-12170 | TNS：连接超时发生 |
| ORA-00028 | 会话已被终止 |
| ORA-03113 | 通信通道文件结束 |

## 最佳实践

1. **确认服务名**——最常见的 Oracle 连接错误是服务名或 SID 不正确
2. **在长时间运行的工作器进程中使用 `ping()` 保活**
3. **多进程工作器**——每个进程应拥有自己的后端实例
4. **Thin 模式**——默认不需要 Oracle Client；thick 模式需要安装 Oracle Instant Client

💡 *AI Prompt:* "如何排查 Oracle 连接错误？后端如何自动恢复连接？"