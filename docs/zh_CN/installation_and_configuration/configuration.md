# 连接配置

## 基本配置

```python
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

config = OracleConnectionConfig(
    host="localhost",       # 数据库服务器主机名
    port=1521,              # 数据库服务器端口（默认：1521）
    database="ORCLPDB1",    # Oracle 服务名或 SID
    username="system",      # 认证用户名
    password="password"     # 认证密码
)
```

## Oracle 特定参数

| 参数 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `service_name` | `str` | `None` | Oracle 服务名（替代 `database`） |
| `sid` | `str` | `None` | Oracle SID（替代 `service_name`） |
| `dsn` | `str` | `None` | 完整数据源名称（覆盖 host/port/database） |
| `mode` | `str` | `None` | 连接模式：`SYSDBA`、`SYSOPER`、`thin`、`thick` |
| `encoding` | `str` | `"UTF-8"` | 字符编码 |
| `nencoding` | `str` | `None` | 国家字符编码 |
| `edition` | `str` | `None` | 版本重定义的版本名称 |

## DSN 格式

DSN（数据源名称）从 `host`、`port` 和 `database` 自动构造：

- 如果设置了 `service_name`：`host:port/service_name`
- 如果设置了 `sid`：`host:port:sid`
- 否则：`host:port/database`

您也可以直接提供完整的 DSN 字符串：

```python
config = OracleConnectionConfig(
    dsn="localhost:1521/ORCLPDB1",
    username="system",
    password="password"
)
```

## 连接池配置

| 参数 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `pool_min` | `int` | `None` | 最小池连接数 |
| `pool_max` | `int` | `None` | 最大池连接数 |
| `pool_increment` | `int` | `None` | 连接增量 |
| `pool_get_timeout` | `int` | `None` | 从池获取连接的超时时间 |

## 会话配置

| 参数 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `stmtcachesize` | `int` | `20` | 语句缓存大小 |
| `prefetchrows` | `int` | `None` | 预取行数 |
| `arraysize` | `int` | `100` | 数组获取大小 |
| `threaded` | `bool` | `True` | 使用线程连接 |
| `events` | `bool` | `False` | 启用 Oracle 事件 |

## 命名连接

您可以在 Python 模块中定义命名连接以供重用：

```python
# myapp/connections/oracle_prod.py
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

def get_config():
    return OracleConnectionConfig(
        host="prod-db.example.com",
        port=1521,
        database="ORCLPDB1",
        username="app_user",
        password="secret"
    )

def get_backend():
    return OracleBackend
```

```bash
# 通过 CLI 使用
rhosocial-activerecord-oracle query \
    --named-connection myapp.connections.oracle_prod \
    "SELECT * FROM users"
```

## 另请参阅

- [SSL/TLS 配置](ssl.md) — 安全连接
- [连接管理](pool.md) — 连接池
