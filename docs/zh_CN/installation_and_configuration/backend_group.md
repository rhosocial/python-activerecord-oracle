# BackendGroup 和 BackendManager（Oracle）

本文档介绍如何将 `BackendGroup` 和 `BackendManager` 与 Oracle 后端一起使用。有关详细的 API 文档，请参阅[核心库文档](../../../rhosocial-activerecord/docs/zh_CN/connection/connection_management.md)。

## 快速示例

```python
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig
from rhosocial.activerecord.model import ActiveRecord


class User(ActiveRecord):
    name: str
    email: str


# 使用上下文管理器
with BackendGroup(
    name="main",
    models=[User],
    config=OracleConnectionConfig(
        host="localhost",
        port=1521,
        database="ORCLPDB1",
        username="app",
        password="secret",
    ),
    backend_class=OracleBackend,
) as group:
    user = User(name="John", email="john@example.com")
    user.save()

# 通过 BackendManager 使用多个组
from rhosocial.activerecord.connection import BackendManager

manager = BackendManager()
manager.create_group(
    name="main",
    models=[User],
    config=OracleConnectionConfig(host="localhost", database="main_db"),
    backend_class=OracleBackend,
)
manager.create_group(
    name="stats",
    config=OracleConnectionConfig(host="localhost", database="stats_db"),
    backend_class=OracleBackend,
)

main_backend = manager.get_group("main").get_backend()
stats_backend = manager.get_group("stats").get_backend()
```

## Oracle 特定功能

### 连接池配置

Oracle 后端通过 `OracleConnectionConfig` 支持 `oracledb` 连接池：

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="app",
    password="secret",
    pool_min=1,
    pool_max=10,
    pool_increment=1,
    pool_get_timeout=30,
)
```

### DSN 配置

```python
# 使用服务名（推荐）
config = OracleConnectionConfig(
    service_name="ORCLPDB1",
    username="app",
    password="secret",
)

# 使用 SID
config = OracleConnectionConfig(
    sid="ORCL",
    username="app",
    password="secret",
)

# 使用完整的 DSN
config = OracleConnectionConfig(
    dsn="localhost:1521/ORCLPDB1",
    username="app",
    password="secret",
)
```

### SSL/TLS 配置

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="app",
    password="secret",
    ssl_verify_cert=True,
    ssl_ca="/path/to/ca.pem",
)
```

## 线程安全说明

与核心的单连接模型一致，单个 `ActiveRecord` 类的 `__backend__` 仍然是一个连接。对于多线程并行工作器场景，建议使用**多进程**，以便每个进程建立自己独立的连接。

## 示例代码

有关使用后端连接管理的多工作器 FastAPI 示例，请参阅[核心库连接管理文档](../../../rhosocial-activerecord/docs/zh_CN/connection/connection_management.md)。

