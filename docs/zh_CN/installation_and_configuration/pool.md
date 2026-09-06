# 连接管理

## 概述

Oracle 后端默认使用单连接生命周期。可以通过 `OracleConnectionConfig` 连接池参数配置连接池。

## 单连接

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig

class User(ActiveRecord):
    __table_name__ = "users"

config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password"
)
User.configure(config, OracleBackend)

# 连接在首次使用时建立
user = User.find_one(1)
```

## 连接池

为生产环境配置连接池参数：

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password",
    pool_min=2,
    pool_max=10,
    pool_increment=1,
    pool_get_timeout=30
)
```

## FastAPI 集成

```python
from fastapi import FastAPI
from rhosocial.activerecord.model import AsyncActiveRecord
from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend, OracleConnectionConfig

app = FastAPI()

class User(AsyncActiveRecord):
    __table_name__ = "users"

@app.on_event("startup")
async def startup():
    config = OracleConnectionConfig(
        host="localhost",
        port=1521,
        database="ORCLPDB1",
        username="system",
        password="password"
    )
    User.configure(config, AsyncOracleBackend)

@app.on_event("shutdown")
async def shutdown():
    await User.close_connection()
```

## 另请参阅

- [连接配置](configuration.md) — 连接参数
- [安装指南](installation.md) — 设置说明
