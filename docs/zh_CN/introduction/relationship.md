# 与核心库的关系

## 架构概述

rhosocial-activerecord 采用模块化设计，核心库（`rhosocial-activerecord`）提供与数据库无关的 ActiveRecord 实现，而数据库后端则作为独立的扩展包存在。

Oracle 后端的命名空间位于 `rhosocial.activerecord.backend.impl.oracle` 下，与其他后端（如 `sqlite`、`dummy`）处于同一层级。这意味着：

- 后端不参与 ActiveRecord 层的变更
- 后端严格遵守后端接口协议
- 后端更新与核心库的 ActiveRecord 功能解耦

```
rhosocial.activerecord
├── backend.impl.sqlite   # SQLite 后端
├── backend.impl.dummy   # 用于测试的 Dummy 后端
└── backend.impl.oracle   # Oracle 后端（本包）
    ├── OracleBackend
    ├── AsyncOracleBackend
    └── ...
```

## 后端职责

Oracle 后端负责以下内容：

### 1. SQL 方言生成

将通用查询构建器转换为 Oracle 特定的 SQL 语句：

```python
# 核心库：通用查询构建
query = User.query().where(User.c.age >= 18).order_by(User.c.created_at)

# Oracle 后端：转换为 Oracle SQL
# SELECT * FROM users WHERE age >= 18 ORDER BY created_at
```

### 2. 数据类型映射

处理 Oracle 特定的数据类型，包括：

- NUMBER、NUMBER(38)、FLOAT、BINARY_FLOAT、BINARY_DOUBLE
- CHAR、VARCHAR2、NVARCHAR2、NCHAR、CLOB、NCLOB、LONG
- DATE、TIMESTAMP、TIMESTAMP WITH TIME ZONE
- RAW、LONG RAW、BLOB、BFILE
- XMLType、SDO_GEOMETRY、VECTOR（23ai+）

### 3. 连接管理

通过 `oracledb` 提供 Oracle 连接的建立、断开及其他底层操作。

### 4. 事务控制

实现 Oracle 事务的 COMMIT、ROLLBACK 和保存点逻辑。Oracle 使用**隐式事务**——DML 自动开始事务，DDL 语句会隐式提交当前事务。

## 快速开始

### 1. 安装

```bash
pip install rhosocial-activerecord
pip install rhosocial-activerecord-oracle
```

### 2. 定义模型

```python
import uuid
from typing import ClassVar
from pydantic import Field
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import UUIDMixin, DefaultTimestampMixin


class User(UUIDMixin, DefaultTimestampMixin, ActiveRecord):
    username: str = Field(..., max_length=50)
    email: str

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'users'
```

### 3. 配置后端

```python
from rhosocial.activerecord.backend.impl.oracle import (
    OracleBackend,
    OracleConnectionConfig,
)

# 配置 Oracle 连接
config = OracleConnectionConfig(
    host='localhost',
    port=1521,
    database='ORCLPDB1',
    username='user',
    password='password',
)

# 为模型配置后端
User.configure(config, OracleBackend)
```

### 4. CRUD 操作

```python
# 创建
user = User(username='tom', email='tom@example.com')
user.save()

# 读取
user = User.query().where(User.c.username == 'tom').one()

# 更新
user.email = 'tom.new@example.com'
user.save()

# 删除
user.delete()
```

> **注意**：Oracle 的自增使用**序列**而非 `AUTO_INCREMENT`。后端会为标识列处理 `NEXTVAL`/`CURRVAL` 的生成。

💡 *AI Prompt:* "什么是 ActiveRecord 模式？它有哪些优点和缺点？"

