# 测试配置

## 概述

本节介绍如何配置 Oracle 后端的测试环境。

有关通用测试策略（DummyBackend、SQLite 集成测试），请参阅[核心后端测试指南](https://github.com/Rhosocial/python-activerecord/tree/main/docs/zh_CN/testing/backend_testing.md)。

## 使用 Oracle 后端进行端到端测试

要测试完整的 Oracle 行为，请使用 Oracle 后端：

```python
import os
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig


class User(ActiveRecord):
    name: str
    email: str

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'users'


# 从环境变量读取配置
config = OracleConnectionConfig(
    host=os.environ.get('ORACLE_HOST', 'localhost'),
    port=int(os.environ.get('ORACLE_PORT', 1521)),
    database=os.environ.get('ORACLE_DATABASE', 'ORCLPDB1'),
    username=os.environ.get('ORACLE_USER', 'system'),
    password=os.environ.get('ORACLE_PASSWORD', ''),
)
User.configure(config, OracleBackend)
```

## 测试夹具（Fixtures）

```python
import pytest
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig


@pytest.fixture
def oracle_config():
    return OracleConnectionConfig(
        host='localhost',
        port=1521,
        database='ORCLPDB1',
        username='system',
        password='password',
    )


@pytest.fixture
def oracle_backend(oracle_config):
    backend = OracleBackend(connection_config=oracle_config)
    backend.connect()
    yield backend
    backend.disconnect()


def test_connection(oracle_backend):
    version = oracle_backend.get_server_version()
    assert version is not None
```

## Oracle 特定测试注意事项

- **DDL 自动提交**：在 Oracle 中，DDL 语句会隐式提交当前事务。请使用 `DELETE`/`TRUNCATE` 清理测试数据，而不是依赖事务回滚来撤销 DDL。
- **空字符串 = NULL**：Oracle 将空字符串视为 `NULL`。断言空字符串行为的测试需要考虑到这一点。
- **序列**：自增使用序列；插入多行的测试需要注意 `NEXTVAL`/`CURRVAL` 语义。
- **版本检测**：`get_server_version()` 查询 `PRODUCT_COMPONENT_VERSION`，如果无法确定则默认返回 `(19, 0, 0)`。

💡 *AI Prompt:* "单元测试、集成测试和端到端测试有什么区别？"