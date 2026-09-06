# 测试

本节介绍 Oracle 后端的测试。

## 内容

- [测试配置](configuration.md): Oracle 特定测试设置
- [本地测试](local.md): 基于 Docker 的本地测试环境

## 测试原则

### 同步/异步对等

所有具有 IO 操作的后端必须为等效场景准备**配对的同步和异步测试**：

```python
# 同步测试
def test_create_user():
    user = User(name="Alice").create()
    assert user.id is not None

# 异步测试——相同逻辑，异步 API
async def test_async_create_user():
    user = await AsyncUser(name="Alice").create()
    assert user.id is not None
```

如果后端仅支持同步或仅支持异步，则仅准备相应的测试。

### 表达式类——无 IO

表达式测试不涉及数据库 IO——它们只构建 SQL 并验证生成的 SQL：

```python
def test_expression_sql():
    expr = Eq(User.name, "Alice")
    assert expr.to_sql(dialect) == '"NAME" = ?'
    assert expr.params == ["Alice"]
```

表达式测试不需要异步对应项。

### ActiveRecord 测试——使用 Testsuite

ActiveRecord 功能测试（模型 CRUD、关系、查询）使用 **testsuite**：

```
python-activerecord-testsuite/
└── src/rhosocial/activerecord/testsuite/feature/
    ├── basic/      # 级别 1——必须首先通过
    ├── relation/   # 级别 1——必须首先通过
    ├── query/      # 级别 1——必须首先通过
    ├── events/     # 级别 2——扩展行为
    ├── mixins/     # 级别 2
    ├── interface/  # 级别 2
    └── examples/   # 级别 2
```

每个后端提供**提供者实现**，将测试连接到其特定数据库。测试逻辑是共享的；只有提供者层因后端而异。

**运行 testsuite 测试：**

```bash
cd python-activerecord-oracle
PYTHONPATH=tests .venv3.14-ubuntu26.04/bin/pytest \
    ../python-activerecord-testsuite/src/rhosocial/activerecord/testsuite/feature/relation/
```

### 测试类别摘要

| 测试内容 | 方法 | IO？ | 异步？ |
|----------|------|------|--------|
| 表达式类（方言 SQL 生成） | 单元测试，无数据库 | 否 | 否 |
| 类型适配器（类型转换） | 单元测试，无数据库 | 否 | 否 |
| 命名功能（连接、表达式、过程、迁移） | 后端 CLI 脚本 | 是 | 如果支持 |
| ActiveRecord 功能（CRUD、关系、查询） | Testsuite + 提供者 | 是 | 是 |
| 后端特定功能（唯一类型、语法） | 项目特定测试 | 是 | 是 |

### Oracle 特定测试注意事项

| 注意事项 | 描述 |
|----------|------|
| 串行执行 | 测试必须串行运行。不要使用 `pytest -n auto`。 |
| DDL 自动提交 | Oracle DDL 自动提交；测试隔离需要仔细设置 |
| 序列状态 | 序列在测试间持久存在；使用 `TRUNCATE` + 序列重置 |
| 空字符串 = NULL | 考虑 Oracle 将 `''` 视为 `NULL` |
| 标识符大小写 | 未加引号的标识符折叠为大写 |

## 另请参阅

- [核心后端测试指南](backend_testing.md)
- [核心 Testsuite 提供者指南](provider_guide.md)
