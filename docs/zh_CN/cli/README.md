# 命令行界面

## 概述

每个 Oracle 后端都包含一个命令行界面（CLI），用于数据库操作。CLI 提供查询、内省和管理 Oracle 数据库的命令，无需编写 Python 代码。

CLI 命令分为两类：

1. **后端特定命令** — Oracle 独有操作（query、introspect、info、status）
2. **核心继承命令** — 所有后端共享（named-expression、named-procedure、named-migration、named-connection）

## 调用方式

安装包后，CLI 作为 `rhosocial-activerecord-oracle` 安装：

```bash
pip install rhosocial-activerecord-oracle
```

然后直接调用命令：

```bash
rhosocial-activerecord-oracle <command> [options]
```

此命令在 `pyproject.toml` 中注册，等效于 `python -m rhosocial.activerecord.backend.impl.oracle`。

## 输出格式

CLI 通过 `-o` / `--output` 选项支持多种输出格式：

| 格式 | 描述 | 需要 Rich |
|------|------|----------|
| `table` | 带边框的人类可读表格（默认） | 是 |
| `json` | JSON 对象数组 | 否 |
| `csv` | 逗号分隔值 | 否 |
| `tsv` | 制表符分隔值 | 否 |

安装 Rich 后，`table` 格式提供带彩色边框的美化输出。未安装 Rich 时，CLI 自动回退到 `json` 格式。

### Rich 集成

CLI 与 [Rich](https://github.com/Textualize/rich) 库集成，提供增强的终端输出：

- **彩色边框**: 使用 Unicode 制表字符的表格边框
- **ASCII 回退**: 使用 `--rich-ascii` 强制 ASCII 边框（`+`、`-`、`|`）
- **自动检测**: 如果未安装 Rich，自动回退到 JSON 输出

```bash
# 默认表格输出（Unicode 边框）
rhosocial-activerecord-oracle query ... "SELECT * FROM users"

# ASCII 边框（适用于不支持 Unicode 的终端）
rhosocial-activerecord-oracle query ... --rich-ascii "SELECT * FROM users"

# 强制 JSON 输出
rhosocial-activerecord-oracle query ... -o json "SELECT * FROM users"
```

### 输出示例

**表格格式（默认）：**
```
┌─────┬─────────┬───────┐
│ ID  │ NAME    │ EMAIL │
├─────┼─────────┼───────┤
│ 1   │ Alice   │ a@x   │
│ 2   │ Bob     │ b@x   │
└─────┴─────────┴───────┘
```

**JSON 格式：**
```json
[
  {"id": 1, "name": "Alice", "email": "a@x"},
  {"id": 2, "name": "Bob", "email": "b@x"}
]
```

## 后端特定命令

这些命令由 Oracle 后端实现，直接与 Oracle 交互：

| 命令 | 描述 | 需要连接 |
|------|------|---------|
| `info` | 显示环境和协议信息 | 否 |
| `query` | 执行 SQL 查询 | 是 |
| `introspect` | 数据库内省（表、列、索引等） | 是 |
| `status` | 显示服务器状态和配置 | 是 |

### info

无需数据库连接即可显示环境信息：

```bash
rhosocial-activerecord-oracle info
```

### query

直接执行 SQL 查询：

```bash
rhosocial-activerecord-oracle query \
    --host localhost --port 1521 --database ORCLPDB1 \
    --user system --password secret \
    "SELECT * FROM users WHERE ROWNUM <= 10"
```

### introspect

检查数据库元数据：

```bash
# 列出所有表
rhosocial-activerecord-oracle introspect tables \
    --host localhost --port 1521 --database ORCLPDB1

# 描述特定表
rhosocial-activerecord-oracle introspect table users \
    --host localhost --port 1521 --database ORCLPDB1

# 列出列
rhosocial-activerecord-oracle introspect columns users \
    --host localhost --port 1521 --database ORCLPDB1
```

#### 内省类型

每个后端支持不同的内省类型：

| 类型 | 描述 |
|------|------|
| `tables` | 列出所有表 |
| `views` | 列出所有视图 |
| `table` | 描述特定表 |
| `columns` | 列出表的列 |
| `indexes` | 列出表的索引 |
| `foreign-keys` | 列出表的外键 |
| `triggers` | 列出触发器 |
| `database` | 数据库信息 |

后端特定类型：

| 后端 | 额外类型 |
|------|---------|
| PostgreSQL | `extensions` |
| **Oracle** | **`sequences`、`synonyms`、`materialized-views`、` tablespaces`** |

### status

显示服务器状态：

```bash
rhosocial-activerecord-oracle status \
    --host localhost --port 1521 --database ORCLPDB1
```

## 核心继承命令

这些命令**从核心 `python-activerecord` 库继承**，在所有后端上完全相同地工作。后端 CLI 只是委托给共享的核心基础设施：

```
后端 CLI 适配器（薄包装器）
    └── 核心 CLI 助手（共享逻辑）
        ├── Resolver — 通过完全限定名加载 Python 可调用对象
        ├── Runner — 使用事务管理执行
        └── CLI 适配器 — 参数解析和输出
```

### 为什么使用命名功能？

命名功能让您**将复杂配置编码为单个名称**，避免冗长的命令行参数，并启用无法通过 CLI 标志表达的参数组合。

**命名连接** — 封装所有连接参数：

```bash
# 不使用命名连接：长参数列表
rhosocial-activerecord-oracle query \
    --host prod-db.example.com --port 1521 --database ORCLPDB1 \
    --user readonly --password secret --encoding UTF-8 \
    "SELECT * FROM users"

# 使用命名连接：一个名称包含所有内容
rhosocial-activerecord-oracle query \
    --named-connection myapp.connections.prod_readonly \
    "SELECT * FROM users"
```

**命名表达式** — 封装复杂查询逻辑：

```bash
# 不使用命名表达式：难以 shell 转义的复杂 SQL
rhosocial-activerecord-oracle query \
    "SELECT u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.created_at >= DATE '2026-01-01' GROUP BY u.id HAVING COUNT(o.id) > 5 ORDER BY order_count DESC FETCH FIRST 20 ROWS ONLY"

# 使用命名表达式：一个名称，类型安全参数
rhosocial-activerecord-oracle named-expression \
    myapp.queries.high_value_customers \
    --param since=2026-01-01 --param min_orders=5
```

**命名过程** — 封装多步骤工作流：

```bash
# 不使用命名过程：多个顺序命令
rhosocial-activerecord-oracle query "BEGIN ..."
rhosocial-activerecord-oracle query "UPDATE inventory ..."
rhosocial-activerecord-oracle query "INSERT INTO orders ..."
rhosocial-activerecord-oracle query "COMMIT;"

# 使用命名过程：一个命令，事务管理
rhosocial-activerecord-oracle named-procedure \
    myapp.workflows.place_order \
    --param user_id=42 --param product_id=100 --param quantity=3
```

**命名迁移** — 封装带依赖关系的版本化模式更改：

```bash
rhosocial-activerecord-oracle named-migration up add_users_table
rhosocial-activerecord-oracle named-migration down add_users_table
```

| 功能 | 好处 |
|------|------|
| 命名连接 | 在可版本化 Python 代码中存储连接配置；跨脚本共享 |
| 命名表达式 | 封装复杂 SQL；类型安全参数；跨工具重用 |
| 命名过程 | 带事务管理的多查询工作流；并行执行 |
| 命名迁移 | 带依赖跟踪的版本化模式更改；向上/向下支持 |

### 命令参考

| 命令 | 源模块 | 描述 |
|------|--------|------|
| `named-expression` | `backend.named_expression` | 执行在 Python 中定义的类型安全参数化 SQL |
| `named-procedure` | `backend.named_expression.procedure` | 执行带事务支持的多查询编排 |
| `named-procedure-graph` | `backend.named_expression.procedure` | 执行过程图（DAG 工作流） |
| `named-migration` | `backend.migration` | 执行带依赖跟踪的版本化模式更改 |
| `named-connection` | `backend.named_connection` | 管理和测试命名连接配置 |

## 连接参数

所有需要数据库连接的命令都接受以下通用参数：

| 参数 | 描述 |
|------|------|
| `--host` | 数据库服务器主机名 |
| `--port` | 数据库服务器端口（默认：1521） |
| `--database` | Oracle 服务名或 SID |
| `--user` | 认证用户名 |
| `--password` | 认证密码 |
| `--async` | 使用异步后端 |
| `--named-connection` | 使用命名连接配置 |
| `--conn-param` | 额外连接参数 |
| `--log-level` | 设置日志级别（DEBUG、INFO、WARNING、ERROR） |

### 后端特定连接参数

| 后端 | 额外参数 |
|------|---------|
| MySQL | `--charset` |
| PostgreSQL | （无额外） |
| SQL Server | `--trusted-connection`、`--driver`、`--encrypt`/`--no-encrypt`、`--trust-server-certificate` |
| **Oracle** | **`--service`（`--database` 的别名）、`--mode`（thin/thick）** |
| Firebird | `--charset`、`--role` |

## 全局选项

| 选项 | 描述 |
|------|------|
| `-h`、`--help` | 显示帮助消息并退出 |
| `--log-level` | 设置日志级别（DEBUG、INFO、WARNING、ERROR） |

## 架构

CLI 在所有后端遵循一致的架构：

```
backend/impl/oracle/
├── __main__.py          # 入口点，构建解析器，分派到处理程序
└── cli/
    ├── __init__.py      # COMMAND_NAMES 列表，register_commands()
    ├── connection.py    # 连接参数解析和后端创建
    ├── output.py        # 输出格式提供者（Rich/JSON/CSV/TSV）
    │
    │   # 后端特定命令
    ├── info.py          # 'info' 命令处理程序
    ├── query.py         # 'query' 命令处理程序
    ├── introspect.py    # 'introspect' 命令处理程序
    ├── status.py        # 'status' 命令处理程序
    │
    │   # 核心继承命令（薄适配器）
    ├── named_expression.py      # 委托给核心 named_expression.cli
    ├── named_procedure.py       # 委托给核心 named_expression.procedure.cli
    ├── named_procedure_graph.py # 委托给核心 named_expression.procedure.cli
    ├── named_migration.py       # 委托给核心 migration.cli
    └── named_connection.py      # 委托给核心 named_connection.cli
```

## 另请参阅

- [安装指南](../installation_and_configuration/installation.md) — 设置说明
- [连接管理](../installation_and_configuration/pool.md) — 连接配置
- [核心命名功能](https://github.com/Rhosocial/python-activerecord/tree/main/docs/zh_CN) — 命名连接、表达式、过程、迁移文档

💡 *AI Prompt:* "如何从命令行列出 Oracle 数据库中的所有表？"
