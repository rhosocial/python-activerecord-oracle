<!-- docs/zh_CN/migration/README.md -->
# Oracle 后端的迁移（Migrations）

本页**只涵盖针对 Oracle 运行迁移时的特有内容**。迁移系统本身——两种编写 API、术语、排序与依赖规则、
版本记录、dry run 捕获、异常层级以及 `migrate` CLI——位于核心章节：

- [Migrations — The Backend Runtime System](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md)
- [可运行的核心示例](https://github.com/rhosocial/python-activerecord/tree/main/docs/examples/migration)

核心章节中的一切都适用于此。请先阅读该章节；本页不重复其内容。

Oracle 改变了一个比其它所有因素都重要的点：**DDL 不是事务性的**。在运行多语句迁移之前，请先阅读
[§2](#2-oracle-ddl-不是事务性的隐式提交)。

## 1. 为迁移配置 Oracle 后端

使用本仓库自己的配置类与后端类。同步与异步路径均受支持：

```python
import os

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig

config = OracleConnectionConfig(
    host=os.getenv("ORACLE_HOST", "localhost"),
    port=int(os.getenv("ORACLE_PORT", "1521")),
    service_name=os.getenv("ORACLE_SERVICE", "XEPDB1"),
    username=os.getenv("ORACLE_USER", "system"),
    password=os.getenv("ORACLE_PASSWORD", ""),
)
backend = OracleBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
```

`introspect_and_adapt()` 是关键的调用：它让 dialect 在**任何 DDL 被渲染之前**获知服务器的真实版本与能力。
在迁移 runner 中绝不要跳过它——未适配的 dialect 无法回答版本门禁（见
[§3](#3-版本门禁的类型与特性)）。

`OracleConnectionConfig` 还带有通用的 `version` 字段（`Optional[Tuple[int, ...]]`）。设置它会固定
dialect 渲染所依据的版本，使生成的 DDL 在 CI 中或无法连接实际服务器时保持一致；保留为 `None` 则由检测
填充。异步对应类是来自
`rhosocial.activerecord.backend.impl.oracle.backend.async_backend` 的 `AsyncOracleBackend`——它具有相同的
`connect()` / `introspect_and_adapt()` 契约。

对于**类风格 CLI**，Oracle 适配器（`named-migration`）从 `--host` / `--port` / `--service` /
`--user` / `--password` 读取相同的值（以 `ORACLE_*` 环境变量作为回退），或从已保存的
`--named-connection` 读取。命令行上不需要 Python 配置对象。

## 2. Oracle DDL 不是事务性的（隐式提交）

Oracle 在**每条 DDL 语句之前和之后都会发出隐式 `COMMIT`**（`CREATE TABLE`、`ALTER TABLE`、
`DROP TABLE` 等）。DDL 无法被包裹进事务，也无法被回滚。这带来了核心章节通用保证之外的一些后果：

- **多语句迁移组不是原子的。** 如果第 3 条语句失败，第 1、2 条语句已经提交。没有任何回滚能把它们撤销。
- **失败会留下 schema 偏差。** `run_migration_group()` 只在成功时写入版本记录——失败时不写入任何记录。
  因此组中途失败后，schema 已被部分修改但没有记录版本，重试会从头开始，可能遇到
  `ORA-00955`（名称已被现有对象占用）之类的错误。
- **`transaction_mode` 无法解决这个问题。** `TransactionMode.AUTO` / `STEP` / `NONE` 控制核心
  `Procedure` runner 如何包裹工作；它们无法阻止 Oracle 的隐式提交。
- **不要相信失败信息中的 `transactional_ddl` 标志。** 核心通过调用
  `dialect.supports_transactional_ddl()` 来设置 `MigrationFunctionExecutionError.transactional_ddl`，
  并在该 dialect 没有此方法时默认为 `True`。Oracle dialect 目前没有覆盖它，因此该标志可能显示为 `True`，
  即使 Oracle 的 DDL 并非事务性的。无论该标志如何，**一律**将所有 Oracle DDL 视为非事务性的。

针对 Oracle 迁移的实用规则：

1. 尽可能让每个迁移只包含一条 DDL 语句；每条语句自行提交，便于推理。
2. 让 `DOWN` 迁移容忍对象缺失（在服务器版本支持时渲染 `IF EXISTS`——见
   [§3](#3-版本门禁的类型与特性)），以便手动清理部分失败留下的状态。
3. 在真正运行前先用 `dry_run=True`（或 `--dry-run`）预览，并把渲染出的语句当作一个已提交的序列来阅读，
   而不是当作单个事务。
4. 失败后先手动校正 schema 再重试；绝不要假设迁移组“已回滚”。

干净成功时版本记录仍会照常写入，已应用/重复检查也仍与核心章节所述完全一致——唯一的注意点是
*失败的*迁移组无法被撤销。

## 3. 版本门禁的类型与特性

Oracle dialect 会根据所连接的服务器自我适配。`get_server_version()`、`version_full` 与
`ru_version`（`VERSION_FULL` 的 release update 分量）是各特性门禁所比较的依据——这些门禁刻意比较数字，
而非 Oracle 的产品名称。会影响 DDL 的门禁示例：

- `supports_identity_column()` —— `GENERATED ... AS IDENTITY` 从 **12c** 起才会渲染。
- `FETCH FIRST ... WITH TIES` —— 门禁版本为 **12c**。
- 原生 `BOOLEAN` / `JSON` 类型以及 `DROP TABLE IF EXISTS` —— 由更新的版本 / release update 控制。
- `ru_version` 用于区分 23ai 产品线与其更名后的 26ai（两者主版本号都是 `23`；不同的数字是 release
  update）。

因此，**同一个迁移函数在不同 Oracle 版本上可能渲染出不同的 SQL**。要保持迁移可复现，可以在
`OracleConnectionConfig` 中固定 `version=(...)`，或在发布清单中记录目标版本。如果版本检测失败，
dialect 会保持未适配状态，门禁无法给出答案——在迁移运行前可用 Oracle CLI 的 `info` 子命令检查服务器视图。

由于门禁是在渲染时应用的，**不要手工拼接原始 SQL 字符串**。请通过 `ctx.apply(...)` 发出语句对象
（例如模型语句 getter `Model.create_table_statement(backend=ctx.backend)`），或通过
`ctx.execute(FQN, ...)` 调用命名表达式。只有这样，dialect 才能应用正确、感知版本的 Oracle 写法。

## 4. Oracle 上的两种 API 风格

两种 API 的选择在核心章节中已有说明，此处不变。简言之：新工作推荐使用函数风格；类风格保留用于维护
已有的迁移类。

### 4.1 函数风格（`@migration` + `run_migration_group`）

按核心章节所述编写迁移函数，只是后端/配置对象换成 Oracle 的。完整可运行脚本见
[`docs/examples/migration/01_function_migration.py`](../../examples/migration/01_function_migration.py)。

```python
from rhosocial.activerecord.backend.migration import migration, MigrationDirection

@migration("v001_create_app_users", description="create the initial app_users table")
def create_app_users(ctx):
    if ctx.direction == MigrationDirection.UP:
        ctx.apply(AppUser.create_table_statement(backend=ctx.backend))
    else:
        ctx.apply(AppUser.drop_table_statement(if_exists=True, backend=ctx.backend))
```

使用 `run_migration_group()` 在进程内针对 Oracle 运行它（dry run → UP → DOWN），或者通过核心
`migrate` CLI 运行——该 CLI 通过 `--backend-class` / `--config-class` / `--config` 以通用方式解析连接：

```bash
python -m rhosocial.activerecord.cli migrate myapp.migrations:create_users \
    --backend-class rhosocial.activerecord.backend.impl.oracle.backend:OracleBackend \
    --config-class rhosocial.activerecord.backend.impl.oracle.config:OracleConnectionConfig \
    --config host=localhost --config service_name=XEPDB1 --config username=system \
    --record-store records.json --dry-run
```

真正运行时还需传入 `--config password=...`。关于 `--list`、`--plan`、`--direction down`、`-o json`、
依赖规则以及异步变体，请参见核心章节。

### 4.2 类风格（`NamedMigration` + `named-migration` CLI）

编写 `NamedMigration` 子类，其 `up()` / `down()` 调用 `ctx.execute(FQN, ...)`，然后用本仓库的 CLI 驱动
它们。CLI 适配器是
`src/rhosocial/activerecord/backend/impl/oracle/cli/named_migration.py`；它委托给共享 helper，并根据连接
参数构造 `OracleBackend`（或使用 `--async` 时构造 `AsyncOracleBackend`）。

```bash
rhosocial-activerecord-oracle named-migration myapp.migrations.v001.CreateUsersTable \
    --host localhost --service XEPDB1 --user system --direction up --record-store ./migrations.json
rhosocial-activerecord-oracle named-migration myapp.migrations.v001.CreateUsersTable \
    --service XEPDB1 --direction down --dry-run
rhosocial-activerecord-oracle named-migration myapp.migrations --list
```

将 `python -m rhosocial.activerecord.backend.impl.oracle` 替换控制台脚本，上述命令同样有效。其它参数
（`--describe`、`--param KEY=VALUE`、`--all`、`--single-transaction`）记录在核心章节中。已有的类风格
示例位于 `src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/`；一个本地的小型
演示在 [`docs/examples/migration/02_named_migration.py`](../../examples/migration/02_named_migration.py)。

## 5. 本地示例

| 脚本 | 演示内容 |
|---|---|
| [`01_function_migration.py`](../../examples/migration/01_function_migration.py) | `@migration` + `ctx.apply()`，使用 Oracle 后端；`run_migration_group` dry run → UP → DOWN |
| [`02_named_migration.py`](../../examples/migration/02_named_migration.py) | `NamedMigration` 类 + `MigrationRunner`，使用 Oracle 配置与命名表达式 DDL |
| [`migration_expressions.py`](../../examples/migration/migration_expressions.py) | 类风格演示所用的感知版本的 DDL 表达式 |

这些脚本需要可连接的 Oracle 服务器，仅供阅读/改造；连接值来自 `ORACLE_*` 环境变量。关于迁移系统的
完整说明，请回到[核心迁移章节](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md)。
