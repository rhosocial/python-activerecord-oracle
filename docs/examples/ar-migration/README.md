# Oracle 专属 AR 迁移举例（离线 dry-run 渲染）

本目录给出 Oracle 后端的分区迁移举例，与 core 的通用举例并列：同一迁移意图，
这里的 SQL 由 Oracle 方言自己的格式化器渲染。全部脚本**离线可运行**——不连数据库、
不调用 `ctx.apply()`，只做 `suggest_partition_*` → `expr.to_sql()[0]` 的渲染，
所有打印出来的 SQL 都来自表达式节点，没有手拼字符串。

## 后端差异要点（相对其它后端）

- **5 个顶层策略**：`RANGE / LIST / HASH / INTERVAL / REFERENCE`（由策略子句或 Oracle 专有词表达）
  （比其它后端少，且没有 SYSTEM；SYSTEM 不在本范围）。INTERVAL 是 Oracle 的自动扩展分区，
  REFERENCE 通过外键继承父表分区、没有自己的分区键。
- **SPLIT / MERGE 是 Oracle 独有**：`suggest_partition_split` / `suggest_partition_merge`
  返回真实的 `OracleSplitPartitionStatement` / `OracleMergePartitionsStatement`
  （PostgreSQL / MySQL 等对这些恒为 `False`）。MERGE 需要恰好两个 `names` 与一个 `into`。
- **MAXVALUE 边界**：RANGE 用 `OraclePartitionMaxValue` 表达上限；retention 永远不淘汰
  MAXVALUE 分区（它没有有限上界）。
- **plan 契约是 dict 或属性对象**：字段名 `table` / `keys` / `definitions` / `partition` /
  `at_values` / `into` / `names` / `cutoff` / `interval` / `fk_constraint` 等；也接受真实的
  `OraclePartitionPlan` / `OracleRangePolicy`（retention 的 plan 形态）。
- **cutoff 必须是 `date`/`datetime`**：retention 不接受 ISO 字符串；INTERVAL 的 `interval`
  是一个 `BaseExpression`，本例用 `RawSQL(dialect, "NUMTOYMINTERVAL(1, 'MONTH')")` 传入。
- **tri-state**：有真实链时返回表达式元组；无对应机制返回 `False`（如 table_alter、
  REFERENCE 的 add/drop/split/merge、未知策略）；无需操作返回 `None`（如未给名称的 drop、
  没有过期分区的 retention）。脚本对这些情形打印文字说明，而不是伪造 SQL。
- **版本**：用 `OracleDialect(version=(23, 1, 0))` 构造，无需连接、无需 `introspect_and_adapt`。

## 脚本清单与场景

### `01_partition_build.py` — 建表分区建议（7 个场景）

| # | 场景 | 策略 | 产物 |
|---|------|------|------|
| 01 | 按 ID 数值区间，末尾 MAXVALUE | RANGE | `OraclePartitionByRange` |
| 02 | 按 STATUS 值列表 | LIST | `OraclePartitionByList` |
| 03 | 按 ID 哈希分 4 区 | HASH | `OraclePartitionByHash` |
| 04 | 按 CREATED_AT 月度自动扩展 | INTERVAL | `OracleIntervalPartitionClause` |
| 05 | 外键继承父表分区 | REFERENCE | `OracleReferencePartitionClause` |
| 06 | 未知策略 SPATIAL → `False` | — | tri-state（无 SQL） |
| 07 | RANGE 缺 definitions → `False` | RANGE | tri-state（无 SQL） |

### `02_partition_lifecycle.py` — 生命周期（9 个场景）

| # | 场景 | 产物 |
|---|------|------|
| 01 | ADD：每个 definition 一条 `ADD PARTITION` | `OracleAddPartitionStatement` ×2 |
| 02 | DROP：按名称删除多个分区 | `OracleDropPartitionStatement` ×2 |
| 02b | DROP：未给名称 → `None` | tri-state |
| 03 | SPLIT：在 AT 边界切分 P0（Oracle 独有） | `OracleSplitPartitionStatement` |
| 04 | MERGE：合并 P0、P1 为 P01（Oracle 独有） | `OracleMergePartitionsStatement` |
| 05 | RETENTION：date cutoff 淘汰旧分区，保留 MAXVALUE | `OracleDropPartitionStatement` ×2 |
| 06 | RETENTION：真实 `OraclePartitionPlan` 形态 | `OracleDropPartitionStatement` ×2 |
| 07 | TABLE DROP：删整张分区表 | `DropTableStatement` |
| 08 | TABLE ALTER：无法原地改分区方案 → `False` | tri-state |

## 运行方式

在本 worktree 根目录执行（无需数据库、无需代理）：

```powershell
$env:PYTHONPATH='src'
.\.venv3.14\Scripts\python.exe docs\examples\ar-migration\01_partition_build.py
.\.venv3.14\Scripts\python.exe docs\examples\ar-migration\02_partition_lifecycle.py
```

两脚本均应退出码 0，并打印由方言渲染的 Oracle DDL。
