# DDL 特征 Spec

Oracle 实现了核心 DDL 特征认领协议（`dialect.build_spec`）。本章说明 Oracle
方言认领哪些 Spec、如何翻译，以及它新增的 Oracle 特定 Spec。

## 认领机制

`Model.generate_create_table(dialect)` 时，生成器把每个声明的 Spec 交给
`dialect.build_spec(spec)`：

- **接受** → 方言构造并返回表达式层实例，进入 `CreateTableExpression`；
- **不接受** → 返回 `None`，该 Spec 被静默忽略。

接受范围由 Oracle 方言自行决定。

## 通用 Spec

全部通用 Spec 由核心默认翻译认领：

| Spec | Oracle 翻译 |
|------|-------------|
| `CheckSpec` | `TableConstraint(CHECK)`，惰性谓词生成时求值 |
| `UniqueSpec` | `TableConstraint(UNIQUE)` |
| `NotNullSpec` | `ColumnConstraint(NOT NULL)` |
| `PrimaryKeySpec` | 单列→列级 PK / 复合→表级 PK |
| `DefaultSpec` | `ColumnConstraint(DEFAULT)`，参数化 `Literal` |
| `ForeignKeySpec` | `ForeignKeyConstraint`（含参照动作） |
| `IndexSpec` / `PartialIndexSpec` | `IndexDefinition` |
| `JsonColumnSpec` | 列类型补丁 → `JsonType` |

## Oracle 特定 Spec

定义于 `rhosocial.activerecord.backend.impl.oracle.ddl_spec`；以 `isinstance`
认领、由 `OracleDDLSpecMixin` 翻译。仅 Oracle 方言认领。

### 分区 Spec

```python
from rhosocial.activerecord.backend.impl.oracle.ddl_spec import (
    OracleRangePartition, OracleListPartition, OracleHashPartition,
    OracleIntervalPartition, OraclePartitionDefinitionSpec, OraclePartitionBound,
)

class Orders(ActiveRecord):
    __table_partition__ = [
        OracleRangePartition("created_at", [
            OraclePartitionDefinitionSpec("p2026", less_than=[OraclePartitionBound(2027)]),
            OraclePartitionDefinitionSpec("p_max", less_than=[OraclePartitionBound("MAXVALUE")]),
        ]),
        OracleListPartition("region", [
            OraclePartitionDefinitionSpec("p_east", in_values=[OraclePartitionBound("EAST")]),
        ]),
        OracleHashPartition("id", partitions_count=4),
        # 间隔分区（11g+）：Oracle 自动创建新分区
        OracleIntervalPartition.monthly("created_at"),
    ]
```

翻译为 Oracle 分区表达式层，渲染为内联 DDL——Oracle DDL 不接受绑定参数，
边界值经安全转义为内联字面量。完整策略表与生命周期管理见[分区](partition.md)。

间隔表达式由 `OracleIntervalFunctionExpression` 构造（`NUMTOYMINTERVAL` /
`NUMTODSINTERVAL`）；`.monthly()` / `.yearly()` / `.daily()` 工厂覆盖常用场景。

### 序列默认

```python
from rhosocial.activerecord.backend.impl.oracle.ddl_spec import (
    OracleSequenceDefault,
)

class Users(ActiveRecord):
    __table_constraints__ = [
        OracleSequenceDefault(column="id", sequence="users_id_seq"),
    ]
```

经既有 `OracleSequenceValueExpression` 翻译为 `DEFAULT USERS_ID_SEQ.NEXTVAL`。
`sequence` 省略时派生 `<column>_seq`。
