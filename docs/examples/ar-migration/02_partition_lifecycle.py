# docs/examples/ar-migration/02_partition_lifecycle.py
"""Oracle 专属 AR 迁移举例：分区生命周期（离线 dry-run 渲染）。

覆盖 add / drop / retention / table_drop / table_alter，并重点展示 Oracle
独有的 SPLIT 与 MERGE —— 二者由方言生成真实的 ``OracleSplitPartitionStatement``
与 ``OracleMergePartitionsStatement``。所有 SQL 均由 ``expr.to_sql()[0]`` 渲染：
不手拼 SQL 字符串，不调用 ``ctx.apply()``（那需要真实后端连接）。
"""

from __future__ import annotations

from datetime import date

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionStrategy,
    OraclePartitionValue,
)
from rhosocial.activerecord.backend.impl.oracle.partition import (
    OraclePartitionPlan,
    OracleRangePolicy,
)


def _range(dialect, name, bound):
    """一个 RANGE 分区定义。"""
    return OraclePartitionDefinition(name=name, less_than=[OraclePartitionValue(dialect, bound)])


def _emit(title, suggestion):
    """打印一个建议：真实表达式链逐条渲染，tri-state 给出文字说明。"""
    print(title)
    operations = suggestion.operations
    if operations is False:
        print("    (operations is False: Oracle 对此无对应机制)")
    elif operations is None:
        print("    (operations is None: 无需操作)")
    else:
        for expr in operations:
            print("    " + expr.to_sql()[0])
    print()


def scenario_add(dialect):
    suggestion = dialect.suggest_partition_add(
        OraclePartitionStrategy.RANGE,
        {
            "table": "T",
            "definitions": [_range(dialect, "P1", 100), _range(dialect, "P2", 200)],
        },
    )
    _emit("[01] ADD PARTITION - 每个 definition 生成一条 ALTER TABLE", suggestion)


def scenario_drop(dialect):
    suggestion = dialect.suggest_partition_drop(
        OraclePartitionStrategy.RANGE, {"table": "T", "names": ["P0", "P1"]}
    )
    _emit("[02] DROP PARTITION - 按名称删除（多条）", suggestion)

    empty = dialect.suggest_partition_drop(OraclePartitionStrategy.RANGE, {"table": "T"})
    _emit("[02b] DROP PARTITION - 未给名称（tri-state: None）", empty)


def scenario_split(dialect):
    suggestion = dialect.suggest_partition_split(
        OraclePartitionStrategy.RANGE,
        {
            "table": "T",
            "partition": "P0",
            "at_values": [OraclePartitionValue(dialect, 150)],
            "into": [_range(dialect, "P0", 150), _range(dialect, "P1", 200)],
        },
    )
    _emit("[03] SPLIT PARTITION - Oracle 独有：在 AT 边界切分现有分区", suggestion)


def scenario_merge(dialect):
    suggestion = dialect.suggest_partition_merge(
        OraclePartitionStrategy.RANGE,
        {
            "table": "T",
            "names": ["P0", "P1"],
            "into": [_range(dialect, "P01", 200)],
        },
    )
    _emit("[04] MERGE PARTITIONS - Oracle 独有：合并相邻分区", suggestion)


def scenario_retention_definitions(dialect):
    definitions = [
        _range(dialect, "P0", date(2026, 1, 1)),
        _range(dialect, "P1", date(2026, 2, 1)),
        _range(dialect, "P2", date(2026, 3, 1)),
        OraclePartitionDefinition(name="PMAX", less_than=[OraclePartitionMaxValue(dialect)]),
    ]
    suggestion = dialect.suggest_partition_retention(
        OraclePartitionStrategy.RANGE,
        {"table": "T", "definitions": definitions, "cutoff": date(2026, 2, 1)},
    )
    _emit("[05] RETENTION - date cutoff 淘汰旧分区（MAXVALUE 永不淘汰）", suggestion)


def scenario_retention_plan(dialect):
    plan = OraclePartitionPlan(
        parent="EVENTS",
        method="RANGE",
        columns="CREATED_AT",
        policy=OracleRangePolicy(
            bounds=(
                (date(2026, 1, 1), date(2026, 2, 1)),
                (date(2026, 2, 1), date(2026, 3, 1)),
                (date(2026, 3, 1), date(2026, 4, 1)),
            ),
            maxvalue=True,
        ),
    )
    suggestion = dialect.suggest_partition_retention(
        OraclePartitionStrategy.RANGE,
        {"table": "EVENTS", "plan": plan, "cutoff": date(2026, 3, 1)},
    )
    _emit("[06] RETENTION - 真实 OraclePartitionPlan 读取 HIGH_VALUE 类比", suggestion)


def scenario_table_drop(dialect):
    suggestion = dialect.suggest_partition_table_drop(OraclePartitionStrategy.RANGE, {"table": "T"})
    _emit("[07] TABLE DROP - 删除整张分区表（DropTableStatement）", suggestion)


def scenario_table_alter(dialect):
    suggestion = dialect.suggest_partition_table_alter(
        OraclePartitionStrategy.RANGE, {"table": "T"}
    )
    _emit("[08] TABLE ALTER - Oracle 无法原地改分区方案（tri-state: False）", suggestion)


def main():
    dialect = OracleDialect(version=(23, 1, 0))
    scenario_add(dialect)
    scenario_drop(dialect)
    scenario_split(dialect)
    scenario_merge(dialect)
    scenario_retention_definitions(dialect)
    scenario_retention_plan(dialect)
    scenario_table_drop(dialect)
    scenario_table_alter(dialect)


if __name__ == "__main__":
    main()
