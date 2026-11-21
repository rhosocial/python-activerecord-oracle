# docs/examples/ar-migration/01_partition_build.py
"""Oracle 专属 AR 迁移举例：建表分区建议（离线 dry-run 渲染）。

无需真实数据库连接：只构造 ``OracleDialect`` 与 plan，调用
``suggest_partition_build`` 得到表达式链，再用 ``expr.to_sql()[0]`` 渲染 SQL。
覆盖 ``OraclePartitionStrategy`` 全部 5 个取值，并展示被拒绝时的 tri-state 返回。
"""

from __future__ import annotations

from datetime import date

from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.operators import RawSQL
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionStrategy,
    OraclePartitionValue,
)


def _range(dialect, name, bound):
    """一个 RANGE 分区定义：``PARTITION name VALUES LESS THAN (bound)``。"""
    return OraclePartitionDefinition(name=name, less_than=[OraclePartitionValue(dialect, bound)])


def _list(dialect, name, *values):
    """一个 LIST 分区定义：``PARTITION name VALUES IN (values)``。"""
    return OraclePartitionDefinition(
        name=name, in_values=[OraclePartitionValue(dialect, value) for value in values]
    )


def main():
    dialect = OracleDialect(version=(23, 1, 0))

    scenarios = [
        (
            "RANGE - 按 ID 数值区间分区，末尾 MAXVALUE 兜底",
            OraclePartitionStrategy.RANGE,
            {
                "keys": [Column(dialect, "ID")],
                "definitions": [
                    _range(dialect, "P1", 100),
                    _range(dialect, "P2", 200),
                    OraclePartitionDefinition(
                        name="PMAX", less_than=[OraclePartitionMaxValue(dialect)]
                    ),
                ],
            },
        ),
        (
            "LIST - 按 STATUS 值列表分区",
            OraclePartitionStrategy.LIST,
            {
                "keys": [Column(dialect, "STATUS")],
                "definitions": [
                    _list(dialect, "P_ACTIVE", "ACTIVE", "PENDING"),
                    _list(dialect, "P_CLOSED", "CLOSED"),
                ],
            },
        ),
        (
            "HASH - 按 ID 哈希分 4 个区",
            OraclePartitionStrategy.HASH,
            {"keys": [Column(dialect, "ID")], "partitions_count": 4},
        ),
        (
            "INTERVAL - 按 CREATED_AT 月度自动扩展（interval 用 RawSQL 传函数）",
            OraclePartitionStrategy.INTERVAL,
            {
                "keys": [Column(dialect, "CREATED_AT")],
                "interval": RawSQL(dialect, "NUMTOYMINTERVAL(1, 'MONTH')"),
                "definitions": [_range(dialect, "P_INITIAL", date(2026, 1, 1))],
            },
        ),
        (
            "REFERENCE - 经外键约束继承父表分区（无自己的分区键）",
            OraclePartitionStrategy.REFERENCE,
            {"fk_constraint": "FK_PARENT"},
        ),
        (
            "拒绝 - 未知策略 SPATIAL（tri-state: False）",
            "SPATIAL",
            {"keys": [Column(dialect, "ID")]},
        ),
        (
            "拒绝 - RANGE 缺少 definitions（tri-state: False）",
            OraclePartitionStrategy.RANGE,
            {"keys": [Column(dialect, "ID")]},
        ),
    ]

    for index, (description, strategy, plan) in enumerate(scenarios, 1):
        print(f"[{index:02d}] {description}")
        operations = dialect.suggest_partition_build(strategy, plan).operations
        if operations is False:
            print("    (declined -> operations is False: 该策略/plan 无可建分区子句)")
        elif operations is None:
            print("    (operations is None: 无需构造)")
        else:
            for expr in operations:
                print("    " + expr.to_sql()[0])
        print()


if __name__ == "__main__":
    main()
