# 表分区

## 概述

Oracle 支持表分区用于大表。

## 分区策略

| 策略 | 说明 | 最低版本 |
|------|------|---------|
| RANGE | 范围分区 | 8.0+ |
| HASH | 哈希分区 | 8.0+ |
| LIST | 列表分区 | 9.0+ |
| COMPOSITE | 复合分区 | 8.0+ |
| INTERVAL | 间隔分区 | 11.0+ |

## 创建分区

### 声明式分区 Spec（模型级）

Oracle 分区可在模型上通过后端定义的 Spec 声明；Oracle 方言在
`generate_create_table(dialect)` 时认领，其他后端自动忽略：

```python
from rhosocial.activerecord.backend.impl.oracle.ddl_spec import (
    OracleRangePartition, OraclePartitionDefinitionSpec, OraclePartitionBound,
    OracleIntervalPartition,
)

class Orders(ActiveRecord):
    __table_partition__ = [
        OracleRangePartition("created_at", [
            OraclePartitionDefinitionSpec("p2026", less_than=[OraclePartitionBound(2027)]),
            OraclePartitionDefinitionSpec("p_max", less_than=[OraclePartitionBound("MAXVALUE")]),
        ]),
        # 间隔分区：OracleIntervalPartition.monthly/.yearly/.daily
        OracleIntervalPartition.monthly("created_at"),
    ]

expr = Orders.generate_create_table(dialect)
```

间隔表达式由 `OracleIntervalFunctionExpression` 构造（`NUMTOYMINTERVAL` /
`NUMTODSINTERVAL`，内联渲染——Oracle DDL 不接受绑定变量）。序列列默认值使用
`OracleSequenceDefault(column, sequence)`。下方表达式层路径仍完全支持，
作为逃生舱保留。

```sql
-- RANGE 分区
CREATE TABLE orders (
    id NUMBER PRIMARY KEY,
    order_date DATE,
    amount NUMBER(10,2)
) PARTITION BY RANGE (order_date) (
    PARTITION p2022 VALUES LESS THAN (DATE '2023-01-01'),
    PARTITION p2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p2024 VALUES LESS THAN (DATE '2025-01-01')
);

-- LIST 分区
CREATE TABLE users (
    id NUMBER PRIMARY KEY,
    region VARCHAR2(20)
) PARTITION BY LIST (region) (
    PARTITION p_north VALUES ('north'),
    PARTITION p_south VALUES ('south'),
    PARTITION p_east VALUES ('east'),
    PARTITION p_west VALUES ('west')
);

-- HASH 分区
CREATE TABLE transactions (
    id NUMBER PRIMARY KEY,
    amount NUMBER(10,2)
) PARTITION BY HASH (id) PARTITIONS 4;
```

## 分区管理

```sql
-- 添加分区
ALTER TABLE orders ADD PARTITION p2025 VALUES LESS THAN (DATE '2026-01-01');

-- 删除分区
ALTER TABLE orders DROP PARTITION p2022;

-- 截断分区
ALTER TABLE orders TRUNCATE PARTITION p2022;

-- 交换分区
ALTER TABLE orders EXCHANGE PARTITION p2023 WITH TABLE orders_archive;
```

## 间隔分区

```sql
-- 自动创建分区（Oracle 11g+）
CREATE TABLE logs (
    id NUMBER PRIMARY KEY,
    log_date DATE
) PARTITION BY RANGE (log_date)
INTERVAL (INTERVAL '1' MONTH) (
    PARTITION p_initial VALUES LESS THAN (DATE '2023-01-01')
);
```

## 另请参阅

- [性能](../troubleshooting/performance.md) — 查询优化

💡 *AI 提示：* "何时应该在 Oracle 中使用间隔分区？"
