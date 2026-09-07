# Table Partitioning

## Overview

Oracle supports table partitioning for large tables.

## Partitioning Strategies

| Strategy | Description | Minimum Version |
|----------|-------------|-----------------|
| RANGE | Range partitioning | 8.0+ |
| HASH | Hash partitioning | 8.0+ |
| LIST | List partitioning | 9.0+ |
| COMPOSITE | Composite partitioning | 8.0+ |
| INTERVAL | Interval partitioning | 11.0+ |

## Creating Partitions

### Declarative Partition Specs (model level)

Oracle partitioning can be declared on the model via backend-defined Specs;
the Oracle dialect claims them at `generate_create_table(dialect)` time and
other backends silently ignore them:

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
        # Interval partitioning: OracleIntervalPartition.monthly/.yearly/.daily
        OracleIntervalPartition.monthly("created_at"),
    ]

expr = Orders.generate_create_table(dialect)
```

Interval expressions are built from `OracleIntervalFunctionExpression`
(`NUMTOYMINTERVAL` / `NUMTODSINTERVAL`, rendered inline — Oracle DDL accepts
no bind variables). Sequence-backed column defaults are available through
`OracleSequenceDefault(column, sequence)`. The expression-level path below
remains fully supported as the escape hatch.

```sql
-- RANGE partitioning
CREATE TABLE orders (
    id NUMBER PRIMARY KEY,
    order_date DATE,
    amount NUMBER(10,2)
) PARTITION BY RANGE (order_date) (
    PARTITION p2022 VALUES LESS THAN (DATE '2023-01-01'),
    PARTITION p2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p2024 VALUES LESS THAN (DATE '2025-01-01')
);

-- LIST partitioning
CREATE TABLE users (
    id NUMBER PRIMARY KEY,
    region VARCHAR2(20)
) PARTITION BY LIST (region) (
    PARTITION p_north VALUES ('north'),
    PARTITION p_south VALUES ('south'),
    PARTITION p_east VALUES ('east'),
    PARTITION p_west VALUES ('west')
);

-- HASH partitioning
CREATE TABLE transactions (
    id NUMBER PRIMARY KEY,
    amount NUMBER(10,2)
) PARTITION BY HASH (id) PARTITIONS 4;
```

## Partition Management

```sql
-- Add partition
ALTER TABLE orders ADD PARTITION p2025 VALUES LESS THAN (DATE '2026-01-01');

-- Drop partition
ALTER TABLE orders DROP PARTITION p2022;

-- Truncate partition
ALTER TABLE orders TRUNCATE PARTITION p2022;

-- Exchange partition
ALTER TABLE orders EXCHANGE PARTITION p2023 WITH TABLE orders_archive;
```

## Interval Partitioning

```sql
-- Auto-create partitions (Oracle 11g+)
CREATE TABLE logs (
    id NUMBER PRIMARY KEY,
    log_date DATE
) PARTITION BY RANGE (log_date)
INTERVAL (INTERVAL '1' MONTH) (
    PARTITION p_initial VALUES LESS THAN (DATE '2023-01-01')
);
```

## See Also

- [Performance](../troubleshooting/performance.md) — Query optimization

💡 *AI Prompt:* "When should I use interval partitioning in Oracle?"
