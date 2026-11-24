# Partitioning: DDL, Suggestions and Introspection

Oracle partitioning is owned by the **backend**, not by an ActiveRecord model.
The framework plan (`2026-11-02/partitioned-model-support.md`) puts the line
here:

| Layer | Responsibility |
|---|---|
| ActiveRecord model | the composite primary-key shape a partitioned table needs, including every partitioning column |
| Oracle backend | partition DDL expressions, lifecycle suggestions and introspection |
| Application | table names, partition columns, retention policy and scheduling |

Nothing in this package hard-codes an application's table name or retention
default.

## DDL expressions

Create the parent with `CreateTableStatement(partition=...)`:

```python
from datetime import date

from rhosocial.activerecord.backend.expression import Column, CreateTableStatement
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import PartitionDefinitionExpression
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionByRange,
    OraclePartitionValue,
    OracleSubpartitionClause,
    OracleSubpartitionStrategy,
    partition_definition_options,
)

clause = OraclePartitionByRange(
    dialect,
    [Column(dialect, "CREATED_AT")],
    subpartition_by=OracleSubpartitionClause(
        dialect,
        OracleSubpartitionStrategy.HASH,
        keys=[Column(dialect, "ID")],
        count=2,
    ),
    partitions=[
        PartitionDefinitionExpression(
            dialect,
            name="P1",
            less_than=[OraclePartitionValue(dialect, date(2026, 2, 1))],
            options=partition_definition_options(dialect, subpartitions_count=3),
        ),
        PartitionDefinitionExpression(
            dialect, name="P2", less_than=[OraclePartitionValue(dialect, date(2026, 3, 1))]
        ),
    ],
)
CreateTableStatement(dialect=dialect, table=Table(dialect, "EVENTS"), columns=[...], partition=clause)
```

Renders:

```sql
CREATE TABLE "EVENTS" (...)
PARTITION BY RANGE ("CREATED_AT")
SUBPARTITION BY HASH ("ID") SUBPARTITIONS 2
(
  PARTITION "P1" VALUES LESS THAN (DATE '2026-02-01') SUBPARTITIONS 3,
  PARTITION "P2" VALUES LESS THAN (DATE '2026-03-01')
)
```

### What may vary per partition

Oracle's subpartition **strategy** is declared once, above all top-level
partitions (`SUBPARTITION BY ...`). What a top-level partition may vary is its
own definitions or its HASH count:

- `PartitionDefinitionExpression(..., options=partition_definition_options(dialect,
  subpartitions_count=N))` renders a per-partition `SUBPARTITIONS N` override for
  `SUBPARTITION BY HASH`.
- `PartitionDefinitionExpression(..., subpartitions=[...])` renders explicit
  `SUBPARTITION ... VALUES ...` definitions for RANGE / LIST subpartitioning.
  The number and boundaries may differ from one top-level partition to the
  next.
- `OracleSubpartitionClause(templates=[...])` renders
  `SUBPARTITION TEMPLATE (...)`, the table-level default inherited by new
  top-level partitions. Oracle prefixes the template names with the partition
  name when it instantiates them.

A partition **cannot** choose a different subpartition strategy. That is
Oracle's rule, not a missing feature: composite partitioning has one
subpartition strategy per table, unlike PostgreSQL, where each branch may
pick its own child strategy.

### Renderer guard rails

`partition_definition_options` refuses a non-positive or non-integer
`subpartitions_count`; the renderer refuses a count together with explicit
`subpartitions`. The plan layer refuses a count override when the table
strategy is RANGE or LIST, and refuses subpartition definitions whose bound kind
does not match the table strategy.

## Plans and lifecycle suggestions

The backend's `partition` package describes a tree by policy rather than by a
list of hand-written names:

```python
from datetime import date

from rhosocial.activerecord.backend.impl.oracle.partition import (
    OraclePartitionBranch,
    OraclePartitionPlan,
)

plan = OraclePartitionPlan(
    parent="EVENTS",
    method="RANGE",
    columns="CREATED_AT",
    branches=(
        OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
        OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
    ),
)
```

A plan has exactly one of:

- `policy=` a generated first-level strategy
  (`OracleRangePolicy`, `OracleListPolicy`, `OracleHashPolicy`), or
- `branches=` explicit first-level partitions,

and at most one `subpartition=` table-level strategy
(`OracleSubpartitionPolicy`). The schema is optional (`schema="APP"`); the
name pattern defaults to `"{parent}_{suffix}"`.

The dialect answers *how* each lifecycle operation is spelled through the
eight backend-suggestion methods. Each returns a frozen suggestion whose
`operations` is an ordered tuple of real DDL expression nodes, `False` when
Oracle has no mechanism for the strategy, or `None` when nothing needs doing:

| Method | Oracle answer |
|---|---|
| `suggest_partition_build` | `PARTITION BY RANGE/LIST/HASH`, or `INTERVAL` / `REFERENCE` clause for CREATE TABLE |
| `suggest_partition_table_drop` | `DROP TABLE` for the whole table (not one partition) |
| `suggest_partition_table_alter` | `COALESCE PARTITION` for HASH — the only in-place shrink Oracle has, one partition per statement and with no target count; `False` for every other strategy or a count-qualified plan |
| `suggest_partition_add` | `ALTER TABLE ... ADD PARTITION <name>` per definition; no `IF NOT EXISTS` |
| `suggest_partition_drop` | `ALTER TABLE ... DROP PARTITION <name>` (drops its subpartitions too) |
| `suggest_partition_split` | `ALTER TABLE ... SPLIT PARTITION ... AT (...) INTO (...)` |
| `suggest_partition_merge` | `ALTER TABLE ... MERGE PARTITIONS p1, p2 INTO p3` |
| `suggest_partition_retention` | `DROP PARTITION` for branches whose bound is at or before the cutoff; `MAXVALUE` is never a candidate |

Adding and dropping are by partition **name**; idempotency comes from reading
the data dictionary first, because Oracle has no `ADD PARTITION IF NOT EXISTS`.
Retention compares each branch's upper bound (the `HIGH_VALUE` analog): a
`MAXVALUE` terminator has no finite end and is never selected. The plan layer
(`OracleRangePolicy(maxvalue=True)`) and the parser (`parse_high_value`) supply
the values those comparisons use.

Oracle commits each `ALTER TABLE` implicitly, so there is no transactional
"ensure" executor in the backend. Callers apply the suggested expression chain
one statement at a time and decide their own failure handling.

## Boundaries and labels

- Generated RANGE policies use half-open `[start, end)` intervals and render
  the upper bound as `VALUES LESS THAN (end)`.
- Supported interval spellings: `day`, `week`, `month`, `quarter`, `year`,
  `"<amount> <unit>"` (for example `"15 day"`), and `(amount, unit)`.
- The default label is `YYYY_MM` for months, `YYYY_qN` for quarters, `YYYY`
  for years, and `YYYY_MM_DD` for day/week granularity.
- `OracleRangePolicy(maxvalue=True)` appends a `MAXVALUE` terminator. Leave it
  off when the application intends to keep appending partitions; otherwise
  additions above the terminator require `SPLIT`.

## Scope limits

- No per-branch subpartition strategy is modelled, because Oracle has none.
- No scheduler, daemon or CLI is added; the application owns scheduling and
  decides how to apply the suggested DDL.
- `attach` / `detach` are not applicable to Oracle; use `EXCHANGE PARTITION`.
- REFERENCES are gated on Oracle version; `SYSTEM` partitioning is out of scope.
