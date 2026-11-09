# Partitioning and Partition Maintenance

Oracle partitioning is owned by the **backend**, not by an ActiveRecord model.
The framework plan (`2026-11-02/partitioned-model-support.md`) puts the line
here:

| Layer | Responsibility |
|---|---|
| ActiveRecord model | the composite primary-key shape a partitioned table needs, including every partitioning column |
| Oracle backend | partition DDL expressions and partition maintenance |
| Application | table names, partition columns, retention policy and scheduling |

Nothing in this package hard-codes an application's table name or retention
default.

## DDL expressions

Create the parent with `CreateTableStatement(partition=...)`:

```python
from datetime import date

from rhosocial.activerecord.backend.expression import Column, CreateTableStatement
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionValue,
    OracleSubpartitionClause,
    OracleSubpartitionStrategy,
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
        OraclePartitionDefinition(
            name="P1",
            less_than=[OraclePartitionValue(dialect, date(2026, 2, 1))],
            subpartitions_count=3,
        ),
        OraclePartitionDefinition(name="P2", less_than=[OraclePartitionValue(dialect, date(2026, 3, 1))]),
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

- `OraclePartitionDefinition(subpartitions_count=N)` renders a per-partition
  `SUBPARTITIONS N` override for `SUBPARTITION BY HASH`.
- `OraclePartitionDefinition(subpartition_definitions=[...])` renders explicit
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

`OraclePartitionDefinition` refuses `subpartitions_count` together with
`subpartition_definitions`, and refuses a non-positive or non-integer count.
The plan layer refuses a count override when the table strategy is RANGE or
LIST, and refuses subpartition definitions whose bound kind does not match the
table strategy.

## Maintenance: `OraclePartitionPlan`

The backend's `partition` package describes a tree by policy rather than by a
list of hand-written names:

```python
from datetime import date

from rhosocial.activerecord.backend.impl.oracle.partition import (
    OraclePartitionBranch,
    OraclePartitionMaintenance,
    OraclePartitionPlan,
    OracleSubpartitionPolicy,
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
maintenance = OraclePartitionMaintenance(backend)
report = maintenance.ensure(plan)
```

A plan has exactly one of:

- `policy=` a generated first-level strategy
  (`OracleRangePolicy`, `OracleListPolicy`, `OracleHashPolicy`), or
- `branches=` explicit first-level partitions.

and at most one `subpartition=` table-level strategy
(`OracleSubpartitionPolicy`). The schema is optional (`schema="APP"`); the
name pattern defaults to `"{parent}_{suffix}"`.

### Operations

| Call | What it does |
|---|---|
| `ensure(plan, through=None)` | Reads `ALL_TAB_PARTITIONS`, then issues `ALTER TABLE ... ADD PARTITION` only for missing partitions. Oracle has no `ADD PARTITION IF NOT EXISTS`, so idempotency comes from the read. |
| `list(plan, through=None)` | Returns `OraclePartitionReport`: the dictionary tree (root, partitions, subpartitions), `missing`, `orphans`, `issues` and `errors`. No DDL. |
| `validate(plan, through=None)` | `list` with the focus on `issues`: strategy mismatch, subpartition-count mismatch, gaps, overlaps, missing parent. It never repairs. |
| `drop_before(plan, cutoff, dry_run=True)` | Drops top-level partitions whose **whole** range ends at or before `cutoff`. Default is a dry run. `MAXVALUE` is never a candidate. |
| `drop_partition(plan, name, dry_run=False)` | Explicitly drops one named top-level partition and its subpartitions. |

`AsyncOraclePartitionMaintenance` exposes the same five methods as coroutines.

`list` and `validate` compare every existing top-level partition's
`HIGH_VALUE` with the plan: a RANGE `less_than` bound, a LIST value set, or a
`MAXVALUE` terminator. A name that exists with a different bound is reported
as `boundary mismatch`; a bound the conservative parser cannot recognize is
reported as `unparsed bound` rather than passing silently. `ensure` therefore
does not treat a boundary-mismatched partition as present. Each
`OraclePartitionInfo` keeps the raw text on `bound` / `high_value` and stores
`range_start`, `range_end` or the LIST `values` in `extras` when parsing
succeeds.

### Reports and Oracle's non-transactional DDL

Oracle commits each `ALTER TABLE` implicitly. The executor therefore does not
pretend a group of statements is atomic:

- operations are executed one statement at a time;
- `report.operations` lists the statements that succeeded (or would run, on a
  dry run);
- `report.errors` records each failure with Oracle's message;
- the post-operation `list` shows what actually exists.

Two Oracle limitations are reported rather than hidden:

- **`ADD` above `MAXVALUE`.** Oracle cannot add a partition whose bound is
  below the existing `MAXVALUE` partition; it raises `ORA-14074`. `validate`
  states that the partition needs `SPLIT PARTITION` first, and `ensure`
  records the server error.
- **Dropping the only partition.** Oracle raises `ORA-14083` when a `DROP
  PARTITION` would leave a partitioned table with no partitions. `drop_before`
  records it; callers should keep at least one partition or plan a
  reorganisation.

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
- No scheduler, daemon or CLI is added; the application calls `ensure` on its
  write path and `drop_before` from its maintenance job.
- `attach` / `detach` are not applicable to Oracle; use `EXCHANGE PARTITION`.
- The plan does not create the parent table. It reconciles partitions below an
  already existing partitioned table, which keeps table DDL and table policy
  separate.
