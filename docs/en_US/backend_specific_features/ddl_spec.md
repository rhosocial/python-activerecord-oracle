# DDL Feature Specs

Oracle implements the core DDL feature-spec claiming protocol
(`dialect.build_spec`). This chapter documents which Specs the Oracle dialect
claims, how it translates them, and the Oracle-specific Specs it adds.

## How claiming works

At `Model.generate_create_table(dialect)` time the generator hands each
declared Spec to `dialect.build_spec(spec)`:

- **Accepted** → the dialect builds and returns an expression-layer instance,
  which lands in the `CreateTableExpression`;
- **Not accepted** → returns `None`, and the Spec is silently ignored.

Acceptance scope is the Oracle dialect's own decision.

## Generic Specs

All generic Specs are claimed and translated by the core default:

| Spec | Oracle translation |
|------|--------------------|
| `CheckSpec` | `TableConstraint(CHECK)`, lazy predicates evaluated at build time |
| `UniqueSpec` | `TableConstraint(UNIQUE)` |
| `NotNullSpec` | `ColumnConstraint(NOT NULL)` |
| `PrimaryKeySpec` | column-level PK (single) / table-level composite PK |
| `DefaultSpec` | `ColumnConstraint(DEFAULT)` with a parameterized `Literal` |
| `ForeignKeySpec` | `ForeignKeyConstraint` with referential actions |
| `IndexSpec` / `PartialIndexSpec` | `IndexDefinition` |
| `JsonColumnSpec` | column type patch → `JsonType` |

## Oracle-specific Specs

Defined in `rhosocial.activerecord.backend.impl.oracle.ddl_spec`; claimed via
`isinstance` and translated by `OracleDDLSpecMixin`. Only the Oracle dialect
claims these.

### Partition Specs

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
        # Interval partitioning (11g+): Oracle auto-creates partitions
        OracleIntervalPartition.monthly("created_at"),
    ]
```

Translated to the Oracle partition expression layer and rendered as inline
DDL — Oracle DDL accepts no bind parameters, so boundary values are
safely-escaped inline literals. See [Partitioning](partition.md) for the full
strategy table and lifecycle management.

Interval expressions are built from `OracleIntervalFunctionExpression`
(`NUMTOYMINTERVAL` / `NUMTODSINTERVAL`); the `.monthly()` / `.yearly()` /
`.daily()` factories cover the common cases.

### Sequence Default

```python
from rhosocial.activerecord.backend.impl.oracle.ddl_spec import (
    OracleSequenceDefault,
)

class Users(ActiveRecord):
    __table_constraints__ = [
        OracleSequenceDefault(column="id", sequence="users_id_seq"),
    ]
```

Translates to `DEFAULT USERS_ID_SEQ.NEXTVAL` via the existing
`OracleSequenceValueExpression`. When `sequence` is omitted, `<column>_seq`
is derived.
