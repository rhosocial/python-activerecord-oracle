# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_partition_phase5_real.py
"""Real Oracle scenario tests for Phase 5 partition strategies.

Builds INTERVAL / REFERENCE / COMPOSITE partitioned tables and exercises
the partition maintenance statements (ADD / DROP / SPLIT / MERGE / EXCHANGE
/ MOVE / TRUNCATE PARTITION) through the backend-specific Expression /
Dialect layer (no raw SQL string concatenation), then introspects
``ALL_TAB_PARTITIONS`` / ``ALL_TAB_SUBPARTITIONS`` to verify the resulting
structure.

Requires a live Oracle 11g+ instance via the ``oracle_backend_single``
fixture. The configured scenarios (18c / 21c / 23c) all satisfy the
INTERVAL / REFERENCE / composite partitioning requirements.
"""

import pytest
from datetime import date

from rhosocial.activerecord.backend.dialect.protocols import PartitionSupport
from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    CreateTableExpression,
    DropTableExpression,
    InsertExpression,
    Literal,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
from rhosocial.activerecord.backend.expression.types import (
    DateType,
    IntegerType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleReferencePartitionClause,
    OracleSubpartitionClause,
    OracleSubpartitionStrategy,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleAddPartitionExpression,
    OracleDropPartitionExpression,
    OracleExchangePartitionExpression,
    OracleMergePartitionsExpression,
    OracleMovePartitionExpression,
    OracleSplitPartitionExpression,
    OracleTruncatePartitionExpression,
)
from rhosocial.activerecord.backend.impl.oracle.protocols.partition import (
    OraclePartitionSupport,
)
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
from rhosocial.activerecord.testsuite.utils.common import requires_protocol


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _drop(backend, name: str):
    """Drop ``name`` if it exists; ignore ORA-00942."""
    expr = DropTableExpression(backend.dialect, name)
    sql, params = expr.to_sql()
    try:
        backend.execute(sql, params)
    except DatabaseError as exc:
        if "ORA-00942" not in str(exc):
            raise


def _exec_query(backend, sql, params=()):
    return backend.execute(
        sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL)
    ).data or []


def _partition_names(backend, table_name: str):
    d = backend.dialect
    query = QueryExpression(
        d,
        select=[Column(d, "PARTITION_NAME")],
        from_=TableExpression(d, "ALL_TAB_PARTITIONS"),
        where=Column(d, "TABLE_NAME") == Literal(d, table_name.upper()),
    )
    sql, params = query.to_sql()
    return {row["partition_name"] for row in _exec_query(backend, sql, params)}


def _count_partitions(backend, table_name: str) -> int:
    return len(_partition_names(backend, table_name))


def _count_subpartitions(backend, table_name: str) -> int:
    """Count subpartitions of ``table_name`` via ALL_TAB_SUBPARTITIONS."""
    d = backend.dialect
    query = QueryExpression(
        d,
        select=[Column(d, "SUBPARTITION_NAME")],
        from_=TableExpression(d, "ALL_TAB_SUBPARTITIONS"),
        where=Column(d, "TABLE_NAME") == Literal(d, table_name.upper()),
    )
    sql, params = query.to_sql()
    return len(_exec_query(backend, sql, params))


def _insert_row(backend, table_name: str, columns: dict):
    d = backend.dialect
    from rhosocial.activerecord.backend.expression.statements import ValuesSource

    col_names = list(columns.keys())
    values = [Literal(d, v) for v in columns.values()]
    expr = InsertExpression(
        d,
        into=table_name,
        source=ValuesSource(d, [values]),
        columns=col_names,
    )
    sql, params = expr.to_sql()
    backend.execute(sql, params)


# ---------------------------------------------------------------------------
# INTERVAL (sub-phase B) — real scenario
# ---------------------------------------------------------------------------


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_interval_partitioning")
def test_interval_partition_auto_creates_partition_real(oracle_backend_single):
    """INTERVAL partitioning auto-creates a partition when data exceeds the
    declared seed boundary."""
    backend = oracle_backend_single
    table_name = "PHASE5_INTERVAL_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = OracleIntervalPartitionClause(
            d, [Column(d, "CREATED_AT")],
            interval=RawSQLExpression(d, "NUMTOYMINTERVAL(1, 'MONTH')"),
            partitions=[
                OraclePartitionDefinition(
                    name="p0",
                    less_than=[OraclePartitionValue(d, date(2026, 1, 1))],
                ),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("CREATED_AT", DateType()),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        assert "INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))" in sql
        backend.execute(sql, params)

        # Initially only the seed partition exists.
        assert _count_partitions(backend, table_name) == 1

        # Insert a row beyond the seed boundary -> Oracle auto-creates a
        # new interval partition. Use a Python date object so the driver
        # binds it as a DATE, not a string.
        _insert_row(backend, table_name, {"ID": 1, "CREATED_AT": date(2026, 6, 15)})
        # The seed partition plus at least one auto-created partition.
        assert _count_partitions(backend, table_name) >= 2
    finally:
        _drop(backend, table_name)


# ---------------------------------------------------------------------------
# REFERENCE (sub-phase C) — real scenario
# ---------------------------------------------------------------------------


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_reference_partitioning")
def test_reference_partitioning_inherits_parent_partitions_real(oracle_backend_single):
    """A child table with REFERENCE partitioning inherits the parent's
    partition count via the foreign key constraint."""
    backend = oracle_backend_single
    parent_name = "PHASE5_REF_PARENT"
    child_name = "PHASE5_REF_CHILD"
    _drop(backend, child_name)
    _drop(backend, parent_name)
    try:
        d = backend.dialect

        # Parent: RANGE partitioned with 2 partitions.
        parent_partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        parent_expr = CreateTableExpression(
            dialect=d,
            table=parent_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
            partition=parent_partition,
        )
        sql, params = parent_expr.to_sql()
        backend.execute(sql, params)
        assert _count_partitions(backend, parent_name) == 2

        # Child: REFERENCE partitioning via a FOREIGN KEY constraint.
        # The FK constraint must be created inline as a table constraint.
        from rhosocial.activerecord.backend.expression.statements import TableConstraint

        fk_constraint = TableConstraint(
            name="fk_child_parent",
            constraint_type="FOREIGN_KEY",
            columns=["PARENT_ID"],
            foreign_key_table=parent_name,
            foreign_key_columns=["ID"],
        )
        child_partition = OracleReferencePartitionClause(d, "fk_child_parent")
        child_expr = CreateTableExpression(
            dialect=d,
            table=child_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("PARENT_ID", IntegerType()),
            ],
            table_constraints=[fk_constraint],
            partition=child_partition,
        )
        sql, params = child_expr.to_sql()
        assert "PARTITION BY REFERENCE" in sql
        backend.execute(sql, params)

        # Child inherits the parent's 2 partitions.
        assert _count_partitions(backend, child_name) == 2
    finally:
        _drop(backend, child_name)
        _drop(backend, parent_name)


# ---------------------------------------------------------------------------
# COMPOSITE (sub-phase D) — real scenario
# ---------------------------------------------------------------------------


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_subpartitioning")
def test_composite_range_hash_partitioning_real(oracle_backend_single):
    """Build a RANGE-HASH composite partitioned table and verify both
    partitions and subpartitions exist via introspection."""
    backend = oracle_backend_single
    table_name = "PHASE5_COMPOSITE_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        subpartition = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.HASH,
            keys=[Column(d, "ID")], count=2,
        )
        partition = OraclePartitionByRange(
            d, [Column(d, "CREATED_AT")],
            subpartition_by=subpartition,
            partitions=[
                OraclePartitionDefinition(
                    name="p1",
                    less_than=[OraclePartitionValue(d, date(2026, 1, 1))],
                ),
                OraclePartitionDefinition(
                    name="p2",
                    less_than=[OraclePartitionMaxValue(d)],
                ),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("CREATED_AT", DateType()),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        assert "SUBPARTITION BY HASH" in sql
        assert "SUBPARTITIONS 2" in sql
        backend.execute(sql, params)

        # 2 top-level partitions.
        assert _count_partitions(backend, table_name) == 2
        # 2 partitions * 2 subpartitions = 4 subpartitions.
        assert _count_subpartitions(backend, table_name) == 4
    finally:
        _drop(backend, table_name)


# ---------------------------------------------------------------------------
# Maintenance statements (sub-phase E) — real scenario
# ---------------------------------------------------------------------------


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_add_partition")
@requires_protocol(OraclePartitionSupport, "supports_drop_partition")
def test_add_and_drop_partition_real(oracle_backend_single):
    """ADD PARTITION then DROP PARTITION; verify via introspection."""
    backend = oracle_backend_single
    table_name = "PHASE5_ADDDROP_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        # Start with a single RANGE partition.
        partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("NAME", VarCharType(100)),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)
        assert _count_partitions(backend, table_name) == 1

        # ADD a new partition p2 with boundary 200.
        add_expr = OracleAddPartitionExpression(
            d, table_name,
            OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
        )
        sql, params = add_expr.to_sql()
        backend.execute(sql, params)
        assert _partition_names(backend, table_name) == {"P1", "P2"}

        # DROP p1.
        drop_expr = OracleDropPartitionExpression(d, table_name, "p1")
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)
        assert _partition_names(backend, table_name) == {"P2"}
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_split_partition")
def test_split_partition_real(oracle_backend_single):
    """SPLIT PARTITION: split the MAXVALUE partition into two."""
    backend = oracle_backend_single
    table_name = "PHASE5_SPLIT_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        # Start with p1 (<100) and p_max (MAXVALUE).
        partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p_max", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)
        assert _partition_names(backend, table_name) == {"P1", "P_MAX"}

        # SPLIT p_max at 200 into p2 (<200) and p_max (MAXVALUE).
        split_expr = OracleSplitPartitionExpression(
            d, table_name, "p_max",
            at_values=[OraclePartitionValue(d, 200)],
            new_partitions=[
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
                OraclePartitionDefinition(name="p_max", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        sql, params = split_expr.to_sql()
        backend.execute(sql, params)
        assert _partition_names(backend, table_name) == {"P1", "P2", "P_MAX"}
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_merge_partition")
def test_merge_partitions_real(oracle_backend_single):
    """MERGE PARTITIONS: merge two adjacent RANGE partitions into one."""
    backend = oracle_backend_single
    table_name = "PHASE5_MERGE_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
                OraclePartitionDefinition(name="p3", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)
        assert _count_partitions(backend, table_name) == 3

        # MERGE p1 and p2 into p12 (<200).
        merge_expr = OracleMergePartitionsExpression(
            d, table_name, ["p1", "p2"],
            OraclePartitionDefinition(name="p12", less_than=[OraclePartitionValue(d, 200)]),
        )
        sql, params = merge_expr.to_sql()
        backend.execute(sql, params)
        assert _partition_names(backend, table_name) == {"P12", "P3"}
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_truncate_partition")
def test_truncate_partition_real(oracle_backend_single):
    """TRUNCATE PARTITION: insert rows, truncate a partition, verify rows removed."""
    backend = oracle_backend_single
    table_name = "PHASE5_TRUNC_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)

        # Insert 2 rows (one in p1, one in p2).
        _insert_row(backend, table_name, {"ID": 1})
        _insert_row(backend, table_name, {"ID": 150})

        # Count total rows.
        count_query = QueryExpression(
            d, select=[Column(d, "ID")], from_=TableExpression(d, table_name),
        )
        sql, params = count_query.to_sql()
        assert len(_exec_query(backend, sql, params)) == 2

        # TRUNCATE p1 (contains ID=1).
        trunc_expr = OracleTruncatePartitionExpression(d, table_name, "p1")
        sql, params = trunc_expr.to_sql()
        backend.execute(sql, params)

        # Only ID=150 remains.
        sql, params = count_query.to_sql()
        rows = _exec_query(backend, sql, params)
        assert len(rows) == 1
        assert rows[0]["id"] == 150
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_move_partition")
def test_move_partition_real(oracle_backend_single):
    """MOVE PARTITION: move a partition's segment (no data change)."""
    backend = oracle_backend_single
    table_name = "PHASE5_MOVE_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)

        # MOVE p1. No assertion on tablespace; we just verify it executes.
        move_expr = OracleMovePartitionExpression(d, table_name, "p1")
        sql, params = move_expr.to_sql()
        backend.execute(sql, params)

        # Partitions still present after move.
        assert _partition_names(backend, table_name) == {"P1", "P2"}
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(OraclePartitionSupport, "supports_exchange_partition")
def test_exchange_partition_real(oracle_backend_single):
    """EXCHANGE PARTITION: swap a partition with a standalone staging table."""
    backend = oracle_backend_single
    table_name = "PHASE5_EXCH_PART"
    staging_name = "PHASE5_EXCH_STAGE"
    _drop(backend, staging_name)
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = OraclePartitionByRange(
            d, [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)

        # Build a non-partitioned staging table with the same shape.
        staging_expr = CreateTableExpression(
            dialect=d,
            table=staging_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
            ],
        )
        sql, params = staging_expr.to_sql()
        backend.execute(sql, params)

        # EXCHANGE p1 with the staging table (WITHOUT VALIDATION for speed).
        exch_expr = OracleExchangePartitionExpression(
            d, table_name, "p1", staging_name, with_validation=False,
        )
        sql, params = exch_expr.to_sql()
        backend.execute(sql, params)

        # The partition and staging table still exist after exchange.
        assert _partition_names(backend, table_name) == {"P1", "P2"}
    finally:
        _drop(backend, staging_name)
        _drop(backend, table_name)
