# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_partition_real.py
"""Real Oracle scenario tests for Phase 4 generic partitioning.

Builds partitioned tables through Expression/Dialect (no raw SQL string
concatenation), introspects ``ALL_TAB_PARTITIONS`` to verify the partition
structure, and runs EXPLAIN to confirm the optimizer recognizes the
partitioned table.

Requires a live Oracle 11g+ instance via the ``oracle_backend`` fixture.
"""

import pytest

from rhosocial.activerecord.backend.dialect.protocols import PartitionSupport
from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    CreateTableExpression,
    DropTableExpression,
    Literal,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression.statements import (
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.expression.types import (
    IntegerType,
    VarCharType,
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
    """Drop ``name`` if it exists. Oracle has no DROP TABLE IF EXISTS, so
    we attempt the drop and ignore the ORA-00942 'table or view does not
    exist' error."""
    expr = DropTableExpression(backend.dialect, name)
    sql, params = expr.to_sql()
    try:
        backend.execute(sql, params)
    except DatabaseError as exc:
        if "ORA-00942" not in str(exc):
            raise


def _exec_query(backend, sql, params=()):
    """Execute a SELECT and return the data rows."""
    return backend.execute(
        sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL)
    ).data or []


def _count_partitions(backend, table_name: str) -> int:
    """Count partitions of ``table_name`` via ALL_TAB_PARTITIONS."""
    d = backend.dialect
    query = QueryExpression(
        d,
        select=[Column(d, "PARTITION_NAME")],
        from_=TableExpression(d, "ALL_TAB_PARTITIONS"),
        where=Column(d, "TABLE_NAME") == Literal(d, table_name.upper()),
    )
    sql, params = query.to_sql()
    return len(_exec_query(backend, sql, params))


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


# ---------------------------------------------------------------------------
# Real scenario tests
# ---------------------------------------------------------------------------

@pytest.mark.backend
@requires_protocol(PartitionSupport, "supports_table_partitioning")
@requires_protocol(PartitionSupport, "supports_range_table_partitioning")
def test_create_range_partitioned_table_real(oracle_backend_single):
    """Build a RANGE partitioned table and verify partitions via introspection."""
    backend = oracle_backend_single
    if not backend.dialect.supports_table_partitioning():
        pytest.skip("Oracle backend does not support table partitioning")

    table_name = "PHASE4_RANGE_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = PartitionClause(
            d, PartitionStrategy.RANGE, [Column(d, "AGE")],
            dialect_options={
                "partitions": [
                    {"name": "p_minor", "less_than": [Literal(d, 18)]},
                    {"name": "p_adult", "less_than": [Literal(d, 65)]},
                    {"name": "p_senior", "less_than": ["MAXVALUE"]},
                ],
            },
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("AGE", IntegerType()),
                ColumnDefinition("NAME", VarCharType(length=100)),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        assert "PARTITION BY RANGE" in sql
        backend.execute(sql, params)

        assert _count_partitions(backend, table_name) == 3
        names = _partition_names(backend, table_name)
        assert names == {"P_MINOR", "P_ADULT", "P_SENIOR"}
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(PartitionSupport, "supports_table_partitioning")
@requires_protocol(PartitionSupport, "supports_list_table_partitioning")
def test_create_list_partitioned_table_real(oracle_backend_single):
    """Build a LIST partitioned table and verify partitions via introspection."""
    backend = oracle_backend_single
    if not backend.dialect.supports_table_partitioning():
        pytest.skip("Oracle backend does not support table partitioning")

    table_name = "PHASE4_LIST_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = PartitionClause(
            d, PartitionStrategy.LIST, [Column(d, "REGION")],
            dialect_options={
                "partitions": [
                    {"name": "p_east", "in_values": [Literal(d, "EAST")]},
                    {"name": "p_west", "in_values": [Literal(d, "WEST")]},
                ],
            },
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("REGION", VarCharType(length=20)),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        assert "PARTITION BY LIST" in sql
        backend.execute(sql, params)

        assert _count_partitions(backend, table_name) == 2
        names = _partition_names(backend, table_name)
        assert names == {"P_EAST", "P_WEST"}
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(PartitionSupport, "supports_table_partitioning")
@requires_protocol(PartitionSupport, "supports_hash_table_partitioning")
def test_create_hash_partitioned_table_real(oracle_backend_single):
    """Build a HASH partitioned table and verify partition count via introspection."""
    backend = oracle_backend_single
    if not backend.dialect.supports_table_partitioning():
        pytest.skip("Oracle backend does not support table partitioning")

    table_name = "PHASE4_HASH_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = PartitionClause(
            d, PartitionStrategy.HASH, [Column(d, "ID")],
            dialect_options={"partitions_count": 4},
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("NAME", VarCharType(length=100)),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        assert "PARTITION BY HASH" in sql
        assert "PARTITIONS 4" in sql
        backend.execute(sql, params)

        assert _count_partitions(backend, table_name) == 4
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
@requires_protocol(PartitionSupport, "supports_table_partitioning")
@requires_protocol(PartitionSupport, "supports_range_table_partitioning")
def test_explain_range_partitioned_table_real(oracle_backend_single):
    """EXPLAIN a query against a RANGE partitioned table to confirm the
    optimizer accepts the DDL and can parse the statement."""
    backend = oracle_backend_single
    if not backend.dialect.supports_table_partitioning():
        pytest.skip("Oracle backend does not support table partitioning")

    table_name = "PHASE4_EXPLAIN_PART"
    _drop(backend, table_name)
    try:
        d = backend.dialect
        partition = PartitionClause(
            d, PartitionStrategy.RANGE, [Column(d, "AGE")],
            dialect_options={
                "partitions": [
                    {"name": "p1", "less_than": [Literal(d, 100)]},
                    {"name": "p2", "less_than": ["MAXVALUE"]},
                ],
            },
        )
        expr = CreateTableExpression(
            dialect=d,
            table=table_name,
            columns=[
                ColumnDefinition("ID", IntegerType(), constraints=[
                    ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY),
                ]),
                ColumnDefinition("AGE", IntegerType()),
            ],
            partition=partition,
        )
        sql, params = expr.to_sql()
        backend.execute(sql, params)

        # Insert one row to make EXPLAIN meaningful.
        from rhosocial.activerecord.backend.expression import InsertExpression
        from rhosocial.activerecord.backend.expression.statements import ValuesSource
        insert = InsertExpression(
            d,
            into=table_name,
            source=ValuesSource(d, [[Literal(d, 1), Literal(d, 25)]]),
            columns=["ID", "AGE"],
        )
        isql, iparams = insert.to_sql()
        backend.execute(isql, iparams)

        # EXPLAIN PLAN FOR - Oracle stores the plan in PLAN_TABLE.
        explain_sql = f"EXPLAIN PLAN FOR SELECT * FROM {table_name} WHERE AGE = 25"
        backend.execute(explain_sql, ())

        # Verify a plan row exists for this statement.
        d2 = backend.dialect
        verify = QueryExpression(
            d2,
            select=[Column(d2, "OPERATION")],
            from_=TableExpression(d2, "PLAN_TABLE"),
            where=Column(d2, "OBJECT_NAME") == Literal(d2, table_name.upper()),
        )
        vsql, vparams = verify.to_sql()
        rows = _exec_query(backend, vsql, vparams)
        assert len(rows) >= 1
        # Clean PLAN_TABLE for this object.
        backend.execute(f"DELETE FROM PLAN_TABLE WHERE OBJECT_NAME = '{table_name.upper()}'", ())
    finally:
        _drop(backend, table_name)


@pytest.mark.backend
def test_oracle_dialect_satisfies_oracle_partition_support_real(oracle_backend_single):
    """The connected backend's dialect satisfies OraclePartitionSupport."""
    backend = oracle_backend_single
    assert isinstance(backend.dialect, OraclePartitionSupport)
    assert isinstance(backend.dialect, PartitionSupport)
