# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/partition/test_partition_maintenance.py
"""Live Oracle scenarios for composite DDL and partition maintenance.

Covers the Oracle half of the partition plan (see
``.claude/plan/2026-11-08/partition-ddl-maintenance.md`` §4/§5):

* composite RANGE-HASH with a per-partition ``SUBPARTITIONS n`` override,
* composite RANGE-LIST with different explicit subpartition counts per
  partition (strategy stays table level),
* ``ensure`` / ``list`` / ``validate`` / ``drop_before`` / ``drop_partition``
  through :class:`OraclePartitionMaintenance`,
* honest reporting when an existing ``MAXVALUE`` partition blocks ``ADD``,
* the async executor mirroring the sync surface.

Requires a live Oracle 11g+ instance. The local ``oracle-free:23-slim``
container is the scenario these tests were written against; 18c/21c remain
untested until their images are available.
"""

from datetime import date

import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnConstraint,
    ColumnDefinition,
    ColumnPrimaryKeyConstraint,
    ComparisonPredicate,
    CreateTableStatement,
    DropTableStatement,
    Literal,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.types import (
    DateType,
    IntegerType,
    VarCharType,
)
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleSubpartitionClause,
    OracleSubpartitionDefinition,
    OracleSubpartitionStrategy,
)
from rhosocial.activerecord.backend.impl.oracle.partition import (
    AsyncOraclePartitionMaintenance,
    OraclePartitionBranch,
    OraclePartitionMaintenance,
    OraclePartitionPlan,
    OracleSubpartitionPolicy,
    OracleSubpartitionSpec,
)
from rhosocial.activerecord.backend.impl.oracle.protocols.partition import (
    OraclePartitionSupport,
)
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
from rhosocial.activerecord.testsuite.utils.common import requires_protocol


_DDL = ExecutionOptions(stmt_type=StatementType.DDL)
_DQL = ExecutionOptions(stmt_type=StatementType.DQL)


def _drop(backend, name):
    """Drop ``name`` if it exists; ignore ORA-00942."""
    expression = DropTableStatement(backend.dialect, Table(backend.dialect, name))
    sql, params = expression.to_sql()
    try:
        backend.execute(sql, params, options=_DDL)
    except DatabaseError as exc:
        if "ORA-00942" not in str(exc):
            raise


async def _async_drop(backend, name):
    """Asynchronously drop ``name`` if it exists; ignore ORA-00942."""
    expression = DropTableStatement(backend.dialect, Table(backend.dialect, name))
    sql, params = expression.to_sql()
    try:
        await backend.execute(sql, params)
    except DatabaseError as exc:
        if "ORA-00942" not in str(exc):
            raise


def _columns(dialect, with_region=False):
    columns = [
        ColumnDefinition(
            dialect,
            "ID",
            IntegerType(dialect),
            constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))
            ],
        ),
        ColumnDefinition(dialect, "CREATED_AT", DateType(dialect)),
    ]
    if with_region:
        columns.append(ColumnDefinition(dialect, "REGION", VarCharType(length=10, dialect=dialect)))
    return columns


def _table_where(dialect, table_name):
    return ComparisonPredicate(
        dialect,
        "eq",
        Column(dialect, "TABLE_NAME"),
        Literal(dialect, table_name.upper()),
    )


def _partition_names(backend, table_name):
    d = backend.dialect
    query = QueryStatement(
        d,
        select=[Column(d, "PARTITION_NAME")],
        from_=Table(d, "ALL_TAB_PARTITIONS"),
        where=_table_where(d, table_name),
    )
    sql, params = query.to_sql()
    rows = backend.execute(sql, params, options=_DQL).data or []
    return {row["partition_name"] for row in rows}


def _subpartition_counts(backend, table_name):
    d = backend.dialect
    query = QueryStatement(
        d,
        select=[Column(d, "PARTITION_NAME"), Column(d, "SUBPARTITION_COUNT")],
        from_=Table(d, "ALL_TAB_PARTITIONS"),
        where=_table_where(d, table_name),
    )
    sql, params = query.to_sql()
    rows = backend.execute(sql, params, options=_DQL).data or []
    return {row["partition_name"]: row["subpartition_count"] for row in rows}


def _create_range_hash(backend, name, count_overrides):
    d = backend.dialect
    definitions = []
    for partition_name, boundary, count in count_overrides:
        definitions.append(
            OraclePartitionDefinition(
                name=partition_name,
                less_than=[OraclePartitionValue(d, boundary)],
                subpartitions_count=count,
            )
        )
    clause = OraclePartitionByRange(
        d,
        [Column(d, "CREATED_AT")],
        subpartition_by=OracleSubpartitionClause(
            d,
            OracleSubpartitionStrategy.HASH,
            keys=[Column(d, "ID")],
            count=2,
        ),
        partitions=definitions,
    )
    expression = CreateTableStatement(
        dialect=d,
        table=Table(d, name),
        columns=_columns(d),
        partition=clause,
    )
    backend.execute(*expression.to_sql(), options=_DDL)


class TestCompositeDdlLive:
    """Composite DDL behaviour on a live server."""

    @pytest.mark.backend
    @requires_protocol(OraclePartitionSupport, "supports_subpartitioning")
    def test_range_hash_per_partition_count(self, oracle_backend_single):
        backend = oracle_backend_single
        table = "PM_COMP_HASH"
        _drop(backend, table)
        try:
            _create_range_hash(
                backend,
                table,
                (
                    ("P1", date(2026, 2, 1), 3),
                    ("P2", date(2026, 3, 1), None),
                ),
            )
            counts = _subpartition_counts(backend, table)
            assert counts["P1"] == 3
            assert counts["P2"] == 2

            plan = OraclePartitionPlan(
                parent=table,
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(
                        name="P1",
                        less_than=(date(2026, 2, 1),),
                        subpartitions_count=3,
                    ),
                    OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                ),
                subpartition=OracleSubpartitionPolicy(
                    method="HASH", columns="ID", count=2
                ),
            )
            report = OraclePartitionMaintenance(backend).list(plan)
            assert report.missing == ()
            assert report.issues == ()
            assert report.errors == ()
        finally:
            _drop(backend, table)

    @pytest.mark.backend
    @requires_protocol(OraclePartitionSupport, "supports_subpartitioning")
    def test_range_list_per_partition_definitions(self, oracle_backend_single):
        backend = oracle_backend_single
        table = "PM_COMP_LIST"
        _drop(backend, table)
        try:
            d = backend.dialect
            clause = OraclePartitionByRange(
                d,
                [Column(d, "CREATED_AT")],
                subpartition_by=OracleSubpartitionClause(
                    d,
                    OracleSubpartitionStrategy.LIST,
                    keys=[Column(d, "REGION")],
                ),
                partitions=[
                    OraclePartitionDefinition(
                        name="P1",
                        less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                        subpartition_definitions=[
                            OracleSubpartitionDefinition(
                                name="P1_E",
                                in_values=[OraclePartitionValue(d, "EAST")],
                            ),
                            OracleSubpartitionDefinition(
                                name="P1_W",
                                in_values=[OraclePartitionValue(d, "WEST")],
                            ),
                        ],
                    ),
                    OraclePartitionDefinition(
                        name="P2",
                        less_than=[OraclePartitionValue(d, date(2026, 3, 1))],
                        subpartition_definitions=[
                            OracleSubpartitionDefinition(
                                name="P2_E",
                                in_values=[OraclePartitionValue(d, "EAST")],
                            ),
                        ],
                    ),
                ],
            )
            expression = CreateTableStatement(
                dialect=d,
                table=Table(d, table),
                columns=_columns(d, with_region=True),
                partition=clause,
            )
            backend.execute(*expression.to_sql(), options=_DDL)

            counts = _subpartition_counts(backend, table)
            assert counts["P1"] == 2
            assert counts["P2"] == 1

            plan = OraclePartitionPlan(
                parent=table,
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(
                        name="P1",
                        less_than=(date(2026, 2, 1),),
                        subpartitions=(
                            OracleSubpartitionSpec(name="P1_E", in_values=("EAST",)),
                            OracleSubpartitionSpec(name="P1_W", in_values=("WEST",)),
                        ),
                    ),
                    OraclePartitionBranch(
                        name="P2",
                        less_than=(date(2026, 3, 1),),
                        subpartitions=(
                            OracleSubpartitionSpec(name="P2_E", in_values=("EAST",)),
                        ),
                    ),
                ),
                subpartition=OracleSubpartitionPolicy(
                    method="LIST", columns="REGION"
                ),
            )
            report = OraclePartitionMaintenance(backend).list(plan)
            assert report.issues == ()
            assert report.errors == ()
        finally:
            _drop(backend, table)


    @pytest.mark.backend
    @requires_protocol(OraclePartitionSupport, "supports_subpartitioning")
    def test_range_list_subpartition_template(self, oracle_backend_single):
        """``SUBPARTITION TEMPLATE`` is inherited by every top-level partition."""
        backend = oracle_backend_single
        table = "PM_COMP_TEMPLATE"
        _drop(backend, table)
        try:
            d = backend.dialect
            clause = OraclePartitionByRange(
                d,
                [Column(d, "CREATED_AT")],
                subpartition_by=OracleSubpartitionClause(
                    d,
                    OracleSubpartitionStrategy.LIST,
                    keys=[Column(d, "REGION")],
                    templates=[
                        OracleSubpartitionDefinition(
                            name="SP_E",
                            in_values=[OraclePartitionValue(d, "EAST")],
                        ),
                        OracleSubpartitionDefinition(
                            name="SP_W",
                            in_values=[OraclePartitionValue(d, "WEST")],
                        ),
                    ],
                ),
                partitions=[
                    OraclePartitionDefinition(
                        name="P1",
                        less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                    ),
                    OraclePartitionDefinition(
                        name="P2",
                        less_than=[OraclePartitionValue(d, date(2026, 3, 1))],
                    ),
                ],
            )
            expression = CreateTableStatement(
                dialect=d,
                table=Table(d, table),
                columns=_columns(d, with_region=True),
                partition=clause,
            )
            sql, params = expression.to_sql()
            assert "SUBPARTITION TEMPLATE" in sql
            backend.execute(sql, params, options=_DDL)

            counts = _subpartition_counts(backend, table)
            assert set(counts.values()) == {2}
        finally:
            _drop(backend, table)


class TestMaintenanceLive:
    """ensure / list / validate / drop_before / drop_partition on a live server."""

    @pytest.mark.backend
    @requires_protocol(OraclePartitionSupport, "supports_add_partition")
    @requires_protocol(OraclePartitionSupport, "supports_drop_partition")
    def test_ensure_list_validate_drop(self, oracle_backend_single):
        backend = oracle_backend_single
        table = "PM_RANGE_MNT"
        _drop(backend, table)
        try:
            d = backend.dialect
            clause = OraclePartitionByRange(
                d,
                [Column(d, "CREATED_AT")],
                partitions=[
                    OraclePartitionDefinition(
                        name="P1",
                        less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                    ),
                ],
            )
            expression = CreateTableStatement(
                dialect=d,
                table=Table(d, table),
                columns=_columns(d),
                partition=clause,
            )
            backend.execute(*expression.to_sql(), options=_DDL)

            plan = OraclePartitionPlan(
                parent=table,
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                    OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                    OraclePartitionBranch(name="P3", less_than=(date(2026, 4, 1),)),
                ),
            )
            maintenance = OraclePartitionMaintenance(backend)

            before = maintenance.list(plan)
            assert before.missing == (("PM_RANGE_MNT", "P2"), ("PM_RANGE_MNT", "P3"))
            assert before.errors == ()

            ensured = maintenance.ensure(plan)
            assert ensured.errors == ()
            assert [operation.name for operation in ensured.operations] == ["P2", "P3"]
            assert _partition_names(backend, table) == {"P1", "P2", "P3"}
            assert ensured.missing == ()
            assert ensured.issues == ()

            # An extra partition the plan does not describe is an orphan.
            add_extra = OraclePartitionDefinition(
                name="PX", less_than=[OraclePartitionValue(d, date(2026, 6, 1))]
            )
            from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
                OracleAddPartitionStatement,
            )

            add_sql, add_params = OracleAddPartitionStatement(
                d, Table(d, table), add_extra
            ).to_sql()
            backend.execute(add_sql, add_params, options=_DDL)
            assert "PX" in (maintenance.list(plan).orphans)

            dry = maintenance.drop_before(plan, date(2026, 3, 1))
            assert [operation.name for operation in dry.operations] == ["P1", "P2"]
            assert dry.dry_run is True
            assert _partition_names(backend, table) == {"P1", "P2", "P3", "PX"}

            dropped = maintenance.drop_before(plan, date(2026, 3, 1), dry_run=False)
            assert dropped.errors == ()
            assert [operation.name for operation in dropped.operations] == ["P1", "P2"]
            assert _partition_names(backend, table) == {"P3", "PX"}

            # Dropping the only remaining planned partition is allowed while a
            # physical partition (PX) still exists.
            final = maintenance.drop_partition(plan, "PX")
            assert final.errors == ()
            assert _partition_names(backend, table) == {"P3"}
        finally:
            _drop(backend, table)

    @pytest.mark.backend
    @requires_protocol(OraclePartitionSupport, "supports_add_partition")
    def test_maxvalue_blocks_add_is_reported(self, oracle_backend_single):
        backend = oracle_backend_single
        table = "PM_MAXVALUE"
        _drop(backend, table)
        try:
            d = backend.dialect
            clause = OraclePartitionByRange(
                d,
                [Column(d, "CREATED_AT")],
                partitions=[
                    OraclePartitionDefinition(
                        name="P1",
                        less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                    ),
                    OraclePartitionDefinition(
                        name="P_MAX",
                        less_than=[OraclePartitionMaxValue(d)],
                    ),
                ],
            )
            expression = CreateTableStatement(
                dialect=d,
                table=Table(d, table),
                columns=_columns(d),
                partition=clause,
            )
            backend.execute(*expression.to_sql(), options=_DDL)

            plan = OraclePartitionPlan(
                parent=table,
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                    OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                    OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
                ),
            )
            maintenance = OraclePartitionMaintenance(backend)
            report = maintenance.list(plan)
            assert any("MAXVALUE partition P_MAX" in issue for issue in report.issues)

            ensured = maintenance.ensure(plan)
            assert ensured.operations == ()
            assert any("ORA-14074" in error for error in ensured.errors)
            assert _partition_names(backend, table) == {"P1", "P_MAX"}
        finally:
            _drop(backend, table)


class TestAsyncMaintenanceLive:
    """Async executor parity on a live server."""

    @pytest.mark.backend
    @pytest.mark.asyncio
    @requires_protocol(OraclePartitionSupport, "supports_add_partition")
    async def test_async_ensure_list_drop(self, async_oracle_backend_single):
        backend = async_oracle_backend_single
        table = "PM_ASYNC_MNT"
        await _async_drop(backend, table)
        try:
            d = backend.dialect
            clause = OraclePartitionByRange(
                d,
                [Column(d, "CREATED_AT")],
                partitions=[
                    OraclePartitionDefinition(
                        name="P1",
                        less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                    ),
                ],
            )
            expression = CreateTableStatement(
                dialect=d,
                table=Table(d, table),
                columns=_columns(d),
                partition=clause,
            )
            await backend.execute(*expression.to_sql())

            plan = OraclePartitionPlan(
                parent=table,
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                    OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                ),
            )
            maintenance = AsyncOraclePartitionMaintenance(backend)
            report = await maintenance.list(plan)
            assert report.missing == (("PM_ASYNC_MNT", "P2"),)

            ensured = await maintenance.ensure(plan)
            assert ensured.errors == ()
            assert [operation.name for operation in ensured.operations] == ["P2"]

            dry = await maintenance.drop_before(plan, date(2026, 3, 1))
            assert [operation.name for operation in dry.operations] == ["P1", "P2"]
            assert dry.dry_run is True
        finally:
            await _async_drop(backend, table)
