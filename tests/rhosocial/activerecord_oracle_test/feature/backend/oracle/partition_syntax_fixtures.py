# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/partition_syntax_fixtures.py
"""Canonical fixture catalog for the Oracle partition syntax matrix.

The catalog is intentionally data-only: one :class:`PartitionSyntaxCase`
describes a plan, the partition names it must generate, the SQL fragments the
renderer must emit, and the fragments it must never emit.  Both the offline
matrix tests and any future live probe consume the same case, so a syntax
family cannot be tested with one shape in one place and a different shape in
another.

Oracle's subpartition strategy is table level.  Every composite case therefore
keeps exactly one :class:`OracleSubpartitionPolicy`; per-branch variation is
limited to a HASH ``SUBPARTITIONS n`` count override or explicit subpartition
definitions.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleSubpartitionClause,
    OracleSubpartitionDefinition,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleDropPartitionStatement,
    OracleExchangePartitionStatement,
    OracleMergePartitionsStatement,
    OracleMovePartitionStatement,
    OracleSplitPartitionStatement,
    OracleTruncatePartitionStatement,
)
from rhosocial.activerecord.backend.impl.oracle.partition import (
    OracleHashPolicy,
    OracleListPolicy,
    OraclePartitionBranch,
    OraclePartitionInfo,
    OraclePartitionMaintenance,
    OraclePartitionPlan,
    OracleRangePolicy,
    OracleSubpartitionPolicy,
    OracleSubpartitionSpec,
)

#: One rendered statement: SQL text plus its (always empty) parameter tuple.
DdlStatement = Tuple[str, Tuple[Any, ...]]

#: A builder receives the dialect and the case plan, and returns statements.
DdlBuilder = Callable[[OracleDialect, OraclePartitionPlan], Tuple[DdlStatement, ...]]


@dataclass(frozen=True)
class MaintenanceScenario:
    """Offline maintenance expectations for a stub backend.

    ``present_names`` seeds the fake ``ALL_TAB_PARTITIONS`` rows.  Setting
    ``fail_marker`` makes the stub raise :class:`DatabaseError` for the first
    statement containing that marker, which is how the matrix proves Oracle's
    non-transactional partial completion is surfaced in ``report.errors``
    rather than swallowed by an imagined rollback.
    """

    present_names: Tuple[str, ...] = ()
    fail_marker: Optional[str] = None
    expected_created: Tuple[str, ...] = ()
    expected_errors_contain: Tuple[str, ...] = ()
    expected_missing: Tuple[str, ...] = ()
    cutoff: Optional[Any] = None
    expected_dropped: Tuple[str, ...] = ()
    named_drop: Optional[str] = None


@dataclass(frozen=True)
class PartitionSyntaxCase:
    """One Oracle partition syntax family.

    Attributes:
        name: Stable case id used as the pytest parameter id.
        description: Human-readable purpose of the case.
        plan: The plan the case exercises.
        expected_partition_names: Exact names ``iter_specs`` must generate.
        expected_fragments: Substrings every rendered SQL surface must contain.
        forbidden_fragments: Substrings no rendered SQL surface may contain.
        through: Optional horizon for policies that do not carry a count.
        expected_branch_shape: ``(name, subpartitions_count, expected_count)``
            triples for per-branch subpartition assertions.
        maxvalue_names: Specs that must be RANGE MAXVALUE terminators.
        drop_cutoff: Cutoff for maintenance candidate selection, if used.
        expected_drop_candidates: Candidate names at ``drop_cutoff``.
        ddl_builders: Statement builders layered on top of the plan clause.
        maintenance: Optional stub-backend maintenance scenario.
        live_setup: Optional callable for a future live probe (never run here).
        live_check: Optional callable for a future live probe (never run here).
    """

    name: str
    description: str
    plan: OraclePartitionPlan
    expected_partition_names: Tuple[str, ...]
    expected_fragments: Tuple[str, ...]
    forbidden_fragments: Tuple[str, ...] = ()
    through: Optional[Any] = None
    expected_branch_shape: Tuple[Tuple[str, Optional[int], Optional[int]], ...] = ()
    maxvalue_names: Tuple[str, ...] = ()
    drop_cutoff: Optional[Any] = None
    expected_drop_candidates: Tuple[str, ...] = ()
    ddl_builders: Tuple[DdlBuilder, ...] = ()
    maintenance: Optional[MaintenanceScenario] = None
    live_setup: Optional[Callable[[Any], None]] = None
    live_check: Optional[Callable[[Any], None]] = None

    def build_plan(self) -> OraclePartitionPlan:
        """Return the case's plan (a fresh read of the frozen declaration)."""
        return self.plan

    def build_specs(self) -> Tuple[Any, ...]:
        """Return every concrete top-level spec the plan generates."""
        return tuple(self.plan.iter_specs(through=self.through))

    def build_ddl(self, dialect: OracleDialect) -> Tuple[DdlStatement, ...]:
        """Return every statement the case's DDL builders render."""
        return tuple(
            statement
            for builder in self.ddl_builders
            for statement in builder(dialect, self.plan)
        )


# ---------------------------------------------------------------------------
# Rendering helpers shared by the catalog
# ---------------------------------------------------------------------------


class _RenderingBackend:
    """The only backend surface the DDL renderers touch."""

    def __init__(self, dialect: OracleDialect):
        self.dialect = dialect


def _boundary(dialect: OracleDialect, value: Any) -> Any:
    if isinstance(value, str) and value.upper() == "MAXVALUE":
        return OraclePartitionMaxValue(dialect)
    return OraclePartitionValue(dialect, value)


def _subpartition_definition(
    dialect: OracleDialect,
    spec: OracleSubpartitionSpec,
    multi_column: bool,
) -> OracleSubpartitionDefinition:
    less_than = None
    if spec.less_than is not None:
        less_than = [_boundary(dialect, value) for value in spec.less_than]
    in_values = None
    if spec.in_values is not None:
        if multi_column:
            in_values = [
                [_boundary(dialect, value) for value in group]
                for group in spec.in_values
            ]
        else:
            in_values = [
                _boundary(dialect, value)
                for group in spec.in_values
                for value in group
            ]
    return OracleSubpartitionDefinition(
        name=spec.name,
        less_than=less_than,
        in_values=in_values,
    )


def _subpartition_clause(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
) -> Optional[OracleSubpartitionClause]:
    policy = plan.subpartition
    if policy is None:
        return None
    templates = [
        _subpartition_definition(dialect, spec, len(policy.columns) > 1)
        for spec in policy.templates
    ]
    return OracleSubpartitionClause(
        dialect,
        policy.method,
        keys=[Column(dialect, column) for column in policy.columns],
        count=policy.count,
        templates=templates or None,
    )


def _partition_definition(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
    spec: Any,
) -> OraclePartitionDefinition:
    less_than = None
    if spec.less_than is not None:
        less_than = [_boundary(dialect, value) for value in spec.less_than]
    in_values = None
    if spec.in_values is not None:
        if len(spec.columns) > 1:
            in_values = [
                [_boundary(dialect, value) for value in group]
                for group in spec.in_values
            ]
        else:
            in_values = [
                _boundary(dialect, value)
                for group in spec.in_values
                for value in group
            ]
    subpartitions = None
    if spec.subpartitions:
        multi_column = bool(plan.subpartition and len(plan.subpartition.columns) > 1)
        subpartitions = [
            _subpartition_definition(dialect, sub, multi_column)
            for sub in spec.subpartitions
        ]
    return OraclePartitionDefinition(
        name=spec.name,
        less_than=less_than,
        in_values=in_values,
        subpartition_definitions=subpartitions,
        subpartitions_count=spec.subpartitions_count,
    )


def build_plan_clause(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
    through: Optional[Any] = None,
) -> DdlStatement:
    """Render the plan as a ``PARTITION BY ...`` CREATE-TABLE fragment."""
    specs = tuple(plan.iter_specs(through=through))
    definitions = [_partition_definition(dialect, plan, spec) for spec in specs]
    subpartition_by = _subpartition_clause(dialect, plan)
    keys = [Column(dialect, column) for column in plan.columns]
    if plan.method == "RANGE":
        clause = OraclePartitionByRange(
            dialect, keys, partitions=definitions, subpartition_by=subpartition_by
        )
    elif plan.method == "LIST":
        clause = OraclePartitionByList(
            dialect, keys, partitions=definitions, subpartition_by=subpartition_by
        )
    else:
        clause = OraclePartitionByHash(
            dialect, keys, partitions=definitions, subpartition_by=subpartition_by
        )
    return clause.to_sql()


def build_add_all(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
    through: Optional[Any] = None,
) -> Tuple[DdlStatement, ...]:
    """Render one ``ALTER TABLE ... ADD PARTITION`` per expected spec."""
    maintenance = OraclePartitionMaintenance(_RenderingBackend(dialect))
    return tuple(
        maintenance._add_statement(plan, spec)
        for spec in plan.iter_specs(through=through)
    )


def build_add_drop_named(name: str) -> DdlBuilder:
    """Return an ADD-all builder followed by a DROP of *name*."""

    def builder(dialect: OracleDialect, plan: OraclePartitionPlan) -> Tuple[DdlStatement, ...]:
        statements = list(build_add_all(dialect, plan))
        drop = OracleDropPartitionStatement(dialect, Table(dialect, plan.parent), name)
        statements.append(drop.to_sql())
        return tuple(statements)

    return builder


def build_add_drop(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
) -> Tuple[DdlStatement, ...]:
    """Render ADD then DROP for the first branch of the case plan."""
    first = tuple(plan.iter_specs())[0].name
    return build_add_drop_named(first)(dialect, plan)


def build_split_merge(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
) -> Tuple[DdlStatement, ...]:
    """Render SPLIT and MERGE, the two resizing lifecycle verbs."""
    table = Table(dialect, plan.parent)
    split = OracleSplitPartitionStatement(
        dialect,
        table,
        "P_MAX",
        at_values=[OraclePartitionValue(dialect, date(2026, 2, 1))],
        new_partitions=[
            OraclePartitionDefinition(
                name="P2",
                less_than=[OraclePartitionValue(dialect, date(2026, 2, 1))],
            ),
            OraclePartitionDefinition(
                name="P_MAX",
                less_than=[OraclePartitionMaxValue(dialect)],
            ),
        ],
    )
    merge = OracleMergePartitionsStatement(
        dialect,
        table,
        ["P1", "P2"],
        OraclePartitionDefinition(
            name="P12",
            less_than=[OraclePartitionValue(dialect, date(2026, 3, 1))],
        ),
    )
    return (split.to_sql(), merge.to_sql())


def build_exchange_truncate(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
) -> Tuple[DdlStatement, ...]:
    """Render EXCHANGE and TRUNCATE, the two content lifecycle verbs."""
    table = Table(dialect, plan.parent)
    exchange = OracleExchangePartitionStatement(
        dialect,
        table,
        "P1",
        Table(dialect, plan.parent + "_P1_STAGING"),
    )
    truncate = OracleTruncatePartitionStatement(
        dialect,
        table,
        "P1",
        update_indexes=True,
    )
    return (exchange.to_sql(), truncate.to_sql())


def build_move(
    dialect: OracleDialect,
    plan: OraclePartitionPlan,
) -> Tuple[DdlStatement, ...]:
    """Render MOVE with a destination tablespace."""
    move = OracleMovePartitionStatement(
        dialect,
        Table(dialect, plan.parent),
        "P1",
        tablespace_name="USERS",
    )
    return (move.to_sql(),)


def partition_infos(
    plan: OraclePartitionPlan,
    present_names: Optional[Tuple[str, ...]] = None,
) -> Tuple[OraclePartitionInfo, ...]:
    """Build catalogue rows for candidate selection without a server."""
    specs = tuple(plan.iter_specs())
    names = tuple(spec.name for spec in specs) if present_names is None else present_names
    return tuple(
        OraclePartitionInfo(
            name=spec.name,
            parent=plan.parent,
            level=1,
            method=plan.method,
            is_partitioned=False,
        )
        for spec in specs
        if spec.name in names
    )


def _quoted_name_after(sql: str, marker: str) -> Optional[str]:
    start = sql.find(marker)
    if start < 0:
        return None
    start += len(marker)
    if start >= len(sql) or sql[start] != '"':
        return None
    end = sql.find('"', start + 1)
    if end < 0:
        return None
    return sql[start + 1:end]


def _oracle_scalar_text(value: Any) -> str:
    """Render one plan value the way ``ALL_TAB_PARTITIONS`` would spell it."""
    if isinstance(value, datetime):
        return "TO_DATE(' %s', 'SYYYY-MM-DD HH24:MI:SS')" % value.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    if isinstance(value, date):
        return "TO_DATE(' %s 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')" % value.isoformat()
    if isinstance(value, str):
        return "'%s'" % value.replace("'", "''")
    return str(value)


def _oracle_bound_text(spec: Optional[Any]) -> Optional[str]:
    """Render a spec's bound as catalogue text, for stub dictionary rows."""
    if spec is None:
        return None
    if spec.method == "RANGE":
        if not spec.less_than:
            return None
        value = spec.less_than[0]
        if value == "MAXVALUE":
            return "MAXVALUE"
        return _oracle_scalar_text(value)
    if spec.method == "LIST":
        groups = spec.in_values or ()
        if len(spec.columns) > 1:
            return ", ".join(
                "(%s)" % ", ".join(_oracle_scalar_text(value) for value in group)
                for group in groups
            )
        values = [value for group in groups for value in group]
        return ", ".join(_oracle_scalar_text(value) for value in values)
    return None


class PartitionStubBackend:
    """Minimal non-transactional Oracle surface for offline maintenance tests.

    The dictionary queries are answered from in-memory rows.  ``execute``
    records every statement and deliberately keeps going after a configured
    failure, which mirrors Oracle's implicit-commit DDL.

    When *plan* is supplied, present rows carry the plan's own bounds rather
    than a fixed date, so bound comparison is exercised against realistic
    dictionary text.  ``bound_overrides`` lets a test seed a deliberately
    wrong bound without changing the plan.
    """

    def __init__(
        self,
        dialect: OracleDialect,
        target: str,
        root_method: str = "RANGE",
        subpartition_method: Optional[str] = None,
        present_names: Tuple[str, ...] = (),
        fail_marker: Optional[str] = None,
        plan: Optional[OraclePartitionPlan] = None,
        bound_overrides: Optional[dict] = None,
    ):
        self.dialect = dialect
        self.target = target
        self.root_method = root_method
        self.subpartition_method = subpartition_method
        self.fail_marker = fail_marker
        self.bound_overrides = dict(bound_overrides or {})
        self.specs = {}
        if plan is not None:
            try:
                self.specs = {spec.name: spec for spec in plan.iter_specs()}
            except ValueError:
                self.specs = {}
        self.executed: list = []
        self.partition_rows: list = [
            {
                "partition_name": name,
                "subpartition_count": 0,
                "composite": "NO",
                "high_value": self._seed_bound(name),
                "partition_position": index + 1,
            }
            for index, name in enumerate(present_names)
        ]
        self.subpartition_rows: list = []

    def _seed_bound(self, name: str) -> Optional[str]:
        if name in self.bound_overrides:
            return self.bound_overrides[name]
        return _oracle_bound_text(self.specs.get(name))

    def fetch_all(self, sql: str, params: Tuple[Any, ...] = ()):
        upper = sql.upper()
        if "ALL_PART_TABLES" in upper:
            return [{
                "owner": "TEST",
                "table_name": self.target,
                "partitioning_type": self.root_method,
                "subpartitioning_type": self.subpartition_method or "NONE",
                "partition_count": len(self.partition_rows),
                "def_subpartition_count": 0,
            }]
        if "ALL_TAB_SUBPARTITIONS" in upper:
            return list(self.subpartition_rows)
        if "ALL_TAB_PARTITIONS" in upper:
            return list(self.partition_rows)
        return []

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        self.executed.append((sql, params))
        if self.fail_marker and self.fail_marker in sql:
            raise DatabaseError("simulated non-transactional DDL failure")
        if "ADD PARTITION" in sql:
            name = _quoted_name_after(sql, "ADD PARTITION ")
            if name and name not in {row["partition_name"] for row in self.partition_rows}:
                self.partition_rows.append({
                    "partition_name": name,
                    "subpartition_count": 0,
                    "composite": "NO",
                    "high_value": _oracle_bound_text(self.specs.get(name)),
                    "partition_position": len(self.partition_rows) + 1,
                })
        elif "DROP PARTITION" in sql:
            name = _quoted_name_after(sql, "DROP PARTITION ")
            self.partition_rows = [
                row for row in self.partition_rows
                if row["partition_name"] != name
            ]


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


CASES: Tuple[PartitionSyntaxCase, ...] = (
    PartitionSyntaxCase(
        name="single_range",
        description="RANGE policy generates monthly half-open partitions.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(
                columns="CREATED_AT",
                start=date(2026, 1, 1),
                interval="month",
                count=3,
            ),
        ),
        expected_partition_names=(
            "EVENTS_2026_01",
            "EVENTS_2026_02",
            "EVENTS_2026_03",
        ),
        expected_fragments=(
            'PARTITION BY RANGE ("CREATED_AT")',
            'PARTITION "EVENTS_2026_01" VALUES LESS THAN (DATE \'2026-02-01\')',
            'ALTER TABLE "EVENTS" ADD PARTITION "EVENTS_2026_03" '
            'VALUES LESS THAN (DATE \'2026-04-01\')',
        ),
        forbidden_fragments=("SUBPARTITION", "MAXVALUE"),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="single_list",
        description="LIST policy groups values and honours explicit labels.",
        plan=OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            policy=OracleListPolicy(
                columns="REGION",
                groups=(("EAST",), ("NORTH",), ("WEST",)),
                labels=("e", "n", "w"),
            ),
        ),
        expected_partition_names=("REGIONS_e", "REGIONS_n", "REGIONS_w"),
        expected_fragments=(
            'PARTITION BY LIST ("REGION")',
            'PARTITION "REGIONS_E" VALUES (\'EAST\')',
            'PARTITION "REGIONS_N" VALUES (\'NORTH\')',
            'ALTER TABLE "REGIONS" ADD PARTITION "REGIONS_W" VALUES (\'WEST\')',
        ),
        forbidden_fragments=("SUBPARTITION", "MAXVALUE", "LESS THAN"),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="single_hash",
        description=(
            "HASH policy names every bucket. Oracle has no per-bucket ADD, so "
            "this case pins the CREATE-TABLE fragment only."
        ),
        plan=OraclePartitionPlan(
            parent="BUCKETS",
            method="HASH",
            columns="ID",
            policy=OracleHashPolicy(columns="ID", modulus=4),
        ),
        expected_partition_names=(
            "BUCKETS_p0",
            "BUCKETS_p1",
            "BUCKETS_p2",
            "BUCKETS_p3",
        ),
        expected_fragments=(
            'PARTITION BY HASH ("ID")',
            'PARTITION "BUCKETS_P0"',
            'PARTITION "BUCKETS_P3"',
        ),
        forbidden_fragments=("VALUES", "PARTITIONS 4", "SUBPARTITION"),
        ddl_builders=(),
    ),
    PartitionSyntaxCase(
        name="composite_range_hash",
        description="RANGE parent, HASH table-level subpartitions, one count override.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
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
        ),
        expected_partition_names=("P1", "P2"),
        expected_fragments=(
            'SUBPARTITION BY HASH ("ID") SUBPARTITIONS 2',
            'PARTITION "P1" VALUES LESS THAN (DATE \'2026-02-01\') SUBPARTITIONS 3',
            'ALTER TABLE "EVENTS" ADD PARTITION "P1" '
            'VALUES LESS THAN (DATE \'2026-02-01\') SUBPARTITIONS 3',
        ),
        forbidden_fragments=("MAXVALUE", "SUBPARTITION TEMPLATE"),
        expected_branch_shape=(("P1", 3, 3), ("P2", None, 2)),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="composite_range_list",
        description="RANGE parent with explicit per-branch LIST subpartitions.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
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
            subpartition=OracleSubpartitionPolicy(method="LIST", columns="REGION"),
        ),
        expected_partition_names=("P1", "P2"),
        expected_fragments=(
            'SUBPARTITION BY LIST ("REGION")',
            'PARTITION "P1" VALUES LESS THAN (DATE \'2026-02-01\') '
            '(SUBPARTITION "P1_E" VALUES (\'EAST\'), '
            'SUBPARTITION "P1_W" VALUES (\'WEST\'))',
            'ALTER TABLE "EVENTS" ADD PARTITION "P2" '
            'VALUES LESS THAN (DATE \'2026-03-01\') '
            '(SUBPARTITION "P2_E" VALUES (\'EAST\'))',
        ),
        forbidden_fragments=("SUBPARTITIONS", "MAXVALUE", "SUBPARTITION TEMPLATE"),
        expected_branch_shape=(("P1", None, 2), ("P2", None, 1)),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="composite_range_range",
        description="RANGE parent with explicit per-branch RANGE subpartitions.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1",
                    less_than=(date(2026, 2, 1),),
                    subpartitions=(
                        OracleSubpartitionSpec(
                            name="P1_S1", less_than=(date(2026, 1, 15),)
                        ),
                        OracleSubpartitionSpec(
                            name="P1_S2", less_than=(date(2026, 2, 1),)
                        ),
                    ),
                ),
                OraclePartitionBranch(
                    name="P2",
                    less_than=(date(2026, 3, 1),),
                    subpartitions=(
                        OracleSubpartitionSpec(
                            name="P2_S1", less_than=(date(2026, 2, 15),)
                        ),
                        OracleSubpartitionSpec(
                            name="P2_S2", less_than=(date(2026, 3, 1),)
                        ),
                    ),
                ),
            ),
            subpartition=OracleSubpartitionPolicy(method="RANGE", columns="SHIPPED_AT"),
        ),
        expected_partition_names=("P1", "P2"),
        expected_fragments=(
            'SUBPARTITION BY RANGE ("SHIPPED_AT")',
            'SUBPARTITION "P1_S1" VALUES LESS THAN (DATE \'2026-01-15\')',
            'SUBPARTITION "P2_S2" VALUES LESS THAN (DATE \'2026-03-01\')',
            'ALTER TABLE "EVENTS" ADD PARTITION "P1" '
            'VALUES LESS THAN (DATE \'2026-02-01\')',
        ),
        forbidden_fragments=("SUBPARTITIONS", "MAXVALUE", "SUBPARTITION TEMPLATE"),
        expected_branch_shape=(("P1", None, 2), ("P2", None, 2)),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="composite_list_hash",
        description="LIST parent with HASH subpartitions and a count override.",
        plan=OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            branches=(
                OraclePartitionBranch(
                    name="P_EAST",
                    in_values=("EAST",),
                    subpartitions_count=3,
                ),
                OraclePartitionBranch(name="P_WEST", in_values=("WEST",)),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="HASH", columns="ID", count=2
            ),
        ),
        expected_partition_names=("P_EAST", "P_WEST"),
        expected_fragments=(
            'PARTITION BY LIST ("REGION")',
            'SUBPARTITION BY HASH ("ID") SUBPARTITIONS 2',
            'PARTITION "P_EAST" VALUES (\'EAST\') SUBPARTITIONS 3',
            'ALTER TABLE "REGIONS" ADD PARTITION "P_EAST" '
            'VALUES (\'EAST\') SUBPARTITIONS 3',
        ),
        forbidden_fragments=("MAXVALUE", "SUBPARTITION TEMPLATE"),
        expected_branch_shape=(("P_EAST", 3, 3), ("P_WEST", None, 2)),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="composite_list_list",
        description="LIST parent with explicit per-branch LIST subpartitions.",
        plan=OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            branches=(
                OraclePartitionBranch(
                    name="P_EAST",
                    in_values=("EAST",),
                    subpartitions=(
                        OracleSubpartitionSpec(name="S_EAST1", in_values=("EAST",)),
                        OracleSubpartitionSpec(name="S_EAST2", in_values=("NORTH",)),
                    ),
                ),
                OraclePartitionBranch(
                    name="P_WEST",
                    in_values=("WEST",),
                    subpartitions=(
                        OracleSubpartitionSpec(name="S_WEST1", in_values=("WEST",)),
                    ),
                ),
            ),
            subpartition=OracleSubpartitionPolicy(method="LIST", columns="REGION"),
        ),
        expected_partition_names=("P_EAST", "P_WEST"),
        expected_fragments=(
            'SUBPARTITION BY LIST ("REGION")',
            'PARTITION "P_EAST" VALUES (\'EAST\') '
            '(SUBPARTITION "S_EAST1" VALUES (\'EAST\'), '
            'SUBPARTITION "S_EAST2" VALUES (\'NORTH\'))',
            'ALTER TABLE "REGIONS" ADD PARTITION "P_WEST" VALUES (\'WEST\') '
            '(SUBPARTITION "S_WEST1" VALUES (\'WEST\'))',
        ),
        forbidden_fragments=("SUBPARTITIONS", "MAXVALUE", "SUBPARTITION TEMPLATE"),
        expected_branch_shape=(("P_EAST", None, 2), ("P_WEST", None, 1)),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="maxvalue",
        description="MAXVALUE terminates RANGE and is never a drop_before candidate.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
            ),
        ),
        expected_partition_names=("P1", "P2", "P_MAX"),
        expected_fragments=(
            'PARTITION "P_MAX" VALUES LESS THAN (MAXVALUE)',
            'ALTER TABLE "EVENTS" ADD PARTITION "P_MAX" VALUES LESS THAN (MAXVALUE)',
            'ALTER TABLE "EVENTS" ADD PARTITION "P1" '
            'VALUES LESS THAN (DATE \'2026-02-01\')',
        ),
        forbidden_fragments=("SUBPARTITION",),
        maxvalue_names=("P_MAX",),
        drop_cutoff=date(2030, 1, 1),
        expected_drop_candidates=("P1", "P2"),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="subpartition_count",
        description="Every branch may override the table-level HASH count independently.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1",
                    less_than=(date(2026, 2, 1),),
                    subpartitions_count=4,
                ),
                OraclePartitionBranch(
                    name="P2",
                    less_than=(date(2026, 3, 1),),
                    subpartitions_count=1,
                ),
                OraclePartitionBranch(name="P3", less_than=(date(2026, 4, 1),)),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="HASH", columns="ID", count=2
            ),
        ),
        expected_partition_names=("P1", "P2", "P3"),
        expected_fragments=(
            'SUBPARTITION BY HASH ("ID") SUBPARTITIONS 2',
            'PARTITION "P1" VALUES LESS THAN (DATE \'2026-02-01\') SUBPARTITIONS 4',
            'PARTITION "P2" VALUES LESS THAN (DATE \'2026-03-01\') SUBPARTITIONS 1',
            'ALTER TABLE "EVENTS" ADD PARTITION "P2" '
            'VALUES LESS THAN (DATE \'2026-03-01\') SUBPARTITIONS 1',
        ),
        forbidden_fragments=("SUBPARTITION TEMPLATE",),
        expected_branch_shape=(
            ("P1", 4, 4),
            ("P2", 1, 1),
            ("P3", None, 2),
        ),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="subpartition_template",
        description="Explicit SUBPARTITION TEMPLATE fixes names for every partition.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="LIST",
                columns="REGION",
                templates=(
                    OracleSubpartitionSpec(name="SP_E", in_values=("EAST",)),
                    OracleSubpartitionSpec(name="SP_W", in_values=("WEST",)),
                ),
            ),
        ),
        expected_partition_names=("P1", "P2"),
        expected_fragments=(
            'SUBPARTITION BY LIST ("REGION") SUBPARTITION TEMPLATE',
            'SUBPARTITION "SP_E" VALUES (\'EAST\')',
            'SUBPARTITION "SP_W" VALUES (\'WEST\')',
            'ALTER TABLE "EVENTS" ADD PARTITION "P1" '
            'VALUES LESS THAN (DATE \'2026-02-01\')',
        ),
        forbidden_fragments=("SUBPARTITIONS", "MAXVALUE"),
        expected_branch_shape=(("P1", None, 2), ("P2", None, 2)),
        ddl_builders=(build_add_all,),
    ),
    PartitionSyntaxCase(
        name="lifecycle_add_drop",
        description="ADD and DROP are the two flat lifecycle verbs.",
        plan=OraclePartitionPlan(
            parent="ORDERS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
        ),
        expected_partition_names=("P1",),
        expected_fragments=(
            'ALTER TABLE "ORDERS" ADD PARTITION "P1" '
            'VALUES LESS THAN (DATE \'2026-02-01\')',
            'ALTER TABLE "ORDERS" DROP PARTITION "P1"',
            'PARTITION BY RANGE ("CREATED_AT")',
        ),
        forbidden_fragments=("SPLIT PARTITION", "MERGE PARTITIONS", "TRUNCATE PARTITION"),
        ddl_builders=(build_add_drop,),
    ),
    PartitionSyntaxCase(
        name="lifecycle_split_merge",
        description="SPLIT and MERGE resize the RANGE tree without dropping data.",
        plan=OraclePartitionPlan(
            parent="ORDERS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
            ),
        ),
        expected_partition_names=("P1", "P2", "P_MAX"),
        expected_fragments=(
            'ALTER TABLE "ORDERS" SPLIT PARTITION "P_MAX" AT (DATE \'2026-02-01\') '
            'INTO (PARTITION "P2", PARTITION "P_MAX")',
            'ALTER TABLE "ORDERS" MERGE PARTITIONS "P1", "P2" INTO PARTITION "P12"',
            'PARTITION "P_MAX" VALUES LESS THAN (MAXVALUE)',
        ),
        forbidden_fragments=("ADD PARTITION", "DROP PARTITION", "TRUNCATE PARTITION"),
        maxvalue_names=("P_MAX",),
        ddl_builders=(build_split_merge,),
    ),
    PartitionSyntaxCase(
        name="lifecycle_exchange_truncate",
        description="EXCHANGE swaps a partition with a table; TRUNCATE empties it.",
        plan=OraclePartitionPlan(
            parent="ORDERS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
        ),
        expected_partition_names=("P1",),
        expected_fragments=(
            'ALTER TABLE "ORDERS" EXCHANGE PARTITION "P1" WITH TABLE '
            '"ORDERS_P1_STAGING" WITH VALIDATION',
            'ALTER TABLE "ORDERS" TRUNCATE PARTITION "P1" UPDATE INDEXES',
        ),
        forbidden_fragments=("ADD PARTITION", "DROP PARTITION", "MERGE PARTITIONS"),
        ddl_builders=(build_exchange_truncate,),
    ),
    PartitionSyntaxCase(
        name="lifecycle_move",
        description="MOVE relocates a partition segment to another tablespace.",
        plan=OraclePartitionPlan(
            parent="ORDERS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
        ),
        expected_partition_names=("P1",),
        expected_fragments=(
            'ALTER TABLE "ORDERS" MOVE PARTITION "P1" TABLESPACE "USERS"',
        ),
        forbidden_fragments=("ADD PARTITION", "DROP PARTITION", "TRUNCATE PARTITION"),
        ddl_builders=(build_move,),
    ),
    PartitionSyntaxCase(
        name="maintenance_ensure_list_drop_validate",
        description=(
            "A stub backend proves ensure keeps going after a failure, list "
            "reports the missing partition, validate agrees, and drop_before "
            "selects only the partition the catalogue actually holds."
        ),
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                OraclePartitionBranch(name="P3", less_than=(date(2026, 4, 1),)),
            ),
        ),
        expected_partition_names=("P1", "P2", "P3"),
        expected_fragments=(
            'PARTITION BY RANGE ("CREATED_AT")',
            'ALTER TABLE "EVENTS" ADD PARTITION "P3" '
            'VALUES LESS THAN (DATE \'2026-04-01\')',
            'ALTER TABLE "EVENTS" DROP PARTITION "P1"',
        ),
        forbidden_fragments=("SUBPARTITION", "MAXVALUE"),
        ddl_builders=(build_add_drop_named("P1"),),
        maintenance=MaintenanceScenario(
            present_names=("P1",),
            fail_marker='ADD PARTITION "P2"',
            expected_created=("P3",),
            expected_errors_contain=("P2:",),
            expected_missing=("P2",),
            cutoff=date(2026, 3, 1),
            expected_dropped=("P1",),
        ),
    ),
    PartitionSyntaxCase(
        name="maintenance_drop_partition",
        description="drop_partition drops one explicitly named partition.",
        plan=OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
            ),
        ),
        expected_partition_names=("P1", "P2"),
        expected_fragments=(
            'ALTER TABLE "EVENTS" ADD PARTITION "P2" '
            'VALUES LESS THAN (DATE \'2026-03-01\')',
            'ALTER TABLE "EVENTS" DROP PARTITION "P2"',
        ),
        forbidden_fragments=("SUBPARTITION", "MAXVALUE"),
        ddl_builders=(build_add_drop_named("P2"),),
        maintenance=MaintenanceScenario(
            present_names=("P1", "P2"),
            named_drop="P2",
        ),
    ),
)


@pytest.fixture(params=CASES, ids=[case.name for case in CASES])
def partition_syntax_case(request) -> PartitionSyntaxCase:
    """Yield every canonical case once, keyed by its stable name."""
    return request.param
