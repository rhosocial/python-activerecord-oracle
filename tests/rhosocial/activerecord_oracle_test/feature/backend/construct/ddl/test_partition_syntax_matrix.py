# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_syntax_matrix.py
"""Offline Oracle partition-syntax matrix.

Every test is parameterized over the canonical catalog in
``partition_syntax_fixtures``.  The matrix pins plan generation, the
table-level subpartition shape, DDL fragments and MAXVALUE handling without
connecting to a server.
"""

from typing import Any, Iterable, Tuple

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.partition import (
    OracleListPolicy,
    OraclePartitionPlan,
    OraclePartitionSpec,
)
from partition_syntax_fixtures import (
    CASES,
    PartitionSyntaxCase,
    build_add_all,
    build_plan_clause,
    partition_syntax_case,
)

# The imported fixture is registered by pytest; __all__ marks it as used for linters.
__all__ = ["partition_syntax_case"]

REQUIRED_CASES = {
    "single_range",
    "single_list",
    "single_hash",
    "composite_range_hash",
    "composite_range_list",
    "composite_range_range",
    "composite_list_hash",
    "composite_list_list",
    "maxvalue",
    "subpartition_count",
    "subpartition_template",
    "lifecycle_add_drop",
    "lifecycle_split_merge",
    "lifecycle_exchange_truncate",
    "lifecycle_move",
    "add_drop_three_branches",
    "drop_named_partition",
}


def _dialect() -> OracleDialect:
    return OracleDialect(version=(23, 1, 0))


def _join_statements(statements: Iterable[Tuple[str, Tuple[Any, ...]]]) -> str:
    return "\n".join(sql for sql, _ in statements)


def _assert_fragments(sql: str, case: PartitionSyntaxCase) -> None:
    for fragment in case.expected_fragments:
        assert fragment in sql, "missing fragment %r in %s" % (fragment, sql)
    for fragment in case.forbidden_fragments:
        assert fragment not in sql, "forbidden fragment %r in %s" % (fragment, sql)


def test_catalog_covers_every_required_syntax_family():
    names = [case.name for case in CASES]
    assert len(names) == len(set(names))
    assert REQUIRED_CASES <= set(names)
    for case in CASES:
        assert isinstance(case, PartitionSyntaxCase)
        assert case.description
        assert case.expected_partition_names
        assert case.expected_fragments


def test_plan_spec_generation(partition_syntax_case):
    case = partition_syntax_case
    specs = case.build_specs()

    assert tuple(spec.name for spec in specs) == case.expected_partition_names
    for spec in specs:
        assert isinstance(spec, OraclePartitionSpec)
        assert spec.parent == case.plan.parent
        assert spec.method == case.plan.method
        assert spec.columns == case.plan.columns
        assert spec.name in case.expected_partition_names


def test_plan_clause_renders_strategy_and_every_branch(partition_syntax_case):
    case = partition_syntax_case
    sql, params = build_plan_clause(_dialect(), case.plan, case.through)

    assert params == ()
    assert "PARTITION BY %s" % case.plan.method in sql
    for spec in case.build_specs():
        assert 'PARTITION "%s"' % spec.name.upper() in sql
    if case.plan.subpartition is not None:
        assert "SUBPARTITION BY %s" % case.plan.subpartition.method in sql


def test_rendered_sql_matches_expected_and_forbidden_fragments(partition_syntax_case):
    case = partition_syntax_case
    dialect = _dialect()

    plan_sql, _ = build_plan_clause(dialect, case.plan, case.through)
    ddl_sql = _join_statements(case.build_ddl(dialect))
    _assert_fragments("\n".join((plan_sql, ddl_sql)), case)


def test_ddl_surfaces_render_required_statements(partition_syntax_case):
    case = partition_syntax_case
    statements = case.build_ddl(_dialect())

    for sql, params in statements:
        assert sql.startswith("ALTER TABLE")
        assert params == ()

    if not statements:
        assert case.ddl_builders == ()
        return

    joined = _join_statements(statements)
    for fragment in case.expected_fragments:
        if fragment.startswith("ALTER TABLE"):
            assert fragment in joined
    for fragment in case.forbidden_fragments:
        assert fragment not in joined


def test_per_branch_subpartition_shape(partition_syntax_case):
    case = partition_syntax_case
    if not case.expected_branch_shape:
        return

    specs = {spec.name: spec for spec in case.build_specs()}
    for name, declared_count, expected_count in case.expected_branch_shape:
        spec = specs[name]
        assert spec.subpartitions_count == declared_count
        assert case.plan.expected_subpartition_count(spec) == expected_count


def test_maxvalue_terminator_handling(partition_syntax_case):
    case = partition_syntax_case
    if not case.maxvalue_names:
        return

    specs = {spec.name: spec for spec in case.build_specs()}
    for name in case.maxvalue_names:
        spec = specs[name]
        assert spec.is_maxvalue is True
        assert spec.less_than == ("MAXVALUE",)
        assert spec.range_end is None


def test_single_column_list_add_keeps_every_value_in_the_group():
    dialect = _dialect()
    plan = OraclePartitionPlan(
        parent="REGIONS",
        method="LIST",
        columns="REGION",
        policy=OracleListPolicy(
            columns="REGION",
            groups=(("EAST", "NORTH"),),
            labels=("e",),
        ),
    )
    statements = build_add_all(dialect, plan)

    assert len(statements) == 1
    sql, params = statements[0]
    assert params == ()
    assert sql == ('ALTER TABLE "REGIONS" ADD PARTITION "REGIONS_E" ' "VALUES ('EAST', 'NORTH')")
