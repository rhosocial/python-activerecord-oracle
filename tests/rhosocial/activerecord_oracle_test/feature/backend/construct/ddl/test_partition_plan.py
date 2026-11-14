# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_plan.py
"""Unit tests for Oracle partition plans and maintenance helpers.

No server is required. The tests pin:

* generated RANGE / LIST / HASH plans and their half-open boundaries,
* explicit branches with per-partition subpartition definitions / counts,
* the table-level subpartition-strategy invariant (no per-branch strategy),
* missing / orphan / continuity reporting,
* drop-before candidate selection, and
* rendered ``ALTER TABLE ... ADD/DROP PARTITION`` statements.
"""

from datetime import date

import pytest

from rhosocial.activerecord.backend.partition import (
    AsyncPartitionEnsurer,
    AsyncPartitionInventory,
    AsyncPartitionRetention,
    AsyncPartitionValidator,
    PartitionEnsurer,
    PartitionInventory,
    PartitionReport as CorePartitionReport,
    PartitionRetention,
    PartitionValidator,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.partition import (
    AsyncOraclePartitionMaintenance,
    OracleHashPolicy,
    OracleListPolicy,
    OraclePartitionBranch,
    OraclePartitionInfo,
    OraclePartitionMaintenance,
    OraclePartitionPlan,
    OraclePartitionReport,
    OracleRangePolicy,
    OracleSubpartitionPolicy,
    OracleSubpartitionSpec,
    parse_high_value,
)
from rhosocial.activerecord.backend.impl.oracle.partition.bounds import (
    add_interval,
    default_label,
    normalize_interval,
)


class _StubBackend:
    """Minimal backend surface the maintenance helpers need for rendering."""

    def __init__(self, version=(23, 1, 0)):
        self.dialect = OracleDialect(version=version)


def _maintenance(version=(23, 1, 0)) -> OraclePartitionMaintenance:
    return OraclePartitionMaintenance(_StubBackend(version))


class TestBounds:
    """Calendar arithmetic used by generated RANGE policies."""

    def test_normalize_interval_spellings(self):
        assert normalize_interval("month") == (1, "month")
        assert normalize_interval("15 day") == (15, "day")
        assert normalize_interval((2, "year")) == (2, "year")

    def test_normalize_interval_rejects_unknown_unit(self):
        with pytest.raises(ValueError, match="interval unit"):
            normalize_interval("fortnight")

    def test_add_interval_clamps_month_end(self):
        assert add_interval(date(2026, 1, 31), "month") == date(2026, 2, 28)

    def test_default_labels(self):
        assert default_label(date(2026, 1, 1), "month") == "2026_01"
        assert default_label(date(2026, 1, 1), "quarter") == "2026_q1"
        assert default_label(date(2026, 1, 1), "year") == "2026"
        assert default_label(date(2026, 1, 1), (15, "day")) == "2026_01_01"


class TestGeneratedPolicies:
    """Policy-generated first-level partitions."""

    def test_range_policy_generates_months(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month", count=3),
        )
        specs = list(plan.iter_specs())
        assert [spec.name for spec in specs] == [
            "EVENTS_2026_01",
            "EVENTS_2026_02",
            "EVENTS_2026_03",
        ]
        assert specs[0].less_than == (date(2026, 2, 1),)
        assert specs[0].range_start == date(2026, 1, 1)
        assert specs[0].range_end == date(2026, 2, 1)

    def test_range_policy_through_is_exclusive(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
        )
        specs = list(plan.iter_specs(through=date(2026, 3, 1)))
        assert len(specs) == 2

    def test_range_maxvalue_terminator(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(
                start=date(2026, 1, 1), interval="month", count=1, maxvalue=True
            ),
        )
        specs = list(plan.iter_specs())
        assert specs[-1].name == "EVENTS_maxvalue"
        assert specs[-1].less_than == ("MAXVALUE",)
        assert specs[-1].is_maxvalue is True
        assert specs[-1].range_end is None

    def test_list_policy_groups(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            policy=OracleListPolicy(groups=(("EAST",), ("WEST",)), labels=("e", "w")),
        )
        specs = list(plan.iter_specs())
        assert [spec.name for spec in specs] == ["REGIONS_e", "REGIONS_w"]
        assert specs[0].in_values == (("EAST",),)

    def test_list_policy_multi_column_groups(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns=("COUNTRY", "REGION"),
            policy=OracleListPolicy(groups=(("US", "EAST"), ("US", "WEST"))),
        )
        specs = list(plan.iter_specs())
        assert specs[0].in_values == (("US", "EAST"),)

    def test_hash_policy_names_explicit_partitions(self):
        plan = OraclePartitionPlan(
            parent="BUCKETS",
            method="HASH",
            columns="ID",
            policy=OracleHashPolicy(modulus=4),
        )
        specs = list(plan.iter_specs())
        assert [spec.name for spec in specs] == [
            "BUCKETS_p0",
            "BUCKETS_p1",
            "BUCKETS_p2",
            "BUCKETS_p3",
        ]
        assert all(spec.less_than is None and spec.in_values is None for spec in specs)

    def test_range_policy_needs_a_horizon(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
        )
        assert plan.is_fully_bounded() is False
        with pytest.raises(ValueError, match="count or through"):
            list(plan.iter_specs())

    def test_duplicate_names_are_rejected(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            policy=OracleListPolicy(groups=(("EAST",), ("WEST",)), labels=("a", "a")),
        )
        with pytest.raises(ValueError, match="duplicate partition name"):
            list(plan.iter_specs())


class TestExplicitBranches:
    """Per-partition subpartition definitions / count variation."""

    def _plan(self, **kwargs):
        defaults = dict(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1",
                    less_than=(date(2026, 2, 1),),
                    subpartitions_count=3,
                ),
                OraclePartitionBranch(
                    name="P2",
                    less_than=(date(2026, 3, 1),),
                ),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="HASH", columns="ID", count=2
            ),
        )
        defaults.update(kwargs)
        return OraclePartitionPlan(**defaults)

    def test_branch_count_override_and_default(self):
        plan = self._plan()
        specs = list(plan.iter_specs())
        assert specs[0].subpartitions_count == 3
        assert plan.expected_subpartition_count(specs[0]) == 3
        assert plan.expected_subpartition_count(specs[1]) == 2

    def test_explicit_list_subpartitions_vary_per_branch(self):
        plan = OraclePartitionPlan(
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
        )
        specs = list(plan.iter_specs())
        assert plan.expected_subpartition_count(specs[0]) == 2
        assert plan.expected_subpartition_count(specs[1]) == 1
        assert specs[0].subpartitions[1].name == "P1_W"

    def test_branch_cannot_declare_a_strategy(self):
        """A branch may carry bounds and definitions, never a strategy.

        The only strategy slot is the plan's single ``subpartition``. A
        RANGE branch carrying LIST-style definitions is refused, which is the
        structural proof that Oracle's strategy is table level.
        """
        with pytest.raises(ValueError, match="RANGE"):
            OraclePartitionPlan(
                parent="EVENTS",
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(
                        name="P1",
                        less_than=(date(2026, 2, 1),),
                        subpartitions=(
                            OracleSubpartitionSpec(
                                name="P1_E", in_values=("EAST",)
                            ),
                        ),
                    ),
                ),
                subpartition=OracleSubpartitionPolicy(
                    method="RANGE", columns="REGION"
                ),
            )

    def test_count_override_requires_hash_subpartitioning(self):
        with pytest.raises(ValueError, match="only valid for HASH"):
            OraclePartitionPlan(
                parent="EVENTS",
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(
                        name="P1",
                        less_than=(date(2026, 2, 1),),
                        subpartitions_count=2,
                    ),
                ),
                subpartition=OracleSubpartitionPolicy(
                    method="LIST", columns="REGION"
                ),
            )

    def test_subpartitions_require_a_table_strategy(self):
        with pytest.raises(ValueError, match="no table-level"):
            OraclePartitionPlan(
                parent="EVENTS",
                method="RANGE",
                columns="CREATED_AT",
                branches=(
                    OraclePartitionBranch(
                        name="P1",
                        less_than=(date(2026, 2, 1),),
                        subpartitions_count=2,
                    ),
                ),
            )

    def test_count_and_definitions_are_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            OraclePartitionBranch(
                name="P1",
                less_than=(date(2026, 2, 1),),
                subpartitions_count=2,
                subpartitions=(OracleSubpartitionSpec(name="SP1"),),
            )

    def test_subpartition_count_and_template_are_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            OracleSubpartitionPolicy(
                method="HASH",
                columns="ID",
                count=2,
                templates=(OracleSubpartitionSpec(name="SP1"),),
            )


class TestPlanShapeValidation:
    """Plan-level shape rules."""

    def test_policy_and_branches_are_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            OraclePartitionPlan(
                parent="EVENTS",
                method="RANGE",
                columns="CREATED_AT",
                policy=OracleRangePolicy(
                    start=date(2026, 1, 1), interval="month", count=1
                ),
                branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
            )

    def test_range_parent_requires_one_column(self):
        with pytest.raises(ValueError, match="exactly one"):
            OraclePartitionPlan(
                parent="EVENTS",
                method="RANGE",
                columns=("A", "B"),
                branches=(OraclePartitionBranch(name="P1", less_than=(1,)),),
            )

    def test_branch_bound_must_match_parent_method(self):
        with pytest.raises(ValueError, match="requires in_values"):
            OraclePartitionPlan(
                parent="REGIONS",
                method="LIST",
                columns="REGION",
                branches=(OraclePartitionBranch(name="P1", less_than=(1,)),),
            )

    def test_name_pattern_requires_suffix(self):
        with pytest.raises(ValueError, match="suffix"):
            OraclePartitionPlan(
                parent="EVENTS",
                method="RANGE",
                columns="CREATED_AT",
                branches=(OraclePartitionBranch(name="P1", less_than=(1,)),),
                name_pattern="{parent}",
            )


class TestMaintenanceRendering:
    """DDL derived from plan specs, without a server."""

    def test_add_partition_with_count_override(self):
        maintenance = _maintenance()
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1",
                    less_than=(date(2026, 2, 1),),
                    subpartitions_count=3,
                ),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="HASH", columns="ID", count=2
            ),
        )
        spec = next(iter(plan.iter_specs()))
        sql, params = maintenance._add_statement(plan, spec)
        assert (
            'ALTER TABLE "EVENTS" ADD PARTITION "P1" '
            "VALUES LESS THAN (DATE '2026-02-01') SUBPARTITIONS 3"
        ) == sql
        assert params == ()

    def test_add_partition_with_list_subpartitions(self):
        maintenance = _maintenance()
        plan = OraclePartitionPlan(
            parent="REGIONS",
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
            ),
            subpartition=OracleSubpartitionPolicy(method="LIST", columns="REGION"),
        )
        spec = next(iter(plan.iter_specs()))
        sql, _ = maintenance._add_statement(plan, spec)
        assert 'SUBPARTITION "P1_E" VALUES (\'EAST\')' in sql
        assert 'SUBPARTITION "P1_W" VALUES (\'WEST\')' in sql

    def test_add_maxvalue_partition(self):
        maintenance = _maintenance()
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
            ),
        )
        spec = next(iter(plan.iter_specs()))
        sql, _ = maintenance._add_statement(plan, spec)
        assert 'ADD PARTITION "P_MAX" VALUES LESS THAN (MAXVALUE)' in sql

    def test_drop_partition_statement(self):
        maintenance = _maintenance()
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
        )
        sql, params = maintenance._drop_statement(plan, "P1")
        assert sql == 'ALTER TABLE "EVENTS" DROP PARTITION "P1"'
        assert params == ()


class TestMaintenanceReports:
    """Missing / orphan / continuity / structure reports."""

    @staticmethod
    def _root(method="RANGE", subpartition="HASH"):
        return OraclePartitionInfo(
            name="EVENTS",
            parent=None,
            level=0,
            method=method,
            subpartition_method=subpartition,
            is_partitioned=True,
        )

    @staticmethod
    def _partition(
        name,
        bound="TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')",
        count=2,
    ):
        return OraclePartitionInfo(
            name=name,
            parent="EVENTS",
            level=1,
            method="RANGE",
            subpartition_method="HASH",
            bound=bound,
            subpartition_count=count,
            is_partitioned=bool(count),
        )

    def _plan(self):
        return OraclePartitionPlan(
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
                OraclePartitionBranch(name="P3", less_than=(date(2026, 4, 1),)),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="HASH", columns="ID", count=2
            ),
        )

    def _report(self, plan, infos):
        return _maintenance()._report(plan, infos, list(plan.iter_specs()), None)

    def test_missing_and_orphans(self):
        plan = self._plan()
        report = self._report(plan, (self._root(), self._partition("P1", count=3)))
        assert report.missing == (("EVENTS", "P2"), ("EVENTS", "P3"))
        # P1 matches its count override, and P1 -> P2 is contiguous.
        assert report.issues == ()

    def test_orphans_reported_for_bounded_plan(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
        )
        report = self._report(
            plan, (self._root(), self._partition("P1"), self._partition("EXTRA"))
        )
        assert report.orphans == ("EXTRA",)

    def test_subpartition_count_mismatch(self):
        plan = self._plan()
        report = self._report(plan, (self._root(), self._partition("P1", count=2)))
        assert any("subpartitions" in issue for issue in report.issues)

    def test_subpartition_strategy_mismatch(self):
        plan = self._plan()
        report = self._report(plan, (self._root(subpartition="LIST"),))
        assert any("subpartitions" in issue for issue in report.issues)

    def test_top_level_strategy_mismatch(self):
        plan = self._plan()
        report = self._report(plan, (self._root(method="LIST"),))
        assert any("plan says RANGE" in issue for issue in report.issues)

    def test_parent_missing(self):
        report = self._report(self._plan(), ())
        assert report.issues == ("EVENTS is not a partitioned table, or does not exist",)

    def test_gap_and_overlap(self):
        maintenance = _maintenance()
        specs = [
            _spec("P1", date(2026, 1, 1), date(2026, 2, 1)),
            _spec("P3", date(2026, 3, 1), date(2026, 4, 1)),
        ]
        issues = maintenance._continuity_issues(specs)
        assert issues == ("gap in EVENTS between P1 and P3",)

        specs = [
            _spec("P1", date(2026, 1, 1), date(2026, 3, 1)),
            _spec("P2", date(2026, 2, 1), date(2026, 4, 1)),
        ]
        issues = maintenance._continuity_issues(specs)
        assert issues == ("overlap in EVENTS between P1 and P2",)

    def test_maxvalue_blocks_add(self):
        maintenance = _maintenance()
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
                OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
            ),
        )
        infos = (
            self._root(),
            self._partition("P1"),
            self._partition("P_MAX", bound="MAXVALUE"),
        )
        report = maintenance._report(plan, infos, list(plan.iter_specs()), None)
        assert any("MAXVALUE partition P_MAX" in issue for issue in report.issues)

    def test_drop_candidates_respect_cutoff(self):
        maintenance = _maintenance()
        plan = self._plan()
        infos = (
            self._root(),
            self._partition("P1"),
            self._partition("P2"),
            self._partition("P3"),
        )
        candidates = maintenance._drop_candidates(
            plan, date(2026, 3, 1), infos, None
        )
        assert [spec.name for spec in candidates] == ["P1", "P2"]


class TestBoundaryComparison:
    """Actual HIGH_VALUE is compared with the plan, never assumed."""

    FEB_1 = "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
    FEB_15 = "TO_DATE(' 2026-02-15 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
    MAR_1 = "TO_DATE(' 2026-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"

    @staticmethod
    def _root(method="RANGE"):
        return OraclePartitionInfo(
            name="EVENTS",
            parent=None,
            level=0,
            method=method,
            is_partitioned=True,
        )

    @staticmethod
    def _partition(name, bound, method="RANGE"):
        return OraclePartitionInfo(
            name=name,
            parent="EVENTS",
            level=1,
            method=method,
            bound=bound,
        )

    def _report(self, plan, infos):
        return _maintenance()._report(plan, infos, list(plan.iter_specs()), None)

    def test_name_right_bound_wrong_is_a_boundary_mismatch(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
            ),
        )
        report = self._report(
            plan, (self._root(), self._partition("P1", self.FEB_15))
        )
        assert any("boundary mismatch" in issue for issue in report.issues)

    def test_bound_right_name_wrong_is_a_boundary_mismatch(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
            ),
        )
        report = self._report(
            plan, (self._root(), self._partition("P2", self.FEB_1))
        )
        assert any("boundary mismatch" in issue for issue in report.issues)
        assert any("P2" in issue for issue in report.issues)

    def test_unparseable_high_value_is_never_a_silent_pass(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
            ),
        )
        report = self._report(
            plan,
            (self._root(), self._partition("P1", "SYS_OP_COMBINED_HASH(1, 2)")),
        )
        assert any("unparsed bound" in issue for issue in report.issues)

    def test_multi_value_list_matches_its_value_set(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            branches=(
                OraclePartitionBranch(name="P_E", in_values=("EAST", "NORTH")),
            ),
        )
        report = self._report(
            plan,
            (
                self._root("LIST"),
                self._partition("P_E", "'EAST', 'NORTH'", method="LIST"),
            ),
        )
        assert not any("boundary mismatch" in issue for issue in report.issues)

    def test_multi_value_list_missing_member_is_a_boundary_mismatch(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            branches=(
                OraclePartitionBranch(name="P_E", in_values=("EAST", "NORTH")),
            ),
        )
        report = self._report(
            plan,
            (self._root("LIST"), self._partition("P_E", "'EAST'", method="LIST")),
        )
        assert any("boundary mismatch" in issue for issue in report.issues)

    def test_maxvalue_matches_the_plan(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
                OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
            ),
        )
        report = self._report(
            plan,
            (
                self._root(),
                self._partition("P1", self.FEB_1),
                self._partition("P_MAX", "MAXVALUE"),
            ),
        )
        assert not any("boundary mismatch" in issue for issue in report.issues)

    def test_maxvalue_against_a_finite_plan_is_a_boundary_mismatch(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P_MAX", less_than=("MAXVALUE",)),
            ),
        )
        report = self._report(
            plan, (self._root(), self._partition("P_MAX", self.FEB_1))
        )
        assert any("boundary mismatch" in issue for issue in report.issues)

    def test_parse_rows_keeps_high_value_and_parsed_range(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),
            ),
        )
        infos = _maintenance()._parse_rows(
            plan,
            (
                {
                    "table_name": "EVENTS",
                    "owner": "APP",
                    "partitioning_type": "RANGE",
                    "subpartitioning_type": "NONE",
                    "def_subpartition_count": 0,
                },
            ),
            (
                {
                    "partition_name": "P1",
                    "subpartition_count": 0,
                    "high_value": self.FEB_1,
                    "partition_position": 1,
                },
                {
                    "partition_name": "P2",
                    "subpartition_count": 0,
                    "high_value": self.MAR_1,
                    "partition_position": 2,
                },
            ),
            (),
        )
        first = infos[1]
        assert first.high_value == self.FEB_1
        assert first.extras["high_value"] == self.FEB_1
        assert first.range_end == date(2026, 2, 1)
        assert first.range_start is None
        second = infos[2]
        assert second.range_start == date(2026, 2, 1)
        assert second.range_end == date(2026, 3, 1)

    def test_parse_rows_keeps_a_multi_value_list(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            branches=(
                OraclePartitionBranch(name="P_E", in_values=("EAST", "NORTH")),
            ),
        )
        infos = _maintenance()._parse_rows(
            plan,
            (
                {
                    "table_name": "REGIONS",
                    "owner": "APP",
                    "partitioning_type": "LIST",
                    "subpartitioning_type": "NONE",
                    "def_subpartition_count": 0,
                },
            ),
            (
                {
                    "partition_name": "P_E",
                    "subpartition_count": 0,
                    "high_value": "'EAST', 'NORTH'",
                    "partition_position": 1,
                },
            ),
            (),
        )
        assert infos[1].extras["high_value"] == "'EAST', 'NORTH'"
        assert infos[1].list_values == ("EAST", "NORTH")

    def test_parser_marks_unknown_expressions_unparsed(self):
        parsed = parse_high_value("INTERVAL '1' DAY", "RANGE")
        assert parsed.parsed is False
        assert parsed.error is not None

    def test_parser_accepts_the_nls_parameter_oracle_appends(self):
        parsed = parse_high_value(
            "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', "
            "'NLS_CALENDAR=GREGORIAN')",
            "RANGE",
        )
        assert parsed.parsed is True
        assert str(parsed.values[0]).startswith("2026-02-01")


def _spec(name, start, end):
    from rhosocial.activerecord.backend.impl.oracle.partition import OraclePartitionSpec

    return OraclePartitionSpec(
        name=name,
        parent="EVENTS",
        method="RANGE",
        columns=("CREATED_AT",),
        less_than=(end,),
        range_start=start,
        range_end=end,
    )


def test_report_is_the_core_contract_envelope():
    assert issubclass(OraclePartitionReport, CorePartitionReport)

    report = OraclePartitionReport(target="EVENTS")

    assert report.target == "EVENTS"
    assert report.parent == "EVENTS"


def test_the_sync_executor_offers_every_canonical_verb():
    executor = _maintenance()

    assert isinstance(executor, PartitionInventory)
    assert isinstance(executor, PartitionEnsurer)
    assert isinstance(executor, PartitionRetention)
    assert isinstance(executor, PartitionValidator)


def test_the_async_executor_offers_every_canonical_verb():
    executor = AsyncOraclePartitionMaintenance(_StubBackend())

    assert isinstance(executor, AsyncPartitionInventory)
    assert isinstance(executor, AsyncPartitionEnsurer)
    assert isinstance(executor, AsyncPartitionRetention)
    assert isinstance(executor, AsyncPartitionValidator)
