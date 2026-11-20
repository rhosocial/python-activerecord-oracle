# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_plan.py
"""Unit tests for Oracle partition plans and bounds.

No server is required. The tests pin:

* generated RANGE / LIST / HASH plans and their half-open boundaries,
* explicit branches with per-partition subpartition definitions / counts, and
* the table-level subpartition-strategy invariant (no per-branch strategy).
"""

from datetime import date

import pytest

from rhosocial.activerecord.backend.impl.oracle.partition import (
    OracleHashPolicy,
    OracleListPolicy,
    OraclePartitionBranch,
    OraclePartitionPlan,
    OracleRangePolicy,
    OracleSubpartitionPolicy,
    OracleSubpartitionSpec,
)
from rhosocial.activerecord.backend.impl.oracle.partition.bounds import (
    add_interval,
    default_label,
    normalize_interval,
)


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
            subpartition=OracleSubpartitionPolicy(method="HASH", columns="ID", count=2),
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
                    subpartitions=(OracleSubpartitionSpec(name="P2_E", in_values=("EAST",)),),
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
                        subpartitions=(OracleSubpartitionSpec(name="P1_E", in_values=("EAST",)),),
                    ),
                ),
                subpartition=OracleSubpartitionPolicy(method="RANGE", columns="REGION"),
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
                subpartition=OracleSubpartitionPolicy(method="LIST", columns="REGION"),
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
                policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month", count=1),
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
