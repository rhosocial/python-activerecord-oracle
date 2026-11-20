# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_offline_coverage.py
"""Offline unit coverage for the Oracle partition grammar.

This file targets the branches the live-server scenarios cannot reach: the
private parsing/derivation helpers of ``high_value`` and the validation and
name/bound derivation paths of ``plan``. No Oracle instance, socket or driver
connection is used.
"""

from datetime import date, datetime, timezone

import pytest

from rhosocial.activerecord.backend.impl.oracle.partition import (
    OracleHashPolicy,
    OracleListPolicy,
    OraclePartitionBranch,
    OraclePartitionPlan,
    OraclePartitionSpec,
    OracleRangePolicy,
    OracleSubpartitionPolicy,
    OracleSubpartitionSpec,
    parse_high_value,
)
from rhosocial.activerecord.backend.impl.oracle.partition.high_value import (
    _is_midnight,
    _parse_date_text,
    _parse_list,
    _parse_range,
    _parse_scalar,
    _split_top_level,
    _unwrap_parentheses,
)
from rhosocial.activerecord.backend.impl.oracle.partition.plan import (
    _as_columns,
    _label_from_values,
    _normalise_list_groups,
    _normalise_method,
    _positive_int,
    _render_label,
    _validate_subpartition_bound,
)

FEB = "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"


def _range_plan(*names_and_bounds, subpartition=None):
    branches = tuple(
        OraclePartitionBranch(name=name, less_than=(bound,)) for name, bound in names_and_bounds
    )
    return OraclePartitionPlan(
        parent="EVENTS",
        method="RANGE",
        columns="CREATED_AT",
        branches=branches,
        subpartition=subpartition,
    )


# --------------------------------------------------------------------------- #
# high_value
# --------------------------------------------------------------------------- #
class TestParseHighValue:
    """parse_high_value accepts a deliberately small closed grammar."""

    @pytest.mark.parametrize(
        ("text", "method", "values"),
        [
            (None, "RANGE", ()),
            ("   ", "LIST", ()),
            ("", "HASH", ()),
            ("MAXVALUE", "RANGE", ("MAXVALUE",)),
            ("maxvalue", "RANGE", ("MAXVALUE",)),
            ("DEFAULT", "LIST", ("DEFAULT",)),
            ("'EAST'", "LIST", ("EAST",)),
            ("'EAST', 'NORTH'", "LIST", ("EAST", "NORTH")),
            ("(1, 'A')", "LIST", ((1, "A"),)),
            ("('A', 'B')", "RANGE", ("A", "B")),
            ("42", "RANGE", (42,)),
            ("-7", "RANGE", (-7,)),
            ("3.5", "RANGE", (3.5,)),
            (".25", "RANGE", (0.25,)),
            ("1e3", "RANGE", (1000.0,)),
            ("NULL", "LIST", (None,)),
            (FEB, "RANGE", (date(2026, 2, 1),)),
            ("DATE '2026-02-01'", "RANGE", (date(2026, 2, 1),)),
            (
                "TIMESTAMP '2026-02-01 12:34:56'",
                "RANGE",
                (datetime(2026, 2, 1, 12, 34, 56),),
            ),
            (
                "TO_TIMESTAMP('2026-02-01 12:34:56', 'YYYY-MM-DD HH24:MI:SS')",
                "RANGE",
                (datetime(2026, 2, 1, 12, 34, 56),),
            ),
        ],
    )
    def test_accepted_texts(self, text, method, values):
        parsed = parse_high_value(text, method)
        assert parsed.parsed is True
        assert parsed.error is None
        assert parsed.values == values
        assert parsed.text is text

    @pytest.mark.parametrize(
        ("text", "method", "error"),
        [
            ("HASH", "HASH", "HASH partitions carry no bound"),
            ("SYSDATE", "VIEW", "unknown partition method"),
            ("INTERVAL '1' DAY", "RANGE", "cannot parse bound token"),
        ],
    )
    def test_rejected_texts(self, text, method, error):
        parsed = parse_high_value(text, method)
        assert parsed.parsed is False
        assert parsed.values == ()
        assert error in parsed.error

    def test_oracle_appends_an_nls_argument(self):
        parsed = parse_high_value(
            "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', "
            "'NLS_CALENDAR=GREGORIAN')",
            "RANGE",
        )
        assert parsed.parsed is True
        assert parsed.values == (date(2026, 2, 1),)

    def test_to_timestamp_tz_keeps_non_midnight_time_and_zone(self):
        parsed = parse_high_value(
            "TO_TIMESTAMP_TZ('2026-02-01 12:34:56 +00:00', "
            "'YYYY-MM-DD HH24:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE=ENGLISH')",
            "RANGE",
        )
        assert parsed.parsed is True
        assert parsed.values == (datetime(2026, 2, 1, 12, 34, 56, tzinfo=timezone.utc),)

    def test_escaped_quotes_in_literal(self):
        assert parse_high_value("'EAST''WEST'", "LIST").values == ("EAST'WEST",)


class TestHighValueInternals:
    """Directly exercise the private scanner helpers."""

    def test_parse_range_single_and_grouped(self):
        assert _parse_range("42") == (42,)
        assert _parse_range("(1, 2, 3)") == (1, 2, 3)

    def test_parse_list_default_and_flat(self):
        assert _parse_list("default") == ("DEFAULT",)
        assert _parse_list("'A','B'") == ("A", "B")

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("MAXVALUE", "MAXVALUE"),
            ("default", "DEFAULT"),
            ("null", None),
            ("'A'", "A"),
            ("'A''B'", "A'B"),
            ("0007", 7),
            ("-2.50", -2.5),
        ],
    )
    def test_parse_scalar(self, text, expected):
        assert _parse_scalar(text) == expected

    def test_parse_scalar_rejects_empty_and_unknown(self):
        with pytest.raises(ValueError, match="empty value"):
            _parse_scalar("   ")
        with pytest.raises(ValueError, match="cannot parse"):
            _parse_scalar("SYSTIMESTAMP")

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("2026-02-01", date(2026, 2, 1)),
            (" 2026-02-01 00:00:00 ", date(2026, 2, 1)),
            ("2026-02-01 12:30", datetime(2026, 2, 1, 12, 30)),
            ("2026-02-01 12:30:45.123456", datetime(2026, 2, 1, 12, 30, 45, 123456)),
            ("2026-02-01 12:30:45 +0000", datetime(2026, 2, 1, 12, 30, 45, tzinfo=timezone.utc)),
        ],
    )
    def test_parse_date_text_formats(self, text, expected):
        assert _parse_date_text(text) == expected

    def test_parse_date_text_rejects_unknown_format(self):
        with pytest.raises(ValueError, match="unknown date bound"):
            _parse_date_text("not-a-date")

    def test_is_midnight(self):
        assert _is_midnight(datetime(2026, 2, 1)) is True
        assert _is_midnight(datetime(2026, 2, 1, 0, 0, 0, 1)) is False
        assert _is_midnight(datetime(2026, 2, 1, 1)) is False

    def test_split_top_level_respects_quotes_and_parentheses(self):
        assert _split_top_level("'A,B', C") == ("'A,B'", " C")
        assert _split_top_level("(1, 2), (3, 4)") == ("(1, 2)", " (3, 4)")
        assert _split_top_level("'A''B, C', D") == ("'A''B, C'", " D")
        assert _split_top_level("") == ()

    def test_unwrap_parentheses(self):
        assert _unwrap_parentheses("('A', 'B')") == ("'A', 'B'", True)
        assert _unwrap_parentheses("  42  ") == ("42", False)
        assert _unwrap_parentheses("('A'), ('B')") == ("('A'), ('B')", False)
        with pytest.raises(ValueError, match="unbalanced"):
            _unwrap_parentheses("(('A')")


# --------------------------------------------------------------------------- #
# plan helpers and validation
# --------------------------------------------------------------------------- #
class TestPlanHelpers:
    def test_as_columns_accepts_string_and_sequence(self):
        assert _as_columns("A") == ("A",)
        assert _as_columns(["A", "B"]) == ("A", "B")

    @pytest.mark.parametrize("value", [None, 7, (), [], ["A", " "]])
    def test_as_columns_rejects_bad_declarations(self, value):
        with pytest.raises((TypeError, ValueError)):
            _as_columns(value)

    def test_normalise_method(self):
        assert _normalise_method(" range ") == "RANGE"
        with pytest.raises(ValueError, match="method must be one of"):
            _normalise_method("VIEW")

    @pytest.mark.parametrize("value", [True, 1.5, "3"])
    def test_positive_int_rejects_non_int(self, value):
        with pytest.raises(TypeError, match="must be an integer"):
            _positive_int(value, "count")

    @pytest.mark.parametrize("value", [0, -1])
    def test_positive_int_rejects_non_positive(self, value):
        with pytest.raises(ValueError, match="positive integer"):
            _positive_int(value, "count")

    def test_label_from_values(self):
        assert _label_from_values([2026, 2]) == "2026_2"
        assert _label_from_values(["A/B"]) == "A_B"
        assert _label_from_values([]) == "value"

    def test_render_label_fields(self):
        rendered = _render_label(
            "{year}q{quarter}_{month}_{day}_{start}_{end}", date(2026, 4, 15), date(2026, 7, 15)
        )
        assert rendered == "2026q2_4_15_2026-04-15_2026-07-15"

    def test_normalise_list_groups(self):
        assert _normalise_list_groups(("A", "B")) == (("A", "B"),)
        assert _normalise_list_groups((("A", 1), ("B", 2))) == (("A", 1), ("B", 2))
        with pytest.raises(ValueError, match="not a mix"):
            _normalise_list_groups(("A", ("B",)))

    def test_validate_subpartition_bound_by_method(self):
        range_spec = OracleSubpartitionSpec(name="R", less_than=(1,))
        list_spec = OracleSubpartitionSpec(name="L", in_values=("A",))
        hash_spec = OracleSubpartitionSpec(name="H")
        _validate_subpartition_bound(range_spec, "RANGE")
        _validate_subpartition_bound(list_spec, "LIST")
        _validate_subpartition_bound(hash_spec, "HASH")
        with pytest.raises(ValueError, match="RANGE"):
            _validate_subpartition_bound(list_spec, "RANGE")
        with pytest.raises(ValueError, match="LIST"):
            _validate_subpartition_bound(range_spec, "LIST")
        with pytest.raises(ValueError, match="HASH"):
            _validate_subpartition_bound(range_spec, "HASH")


class TestPolicyValidation:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"columns": ("A", "B"), "start": date(2026, 1, 1)},
            {"start": date(2026, 1, 1), "bounds": ((date(2026, 1, 1), date(2026, 2, 1)),)},
            {"bounds": ()},
            {"bounds": ((date(2026, 1, 1),),)},
            {"bounds": ((1, date(2026, 2, 1)),)},
            {"bounds": ((date(2026, 2, 1), date(2026, 1, 1)),)},
            {},
            {"start": 1},
            {"start": date(2026, 1, 1), "count": 0},
            {"start": date(2026, 1, 1), "maxvalue": "yes"},
        ],
    )
    def test_range_policy_rejects_invalid_shapes(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            OracleRangePolicy(**kwargs)

    def test_range_policy_bounds_iteration_and_labels(self):
        policy = OracleRangePolicy(
            bounds=((date(2026, 1, 1), date(2026, 2, 1)),), label_pattern="{year}_{month}"
        )
        assert list(policy.iter_bounds()) == [(date(2026, 1, 1), date(2026, 2, 1))]
        assert policy.label_for(date(2026, 1, 1), date(2026, 2, 1)) == "2026_1"
        assert (
            OracleRangePolicy(bounds=((date(2026, 1, 1), date(2026, 2, 1)),)).label_for(
                date(2026, 1, 1), date(2026, 2, 1)
            )
            == "2026_01_01"
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"groups": ()},
            {"groups": ((),)},
            {"groups": (("A",),), "labels": ("a", "b")},
            {"groups": (("A",),), "labels": ("",)},
        ],
    )
    def test_list_policy_rejects_invalid_shapes(self, kwargs):
        with pytest.raises(ValueError):
            OracleListPolicy(**kwargs)

    def test_list_policy_default_labels(self):
        policy = OracleListPolicy(groups=(("A/B",), ("C",)))
        assert [label for _values, label in policy.iter_groups()] == ["A_B", "C"]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"modulus": 0},
            {"modulus": True},
            {"modulus": 2.5},
            {"modulus": 2, "remainders": ()},
            {"modulus": 2, "remainders": (-1,)},
            {"modulus": 2, "remainders": (2,)},
            {"modulus": 2, "remainders": (0, 0)},
            {"modulus": 2, "remainders": ("0",)},
        ],
    )
    def test_hash_policy_rejects_invalid_shapes(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            OracleHashPolicy(**kwargs)

    def test_hash_policy_remainders(self):
        assert list(OracleHashPolicy(modulus=3).iter_remainders()) == [0, 1, 2]
        assert list(OracleHashPolicy(modulus=3, remainders=(2, 0)).iter_remainders()) == [2, 0]

    def test_subpartition_spec_validation(self):
        with pytest.raises(ValueError, match="non-empty"):
            OracleSubpartitionSpec(name=" ")
        with pytest.raises(ValueError, match="mutually exclusive"):
            OracleSubpartitionSpec(name="S", less_than=(1,), in_values=("A",))
        with pytest.raises(ValueError, match="must not be empty"):
            OracleSubpartitionSpec(name="S", less_than=())
        with pytest.raises(ValueError, match="must not be empty"):
            OracleSubpartitionSpec(name="S", in_values=())

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"method": "VIEW", "columns": "A"},
            {"method": "HASH", "columns": "A", "count": 0},
            {"method": "LIST", "columns": "A", "count": 2},
            {
                "method": "HASH",
                "columns": "A",
                "count": 2,
                "templates": (OracleSubpartitionSpec("S"),),
            },
            {"method": "RANGE", "columns": "A", "templates": (OracleSubpartitionSpec("S"),)},
        ],
    )
    def test_subpartition_policy_rejects_invalid_shapes(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            OracleSubpartitionPolicy(**kwargs)

    def test_subpartition_policy_normalises_columns(self):
        policy = OracleSubpartitionPolicy(method="list", columns="REGION")
        assert policy.method == "LIST"
        assert policy.columns == ("REGION",)


class TestPlanDerivation:
    @pytest.mark.parametrize(
        ("branch", "expected"),
        [
            (OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)), "P1"),
            (OraclePartitionBranch(label="Q1", less_than=(date(2026, 2, 1),)), "EVENTS_Q1"),
            (OraclePartitionBranch(less_than=(date(2026, 2, 1),)), "EVENTS_2026_02_01"),
            (OraclePartitionBranch(less_than=("MAXVALUE",)), "EVENTS_maxvalue"),
            (OraclePartitionBranch(less_than=(7,)), "EVENTS_lt_7"),
        ],
    )
    def test_branch_name_forms(self, branch, expected):
        plan = _range_plan(("P0", date(2026, 1, 1)))
        assert plan.branch_name(branch) == expected

    def test_branch_name_from_in_values(self):
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns="REGION",
            branches=(OraclePartitionBranch(in_values=("EAST",)),),
        )
        assert plan.branch_name(plan.branches[0]) == "REGIONS_EAST"

    def test_branch_name_falls_back_to_partition(self):
        plan = OraclePartitionPlan(
            parent="BUCKETS",
            method="HASH",
            columns="ID",
            branches=(OraclePartitionBranch(),),
        )
        assert plan.branch_name(plan.branches[0]) == "BUCKETS_partition"

    def test_branch_spec_leaves_range_fields_for_maxvalue(self):
        plan = _range_plan(("P_MAX", "MAXVALUE"), ("P1", date(2026, 2, 1)))
        specs = list(plan.iter_specs())
        assert specs[0].range_start is None and specs[0].range_end is None
        assert specs[1].range_end == date(2026, 2, 1)

    def test_spec_is_maxvalue(self):
        spec = OraclePartitionSpec(
            name="P",
            parent="EVENTS",
            method="RANGE",
            columns=("CREATED_AT",),
            less_than=("MAXVALUE",),
        )
        assert spec.is_maxvalue is True

    def test_expected_subpartition_count_precedence(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1", less_than=(date(2026, 2, 1),), subpartitions_count=5
                ),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
            ),
            subpartition=OracleSubpartitionPolicy(method="HASH", columns="ID", count=2),
        )
        specs = list(plan.iter_specs())
        assert [plan.expected_subpartition_count(s) for s in specs] == [5, 2]

    def test_explicit_subpartitions_win_over_template(self):
        subpartitions = (OracleSubpartitionSpec("SP1", in_values=("A",)),)
        templates = (
            OracleSubpartitionSpec("T1", in_values=("A",)),
            OracleSubpartitionSpec("T2", in_values=("B",)),
        )
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1", less_than=(date(2026, 2, 1),), subpartitions=subpartitions
                ),
                OraclePartitionBranch(name="P2", less_than=(date(2026, 3, 1),)),
            ),
            subpartition=OracleSubpartitionPolicy(
                method="LIST", columns="REGION", templates=templates
            ),
        )
        specs = list(plan.iter_specs())
        assert [plan.expected_subpartition_count(s) for s in specs] == [1, 2]

    def test_expected_subpartition_count_uses_templates(self):
        templates = (
            OracleSubpartitionSpec("SP1", in_values=("A",)),
            OracleSubpartitionSpec("SP2", in_values=("B",)),
        )
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(OraclePartitionBranch(name="P1", less_than=(date(2026, 2, 1),)),),
            subpartition=OracleSubpartitionPolicy(
                method="LIST", columns="REGION", templates=templates
            ),
        )
        assert plan.expected_subpartition_count(next(iter(plan.iter_specs()))) == 2
        assert (
            plan.expected_subpartition_count(
                OraclePartitionSpec("X", "EVENTS", "RANGE", ("CREATED_AT",))
            )
            == 2
        )

    def test_expected_subpartition_count_without_strategy(self):
        plan = _range_plan(("P1", date(2026, 2, 1)))
        assert plan.expected_subpartition_count(next(iter(plan.iter_specs()))) is None

    @pytest.mark.parametrize(
        ("plan", "through", "expected"),
        [
            (_range_plan(("P1", date(2026, 2, 1))), None, True),
            (
                OraclePartitionPlan(
                    parent="E",
                    method="RANGE",
                    columns="T",
                    policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month", count=2),
                ),
                None,
                True,
            ),
            (
                OraclePartitionPlan(
                    parent="E",
                    method="RANGE",
                    columns="T",
                    policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
                ),
                None,
                False,
            ),
            (
                OraclePartitionPlan(
                    parent="E",
                    method="RANGE",
                    columns="T",
                    policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
                ),
                date(2026, 3, 1),
                True,
            ),
            (
                OraclePartitionPlan(
                    parent="R",
                    method="LIST",
                    columns="C",
                    policy=OracleListPolicy(groups=(("A",),)),
                ),
                None,
                True,
            ),
        ],
    )
    def test_is_fully_bounded(self, plan, through, expected):
        assert plan.is_fully_bounded(through=through) is expected

    @pytest.mark.parametrize(
        "kwargs",
        [
            {
                "parent": " ",
                "method": "HASH",
                "columns": "ID",
                "branches": (OraclePartitionBranch(),),
            },
            {"parent": "E", "method": "HASH", "columns": "ID", "branches": (), "name_pattern": ""},
            {"parent": "E", "method": "HASH", "columns": "ID", "branches": (), "schema": " "},
            {
                "parent": "E",
                "method": "RANGE",
                "columns": "T",
                "branches": (),
                "policy": OracleHashPolicy(),
            },
            {"parent": "E", "method": "HASH", "columns": "ID", "branches": ()},
        ],
    )
    def test_plan_rejects_invalid_shapes(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            OraclePartitionPlan(**kwargs)

    def test_plan_rejects_non_branch_entries(self):
        with pytest.raises(TypeError, match="OraclePartitionBranch"):
            OraclePartitionPlan(parent="E", method="HASH", columns="ID", branches=("P1",))
