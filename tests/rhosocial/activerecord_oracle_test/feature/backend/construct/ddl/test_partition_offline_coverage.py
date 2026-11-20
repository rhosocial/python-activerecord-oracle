# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_offline_coverage.py
"""Offline unit coverage for the Oracle partition modules.

This file targets the branches the live-server scenarios cannot reach: the
private parsing/derivation helpers of ``high_value``, the validation and
name/bound derivation paths of ``plan``, and every pure helper plus both the
sync and async I/O seams of ``maintenance``.

No Oracle instance, socket or driver connection is used. The maintenance
executors are driven through their documented I/O seams (``_fetch_rows`` /
``_execute`` / ``_infos``) with MagicMock / AsyncMock, and a real connectionless
:class:`OracleDialect` renders DDL.
"""

import re
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from rhosocial.activerecord.backend.errors import DatabaseError

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionMaxValue,
    OraclePartitionValue,
)
from rhosocial.activerecord.backend.impl.oracle.partition import (
    AsyncOraclePartitionMaintenance,
    OracleHashPolicy,
    OracleListPolicy,
    OraclePartitionBranch,
    OraclePartitionInfo,
    OraclePartitionMaintenance,
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
from rhosocial.activerecord.backend.impl.oracle.partition.maintenance import (
    _bound_equal,
    _bound_extras,
    _boundary,
    _canonical,
    _canonical_set,
    _display_values,
    _expected_list_values,
    _is_maxvalue,
    _optional_int,
    _subpartition_names,
    _unordered_equal,
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
MAR = "TO_DATE(' 2026-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"


def _dialect():
    return OracleDialect(version=(23, 1, 0))


def _backend():
    """A MagicMock backend carrying only a real, connectionless dialect."""
    backend = MagicMock()
    backend.dialect = _dialect()
    return backend


def _sync_maintenance():
    return OraclePartitionMaintenance(_backend())


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


def _root_row(method="RANGE", subpartition="NONE", count=0):
    return {
        "owner": "APP",
        "table_name": "EVENTS",
        "partitioning_type": method,
        "subpartitioning_type": subpartition,
        "partition_count": 2,
        "def_subpartition_count": count,
    }


def _part_row(name, high, pos, count=0):
    return {
        "partition_name": name,
        "subpartition_count": count,
        "composite": "YES" if count else "NO",
        "high_value": high,
        "partition_position": pos,
    }


def _sub_row(part, name, high, pos):
    return {
        "partition_name": part,
        "subpartition_name": name,
        "high_value": high,
        "subpartition_position": pos,
    }


class _ScriptedDb:
    """Stateful stand-in for the dictionary and DDL I/O seams."""

    def __init__(self, root_rows=None, part_rows=None, sub_rows=None):
        self.root_rows = list(root_rows or [])
        self.part_rows = list(part_rows or [])
        self.sub_rows = list(sub_rows or [])
        self.executed = []
        self.fail_sql = None
        self.created_bounds = {}

    def fetch(self, sql, params):
        if "ALL_TAB_SUBPARTITIONS" in sql:
            return list(self.sub_rows)
        if "ALL_TAB_PARTITIONS" in sql:
            return list(self.part_rows)
        if "ALL_PART_TABLES" in sql:
            return list(self.root_rows)
        return []

    def execute(self, sql, params):
        self.executed.append(sql)
        if self.fail_sql and self.fail_sql(sql):
            raise DatabaseError("ORA-14074: simulated DDL failure")
        added = re.search(r'ADD PARTITION "([^"]+)"', sql)
        if added:
            name = added.group(1)
            bound = self.created_bounds.get(name, FEB)
            self.part_rows.append(_part_row(name, bound, len(self.part_rows) + 1))
        dropped = re.search(r'DROP PARTITION "([^"]+)"', sql)
        if dropped:
            name = dropped.group(1).upper()
            self.part_rows = [
                row for row in self.part_rows if row["partition_name"].upper() != name
            ]

    def wire(self, executor):
        """Attach this script to either the sync or the async executor."""
        is_async = isinstance(executor, AsyncOraclePartitionMaintenance)
        fetch = AsyncMock(side_effect=self.fetch) if is_async else MagicMock(side_effect=self.fetch)
        execute = (
            AsyncMock(side_effect=self.execute) if is_async else MagicMock(side_effect=self.execute)
        )
        executor._fetch_rows = fetch
        executor._execute = execute
        return executor


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


# --------------------------------------------------------------------------- #
# maintenance pure helpers
# --------------------------------------------------------------------------- #
class TestMaintenancePureHelpers:
    def test_bound_extras_keeps_raw_and_parses_range(self):
        extras = _bound_extras("RANGE", FEB)
        assert extras["high_value"] == FEB
        assert extras["range_end"] == date(2026, 2, 1)
        assert "range_start" not in extras

    def test_bound_extras_includes_start_when_available(self):
        extras = _bound_extras("RANGE", FEB, date(2026, 1, 1))
        assert extras["range_start"] == date(2026, 1, 1)

    def test_bound_extras_for_list_records_values(self):
        extras = _bound_extras("LIST", "'EAST', 'WEST'")
        assert extras["values"] == ("EAST", "WEST")

    def test_bound_extras_for_unparsed_text(self):
        extras = _bound_extras("RANGE", "INTERVAL '1' DAY")
        assert extras["high_value"] == "INTERVAL '1' DAY"
        assert "unparsed" in extras
        assert "range_end" not in extras

    def test_bound_extras_ignores_other_methods_and_none(self):
        assert _bound_extras("HASH", "HASH") == {"high_value": "HASH"}
        assert _bound_extras(None, None) == {}
        assert _bound_extras("RANGE", None, date(2026, 1, 1)) == {}

    def test_bound_extras_multi_value_range(self):
        extras = _bound_extras("RANGE", "(1, 2)")
        assert extras["range_end"] == (1, 2)

    def test_expected_list_values_single_and_multi_column(self):
        single = OraclePartitionSpec(
            "P", "EVENTS", "LIST", ("REGION",), in_values=(("EAST",), ("WEST",))
        )
        multi = OraclePartitionSpec(
            "P", "EVENTS", "LIST", ("COUNTRY", "REGION"), in_values=(("US", "EAST"),)
        )
        assert _expected_list_values(single) == ("EAST", "WEST")
        assert _expected_list_values(multi) == (("US", "EAST"),)

    def test_canonical_normalises_midnight_datetime(self):
        assert _canonical(datetime(2026, 2, 1)) == date(2026, 2, 1)
        assert _canonical(datetime(2026, 2, 1, 12, 30)) == datetime(2026, 2, 1, 12, 30)

    def test_canonical_recurses_into_tuples(self):
        assert _canonical((datetime(2026, 2, 1), 7)) == (date(2026, 2, 1), 7)

    @pytest.mark.parametrize(
        ("actual", "expected", "result"),
        [
            ((date(2026, 2, 1),), (datetime(2026, 2, 1),), True),
            ((1, 2), (1,), False),
            ((1, 2), (1, 3), False),
            (((1, 2),), ((1, 2),), True),
        ],
    )
    def test_bound_equal(self, actual, expected, result):
        assert _bound_equal(actual, expected) is result

    def test_unordered_equal_and_canonical_set_fallback(self):
        assert _unordered_equal(("A", "B"), ("B", "A")) is True
        assert _unordered_equal((date(2026, 2, 1),), (datetime(2026, 2, 1),)) is True
        assert _unordered_equal(("A",), ("A", "B")) is False
        assert _canonical_set([[1], [2]]) == ([1], [2])

    def test_display_values(self):
        assert _display_values(("A", 1)) == "('A', 1)"

    def test_optional_int(self):
        assert _optional_int(None) is None
        assert _optional_int("3") == 3

    def test_boundary_expression_factory(self):
        assert isinstance(_boundary(_dialect(), "maxvalue"), OraclePartitionMaxValue)
        assert isinstance(_boundary(_dialect(), "MAXVALUE"), OraclePartitionMaxValue)
        value = OraclePartitionValue(_dialect(), 1)
        assert _boundary(_dialect(), value) is value
        assert isinstance(_boundary(_dialect(), 7), OraclePartitionValue)

    def test_subpartition_names_group_by_parent(self):
        infos = [
            OraclePartitionInfo("SP1", "P1", 2, is_partitioned=False),
            OraclePartitionInfo("SP2", "P1", 2, is_partitioned=False),
            OraclePartitionInfo("SP3", "p1", 2, is_partitioned=False),
            OraclePartitionInfo("P1", "EVENTS", 1, is_partitioned=True),
            OraclePartitionInfo("ORPHAN", None, 2, is_partitioned=False),
        ]
        assert _subpartition_names(infos) == {"P1": ("SP1", "SP2", "SP3")}

    def test_is_maxvalue(self):
        assert _is_maxvalue(OraclePartitionInfo("P_MAX", "E", 1, bound=" maxvalue ")) is True
        assert _is_maxvalue(OraclePartitionInfo("P1", "E", 1, bound=FEB)) is False
        assert _is_maxvalue(OraclePartitionInfo("P_MAX", "E", 0, bound="MAXVALUE")) is False


# --------------------------------------------------------------------------- #
# maintenance shared base helpers
# --------------------------------------------------------------------------- #
class TestMaintenanceBaseHelpers:
    def test_dictionary_queries_include_schema_or_current_schema(self):
        maintenance = _sync_maintenance()
        with_schema, _ = maintenance._dictionary_query(
            "ALL_PART_TABLES", "OWNER", ("OWNER",), "app", "events"
        ).to_sql()
        without_schema, _ = maintenance._dictionary_query(
            "ALL_PART_TABLES", "OWNER", ("OWNER",), None, "events"
        ).to_sql()
        assert "ALL_PART_TABLES" in with_schema
        assert "ALL_PART_TABLES" in without_schema
        assert "SYS_CONTEXT" in without_schema

    def test_query_builders_select_expected_views(self):
        maintenance = _sync_maintenance()
        plan = _range_plan(("P1", date(2026, 2, 1)))
        assert "ALL_PART_TABLES" in maintenance._root_query(plan).to_sql()[0]
        assert "ALL_TAB_PARTITIONS" in maintenance._partition_query(plan).to_sql()[0]
        assert "ALL_TAB_SUBPARTITIONS" in maintenance._subpartition_query(plan).to_sql()[0]
        assert "EVENTS" in maintenance._table(plan).name

    def test_parse_rows_empty_root(self):
        plan = _range_plan(("P1", date(2026, 2, 1)))
        assert _sync_maintenance()._parse_rows(plan, [], [], []) == ()

    def test_parse_rows_sorts_and_derives_ranges(self):
        plan = _range_plan(("P1", date(2026, 2, 1)))
        infos = _sync_maintenance()._parse_rows(
            plan,
            [_root_row()],
            [_part_row("P2", MAR, 2), _part_row("P1", FEB, 1)],
            [],
        )
        assert [info.name for info in infos] == ["EVENTS", "P1", "P2"]
        assert infos[0].subpartition_method is None
        assert infos[1].range_end == date(2026, 2, 1)
        assert infos[2].range_start == date(2026, 2, 1)

    def test_parse_rows_includes_subpartitions(self):
        plan = _range_plan(("P1", date(2026, 2, 1)))
        infos = _sync_maintenance()._parse_rows(
            plan,
            [_root_row(subpartition="LIST")],
            [_part_row("P1", FEB, 1, count=2)],
            [_sub_row("P1", "SP_W", "'WEST'", 2), _sub_row("P1", "SP_E", "'EAST'", 1)],
        )
        assert [info.name for info in infos] == ["EVENTS", "P1", "SP_E", "SP_W"]
        assert infos[0].subpartition_method == "LIST"
        assert infos[1].is_partitioned is True
        assert infos[2].level == 2
        assert infos[2].list_values == ("EAST",)

    def test_expected_and_safe_expected(self):
        maintenance = _sync_maintenance()
        bounded = _range_plan(("P1", date(2026, 2, 1)))
        unbounded = OraclePartitionPlan(
            parent="E",
            method="RANGE",
            columns="T",
            policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
        )
        assert len(maintenance._expected(bounded, None)) == 1
        assert maintenance._safe_expected(unbounded, None) == ()
        assert len(maintenance._safe_expected(unbounded, date(2026, 3, 1))) == 2
        with pytest.raises(ValueError, match="count or through"):
            maintenance._expected(unbounded, None)

    def test_missing_needs_create_and_orphans(self):
        maintenance = _sync_maintenance()
        plan = _range_plan(("P1", date(2026, 2, 1)), ("P2", date(2026, 3, 1)))
        specs = list(plan.iter_specs())
        actual = maintenance._actual_names(
            [
                OraclePartitionInfo("P1", "EVENTS", 1, bound=FEB),
                OraclePartitionInfo("EXTRA", "EVENTS", 1),
            ]
        )
        assert "P1" in actual and "EXTRA" in actual
        assert maintenance._missing(specs, actual) == (("EVENTS", "P2"),)
        assert maintenance._needs_create(specs[0], actual) is False
        assert maintenance._needs_create(specs[1], actual) is True
        assert maintenance._orphans(plan, specs, actual, None) == ("EXTRA",)
        assert (
            maintenance._orphans(
                OraclePartitionPlan(
                    parent="E",
                    method="RANGE",
                    columns="T",
                    policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
                ),
                specs,
                actual,
                None,
            )
            == ()
        )

    def test_range_and_list_bound_issues(self):
        maintenance = _sync_maintenance()
        range_spec = next(iter(_range_plan(("P1", date(2026, 2, 1))).iter_specs()))
        assert (
            maintenance._range_bound_issues(
                range_spec, OraclePartitionInfo("P1", "E", 1, bound=FEB)
            )
            == ()
        )
        assert maintenance._range_bound_issues(
            range_spec, OraclePartitionInfo("P1", "E", 1, bound="")
        )[0].startswith("partition P1 bound")
        assert (
            maintenance._range_bound_issues(
                range_spec, OraclePartitionInfo("P1", "E", 1, bound="INTERVAL '1' DAY")
            )[0].find("unparsed bound")
            >= 0
        )
        assert (
            maintenance._range_bound_issues(
                range_spec, OraclePartitionInfo("P1", "E", 1, bound=MAR)
            )[0].find("boundary mismatch")
            >= 0
        )
        assert (
            maintenance._range_bound_issues(range_spec, OraclePartitionInfo("P1", "E", 1))[0].find(
                "unparsed bound"
            )
            >= 0
        )

        list_spec = next(
            iter(
                OraclePartitionPlan(
                    parent="R",
                    method="LIST",
                    columns="C",
                    branches=(OraclePartitionBranch(name="P_E", in_values=("EAST", "WEST")),),
                ).iter_specs()
            )
        )
        assert (
            maintenance._list_bound_issues(
                list_spec, OraclePartitionInfo("P_E", "R", 1, bound="'EAST', 'WEST'")
            )
            == ()
        )
        assert (
            maintenance._list_bound_issues(
                list_spec, OraclePartitionInfo("P_E", "R", 1, bound="'EAST'")
            )[0].find("boundary mismatch")
            >= 0
        )
        assert (
            maintenance._list_bound_issues(
                list_spec, OraclePartitionInfo("P_E", "R", 1, bound="SYSDATE")
            )[0].find("unparsed bound")
            >= 0
        )
        assert (
            maintenance._list_bound_issues(
                list_spec, OraclePartitionInfo("P_E", "R", 1, bound=None)
            )[0].find("unparsed bound")
            >= 0
        )
        assert (
            maintenance._bound_issues(
                OraclePartitionSpec("P", "E", "HASH", ("ID",)), OraclePartitionInfo("P", "E", 1)
            )
            == ()
        )

    def test_bound_name_conflict(self):
        maintenance = _sync_maintenance()
        spec = next(iter(_range_plan(("P1", date(2026, 2, 1))).iter_specs()))
        conflict = OraclePartitionInfo("PX", "EVENTS", 1, bound=FEB)
        assert maintenance._bound_name_conflict(spec, {"PX": conflict}, ("P1",)) is conflict
        assert maintenance._bound_name_conflict(spec, {"P1": conflict}, ("P1",)) is None
        assert (
            maintenance._bound_name_conflict(
                OraclePartitionSpec("P", "E", "HASH", ("ID",)), {"PX": conflict}, ()
            )
            is None
        )

    def test_structure_issues_missing_subpartitions_and_hash_count(self):
        maintenance = _sync_maintenance()
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            branches=(
                OraclePartitionBranch(
                    name="P1",
                    less_than=(date(2026, 2, 1),),
                    subpartitions=(
                        OracleSubpartitionSpec("SP1", in_values=("A",)),
                        OracleSubpartitionSpec("SP2", in_values=("B",)),
                    ),
                ),
            ),
            subpartition=OracleSubpartitionPolicy(method="LIST", columns="REGION"),
        )
        specs = list(plan.iter_specs())
        infos = [
            OraclePartitionInfo(
                "EVENTS", None, 0, method="RANGE", subpartition_method="LIST", is_partitioned=True
            ),
            OraclePartitionInfo(
                "P1", "EVENTS", 1, method="RANGE", subpartition_count=2, is_partitioned=True
            ),
            OraclePartitionInfo("SP1", "P1", 2, method="LIST"),
        ]
        assert any(
            "missing subpartitions SP2" in issue
            for issue in maintenance._structure_issues(plan, infos, specs)
        )

        hash_plan = OraclePartitionPlan(
            parent="EVENTS", method="HASH", columns="ID", policy=OracleHashPolicy(modulus=3)
        )
        hash_infos = [
            OraclePartitionInfo("EVENTS", None, 0, method="HASH", is_partitioned=True),
            OraclePartitionInfo("P0", "EVENTS", 1, method="HASH"),
            OraclePartitionInfo("P1", "EVENTS", 1, method="HASH"),
        ]
        assert any(
            "hash strategy mismatch" in issue
            for issue in maintenance._structure_issues(
                hash_plan, hash_infos, list(hash_plan.iter_specs())
            )
        )

    def test_definitions_multi_column_and_subpartitions(self):
        maintenance = _sync_maintenance()
        plan = OraclePartitionPlan(
            parent="REGIONS",
            method="LIST",
            columns=("COUNTRY", "REGION"),
            branches=(
                OraclePartitionBranch(
                    name="P1",
                    in_values=(("US", "EAST"), ("US", "WEST")),
                    subpartitions=(OracleSubpartitionSpec("SP1", in_values=(("A", 1), ("B", 2))),),
                ),
            ),
            subpartition=OracleSubpartitionPolicy(method="LIST", columns=("C1", "C2")),
        )
        spec = next(iter(plan.iter_specs()))
        definition = maintenance._definition(plan, spec)
        assert isinstance(definition.in_values[0], tuple)
        assert isinstance(definition.subpartition_definitions[0].in_values[0], tuple)
        sql, params = maintenance._add_statement(plan, spec)
        assert "ADD PARTITION" in sql and params == ()

    def test_continuity_issues_sorted_and_empty(self):
        maintenance = _sync_maintenance()
        assert maintenance._continuity_issues([]) == ()

    def test_report_shape(self):
        maintenance = _sync_maintenance()
        plan = _range_plan(("P1", date(2026, 2, 1)))
        infos = (OraclePartitionInfo("EVENTS", None, 0, method="RANGE", is_partitioned=True),)
        report = maintenance._report(plan, infos, list(plan.iter_specs()), None)
        assert report.target == "EVENTS"
        assert report.partitions == infos
        assert report.dry_run is False

    def test_render_creates_and_without(self):
        maintenance = _sync_maintenance()
        plan = _range_plan(("P1", date(2026, 2, 1)), ("P2", date(2026, 3, 1)))
        specs = list(plan.iter_specs())
        operations = maintenance._render_creates(plan, specs)
        assert [op.name for op in operations] == ["P1", "P2"]
        assert all(op.action == "create" and op.level == 1 for op in operations)
        assert [s.name for s in maintenance._without(specs, ["p1"])] == ["P2"]

    @pytest.mark.parametrize("cutoff", ["2026-03-01", 7, None, object()])
    def test_drop_candidates_rejects_non_dates(self, cutoff):
        maintenance = _sync_maintenance()
        plan = _range_plan(("P1", date(2026, 2, 1)))
        with pytest.raises(TypeError, match="cutoff must be a date"):
            maintenance._drop_candidates(plan, cutoff, (), None)

    def test_drop_candidates_type_mismatch(self):
        maintenance = _sync_maintenance()
        plan = _range_plan(("P1", date(2026, 2, 1)))
        infos = (OraclePartitionInfo("P1", "EVENTS", 1, bound=FEB),)
        with pytest.raises(TypeError, match="cutoff must match"):
            maintenance._drop_candidates(plan, datetime(2026, 3, 1), infos, None)

    def test_drop_candidates_skips_maxvalue_and_absent(self):
        maintenance = _sync_maintenance()
        plan = _range_plan(
            ("P1", date(2026, 2, 1)),
            ("P_MAX", "MAXVALUE"),
            ("P3", date(2026, 4, 1)),
        )
        infos = (
            OraclePartitionInfo("P1", "EVENTS", 1, bound=FEB),
            OraclePartitionInfo("P_MAX", "EVENTS", 1, bound="MAXVALUE"),
        )
        candidates = maintenance._drop_candidates(plan, date(2026, 5, 1), infos, None)
        assert [spec.name for spec in candidates] == ["P1"]


# --------------------------------------------------------------------------- #
# sync maintenance public surface (mocked I/O seams)
# --------------------------------------------------------------------------- #
class TestSyncMaintenanceIO:
    def _plan(self):
        return _range_plan(
            ("P1", date(2026, 2, 1)),
            ("P2", date(2026, 3, 1)),
            ("P3", date(2026, 4, 1)),
        )

    def _executor(self, db):
        return db.wire(OraclePartitionMaintenance(_backend()))

    def test_list_reports_missing(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = self._executor(db).list(self._plan())
        assert report.missing == (("EVENTS", "P2"), ("EVENTS", "P3"))
        assert report.errors == () and report.dry_run is False

    def test_list_with_no_dictionary_rows(self):
        report = self._executor(_ScriptedDb()).list(self._plan())
        assert report.partitions == ()
        assert "not a partitioned table" in report.issues[0]

    def test_list_with_generated_horizon(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
        )
        db = _ScriptedDb(root_rows=[_root_row()])
        report = self._executor(db).list(plan, through=date(2026, 3, 1))
        assert len(report.missing) == 2

    def test_ensure_creates_only_missing_partitions(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        db.created_bounds = {
            "P2": MAR,
            "P3": "TO_DATE(' 2026-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')",
        }
        report = self._executor(db).ensure(self._plan())
        assert [op.name for op in report.operations] == ["P2", "P3"]
        assert report.errors == ()
        assert len([sql for sql in db.executed if "ADD PARTITION" in sql]) == 2

    def test_ensure_raises_when_parent_absent(self):
        with pytest.raises(ValueError, match="does not exist"):
            self._executor(_ScriptedDb()).ensure(self._plan())

    def test_ensure_records_error_and_continues(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        db.fail_sql = lambda sql: 'ADD PARTITION "P2"' in sql
        report = self._executor(db).ensure(self._plan())
        assert [op.name for op in report.operations] == ["P3"]
        assert len(report.errors) == 1 and "P2" in report.errors[0]

    def test_ensure_retries_name_present_with_wrong_bound(self):
        plan = _range_plan(("P1", date(2026, 2, 1)))
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", MAR, 1)])
        report = self._executor(db).ensure(plan)
        assert [op.name for op in report.operations] == ["P1"]

    def test_drop_before_dry_run_renders_without_executing(self):
        db = _ScriptedDb(
            root_rows=[_root_row()],
            part_rows=[
                _part_row("P1", FEB, 1),
                _part_row("P2", MAR, 2),
                _part_row("P3", "MAXVALUE", 3),
            ],
        )
        report = self._executor(db).drop_before(self._plan(), date(2026, 3, 1))
        assert [op.name for op in report.operations] == ["P1", "P2"]
        assert report.dry_run is True and db.executed == []

    def test_drop_before_executes_and_refreshes(self):
        db = _ScriptedDb(
            root_rows=[_root_row()],
            part_rows=[
                _part_row("P1", FEB, 1),
                _part_row("P2", MAR, 2),
                _part_row("P3", "MAXVALUE", 3),
            ],
        )
        report = self._executor(db).drop_before(self._plan(), date(2026, 3, 1), dry_run=False)
        assert [op.name for op in report.operations] == ["P1", "P2"]
        assert report.dry_run is False
        assert len([sql for sql in db.executed if "DROP PARTITION" in sql]) == 2
        assert [row["partition_name"] for row in db.part_rows] == ["P3"]

    def test_drop_before_records_error(self):
        db = _ScriptedDb(
            root_rows=[_root_row()],
            part_rows=[_part_row("P1", FEB, 1), _part_row("P2", MAR, 2)],
        )
        db.fail_sql = lambda sql: 'DROP PARTITION "P1"' in sql
        report = self._executor(db).drop_before(self._plan(), date(2026, 3, 1), dry_run=False)
        assert len(report.errors) == 1 and "P1" in report.errors[0]
        assert [row["partition_name"] for row in db.part_rows] == ["P1"]

    def test_drop_before_with_wider_through(self):
        plan = OraclePartitionPlan(
            parent="EVENTS",
            method="RANGE",
            columns="CREATED_AT",
            policy=OracleRangePolicy(start=date(2026, 1, 1), interval="month"),
        )
        db = _ScriptedDb(
            root_rows=[_root_row()],
            part_rows=[
                _part_row("EVENTS_2026_01", FEB, 1),
                _part_row("EVENTS_2026_02", MAR, 2),
            ],
        )
        report = self._executor(db).drop_before(plan, date(2026, 2, 1), through=date(2026, 4, 1))
        assert [op.name for op in report.operations] == ["EVENTS_2026_01"]

    def test_drop_partition_dry_run(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = self._executor(db).drop_partition(self._plan(), "P1", dry_run=True)
        assert report.operations[0].name == "P1"
        assert report.dry_run is True and db.executed == []

    def test_drop_partition_executes_and_refreshes(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = self._executor(db).drop_partition(self._plan(), "P1")
        assert report.errors == () and report.dry_run is False
        assert any("DROP PARTITION" in sql for sql in db.executed)

    def test_drop_partition_records_error(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        db.fail_sql = lambda sql: "DROP PARTITION" in sql
        report = self._executor(db).drop_partition(self._plan(), "P1")
        assert len(report.errors) == 1 and "P1" in report.errors[0]

    def test_validate_matches_list(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        executor = self._executor(db)
        assert executor.validate(self._plan()).issues == executor.list(self._plan()).issues

    def test_execute_receives_rendered_sql(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        self._executor(db).ensure(self._plan())
        assert all("ALTER TABLE" in sql for sql in db.executed)


# --------------------------------------------------------------------------- #
# async maintenance public surface (native async driver seams)
# --------------------------------------------------------------------------- #
class TestAsyncMaintenanceIO:
    def _plan(self):
        return _range_plan(("P1", date(2026, 2, 1)), ("P2", date(2026, 3, 1)))

    def _executor(self, db):
        return db.wire(AsyncOraclePartitionMaintenance(_backend()))

    async def test_async_list(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = await self._executor(db).list(self._plan())
        assert report.missing == (("EVENTS", "P2"),)

    async def test_async_list_with_no_rows(self):
        report = await self._executor(_ScriptedDb()).list(self._plan())
        assert report.partitions == ()

    async def test_async_ensure(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        db.created_bounds = {"P2": MAR}
        report = await self._executor(db).ensure(self._plan())
        assert [op.name for op in report.operations] == ["P2"]
        assert report.errors == ()
        assert any("ADD PARTITION" in sql for sql in db.executed)

    async def test_async_ensure_raises_without_parent(self):
        with pytest.raises(ValueError, match="does not exist"):
            await self._executor(_ScriptedDb()).ensure(self._plan())

    async def test_async_ensure_records_error_and_continues(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[])
        db.created_bounds = {"P1": FEB, "P2": MAR}
        db.fail_sql = lambda sql: 'ADD PARTITION "P1"' in sql
        report = await self._executor(db).ensure(self._plan())
        assert [op.name for op in report.operations] == ["P2"]
        assert len(report.errors) == 1

    async def test_async_drop_before_dry_run(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = await self._executor(db).drop_before(self._plan(), date(2026, 3, 1))
        assert [op.name for op in report.operations] == ["P1"]
        assert report.dry_run is True and db.executed == []

    async def test_async_drop_before_executes(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = await self._executor(db).drop_before(self._plan(), date(2026, 3, 1), dry_run=False)
        assert report.errors == ()
        assert any("DROP PARTITION" in sql for sql in db.executed)

    async def test_async_drop_partition_dry_run(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        report = await self._executor(db).drop_partition(self._plan(), "P1", dry_run=True)
        assert report.operations[0].name == "P1"
        assert db.executed == []

    async def test_async_drop_partition_records_error(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        db.fail_sql = lambda sql: "DROP PARTITION" in sql
        report = await self._executor(db).drop_partition(self._plan(), "P1")
        assert len(report.errors) == 1

    async def test_async_validate_matches_list(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        executor = self._executor(db)
        assert (await executor.validate(self._plan())).issues == (
            await executor.list(self._plan())
        ).issues

    async def test_async_dictionary_queries_use_await_seams(self):
        plan = self._plan()
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        executor = self._executor(db)
        await executor.list(plan)
        assert executor._fetch_rows.await_count == 3
        executor._execute = AsyncMock()
        await executor.drop_before(plan, date(2026, 3, 1), dry_run=False)
        assert executor._execute.await_count >= 1


class TestRealDriverSeams:
    """The concrete seams delegate verbatim to the (mocked) driver backend."""

    def test_sync_fetch_and_execute_delegate_to_backend(self):
        backend = _backend()
        backend.fetch_all = MagicMock(return_value=[{"x": 1}])
        executor = OraclePartitionMaintenance(backend)
        assert executor._fetch_rows("SQL", (1,)) == [{"x": 1}]
        backend.fetch_all.assert_called_once_with("SQL", (1,))
        executor._execute("DDL", ())
        backend.execute.assert_called_once_with("DDL", ())

    async def test_async_fetch_and_execute_delegate_to_backend(self):
        backend = _backend()
        backend.fetch_all = AsyncMock(return_value=[{"x": 1}])
        backend.execute = AsyncMock()
        executor = AsyncOraclePartitionMaintenance(backend)
        assert await executor._fetch_rows("SQL", (1,)) == [{"x": 1}]
        backend.fetch_all.assert_awaited_once_with("SQL", (1,))
        await executor._execute("DDL", ())
        backend.execute.assert_awaited_once_with("DDL", ())

    async def test_async_native_driver_drop_runs_ddl(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        backend = _backend()
        backend.fetch_all = AsyncMock(side_effect=db.fetch)
        backend.execute = AsyncMock(side_effect=db.execute)
        executor = AsyncOraclePartitionMaintenance(backend)
        plan = _range_plan(("P1", date(2026, 2, 1)))
        report = await executor.drop_before(plan, date(2026, 3, 1), dry_run=False)
        assert [op.name for op in report.operations] == ["P1"]
        backend.execute.assert_awaited()
        assert any("DROP PARTITION" in call.args[0] for call in backend.execute.await_args_list)

    async def test_async_native_driver_drop_records_error(self):
        db = _ScriptedDb(root_rows=[_root_row()], part_rows=[_part_row("P1", FEB, 1)])
        db.fail_sql = lambda sql: "DROP PARTITION" in sql
        backend = _backend()
        backend.fetch_all = AsyncMock(side_effect=db.fetch)
        backend.execute = AsyncMock(side_effect=db.execute)
        executor = AsyncOraclePartitionMaintenance(backend)
        plan = _range_plan(("P1", date(2026, 2, 1)))
        report = await executor.drop_before(plan, date(2026, 3, 1), dry_run=False)
        assert len(report.errors) == 1 and "P1" in report.errors[0]
