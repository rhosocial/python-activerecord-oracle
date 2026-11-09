# tests/rhosocial/activerecord_oracle_test/feature/backend/cli/test_cli_introspect_families.py
"""The eight families, as the CLI asks for them -- and mostly refuses.

No server is needed here. What is checked is the wiring the CLI owns: that
each type reaches its introspector method, that NAME and the type-specific
flags arrive as arguments rather than being parsed and dropped, that a family
the connected server refuses is refused, and that the two data families carry
the ``source`` column that tells one from the other.

Six of the eight are declared gaps on Oracle today -- the statements would
have to read GV$/V$ performance views, and each is pinned as a non-render by
the roundtrip suite -- so their probes answer ``False`` and the CLI refuses
them. Two consequences shape this file. The wiring tests run those branches
behind a dialect that *answers* for the one switch under test, because a run
against the real dialect never reaches the branch. The refusal tests use the
real ``OracleDialect``, because the promise under test is Oracle's own: a
probe that drifted to ``True`` while its statement was still unwritten would
turn a clean refusal into a run that fails mid-query, which is the exact
defect that cost MySQL a red CI row on 5.6. The two the dialect can answer --
the estimate profile and the aggregate value listing -- are asserted to pass
that same gate, so the gate is shown to be read rather than uniformly
refused.

DDL extraction is not among the eight: its switch is permanently ``False``,
so the CLI offers no ``ddl`` type at all rather than an entry every run
refuses.
"""

import argparse
import json
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from rhosocial.activerecord.backend.impl.oracle.cli import introspect as introspect_cli
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.introspection.types import (
    ColumnDataProfile,
    ColumnValueFrequency,
    JoinCoverage,
)

#: The eight families this change wires, and the switch that gates each.
NEW_FAMILIES = {
    "unused-indexes": "supports_unused_indexes_detection",
    "table-stats": "supports_table_stats",
    "runtime": "supports_runtime_stats",
    "index-stats": "supports_index_stats",
    "dependencies": "supports_object_dependencies",
    "column-profiles": "supports_column_data_profile",
    "value-samples": "supports_column_value_sample",
    "join-coverage": "supports_join_coverage",
}

#: The four counter listings answer for the whole schema and take no NAME.
WHOLE_DATABASE_FAMILIES = (
    "unused-indexes",
    "table-stats",
    "runtime",
    "index-stats",
)

#: The two one-table questions: NAME is required and is the table.
ONE_TABLE_FAMILIES = ("column-profiles", "value-samples")

#: The two families this dialect can actually answer on a live server. The
#: profile is an estimate read from ``ALL_TAB_COLUMNS``; the value listing is
#: a grouped scan of the rows. They are the reason ``source`` is a column.
ANSWERABLE_FAMILIES = ("column-profiles", "value-samples")

#: A server modern enough for every statement this dialect has. None of the
#: eight probes reads a version floor here -- the one that does on MySQL is
#: that dialect's own business -- so the value only has to be unambiguous.
STUB_SERVER_VERSION = (21, 0, 0)


def _answer_dialect(switch: str) -> Any:
    """A dialect that answers for exactly one switch, and for nothing else.

    The real Oracle dialect refuses six of the eight today, so a test that
    wants to reach one of those branches has to say something about the
    server. Saying yes to the one switch under test says as little as possible
    and names it: the refusal is tested separately, against the real thing.
    """
    class _Dialect:
        pass

    setattr(_Dialect, switch, staticmethod(lambda: True))
    return _Dialect()


def _real_dialect() -> OracleDialect:
    return OracleDialect(version=STUB_SERVER_VERSION)


class _RecordingIntrospector:
    """Records each introspection call, and returns what the test declared.

    ``__getattr__`` is the whole implementation: a test names the method once,
    in the assertion, and a branch that reaches a different one fails on the
    recorded name rather than on an attribute error that would hide the typo.
    """

    def __init__(self, results: Optional[dict] = None) -> None:
        self.results = results or {}
        self.calls: List[tuple] = []

    def __getattr__(self, name: str):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self.results.get(name)

        return call


class _AsyncRecordingIntrospector(_RecordingIntrospector):
    """The async shape of the same recorder: methods are awaited, calls are not."""

    def __getattr__(self, name: str):
        async def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self.results.get(name)

        return call


def _args(type_: str, name: Optional[str] = None, **overrides) -> argparse.Namespace:
    """The attributes a parsed invocation carries, with sane defaults."""
    values: Dict[str, Any] = dict(
        output="json",
        rich_ascii=False,
        type=type_,
        name=name,
        database="mydb",
        is_async=False,
        named_connection=None,
        include_system=False,
        column=None,
        right_table=None,
        right_column=None,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def _run_sync(monkeypatch, introspector, args, dialect: Any = None) -> None:
    """Drive ``handle()`` synchronously against a recording introspector.

    ``dialect`` defaults to the real Oracle dialect, adapted to a modern
    server -- which is what the refusal tests want. The wiring tests pass
    :func:`_answer_dialect` instead, and say so at the call site.
    """
    installed = _real_dialect() if dialect is None else dialect

    class _Backend:
        _connection = None

        def __init__(self, **_kwargs) -> None:
            self.introspector = introspector
            self.dialect = installed

        def connect(self) -> None:
            pass

        def introspect_and_adapt(self) -> None:
            # The switches are read *after* this call, exactly as the real
            # backend runs: a dialect left unadapted would answer every gate
            # from a version nobody confirmed.
            pass

        def disconnect(self) -> None:
            pass

    monkeypatch.setattr(introspect_cli, "OracleBackend", _Backend)
    monkeypatch.setattr(
        introspect_cli, "resolve_connection_config_from_args", lambda _args: None
    )
    introspect_cli.handle(args)


def _run_async(monkeypatch, introspector, args, dialect: Any = None) -> None:
    """The same, awaited."""
    installed = _real_dialect() if dialect is None else dialect

    class _Backend:
        _connection = None

        def __init__(self, **_kwargs) -> None:
            self.introspector = introspector
            self.dialect = installed

        async def connect(self) -> None:
            pass

        async def introspect_and_adapt(self) -> None:
            pass

        async def disconnect(self) -> None:
            pass

    monkeypatch.setattr(introspect_cli, "AsyncOracleBackend", _Backend)
    monkeypatch.setattr(
        introspect_cli, "resolve_connection_config_from_args", lambda _args: None
    )
    args.is_async = True
    introspect_cli.handle(args)


def _profile() -> ColumnDataProfile:
    """A profile as Oracle's parser labels it: an estimate with a vintage."""
    return ColumnDataProfile(
        schema="AR_CRM",
        table_name="CUSTOMERS",
        column_name="STATUS",
        null_fraction=0.1,
        distinct_count=3,
        row_count=1000,
        source="estimate",
        collected_at="2026-11-01T00:00:00",
    )


def _coverage() -> JoinCoverage:
    return JoinCoverage(
        left_schema="AR_CRM",
        left_table="ORDERS",
        left_column="CUSTOMER_ID",
        right_schema="AR_CRM",
        right_table="CUSTOMERS",
        right_column="ID",
        left_coverage=1.0,
        right_coverage=0.75,
    )


class TestTheSchemaWideFamilies:
    """The counters and listings describe the schema, so NAME is refused."""

    @pytest.mark.parametrize(
        "type_, method",
        [
            ("unused-indexes", "list_unused_indexes"),
            ("table-stats", "list_table_usage_stats"),
            ("runtime", "list_runtime_metrics"),
            ("index-stats", "list_index_usage_stats"),
        ],
    )
    def test_the_family_asks_its_method_for_the_whole_schema(
        self, type_, method, monkeypatch
    ):
        switch = NEW_FAMILIES[type_]
        introspector = _RecordingIntrospector({method: []})

        _run_sync(
            monkeypatch, introspector, _args(type_), dialect=_answer_dialect(switch)
        )

        assert introspector.calls == [(method, (), {"schema": None})]

    @pytest.mark.parametrize("type_", WHOLE_DATABASE_FAMILIES)
    def test_a_name_is_refused(self, type_, capsys):
        """A dropped NAME looks like a filter; it is refused before connecting."""
        with pytest.raises(SystemExit) as excinfo:
            introspect_cli._validate_name(_args(type_, "customers"))

        assert excinfo.value.code == 2
        assert "takes no name" in capsys.readouterr().err

    @pytest.mark.parametrize("type_", WHOLE_DATABASE_FAMILIES)
    def test_the_refusal_happens_before_anything_connects(self, type_, monkeypatch):
        introspector = _RecordingIntrospector()

        with pytest.raises(SystemExit):
            _run_sync(monkeypatch, introspector, _args(type_, "customers"))

        assert introspector.calls == []


class TestTheDependenciesBranch:
    """One optional NAME narrows the graph; omitting it asks for all of it."""

    def test_a_named_object_is_asked_for_by_name(self, monkeypatch):
        introspector = _RecordingIntrospector({"list_object_dependencies": []})

        _run_sync(
            monkeypatch,
            introspector,
            _args("dependencies", "orders"),
            dialect=_answer_dialect(NEW_FAMILIES["dependencies"]),
        )

        assert introspector.calls == [
            ("list_object_dependencies", (), {"object_name": "orders", "schema": None})
        ]

    def test_an_unnamed_run_asks_for_the_whole_graph(self, monkeypatch):
        introspector = _RecordingIntrospector({"list_object_dependencies": []})

        _run_sync(
            monkeypatch,
            introspector,
            _args("dependencies"),
            dialect=_answer_dialect(NEW_FAMILIES["dependencies"]),
        )

        assert introspector.calls == [
            ("list_object_dependencies", (), {"object_name": None, "schema": None})
        ]


class TestTheColumnProfilesBranch:
    """The estimate label must reach the reader; the numbers are catalogue."""

    def test_the_table_is_asked_for_by_name(self, monkeypatch):
        introspector = _RecordingIntrospector(
            {"list_column_data_profiles": [_profile()]}
        )

        _run_sync(monkeypatch, introspector, _args("column-profiles", "customers"))

        assert introspector.calls == [
            ("list_column_data_profiles", ("customers",), {"schema": None})
        ]

    def test_every_row_says_it_is_an_estimate(self, monkeypatch, capsys):
        """Oracle is the one engine whose profile is an estimate.

        ``ALL_TAB_COLUMNS`` holds what the last statistics gathering found, so
        the figure is cheap and possibly stale -- and a reader must be able to
        tell it from the counted scan in the value listing beside it without
        knowing which engine produced it. That is what ``source`` is for.
        """
        introspector = _RecordingIntrospector(
            {"list_column_data_profiles": [_profile()]}
        )

        _run_sync(monkeypatch, introspector, _args("column-profiles", "customers"))

        payload = json.loads(capsys.readouterr().out)
        assert payload[0]["source"] == "estimate"
        assert payload[0]["collected_at"] == "2026-11-01T00:00:00"
        # The figures are estimates, so the vintage travels with them.
        assert payload[0]["row_count"] == 1000
        # RAW extremes stay absent rather than cast to unreadable bytes.
        assert payload[0]["min_value"] is None
        assert payload[0]["max_value"] is None

    def test_an_unanalysed_column_says_unknown_rather_than_zero(self, monkeypatch, capsys):
        """Never analysed: the dictionary still lists the column.

        Its statistics columns are NULL, so every figure is ``None`` and the
        ``source`` says why -- which is a different answer from "no such
        column" and must not be smoothed into zeros.
        """
        blind = ColumnDataProfile(
            schema="AR_CRM",
            table_name="CUSTOMERS",
            column_name="NEWS",
            source="unknown",
        )
        introspector = _RecordingIntrospector({"list_column_data_profiles": [blind]})

        _run_sync(monkeypatch, introspector, _args("column-profiles", "customers"))

        payload = json.loads(capsys.readouterr().out)
        assert payload[0]["source"] == "unknown"
        assert payload[0]["distinct_count"] is None
        assert payload[0]["null_fraction"] is None


class TestTheValueSamplesBranch:
    """A table and a column, and a row that says it was counted, not sampled."""

    def test_the_table_and_the_column_reach_the_introspector(self, monkeypatch):
        """The two names and the row bound.

        ``limit`` is not on the command line here -- there is no flag to carry
        it -- so the introspector's own bound applies rather than a number the
        CLI restates. The schema is passed explicitly, because the connection
        fixes it and the question must still say where it was asked.
        """
        introspector = _RecordingIntrospector(
            {"sample_column_values": [ColumnValueFrequency(value="ACTIVE", count=700)]}
        )

        _run_sync(
            monkeypatch,
            introspector,
            _args("value-samples", "customers", column="status"),
        )

        assert introspector.calls == [
            ("sample_column_values", ("customers", "status"), {"schema": None})
        ]

    def test_every_row_says_it_was_aggregated(self, monkeypatch, capsys):
        """The listing is a ``GROUP BY``, so its counts are exact -- unlike the
        profile's.

        The column is added at the CLI because core's
        ``ColumnValueFrequency`` has no such field: an engine keeping a
        most-common-values list would need a different word. Without it the
        two tables of one run would read as the same kind of answer.
        """
        introspector = _RecordingIntrospector(
            {"sample_column_values": [ColumnValueFrequency(value="ACTIVE", count=700)]}
        )

        _run_sync(
            monkeypatch,
            introspector,
            _args("value-samples", "customers", column="status"),
        )

        payload = json.loads(capsys.readouterr().out)
        assert payload[0]["value"] == "ACTIVE"
        assert payload[0]["count"] == 700
        assert payload[0]["source"] == "aggregate"
        # The scan is not a share of anything: the row count is not carried.
        assert payload[0]["frequency"] is None

    def test_the_two_data_families_do_not_render_as_one_answer(
        self, monkeypatch, capsys
    ):
        """The pair a reader must not blur: an estimate and a counted scan.

        Both types are run and their ``source`` columns read back. One says
        ``estimate``, the other ``aggregate``, so the same table in one run
        yields two visibly different kinds of fact rather than one homogeneous
        table whose provenance nobody can tell.
        """
        introspector = _RecordingIntrospector({
            "list_column_data_profiles": [_profile()],
            "sample_column_values": [ColumnValueFrequency(value="ACTIVE", count=700)],
        })

        _run_sync(monkeypatch, introspector, _args("column-profiles", "customers"))
        profile_rows = json.loads(capsys.readouterr().out)
        _run_sync(
            monkeypatch,
            introspector,
            _args("value-samples", "customers", column="status"),
        )
        sample_rows = json.loads(capsys.readouterr().out)

        assert profile_rows[0]["source"] == "estimate"
        assert sample_rows[0]["source"] == "aggregate"
        # One row's count is what the sample said; the other's is what the
        # scan found. They are not comparable, and the label says which.
        assert {profile_rows[0]["source"], sample_rows[0]["source"]} == {
            "estimate",
            "aggregate",
        }

    def test_the_scan_is_announced_on_stderr(self, monkeypatch, capsys):
        """A full-table scan takes a while; the wait is explained, not silent.

        It goes to stderr so the machine formats on stdout stay parseable.
        """
        introspector = _RecordingIntrospector({"sample_column_values": []})

        _run_sync(
            monkeypatch, introspector, _args("value-samples", "customers", column="status")
        )

        captured = capsys.readouterr()
        assert "aggregate" in captured.err
        assert captured.out.lstrip().startswith("[")

    def test_a_missing_column_is_an_incomplete_invocation(
        self, monkeypatch, capsys
    ):
        introspector = _RecordingIntrospector({"sample_column_values": []})

        with pytest.raises(SystemExit) as excinfo:
            _run_sync(monkeypatch, introspector, _args("value-samples", "customers"))

        assert excinfo.value.code == 1
        assert "--column is required" in capsys.readouterr().err


class TestTheJoinCoverageBranch:
    """Four names, both sides of the measurement, in the order they are asked."""

    def test_the_four_names_reach_the_introspector(self, monkeypatch):
        introspector = _RecordingIntrospector({"estimate_join_coverage": _coverage()})

        _run_sync(
            monkeypatch,
            introspector,
            _args(
                "join-coverage",
                "orders",
                column="customer_id",
                right_table="customers",
                right_column="id",
            ),
            dialect=_answer_dialect(NEW_FAMILIES["join-coverage"]),
        )

        assert introspector.calls == [
            (
                "estimate_join_coverage",
                ("orders", "customer_id", "customers", "id"),
                {"schema": None},
            )
        ]

    def test_both_fractions_are_reported(self, monkeypatch, capsys):
        introspector = _RecordingIntrospector({"estimate_join_coverage": _coverage()})

        _run_sync(
            monkeypatch,
            introspector,
            _args(
                "join-coverage",
                "orders",
                column="customer_id",
                right_table="customers",
                right_column="id",
            ),
            dialect=_answer_dialect(NEW_FAMILIES["join-coverage"]),
        )

        payload = json.loads(capsys.readouterr().out)
        assert payload[0]["left_coverage"] == 1.0
        assert payload[0]["right_coverage"] == 0.75

    def test_an_unmeasurable_pair_is_an_error(self, monkeypatch, capsys):
        """``None`` is "not measured", which is not a zero-overlap answer."""
        introspector = _RecordingIntrospector({"estimate_join_coverage": None})

        with pytest.raises(SystemExit) as excinfo:
            _run_sync(
                monkeypatch,
                introspector,
                _args(
                    "join-coverage",
                    "orders",
                    column="customer_id",
                    right_table="customers",
                    right_column="id",
                ),
                dialect=_answer_dialect(NEW_FAMILIES["join-coverage"]),
            )

        assert excinfo.value.code == 1
        assert "cannot be measured" in capsys.readouterr().err

    def test_a_missing_name_is_an_incomplete_invocation(self, monkeypatch, capsys):
        introspector = _RecordingIntrospector({"estimate_join_coverage": None})

        with pytest.raises(SystemExit) as excinfo:
            _run_sync(
                monkeypatch,
                introspector,
                _args(
                    "join-coverage",
                    column="customer_id",
                    right_table="customers",
                    right_column="id",
                ),
                dialect=_answer_dialect(NEW_FAMILIES["join-coverage"]),
            )

        assert excinfo.value.code == 1
        assert "Left table name is required" in capsys.readouterr().err
        assert introspector.calls == []


class TestTheArgumentRules:
    """A flag the chosen type does not read is refused, not dropped."""

    @pytest.mark.parametrize(
        "type_", ["tables", "columns", "dependencies", "column-profiles"]
    )
    def test_a_flag_the_type_does_not_read_is_refused(self, type_, capsys):
        with pytest.raises(SystemExit) as excinfo:
            introspect_cli._refuse_unused_arguments(
                _args(type_, "customers", column="id")
            )

        assert excinfo.value.code == 2
        assert "does not take --column" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "type_, kwargs",
        [
            ("value-samples", {"column": "status"}),
            (
                "join-coverage",
                {
                    "column": "id",
                    "right_table": "customers",
                    "right_column": "id",
                },
            ),
        ],
    )
    def test_the_families_that_read_it_are_not_refused(self, type_, kwargs):
        """The check narrows the flag to its two readers and to nothing else."""
        introspect_cli._refuse_unused_arguments(_args(type_, "customers", **kwargs))

    @pytest.mark.parametrize("type_", ["tables", "columns", "value-samples"])
    def test_a_right_table_the_type_does_not_read_is_refused(self, type_, capsys):
        with pytest.raises(SystemExit) as excinfo:
            introspect_cli._refuse_unused_arguments(
                _args(type_, "customers", right_table="customers", right_column="id")
            )

        assert excinfo.value.code == 2
        assert "--right-table or --right-column" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "type_, dialect",
        [
            ("column-profiles", "column-profiles"),
            ("value-samples", "value-samples"),
            ("join-coverage", "join-coverage"),
        ],
    )
    def test_a_required_name_is_required(self, type_, dialect, monkeypatch, capsys):
        introspector = _RecordingIntrospector()

        with pytest.raises(SystemExit) as excinfo:
            _run_sync(
                monkeypatch,
                introspector,
                _args(type_),
                dialect=_answer_dialect(NEW_FAMILIES[dialect]),
            )

        assert excinfo.value.code == 1
        assert "required" in capsys.readouterr().err
        assert introspector.calls == []


class TestTheCapabilityGate:
    """The gate is read against the connected dialect, and it is read here.

    These use the real ``OracleDialect`` rather than a stub, because the
    promise under test is Oracle's own: six of the eight answer ``False``
    today, so the CLI must refuse them rather than run them into a formatter
    that does not exist. A probe that drifted to ``True`` while its statement
    was still unwritten would produce exactly the red CI row MySQL got on 5.6.
    """

    @pytest.mark.parametrize("type_", list(NEW_FAMILIES))
    def test_the_gate_answers_from_the_dialect(self, type_, capsys):
        switch = NEW_FAMILIES[type_]
        dialect = _real_dialect()
        predicate = getattr(dialect, switch, None)
        assert callable(predicate), f"{type_} -> {switch} is not a dialect method"

        backend = SimpleNamespace(dialect=dialect)

        if type_ in ANSWERABLE_FAMILIES:
            # The two this dialect answers must pass: a gate that refused
            # everything would make every test above vacuous.
            introspect_cli._require_capability(backend, _args(type_, "customers"))
        else:
            with pytest.raises(SystemExit) as excinfo:
                introspect_cli._require_capability(backend, _args(type_))
            assert excinfo.value.code == 2
            assert "not supported by this server" in capsys.readouterr().err

    #: The six declared gaps, with the arguments that would otherwise reach
    #: their branch. ``name`` is given where the family requires it and omitted
    #: where a name would be refused for an unrelated reason.
    REFUSED_FAMILIES = {
        "unused-indexes": {},
        "table-stats": {},
        "runtime": {},
        "index-stats": {},
        "dependencies": {"name": "customers"},
        "join-coverage": {
            "name": "orders",
            "column": "customer_id",
            "right_table": "customers",
            "right_column": "id",
        },
    }

    @pytest.mark.parametrize("type_", list(REFUSED_FAMILIES))
    def test_the_introspector_is_never_reached(self, type_, monkeypatch, capsys):
        """The refusal is the capability answer, not an execution error.

        A run that reached the statement layer would render a statement the
        dispatch cannot produce and report the failure as a generic
        "Error during introspection" -- a red CI row whose message is about
        the symptom rather than the cause.
        """
        introspector = _RecordingIntrospector()

        with pytest.raises(SystemExit) as excinfo:
            _run_sync(
                monkeypatch, introspector, _args(type_, **self.REFUSED_FAMILIES[type_])
            )

        assert excinfo.value.code == 2
        assert "not supported by this server" in capsys.readouterr().err
        assert introspector.calls == []

    @pytest.mark.parametrize("type_", list(NEW_FAMILIES))
    def test_every_switch_names_a_real_dialect_predicate(self, type_):
        switch = NEW_FAMILIES[type_]
        assert getattr(_real_dialect(), switch, None) is not None

    @pytest.mark.parametrize("type_", ANSWERABLE_FAMILIES)
    def test_the_answerable_families_pass_the_gate(self, type_):
        introspect_cli._require_capability(
            SimpleNamespace(dialect=_real_dialect()), _args(type_, "customers")
        )


class TestTheAsyncPath:
    """The second dispatch chain must ask the same question the first one does."""

    @pytest.mark.parametrize(
        "type_, method, args_kwargs, results, expected",
        [
            (
                "unused-indexes",
                "list_unused_indexes",
                {},
                {"list_unused_indexes": []},
                ((), {"schema": None}),
            ),
            (
                "table-stats",
                "list_table_usage_stats",
                {},
                {"list_table_usage_stats": []},
                ((), {"schema": None}),
            ),
            (
                "runtime",
                "list_runtime_metrics",
                {},
                {"list_runtime_metrics": []},
                ((), {"schema": None}),
            ),
            (
                "index-stats",
                "list_index_usage_stats",
                {},
                {"list_index_usage_stats": []},
                ((), {"schema": None}),
            ),
            (
                "dependencies",
                "list_object_dependencies",
                {"name": "orders"},
                {"list_object_dependencies": []},
                ((), {"object_name": "orders", "schema": None}),
            ),
            (
                "dependencies",
                "list_object_dependencies",
                {},
                {"list_object_dependencies": []},
                ((), {"object_name": None, "schema": None}),
            ),
            (
                "column-profiles",
                "list_column_data_profiles",
                {"name": "customers"},
                {"list_column_data_profiles": [_profile()]},
                (("customers",), {"schema": None}),
            ),
            (
                "value-samples",
                "sample_column_values",
                {"name": "customers", "column": "status"},
                {
                    "sample_column_values": [
                        ColumnValueFrequency(value="ACTIVE", count=700)
                    ]
                },
                (("customers", "status"), {"schema": None}),
            ),
            (
                "join-coverage",
                "estimate_join_coverage",
                {
                    "name": "orders",
                    "column": "customer_id",
                    "right_table": "customers",
                    "right_column": "id",
                },
                {"estimate_join_coverage": _coverage()},
                (("orders", "customer_id", "customers", "id"), {"schema": None}),
            ),
        ],
    )
    def test_the_async_branch_calls_the_same_method_with_the_same_arguments(
        self, type_, method, args_kwargs, results, expected, monkeypatch
    ):
        """Same method, same argument, awaited -- not a second spelling."""
        switch = NEW_FAMILIES[type_]
        introspector = _AsyncRecordingIntrospector(results)

        _run_async(
            monkeypatch,
            introspector,
            _args(type_, **args_kwargs),
            dialect=_answer_dialect(switch),
        )

        assert introspector.calls == [(method, expected[0], expected[1])]

    def test_the_async_path_refuses_the_same_families(self, monkeypatch, capsys):
        introspector = _AsyncRecordingIntrospector()

        with pytest.raises(SystemExit) as excinfo:
            _run_async(monkeypatch, introspector, _args("unused-indexes"))

        assert excinfo.value.code == 2
        assert "not supported by this server" in capsys.readouterr().err
        assert introspector.calls == []

    def test_the_async_path_answers_the_data_families(self, monkeypatch):
        """The two families the dialect answers are wired on the async chain."""
        introspector = _AsyncRecordingIntrospector({
            "list_column_data_profiles": [_profile()],
            "sample_column_values": [ColumnValueFrequency(value="ACTIVE", count=7)],
        })

        _run_async(monkeypatch, introspector, _args("column-profiles", "customers"))
        _run_async(
            monkeypatch,
            introspector,
            _args("value-samples", "customers", column="status"),
        )

        assert [name for name, _args_, _kwargs in introspector.calls] == [
            "list_column_data_profiles",
            "sample_column_values",
        ]


class TestTheTypeSurfaceExtension:
    """A family cannot be offered without a switch and without a name rule."""

    @pytest.mark.parametrize("type_", list(NEW_FAMILIES))
    def test_the_type_is_offered_and_mapped(self, type_):
        assert type_ in introspect_cli.INTROSPECT_TYPES
        assert introspect_cli._TYPE_CAPABILITIES[type_] == NEW_FAMILIES[type_]
        assert type_ in introspect_cli._NAME_RULE

    def test_the_advertised_list_is_the_capability_table(self):
        assert list(introspect_cli._TYPE_CAPABILITIES) == introspect_cli.INTROSPECT_TYPES

    @pytest.mark.parametrize("type_", WHOLE_DATABASE_FAMILIES)
    def test_the_whole_schema_families_take_no_name(self, type_):
        assert introspect_cli._NAME_RULE[type_] == "forbidden"

    @pytest.mark.parametrize("type_", ONE_TABLE_FAMILIES)
    def test_the_one_table_families_require_one(self, type_):
        assert introspect_cli._NAME_RULE[type_] == "required"

    def test_the_dependency_graph_takes_an_optional_one(self):
        assert introspect_cli._NAME_RULE["dependencies"] == "optional"

    def test_join_coverage_names_the_left_table(self):
        assert introspect_cli._NAME_RULE["join-coverage"] == "required"
