# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/cli/test_cli_introspect_p0_families.py
"""The P0 families as the CLI asks for them, offline.

No server is needed: each branch is driven through a recording introspector,
which makes the *call* the assertion. What is checked here is the wiring the
CLI owns -- that a type reaches its introspector method, that NAME means the
table for ``partitions`` and ``constraints`` and the object itself for
``procedures``, ``functions`` and ``types``, that a missing named object is a
runtime error rather than an empty listing, and that the capability gate
passes on the real Oracle dialect for the families this backend now answers.
"""

import argparse
from typing import Any, Dict, List, Optional

import pytest

from rhosocial.activerecord.backend.impl.oracle.cli import introspect as introspect_cli
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

#: The families this change adds, and the dialect switch that gates each.
NEW_FAMILIES = {
    "partitions": "supports_partition_metadata_introspection",
    "constraints": "supports_constraint_introspection",
    "procedures": "supports_procedure_introspection",
    "functions": "supports_function_introspection",
    "types": "supports_type_introspection",
}

REQUIRED_NAME_FAMILIES = ("partitions", "constraints")
OPTIONAL_NAME_FAMILIES = ("procedures", "functions", "types")


class RecordingProvider:
    """A provider that remembers what was rendered instead of printing it."""

    def __init__(self) -> None:
        self.results: List[tuple] = []
        self.connection_errors: List[Any] = []
        self.query_errors: List[Any] = []

    def display_results(self, data: Any, title: Optional[str] = None) -> None:
        self.results.append((title, data))

    def display_connection_error(self, error: Any) -> None:
        self.connection_errors.append(error)

    def display_query_error(self, error: Any) -> None:
        self.query_errors.append(error)


class RecordingIntrospector:
    """Records each method call and returns a declared JSON-ready row."""

    def __init__(self, results: Optional[Dict[str, Any]] = None) -> None:
        self.results = results or {}
        self.calls: List[tuple] = []

    def __getattr__(self, name: str):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self.results.get(name)

        return call


class AsyncRecordingIntrospector(RecordingIntrospector):
    """The awaited twin of the same recorder."""

    def __getattr__(self, name: str):
        async def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self.results.get(name)

        return call


def _args(type_: str, name: Optional[str] = None, **overrides) -> argparse.Namespace:
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


def _run_sync(monkeypatch, introspector, args) -> RecordingProvider:
    provider = RecordingProvider()

    class _Backend:
        _connection = None

        def __init__(self, **_kwargs) -> None:
            self.introspector = introspector
            #: The real dialect: the families under test are answerable, so
            #: the gate is exercised rather than bypassed.
            self.dialect = OracleDialect(version=(23, 0, 0))

        def connect(self) -> None:
            pass

        def introspect_and_adapt(self) -> None:
            pass

        def disconnect(self) -> None:
            pass

    monkeypatch.setattr(introspect_cli, "OracleBackend", _Backend)
    monkeypatch.setattr(
        introspect_cli, "resolve_connection_config_from_args", lambda _args: None
    )
    monkeypatch.setattr(
        introspect_cli,
        "create_provider",
        lambda fmt, ascii_borders=False: provider,
    )
    introspect_cli.handle(args)
    return provider


def _run_async(monkeypatch, introspector, args) -> RecordingProvider:
    provider = RecordingProvider()

    class _Backend:
        _connection = None

        def __init__(self, **_kwargs) -> None:
            self.introspector = introspector
            self.dialect = OracleDialect(version=(23, 0, 0))

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
    monkeypatch.setattr(
        introspect_cli,
        "create_provider",
        lambda fmt, ascii_borders=False: provider,
    )
    args.is_async = True
    introspect_cli.handle(args)
    return provider


class TestTheTypeSurface:
    def test_every_new_family_is_advertised_mapped_and_has_a_name_rule(self):
        for type_ in NEW_FAMILIES:
            assert type_ in introspect_cli.INTROSPECT_TYPES
            assert introspect_cli._TYPE_CAPABILITIES[type_] == NEW_FAMILIES[type_]
            assert type_ in introspect_cli._NAME_RULE

    def test_the_advertised_list_stays_the_capability_table(self):
        assert list(introspect_cli._TYPE_CAPABILITIES) == introspect_cli.INTROSPECT_TYPES

    @pytest.mark.parametrize("type_", REQUIRED_NAME_FAMILIES)
    def test_the_table_families_require_a_name(self, type_):
        assert introspect_cli._NAME_RULE[type_] == "required"

    @pytest.mark.parametrize("type_", OPTIONAL_NAME_FAMILIES)
    def test_the_object_families_take_an_optional_name(self, type_):
        assert introspect_cli._NAME_RULE[type_] == "optional"

    @pytest.mark.parametrize("type_", REQUIRED_NAME_FAMILIES)
    def test_a_missing_table_name_is_an_incomplete_invocation(
        self, type_, capsys
    ):
        with pytest.raises(SystemExit) as excinfo:
            introspect_cli._validate_name(_args(type_))
        assert excinfo.value.code == 1
        assert "Table name is required" in capsys.readouterr().err

    @pytest.mark.parametrize("type_", NEW_FAMILIES)
    def test_a_flag_the_type_does_not_read_is_refused(self, type_, capsys):
        with pytest.raises(SystemExit) as excinfo:
            introspect_cli._refuse_unused_arguments(_args(type_, "customers", column="id"))
        assert excinfo.value.code == 2
        assert "does not take --column" in capsys.readouterr().err


class TestTheSyncBranches:
    @pytest.mark.parametrize(
        "type_,method,title",
        [
            ("partitions", "list_partitions", "Partitions of orders"),
            ("constraints", "list_constraints", "Constraints of orders"),
        ],
    )
    def test_the_table_is_asked_for_by_name(self, monkeypatch, type_, method, title):
        introspector = RecordingIntrospector({method: []})
        provider = _run_sync(monkeypatch, introspector, _args(type_, "orders"))
        assert introspector.calls == [(method, ("orders",), {"schema": None})]
        assert provider.results[0][0] == title

    @pytest.mark.parametrize(
        "type_,method,title",
        [
            ("procedures", "list_procedures", "Procedures"),
            ("functions", "list_functions", "Functions"),
            ("types", "list_types", "Types"),
        ],
    )
    def test_an_unnamed_run_lists_the_owner(self, monkeypatch, type_, method, title):
        introspector = RecordingIntrospector({method: []})
        provider = _run_sync(monkeypatch, introspector, _args(type_))
        assert introspector.calls == [(method, (), {"owner": None})]
        assert provider.results[0][0] == title

    @pytest.mark.parametrize(
        "type_,method,title",
        [
            ("procedures", "get_procedure_info", "Procedure calculate_tax"),
            ("functions", "get_function_info", "Function calculate_total"),
            ("types", "get_type_info", "Type address_t"),
        ],
    )
    def test_a_named_object_is_described(self, monkeypatch, type_, method, title):
        name = {"procedures": "calculate_tax", "functions": "calculate_total",
                "types": "address_t"}[type_]
        introspector = RecordingIntrospector({method: {"OBJECT_NAME": name}})
        provider = _run_sync(monkeypatch, introspector, _args(type_, name))
        assert introspector.calls == [(method, (name,), {"owner": None})]
        assert provider.results[0][0] == title

    @pytest.mark.parametrize("type_", OPTIONAL_NAME_FAMILIES)
    def test_a_named_object_that_is_absent_is_an_error(
        self, monkeypatch, type_, capsys
    ):
        method = {"procedures": "get_procedure_info", "functions": "get_function_info",
                  "types": "get_type_info"}[type_]
        introspector = RecordingIntrospector({method: None})
        with pytest.raises(SystemExit) as excinfo:
            _run_sync(monkeypatch, introspector, _args(type_, "ghost"))
        assert excinfo.value.code == 1
        assert "not found" in capsys.readouterr().err

    def test_the_procedure_response_survives_json_serialisation(self, monkeypatch):
        """The value objects the introspector returns are dataclasses."""
        from rhosocial.activerecord.backend.impl.oracle.introspection.types import (
            OracleRoutineInfo,
        )

        introspector = RecordingIntrospector({
            "list_procedures": [
                OracleRoutineInfo(owner="AR_CRM", name="CALCULATE_TAX",
                                  kind="PROCEDURE")
            ]
        })
        provider = _run_sync(monkeypatch, introspector, _args("procedures"))
        payload = provider.results[0][1]
        assert payload[0]["name"] == "CALCULATE_TAX"
        assert payload[0]["comment"] is None


class TestTheAsyncBranches:
    @pytest.mark.parametrize(
        "type_,method,expected",
        [
            ("partitions", "list_partitions", (("orders",), {"schema": None})),
            ("constraints", "list_constraints", (("orders",), {"schema": None})),
            ("procedures", "list_procedures", ((), {"owner": None})),
            ("functions", "list_functions", ((), {"owner": None})),
            ("types", "list_types", ((), {"owner": None})),
            ("procedures", "get_procedure_info", (("calculate_tax",), {"owner": None})),
            ("functions", "get_function_info", (("calculate_total",), {"owner": None})),
            ("types", "get_type_info", (("address_t",), {"owner": None})),
        ],
    )
    def test_the_async_path_asks_the_same_question(
        self, monkeypatch, type_, method, expected
    ):
        name = {
            "get_procedure_info": "calculate_tax",
            "get_function_info": "calculate_total",
            "get_type_info": "address_t",
            "list_partitions": "orders",
            "list_constraints": "orders",
        }.get(method)
        introspector = AsyncRecordingIntrospector({method: {"OBJECT_NAME": name}})
        _run_async(monkeypatch, introspector, _args(type_, name))
        assert introspector.calls == [(method, expected[0], expected[1])]


class TestTheCapabilityGate:
    @pytest.mark.parametrize("type_", NEW_FAMILIES)
    def test_the_real_dialect_answers_for_every_new_family(self, type_):
        switch = NEW_FAMILIES[type_]
        assert callable(getattr(OracleDialect(version=(23, 0, 0)), switch, None))

    @pytest.mark.parametrize("type_", NEW_FAMILIES)
    def test_the_gate_passes_on_a_real_dialect(self, type_):
        from types import SimpleNamespace

        introspect_cli._require_capability(
            SimpleNamespace(dialect=OracleDialect(version=(23, 0, 0)),
                            ),
            _args(type_, "customers"),
        )

    def test_a_server_that_cannot_answer_is_refused(self, capsys):
        from types import SimpleNamespace

        dialect = SimpleNamespace(supports_type_introspection=lambda: False)
        with pytest.raises(SystemExit) as excinfo:
            introspect_cli._require_capability(
                SimpleNamespace(dialect=dialect), _args("types")
            )
        assert excinfo.value.code == 2
        assert "not supported by this server" in capsys.readouterr().err
