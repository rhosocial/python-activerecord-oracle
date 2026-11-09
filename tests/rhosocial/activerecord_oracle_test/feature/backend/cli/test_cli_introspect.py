# tests/rhosocial/activerecord_oracle_test/feature/backend/cli/test_cli_introspect.py
"""Offline black-box tests for the ``introspect`` CLI subcommand.

Parser contract plus every ``INTROSPECT_TYPES`` branch of ``handle()`` are
exercised against a fake introspector, covering table/view/column/index/
foreign-key/sequence/database paths, the missing-name guards, and the
error exits.
"""
import argparse
import json
from types import SimpleNamespace
from typing import Any, List, Optional

import pytest

from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.impl.oracle.cli import introspect as cli_introspect
from rhosocial.activerecord.backend.introspection.types import (
    ColumnDataProfile,
    ColumnValueFrequency,
    JoinCoverage,
)


class RecordingProvider:
    def __init__(self) -> None:
        self.results: List[Any] = []
        self.connection_errors: List[Exception] = []
        self.query_errors: List[Exception] = []

    def display_results(self, data: Any, title: Optional[str] = None) -> None:
        self.results.append((title, data))

    def display_connection_error(self, error: Exception) -> None:
        self.connection_errors.append(error)

    def display_query_error(self, error: Exception) -> None:
        self.query_errors.append(error)


TABLE_INFO = SimpleNamespace(
    columns=[{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "NAME"}],
    indexes=[{"INDEX_NAME": "SYS_C001"}],
    foreign_keys=[{"CONSTRAINT_NAME": "FK_ORD"}],
)

TRIGGERS = [
    {"TRIGGER_NAME": "TRG_CUSTOMERS_BI", "TABLE_NAME": "CUSTOMERS"},
    {"TRIGGER_NAME": "TRG_ORDERS_BI", "TABLE_NAME": "ORDERS"},
]

# The nine operational and data families. Four are listings over the schema's
# scope, one is the dependency graph, three are one-table questions and the
# ninth measures two columns against each other. The values are the shapes
# core hands back, so ``_serialize_for_output`` and the ``source`` column the
# data families carry are exercised rather than bypassed.
UNUSED_INDEXES = [{"INDEX_NAME": "SYS_C001", "TABLE_NAME": "CUSTOMERS"}]
TABLE_USAGE_STATS = [
    {"TABLE_NAME": "CUSTOMERS", "SEQUENTIAL_SCANS": 12, "INSERTS": 3}
]
RUNTIME_METRICS = [
    {"NAME": "SESSION_LOGICAL_READS", "VALUE": 812, "SOURCE": "since startup"}
]
INDEX_USAGE_STATS = [{"INDEX_NAME": "SYS_C001", "TABLE_NAME": "CUSTOMERS", "SCANS": 4}]
OBJECT_DEPENDENCIES = [
    {"DEPENDENT": "V_CUSTOMERS", "REFERENCED": "CUSTOMERS", "KIND": "VIEW"}
]
COLUMN_PROFILES = [
    ColumnDataProfile(
        schema="AR_CRM",
        table_name="CUSTOMERS",
        column_name="STATUS",
        null_fraction=0.0,
        distinct_count=3,
        row_count=1000,
        source="estimate",
    )
]
COLUMN_VALUES = [ColumnValueFrequency(value="ACTIVE", count=700)]
JOIN_COVERAGE = JoinCoverage(
    left_schema="AR_CRM",
    left_table="CUSTOMERS",
    left_column="ID",
    right_schema="AR_CRM",
    right_table="ORDERS",
    right_column="CUSTOMER_ID",
    left_coverage=1.0,
    right_coverage=0.75,
)


class FakeIntrospector:
    def __init__(self, table_exists: bool = True, object_exists: bool = True) -> None:
        self.table_exists = table_exists
        self.object_exists = object_exists
        self.calls: List[tuple] = []

    def list_tables(self, schema=None, include_system=False):
        self.calls.append(("tables", schema, include_system))
        return [{"TABLE_NAME": "CUSTOMERS", "OWNER": schema or "SYSTEM"}]

    def list_views(self, schema=None):
        self.calls.append(("views", schema))
        return [{"VIEW_NAME": "V_CUSTOMERS"}]

    def get_view_info(self, view_name, schema=None):
        self.calls.append(("view_info", view_name, schema))
        if not self.object_exists:
            return None
        return {"VIEW_NAME": view_name, "DEFINITION": "SELECT 1 FROM dual"}

    def get_table_info(self, name, schema=None):
        self.calls.append(("table_info", name, schema))
        return TABLE_INFO if self.table_exists else None

    def list_columns(self, name, schema=None):
        self.calls.append(("columns", name, schema))
        return TABLE_INFO.columns

    def list_indexes(self, name, schema=None):
        self.calls.append(("indexes", name, schema))
        return TABLE_INFO.indexes

    def list_foreign_keys(self, name, schema=None):
        self.calls.append(("foreign_keys", name, schema))
        return TABLE_INFO.foreign_keys

    def list_triggers(self, table_name=None, schema=None):
        self.calls.append(("triggers", table_name, schema))
        if table_name is None:
            return TRIGGERS
        return [t for t in TRIGGERS if t["TABLE_NAME"] == table_name.upper()]

    def list_sequences(self, owner=None):
        self.calls.append(("sequences", owner))
        return [{"SEQUENCE_NAME": "CUSTOMERS_SEQ"}]

    def get_sequence_info(self, name, owner=None):
        self.calls.append(("sequence_info", name, owner))
        if not self.object_exists:
            return None
        return {"SEQUENCE_NAME": name, "LAST_NUMBER": 42}

    def list_synonyms(self, owner=None):
        self.calls.append(("synonyms", owner))
        return [{"SYNONYM_NAME": "CUSTOMERS_SYN"}]

    def get_synonym_info(self, name, owner=None):
        self.calls.append(("synonym_info", name, owner))
        if not self.object_exists:
            return None
        return {"SYNONYM_NAME": name, "TABLE_NAME": "CUSTOMERS"}

    def list_packages(self, owner=None):
        self.calls.append(("packages", owner))
        return [{"OBJECT_NAME": "CUSTOMERS_PKG"}]

    def list_partitions(self, table_name, schema=None):
        self.calls.append(("partitions", table_name, schema))
        return [{"TABLE_NAME": table_name, "PARTITION_NAME": "P1"}]

    def list_constraints(self, table_name, schema=None):
        self.calls.append(("constraints", table_name, schema))
        return [{"TABLE_NAME": table_name, "CONSTRAINT_NAME": "PK_CUSTOMERS"}]

    def list_procedures(self, owner=None):
        self.calls.append(("procedures", owner))
        return [{"OBJECT_NAME": "CALCULATE_TAX"}]

    def get_procedure_info(self, name, owner=None):
        self.calls.append(("procedure_info", name, owner))
        if not self.object_exists:
            return None
        return {"OBJECT_NAME": name, "OBJECT_TYPE": "PROCEDURE"}

    def list_functions(self, owner=None):
        self.calls.append(("functions", owner))
        return [{"OBJECT_NAME": "CALCULATE_TOTAL"}]

    def get_function_info(self, name, owner=None):
        self.calls.append(("function_info", name, owner))
        if not self.object_exists:
            return None
        return {"OBJECT_NAME": name, "OBJECT_TYPE": "FUNCTION"}

    def list_types(self, owner=None):
        self.calls.append(("types", owner))
        return [{"TYPE_NAME": "ADDRESS_T"}]

    def get_type_info(self, name, owner=None):
        self.calls.append(("type_info", name, owner))
        if not self.object_exists:
            return None
        return {"TYPE_NAME": name, "TYPECODE": "OBJECT"}

    # ---- the nine operational and data families ------------------------
    # The four counter listings and the dependency graph answer for the
    # connection's whole scope; ``dependencies`` may be narrowed to one
    # object. The three one-table questions carry the table NAME through.

    def list_unused_indexes(self, schema=None):
        self.calls.append(("unused_indexes", schema))
        return UNUSED_INDEXES

    def list_table_usage_stats(self, schema=None):
        self.calls.append(("table_usage_stats", schema))
        return TABLE_USAGE_STATS

    def list_runtime_metrics(self, schema=None):
        self.calls.append(("runtime_metrics", schema))
        return RUNTIME_METRICS

    def list_index_usage_stats(self, schema=None):
        self.calls.append(("index_usage_stats", schema))
        return INDEX_USAGE_STATS

    def list_object_dependencies(self, object_name=None, schema=None):
        self.calls.append(("object_dependencies", object_name, schema))
        return OBJECT_DEPENDENCIES

    def list_column_data_profiles(self, table_name, schema=None):
        self.calls.append(("column_data_profiles", table_name, schema))
        return COLUMN_PROFILES

    def sample_column_values(self, table_name, column_name, limit=100, schema=None):
        self.calls.append(("value_samples", table_name, column_name, limit, schema))
        return COLUMN_VALUES

    def estimate_join_coverage(
        self, left_table, left_column, right_table, right_column, schema=None
    ):
        self.calls.append((
            "join_coverage", left_table, left_column, right_table, right_column,
            schema,
        ))
        return JOIN_COVERAGE

    def get_database_info(self):
        self.calls.append(("database",))
        return {"name": "FREEPDB1", "vendor": "Oracle"}


def build_parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser()
    subparsers = top.add_subparsers(dest="command")
    cli_introspect.create_parser(subparsers)
    return top


@pytest.fixture
def provider(monkeypatch) -> RecordingProvider:
    rec = RecordingProvider()
    monkeypatch.setattr(
        cli_introspect, "create_provider", lambda fmt, ascii_borders=False: rec
    )
    return rec


def patch_sync_backend(monkeypatch, introspector: Any,
                       connect_error: Optional[Exception] = None,
                       dialect: Any = None) -> List[Any]:
    instances: List[Any] = []

    class FakeSyncBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.connection_config = connection_config
            self.introspector = introspector
            #: The capability switches are asked of this, once adapted. A stub
            #: without one is the offline default (nothing to gate); a test
            #: that passes one exercises the gate.
            self.dialect = dialect
            self._connection: Optional[object] = object()
            self.disconnected = False
            #: Set when the CLI adapts the dialect to the server it reached.
            #: The introspect command must do that before its first question,
            #: because the version-gated answers depend on it.
            self.adapted = False
            self.calls: List[str] = []
            instances.append(self)

        def connect(self) -> None:
            self.calls.append("connect")
            if connect_error is not None:
                raise connect_error

        def introspect_and_adapt(self) -> None:
            self.calls.append("introspect_and_adapt")
            self.adapted = True

        def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(cli_introspect, "OracleBackend", FakeSyncBackend)
    return instances


def patch_async_backend(monkeypatch, introspector: Any,
                        dialect: Any = None) -> List[Any]:
    """Async fake whose introspector mirrors the real async contract.

    The async introspector keeps the sync method names (no ``_async``
    suffix) and is awaited, so the facade exposes exactly those names.
    """
    instances: List[Any] = []

    class AsyncFacade:
        def __init__(self, sync: Any) -> None:
            for attr in ("list_tables", "list_views", "get_table_info",
                         "list_columns", "list_indexes", "list_foreign_keys",
                         "list_triggers", "get_view_info", "list_sequences",
                         "get_sequence_info", "list_synonyms",
                         "get_synonym_info", "list_packages",
                         "list_partitions", "list_constraints",
                         "list_procedures", "get_procedure_info",
                         "list_functions", "get_function_info",
                         "list_types", "get_type_info",
                         "get_database_info",
                         "list_unused_indexes", "list_table_usage_stats",
                         "list_runtime_metrics", "list_index_usage_stats",
                         "list_object_dependencies",
                         "list_column_data_profiles", "sample_column_values",
                         "estimate_join_coverage"):
                fn = getattr(sync, attr)

                async def call(*a: Any, _fn: Any = fn, **k: Any) -> Any:
                    return _fn(*a, **k)

                setattr(self, attr, call)

    class FakeAsyncBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.connection_config = connection_config
            self.introspector = AsyncFacade(introspector)
            self.dialect = dialect
            self._connection: Optional[object] = object()
            self.disconnected = False
            self.adapted = False
            self.calls: List[str] = []
            instances.append(self)

        async def connect(self) -> None:
            self.calls.append("connect")

        async def introspect_and_adapt(self) -> None:
            self.calls.append("introspect_and_adapt")
            self.adapted = True

        async def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(cli_introspect, "AsyncOracleBackend", FakeAsyncBackend)
    return instances


class TestIntrospectParserContract:
    def test_defaults(self):
        args = build_parser().parse_args(["introspect", "tables"])
        assert args.type == "tables"
        assert args.name is None
        assert args.include_system is False
        assert args.output == "table"
        assert args.is_async is False

    def test_name_and_flags(self):
        args = build_parser().parse_args([
            "introspect", "columns", "customers",
            "--include-system", "-o", "json",
        ])
        assert args.type == "columns"
        assert args.name == "customers"
        assert args.include_system is True
        assert args.output == "json"

    def test_owner_flag_is_refused(self):
        """The connection fixes the owner, so a second owner argument is gone."""
        with pytest.raises(SystemExit):
            build_parser().parse_args(
                ["introspect", "tables", "--owner", "AR_CRM"]
            )

    def test_help_lists_every_type_and_no_owner_option(self, capsys):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["introspect", "--help"])
        out = capsys.readouterr().out
        for itype in cli_introspect.INTROSPECT_TYPES:
            assert itype in out
        assert "--owner" not in out

    def test_all_introspect_types_accepted(self):
        for itype in cli_introspect.INTROSPECT_TYPES:
            args = build_parser().parse_args(["introspect", itype])
            assert args.type == itype

    def test_sequences_is_oracle_specific_type(self):
        assert "sequences" in cli_introspect.INTROSPECT_TYPES

    def test_schema_object_types_are_oracle_specific(self):
        for itype in (
            "sequences", "synonyms", "packages", "partitions",
            "procedures", "functions", "types", "constraints",
        ):
            assert itype in cli_introspect.INTROSPECT_TYPES

    def test_triggers_is_a_declared_type(self):
        """The dialect declared trigger introspection all along; the word is here."""
        assert "triggers" in cli_introspect.INTROSPECT_TYPES

    def test_invalid_type_rejected(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["introspect", "grants"])

    def test_ddl_is_rejected_as_an_unknown_choice(self, capsys):
        """DDL extraction is permanently unanswerable, so ``ddl`` is not a type.

        The parser refuses the word before anything connects, with the same
        exit code argparse gives any other invalid choice.
        """
        with pytest.raises(SystemExit) as excinfo:
            build_parser().parse_args(["introspect", "ddl", "customers"])
        assert excinfo.value.code == 2
        assert "invalid choice" in capsys.readouterr().err
        assert "ddl" not in cli_introspect.INTROSPECT_TYPES

    def test_connection_defaults_flow_through(self):
        args = build_parser().parse_args([
            "introspect", "tables", "--host", "h", "--port", "1234",
            "--service", "s", "--user", "u", "--password", "p",
        ])
        assert (args.host, args.port, args.service) == ("h", 1234, "s")
        assert (args.user, args.password) == ("u", "p")


class TestNameRules:
    """NAME means a table for some types and an object for others."""

    def test_every_declared_type_has_a_name_rule(self):
        """A new type must say what it does with NAME, or it is not complete."""
        assert set(cli_introspect._NAME_RULE) == set(cli_introspect.INTROSPECT_TYPES)

    def test_the_table_says_what_each_types_name_means(self):
        """The table as data, so editing one type is a visible change.

        A set nobody asserts the members of is still a set whose answers can
        drift one at a time, which is why the contents are pinned beside the
        set-level check above.
        """
        assert cli_introspect._NAME_RULE == {
            "columns": "required",
            "indexes": "required",
            "foreign-keys": "required",
            "constraints": "required",
            "triggers": "optional",
            "partitions": "required",
            "views": "optional",
            "sequences": "optional",
            "synonyms": "optional",
            "procedures": "optional",
            "functions": "optional",
            "types": "optional",
            "tables": "forbidden",
            "packages": "forbidden",
            "unused-indexes": "forbidden",
            "table-stats": "forbidden",
            "runtime": "forbidden",
            "index-stats": "forbidden",
            "dependencies": "optional",
            "column-profiles": "required",
            "value-samples": "required",
            "join-coverage": "required",
            "database": "forbidden",
        }

    @pytest.mark.parametrize(
        "itype",
        sorted(
            k for k, rule in cli_introspect._NAME_RULE.items() if rule == "forbidden"
        ),
    )
    def test_a_refused_type_is_refused_by_the_table(self, itype, capsys):
        """Each category, read through the one validator that reads the table.

        The per-branch copies are gone, so this is the table's own behaviour
        rather than a branch that happens to remember it.
        """
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect._validate_name(
                argparse.Namespace(type=itype, name="customers")
            )
        assert excinfo.value.code == 2
        assert f"Error: {itype} introspection takes no name" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "itype",
        sorted(
            k for k, rule in cli_introspect._NAME_RULE.items() if rule == "required"
        ),
    )
    def test_a_required_name_is_required_by_the_table(self, itype, capsys):
        """A missing required NAME is an incomplete invocation, exit 1.

        The noun is the type's own: NAME is the *left* table of the pair for
        ``join-coverage`` and "the table" for the rest, which is a wording
        choice rather than a second decision about whether it is required --
        the rule is read from the table either way.
        """
        noun = "Left table name" if itype == "join-coverage" else "Table name"
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect._validate_name(argparse.Namespace(type=itype, name=None))
        assert excinfo.value.code == 1
        assert f"Error: {noun} is required for '{itype}'" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "itype",
        sorted(
            k for k, rule in cli_introspect._NAME_RULE.items() if rule == "optional"
        ),
    )
    def test_an_optional_name_may_be_given_or_not(self, itype):
        cli_introspect._validate_name(argparse.Namespace(type=itype, name=None))
        cli_introspect._validate_name(argparse.Namespace(type=itype, name="customers"))

    def test_help_states_what_name_means(self, capsys):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["introspect", "--help"])
        out = capsys.readouterr().out
        # The trigger rule is the one that is easiest to misread: NAME is the
        # table, not the trigger.
        assert "NAME is the table whose triggers" in out
        assert "take no NAME" in out

    @pytest.mark.parametrize("itype", ["tables", "packages", "database"])
    def test_a_name_the_type_cannot_use_is_refused(self, capsys, monkeypatch,
                                                   itype):
        """A dropped NAME looks like a filter; it is refused before connecting."""
        instances = patch_sync_backend(monkeypatch, FakeIntrospector())
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", itype, "some_name"]
            ))
        assert excinfo.value.code == 2
        assert "takes no name" in capsys.readouterr().err
        assert instances == []


class TestHandleSyncBranches:
    @pytest.mark.parametrize("itype,name,title,data_key,value", [
        ("tables", None, "Tables", None, "CUSTOMERS"),
        ("views", None, "Views", None, "V_CUSTOMERS"),
        ("sequences", None, "Sequences", None, "CUSTOMERS_SEQ"),
        ("synonyms", None, "Synonyms", None, "CUSTOMERS_SYN"),
        ("packages", None, "Packages", None, "CUSTOMERS_PKG"),
    ])
    def test_list_branches(self, provider, monkeypatch, itype, name, title,
                           data_key, value):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        argv = ["introspect", itype]
        if name:
            argv += [name]
        cli_introspect.handle(build_parser().parse_args(argv))
        assert [t for t, _ in provider.results] == [title]
        assert value in str(provider.results[0][1])

    def test_tables_passes_owner_none_and_include_system(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "tables", "--include-system"]
        ))
        assert insp.calls[-1] == ("tables", None, True)

    def test_the_dialect_is_adapted_after_connecting(self, provider, monkeypatch):
        """Adaptation comes right after the connection and before any question.

        The dialect's version is what the version-gated answers and the
        reported database version depend on, so the command may not ask the
        dictionary anything until it has been adapted to the server it reached.
        """
        instances = patch_sync_backend(monkeypatch, FakeIntrospector())
        cli_introspect.handle(build_parser().parse_args(["introspect", "tables"]))
        assert instances[0].calls == ["connect", "introspect_and_adapt"]
        assert instances[0].adapted is True

    def test_schema_object_branches_pass_owner_none(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        for itype in ("sequences", "synonyms", "packages"):
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", itype]
            ))
        assert insp.calls == [
            ("sequences", None),
            ("synonyms", None),
            ("packages", None),
        ]

    @pytest.mark.parametrize("itype", ["columns", "indexes",
                                       "foreign-keys"])
    def test_missing_name_guards_exit_nonzero(self, provider, capsys, monkeypatch,
                                              itype):
        patch_sync_backend(monkeypatch, FakeIntrospector())
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(["introspect", itype]))
        assert excinfo.value.code == 1
        assert "Table name is required" in capsys.readouterr().err

    def test_columns_branch_uses_default_schema(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "columns", "customers"]
        ))
        assert insp.calls[-1] == ("columns", "customers", None)
        assert provider.results[0][0] == "Columns of customers"

    def test_indexes_branch(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "indexes", "customers"]
        ))
        assert provider.results[0][0] == "Indexes of customers"

    def test_foreign_keys_branch(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "foreign-keys", "orders"]
        ))
        assert provider.results[0][0] == "Foreign Keys of orders"

    def test_triggers_branch_lists_every_trigger(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(["introspect", "triggers"]))
        assert insp.calls[-1] == ("triggers", None, None)
        assert provider.results[0][0] == "Triggers"
        assert "TRG_ORDERS_BI" in str(provider.results[0][1])

    def test_triggers_name_is_the_table(self, provider, monkeypatch):
        """``triggers NAME`` asks for the triggers *on* NAME, not a trigger NAME.

        This is the opposite of ``views NAME``; the filter must reach
        ``list_triggers(table_name=...)`` so the list narrows to that table.
        """
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "triggers", "customers"]
        ))
        assert insp.calls[-1] == ("triggers", "customers", None)
        data = str(provider.results[0][1])
        assert "TRG_CUSTOMERS_BI" in data
        assert "TRG_ORDERS_BI" not in data

    def test_views_name_describes_one_view(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "views", "active_customers"]
        ))
        assert insp.calls[-1] == ("view_info", "active_customers", None)
        assert provider.results[0][0] == "View active_customers"
        assert "SELECT 1 FROM dual" in str(provider.results[0][1])

    @pytest.mark.parametrize("itype,title", [
        ("views", "View active_customers"),
        ("sequences", "Sequence customers_seq"),
        ("synonyms", "Synonym customers"),
    ])
    def test_catalogue_name_describes_one_object(self, provider, monkeypatch,
                                                 itype, title):
        names = {"views": "active_customers", "sequences": "customers_seq",
                 "synonyms": "customers"}
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", itype, names[itype]]
        ))
        assert provider.results[0][0] == title

    @pytest.mark.parametrize("itype", ["views", "sequences", "synonyms"])
    def test_missing_catalogue_object_exits_one(self, capsys, provider,
                                                monkeypatch, itype):
        """A named object that is absent is a runtime answer, so exit 1."""
        patch_sync_backend(monkeypatch, FakeIntrospector(object_exists=False))
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", itype, "nope"]
            ))
        assert excinfo.value.code == 1
        assert "not found" in capsys.readouterr().err

    def test_sequences_name_passes_the_owner_through(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "sequences", "customers_seq"]
        ))
        assert insp.calls[-1] == ("sequence_info", "customers_seq", None)

    def test_synonyms_name_passes_the_owner_through(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "synonyms", "customers"]
        ))
        assert insp.calls[-1] == ("synonym_info", "customers", None)

    def test_database_branch_wraps_in_list(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(["introspect", "database"]))
        assert provider.results == [("Database Info",
                                     [{"name": "FREEPDB1", "vendor": "Oracle"}])]

    def test_json_output_prints_serialised_rows(self, capsys, monkeypatch):
        monkeypatch.setattr(
            cli_introspect, "create_provider",
            lambda fmt, ascii_borders=False: _real_provider(fmt),
        )
        patch_sync_backend(monkeypatch, FakeIntrospector())
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "tables", "-o", "json"]
        ))
        payload = json.loads(capsys.readouterr().out)
        assert payload[0]["TABLE_NAME"] == "CUSTOMERS"

    def test_connection_error_exits_nonzero(self, provider, monkeypatch):
        patch_sync_backend(
            monkeypatch, FakeIntrospector(),
            connect_error=ConnectionError("ORA-01017"),
        )
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(["introspect", "tables"]))
        assert excinfo.value.code == 1
        assert provider.connection_errors

    def test_query_error_exits_nonzero(self, provider, monkeypatch):
        class Exploding(FakeIntrospector):
            def list_tables(self, schema=None, include_system=False):
                raise QueryError("ORA-00942")

        patch_sync_backend(monkeypatch, Exploding())
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(["introspect", "tables"]))
        assert excinfo.value.code == 1
        assert provider.query_errors

    def test_unexpected_error_writes_stderr(self, capsys, provider, monkeypatch):
        class Exploding(FakeIntrospector):
            def list_tables(self, schema=None, include_system=False):
                raise RuntimeError("boom")

        patch_sync_backend(monkeypatch, Exploding())
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(["introspect", "tables"]))
        assert excinfo.value.code == 1
        assert "Error during introspection" in capsys.readouterr().err


class TestHandleAsyncBranches:
    @pytest.mark.parametrize("itype,name,title", [
        ("tables", None, "Tables"),
        ("views", None, "Views"),
        ("views", "active_customers", "View active_customers"),
        ("columns", "customers", "Columns of customers"),
        ("indexes", "customers", "Indexes of customers"),
        ("foreign-keys", "orders", "Foreign Keys of orders"),
        ("constraints", "orders", "Constraints of orders"),
        ("partitions", "orders", "Partitions of orders"),
        ("triggers", None, "Triggers"),
        ("triggers", "customers", "Triggers"),
        ("sequences", None, "Sequences"),
        ("sequences", "customers_seq", "Sequence customers_seq"),
        ("synonyms", None, "Synonyms"),
        ("synonyms", "customers", "Synonym customers"),
        ("packages", None, "Packages"),
        ("procedures", None, "Procedures"),
        ("procedures", "calculate_tax", "Procedure calculate_tax"),
        ("functions", None, "Functions"),
        ("functions", "calculate_total", "Function calculate_total"),
        ("types", None, "Types"),
        ("types", "address_t", "Type address_t"),
        ("unused-indexes", None, "Unused Indexes"),
        ("table-stats", None, "Table Usage Stats"),
        ("runtime", None, "Runtime Metrics"),
        ("index-stats", None, "Index Usage Stats"),
        ("dependencies", None, "Object Dependencies"),
        ("dependencies", "customers", "Dependencies of customers"),
        ("column-profiles", "customers", "Column profiles of customers"),
        ("value-samples", "customers", "Values of customers.status"),
        ("join-coverage", "customers", "Join Coverage"),
    ])
    def test_async_branches(self, provider, monkeypatch, itype, name, title):
        insp = FakeIntrospector()
        patch_async_backend(monkeypatch, insp)
        argv = ["introspect", itype, "--async", "-o", "json"]
        if name:
            argv.insert(2, name)
        if itype == "value-samples":
            argv += ["--column", "status"]
        if itype == "join-coverage":
            argv += [
                "--column", "id", "--right-table", "orders", "--right-column",
                "customer_id",
            ]
        cli_introspect.handle(build_parser().parse_args(argv))
        assert [t for t, _ in provider.results][0] == title

    def test_async_database_branch(self, provider, monkeypatch):
        patch_async_backend(monkeypatch, FakeIntrospector())
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "database", "--async"]
        ))
        assert provider.results[0][0] == "Database Info"

    def test_async_dialect_is_adapted_after_connecting(self, provider, monkeypatch):
        """The async path adapts too, awaited, before its first question."""
        instances = patch_async_backend(monkeypatch, FakeIntrospector())
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "database", "--async"]
        ))
        assert instances[0].calls == ["connect", "introspect_and_adapt"]
        assert instances[0].adapted is True


class TestIntrospectTypeGates:
    """Every type the CLI advertises is a switch, a branch, and a help line.

    The gates exist so the next family cannot be added in one place and
    forgotten in another: the advertised list, the capability table, the
    branches and the help text all have to agree.
    """

    #: One runnable argument list per declared type. The test is parametrised
    #: over ``INTROSPECT_TYPES`` and looks the entry up here, so a type that is
    #: declared without a branch entry fails as a KeyError instead of going
    #: unnoticed.
    TYPE_ARGS = {
        "tables": [],
        "views": [],
        "columns": ["customers"],
        "indexes": ["customers"],
        "foreign-keys": ["orders"],
        "constraints": ["orders"],
        "triggers": ["customers"],
        "partitions": ["orders"],
        "sequences": [],
        "synonyms": [],
        "packages": [],
        "procedures": [],
        "functions": [],
        "types": [],
        "database": [],
        "unused-indexes": [],
        "table-stats": [],
        "runtime": [],
        "index-stats": [],
        "dependencies": [],
        "column-profiles": ["customers"],
        "value-samples": ["customers", "--column", "status"],
        "join-coverage": [
            "customers", "--column", "id",
            "--right-table", "orders", "--right-column", "customer_id",
        ],
    }

    def test_the_advertised_list_is_the_capability_table(self):
        assert list(cli_introspect._TYPE_CAPABILITIES) == cli_introspect.INTROSPECT_TYPES

    def test_no_type_outlives_its_removal(self):
        """``table`` was removed on 2026-10-27; a name here would revive it."""
        assert "table" not in cli_introspect.INTROSPECT_TYPES

    def test_every_switch_names_a_real_dialect_predicate(self):
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = OracleDialect(version=(21, 0, 0))
        for itype, switch in cli_introspect._TYPE_CAPABILITIES.items():
            predicate = getattr(dialect, switch, None)
            assert callable(predicate), f"{itype} -> {switch} is not a dialect method"

    @pytest.mark.parametrize("itype", cli_introspect.INTROSPECT_TYPES)
    def test_every_declared_type_lands_in_a_branch(self, provider, monkeypatch,
                                                   itype):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", itype] + self.TYPE_ARGS[itype]
        ))
        assert len(provider.results) == 1
        assert insp.calls

    def test_a_type_the_server_cannot_answer_is_refused(self, provider, capsys,
                                                        monkeypatch):
        """The claim is checked against the connected dialect, not assumed."""
        dialect = SimpleNamespace(supports_trigger_introspection=lambda: False)
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp, dialect=dialect)
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", "triggers"]
            ))
        assert excinfo.value.code == 2
        assert capsys.readouterr().err.strip() == (
            "Error: triggers introspection is not supported by this server"
        )
        assert insp.calls == []

    def test_the_async_path_refuses_too(self, provider, capsys, monkeypatch):
        dialect = SimpleNamespace(supports_trigger_introspection=lambda: False)
        insp = FakeIntrospector()
        patch_async_backend(monkeypatch, insp, dialect=dialect)
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", "triggers", "--async"]
            ))
        assert excinfo.value.code == 2
        assert "not supported by this server" in capsys.readouterr().err
        assert insp.calls == []


def _real_provider(fmt: str) -> Any:
    from rhosocial.activerecord.backend.output import JsonOutputProvider

    return JsonOutputProvider()
