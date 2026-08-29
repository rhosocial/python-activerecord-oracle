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


class FakeIntrospector:
    def __init__(self, table_exists: bool = True) -> None:
        self.table_exists = table_exists
        self.calls: List[tuple] = []

    def list_tables(self, schema=None, include_system=False):
        self.calls.append(("tables", schema, include_system))
        return [{"TABLE_NAME": "CUSTOMERS", "OWNER": schema or "SYSTEM"}]

    def list_views(self, schema=None):
        self.calls.append(("views", schema))
        return [{"VIEW_NAME": "V_CUSTOMERS"}]

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

    def list_sequences(self, schema=None):
        self.calls.append(("sequences", schema))
        return [{"SEQUENCE_NAME": "CUSTOMERS_SEQ"}]

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
                       connect_error: Optional[Exception] = None) -> List[Any]:
    instances: List[Any] = []

    class FakeSyncBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.connection_config = connection_config
            self.introspector = introspector
            self._connection: Optional[object] = object()
            self.disconnected = False
            instances.append(self)

        def connect(self) -> None:
            if connect_error is not None:
                raise connect_error

        def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(cli_introspect, "OracleBackend", FakeSyncBackend)
    return instances


def patch_async_backend(monkeypatch, introspector: Any) -> List[Any]:
    """Async fake whose introspector exposes only the ``*_async`` methods."""
    instances: List[Any] = []

    class AsyncFacade:
        def __init__(self, sync: Any) -> None:
            for attr in ("list_tables", "list_views", "get_table_info",
                         "list_columns", "list_indexes", "list_foreign_keys",
                         "list_sequences", "get_database_info"):
                fn = getattr(sync, attr)

                async def call(*a: Any, _fn: Any = fn, **k: Any) -> Any:
                    return _fn(*a, **k)

                setattr(self, f"{attr}_async", call)

    class FakeAsyncBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.connection_config = connection_config
            self.introspector = AsyncFacade(introspector)
            self._connection: Optional[object] = object()
            self.disconnected = False
            instances.append(self)

        async def connect(self) -> None:
            pass

        async def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(cli_introspect, "AsyncOracleBackend", FakeAsyncBackend)
    return instances


class TestIntrospectParserContract:
    def test_defaults(self):
        args = build_parser().parse_args(["introspect", "tables"])
        assert args.type == "tables"
        assert args.name is None
        assert args.owner is None
        assert args.include_system is False
        assert args.output == "table"
        assert args.is_async is False

    def test_name_owner_and_flags(self):
        args = build_parser().parse_args([
            "introspect", "table", "customers",
            "--owner", "AR_CRM", "--include-system", "-o", "json",
        ])
        assert args.type == "table"
        assert args.name == "customers"
        assert args.owner == "AR_CRM"
        assert args.include_system is True
        assert args.output == "json"

    def test_all_introspect_types_accepted(self):
        for itype in cli_introspect.INTROSPECT_TYPES:
            args = build_parser().parse_args(["introspect", itype])
            assert args.type == itype

    def test_sequences_is_oracle_specific_type(self):
        assert "sequences" in cli_introspect.INTROSPECT_TYPES

    def test_invalid_type_rejected(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["introspect", "grants"])

    def test_connection_defaults_flow_through(self):
        args = build_parser().parse_args([
            "introspect", "tables", "--host", "h", "--port", "1234",
            "--service", "s", "--user", "u", "--password", "p",
        ])
        assert (args.host, args.port, args.service) == ("h", 1234, "s")
        assert (args.user, args.password) == ("u", "p")


class TestHandleSyncBranches:
    @pytest.mark.parametrize("itype,name,title,data_key,value", [
        ("tables", None, "Tables", None, "CUSTOMERS"),
        ("views", None, "Views", None, "V_CUSTOMERS"),
        ("sequences", None, "Sequences", None, "CUSTOMERS_SEQ"),
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

    def test_tables_passes_schema_and_include_system(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "tables", "--owner", "AR_SHOP", "--include-system"]
        ))
        assert insp.calls[-1] == ("tables", "AR_SHOP", True)

    def test_table_detail_renders_columns_indexes_fks(self, provider, monkeypatch):
        insp = FakeIntrospector()
        backend_instances = patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "table", "customers", "--owner", "ar_crm"]
        ))
        titles = [t for t, _ in provider.results]
        assert titles == [
            "Columns of customers",
            "Indexes of customers",
            "Foreign Keys of customers",
        ]
        assert insp.calls[-1] == ("table_info", "customers", "ar_crm")
        assert backend_instances[-1].disconnected

    def test_table_without_fks_skips_fk_section(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        TABLE_INFO.foreign_keys = []
        try:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", "table", "customers"]
            ))
            titles = [t for t, _ in provider.results]
            assert "Foreign Keys of customers" not in titles
            assert len(titles) == 2
        finally:
            TABLE_INFO.foreign_keys = [{"CONSTRAINT_NAME": "FK_ORD"}]

    def test_table_not_found_exits_nonzero(self, provider, capsys, monkeypatch):
        patch_sync_backend(monkeypatch, FakeIntrospector(table_exists=False))
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", "table", "ghost"]
            ))
        assert excinfo.value.code == 1
        assert "not found" in capsys.readouterr().err

    @pytest.mark.parametrize("itype", ["table", "columns", "indexes",
                                       "foreign-keys"])
    def test_missing_name_guards_exit_nonzero(self, provider, capsys, monkeypatch,
                                              itype):
        patch_sync_backend(monkeypatch, FakeIntrospector())
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(["introspect", itype]))
        assert excinfo.value.code == 1
        assert "Table name is required" in capsys.readouterr().err

    def test_columns_branch_uses_schema(self, provider, monkeypatch):
        insp = FakeIntrospector()
        patch_sync_backend(monkeypatch, insp)
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "columns", "customers", "--owner", "AR_CRM"]
        ))
        assert insp.calls[-1] == ("columns", "customers", "AR_CRM")
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
        ("table", "customers", "Columns of customers"),
        ("columns", "customers", "Columns of customers"),
        ("indexes", "customers", "Indexes of customers"),
        ("foreign-keys", "orders", "Foreign Keys of orders"),
        ("sequences", None, "Sequences"),
    ])
    def test_async_branches(self, provider, monkeypatch, itype, name, title):
        insp = FakeIntrospector()
        patch_async_backend(monkeypatch, insp)
        argv = ["introspect", itype, "--async"]
        if name:
            argv.insert(2, name)
        cli_introspect.handle(build_parser().parse_args(argv))
        assert [t for t, _ in provider.results][0] == title

    def test_async_table_not_found_exits(self, provider, capsys, monkeypatch):
        patch_async_backend(monkeypatch, FakeIntrospector(table_exists=False))
        with pytest.raises(SystemExit) as excinfo:
            cli_introspect.handle(build_parser().parse_args(
                ["introspect", "table", "ghost", "--async"]
            ))
        assert excinfo.value.code == 1
        assert "not found" in capsys.readouterr().err

    def test_async_database_branch(self, provider, monkeypatch):
        patch_async_backend(monkeypatch, FakeIntrospector())
        cli_introspect.handle(build_parser().parse_args(
            ["introspect", "database", "--async"]
        ))
        assert provider.results[0][0] == "Database Info"


def _real_provider(fmt: str) -> Any:
    from rhosocial.activerecord.backend.output import JsonOutputProvider

    return JsonOutputProvider()
