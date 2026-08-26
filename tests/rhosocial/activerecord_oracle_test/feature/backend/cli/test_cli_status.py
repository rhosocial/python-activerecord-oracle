# tests/rhosocial/activerecord_oracle_test/feature/backend/cli/test_cli_status.py
"""Offline black-box tests for the ``status`` CLI subcommand.

The parser contract (``create_parser`` + ``add_connection_args``) and the
``handle()`` dispatch are exercised against fake backends so every
``STATUS_TYPES`` branch, output format, and error path is covered without
a live server.
"""
import argparse
import dataclasses
import json
from enum import Enum
from typing import Any, Dict, List, Optional

import pytest

from types import SimpleNamespace

from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.impl.oracle.cli import status as cli_status


class _Mode(Enum):
    OPEN = "OPEN"


@dataclasses.dataclass
class _Session:
    user: str = "SYSTEM"
    database: str = "FREEPDB1"
    host: str = "db.local"


@dataclasses.dataclass
class _Overview:
    server_version: str = "Oracle Database 23ai"
    server_vendor: str = "Oracle"
    mode: _Mode = _Mode.OPEN
    tags: List[str] = dataclasses.field(default_factory=lambda: ["a", "b"])
    session: Optional[_Session] = None


class FakeStatusIntrospector:
    def __init__(self, include_tablespaces_attr: bool = True) -> None:
        self.include_tablespaces_attr = include_tablespaces_attr
        self.overview: Any = _Overview(session=_Session())

    def get_overview(self) -> Any:
        return self.overview

    def list_configuration(self, category: Any = None) -> List[Dict[str, str]]:
        return [{"name": "open_cursors", "value": "300"}]

    def get_connection_info(self) -> Dict[str, str]:
        return {"user": "SYSTEM", "database": "FREEPDB1"}

    def get_storage_info(self) -> Dict[str, int]:
        return {"sessions": 7}

    def list_users(self) -> List[Dict[str, str]]:
        return [{"username": "SYSTEM"}]

    def list_tablespaces(self) -> List[Dict[str, str]]:
        if not self.include_tablespaces_attr:
            raise AssertionError("list_tablespaces must not be called when absent")
        return [{"name": "USERS", "size_mb": "1024"}]


class _NoTablespacesIntrospector:
    """Mimics an introspector without ``list_tablespaces``."""

    overview: Any = _Overview()

    def list_users(self) -> List[Dict[str, str]]:
        return []


def make_fake_status() -> FakeStatusIntrospector:
    return FakeStatusIntrospector()


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


@pytest.fixture
def recording_provider(monkeypatch) -> RecordingProvider:
    rec = RecordingProvider()
    monkeypatch.setattr(
        cli_status, "create_provider", lambda fmt, ascii_borders=False: rec
    )
    return rec


def build_parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser()
    subparsers = top.add_subparsers(dest="command")
    cli_status.create_parser(subparsers)
    return top


def patch_sync_backend(
    monkeypatch: pytest.MonkeyPatch,
    status_introspector: Any,
    connect_error: Optional[Exception] = None,
    query_error: Optional[Exception] = None,
    unexpected_error: Optional[Exception] = None,
) -> List[Any]:
    instances: List[Any] = []

    class FakeSyncBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.connection_config = connection_config
            self.introspector = SimpleNamespace(status=status_introspector)
            self._connection: Optional[object] = object()
            self.connected = False
            self.disconnected = False
            instances.append(self)

        def connect(self) -> None:
            if connect_error is not None:
                raise connect_error
            self.connected = True

        def introspect_and_adapt(self) -> None:
            if unexpected_error is not None:
                raise unexpected_error
            if query_error is not None:
                raise query_error

        def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(cli_status, "OracleBackend", FakeSyncBackend)
    return instances


class AsyncProxy:
    """Wraps a sync fake so every method becomes awaitable."""

    def __init__(self, target: Any) -> None:
        self._target = target

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)

        if not callable(attr):
            return attr

        async def call(*args: Any, **kwargs: Any) -> Any:
            return attr(*args, **kwargs)

        return call


def patch_async_backend(
    monkeypatch: pytest.MonkeyPatch,
    status_introspector: Any,
    connect_error: Optional[Exception] = None,
) -> List[Any]:
    instances: List[Any] = []

    class FakeAsyncBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.connection_config = connection_config
            self.introspector = SimpleNamespace(status=AsyncProxy(status_introspector))
            self._connection: Optional[object] = object()
            self.connected = False
            self.disconnected = False
            instances.append(self)

        async def connect(self) -> None:
            if connect_error is not None:
                raise connect_error
            self.connected = True

        async def introspect_and_adapt(self) -> None:
            pass

        async def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(cli_status, "AsyncOracleBackend", FakeAsyncBackend)
    return instances


class TestStatusParserContract:
    def test_defaults_without_env(self, monkeypatch):
        for var in ("ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER",
                    "ORACLE_PASSWORD"):
            monkeypatch.delenv(var, raising=False)
        args = build_parser().parse_args(["status"])
        assert args.type == "all"
        assert args.output == "table"
        assert args.host == "localhost"
        assert args.port == 1521
        assert args.service == "ORCL"
        assert args.user == "system"
        assert args.password == ""
        assert args.ssl == "auto"
        assert args.is_async is False
        assert args.verbose == 0
        assert args.rich_ascii is False
        assert args.named_connection is None
        assert args.connection_params == []
        assert args.mode == "thin"

    def test_env_defaults_are_honoured(self, monkeypatch):
        monkeypatch.setenv("ORACLE_HOST", "env-host")
        monkeypatch.setenv("ORACLE_PORT", "11523")
        monkeypatch.setenv("ORACLE_SERVICE", "ENVSVC")
        monkeypatch.setenv("ORACLE_USER", "envuser")
        monkeypatch.setenv("ORACLE_PASSWORD", "envpass")
        args = build_parser().parse_args(["status"])
        assert args.host == "env-host"
        assert args.port == 11523
        assert args.service == "ENVSVC"
        assert args.user == "envuser"
        assert args.password == "envpass"

    def test_explicit_args_override_env(self, monkeypatch):
        monkeypatch.setenv("ORACLE_HOST", "env-host")
        monkeypatch.setenv("ORACLE_PORT", "11523")
        args = build_parser().parse_args(["status", "--host", "cli-host",
                                          "--port", "9999"])
        assert args.host == "cli-host"
        assert args.port == 9999

    def test_database_alias_maps_to_service_dest(self):
        args = build_parser().parse_args(["status", "--database", "MYSVC"])
        assert args.service == "MYSVC"

    def test_all_status_types_parse(self):
        for status_type in cli_status.STATUS_TYPES:
            args = build_parser().parse_args(["status", status_type])
            assert args.type == status_type

    def test_invalid_status_type_rejected(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["status", "bogus"])

    def test_invalid_ssl_choice_rejected(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["status", "--ssl", "bogus"])

    def test_ssl_choices_accepted(self):
        for mode in ("auto", "require", "verify-ca", "verify-full", "disabled"):
            args = build_parser().parse_args(["status", "--ssl", mode])
            assert args.ssl == mode

    def test_verbose_counts_and_conn_params_accumulate(self):
        args = build_parser().parse_args([
            "status", "-v", "-v",
            "--conn-param", "pool_size=10",
            "--conn-param", "stmtcachesize=5",
        ])
        assert args.verbose == 2
        assert args.connection_params == ["pool_size=10", "stmtcachesize=5"]

    def test_async_flag_sets_is_async(self):
        args = build_parser().parse_args(["status", "--async"])
        assert args.is_async is True


class TestResolveConnectionConfig:
    def test_explicit_values_flow_into_config(self, monkeypatch):
        for var in ("ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER",
                    "ORACLE_PASSWORD"):
            monkeypatch.delenv(var, raising=False)
        args = build_parser().parse_args([
            "status", "--host", "h1", "--port", "1234",
            "--service", "svc1", "--user", "u1", "--password", "p1",
        ])
        config = cli_status.resolve_connection_config_from_args(args)
        assert config.host == "h1"
        assert config.port == 1234
        assert config.service_name == "svc1"
        assert config.username == "u1"
        assert config.password == "p1"

    def test_empty_host_port_fall_back(self):
        from types import SimpleNamespace

        args = SimpleNamespace(
            named_connection=None, connection_params=[],
            host="", port=0, service="svc", user="u", password="p",
        )
        config = cli_status.resolve_connection_config_from_args(args)
        assert config.host == "localhost"
        assert config.port == 1521


class TestSerializeHelpers:
    def test_format_size_units(self):
        assert cli_status._format_size(512) == "512.0 B"
        assert cli_status._format_size(2048) == "2.0 KB"
        assert cli_status._format_size(5 * 1024 * 1024) == "5.0 MB"
        assert cli_status._format_size(3 * 1024 ** 3) == "3.0 GB"
        assert cli_status._format_size(2 * 1024 ** 4) == "2.0 TB"
        assert cli_status._format_size(int(1.5 * 1024 ** 5)) == "1.5 PB"

    def test_serialize_scalars_and_containers(self):
        assert cli_status._serialize_for_output(None) is None
        assert cli_status._serialize_for_output("x") == "x"
        assert cli_status._serialize_for_output(3) == 3
        assert cli_status._serialize_for_output({"a": [1, _Mode.OPEN]}) == {"a": [1, "OPEN"]}
        assert cli_status._serialize_for_output((_Mode.OPEN,)) == ["OPEN"]
        assert isinstance(cli_status._serialize_for_output(object()), str)

    def test_serialize_dataclass_nested(self):
        data = cli_status._serialize_for_output(_Overview(session=_Session()))
        assert data["server_vendor"] == "Oracle"
        assert data["mode"] == "OPEN"
        assert data["session"]["user"] == "SYSTEM"


class TestHandleSyncBranches:
    def test_all_json_prints_overview_and_disconnects(self, capsys, monkeypatch):
        instances = patch_sync_backend(monkeypatch, FakeStatusIntrospector())
        cli_status.handle(build_parser().parse_args(["status", "all", "-o", "json"]))
        payload = json.loads(capsys.readouterr().out)
        assert payload["server_version"] == "Oracle Database 23ai"
        assert payload["mode"] == "OPEN"
        assert payload["session"]["host"] == "db.local"
        backend = instances[-1]
        assert backend.connected and backend.disconnected
        assert backend.connection_config.service_name == "ORCL"

    def test_all_table_without_rich_falls_back_to_json(self, capsys, monkeypatch):
        monkeypatch.setattr(cli_status, "RICH_AVAILABLE", False)
        patch_sync_backend(monkeypatch, FakeStatusIntrospector())
        cli_status.handle(build_parser().parse_args(["status"]))
        payload = json.loads(capsys.readouterr().out)
        assert payload["server_vendor"] == "Oracle"

    @pytest.mark.parametrize("status_type,title", [
        ("config", "Configuration"),
        ("performance", "Performance"),
        ("connections", "Connections"),
        ("storage", "Storage"),
        ("tablespaces", "Tablespaces"),
        ("users", "Users"),
    ])
    def test_each_section_branch(self, recording_provider, monkeypatch,
                                 status_type, title):
        patch_sync_backend(monkeypatch, FakeStatusIntrospector())
        cli_status.handle(build_parser().parse_args(["status", status_type]))
        assert [t for t, _ in recording_provider.results] == [title]
        assert recording_provider.results[0][1] is not None

    def test_csv_tsv_fall_back_to_json_for_all(self, capsys, monkeypatch):
        patch_sync_backend(monkeypatch, FakeStatusIntrospector())
        for fmt in ("csv", "tsv"):
            cli_status.handle(build_parser().parse_args(["status", "all", "-o", fmt]))
            payload = json.loads(capsys.readouterr().out)
            assert payload["server_vendor"] == "Oracle"

    def test_tablespaces_without_attribute_yields_empty(self, recording_provider,
                                                        monkeypatch):
        patch_sync_backend(monkeypatch, _NoTablespacesIntrospector())
        cli_status.handle(build_parser().parse_args(["status", "tablespaces"]))
        assert recording_provider.results == [("Tablespaces", [])]

    def test_connection_error_exits_nonzero(self, recording_provider, monkeypatch):
        patch_sync_backend(
            monkeypatch, FakeStatusIntrospector(),
            connect_error=ConnectionError("ORA-01017: invalid username/password"),
        )
        with pytest.raises(SystemExit) as excinfo:
            cli_status.handle(build_parser().parse_args(["status", "config"]))
        assert excinfo.value.code == 1
        assert len(recording_provider.connection_errors) == 1

    def test_query_error_exits_nonzero(self, recording_provider, monkeypatch):
        patch_sync_backend(
            monkeypatch, FakeStatusIntrospector(),
            query_error=QueryError("ORA-00942: table or view does not exist"),
        )
        with pytest.raises(SystemExit) as excinfo:
            cli_status.handle(build_parser().parse_args(["status", "config"]))
        assert excinfo.value.code == 1
        assert recording_provider.query_errors

    def test_unexpected_error_writes_stderr(self, recording_provider, capsys,
                                            monkeypatch):
        patch_sync_backend(monkeypatch, FakeStatusIntrospector(),
                           unexpected_error=RuntimeError("kaboom"))
        with pytest.raises(SystemExit) as excinfo:
            cli_status.handle(build_parser().parse_args(["status", "config"]))
        assert excinfo.value.code == 1
        assert "Error during status retrieval" in capsys.readouterr().err


class TestHandleAsyncBranches:
    def test_async_users_branch(self, recording_provider, monkeypatch):
        instances = patch_async_backend(monkeypatch, FakeStatusIntrospector())
        cli_status.handle(build_parser().parse_args(["status", "users", "--async"]))
        assert recording_provider.results == [("Users", [{"username": "SYSTEM"}])]
        assert instances[-1].disconnected

    def test_async_all_prints_json(self, capsys, monkeypatch):
        patch_async_backend(monkeypatch, FakeStatusIntrospector())
        cli_status.handle(build_parser().parse_args(
            ["status", "all", "--async", "-o", "json"]
        ))
        payload = json.loads(capsys.readouterr().out)
        assert payload["server_version"] == "Oracle Database 23ai"

    @pytest.mark.parametrize("status_type,title", [
        ("config", "Configuration"),
        ("performance", "Performance"),
        ("connections", "Connections"),
        ("storage", "Storage"),
        ("tablespaces", "Tablespaces"),
    ])
    def test_async_section_branches(self, recording_provider, monkeypatch,
                                    status_type, title):
        patch_async_backend(monkeypatch, FakeStatusIntrospector())
        cli_status.handle(build_parser().parse_args(["status", status_type,
                                                     "--async"]))
        assert [t for t, _ in recording_provider.results] == [title]

    def test_async_connection_error_exits(self, recording_provider, monkeypatch):
        patch_async_backend(
            monkeypatch, FakeStatusIntrospector(),
            connect_error=ConnectionError("ORA-12541: no listener"),
        )
        with pytest.raises(SystemExit) as excinfo:
            cli_status.handle(build_parser().parse_args(["status", "users",
                                                         "--async"]))
        assert excinfo.value.code == 1
