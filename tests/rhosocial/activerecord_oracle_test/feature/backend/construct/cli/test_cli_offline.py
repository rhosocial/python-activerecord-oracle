# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/cli/test_cli_offline.py
"""Connectionless unit tests for the Oracle CLI helper modules.

No driver, session or network is involved: backends, output providers and the
I/O seams are replaced with mocks, while the argument parsers and pure helpers
are the real production objects.  Every sync path has an async twin in Oracle's
native driver, so the async handlers are exercised with ``AsyncMock`` too.
"""

import argparse
import asyncio
import json
from dataclasses import dataclass
from enum import Enum
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from rhosocial.activerecord.backend.dialect import capabilities as cap
from rhosocial.activerecord.backend.dialect.exceptions import DialectNotAdaptedException
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.impl.oracle.cli import (
    info,
    introspect,
    named_expression,
    query,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.options import StatementType


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_parser(module: Any) -> argparse.ArgumentParser:
    top = argparse.ArgumentParser()
    module.create_parser(top.add_subparsers(dest="command"))
    return top


def _info_args(**overrides: Any) -> SimpleNamespace:
    values = dict(
        output="json",
        rich_ascii=False,
        verbose=0,
        version=None,
        named_connection=None,
        host="localhost",
        port=1521,
        service="ORCL",
        user="system",
        password="",
        connection_params=[],
        is_async=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


# ===========================================================================
# info.py
# ===========================================================================


class TestInfoHelpers:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("19.0.0", (19, 0, 0)),
            ("21", (21, 0, 0)),
            ("21.3", (21, 3, 0)),
            ("23.1.4", (23, 1, 4)),
            ("19.0.0.7", (19, 0, 0)),
        ],
    )
    def test_parse_version(self, raw: str, expected: tuple) -> None:
        assert info.parse_version(raw) == expected

    def test_get_protocol_support_methods(self) -> None:
        class FakeProtocol:
            constant = "not callable"

            def supports_widgets(self) -> bool:
                return True

            def is_gadgets_available(self) -> bool:
                return True

            def unrelated(self) -> None:
                pass

        assert cap.get_support_methods(FakeProtocol) == [
            "is_gadgets_available",
            "supports_widgets",
        ]

    def test_check_protocol_support(self) -> None:
        class FakeProtocol:
            def supports_bool(self) -> bool:
                return True

            def supports_explain_format(self, fmt: str) -> bool:
                return True

            def supports_needs_arg(self, value: str) -> bool:
                return True

            def is_thing_available(self) -> bool:
                return True

            def supports_raises(self) -> bool:
                raise RuntimeError("boom")

        class FakeDialect:
            def supports_bool(self) -> bool:
                return True

            def supports_explain_format(self, fmt: str) -> bool:
                return fmt == "JSON"

            def is_thing_available(self) -> bool:
                return False

            def supports_raises(self) -> bool:
                raise RuntimeError("boom")

        result = cap.check_protocol_support(
            FakeDialect(), FakeProtocol, info.SUPPORT_METHOD_ALL_ARGS
        )
        assert result["supports_bool"] is True
        assert result["is_thing_available"] is False
        assert result["supports_raises"] is False
        assert result["supports_needs_arg"] is False
        fmt = result["supports_explain_format"]
        assert fmt["supported"] == 1
        assert fmt["total"] == 6
        assert fmt["args"]["JSON"] is True
        assert fmt["args"]["TEXT"] is False

    def test_check_protocol_support_missing_method(self) -> None:
        class FakeProtocol:
            def supports_missing(self) -> bool:
                return True

        assert cap.check_protocol_support(object(), FakeProtocol) == {
            "supports_missing": False
        }

    @pytest.mark.parametrize(
        "support,expected",
        [
            ({"a": True, "b": False}, (1, 2)),
            ({"a": {"supported": 2, "total": 5, "args": {}}}, (2, 5)),
            ({}, (0, 0)),
        ],
    )
    def test_calculate_protocol_stats(self, support: dict, expected: tuple) -> None:
        assert cap._support_stats(support) == expected

    @pytest.mark.parametrize(
        "pct,expected",
        [
            (100.0, ("green", "[OK]")),
            (75.0, ("yellow", "[~]")),
            (50.0, ("yellow", "[~]")),
            (25.0, ("red", "[~]")),
            (0.0, ("red", "[X]")),
        ],
    )
    def test_status_style(self, pct: float, expected: tuple) -> None:
        assert info._get_status_style(pct) == expected

    @pytest.mark.parametrize(
        "method,expected",
        [
            ("supports_native_json", "native json"),
            ("is_vector_available", "is vector available"),
            ("supports_for_update_nowait", "for update nowait"),
        ],
    )
    def test_format_method_display(self, method: str, expected: str) -> None:
        assert info._format_method_display(method) == expected

    def test_display_method_details_bool(self) -> None:
        console = MagicMock()
        info._display_method_details(console, "supports_pivot", True)
        text = " ".join(str(c) for c in console.print.call_args_list)
        assert "pivot" in text
        assert "[OK]" in text

    def test_display_method_details_dict(self) -> None:
        console = MagicMock()
        info._display_method_details(
            console,
            "supports_explain_format",
            {"supported": 1, "total": 2, "args": {"JSON": True, "TEXT": False}},
        )
        text = " ".join(str(c) for c in console.print.call_args_list)
        assert "JSON" in text and "TEXT" in text

    def test_capability_report_verbose_levels(self) -> None:
        class FakeProtocol:
            def supports_one(self) -> bool:
                return True

        dialect = FakeProtocol()
        brief = cap.build_capability_report(dialect, {"g": [FakeProtocol]})
        detailed = cap.build_capability_report(dialect, {"g": [FakeProtocol]}, verbose=2)
        assert set(brief["g"]["FakeProtocol"]) == {
            "supported",
            "total",
            "percentage",
            "origin",
        }
        assert "methods" in detailed["g"]["FakeProtocol"]
        assert "method_origins" in detailed["g"]["FakeProtocol"]

    def test_display_protocol_item_and_group(self) -> None:
        console = MagicMock()
        stats = {"percentage": 50.0, "supported": 1, "total": 2}
        info._display_protocol_item(console, "P", stats, 0)
        text = " ".join(str(c) for c in console.print.call_args_list)
        assert "P:" in text and "50%" in text and "1/2" in text

        console.reset_mock()
        info._display_protocol_group(console, "Query Features", {"P": stats}, 0)
        assert console.print.called

    def test_display_info_rich(self, monkeypatch: pytest.MonkeyPatch) -> None:
        console = MagicMock()
        fake_console_module = SimpleNamespace(Console=lambda **kwargs: console)
        monkeypatch.setitem(__import__("sys").modules, "rich", SimpleNamespace())
        monkeypatch.setitem(
            __import__("sys").modules, "rich.console", fake_console_module
        )
        monkeypatch.setattr(
            info, "_display_protocol_group", lambda *a: None
        )
        info._display_info_rich(
            {"protocols": {}}, 0, "19.0.0", is_connected=False
        )
        text = " ".join(str(c) for c in console.print.call_args_list)
        assert "Oracle Environment Information" in text
        assert "no database connection" in text


class TestInfoParser:
    def test_defaults(self) -> None:
        args = _make_parser(info).parse_args(["info"])
        assert args.output == "table"
        assert args.verbose == 0
        assert args.rich_ascii is False
        assert args.version is None

    def test_choices_and_version(self) -> None:
        parser = _make_parser(info)
        args = parser.parse_args(["info", "-o", "json", "-vv", "--version", "21.0.0"])
        assert args.output == "json"
        assert args.verbose == 2
        assert args.version == "21.0.0"
        with pytest.raises(SystemExit):
            parser.parse_args(["info", "-o", "yaml"])


class TestInfoHandle:
    def test_offline_with_version(self, capsys: pytest.CaptureFixture) -> None:
        result = info.handle(_info_args(version="19.0.0"))
        assert result["database"] == {
            "type": "oracle",
            "version": "19.0.0",
            "version_tuple": [19, 0, 0],
            "connected": False,
        }
        assert result["pagination"]["fetch_first"] is True
        assert result["pagination"]["rownum"] is True
        assert set(result["features"]) == {
            "hierarchical_queries",
            "pivot",
            "unpivot",
            "query_hints",
            "native_json",
            "boolean_type",
            "vector_type",
            "json_duality",
        }
        assert set(result["locking"]) == {
            "for_update",
            "for_update_nowait",
            "for_update_wait",
            "for_update_skip_locked",
        }
        assert result["protocols"]
        assert result["capabilities"]
        assert json.loads(capsys.readouterr().out)["database"]["version"] == "19.0.0"

    def test_capability_suggestion_and_server_sections(self) -> None:
        result = info.handle(_info_args(version="19.0.0"))
        assert result["protocols"] is result["capabilities"]
        assert result["server"]["version"] == "19.0.0"
        assert result["server"]["variables"] == {}
        assert result["server"]["connected"] is False
        assert "V$PARAMETER" in result["server"]["source"]
        origins = {
            entry["origin"]
            for group in result["capabilities"].values()
            for entry in group.values()
        }
        assert origins <= {"reuse", "override", "proprietary", "not-supported"}
        assert all(
            entry["origin"] == "proprietary"
            for entry in result["capabilities"]["Oracle-specific"].values()
        )
        assert result["suggestions"]
        assert all({"supported"} <= set(entry) for entry in result["suggestions"].values())

    def test_verbose_capability_report_has_method_origins(self) -> None:
        result = info.handle(_info_args(version="19.0.0", verbose=2))
        first = next(iter(next(iter(result["capabilities"].values())).values()))
        assert "methods" in first and "method_origins" in first
        assert all({"supported"} <= set(entry) for entry in result["suggestions"].values())

    def test_offline_without_version_marks_unknown(self) -> None:
        result = info.handle(_info_args(version=None))
        assert result["database"]["version"] == "unknown"
        assert result["database"]["version_tuple"] is None
        assert result["pagination"]["fetch_first"] is None
        assert result["locking"]["for_update_skip_locked"] is None

    def test_connected_mode(self) -> None:
        backend = MagicMock()
        backend.dialect = OracleDialect(version=(21, 0, 0))
        backend.get_server_version.return_value = (21, 3, 0)
        with (
            patch.object(info, "connection_named", return_value=True),
            patch.object(info, "resolve_connection_config_from_args", return_value=object()),
            patch(
                "rhosocial.activerecord.backend.impl.oracle.backend.OracleBackend",
                return_value=backend,
            ),
        ):
            result = info.handle(_info_args())
        assert result["database"]["version"] == "21.3.0"
        assert result["database"]["connected"] is True
        assert result["database"]["version_tuple"] == [21, 0, 0]
        backend.connect.assert_called_once()
        backend.introspect_and_adapt.assert_called_once()
        backend.disconnect.assert_called_once()

    def test_connection_failure_falls_back_to_offline(self) -> None:
        backend = MagicMock()
        backend.connect.side_effect = ConnectionError("ORA-01017")
        with (
            patch.object(info, "connection_named", return_value=True),
            patch.object(info, "resolve_connection_config_from_args", return_value=object()),
            patch(
                "rhosocial.activerecord.backend.impl.oracle.backend.OracleBackend",
                return_value=backend,
            ),
        ):
            result = info.handle(_info_args(version="12.2.0"))
        assert result["database"]["connected"] is False
        assert result["database"]["version"] == "12.2.0"
        backend.disconnect.assert_not_called()


# ===========================================================================
# query.py
# ===========================================================================


class TestQueryParser:
    def test_defaults(self) -> None:
        args = _make_parser(query).parse_args(["query", "SELECT 1 FROM DUAL"])
        assert args.output == "table"
        assert args.log_level == "INFO"
        assert args.rich_ascii is False
        assert args.sql == "SELECT 1 FROM DUAL"
        assert args.file is None
        assert args.is_async is False

    def test_file_and_async_flags(self) -> None:
        args = _make_parser(query).parse_args(
            ["query", "-f", "q.sql", "--async", "-o", "json", "--log-level", "DEBUG"]
        )
        assert args.sql is None
        assert args.file == "q.sql"
        assert args.is_async is True
        assert args.output == "json"
        assert args.log_level == "DEBUG"

    def test_invalid_output_rejected(self) -> None:
        with pytest.raises(SystemExit):
            _make_parser(query).parse_args(["query", "x", "-o", "excel"])


class TestQueryHandle:
    def test_invalid_log_level(self) -> None:
        with pytest.raises(ValueError, match="Invalid log level"):
            query.handle(_info_args(output="json", log_level="LOUD", sql="SELECT 1"))

    def test_no_sql_exits(self) -> None:
        args = _info_args(output="json", log_level="INFO", sql=None, file=None, rich_ascii=False)
        stdin = MagicMock()
        stdin.isatty.return_value = True
        with patch.object(query.sys, "stdin", stdin), pytest.raises(SystemExit) as exc:
            query.handle(args)
        assert exc.value.code == 1

    def test_stdin_source(self) -> None:
        args = _info_args(output="json", log_level="INFO", sql=None, file=None, rich_ascii=False)
        stdin = MagicMock()
        stdin.isatty.return_value = False
        stdin.read.return_value = "SELECT 99 FROM DUAL"
        with (
            patch.object(query.sys, "stdin", stdin),
            patch.object(query, "resolve_connection_config_from_args", return_value=object()),
            patch.object(query, "create_provider", return_value=MagicMock()),
            patch.object(query, "_execute_query_sync") as sync,
        ):
            query.handle(args)
        assert sync.call_args.args[0] == "SELECT 99 FROM DUAL"

    def test_file_not_found_exits(self) -> None:
        args = _info_args(
            output="json", log_level="INFO", sql=None, file="missing.sql", rich_ascii=False
        )
        with patch("builtins.open", side_effect=FileNotFoundError), pytest.raises(SystemExit) as exc:
            query.handle(args)
        assert exc.value.code == 1

    def test_file_source(self) -> None:
        args = _info_args(
            output="json", log_level="INFO", sql=None, file="q.sql", rich_ascii=False
        )
        with (
            patch("builtins.open", mock_open(read_data="SELECT 7 FROM DUAL")),
            patch.object(query, "resolve_connection_config_from_args", return_value=object()),
            patch.object(query, "create_provider", return_value=MagicMock()),
            patch.object(query, "_execute_query_sync") as sync,
        ):
            query.handle(args)
        assert sync.call_args.args[0].strip() == "SELECT 7 FROM DUAL"

    def test_sync_dispatch(self) -> None:
        args = _info_args(output="json", log_level="INFO", sql="SELECT 1", rich_ascii=False)
        args.is_async = False
        with (
            patch.object(query, "resolve_connection_config_from_args", return_value=object()),
            patch.object(query, "create_provider", return_value=MagicMock()),
            patch.object(query, "_execute_query_sync") as sync,
            patch.object(query, "OracleBackend") as backend_cls,
        ):
            query.handle(args)
        sync.assert_called_once()
        assert sync.call_args.kwargs == {"use_ascii": False}
        backend_cls.assert_called_once()

    def test_async_dispatch(self) -> None:
        args = _info_args(output="json", log_level="INFO", sql="SELECT 1", rich_ascii=True)
        args.is_async = True

        def _run(coro: Any) -> None:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(coro)
            finally:
                loop.close()

        with (
            patch.object(query, "resolve_connection_config_from_args", return_value=object()),
            patch.object(query, "create_provider", return_value=MagicMock()),
            patch.object(query.asyncio, "run", side_effect=_run),
            patch.object(query, "_execute_query_async", new_callable=AsyncMock) as async_exec,
            patch.object(query, "AsyncOracleBackend") as backend_cls,
        ):
            query.handle(args)
        async_exec.assert_awaited_once()
        assert async_exec.call_args.kwargs == {"use_ascii": True}
        backend_cls.assert_called_once()


class TestQueryExecution:
    @staticmethod
    def _result(data: Any = None) -> SimpleNamespace:
        return SimpleNamespace(affected_rows=3, duration=0.25, data=data)

    def _sync_backend(self, result: Any = None, error: Exception | None = None) -> MagicMock:
        backend = MagicMock()
        backend._connection = object()
        if error is not None:
            backend.execute.side_effect = error
        else:
            backend.execute.return_value = result if result is not None else self._result()
        return backend

    def test_sync_success_with_data(self) -> None:
        backend = self._sync_backend(result=self._result(data=[{"ID": 1}]))
        provider = MagicMock()
        query._execute_query_sync("SQL", backend, provider, use_ascii=False)
        provider.display_query.assert_called_once_with("SQL", is_async=False)
        provider.display_success.assert_called_once_with(3, 0.25)
        provider.display_results.assert_called_once_with([{"ID": 1}], use_ascii=False)
        backend.disconnect.assert_called_once()
        provider.display_disconnect.assert_called_once_with(is_async=False)

    def test_sync_no_result(self) -> None:
        backend = self._sync_backend(result=None)
        backend.execute.return_value = None
        provider = MagicMock()
        query._execute_query_sync("SQL", backend, provider)
        provider.display_no_result_object.assert_called_once()

    def test_sync_result_without_data(self) -> None:
        backend = self._sync_backend(result=self._result(data=None))
        provider = MagicMock()
        query._execute_query_sync("SQL", backend, provider)
        provider.display_no_data.assert_called_once()

    @pytest.mark.parametrize(
        "error,method",
        [
            (ConnectionError("ORA-01017"), "display_connection_error"),
            (QueryError("ORA-00942"), "display_query_error"),
            (RuntimeError("boom"), "display_unexpected_error"),
        ],
    )
    def test_sync_error_paths(self, error: Exception, method: str) -> None:
        backend = self._sync_backend(error=error)
        provider = MagicMock()
        with pytest.raises(SystemExit) as exc:
            query._execute_query_sync("SQL", backend, provider)
        assert exc.value.code == 1
        getattr(provider, method).assert_called_once()
        backend.disconnect.assert_called_once()

    def test_sync_no_disconnect_without_connection(self) -> None:
        backend = self._sync_backend()
        backend._connection = None
        provider = MagicMock()
        query._execute_query_sync("SQL", backend, provider)
        backend.disconnect.assert_not_called()

    async def test_async_success_with_data(self) -> None:
        backend = MagicMock()
        backend._connection = object()
        backend.connect = AsyncMock()
        backend.execute = AsyncMock(return_value=self._result(data=[{"ID": 2}]))
        backend.disconnect = AsyncMock()
        provider = MagicMock()
        await query._execute_query_async("SQL", backend, provider, use_ascii=True)
        provider.display_success.assert_called_once_with(3, 0.25)
        provider.display_results.assert_called_once_with([{"ID": 2}], use_ascii=True)
        backend.disconnect.assert_awaited_once()
        provider.display_disconnect.assert_called_once_with(is_async=True)

    async def test_async_no_result(self) -> None:
        backend = MagicMock()
        backend._connection = object()
        backend.connect = AsyncMock()
        backend.execute = AsyncMock(return_value=None)
        backend.disconnect = AsyncMock()
        provider = MagicMock()
        await query._execute_query_async("SQL", backend, provider)
        provider.display_no_result_object.assert_called_once()

    @pytest.mark.parametrize(
        "error,method",
        [
            (ConnectionError("ORA-01017"), "display_connection_error"),
            (QueryError("ORA-00942"), "display_query_error"),
            (RuntimeError("boom"), "display_unexpected_error"),
        ],
    )
    async def test_async_error_paths(self, error: Exception, method: str) -> None:
        backend = MagicMock()
        backend._connection = object()
        backend.connect = AsyncMock()
        backend.execute = AsyncMock(side_effect=error)
        backend.disconnect = AsyncMock()
        provider = MagicMock()
        with pytest.raises(SystemExit) as exc:
            await query._execute_query_async("SQL", backend, provider)
        assert exc.value.code == 1
        getattr(provider, method).assert_called_once()
        backend.disconnect.assert_awaited_once()


# ===========================================================================
# named_expression.py
# ===========================================================================


class TestNamedExpression:
    def test_create_parser(self) -> None:
        args = _make_parser(named_expression).parse_args(["named-expression"])
        assert args.dialect_version is None
        assert args.output == "table"
        assert args.rich_ascii is False
        assert hasattr(args, "is_async")

    def _args(self, **overrides: Any) -> SimpleNamespace:
        values = dict(
            output="json",
            rich_ascii=False,
            dialect_version="23.0.0",
            is_async=False,
            named_connection=None,
            host="localhost",
            port=1521,
            service="ORCL",
            user="system",
            password="",
            connection_params=[],
        )
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_sync_handle_wires_callbacks(self) -> None:
        captured: dict = {}

        def fake_handle(args: Any, provider: Any, **kwargs: Any) -> None:
            captured.update(kwargs)

        backend = MagicMock()
        with (
            patch(
                "rhosocial.activerecord.backend.named_expression.cli.handle_named_expression",
                fake_handle,
            ),
            patch.object(named_expression, "create_provider", return_value=MagicMock()),
            patch.object(
                named_expression, "resolve_connection_config_from_args", return_value=object()
            ),
            patch.object(named_expression, "OracleBackend", return_value=backend),
        ):
            named_expression.handle(self._args())

            assert {
                "backend_factory",
                "get_dialect",
                "execute_query",
                "disconnect",
                "create_dialect",
            } <= set(captured)
            assert "backend_async_factory" not in captured

            # Exercise the closures themselves while the patches are active.
            assert captured["create_dialect"]().version == (23, 0, 0)
            assert captured["backend_factory"]() is backend
            backend.connect.assert_called_once()
            backend.introspect_and_adapt.assert_called_once()
            assert captured["get_dialect"](backend) is backend.dialect
            captured["execute_query"]("SQL", (), StatementType.DQL)
            assert backend.execute.call_args.kwargs["options"].stmt_type is StatementType.DQL
            captured["disconnect"]()
            backend.disconnect.assert_called_once()

    async def test_async_handle_wires_callbacks(self) -> None:
        captured: dict = {}

        def fake_handle(args: Any, provider: Any, **kwargs: Any) -> None:
            captured.update(kwargs)

        backend = MagicMock()
        backend.connect = MagicMock()
        backend.introspect_and_adapt = MagicMock()
        backend.disconnect = AsyncMock()
        backend.execute = AsyncMock()
        with (
            patch(
                "rhosocial.activerecord.backend.named_expression.cli.handle_named_expression",
                fake_handle,
            ),
            patch.object(named_expression, "create_provider", return_value=MagicMock()),
            patch.object(
                named_expression, "resolve_connection_config_from_args", return_value=object()
            ),
            patch.object(named_expression, "AsyncOracleBackend", return_value=backend),
        ):
            named_expression.handle(self._args(is_async=True))

            assert {
                "backend_async_factory",
                "get_dialect_async",
                "execute_query_async",
                "disconnect_async",
                "create_dialect",
            } <= set(captured)

            assert await captured["get_dialect_async"](backend) is backend.dialect
            factory = captured["backend_async_factory"]()
            assert factory is backend
            await captured["execute_query_async"]("SQL", (), StatementType.DQL)
            assert backend.execute.await_count == 1
            await captured["disconnect_async"]()
            backend.disconnect.assert_awaited_once()

    def test_create_dialect_without_version(self) -> None:
        captured: dict = {}
        with (
            patch(
                "rhosocial.activerecord.backend.named_expression.cli.handle_named_expression",
                lambda args, provider, **kwargs: captured.update(kwargs),
            ),
            patch.object(named_expression, "create_provider", return_value=MagicMock()),
        ):
            named_expression.handle(self._args(dialect_version=None))
        with pytest.raises(DialectNotAdaptedException):
            _ = captured["create_dialect"]().version


# ===========================================================================
# introspect.py helpers
# ===========================================================================


class _Colour(Enum):
    RED = "red"
    BLUE = "blue"


@dataclass
class _Row:
    name: str
    count: int


class _Model:
    def model_dump(self, mode: str = "python") -> dict:
        if mode == "json":
            raise TypeError("no json mode")
        return {"name": "x"}


class TestIntrospectSerialization:
    def test_none_and_primitives(self) -> None:
        assert introspect._serialize_for_output(None) is None
        assert introspect._serialize_for_output("x") == "x"
        assert introspect._serialize_for_output(5) == 5
        assert introspect._serialize_for_output(1.5) == 1.5
        assert introspect._serialize_for_output(True) is True

    def test_containers_and_enum(self) -> None:
        assert introspect._serialize_for_output([1, _Colour.RED]) == [1, "red"]
        assert introspect._serialize_for_output((1, 2)) == [1, 2]
        assert introspect._serialize_for_output({"c": _Colour.BLUE}) == {"c": "blue"}

    def test_dataclass(self) -> None:
        assert introspect._serialize_for_output(_Row("x", 2)) == {"name": "x", "count": 2}

    def test_model_dump_fallback(self) -> None:
        assert introspect._serialize_for_output(_Model()) == {"name": "x"}

    def test_model_dump_json_mode(self) -> None:
        class JsonModel(_Model):
            def model_dump(self, mode: str = "python") -> dict:
                return {"mode": mode}

        assert introspect._serialize_for_output(JsonModel()) == {"mode": "json"}

    def test_arbitrary_object_becomes_string(self) -> None:
        assert introspect._serialize_for_output(object()) != ""

    def test_value_sample_rows_adds_source(self) -> None:
        rows = introspect._value_sample_rows([{"value": "A", "count": 3}])
        assert rows[0]["source"] == "aggregate"
        assert rows[0]["value"] == "A"

    def test_value_sample_rows_empty_and_non_dict(self) -> None:
        assert introspect._value_sample_rows([]) == []
        assert introspect._value_sample_rows(["raw"]) == ["raw"]


class TestIntrospectArgumentGuards:
    def test_unknown_type_name_is_ignored(self) -> None:
        introspect._validate_name(argparse.Namespace(type="not-a-type", name="x"))

    @pytest.mark.parametrize("itype", ["tables", "database"])
    def test_forbidden_name_exits(self, itype: str, capsys: pytest.CaptureFixture) -> None:
        with pytest.raises(SystemExit) as exc:
            introspect._validate_name(argparse.Namespace(type=itype, name="x"))
        assert exc.value.code == 2
        assert "takes no name" in capsys.readouterr().err

    def test_join_coverage_noun(self, capsys: pytest.CaptureFixture) -> None:
        with pytest.raises(SystemExit) as exc:
            introspect._validate_name(
                argparse.Namespace(type="join-coverage", name=None)
            )
        assert exc.value.code == 1
        assert "Left table name" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"type": "tables", "column": "c"},
            {"type": "tables", "right_table": "t"},
            {"type": "value-samples", "right_column": "c"},
        ],
    )
    def test_unused_flags_exit(
        self, kwargs: dict, capsys: pytest.CaptureFixture
    ) -> None:
        args = argparse.Namespace(**kwargs)
        with pytest.raises(SystemExit) as exc:
            introspect._refuse_unused_arguments(args)
        assert exc.value.code == 2
        assert "does not take" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"type": "value-samples", "column": "c"},
            {"type": "join-coverage", "column": "c", "right_table": "t", "right_column": "rc"},
            {"type": "tables"},
        ],
    )
    def test_allowed_flags_pass(self, kwargs: dict) -> None:
        introspect._refuse_unused_arguments(argparse.Namespace(**kwargs))

    def test_capability_gate(self, capsys: pytest.CaptureFixture) -> None:
        backend = SimpleNamespace(dialect=SimpleNamespace(supports_trigger=lambda: False))
        args = argparse.Namespace(type="triggers")
        with patch.object(introspect, "_TYPE_CAPABILITIES", {"triggers": "supports_trigger"}):
            with pytest.raises(SystemExit) as exc:
                introspect._require_capability(backend, args)
        assert exc.value.code == 2
        assert "not supported by this server" in capsys.readouterr().err

    def test_capability_gate_allows(self) -> None:
        backend = SimpleNamespace(dialect=SimpleNamespace(supports_trigger=lambda: True))
        args = argparse.Namespace(type="triggers")
        with patch.object(introspect, "_TYPE_CAPABILITIES", {"triggers": "supports_trigger"}):
            introspect._require_capability(backend, args)

    def test_capability_gate_without_switch_or_dialect(self) -> None:
        introspect._require_capability(object(), argparse.Namespace(type="unknown"))
        introspect._require_capability(
            object(), argparse.Namespace(type="triggers")
        )


class TestIntrospectParser:
    def test_defaults(self) -> None:
        args = _make_parser(introspect).parse_args(["introspect", "tables"])
        assert args.type == "tables"
        assert args.name is None
        assert args.output == "table"
        assert args.rich_ascii is False
        assert args.include_system is False
        assert args.column is None
        assert args.right_table is None
        assert args.right_column is None
        assert args.is_async is False

    def test_optional_flags(self) -> None:
        args = _make_parser(introspect).parse_args(
            ["introspect", "value-samples", "customers", "--column", "status", "-o", "json"]
        )
        assert args.name == "customers"
        assert args.column == "status"
        assert args.output == "json"

    def test_invalid_type_rejected(self) -> None:
        with pytest.raises(SystemExit):
            _make_parser(introspect).parse_args(["introspect", "grants"])
