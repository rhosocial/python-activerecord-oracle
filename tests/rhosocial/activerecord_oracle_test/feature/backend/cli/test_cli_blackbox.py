# tests/rhosocial/activerecord_oracle_test/feature/backend/cli/test_cli_blackbox.py
"""Black-box CLI tests for the Oracle backend (live scenario server)."""

import io
import json
from contextlib import redirect_stderr, redirect_stdout

import pytest

from rhosocial.activerecord.backend.impl.oracle.__main__ import main
from providers.scenarios import get_scenario_raw

import socket
COMMANDS = [
    "info", "query", "introspect", "status",
    "named-expression", "named-procedure", "named-procedure-graph",
    "named-migration", "named-connection",
]


@pytest.fixture(scope="module")
def conn_args():
    backend_cls, config = get_scenario_raw("oracle_23c")
    _s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _s.settimeout(2)
    try:
        _s.connect((config.host, int(config.port)))
    except OSError:
        pytest.skip(f"Scenario server unreachable: {config.host}:{config.port}")
    finally:
        _s.close()
    return [
        "--host", config.host,
        "--port", str(config.port),
        "--service", config.service_name,
        "--user", config.username,
        "--password", config.password,
    ]


def run_cli(argv):
    out, err = io.StringIO(), io.StringIO()
    exc = None
    with redirect_stdout(out), redirect_stderr(err):
        try:
            main(argv)
        except SystemExit as e:
            exc = e
    return out.getvalue(), err.getvalue(), exc


class TestCommandSurface:
    def test_help_lists_all_commands(self):
        out, _, _ = run_cli(["--help"])
        for cmd in COMMANDS:
            assert cmd in out

    def test_missing_command_errors(self):
        _, _, exc = run_cli([])
        assert exc is not None and exc.code == 1


class TestQuery:
    def test_query_json(self, conn_args):
        out, err, exc = run_cli(
            ["query"] + conn_args + ["SELECT 1 AS one FROM DUAL", "-o", "json"]
        )
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        assert json.loads(out) == [{"one": 1}]

    def test_query_async(self, conn_args):
        out, _, exc = run_cli(
            ["query"] + conn_args + ["SELECT 1 AS one FROM DUAL", "-o", "json", "--async"]
        )
        assert exc is None
        assert json.loads(out) == [{"one": 1}]


class TestStatus:
    def test_status(self, conn_args):
        out, err, exc = run_cli(["status"] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        assert json.loads(out)


class TestInfo:
    def test_info(self):
        out, _, exc = run_cli(["info"])
        assert exc is None
        assert "Oracle" in out or "oracle" in out
