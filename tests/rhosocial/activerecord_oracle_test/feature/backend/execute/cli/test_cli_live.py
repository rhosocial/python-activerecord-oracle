# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/cli/test_cli_live.py
"""Real-database CLI integration tests for the Oracle backend.

The pure-mock suite under ``feature/backend/construct/cli/`` proves dispatch
with fakes; this suite drives ``main()`` against the live server supplied by
``oracle_backend_single`` and reads the JSON document written to stdout. Every
command gets the connection flags for the very config the fixture used, so a
passing test means the CLI can reach a real Oracle instance and answer -- not
just that a stub answered the right method.

Oracle needs ``FROM DUAL`` for a constant SELECT, and some catalog / ``V$``
views are not readable by the per-directory schema user the fixture connects
as; the assertions stick to what that user can actually be asked.
"""

import json

from rhosocial.activerecord.backend.impl.oracle.__main__ import main


def _connection_argv(config):
    """Connection flags for the scenario the fixture connected with."""
    service = config.service_name or config.database
    return [
        "--host",
        str(config.host),
        "--port",
        str(config.port),
        "--service",
        str(service),
        "--user",
        str(config.username),
        "--password",
        config.password or "",
    ]


def _json_stdout(capsys):
    """Parse the single JSON document the CLI wrote to stdout."""
    return json.loads(capsys.readouterr().out)


class TestCliLive:
    """``main(...)`` invoked with real connection flags against a real server."""

    def test_info(self, oracle_backend_single, capsys):
        # ``info`` takes no positional argument; the connection flags name a
        # real service, so it reads the server rather than the offline defaults.
        main(["info", "-o", "json", *_connection_argv(oracle_backend_single.config)])
        payload = _json_stdout(capsys)
        assert isinstance(payload, dict)
        assert payload["database"]["type"] == "oracle"
        assert payload["database"]["connected"] is True
        assert payload["database"]["version"]
        assert "features" in payload and "protocols" in payload

    def test_query_select_from_dual(self, oracle_backend_single, capsys):
        main(
            [
                "query",
                "SELECT 1 AS n FROM DUAL",
                "-o",
                "json",
                *_connection_argv(oracle_backend_single.config),
            ]
        )
        payload = _json_stdout(capsys)
        assert isinstance(payload, list) and len(payload) == 1
        row = payload[0]
        assert len(row) == 1
        key = next(iter(row))
        # Oracle uppercases an unquoted alias; either case answers the query.
        assert key.lower() == "n"
        # oracledb returns an int, which the JSON provider may render as text.
        assert str(row[key]).strip() == "1"

    def test_introspect_tables(self, oracle_backend_single, capsys):
        main(
            [
                "introspect",
                "tables",
                "-o",
                "json",
                *_connection_argv(oracle_backend_single.config),
            ]
        )
        payload = _json_stdout(capsys)
        # A freshly reset directory schema may hold no tables; the answer is
        # still a catalogue list, and each row names a table.
        assert isinstance(payload, list)
        assert all(isinstance(row, dict) and row.get("name") for row in payload)

    def test_introspect_database(self, oracle_backend_single, capsys):
        main(
            [
                "introspect",
                "database",
                "-o",
                "json",
                *_connection_argv(oracle_backend_single.config),
            ]
        )
        payload = _json_stdout(capsys)
        assert isinstance(payload, list) and len(payload) == 1
        assert payload[0]["name"]
        assert payload[0]["version"]

    def test_status_all(self, oracle_backend_single, capsys):
        main(
            [
                "status",
                "all",
                "-o",
                "json",
                *_connection_argv(oracle_backend_single.config),
            ]
        )
        payload = _json_stdout(capsys)
        assert isinstance(payload, dict)
        assert payload["server_version"]
        assert payload["server_vendor"]

    def test_status_config(self, oracle_backend_single, capsys):
        main(
            [
                "status",
                "config",
                "-o",
                "json",
                *_connection_argv(oracle_backend_single.config),
            ]
        )
        payload = _json_stdout(capsys)
        # The schema user may not read V$PARAMETER, in which case the command
        # answers with an empty list rather than failing.
        assert isinstance(payload, list)
        assert all(isinstance(row, dict) and "name" in row for row in payload)
