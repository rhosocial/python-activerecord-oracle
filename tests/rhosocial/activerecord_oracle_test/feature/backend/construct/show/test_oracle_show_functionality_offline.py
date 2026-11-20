# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/show/test_oracle_show_functionality_offline.py
"""Database-free unit tests for the synchronous Oracle SHOW functionality helpers.

Every function in ``show.functionality`` builds an ``OracleQuery*Expression``,
executes its SQL through ``backend.execute()`` and maps ``result.data`` rows
into a typed dataclass.  These tests drive that whole path with a MagicMock
backend: the dialect is real (expressions validate it), but no connection,
cursor, listener or network is involved.

The only async member of the module, ``AsyncOracleShowFunctionality.sessions``,
is intentionally out of scope for this file (it awaits ``backend.execute``).
"""

from unittest.mock import MagicMock

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.show import functionality as show
from rhosocial.activerecord.backend.impl.oracle.show.types import (
    OracleDatabaseInfo,
    OracleInstanceInfo,
    OracleLockInfo,
    OracleNlsParameter,
    OracleObjectInfo,
    OracleSessionInfo,
    OracleSqlInfo,
    OracleWaitEventInfo,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def seeded_backend(rows):
    """Return a MagicMock backend whose ``execute`` yields the given rows."""
    backend = MagicMock()
    backend.dialect = OracleDialect(version=(19, 0, 0))
    result = MagicMock()
    result.data = rows
    backend.execute.return_value = result
    return backend


def executed_sql(backend):
    """Return the ``(sql, params)`` pair handed to ``backend.execute``."""
    assert backend.execute.call_count == 1
    sql, params = backend.execute.call_args.args
    assert isinstance(sql, str) and sql.strip()
    return sql, params


# Superset row containing every key the mapping comprehensions look up.  Used
# by registry/wrapper tests so a single row can seed any of the functions.
UNIVERSAL_ROW = {
    "sid": 10,
    "serial_num": 20,
    "username": "SCOTT",
    "status": "ACTIVE",
    "machine": "WORKSTATION",
    "program": "python.exe",
    "module": "APP",
    "action": "QUERY",
    "logon_time": "2026-11-20 09:00:00",
    "process": "4321",
    "sql_id": "abc123def456",
    "child_number": 0,
    "sql_text": "SELECT 1 FROM DUAL",
    "executions": 12,
    "elapsed_seconds": 1.25,
    "cpu_seconds": 0.5,
    "buffer_gets": 100,
    "disk_reads": 5,
    "rows_processed": 30,
    "parsing_schema_name": "SCOTT",
    "last_active_time": "2026-11-20 09:30:00",
    "name": "ORCL",
    "dbid": 1234567890,
    "created": "2019-01-01 00:00:00",
    "log_mode": "ARCHIVELOG",
    "open_mode": "READ WRITE",
    "platform_name": "Linux x86 64-bit",
    "role": "PRIMARY",
    "instance_name": "orcl",
    "instance_num": 1,
    "host_name": "dbhost",
    "version": "19.0.0.0.0",
    "startup_time": "2026-11-20 08:00:00",
    "archiver": "STOPPED",
    "object_name": "EMP",
    "object_type": "TABLE",
    "owner": "SCOTT",
    "object_id": 12345,
    "last_ddl_time": "2021-01-01 00:00:00",
    "temporary": "N",
    "lock_type": "TM",
    "id1": 100,
    "id2": 0,
    "lmode": 3,
    "request": 0,
    "block": 0,
    "seq#": 7,
    "event": "db file sequential read",
    "p1text": "file#",
    "p1": 4,
    "wait_class": "User I/O",
    "wait_time": 0,
    "seconds_in_wait": 3.5,
    "state": "WAITING",
    "parameter": "NLS_LANGUAGE",
    "value": "AMERICAN",
}


# ---------------------------------------------------------------------------
# query_sessions
# ---------------------------------------------------------------------------


class TestQuerySessions:
    """Tests for ``query_sessions``."""

    def test_maps_full_row_to_oracle_session_info(self):
        backend = seeded_backend(
            [
                {
                    "sid": 12,
                    "serial_num": 345,
                    "username": "SCOTT",
                    "status": "ACTIVE",
                    "machine": "WORKSTATION",
                    "program": "python.exe",
                    "module": "APP",
                    "action": "QUERY",
                    "logon_time": "2026-11-20 09:00:00",
                    "process": "1234",
                }
            ]
        )

        sessions = show.query_sessions(backend)

        assert len(sessions) == 1
        session = sessions[0]
        assert isinstance(session, OracleSessionInfo)
        assert session.sid == 12
        assert session.serial_num == 345
        assert session.username == "SCOTT"
        assert session.status == "ACTIVE"
        assert session.machine == "WORKSTATION"
        assert session.program == "python.exe"
        assert session.module == "APP"
        assert session.action == "QUERY"
        assert session.logon_time == "2026-11-20 09:00:00"
        assert session.process == "1234"

    def test_missing_optional_fields_become_none(self):
        backend = seeded_backend([{"sid": 1, "serial_num": 2}])

        session = show.query_sessions(backend)[0]

        assert session.username is None
        assert session.status is None
        assert session.logon_time is None
        assert session.process is None

    def test_empty_rows_return_empty_list(self):
        backend = seeded_backend([])

        assert show.query_sessions(backend) == []

    @pytest.mark.parametrize(
        ("active_only", "expected_params"),
        [(False, ()), (True, ("ACTIVE",))],
    )
    def test_active_only_changes_filter_and_params(self, active_only, expected_params):
        backend = seeded_backend([])

        show.query_sessions(backend, active_only=active_only)

        sql, params = executed_sql(backend)
        assert "V$SESSION" in sql.upper()
        if active_only:
            assert '"STATUS" = ?' in sql
            assert "IS NOT NULL" not in sql
        else:
            assert "IS NOT NULL" in sql
            assert "?" not in sql
        assert params == expected_params


# ---------------------------------------------------------------------------
# query_running_sql
# ---------------------------------------------------------------------------


class TestQueryRunningSql:
    """Tests for ``query_running_sql``."""

    def test_maps_full_row_to_oracle_sql_info(self):
        backend = seeded_backend(
            [
                {
                    "sql_id": "abc123def456",
                    "child_number": 0,
                    "sql_text": "SELECT 1 FROM DUAL",
                    "executions": 10,
                    "elapsed_seconds": 1.25,
                    "cpu_seconds": 0.5,
                    "buffer_gets": 100,
                    "disk_reads": 5,
                    "rows_processed": 20,
                    "parsing_schema_name": "SCOTT",
                    "last_active_time": "2026-11-20 09:00:00",
                }
            ]
        )

        statements = show.query_running_sql(backend)

        assert len(statements) == 1
        statement = statements[0]
        assert isinstance(statement, OracleSqlInfo)
        assert statement.sql_id == "abc123def456"
        assert statement.child_number == 0
        assert statement.sql_text == "SELECT 1 FROM DUAL"
        assert statement.executions == 10
        assert statement.elapsed_seconds == 1.25
        assert statement.cpu_seconds == 0.5
        assert statement.buffer_gets == 100
        assert statement.disk_reads == 5
        assert statement.rows_processed == 20
        assert statement.parsing_schema_name == "SCOTT"
        assert statement.last_active_time == "2026-11-20 09:00:00"

    def test_empty_rows_return_empty_list(self):
        backend = seeded_backend([])

        assert show.query_running_sql(backend) == []

    @pytest.mark.parametrize("limit", [1, 7, 50])
    def test_limit_is_bound_as_fetch_first_param(self, limit):
        backend = seeded_backend([])

        show.query_running_sql(backend, limit=limit)

        sql, params = executed_sql(backend)
        assert "V$SQL" in sql.upper()
        assert "FETCH FIRST ? ROWS ONLY" in sql
        assert params == (limit,)


# ---------------------------------------------------------------------------
# query_database_info / query_instance_info
# ---------------------------------------------------------------------------


class TestQueryDatabaseInfo:
    """Tests for ``query_database_info``."""

    def test_maps_row_to_oracle_database_info(self):
        backend = seeded_backend(
            [
                {
                    "name": "ORCL",
                    "dbid": 1234567890,
                    "created": "2019-01-01 00:00:00",
                    "log_mode": "ARCHIVELOG",
                    "open_mode": "READ WRITE",
                    "platform_name": "Linux x86 64-bit",
                    "role": "PRIMARY",
                }
            ]
        )

        info = show.query_database_info(backend)

        assert isinstance(info, OracleDatabaseInfo)
        assert info.name == "ORCL"
        assert info.dbid == 1234567890
        assert info.created == "2019-01-01 00:00:00"
        assert info.log_mode == "ARCHIVELOG"
        assert info.open_mode == "READ WRITE"
        assert info.platform_name == "Linux x86 64-bit"
        assert info.role == "PRIMARY"

    def test_empty_rows_return_none(self):
        backend = seeded_backend([])

        assert show.query_database_info(backend) is None

    def test_uses_first_row_only(self):
        backend = seeded_backend([{"name": "FIRST", "dbid": 1}, {"name": "SECOND", "dbid": 2}])

        info = show.query_database_info(backend)

        assert info.name == "FIRST"

    def test_uses_v_database_view(self):
        backend = seeded_backend([])

        show.query_database_info(backend)

        sql, params = executed_sql(backend)
        assert "V$DATABASE" in sql.upper()
        assert params == ()


class TestQueryInstanceInfo:
    """Tests for ``query_instance_info``."""

    def test_maps_row_to_oracle_instance_info(self):
        backend = seeded_backend(
            [
                {
                    "instance_name": "orcl",
                    "instance_num": 1,
                    "host_name": "dbhost",
                    "version": "19.0.0.0.0",
                    "status": "OPEN",
                    "startup_time": "2026-11-20 08:00:00",
                    "archiver": "STOPPED",
                }
            ]
        )

        info = show.query_instance_info(backend)

        assert isinstance(info, OracleInstanceInfo)
        assert info.instance_name == "orcl"
        assert info.instance_num == 1
        assert info.host_name == "dbhost"
        assert info.version == "19.0.0.0.0"
        assert info.status == "OPEN"
        assert info.startup_time == "2026-11-20 08:00:00"
        assert info.archiver == "STOPPED"

    def test_empty_rows_return_none(self):
        backend = seeded_backend([])

        assert show.query_instance_info(backend) is None

    def test_uses_v_instance_view(self):
        backend = seeded_backend([])

        show.query_instance_info(backend)

        sql, params = executed_sql(backend)
        assert "V$INSTANCE" in sql.upper()
        assert params == ()


# ---------------------------------------------------------------------------
# query_objects
# ---------------------------------------------------------------------------


class TestQueryObjects:
    """Tests for ``query_objects``."""

    def test_maps_full_row_to_oracle_object_info(self):
        backend = seeded_backend(
            [
                {
                    "object_name": "EMP",
                    "object_type": "TABLE",
                    "owner": "SCOTT",
                    "object_id": 12345,
                    "created": "2020-01-01 00:00:00",
                    "last_ddl_time": "2021-01-01 00:00:00",
                    "status": "VALID",
                    "temporary": "N",
                }
            ]
        )

        objects = show.query_objects(backend)

        assert len(objects) == 1
        obj = objects[0]
        assert isinstance(obj, OracleObjectInfo)
        assert obj.object_name == "EMP"
        assert obj.object_type == "TABLE"
        assert obj.owner == "SCOTT"
        assert obj.object_id == 12345
        assert obj.created == "2020-01-01 00:00:00"
        assert obj.last_ddl_time == "2021-01-01 00:00:00"
        assert obj.status == "VALID"
        assert obj.temporary == "N"

    def test_empty_rows_return_empty_list(self):
        backend = seeded_backend([])

        assert show.query_objects(backend) == []

    def test_defaults_use_user_objects_with_valid_filter(self):
        backend = seeded_backend([])

        show.query_objects(backend)

        sql, params = executed_sql(backend)
        assert "USER_OBJECTS" in sql.upper()
        assert "ALL_OBJECTS" not in sql.upper()
        assert "'VALID'" in sql
        assert params == ()

    @pytest.mark.parametrize(
        ("owner", "expected_view"),
        [(None, "USER_OBJECTS"), ("SCOTT", "ALL_OBJECTS")],
    )
    def test_owner_selects_view(self, owner, expected_view):
        backend = seeded_backend([])

        show.query_objects(backend, owner=owner)

        sql, _ = executed_sql(backend)
        assert expected_view in sql.upper()

    def test_filters_are_bound_and_include_invalid_drops_valid_filter(self):
        backend = seeded_backend([])

        show.query_objects(
            backend,
            object_type="TABLE",
            owner="SCOTT",
            name_pattern="EMP%",
            include_invalid=True,
        )

        sql, params = executed_sql(backend)
        assert params == ("SCOTT", "TABLE", "EMP%")
        assert "LIKE ?" in sql
        assert "'VALID'" not in sql

    def test_include_invalid_false_keeps_valid_filter(self):
        backend = seeded_backend([])

        show.query_objects(backend, include_invalid=False)

        sql, params = executed_sql(backend)
        assert "'VALID'" in sql
        assert params == ()


# ---------------------------------------------------------------------------
# query_locks / query_wait_events
# ---------------------------------------------------------------------------


class TestQueryLocks:
    """Tests for ``query_locks``."""

    def test_maps_full_row_to_oracle_lock_info(self):
        backend = seeded_backend(
            [
                {
                    "sid": 12,
                    "lock_type": "TM",
                    "id1": 100,
                    "id2": 0,
                    "lmode": 3,
                    "request": 0,
                    "block": 0,
                }
            ]
        )

        locks = show.query_locks(backend)

        assert len(locks) == 1
        lock = locks[0]
        assert isinstance(lock, OracleLockInfo)
        assert lock.sid == 12
        assert lock.lock_type == "TM"
        assert lock.id1 == 100
        assert lock.id2 == 0
        assert lock.lmode == 3
        assert lock.request == 0
        assert lock.block == 0

    def test_empty_rows_return_empty_list(self):
        backend = seeded_backend([])

        assert show.query_locks(backend) == []

    def test_uses_v_lock_view(self):
        backend = seeded_backend([])

        show.query_locks(backend)

        sql, params = executed_sql(backend)
        assert "V$LOCK" in sql.upper()
        assert params == ()


class TestQueryWaitEvents:
    """Tests for ``query_wait_events``."""

    def test_maps_full_row_to_oracle_wait_event_info(self):
        backend = seeded_backend(
            [
                {
                    "sid": 12,
                    "seq#": 7,
                    "event": "db file sequential read",
                    "p1text": "file#",
                    "p1": 4,
                    "wait_class": "User I/O",
                    "wait_time": 0,
                    "seconds_in_wait": 3.5,
                    "state": "WAITING",
                }
            ]
        )

        events = show.query_wait_events(backend)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, OracleWaitEventInfo)
        assert event.sid == 12
        assert event.seq_num == 7
        assert event.event == "db file sequential read"
        assert event.p1text == "file#"
        assert event.p1 == 4
        assert event.wait_class == "User I/O"
        assert event.wait_time == 0
        assert event.seconds_in_wait == 3.5
        assert event.state == "WAITING"

    def test_empty_rows_return_empty_list(self):
        backend = seeded_backend([])

        assert show.query_wait_events(backend) == []

    def test_uses_v_session_wait_view(self):
        backend = seeded_backend([])

        show.query_wait_events(backend)

        sql, params = executed_sql(backend)
        assert "V$SESSION_WAIT" in sql.upper()
        assert params == ()


# ---------------------------------------------------------------------------
# query_nls_parameters
# ---------------------------------------------------------------------------


class TestQueryNlsParameters:
    """Tests for ``query_nls_parameters``."""

    def test_maps_rows_to_oracle_nls_parameter(self):
        backend = seeded_backend(
            [
                {"parameter": "NLS_LANGUAGE", "value": "AMERICAN"},
                {"parameter": "NLS_TERRITORY", "value": "AMERICA"},
            ]
        )

        params = show.query_nls_parameters(backend)

        assert [type(param) for param in params] == [OracleNlsParameter] * 2
        assert params[0].parameter == "NLS_LANGUAGE"
        assert params[0].value == "AMERICAN"
        assert params[1].parameter == "NLS_TERRITORY"
        assert params[1].value == "AMERICA"

    def test_missing_value_becomes_none(self):
        backend = seeded_backend([{"parameter": "NLS_CURRENCY"}])

        param = show.query_nls_parameters(backend)[0]

        assert param.value is None

    def test_empty_rows_return_empty_list(self):
        backend = seeded_backend([])

        assert show.query_nls_parameters(backend) == []

    @pytest.mark.parametrize(
        ("scope", "expected_view"),
        [
            ("SESSION", "NLS_SESSION_PARAMETERS"),
            ("DATABASE", "NLS_DATABASE_PARAMETERS"),
            ("database", "NLS_DATABASE_PARAMETERS"),
        ],
    )
    def test_scope_selects_view(self, scope, expected_view):
        backend = seeded_backend([])

        show.query_nls_parameters(backend, scope=scope)

        sql, params = executed_sql(backend)
        assert expected_view in sql.upper()
        assert params == ()


# ---------------------------------------------------------------------------
# QUERY_LAMBDAS registry and stateful wrapper
# ---------------------------------------------------------------------------

REGISTRY = {
    "sessions": "query_sessions",
    "running_sql": "query_running_sql",
    "database_info": "query_database_info",
    "instance_info": "query_instance_info",
    "objects": "query_objects",
    "locks": "query_locks",
    "wait_events": "query_wait_events",
    "nls_parameters": "query_nls_parameters",
}


class TestQueryLambdasRegistry:
    """Tests for the public ``QUERY_LAMBDAS`` table."""

    @pytest.mark.parametrize(("name", "func_name"), sorted(REGISTRY.items()))
    def test_registry_points_at_module_functions(self, name, func_name):
        assert show.QUERY_LAMBDAS[name] is getattr(show, func_name)

    @pytest.mark.parametrize("name", sorted(REGISTRY))
    def test_registry_callable_end_to_end(self, name):
        backend = seeded_backend([UNIVERSAL_ROW])

        result = show.QUERY_LAMBDAS[name](backend)

        assert result is not None
        executed_sql(backend)


class TestOracleShowFunctionality:
    """Tests for the stateful ``OracleShowFunctionality`` wrapper."""

    def test_exposes_backend_dialect_and_version(self):
        backend = seeded_backend([])

        wrapper = show.OracleShowFunctionality(backend, version=(19, 0, 0))

        assert wrapper.dialect is backend.dialect
        assert wrapper._version == (19, 0, 0)

    @pytest.mark.parametrize(
        ("method", "kwargs", "expected_type"),
        [
            ("sessions", {}, list),
            ("sessions", {"active_only": True}, list),
            ("running_sql", {"limit": 3}, list),
            ("database_info", {}, OracleDatabaseInfo),
            ("instance_info", {}, OracleInstanceInfo),
            ("objects", {"object_type": "TABLE"}, list),
            ("locks", {}, list),
            ("wait_events", {}, list),
            ("nls_parameters", {"scope": "DATABASE"}, list),
        ],
    )
    def test_methods_delegate_to_module_functions(self, method, kwargs, expected_type):
        backend = seeded_backend([UNIVERSAL_ROW])
        wrapper = show.OracleShowFunctionality(backend)

        result = getattr(wrapper, method)(**kwargs)

        assert isinstance(result, expected_type)
        executed_sql(backend)

    def test_objects_forwards_all_kwargs(self):
        backend = seeded_backend([])
        wrapper = show.OracleShowFunctionality(backend)

        wrapper.objects(object_type="VIEW", owner="SCOTT", include_invalid=True, name_pattern="V%")

        sql, params = executed_sql(backend)
        assert "ALL_OBJECTS" in sql.upper()
        assert params == ("SCOTT", "VIEW", "V%")
