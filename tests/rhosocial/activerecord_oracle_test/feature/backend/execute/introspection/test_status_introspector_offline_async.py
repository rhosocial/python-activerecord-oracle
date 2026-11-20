# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/introspection/
# test_status_introspector_offline_async.py
"""Database-free unit tests for the asynchronous Oracle status introspector.

Oracle's ``oracledb`` driver has native asyncio support, so the async
introspector is a first-class surface and gets the same deterministic,
mock-driven coverage as its synchronous sibling in
``test_status_introspector_offline.py``.  No listener, socket or network is
involved: every I/O seam is an ``AsyncMock``.

``asyncio_mode = auto`` is configured for the suite, so plain ``async def``
tests need no explicit marker.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.status_introspector import (
    ORACLE_CONFIG_PARAMS,
    TablespaceInfo,
    AsyncOracleStatusIntrospector,
)
from rhosocial.activerecord.backend.introspection.status import (
    ConnectionInfo,
    DatabaseBriefInfo,
    SessionInfo,
    StatusCategory,
    StatusItem,
    StorageInfo,
    UserInfo,
)


def make_async_introspector():
    """Build an async introspector around a fully mocked backend."""
    backend = MagicMock()
    backend.dialect = OracleDialect()
    backend._convert_placeholders_to_oracle.side_effect = lambda sql, params: (sql, params)
    return AsyncOracleStatusIntrospector(backend=backend), backend


def make_async_cursor(rows, description, error=None):
    """Return an oracledb-like async cursor double.

    ``execute`` and ``fetchall`` are awaited by the introspector, while
    ``close`` stays synchronous -- oracledb's AsyncCursor.close() is not a
    coroutine, a distinction these tests pin down explicitly.
    """
    cursor = MagicMock()
    cursor.description = description
    cursor.execute = AsyncMock(side_effect=error) if error else AsyncMock()
    cursor.fetchall = AsyncMock(return_value=rows)
    cursor.close = MagicMock()
    return cursor


class TestAsyncQueryHelpers:
    """Tests for the async private cursor-driving helpers."""

    async def test_execute_query_returns_rows_and_closes_cursor(self):
        introspector, backend = make_async_introspector()
        cursor = make_async_cursor([("Oracle Database 19c",)], [("BANNER",)])
        backend._connection.cursor.return_value = cursor

        rows = await introspector._execute_query("SELECT banner FROM V$VERSION")

        assert rows == [("Oracle Database 19c",)]
        cursor.execute.assert_awaited_once_with("SELECT banner FROM V$VERSION")
        cursor.close.assert_called_once_with()

    async def test_execute_query_closes_cursor_on_failure(self):
        introspector, backend = make_async_introspector()
        cursor = make_async_cursor([], [], error=RuntimeError("boom"))
        backend._connection.cursor.return_value = cursor

        with pytest.raises(RuntimeError, match="boom"):
            await introspector._execute_query("SELECT 1 FROM DUAL")
        cursor.close.assert_called_once_with()

    async def test_execute_query_dict_zips_lowercased_columns(self):
        introspector, backend = make_async_introspector()
        cursor = make_async_cursor(
            [("processes", "500", "TRUE")],
            [("NAME", "VARCHAR2"), ("VALUE", "VARCHAR2"), ("ISDEFAULT", "VARCHAR2")],
        )
        backend._connection.cursor.return_value = cursor

        rows = await introspector._execute_query_dict("SELECT ? FROM V$PARAMETER", ("name",))

        assert rows == [{"name": "processes", "value": "500", "isdefault": "TRUE"}]
        backend._convert_placeholders_to_oracle.assert_called_once_with(
            "SELECT ? FROM V$PARAMETER", ("name",)
        )
        cursor.execute.assert_awaited_once_with("SELECT ? FROM V$PARAMETER", ("name",))
        cursor.fetchall.assert_awaited_once_with()
        cursor.close.assert_called_once_with()

    async def test_execute_query_dict_without_params_uses_single_argument(self):
        introspector, backend = make_async_introspector()
        cursor = make_async_cursor([(1,)], [("CNT",)])
        backend._connection.cursor.return_value = cursor

        rows = await introspector._execute_query_dict("SELECT COUNT(*) FROM DBA_USERS")

        assert rows == [{"cnt": 1}]
        cursor.execute.assert_awaited_once_with("SELECT COUNT(*) FROM DBA_USERS")

    async def test_execute_query_dict_closes_cursor_on_failure(self):
        introspector, backend = make_async_introspector()
        cursor = make_async_cursor([], [], error=RuntimeError("bad bind"))
        backend._connection.cursor.return_value = cursor

        with pytest.raises(RuntimeError, match="bad bind"):
            await introspector._execute_query_dict("SELECT ? FROM DUAL", (1,))
        cursor.close.assert_called_once_with()


class TestAsyncGetVersionString:
    """Tests for the async ``_get_version_string``."""

    async def test_returns_first_column_of_first_row(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query = AsyncMock(
            return_value=[("Oracle Database 21c",), ("second banner",)]
        )

        assert await introspector._get_version_string() == "Oracle Database 21c"
        introspector._execute_query.assert_awaited_once()

    async def test_returns_unknown_for_empty_result(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query = AsyncMock(return_value=[])

        assert await introspector._get_version_string() == "Unknown"

    async def test_returns_unknown_when_query_raises(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query = AsyncMock(side_effect=RuntimeError("no listener"))

        assert await introspector._get_version_string() == "Unknown"


class TestAsyncListConfiguration:
    """Tests for the async ``list_configuration``."""

    @staticmethod
    def _rows():
        return [
            {"name": "processes", "value": "500", "isdefault": "TRUE"},
            {"name": "sga_target", "value": "1073741824", "isdefault": "FALSE"},
            {"name": "db_block_size", "value": "8192", "isdefault": "TRUE"},
            {"name": "not_a_known_param", "value": "x", "isdefault": "TRUE"},
        ]

    async def test_maps_known_rows_to_status_items(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(return_value=self._rows())

        items = await introspector.list_configuration()

        assert [item.name for item in items] == ["processes", "sga_target", "db_block_size"]
        processes, sga, block = items
        assert all(isinstance(item, StatusItem) for item in items)
        assert processes.category is StatusCategory.CONNECTION
        assert processes.description == "Maximum number of OS processes"
        assert processes.unit is None
        assert processes.is_readonly is True
        assert sga.category is StatusCategory.PERFORMANCE
        assert sga.unit == "bytes"
        assert sga.description == "SGA target size"
        assert sga.is_readonly is False
        assert block.category is StatusCategory.STORAGE

        sql, params = introspector._execute_query_dict.call_args.args
        assert "V$PARAMETER" in sql
        assert params == tuple(name for name, *_ in ORACLE_CONFIG_PARAMS)

    @pytest.mark.parametrize(
        ("category", "expected_names"),
        [
            (StatusCategory.CONNECTION, ["processes"]),
            (StatusCategory.PERFORMANCE, ["sga_target"]),
            (StatusCategory.STORAGE, ["db_block_size"]),
        ],
    )
    async def test_filters_by_category(self, category, expected_names):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(return_value=self._rows())

        items = await introspector.list_configuration(category)

        assert [item.name for item in items] == expected_names

    async def test_missing_isdefault_defaults_to_readonly(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[{"name": "sessions", "value": "300"}]
        )

        items = await introspector.list_configuration()

        assert len(items) == 1
        assert items[0].is_readonly is True
        assert items[0].value == "300"

    async def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("ORA-00942"))

        assert await introspector.list_configuration() == []


class TestAsyncListPerformanceMetrics:
    """Tests for the async ``list_performance_metrics``."""

    async def test_maps_rows_to_performance_items(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[
                {"name": "physical reads", "value": 123},
                {"name": "execute count", "value": 456},
            ]
        )

        items = await introspector.list_performance_metrics()

        assert [item.name for item in items] == ["physical reads", "execute count"]
        assert all(item.category is StatusCategory.PERFORMANCE for item in items)
        assert all(item.is_readonly is True for item in items)
        assert all(item.is_dynamic is False for item in items)
        sql = introspector._execute_query_dict.call_args.args[0]
        assert "V$SYSSTAT" in sql

    async def test_accepts_category_argument_and_returns_empty_for_empty_rows(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(return_value=[])

        assert await introspector.list_performance_metrics(StatusCategory.CONNECTION) == []

    async def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("ORA-00942"))

        assert await introspector.list_performance_metrics() == []


class TestAsyncGetConnectionInfo:
    """Tests for the async ``get_connection_info``."""

    async def test_reads_active_count_and_max_sessions(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            side_effect=[[{"cnt": 17}], [{"value": "500"}]]
        )

        info = await introspector.get_connection_info()

        assert isinstance(info, ConnectionInfo)
        assert info.active_count == 17
        assert info.max_connections == 500
        assert introspector._execute_query_dict.await_count == 2

    async def test_defaults_when_both_queries_fail(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("down"))

        info = await introspector.get_connection_info()

        assert info.active_count == 0
        assert info.max_connections is None

    async def test_empty_results_use_fallbacks(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=[[], []])

        info = await introspector.get_connection_info()

        assert info.active_count == 0
        assert info.max_connections is None


class TestAsyncGetStorageInfo:
    """Tests for the async ``get_storage_info``."""

    async def test_reads_total_and_free_bytes(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            side_effect=[
                [{"total_bytes": 1073741824}],
                [{"free_bytes": 268435456}],
            ]
        )

        info = await introspector.get_storage_info()

        assert isinstance(info, StorageInfo)
        assert info.total_size_bytes == 1073741824
        assert info.free_space_bytes == 268435456
        assert introspector._execute_query_dict.await_count == 2

    async def test_null_totals_become_none(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            side_effect=[[{"total_bytes": None}], [{"free_bytes": None}]]
        )

        info = await introspector.get_storage_info()

        assert info.total_size_bytes is None
        assert info.free_space_bytes is None

    async def test_query_errors_become_none(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("ORA-00942"))

        info = await introspector.get_storage_info()

        assert info.total_size_bytes is None
        assert info.free_space_bytes is None


class TestAsyncListDatabases:
    """Tests for the async ``list_databases``."""

    async def test_maps_database_names(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[{"name": "ORCL"}, {"name": "PDB1"}]
        )

        dbs = await introspector.list_databases()

        assert [db.name for db in dbs] == ["ORCL", "PDB1"]
        assert all(isinstance(db, DatabaseBriefInfo) for db in dbs)
        assert "V$DATABASE" in introspector._execute_query_dict.call_args.args[0]

    async def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("down"))

        assert await introspector.list_databases() == []


class TestAsyncListUsers:
    """Tests for the async ``list_users``."""

    async def test_maps_users_and_dba_flag(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[
                {"username": "SYS", "account_status": "OPEN", "is_dba": "Y"},
                {"username": "SCOTT", "account_status": "LOCKED", "is_dba": "N"},
            ]
        )

        users = await introspector.list_users()

        assert [user.name for user in users] == ["SYS", "SCOTT"]
        assert all(isinstance(user, UserInfo) for user in users)
        assert users[0].is_superuser is True
        assert users[0].extra == {"account_status": "OPEN"}
        assert users[1].is_superuser is False
        assert users[1].extra == {"account_status": "LOCKED"}

    async def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("down"))

        assert await introspector.list_users() == []


class TestAsyncGetSessionInfo:
    """Tests for the async ``get_session_info``."""

    async def test_maps_sys_context_row(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[
                {
                    "session_user": "SCOTT",
                    "db_name": "ORCL",
                    "current_schema": "SCOTT",
                    "host": "workstation",
                }
            ]
        )

        session = await introspector.get_session_info()

        assert isinstance(session, SessionInfo)
        assert session.user == "SCOTT"
        assert session.database == "ORCL"
        assert session.schema == "SCOTT"
        assert session.host == "workstation"

    async def test_empty_result_returns_default_session(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(return_value=[])

        session = await introspector.get_session_info()

        assert session.user is None
        assert session.database is None
        assert session.schema is None
        assert session.host is None

    async def test_query_error_returns_default_session(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("down"))

        session = await introspector.get_session_info()

        assert session.user is None
        assert session.extra == {}


class TestAsyncListTablespaces:
    """Tests for the async ``list_tablespaces``."""

    async def test_computes_used_bytes_and_percentage(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[
                {
                    "tablespace_name": "USERS",
                    "status": "ONLINE",
                    "contents": "PERMANENT",
                    "total_bytes": 1000,
                    "free_bytes": 250,
                }
            ]
        )

        result = await introspector.list_tablespaces()

        assert len(result) == 1
        ts = result[0]
        assert isinstance(ts, TablespaceInfo)
        assert ts.name == "USERS"
        assert ts.status == "ONLINE"
        assert ts.contents == "PERMANENT"
        assert ts.size_bytes == 1000
        assert ts.free_bytes == 250
        assert ts.used_bytes == 750
        assert ts.pct_used == 75.0

    async def test_zero_total_gives_zero_percentage(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[{"tablespace_name": "EMPTY", "total_bytes": 0, "free_bytes": 0}]
        )

        ts = (await introspector.list_tablespaces())[0]

        assert ts.used_bytes == 0
        assert ts.pct_used == 0.0

    async def test_missing_and_null_sizes_default_to_zero(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[{"tablespace_name": "NULLS", "total_bytes": None, "free_bytes": None}]
        )

        ts = (await introspector.list_tablespaces())[0]

        assert ts.size_bytes == 0
        assert ts.free_bytes == 0
        assert ts.status is None
        assert ts.contents is None

    async def test_percentage_is_rounded_to_one_decimal(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(
            return_value=[{"tablespace_name": "R", "total_bytes": 3, "free_bytes": 1}]
        )

        ts = (await introspector.list_tablespaces())[0]

        assert ts.pct_used == 66.7

    async def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_async_introspector()
        introspector._execute_query_dict = AsyncMock(side_effect=RuntimeError("down"))

        assert await introspector.list_tablespaces() == []


class TestAsyncGetOverview:
    """Tests for the async orchestration in ``get_overview``."""

    async def test_assembles_all_sections(self):
        introspector, _ = make_async_introspector()
        config = [StatusItem("processes", "500", StatusCategory.CONNECTION)]
        perf = [StatusItem("physical reads", 9, StatusCategory.PERFORMANCE)]
        session = SessionInfo(user="SCOTT")
        conn = ConnectionInfo(active_count=3)
        storage = StorageInfo(total_size_bytes=1)
        databases = [DatabaseBriefInfo(name="ORCL")]
        users = [UserInfo(name="SYS")]
        providers = {
            "_get_version_string": "Oracle Database 19c",
            "get_session_info": session,
            "list_configuration": config,
            "list_performance_metrics": perf,
            "get_connection_info": conn,
            "get_storage_info": storage,
            "list_databases": databases,
            "list_users": users,
        }
        for name, value in providers.items():
            setattr(introspector, name, AsyncMock(return_value=value))

        overview = await introspector.get_overview()

        assert overview.server_version == "Oracle Database 19c"
        assert overview.server_vendor == "Oracle"
        assert overview.configuration == config + perf
        assert overview.session is session
        assert overview.connections is conn
        assert overview.storage is storage
        assert overview.databases is databases
        assert overview.users is users
        for name in providers:
            getattr(introspector, name).assert_awaited_once_with()
