# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/introspection/test_status_introspector_offline.py
"""Database-free unit tests for the synchronous Oracle status introspector.

The sibling ``test_introspection_status.py`` only checks compatibility
contracts against a live server; it cannot exercise the parsing branches of
``OracleStatusIntrospector`` deterministically.  Every test here drives the
sync class through fake backends, cursors and query results, so the whole
file runs with no Oracle listener and no network access.

The async introspector is intentionally out of scope for this module.
"""

from unittest.mock import MagicMock

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.status_introspector import (
    ORACLE_CONFIG_PARAMS,
    TablespaceInfo,
    OracleStatusIntrospector,
    OracleStatusIntrospectorMixin,
    _dialect_of,
    _oracle_statement,
    _parameter_query,
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


def make_introspector():
    """Build a sync introspector around a fully mocked backend."""
    backend = MagicMock()
    backend.dialect = OracleDialect()
    # The placeholder conversion is a backend concern; echo its inputs back
    # so tests can assert what the introspector handed to the driver.
    backend._convert_placeholders_to_oracle.side_effect = lambda sql, params: (sql, params)
    return OracleStatusIntrospector(backend=backend), backend


def make_cursor(rows, description):
    """Return a cursor mock with a fixed result set."""
    cursor = MagicMock()
    cursor.description = description
    cursor.fetchall.return_value = rows
    return cursor


class TestPureHelpers:
    """Tests for the module-level helpers and the non-I/O mixin."""

    def test_get_vendor_name(self):
        introspector, _ = make_introspector()
        assert introspector._get_vendor_name() == "Oracle"

    def test_create_status_item_defaults(self):
        item = OracleStatusIntrospectorMixin()._create_status_item("pga_aggregate_target", 1024,
                                                                   StatusCategory.PERFORMANCE)
        assert isinstance(item, StatusItem)
        assert item.name == "pga_aggregate_target"
        assert item.value == 1024
        assert item.category is StatusCategory.PERFORMANCE
        assert item.description is None
        assert item.unit is None
        assert item.is_readonly is True
        assert item.is_dynamic is False
        assert item.extra == {}

    def test_create_status_item_all_fields(self):
        item = OracleStatusIntrospectorMixin()._create_status_item(
            "db_block_size", 8192, StatusCategory.STORAGE,
            description="Database block size", unit="bytes",
            is_readonly=False, is_dynamic=True,
        )
        assert item.description == "Database block size"
        assert item.unit == "bytes"
        assert item.is_readonly is False
        assert item.is_dynamic is True

    def test_build_server_overview_merges_configuration_and_performance(self):
        introspector, _ = make_introspector()
        config = [StatusItem("db_block_size", 8192, StatusCategory.STORAGE)]
        perf = [StatusItem("physical reads", 42, StatusCategory.PERFORMANCE)]
        session = SessionInfo(user="SCOTT", database="ORCL")
        conn = ConnectionInfo(active_count=5)
        storage = StorageInfo(total_size_bytes=100)
        dbs = [DatabaseBriefInfo(name="ORCL")]
        users = [UserInfo(name="SYS", is_superuser=True)]

        overview = introspector._build_server_overview(
            "Oracle Database 19c", session, config, perf, conn, storage, dbs, users
        )

        assert isinstance(overview, object)
        assert overview.server_vendor == "Oracle"
        assert overview.server_version == "Oracle Database 19c"
        assert overview.configuration == config + perf
        assert overview.performance == []
        assert overview.session is session
        assert overview.connections is conn
        assert overview.storage is storage
        assert overview.databases is dbs
        assert overview.users is users
        assert overview.get_items_by_category(StatusCategory.STORAGE) == [config[0]]

    def test_parameter_query_binds_names_instead_of_interpolating(self):
        sql, params = _parameter_query(OracleDialect()).to_sql()

        assert "V$PARAMETER" in sql
        assert "?" in sql
        assert sql.count("?") == len(params)
        assert params == tuple(name for name, *_ in ORACLE_CONFIG_PARAMS)
        for name, *_ in ORACLE_CONFIG_PARAMS:
            assert name not in sql, f"{name} must travel as a bind value, not SQL text"

    @pytest.mark.parametrize(
        "entry",
        ORACLE_CONFIG_PARAMS,
        ids=[param[0] for param in ORACLE_CONFIG_PARAMS],
    )
    def test_oracle_config_params_are_well_formed(self, entry):
        name, category, description, unit = entry
        assert isinstance(name, str) and name
        assert isinstance(category, StatusCategory)
        assert isinstance(description, str) and description
        assert unit is None or isinstance(unit, str)

    def test_dialect_of_reads_backend_dialect(self):
        introspector, backend = make_introspector()
        assert _dialect_of(introspector) is backend.dialect

    def test_oracle_statement_delegates_placeholder_conversion(self):
        introspector, backend = make_introspector()
        backend._convert_placeholders_to_oracle.side_effect = None
        backend._convert_placeholders_to_oracle.return_value = ("SELECT :1", (7,))

        result = _oracle_statement(introspector, "SELECT ?", (7,))

        assert result == ("SELECT :1", (7,))
        backend._convert_placeholders_to_oracle.assert_called_once_with("SELECT ?", (7,))


class TestQueryHelpers:
    """Tests for the private cursor-driving helpers."""

    def test_execute_query_returns_rows_and_closes_cursor(self):
        introspector, backend = make_introspector()
        cursor = make_cursor([("Oracle Database 19c",)], [("BANNER",)])
        backend._connection.cursor.return_value = cursor

        rows = introspector._execute_query("SELECT banner FROM V$VERSION")

        assert rows == [("Oracle Database 19c",)]
        cursor.execute.assert_called_once_with("SELECT banner FROM V$VERSION")
        cursor.close.assert_called_once_with()

    def test_execute_query_closes_cursor_on_failure(self):
        introspector, backend = make_introspector()
        cursor = make_cursor([], [])
        cursor.execute.side_effect = RuntimeError("boom")
        backend._connection.cursor.return_value = cursor

        with pytest.raises(RuntimeError, match="boom"):
            introspector._execute_query("SELECT 1 FROM DUAL")
        cursor.close.assert_called_once_with()

    def test_execute_query_dict_zips_lowercased_columns(self):
        introspector, backend = make_introspector()
        cursor = make_cursor(
            [("processes", "500", "TRUE")],
            [("NAME", "VARCHAR2"), ("VALUE", "VARCHAR2"), ("ISDEFAULT", "VARCHAR2")],
        )
        backend._connection.cursor.return_value = cursor

        rows = introspector._execute_query_dict("SELECT ? FROM V$PARAMETER", ("name",))

        assert rows == [{"name": "processes", "value": "500", "isdefault": "TRUE"}]
        backend._convert_placeholders_to_oracle.assert_called_once_with(
            "SELECT ? FROM V$PARAMETER", ("name",)
        )
        cursor.execute.assert_called_once_with("SELECT ? FROM V$PARAMETER", ("name",))
        cursor.close.assert_called_once_with()

    def test_execute_query_dict_without_params_passes_single_argument(self):
        introspector, backend = make_introspector()
        cursor = make_cursor([(1,)], [("CNT",)])
        backend._connection.cursor.return_value = cursor

        rows = introspector._execute_query_dict("SELECT COUNT(*) FROM DBA_USERS")

        assert rows == [{"cnt": 1}]
        cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM DBA_USERS")

    def test_execute_query_dict_closes_cursor_on_failure(self):
        introspector, backend = make_introspector()
        cursor = make_cursor([], [])
        cursor.execute.side_effect = RuntimeError("bad bind")
        backend._connection.cursor.return_value = cursor

        with pytest.raises(RuntimeError, match="bad bind"):
            introspector._execute_query_dict("SELECT ? FROM DUAL", (1,))
        cursor.close.assert_called_once_with()


class TestGetVersionString:
    """Tests for ``_get_version_string``."""

    def test_returns_first_column_of_first_row(self):
        introspector, _ = make_introspector()
        introspector._execute_query = MagicMock(
            return_value=[("Oracle Database 21c",), ("second banner",)]
        )

        assert introspector._get_version_string() == "Oracle Database 21c"

    def test_returns_unknown_for_empty_result(self):
        introspector, _ = make_introspector()
        introspector._execute_query = MagicMock(return_value=[])

        assert introspector._get_version_string() == "Unknown"

    def test_returns_unknown_when_query_raises(self):
        introspector, _ = make_introspector()
        introspector._execute_query = MagicMock(side_effect=RuntimeError("no listener"))

        assert introspector._get_version_string() == "Unknown"


class TestListConfiguration:
    """Tests for ``list_configuration``."""

    def _rows(self):
        return [
            {"name": "processes", "value": "500", "isdefault": "TRUE"},
            {"name": "sga_target", "value": "1073741824", "isdefault": "FALSE"},
            {"name": "db_block_size", "value": "8192", "isdefault": "TRUE"},
            {"name": "not_a_known_param", "value": "x", "isdefault": "TRUE"},
        ]

    def test_maps_known_rows_to_status_items(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(return_value=self._rows())

        items = introspector.list_configuration()

        assert [item.name for item in items] == ["processes", "sga_target", "db_block_size"]
        processes, sga, block = items
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
    def test_filters_by_category(self, category, expected_names):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(return_value=self._rows())

        items = introspector.list_configuration(category)

        assert [item.name for item in items] == expected_names

    def test_missing_isdefault_defaults_to_readonly(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[{"name": "sessions", "value": "300"}]
        )

        items = introspector.list_configuration()

        assert len(items) == 1
        assert items[0].is_readonly is True

    def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("ORA-00942"))

        assert introspector.list_configuration() == []


class TestListPerformanceMetrics:
    """Tests for ``list_performance_metrics``."""

    def test_maps_rows_to_performance_items(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[
                {"name": "physical reads", "value": 123},
                {"name": "execute count", "value": 456},
            ]
        )

        items = introspector.list_performance_metrics()

        assert [item.name for item in items] == ["physical reads", "execute count"]
        assert all(item.category is StatusCategory.PERFORMANCE for item in items)
        assert all(item.is_readonly is True for item in items)
        assert all(item.is_dynamic is False for item in items)
        assert all(item.value is not None for item in items)
        sql = introspector._execute_query_dict.call_args.args[0]
        assert "V$SYSSTAT" in sql

    def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("ORA-00942"))

        assert introspector.list_performance_metrics() == []


class TestGetConnectionInfo:
    """Tests for ``get_connection_info``."""

    def test_reads_active_count_and_max_sessions(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            side_effect=[[{"cnt": 17}], [{"value": "500"}]]
        )

        info = introspector.get_connection_info()

        assert isinstance(info, ConnectionInfo)
        assert info.active_count == 17
        assert info.max_connections == 500
        assert introspector._execute_query_dict.call_count == 2

    def test_defaults_when_both_queries_fail(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("down"))

        info = introspector.get_connection_info()

        assert info.active_count == 0
        assert info.max_connections is None

    def test_empty_results_use_fallbacks(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=[[], []])

        info = introspector.get_connection_info()

        assert info.active_count == 0
        assert info.max_connections is None


class TestGetStorageInfo:
    """Tests for ``get_storage_info``."""

    def test_reads_total_and_free_bytes(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            side_effect=[
                [{"total_bytes": 1073741824}],
                [{"free_bytes": 268435456}],
            ]
        )

        info = introspector.get_storage_info()

        assert isinstance(info, StorageInfo)
        assert info.total_size_bytes == 1073741824
        assert info.free_space_bytes == 268435456
        assert introspector._execute_query_dict.call_count == 2

    def test_null_totals_become_none(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            side_effect=[[{"total_bytes": None}], [{"free_bytes": None}]]
        )

        info = introspector.get_storage_info()

        assert info.total_size_bytes is None
        assert info.free_space_bytes is None

    def test_query_errors_become_none(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("ORA-00942"))

        info = introspector.get_storage_info()

        assert info.total_size_bytes is None
        assert info.free_space_bytes is None


class TestListDatabases:
    """Tests for ``list_databases``."""

    def test_maps_database_names(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[{"name": "ORCL"}, {"name": "PDB1"}]
        )

        dbs = introspector.list_databases()

        assert [db.name for db in dbs] == ["ORCL", "PDB1"]
        assert all(isinstance(db, DatabaseBriefInfo) for db in dbs)
        assert "V$DATABASE" in introspector._execute_query_dict.call_args.args[0]

    def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("down"))

        assert introspector.list_databases() == []


class TestListUsers:
    """Tests for ``list_users``."""

    def test_maps_users_and_dba_flag(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[
                {"username": "SYS", "account_status": "OPEN", "is_dba": "Y"},
                {"username": "SCOTT", "account_status": "OPEN", "is_dba": "N"},
            ]
        )

        users = introspector.list_users()

        assert [user.name for user in users] == ["SYS", "SCOTT"]
        assert all(isinstance(user, UserInfo) for user in users)
        assert users[0].is_superuser is True
        assert users[0].extra == {"account_status": "OPEN"}
        assert users[1].is_superuser is False

    def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("down"))

        assert introspector.list_users() == []


class TestGetSessionInfo:
    """Tests for ``get_session_info``."""

    def test_maps_sys_context_row(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[
                {
                    "session_user": "SCOTT",
                    "db_name": "ORCL",
                    "current_schema": "SCOTT",
                    "host": "workstation",
                }
            ]
        )

        session = introspector.get_session_info()

        assert isinstance(session, SessionInfo)
        assert session.user == "SCOTT"
        assert session.database == "ORCL"
        assert session.schema == "SCOTT"
        assert session.host == "workstation"

    def test_empty_result_returns_default_session(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(return_value=[])

        session = introspector.get_session_info()

        assert session.user is None
        assert session.database is None
        assert session.schema is None
        assert session.host is None

    def test_query_error_returns_default_session(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("down"))

        session = introspector.get_session_info()

        assert session.user is None
        assert session.extra == {}


class TestListTablespaces:
    """Tests for ``list_tablespaces``."""

    def test_computes_used_bytes_and_percentage(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
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

        result = introspector.list_tablespaces()

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

    def test_zero_total_gives_zero_percentage(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[{"tablespace_name": "EMPTY", "total_bytes": 0, "free_bytes": 0}]
        )

        ts = introspector.list_tablespaces()[0]

        assert ts.used_bytes == 0
        assert ts.pct_used == 0.0

    def test_missing_and_null_sizes_default_to_zero(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[{"tablespace_name": "NULLS", "total_bytes": None, "free_bytes": None}]
        )

        ts = introspector.list_tablespaces()[0]

        assert ts.size_bytes == 0
        assert ts.free_bytes == 0
        assert ts.status is None
        assert ts.contents is None

    def test_percentage_is_rounded_to_one_decimal(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(
            return_value=[{"tablespace_name": "R", "total_bytes": 3, "free_bytes": 1}]
        )

        ts = introspector.list_tablespaces()[0]

        assert ts.pct_used == 66.7

    def test_returns_empty_list_on_query_error(self):
        introspector, _ = make_introspector()
        introspector._execute_query_dict = MagicMock(side_effect=RuntimeError("down"))

        assert introspector.list_tablespaces() == []


class TestGetOverview:
    """Tests for the orchestration in ``get_overview``."""

    def test_assembles_all_sections(self):
        introspector, _ = make_introspector()
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
            setattr(introspector, name, MagicMock(return_value=value))

        overview = introspector.get_overview()

        assert isinstance(overview, object)
        assert overview.server_version == "Oracle Database 19c"
        assert overview.server_vendor == "Oracle"
        assert overview.configuration == config + perf
        assert overview.session is session
        assert overview.connections is conn
        assert overview.storage is storage
        assert overview.databases is databases
        assert overview.users is users
        for name in providers:
            getattr(introspector, name).assert_called_once_with()
