# src/rhosocial/activerecord/backend/impl/oracle/show/functionality.py
"""
Oracle SHOW-style functionality implementation.

Provides convenience query functions that:

1. Construct an OracleQuery*Expression with the backend's dialect.
2. Execute the SQL through the backend's `execute()` method.
3. Map each result row into the corresponding typed dataclass.

The class is intentionally thin; SQL generation lives in the dialect
mixin and result shapes live in `.types`.
"""

from typing import Optional, TYPE_CHECKING

from .expressions import (
    OracleQuerySessionsExpression,
    OracleQueryRunningSQLExpression,
    OracleQueryDatabaseInfoExpression,
    OracleQueryInstanceInfoExpression,
    OracleQueryObjectsExpression,
    OracleQueryLocksExpression,
    OracleQueryWaitEventsExpression,
    OracleQueryNlsParametersExpression,
)
from .types import (
    OracleSessionInfo,
    OracleSqlInfo,
    OracleDatabaseInfo,
    OracleInstanceInfo,
    OracleObjectInfo,
    OracleLockInfo,
    OracleWaitEventInfo,
    OracleNlsParameter,
)

if TYPE_CHECKING:
    from ..backend import OracleBackend
    from ..async_backend import AsyncOracleBackend


def query_sessions(backend: "OracleBackend", active_only: bool = False):
    expr = OracleQuerySessionsExpression(backend.dialect, active_only=active_only)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    return [
        OracleSessionInfo(
            sid=row["sid"],
            serial_num=row["serial_num"],
            username=row.get("username"),
            status=row.get("status"),
            machine=row.get("machine"),
            program=row.get("program"),
            module=row.get("module"),
            action=row.get("action"),
            logon_time=row.get("logon_time"),
            process=row.get("process"),
        )
        for row in result.data
    ]


def query_running_sql(backend: "OracleBackend", limit: int = 50):
    expr = OracleQueryRunningSQLExpression(backend.dialect, limit=limit)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    return [
        OracleSqlInfo(
            sql_id=row.get("sql_id"),
            child_number=row["child_number"],
            sql_text=row.get("sql_text"),
            executions=row["executions"],
            elapsed_seconds=row["elapsed_seconds"],
            cpu_seconds=row["cpu_seconds"],
            buffer_gets=row["buffer_gets"],
            disk_reads=row["disk_reads"],
            rows_processed=row["rows_processed"],
            parsing_schema_name=row.get("parsing_schema_name"),
            last_active_time=row.get("last_active_time"),
        )
        for row in result.data
    ]


def query_database_info(backend: "OracleBackend"):
    expr = OracleQueryDatabaseInfoExpression(backend.dialect)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    if not result.data:
        return None
    row = result.data[0]
    return OracleDatabaseInfo(
        name=row["name"],
        dbid=row["dbid"],
        created=row.get("created"),
        log_mode=row.get("log_mode"),
        open_mode=row.get("open_mode"),
        platform_name=row.get("platform_name"),
        role=row.get("role"),
    )


def query_instance_info(backend: "OracleBackend"):
    expr = OracleQueryInstanceInfoExpression(backend.dialect)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    if not result.data:
        return None
    row = result.data[0]
    return OracleInstanceInfo(
        instance_name=row["instance_name"],
        instance_num=row["instance_num"],
        host_name=row.get("host_name"),
        version=row.get("version"),
        status=row.get("status"),
        startup_time=row.get("startup_time"),
        archiver=row.get("archiver"),
    )


def query_objects(
    backend: "OracleBackend",
    object_type: Optional[str] = None,
    owner: Optional[str] = None,
    include_invalid: bool = False,
    name_pattern: Optional[str] = None,
):
    expr = OracleQueryObjectsExpression(backend.dialect, object_type=object_type)
    if owner is not None:
        expr.owner(owner)
    if name_pattern is not None:
        expr.like(name_pattern)
    if include_invalid:
        expr.include_invalid(True)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    return [
        OracleObjectInfo(
            object_name=row["object_name"],
            object_type=row["object_type"],
            owner=row.get("owner"),
            object_id=row.get("object_id"),
            created=row.get("created"),
            last_ddl_time=row.get("last_ddl_time"),
            status=row.get("status"),
            temporary=row.get("temporary"),
        )
        for row in result.data
    ]


def query_locks(backend: "OracleBackend"):
    expr = OracleQueryLocksExpression(backend.dialect)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    return [
        OracleLockInfo(
            sid=row["sid"],
            lock_type=row.get("lock_type"),
            id1=row.get("id1"),
            id2=row.get("id2"),
            lmode=row.get("lmode"),
            request=row.get("request"),
            block=row.get("block"),
        )
        for row in result.data
    ]


def query_wait_events(backend: "OracleBackend"):
    expr = OracleQueryWaitEventsExpression(backend.dialect)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    return [
        OracleWaitEventInfo(
            sid=row["sid"],
            seq_num=row["seq#"],
            event=row.get("event"),
            p1text=row.get("p1text"),
            p1=row.get("p1"),
            wait_class=row.get("wait_class"),
            wait_time=row.get("wait_time"),
            seconds_in_wait=row.get("seconds_in_wait"),
            state=row.get("state"),
        )
        for row in result.data
    ]


def query_nls_parameters(backend: "OracleBackend", scope: str = "SESSION"):
    expr = OracleQueryNlsParametersExpression(backend.dialect, scope=scope)
    sql, params = expr.to_sql()
    result = backend.execute(sql, params)
    return [
        OracleNlsParameter(
            parameter=row["parameter"],
            value=row.get("value"),
        )
        for row in result.data
    ]


# Convenience lambdas retained as a public table so callers can
# invoke via OracleShowFunctionality.<name>(backends).
QUERY_LAMBDAS = {
    "sessions": query_sessions,
    "running_sql": query_running_sql,
    "database_info": query_database_info,
    "instance_info": query_instance_info,
    "objects": query_objects,
    "locks": query_locks,
    "wait_events": query_wait_events,
    "nls_parameters": query_nls_parameters,
}


class OracleShowFunctionality:
    """Stateful wrapper exposing the introspection lambdas as methods.

    Holds a backend reference so callers receive a one-line API:

        backend.show().sessions(active_only=True)
    """

    def __init__(self, backend: "OracleBackend", version: Optional[tuple] = None):
        self._backend = backend
        self._version = version
        self.dialect = backend.dialect

    def sessions(self, active_only: bool = False):
        return query_sessions(self._backend, active_only=active_only)

    def running_sql(self, limit: int = 50):
        return query_running_sql(self._backend, limit=limit)

    def database_info(self):
        return query_database_info(self._backend)

    def instance_info(self):
        return query_instance_info(self._backend)

    def objects(self, **kwargs):
        return query_objects(self._backend, **kwargs)

    def locks(self):
        return query_locks(self._backend)

    def wait_events(self):
        return query_wait_events(self._backend)

    def nls_parameters(self, scope: str = "SESSION"):
        return query_nls_parameters(self._backend, scope=scope)


class AsyncOracleShowFunctionality:
    """Async subclass; awaits `backend.execute` coroutine.

    Mirrors the sync API but uses `await self._backend.execute(...)`.
    Provided as a stub: async backends are expected to inject this.
    """

    def __init__(self, backend: "AsyncOracleBackend", version: Optional[tuple] = None):
        self._backend = backend
        self._version = version
        self.dialect = backend.dialect

    async def sessions(self, active_only: bool = False):
        expr = OracleQuerySessionsExpression(self.dialect, active_only=active_only)
        sql, params = expr.to_sql()
        result = await self._backend.execute(sql, params)
        return [
            OracleSessionInfo(
                sid=row["sid"],
                serial_num=row["serial_num"],
                username=row.get("username"),
                status=row.get("status"),
                machine=row.get("machine"),
                program=row.get("program"),
            )
            for row in result.data
        ]


__all__ = [
    "OracleShowFunctionality",
    "AsyncOracleShowFunctionality",
    "QUERY_LAMBDAS",
]
