# src/rhosocial/activerecord/backend/impl/oracle/show/dialect.py
"""
Oracle SHOW-style data dictionary SQL composition mixin.

Mirrors MySQL's `MySQLShowDialectMixin` but emits SELECTs against
Oracle's data dictionary views instead of SHOW statements.

All `compose_query_*` methods take an OracleQueryExpression subclass
instance, read parameters via `expr.get_params()`, and return a
`(sql, params)` tuple where `params` is a tuple of bind values that
follow Oracle's colon-prefixed bind-variable convention.

The mixin is intended to be added to OracleDialect; it relies only on
`self.format_identifier()` provided by the host dialect.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
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


class OracleShowDialectMixin:
    """SQL composition mixin for Oracle data dictionary introspection.

    Methods accept an expression and return ``(sql, params)`` tuples.
    Bind parameters are returned positionally in the order they appear
    in the SQL string. The mixin relies on the host dialect's
    `format_identifier` for quoting.
    """

    # ============================================================
    # Sessions / Processes (V$SESSION, V$PROCESS)
    # ============================================================

    def compose_query_sessions_sql(
        self, expr: "OracleQuerySessionsExpression"
    ) -> Tuple[str, tuple]:
        params = expr.get_params()
        active_only = params.get("active_only", False)

        clauses = [
            "SELECT s.sid, s.serial# AS serial_num, s.username, s.status,",
            "       s.machine, s.program, s.module, s.action,",
            "       TO_CHAR(s.logon_time, 'YYYY-MM-DD HH24:MI:SS') AS logon_time,",
            "       s.process",
            "FROM v$session s",
        ]
        binds = []
        if active_only:
            clauses.append("WHERE s.status = :status")
            binds.append("ACTIVE")
        else:
            clauses.append("WHERE s.username IS NOT NULL")
        clauses.append("ORDER BY s.sid")
        return "\n".join(clauses), tuple(binds)

    def compose_query_process_sql(self) -> Tuple[str, tuple]:
        return (
            "SELECT p.spid, p.pid, p.program, p.background "
            "FROM v$process p ORDER BY p.pid",
            (),
        )

    # ============================================================
    # Running SQL (V$SQL)
    # ============================================================

    def compose_query_running_sql_sql(
        self, expr: "OracleQueryRunningSQLExpression"
    ) -> Tuple[str, tuple]:
        params = expr.get_params()
        limit = int(params.get("limit", 50))
        if limit < 1:
            limit = 1
        sql = (
            "SELECT sql_id, child_number, sql_text, executions, "
            "       elapsed_time / 1000000 AS elapsed_seconds, "
            "       cpu_time / 1000000 AS cpu_seconds, "
            "       buffer_gets, disk_reads, rows_processed, "
            "       parsing_schema_name, "
            "       TO_CHAR(last_active_time, 'YYYY-MM-DD HH24:MI:SS') AS last_active_time "
            "FROM v$sql "
            "WHERE executions > 0 "
            "ORDER BY elapsed_time DESC "
            "FETCH FIRST :row_count ROWS ONLY"
        )
        return sql, (limit,)

    # ============================================================
    # Database / Instance info (V$DATABASE, V$INSTANCE)
    # ============================================================

    def compose_query_database_info_sql(
        self, expr: "OracleQueryDatabaseInfoExpression"
    ) -> Tuple[str, tuple]:
        sql = (
            "SELECT name, dbid, "
            "       TO_CHAR(created, 'YYYY-MM-DD HH24:MI:SS') AS created, "
            "       log_mode, open_mode, platform_name, database_role AS role "
            "FROM v$database"
        )
        return sql, ()

    def compose_query_instance_info_sql(
        self, expr: "OracleQueryInstanceInfoExpression"
    ) -> Tuple[str, tuple]:
        sql = (
            "SELECT instance_name, instance_number AS instance_num, host_name, "
            "       version, status, "
            "       TO_CHAR(startup_time, 'YYYY-MM-DD HH24:MI:SS') AS startup_time, "
            "       archiver "
            "FROM v$instance"
        )
        return sql, ()

    # ============================================================
    # Objects (USER_/ALL_/DBA_OBJECTS)
    # ============================================================

    def compose_query_objects_sql(
        self, expr: "OracleQueryObjectsExpression"
    ) -> Tuple[str, tuple]:
        params = expr.get_params()
        include_invalid = params.get("include_invalid", False)
        object_type = params.get("object_type")
        name_pattern = params.get("name_pattern")
        binds = []

        view = self._select_objects_view(params.get("-owner"))
        where_parts: list = []
        if not include_invalid:
            where_parts.append("status = 'VALID'")
        if object_type is not None:
            where_parts.append("object_type = :obj_type")
            binds.append(object_type)
        if name_pattern is not None:
            where_parts.append("object_name LIKE :name_like")
            binds.append(name_pattern)

        sql = (
            f"SELECT owner, object_name, object_type, object_id, "
            f"TO_CHAR(created, 'YYYY-MM-DD HH24:MI:SS') AS created, "
            f"TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') AS last_ddl_time, "
            f"status, temporary "
            f"FROM {view}"
        )
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        sql += " ORDER BY object_type, object_name"
        return sql, tuple(binds)

    # ============================================================
    # Locks (V$LOCK)
    # ============================================================

    def compose_query_locks_sql(
        self, expr: "OracleQueryLocksExpression"
    ) -> Tuple[str, tuple]:
        sql = (
            "SELECT sid, type AS lock_type, id1, id2, lmode, request, block "
            "FROM v$lock ORDER BY sid"
        )
        return sql, ()

    # ============================================================
    # Wait events (V$SESSION_WAIT)
    # ============================================================

    def compose_query_wait_events_sql(
        self, expr: "OracleQueryWaitEventsExpression"
    ) -> Tuple[str, tuple]:
        sql = (
            "SELECT sid, seq# AS seq_num, event, p1text, p1, wait_class, "
            "       wait_time, seconds_in_wait, state "
            "FROM v$session_wait ORDER BY sid"
        )
        return sql, ()

    # ============================================================
    # NLS parameters
    # ============================================================

    def compose_query_nls_parameters_sql(
        self, expr: "OracleQueryNlsParametersExpression"
    ) -> Tuple[str, tuple]:
        params = expr.get_params()
        scope = str(params.get("scope", "SESSION")).upper()
        view = "nls_database_parameters" if scope == "DATABASE" else "nls_session_parameters"
        return f"SELECT parameter, value FROM {view} ORDER BY parameter", ()

    # ============================================================
    # Helpers
    # ============================================================

    def _select_objects_view(self, owner: str) -> str:
        """Choose USER_/ALL_/DBA_OBJECTS based on filter intent."""
        # DBA_ requires privileges; use ALL_ when an owner filter is set,
        # otherwise default to USER_OBJECTS for the connected schema.
        if owner is None:
            return "user_objects"
        return "all_objects"

    # type helper to acknowledge attribute is provided by the host dialect
    def format_identifier(self, identifier: str) -> str:  # pragma: no cover
        raise NotImplementedError("format_identifier must be supplied by host dialect")


__all__ = ["OracleShowDialectMixin"]
