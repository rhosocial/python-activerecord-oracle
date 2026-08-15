# src/rhosocial/activerecord/backend/impl/oracle/introspection/status_introspector.py
"""Oracle server status introspector.

Provides server status information by querying Oracle data dictionary views:
V$INSTANCE, V$PARAMETER, V$SESSION, V$SYSSTAT, DBA_TABLESPACES, DBA_USERS.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.introspection.status import (
    StatusItem,
    StatusCategory,
    ServerOverview,
    DatabaseBriefInfo,
    UserInfo,
    ConnectionInfo,
    StorageInfo,
    SessionInfo,
    SyncAbstractStatusIntrospector,
    AsyncAbstractStatusIntrospector,
)


@dataclass
class TablespaceInfo:
    """Oracle tablespace information."""

    name: str
    status: Optional[str] = None
    contents: Optional[str] = None
    size_bytes: Optional[int] = None
    free_bytes: Optional[int] = None
    used_bytes: Optional[int] = None
    max_size_bytes: Optional[int] = None
    pct_used: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)


ORACLE_CONFIG_PARAMS = [
    ("processes", StatusCategory.CONNECTION, "Maximum number of OS processes", None),
    ("sessions", StatusCategory.CONNECTION, "Maximum number of sessions", None),
    ("sga_target", StatusCategory.PERFORMANCE, "SGA target size", "bytes"),
    ("pga_aggregate_target", StatusCategory.PERFORMANCE, "PGA aggregate target", "bytes"),
    ("db_block_size", StatusCategory.STORAGE, "Database block size", "bytes"),
    ("open_cursors", StatusCategory.PERFORMANCE, "Maximum open cursors per session", None),
    ("cursor_sharing", StatusCategory.PERFORMANCE, "Cursor sharing mode", None),
    ("optimizer_mode", StatusCategory.PERFORMANCE, "Optimizer mode", None),
    ("undo_tablespace", StatusCategory.STORAGE, "Undo tablespace name", None),
]


class OracleStatusIntrospectorMixin:
    """Shared non-I/O logic for Oracle status introspectors."""

    def _get_vendor_name(self) -> str:
        return "Oracle"

    def _create_status_item(
        self, name: str, value: Any, category: StatusCategory,
        description: Optional[str] = None, unit: Optional[str] = None,
        is_readonly: bool = True, is_dynamic: bool = False,
    ) -> StatusItem:
        return StatusItem(
            name=name, value=value, category=category,
            description=description, unit=unit,
            is_readonly=is_readonly, is_dynamic=is_dynamic,
        )

    def _build_server_overview(
        self, version: str, session: SessionInfo,
        configuration: List[StatusItem], performance: List[StatusItem],
        connections: ConnectionInfo, storage: StorageInfo,
        databases: List[DatabaseBriefInfo], users: List[UserInfo],
    ) -> ServerOverview:
        return ServerOverview(
            server_version=version,
            server_vendor=self._get_vendor_name(),
            session=session,
            configuration=configuration + performance,
            connections=connections,
            storage=storage,
            databases=databases,
            users=users,
        )


class SyncOracleStatusIntrospector(OracleStatusIntrospectorMixin, SyncAbstractStatusIntrospector):
    """Synchronous Oracle status introspector."""

    def _execute_query(self, sql: str) -> List[tuple]:
        cursor = self._backend._connection.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()

    def _execute_query_dict(self, sql: str) -> List[Dict[str, Any]]:
        cursor = self._backend._connection.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0].lower() for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def get_overview(self) -> ServerOverview:
        version = self._get_version_string()
        session = self.get_session_info()
        config = self.list_configuration()
        perf = self.list_performance_metrics()
        conn = self.get_connection_info()
        storage = self.get_storage_info()
        databases = self.list_databases()
        users = self.list_users()
        return self._build_server_overview(
            version, session, config, perf, conn, storage, databases, users,
        )

    def _get_version_string(self) -> str:
        try:
            rows = self._execute_query("SELECT banner FROM V$VERSION WHERE ROWNUM = 1")
            return rows[0][0] if rows else "Unknown"
        except Exception:
            return "Unknown"

    def list_configuration(self, category: Optional[StatusCategory] = None) -> List[StatusItem]:
        items: List[StatusItem] = []
        param_names = [p[0] for p in ORACLE_CONFIG_PARAMS]
        placeholders = ", ".join(f"'{n}'" for n in param_names)
        sql = f"SELECT name, value, isdefault FROM V$PARAMETER WHERE name IN ({placeholders})"
        try:
            rows = self._execute_query_dict(sql)
        except Exception:
            return items

        param_map = {p[0]: p for p in ORACLE_CONFIG_PARAMS}
        for row in rows:
            name = row["name"]
            if name in param_map:
                _, cat, desc, unit = param_map[name]
                if category and cat != category:
                    continue
                items.append(self._create_status_item(
                    name=name, value=row["value"], category=cat,
                    description=desc, unit=unit,
                    is_readonly=(row.get("isdefault", "TRUE") == "TRUE"),
                ))
        return items

    def list_performance_metrics(self, category: Optional[StatusCategory] = None) -> List[StatusItem]:
        items: List[StatusItem] = []
        sql = """
            SELECT name, value FROM V$SYSSTAT
            WHERE name IN (
                'physical reads', 'physical writes', 'redo writes',
                'user commits', 'user rollbacks', 'parse count (total)',
                'execute count', 'session logical reads', 'db block gets',
                'consistent gets'
            )
        """
        try:
            rows = self._execute_query_dict(sql)
        except Exception:
            return items

        for row in rows:
            items.append(self._create_status_item(
                name=row["name"], value=row["value"],
                category=StatusCategory.PERFORMANCE,
                is_readonly=True, is_dynamic=False,
            ))
        return items

    def get_connection_info(self) -> ConnectionInfo:
        try:
            rows = self._execute_query_dict(
                "SELECT COUNT(*) AS cnt FROM V$SESSION WHERE type = 'USER'"
            )
            active = rows[0]["cnt"] if rows else 0
        except Exception:
            active = 0

        try:
            rows = self._execute_query_dict(
                "SELECT value FROM V$PARAMETER WHERE name = 'sessions'"
            )
            max_conn = int(rows[0]["value"]) if rows else None
        except Exception:
            max_conn = None

        return ConnectionInfo(active_count=active, max_connections=max_conn)

    def get_storage_info(self) -> StorageInfo:
        try:
            rows = self._execute_query_dict("""
                SELECT SUM(bytes) AS total_bytes FROM DBA_DATA_FILES
            """)
            total = int(rows[0]["total_bytes"]) if rows and rows[0]["total_bytes"] else None
        except Exception:
            total = None

        try:
            rows = self._execute_query_dict("""
                SELECT SUM(bytes) AS free_bytes FROM DBA_FREE_SPACE
            """)
            free = int(rows[0]["free_bytes"]) if rows and rows[0]["free_bytes"] else None
        except Exception:
            free = None

        return StorageInfo(total_size_bytes=total, free_space_bytes=free)

    def list_databases(self) -> List[DatabaseBriefInfo]:
        try:
            rows = self._execute_query_dict(
                "SELECT name FROM V$DATABASE"
            )
            return [DatabaseBriefInfo(name=row["name"]) for row in rows]
        except Exception:
            return []

    def list_users(self) -> List[UserInfo]:
        try:
            rows = self._execute_query_dict("""
                SELECT username, account_status,
                       CASE WHEN username IN ('SYS', 'SYSTEM') THEN 'Y' ELSE 'N' END AS is_dba
                FROM DBA_USERS ORDER BY username
            """)
        except Exception:
            return []

        users: List[UserInfo] = []
        for row in rows:
            users.append(UserInfo(
                name=row["username"],
                is_superuser=(row.get("is_dba") == "Y"),
                extra={"account_status": row.get("account_status")},
            ))
        return users

    def get_session_info(self) -> SessionInfo:
        try:
            rows = self._execute_query_dict("""
                SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') AS session_user,
                       SYS_CONTEXT('USERENV', 'DB_NAME') AS db_name,
                       SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema,
                       SYS_CONTEXT('USERENV', 'HOST') AS host
                FROM DUAL
            """)
        except Exception:
            return SessionInfo()

        if not rows:
            return SessionInfo()
        row = rows[0]
        return SessionInfo(
            user=row.get("session_user"),
            database=row.get("db_name"),
            schema=row.get("current_schema"),
            host=row.get("host"),
        )

    def list_tablespaces(self) -> List[TablespaceInfo]:
        try:
            rows = self._execute_query_dict("""
                SELECT t.tablespace_name, t.status, t.contents,
                       NVL(d.total_bytes, 0) AS total_bytes,
                       NVL(f.free_bytes, 0) AS free_bytes
                FROM DBA_TABLESPACES t
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) AS total_bytes
                    FROM DBA_DATA_FILES GROUP BY tablespace_name
                ) d ON t.tablespace_name = d.tablespace_name
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) AS free_bytes
                    FROM DBA_FREE_SPACE GROUP BY tablespace_name
                ) f ON t.tablespace_name = f.tablespace_name
                ORDER BY t.tablespace_name
            """)
        except Exception:
            return []

        result: List[TablespaceInfo] = []
        for row in rows:
            total = int(row.get("total_bytes") or 0)
            free = int(row.get("free_bytes") or 0)
            used = total - free
            pct = (used / total * 100) if total > 0 else 0.0
            result.append(TablespaceInfo(
                name=row["tablespace_name"],
                status=row.get("status"),
                contents=row.get("contents"),
                size_bytes=total,
                free_bytes=free,
                used_bytes=used,
                pct_used=round(pct, 1),
            ))
        return result


class AsyncOracleStatusIntrospector(OracleStatusIntrospectorMixin, AsyncAbstractStatusIntrospector):
    """Asynchronous Oracle status introspector."""

    async def _execute_query(self, sql: str) -> List[tuple]:
        cursor = self._backend._connection.cursor()
        try:
            await cursor.execute(sql)
            return await cursor.fetchall()
        finally:
            await cursor.close()

    async def _execute_query_dict(self, sql: str) -> List[Dict[str, Any]]:
        cursor = self._backend._connection.cursor()
        try:
            await cursor.execute(sql)
            columns = [desc[0].lower() for desc in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            await cursor.close()

    async def get_overview(self) -> ServerOverview:
        version = await self._get_version_string()
        session = await self.get_session_info()
        config = await self.list_configuration()
        perf = await self.list_performance_metrics()
        conn = await self.get_connection_info()
        storage = await self.get_storage_info()
        databases = await self.list_databases()
        users = await self.list_users()
        return self._build_server_overview(
            version, session, config, perf, conn, storage, databases, users,
        )

    async def _get_version_string(self) -> str:
        try:
            rows = await self._execute_query("SELECT banner FROM V$VERSION WHERE ROWNUM = 1")
            return rows[0][0] if rows else "Unknown"
        except Exception:
            return "Unknown"

    async def list_configuration(self, category: Optional[StatusCategory] = None) -> List[StatusItem]:
        items: List[StatusItem] = []
        param_names = [p[0] for p in ORACLE_CONFIG_PARAMS]
        placeholders = ", ".join(f"'{n}'" for n in param_names)
        sql = f"SELECT name, value, isdefault FROM V$PARAMETER WHERE name IN ({placeholders})"
        try:
            rows = await self._execute_query_dict(sql)
        except Exception:
            return items

        param_map = {p[0]: p for p in ORACLE_CONFIG_PARAMS}
        for row in rows:
            name = row["name"]
            if name in param_map:
                _, cat, desc, unit = param_map[name]
                if category and cat != category:
                    continue
                items.append(self._create_status_item(
                    name=name, value=row["value"], category=cat,
                    description=desc, unit=unit,
                    is_readonly=(row.get("isdefault", "TRUE") == "TRUE"),
                ))
        return items

    async def list_performance_metrics(self, category: Optional[StatusCategory] = None) -> List[StatusItem]:
        items: List[StatusItem] = []
        sql = """
            SELECT name, value FROM V$SYSSTAT
            WHERE name IN (
                'physical reads', 'physical writes', 'redo writes',
                'user commits', 'user rollbacks', 'parse count (total)',
                'execute count', 'session logical reads', 'db block gets',
                'consistent gets'
            )
        """
        try:
            rows = await self._execute_query_dict(sql)
        except Exception:
            return items

        for row in rows:
            items.append(self._create_status_item(
                name=row["name"], value=row["value"],
                category=StatusCategory.PERFORMANCE,
                is_readonly=True, is_dynamic=False,
            ))
        return items

    async def get_connection_info(self) -> ConnectionInfo:
        try:
            rows = await self._execute_query_dict(
                "SELECT COUNT(*) AS cnt FROM V$SESSION WHERE type = 'USER'"
            )
            active = rows[0]["cnt"] if rows else 0
        except Exception:
            active = 0

        try:
            rows = await self._execute_query_dict(
                "SELECT value FROM V$PARAMETER WHERE name = 'sessions'"
            )
            max_conn = int(rows[0]["value"]) if rows else None
        except Exception:
            max_conn = None

        return ConnectionInfo(active_count=active, max_connections=max_conn)

    async def get_storage_info(self) -> StorageInfo:
        try:
            rows = await self._execute_query_dict(
                "SELECT SUM(bytes) AS total_bytes FROM DBA_DATA_FILES"
            )
            total = int(rows[0]["total_bytes"]) if rows and rows[0]["total_bytes"] else None
        except Exception:
            total = None

        try:
            rows = await self._execute_query_dict(
                "SELECT SUM(bytes) AS free_bytes FROM DBA_FREE_SPACE"
            )
            free = int(rows[0]["free_bytes"]) if rows and rows[0]["free_bytes"] else None
        except Exception:
            free = None

        return StorageInfo(total_size_bytes=total, free_space_bytes=free)

    async def list_databases(self) -> List[DatabaseBriefInfo]:
        try:
            rows = await self._execute_query_dict("SELECT name FROM V$DATABASE")
            return [DatabaseBriefInfo(name=row["name"]) for row in rows]
        except Exception:
            return []

    async def list_users(self) -> List[UserInfo]:
        try:
            rows = await self._execute_query_dict("""
                SELECT username, account_status,
                       CASE WHEN username IN ('SYS', 'SYSTEM') THEN 'Y' ELSE 'N' END AS is_dba
                FROM DBA_USERS ORDER BY username
            """)
        except Exception:
            return []

        users: List[UserInfo] = []
        for row in rows:
            users.append(UserInfo(
                name=row["username"],
                is_superuser=(row.get("is_dba") == "Y"),
                extra={"account_status": row.get("account_status")},
            ))
        return users

    async def get_session_info(self) -> SessionInfo:
        try:
            rows = await self._execute_query_dict("""
                SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') AS session_user,
                       SYS_CONTEXT('USERENV', 'DB_NAME') AS db_name,
                       SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema,
                       SYS_CONTEXT('USERENV', 'HOST') AS host
                FROM DUAL
            """)
        except Exception:
            return SessionInfo()

        if not rows:
            return SessionInfo()
        row = rows[0]
        return SessionInfo(
            user=row.get("session_user"),
            database=row.get("db_name"),
            schema=row.get("current_schema"),
            host=row.get("host"),
        )

    async def list_tablespaces(self) -> List[TablespaceInfo]:
        try:
            rows = await self._execute_query_dict("""
                SELECT t.tablespace_name, t.status, t.contents,
                       NVL(d.total_bytes, 0) AS total_bytes,
                       NVL(f.free_bytes, 0) AS free_bytes
                FROM DBA_TABLESPACES t
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) AS total_bytes
                    FROM DBA_DATA_FILES GROUP BY tablespace_name
                ) d ON t.tablespace_name = d.tablespace_name
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) AS free_bytes
                    FROM DBA_FREE_SPACE GROUP BY tablespace_name
                ) f ON t.tablespace_name = f.tablespace_name
                ORDER BY t.tablespace_name
            """)
        except Exception:
            return []

        result: List[TablespaceInfo] = []
        for row in rows:
            total = int(row.get("total_bytes") or 0)
            free = int(row.get("free_bytes") or 0)
            used = total - free
            pct = (used / total * 100) if total > 0 else 0.0
            result.append(TablespaceInfo(
                name=row["tablespace_name"],
                status=row.get("status"),
                contents=row.get("contents"),
                size_bytes=total,
                free_bytes=free,
                used_bytes=used,
                pct_used=round(pct, 1),
            ))
        return result
