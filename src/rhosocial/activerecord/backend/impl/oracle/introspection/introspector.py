# src/rhosocial/activerecord/backend/impl/oracle/introspection/introspector.py
"""
Oracle concrete introspectors.

Implements SyncAbstractIntrospector and AsyncAbstractIntrospector for Oracle
databases using Oracle data dictionary views for metadata queries.

The introspectors are exposed via ``backend.introspector``.

Architecture:
  - SQL generation: Direct SQL queries to Oracle data dictionary views
  - Query execution: Handled by IntrospectorExecutor
  - Result parsing: _parse_* methods in this module (pure Python, no I/O)

Key behaviours:
  - Queries USER_TABLES, USER_TAB_COLUMNS, USER_INDEXES,
    USER_CONSTRAINTS, USER_VIEWS, USER_TRIGGERS
  - _parse_* methods are pure Python — shared by sync and async introspectors

Design principle: Sync and Async are separate and cannot coexist.
- SyncOracleIntrospector: for synchronous backends
- AsyncOracleIntrospector: for asynchronous backends
"""

import copy
from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.introspection.base import (
    IntrospectorMixin,
    SyncAbstractIntrospector,
    AsyncAbstractIntrospector,
)
from rhosocial.activerecord.backend.introspection.executor import (
    SyncIntrospectorExecutor,
    AsyncIntrospectorExecutor,
)
from rhosocial.activerecord.backend.introspection.types import (
    DatabaseInfo,
    TableInfo,
    TableType,
    ColumnInfo,
    ColumnNullable,
    IndexInfo,
    IndexColumnInfo,
    IndexType,
    ForeignKeyInfo,
    ReferentialAction,
    ViewInfo,
    TriggerInfo,
    IntrospectionScope,
)


class OracleIntrospectorMixin(IntrospectorMixin):
    """Mixin providing shared Oracle-specific introspection logic.

    Both SyncOracleIntrospector and AsyncOracleIntrospector inherit
    from this mixin to share:
    - Default schema handling
    - Oracle version detection
    - _parse_* implementations
    """

    def _get_default_schema(self) -> str:
        """Return the Oracle schema (owner) from the backend config."""
        if hasattr(self._backend, 'config') and hasattr(self._backend.config, 'username'):
            return self._backend.config.username.upper() if self._backend.config.username else ""
        return ""

    def _get_version(self) -> tuple:
        """Return the Oracle server version tuple from the backend."""
        return getattr(self._backend, '_version', (19, 0, 0))

    # ------------------------------------------------------------------ #
    # SQL builders — Oracle data dictionary queries
    # ------------------------------------------------------------------ #

    def _build_database_info_sql(self) -> str:
        """Build SQL to get database information."""
        return """
            SELECT
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_CHARACTERSET') AS charset,
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_NCHAR_CHARACTERSET') AS nchar_charset,
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_LANGUAGE') AS language,
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_TERRITORY') AS territory
            FROM dual
        """

    def _build_tables_sql(self, schema: Optional[str] = None) -> str:
        """Build SQL to list tables."""
        owner_clause = f"AND owner = '{schema.upper()}'" if schema else ""
        return f"""
            SELECT
                table_name,
                'BASE TABLE' AS table_type,
                comments,
                num_rows,
                blocks * {self._get_block_size()} AS data_length,
                last_analyzed
            FROM all_tables t
            LEFT JOIN all_tab_comments c ON t.table_name = c.table_name AND t.owner = c.owner
            WHERE 1=1 {owner_clause}
            ORDER BY table_name
        """

    def _get_block_size(self) -> int:
        """Get default Oracle block size (8KB is typical)."""
        return 8192

    def _build_columns_sql(self, table_name: str, schema: str) -> str:
        """Build SQL to get columns for a table."""
        return f"""
            SELECT
                column_name,
                data_type,
                data_type_mod,
                data_type_owner,
                data_length,
                data_precision,
                data_scale,
                nullable,
                column_id,
                default_length,
                data_default,
                num_distinct,
                low_value,
                high_value,
                density,
                num_nulls,
                num_buckets,
                last_analyzed,
                sample_size,
                character_set_name,
                char_col_decl_length,
                global_stats,
                user_stats,
                avg_col_len,
                char_length,
                char_used,
                v80_fmt_image,
                data_upgraded,
                hidden_column,
                virtual_column,
                segment_column_id,
                internal_column_id,
                histogram,
                qualified_col_name,
                user_generated,
                default_on_null,
                identity_column,
                evaluation_edition,
                unusable_before,
                unusable_beginning,
                collation,
                collated_column_id
            FROM all_tab_columns
            WHERE table_name = '{table_name.upper()}'
              AND owner = '{schema.upper()}'
            ORDER BY column_id
        """

    def _build_primary_key_sql(self, table_name: str, schema: str) -> str:
        """Build SQL to get primary key columns."""
        return f"""
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
            WHERE cons.table_name = '{table_name.upper()}'
              AND cons.owner = '{schema.upper()}'
              AND cons.constraint_type = 'P'
            ORDER BY cols.position
        """

    def _build_indexes_sql(self, table_name: str, schema: str) -> str:
        """Build SQL to get indexes for a table."""
        return f"""
            SELECT
                i.index_name,
                i.index_type,
                i.uniqueness,
                i.compression,
                i.prefix_length,
                i.table_owner,
                i.table_name,
                i.table_type,
                i.uniqueness,
                i.status,
                i.num_rows,
                i.last_analyzed,
                ic.column_name,
                ic.column_position,
                ic.descend
            FROM all_indexes i
            JOIN all_ind_columns ic ON i.index_name = ic.index_name
                AND i.owner = ic.index_owner
            WHERE i.table_name = '{table_name.upper()}'
              AND i.table_owner = '{schema.upper()}'
            ORDER BY i.index_name, ic.column_position
        """

    def _build_foreign_keys_sql(self, table_name: str, schema: str) -> str:
        """Build SQL to get foreign keys for a table."""
        return f"""
            SELECT
                cons.constraint_name,
                cons.delete_rule,
                ref_cons.table_name AS referenced_table_name,
                cols.column_name,
                ref_cols.column_name AS referenced_column_name,
                cons.status
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
            JOIN all_constraints ref_cons ON cons.r_constraint_name = ref_cons.constraint_name
                AND cons.r_owner = ref_cons.owner
            JOIN all_cons_columns ref_cols ON ref_cons.constraint_name = ref_cols.constraint_name
                AND ref_cons.owner = ref_cols.owner
                AND cols.position = ref_cols.position
            WHERE cons.table_name = '{table_name.upper()}'
              AND cons.owner = '{schema.upper()}'
              AND cons.constraint_type = 'R'
            ORDER BY cons.constraint_name, cols.position
        """

    def _build_views_sql(self, schema: Optional[str] = None) -> str:
        """Build SQL to list views."""
        owner_clause = f"AND owner = '{schema.upper()}'" if schema else ""
        return f"""
            SELECT
                view_name,
                text_length,
                text,
                text_vc,
                type_text_length,
                type_text,
                oid_text_length,
                oid_text,
                view_type_owner,
                view_type,
                superview_owner,
                superview_name,
                subview_name,
                editioning_view,
                read_only,
                container_map,
                bequeath,
                origin_con_id,
                default_collation
            FROM all_views
            WHERE 1=1 {owner_clause}
            ORDER BY view_name
        """

    def _build_view_info_sql(self, view_name: str, schema: str) -> str:
        """Build SQL to get view info."""
        return f"""
            SELECT
                view_name,
                text_vc AS view_definition,
                read_only,
                editioning_view
            FROM all_views
            WHERE view_name = '{view_name.upper()}'
              AND owner = '{schema.upper()}'
        """

    def _build_triggers_sql(self, schema: Optional[str] = None) -> str:
        """Build SQL to list triggers."""
        owner_clause = f"AND owner = '{schema.upper()}'" if schema else ""
        return f"""
            SELECT
                trigger_name,
                trigger_type,
                triggering_event,
                table_owner,
                base_object_type,
                table_name,
                column_name,
                referencing_names,
                when_clause,
                status,
                description,
                action_type,
                trigger_body
            FROM all_triggers
            WHERE 1=1 {owner_clause}
            ORDER BY trigger_name
        """

    # ------------------------------------------------------------------ #
    # Parse methods — pure Python, no I/O
    # ------------------------------------------------------------------ #

    def _parse_database_info(self, rows: List[Dict[str, Any]]) -> DatabaseInfo:
        version = self._get_version()
        version_str = ".".join(str(v) for v in version)
        schema = self._get_default_schema()

        db_row = rows[0] if rows else {}

        return DatabaseInfo(
            name=schema,
            version=version_str,
            version_tuple=version,
            vendor="Oracle",
            encoding=db_row.get("CHARSET"),
            collation=db_row.get("LANGUAGE"),
        )

    def _parse_tables(
        self, rows: List[Dict[str, Any]], schema: Optional[str]
    ) -> List[TableInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        tables = []
        for row in rows:
            tables.append(
                TableInfo(
                    name=row["TABLE_NAME"],
                    schema=target_schema,
                    table_type=TableType.BASE_TABLE,
                    comment=row.get("COMMENTS"),
                    row_count=row.get("NUM_ROWS"),
                    size_bytes=row.get("DATA_LENGTH"),
                    create_time=None,  # Oracle doesn't store creation time
                    update_time=str(row["LAST_ANALYZED"]) if row.get("LAST_ANALYZED") else None,
                )
            )
        return tables

    def _parse_columns(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
        primary_keys: List[str] = None,
    ) -> List[ColumnInfo]:
        pk_set = set(primary_keys or [])
        columns = []
        for row in rows:
            nullable = (
                ColumnNullable.NULLABLE
                if row.get("NULLABLE") == "Y"
                else ColumnNullable.NOT_NULL
            )
            data_type = row.get("DATA_TYPE", "VARCHAR2")
            # Build full data type string
            if data_type in ("NUMBER", "FLOAT"):
                precision = row.get("DATA_PRECISION")
                scale = row.get("DATA_SCALE")
                if precision is not None:
                    if scale is not None and scale > 0:
                        data_type_full = f"{data_type}({precision},{scale})"
                    else:
                        data_type_full = f"{data_type}({precision})"
                else:
                    data_type_full = data_type
            elif data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
                char_length = row.get("CHAR_LENGTH") or row.get("DATA_LENGTH")
                data_type_full = f"{data_type}({char_length})"
            else:
                data_type_full = data_type

            col_name = row["COLUMN_NAME"]
            columns.append(
                ColumnInfo(
                    name=col_name,
                    table_name=table_name,
                    schema=schema,
                    ordinal_position=row["COLUMN_ID"],
                    data_type=data_type.lower(),
                    data_type_full=data_type_full,
                    nullable=nullable,
                    default_value=row.get("DATA_DEFAULT"),
                    is_primary_key=col_name in pk_set,
                    is_unique=False,  # Determined separately
                    is_auto_increment=row.get("IDENTITY_COLUMN") == "YES",
                    comment=None,  # Oracle column comments require separate query
                    character_maximum_length=row.get("CHAR_LENGTH") or row.get("DATA_LENGTH"),
                    numeric_precision=row.get("DATA_PRECISION"),
                    numeric_scale=row.get("DATA_SCALE"),
                    charset=row.get("CHARACTER_SET_NAME"),
                )
            )
        return columns

    def _parse_indexes(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
    ) -> List[IndexInfo]:
        index_type_map = {
            "NORMAL": IndexType.BTREE,
            "BITMAP": IndexType.BITMAP,
            "FUNCTION-BASED NORMAL": IndexType.FUNCTION,
            "DOMAIN": IndexType.DOMAIN,
            "IOT - TOP": IndexType.IOT,
            "LOB": IndexType.LOB,
        }
        index_map: Dict[str, IndexInfo] = {}
        for row in rows:
            idx_name = row.get("INDEX_NAME", "")
            if idx_name not in index_map:
                idx_type_str = (row.get("INDEX_TYPE") or "NORMAL").upper()
                uniqueness = row.get("UNIQUENESS", "NONUNIQUE")
                index_map[idx_name] = IndexInfo(
                    name=idx_name,
                    table_name=table_name,
                    schema=schema,
                    is_unique=uniqueness == "UNIQUE",
                    is_primary=False,  # Primary keys have separate constraint
                    index_type=index_type_map.get(idx_type_str, IndexType.BTREE),
                    columns=[],
                )
            descend = row.get("DESCEND", "ASC")
            index_map[idx_name].columns.append(
                IndexColumnInfo(
                    name=row.get("COLUMN_NAME", ""),
                    ordinal_position=int(row.get("COLUMN_POSITION", 1)),
                    is_descending=descend == "DESC",
                )
            )
        return list(index_map.values())

    def _parse_foreign_keys(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
    ) -> List[ForeignKeyInfo]:
        action_map = {
            "NO ACTION": ReferentialAction.NO_ACTION,
            "CASCADE": ReferentialAction.CASCADE,
            "SET NULL": ReferentialAction.SET_NULL,
            "SET DEFAULT": ReferentialAction.SET_DEFAULT,
        }
        fk_map: Dict[str, ForeignKeyInfo] = {}
        for row in rows:
            fk_name = row.get("CONSTRAINT_NAME", "")
            if fk_name not in fk_map:
                delete_rule = (row.get("DELETE_RULE") or "NO ACTION").upper()
                fk_map[fk_name] = ForeignKeyInfo(
                    name=fk_name,
                    table_name=table_name,
                    schema=schema,
                    referenced_table=row.get("REFERENCED_TABLE_NAME", ""),
                    on_update=ReferentialAction.NO_ACTION,  # Oracle doesn't support ON UPDATE
                    on_delete=action_map.get(delete_rule, ReferentialAction.NO_ACTION),
                    columns=[],
                    referenced_columns=[],
                )
            fk_map[fk_name].columns.append(row.get("COLUMN_NAME", ""))
            fk_map[fk_name].referenced_columns.append(row.get("REFERENCED_COLUMN_NAME", ""))
        return list(fk_map.values())

    def _parse_views(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[ViewInfo]:
        return [
            ViewInfo(
                name=row.get("VIEW_NAME", ""),
                schema=schema,
                definition=row.get("TEXT_VC") or row.get("TEXT"),
                check_option=None,  # Oracle doesn't have CHECK OPTION info in all_views
                is_updatable=row.get("READ_ONLY") != "Y",
            )
            for row in rows
        ]

    def _parse_view_info(
        self,
        rows: List[Dict[str, Any]],
        view_name: str,
        schema: str,
    ) -> Optional[ViewInfo]:
        if not rows:
            return None
        row = rows[0]
        return ViewInfo(
            name=row.get("VIEW_NAME", view_name),
            schema=schema,
            definition=row.get("VIEW_DEFINITION"),
            check_option=None,
            is_updatable=row.get("READ_ONLY") != "Y",
        )

    def _parse_triggers(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[TriggerInfo]:
        triggers = []
        for row in rows:
            trigger_type = row.get("TRIGGER_TYPE", "")
            triggering_event = row.get("TRIGGERING_EVENT", "")
            # Parse events (comma-separated)
            events = [e.strip() for e in triggering_event.split(" OR ")] if triggering_event else []

            triggers.append(
                TriggerInfo(
                    name=row.get("TRIGGER_NAME", ""),
                    table_name=row.get("TABLE_NAME", ""),
                    schema=schema,
                    timing=trigger_type,  # BEFORE, AFTER, INSTEAD OF
                    events=events,
                    definition=row.get("TRIGGER_BODY"),
                )
            )
        return triggers


class SyncOracleIntrospector(OracleIntrospectorMixin, SyncAbstractIntrospector):
    """Synchronous introspector for Oracle backends.

    Provides introspection capabilities for Oracle databases using
    the data dictionary views (ALL_TABLES, ALL_TAB_COLUMNS, etc.).
    """

    def __init__(self, backend: Any, executor: SyncIntrospectorExecutor) -> None:
        super().__init__(backend, executor)

    # ------------------------------------------------------------------ #
    # get_table_info override
    # ------------------------------------------------------------------ #

    def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> Optional[TableInfo]:
        key = self._make_cache_key(
            IntrospectionScope.TABLE, table_name, schema=schema
        )
        cached = self._get_cached(key)
        if cached is not None:
            return cached

        tables = self.list_tables(schema)
        table = next((t for t in tables if t.name == table_name.upper()), None)
        if table is None:
            return None

        table = copy.copy(table)
        table.columns = self.list_columns(table_name, schema)
        table.indexes = self.list_indexes(table_name, schema)
        table.foreign_keys = self.list_foreign_keys(table_name, schema)
        self._set_cached(key, table)
        return table

    # ------------------------------------------------------------------ #
    # list_columns override to handle primary keys
    # ------------------------------------------------------------------ #

    def list_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[ColumnInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        key = self._make_cache_key(
            IntrospectionScope.COLUMN, table_name, schema=target_schema
        )
        cached = self._get_cached(key)
        if cached is not None:
            return cached

        # Get primary key columns first
        pk_sql = self._build_primary_key_sql(table_name, target_schema)
        pk_result = self._executor.execute(pk_sql)
        primary_keys = [row.get("COLUMN_NAME") for row in pk_result.rows]

        # Get columns
        sql = self._build_columns_sql(table_name, target_schema)
        result = self._executor.execute(sql)
        columns = self._parse_columns(result.rows, table_name, target_schema, primary_keys)

        self._set_cached(key, columns)
        return columns


class AsyncOracleIntrospector(OracleIntrospectorMixin, AsyncAbstractIntrospector):
    """Asynchronous introspector for Oracle backends.

    Provides introspection capabilities for Oracle databases using
    the data dictionary views (ALL_TABLES, ALL_TAB_COLUMNS, etc.).
    """

    def __init__(self, backend: Any, executor: AsyncIntrospectorExecutor) -> None:
        super().__init__(backend, executor)

    # ------------------------------------------------------------------ #
    # get_table_info override
    # ------------------------------------------------------------------ #

    async def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> Optional[TableInfo]:
        key = self._make_cache_key(
            IntrospectionScope.TABLE, table_name, schema=schema
        )
        cached = self._get_cached(key)
        if cached is not None:
            return cached

        tables = await self.list_tables(schema)
        table = next((t for t in tables if t.name == table_name.upper()), None)
        if table is None:
            return None

        table = copy.copy(table)
        table.columns = await self.list_columns(table_name, schema)
        table.indexes = await self.list_indexes(table_name, schema)
        table.foreign_keys = await self.list_foreign_keys(table_name, schema)
        self._set_cached(key, table)
        return table

    # ------------------------------------------------------------------ #
    # list_columns override to handle primary keys
    # ------------------------------------------------------------------ #

    async def list_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[ColumnInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        key = self._make_cache_key(
            IntrospectionScope.COLUMN, table_name, schema=target_schema
        )
        cached = self._get_cached(key)
        if cached is not None:
            return cached

        # Get primary key columns first
        pk_sql = self._build_primary_key_sql(table_name, target_schema)
        pk_result = await self._executor.execute(pk_sql)
        primary_keys = [row.get("COLUMN_NAME") for row in pk_result.rows]

        # Get columns
        sql = self._build_columns_sql(table_name, target_schema)
        result = await self._executor.execute(sql)
        columns = self._parse_columns(result.rows, table_name, target_schema, primary_keys)

        self._set_cached(key, columns)
        return columns
