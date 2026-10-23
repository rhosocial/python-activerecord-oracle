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
  - Queries ALL_SEQUENCES, ALL_SYNONYMS and ALL_OBJECTS for the
    backend-specific sequence/synonym/package families
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
from rhosocial.activerecord.backend.introspection.cache import IntrospectionCache
from rhosocial.activerecord.backend.introspection.executor import (
    SyncIntrospectorExecutor,
    AsyncIntrospectorExecutor,
)
from rhosocial.activerecord.backend.introspection.types import (
    IntrospectionDatabaseInfo,
    IntrospectionTableInfo,
    TableType,
    IntrospectionColumnInfo,
    ColumnNullable,
    IndexInfo,
    IndexColumnInfo,
    IndexType,
    IntrospectionForeignKeyInfo,
    ReferentialAction,
    IntrospectionViewInfo,
    IntrospectionTriggerInfo,
    IntrospectionScope,
)
from rhosocial.activerecord.backend.expression.types import DataType
from .status_introspector import (
    SyncOracleStatusIntrospector,
    AsyncOracleStatusIntrospector,
)
from .types import (
    OraclePackageInfo,
    OracleSequenceInfo,
    OracleSynonymInfo,
)

#: Cache namespaces for the schema-object families core has no scope for.
#: Sequences have ``IntrospectionScope.SEQUENCE`` of their own; synonyms and
#: packages are schema-level objects, so they share
#: ``IntrospectionScope.SCHEMA`` and are told apart by these ``extra`` values
#: rather than by overloading a scope that means something else.
_SYNONYM_FAMILY = "oracle:synonyms"
_PACKAGE_FAMILY = "oracle:packages"


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

    def _normalise_owner(self, owner: Optional[str]) -> str:
        """Fold an optional owner to the spelling the data dictionary uses.

        Every query in this module filters ``..._owner = ?`` with an upper
        case bind, because Oracle stores unquoted identifiers upper case.
        Folding here as well means the cache key and the bind value agree:
        ``owner="ar_crm"`` and ``owner="AR_CRM"`` are one question, asked
        once. ``None`` means "the connected user's schema", the same
        default the table and view methods take.
        """
        if owner is None:
            return self._get_default_schema()
        return owner.upper()

    def _get_version(self) -> Optional[Tuple[int, ...]]:
        """Return the Oracle server version tuple from the backend, or ``None``.

        ``None`` is a real answer here, not a gap: the backend leaves its
        version unset when the caller stated none and
        :meth:`~..backend.OracleBackend.get_server_version` could not measure
        one, and it used to report ``(19, 0, 0)`` in both cases — a version
        nobody measured, which every version gate then compared against.
        """
        return getattr(self._backend, '_version', None)

    # ------------------------------------------------------------------ #
    # SQL builders — Oracle data dictionary queries
    # ------------------------------------------------------------------ #

    def _build_database_info_sql(self) -> tuple:
        """Build SQL to get database information."""
        return (
            """
            SELECT
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_CHARACTERSET') AS charset,
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_NCHAR_CHARACTERSET') AS nchar_charset,
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_LANGUAGE') AS language,
                (SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_TERRITORY') AS territory
            FROM dual
        """,
            (),
        )

    def _build_columns_sql(self, table_name: str, schema: str) -> tuple:
        """Build SQL to get columns for a table.

        Args:
            table_name: Table to describe.
            schema: Owner (Oracle's schema) of that table.

        Returns:
            Tuple of (SQL, params). The names travel as bind values rather
            than as interpolated string literals: the data dictionary stores
            them in upper case, so the caller still folds them, but a table
            name is caller-supplied data and must never be concatenated into
            SQL text.
        """
        return (
            """
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
                data_upgraded,
                avg_col_len,
                char_length,
                char_used,
                v80_fmt_image,
                histogram,
                default_on_null,
                identity_column,
                evaluation_edition,
                unusable_before,
                unusable_beginning,
                collation
            FROM all_tab_columns
            WHERE table_name = ?
              AND owner = ?
            ORDER BY column_id
        """,
            (table_name.upper(), schema.upper()),
        )

    def _build_primary_key_sql(self, table_name: str, schema: str) -> tuple:
        """Build SQL to get primary key columns.

        Returns:
            Tuple of (SQL, params). See :meth:`_build_columns_sql` for why the
            names are bound rather than interpolated.
        """
        return (
            """
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
            WHERE cons.table_name = ?
              AND cons.owner = ?
              AND cons.constraint_type = 'P'
            ORDER BY cols.position
        """,
            (table_name.upper(), schema.upper()),
        )

    def _build_sequence_list_sql(self, owner: str) -> tuple:
        """Build SQL to list the sequences one schema owns.

        ``ALL_SEQUENCES`` is used rather than ``USER_SEQUENCES`` so that
        ``owner`` is a real filter instead of "whoever is connected". The
        owner column in this view is ``SEQUENCE_OWNER``, not ``OWNER``.

        Returns:
            Tuple of (SQL, params), with the owner bound, never
            interpolated.
        """
        return (
            """
            SELECT
                sequence_owner,
                sequence_name,
                min_value,
                max_value,
                increment_by,
                cycle_flag,
                order_flag,
                cache_size,
                last_number
            FROM all_sequences
            WHERE sequence_owner = ?
            ORDER BY sequence_name
        """,
            (owner.upper(),),
        )

    def _build_sequence_info_sql(self, name: str, owner: str) -> tuple:
        """Build SQL to read one sequence.

        Returns:
            Tuple of (SQL, params); both name and owner travel as binds.
        """
        return (
            """
            SELECT
                sequence_owner,
                sequence_name,
                min_value,
                max_value,
                increment_by,
                cycle_flag,
                order_flag,
                cache_size,
                last_number
            FROM all_sequences
            WHERE sequence_name = ?
              AND sequence_owner = ?
        """,
            (name.upper(), owner.upper()),
        )

    def _build_synonym_list_sql(self, owner: str) -> tuple:
        """Build SQL to list the synonyms one schema owns.

        Public synonyms are rows of the same view with ``owner='PUBLIC'``:
        ``PUBLIC`` is not a schema a user connects as, but it is the owner
        the dictionary records for them. Passing ``owner="PUBLIC"`` is
        therefore how public synonyms are asked for -- they are never
        mixed into a schema's private list, because a caller inspecting a
        schema should not silently receive every global alias on the
        instance.

        Returns:
            Tuple of (SQL, params), owner bound.
        """
        return (
            """
            SELECT
                owner,
                synonym_name,
                table_owner,
                table_name,
                db_link
            FROM all_synonyms
            WHERE owner = ?
            ORDER BY synonym_name
        """,
            (owner.upper(),),
        )

    def _build_synonym_info_sql(self, name: str, owner: str) -> tuple:
        """Build SQL to read one synonym.

        Returns:
            Tuple of (SQL, params); name and owner travel as binds, so the
            ``PUBLIC`` owner works here too.
        """
        return (
            """
            SELECT
                owner,
                synonym_name,
                table_owner,
                table_name,
                db_link
            FROM all_synonyms
            WHERE synonym_name = ?
              AND owner = ?
        """,
            (name.upper(), owner.upper()),
        )

    def _build_package_list_sql(self, owner: str) -> tuple:
        """Build SQL to list the PL/SQL package specifications one schema owns.

        ``ALL_OBJECTS`` carries both halves of a package -- ``PACKAGE``
        and ``PACKAGE BODY`` -- so the object type is part of the filter
        rather than something the parser sorts out afterwards: a package
        body without a specification is not a package, and counting both
        rows would report every package twice.

        Returns:
            Tuple of (SQL, params), owner bound; the object type is a
            literal because it is part of the question, not caller data.
        """
        return (
            """
            SELECT
                owner,
                object_name,
                status,
                created,
                last_ddl_time
            FROM all_objects
            WHERE object_type = 'PACKAGE'
              AND owner = ?
            ORDER BY object_name
        """,
            (owner.upper(),),
        )

    # ------------------------------------------------------------------ #
    # Parse methods — pure Python, no I/O
    # ------------------------------------------------------------------ #

    def _parse_database_info(self, rows: List[Dict[str, Any]]) -> IntrospectionDatabaseInfo:
        version = self._get_version()
        # Reported as the string "unknown" rather than a fabricated number: this
        # is a status surface, and a status surface that invents a version is the
        # same defect as a dialect that invents one.
        version_str = (
            ".".join(str(v) for v in version) if version else "unknown"
        )
        schema = self._get_default_schema()

        db_row = rows[0] if rows else {}

        return IntrospectionDatabaseInfo(
            name=schema,
            version=version_str,
            version_tuple=version,
            vendor="Oracle",
            encoding=db_row.get("CHARSET"),
            collation=db_row.get("LANGUAGE"),
        )

    def _parse_tables(
        self, rows: List[Dict[str, Any]], schema: Optional[str]
    ) -> List[IntrospectionTableInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        tables = []
        for row in rows:
            tables.append(
                IntrospectionTableInfo(
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
    ) -> List[IntrospectionColumnInfo]:
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
            elif data_type == "RAW":
                # RAW's declared width is its whole identity — two RAW columns
                # of different widths are different columns — and DATA_LENGTH
                # holds it exactly: 16 for a RAW(16), 64 for a RAW(64), with
                # DATA_PRECISION and DATA_SCALE both NULL. Leaving it out sent
                # every RAW column through the parser as a bare "RAW", which
                # could only come back with an invented width.
                # https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
                data_type_full = f"RAW({row.get('DATA_LENGTH')})"
            else:
                data_type_full = data_type

            col_name = row["COLUMN_NAME"]
            # Parse the full type string into a structured DataType so that
            # SchemaDiffer._columns_equivalent can compare two DataType
            # instances with `!=` — class plus the fields named by PARAMETERS —
            # instead of raw string comparison (core #108).
            parsed_data_type: Optional[DataType] = None
            try:
                parsed_data_type = DataType.parse_data_type_str(
                    self._backend.dialect, data_type_full
                )
            except Exception:
                # Unparseable type strings fall back to None; the differ
                # then degrades to data_type string comparison.
                parsed_data_type = None
            columns.append(
                IntrospectionColumnInfo(
                    name=col_name,
                    table_name=table_name,
                    schema=schema,
                    ordinal_position=row["COLUMN_ID"],
                    data_type=data_type.lower(),
                    data_type_full=data_type_full,
                    parsed_data_type=parsed_data_type,
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
        }
        index_map: Dict[str, IndexInfo] = {}
        for row in rows:
            idx_name = row.get("INDEX_NAME", "")
            if idx_name not in index_map:
                idx_type_str = (row.get("INDEX_TYPE") or "NORMAL").upper()
                uniqueness = row.get("UNIQUENESS", "NONUNIQUE")
                extra: Dict[str, Any] = {"oracle_index_type": idx_type_str}
                if idx_type_str == "BITMAP":
                    extra["bitmap"] = True
                elif idx_type_str.startswith("FUNCTION-BASED"):
                    extra["function_based"] = True
                index_map[idx_name] = IndexInfo(
                    name=idx_name,
                    table_name=table_name,
                    schema=schema,
                    is_unique=uniqueness == "UNIQUE",
                    is_primary=False,  # Primary keys have separate constraint
                    index_type=index_type_map.get(idx_type_str, IndexType.UNKNOWN),
                    columns=[],
                    extra=extra,
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
    ) -> List[IntrospectionForeignKeyInfo]:
        action_map = {
            "NO ACTION": ReferentialAction.NO_ACTION,
            "CASCADE": ReferentialAction.CASCADE,
            "SET NULL": ReferentialAction.SET_NULL,
            "SET DEFAULT": ReferentialAction.SET_DEFAULT,
        }
        fk_map: Dict[str, IntrospectionForeignKeyInfo] = {}
        for row in rows:
            fk_name = row.get("CONSTRAINT_NAME", "")
            if fk_name not in fk_map:
                delete_rule = (row.get("DELETE_RULE") or "NO ACTION").upper()
                fk_map[fk_name] = IntrospectionForeignKeyInfo(
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
    ) -> List[IntrospectionViewInfo]:
        return [
            IntrospectionViewInfo(
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
    ) -> Optional[IntrospectionViewInfo]:
        if not rows:
            return None
        row = rows[0]
        return IntrospectionViewInfo(
            name=row.get("VIEW_NAME", view_name),
            schema=schema,
            definition=row.get("VIEW_DEFINITION"),
            check_option=None,
            is_updatable=row.get("READ_ONLY") != "Y",
        )

    def _parse_triggers(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[IntrospectionTriggerInfo]:
        triggers = []
        for row in rows:
            trigger_type = row.get("TRIGGER_TYPE", "")
            triggering_event = row.get("TRIGGERING_EVENT", "")
            # Parse events (comma-separated)
            events = [e.strip() for e in triggering_event.split(" OR ")] if triggering_event else []

            triggers.append(
                IntrospectionTriggerInfo(
                    name=row.get("TRIGGER_NAME", ""),
                    table_name=row.get("TABLE_NAME", ""),
                    schema=schema,
                    timing=trigger_type,  # BEFORE, AFTER, INSTEAD OF
                    events=events,
                    definition=row.get("TRIGGER_BODY"),
                )
            )
        return triggers

    # ------------------------------------------------------------------ #
    # Parse methods — the schema-object families
    # ------------------------------------------------------------------ #

    def _parse_sequences(self, rows: List[Dict[str, Any]]) -> List[OracleSequenceInfo]:
        """Turn ``ALL_SEQUENCES`` rows into sequence value objects."""
        return [
            OracleSequenceInfo(
                name=row.get("SEQUENCE_NAME", ""),
                owner=row.get("SEQUENCE_OWNER", ""),
                min_value=row.get("MIN_VALUE"),
                max_value=row.get("MAX_VALUE"),
                increment=row.get("INCREMENT_BY"),
                cycle=row.get("CYCLE_FLAG") == "Y",
                cache_size=row.get("CACHE_SIZE"),
                order=row.get("ORDER_FLAG") == "Y",
                last_number=row.get("LAST_NUMBER"),
            )
            for row in rows
        ]

    def _parse_sequence_info(
        self, rows: List[Dict[str, Any]], name: str, owner: str
    ) -> Optional[OracleSequenceInfo]:
        """Turn one ``ALL_SEQUENCES`` row into a sequence value object.

        ``None`` means the sequence is not visible in that schema, which
        is a real answer: the caller asked about one object and there is
        no such object.
        """
        if not rows:
            return None
        row = rows[0]
        return OracleSequenceInfo(
            name=row.get("SEQUENCE_NAME", name),
            owner=row.get("SEQUENCE_OWNER", owner),
            min_value=row.get("MIN_VALUE"),
            max_value=row.get("MAX_VALUE"),
            increment=row.get("INCREMENT_BY"),
            cycle=row.get("CYCLE_FLAG") == "Y",
            cache_size=row.get("CACHE_SIZE"),
            order=row.get("ORDER_FLAG") == "Y",
            last_number=row.get("LAST_NUMBER"),
        )

    def _parse_synonyms(self, rows: List[Dict[str, Any]]) -> List[OracleSynonymInfo]:
        """Turn ``ALL_SYNONYMS`` rows into synonym value objects.

        ``is_public`` is derived from the owner rather than from a flag:
        Oracle has no separate public-synonym view, and ``PUBLIC`` is the
        owner row that marks one.
        """
        return [
            OracleSynonymInfo(
                name=row.get("SYNONYM_NAME", ""),
                owner=row.get("OWNER", ""),
                table_owner=row.get("TABLE_OWNER", ""),
                table_name=row.get("TABLE_NAME", ""),
                db_link=row.get("DB_LINK"),
                is_public=row.get("OWNER") == "PUBLIC",
            )
            for row in rows
        ]

    def _parse_synonym_info(
        self, rows: List[Dict[str, Any]], name: str, owner: str
    ) -> Optional[OracleSynonymInfo]:
        """Turn one ``ALL_SYNONYMS`` row into a synonym value object."""
        if not rows:
            return None
        row = rows[0]
        return OracleSynonymInfo(
            name=row.get("SYNONYM_NAME", name),
            owner=row.get("OWNER", owner),
            table_owner=row.get("TABLE_OWNER", ""),
            table_name=row.get("TABLE_NAME", ""),
            db_link=row.get("DB_LINK"),
            is_public=row.get("OWNER") == "PUBLIC",
        )

    def _parse_packages(self, rows: List[Dict[str, Any]]) -> List[OraclePackageInfo]:
        """Turn ``ALL_OBJECTS`` package-specification rows into value objects."""
        packages = []
        for row in rows:
            created = row.get("CREATED")
            last_ddl = row.get("LAST_DDL_TIME")
            packages.append(
                OraclePackageInfo(
                    name=row.get("OBJECT_NAME", ""),
                    owner=row.get("OWNER", ""),
                    status=row.get("STATUS", ""),
                    created_time=str(created) if created is not None else None,
                    last_ddl_time=str(last_ddl) if last_ddl is not None else None,
                )
            )
        return packages


class SyncOracleIntrospector(OracleIntrospectorMixin, SyncAbstractIntrospector):
    """Synchronous introspector for Oracle backends.

    Provides introspection capabilities for Oracle databases using
    the data dictionary views (ALL_TABLES, ALL_TAB_COLUMNS, etc.).
    """

    def __init__(
        self,
        backend: Any,
        executor: SyncIntrospectorExecutor,
        cache: Optional[IntrospectionCache] = None,
    ) -> None:
        super().__init__(backend, executor, cache)
        self._status_instance: Optional[SyncOracleStatusIntrospector] = None

    # ------------------------------------------------------------------ #
    # status introspector
    # ------------------------------------------------------------------ #

    @property
    def status(self) -> SyncOracleStatusIntrospector:
        """Oracle status introspector (lazily created)."""
        if self._status_instance is None:
            self._status_instance = SyncOracleStatusIntrospector(self._backend)
        return self._status_instance

    # ------------------------------------------------------------------ #
    # get_table_info override
    # ------------------------------------------------------------------ #

    def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> Optional[IntrospectionTableInfo]:
        key = self._cache.key(IntrospectionScope.TABLE, table_name, schema=schema)
        cached = self._cache.get(key)
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
        self._cache.set(key, table)
        return table

    # ------------------------------------------------------------------ #
    # list_columns override to handle primary keys
    # ------------------------------------------------------------------ #

    def list_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[IntrospectionColumnInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        key = self._cache.key(IntrospectionScope.COLUMN, table_name, schema=target_schema)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Get primary key columns first
        pk_sql, pk_params = self._build_primary_key_sql(table_name, target_schema)
        primary_keys = [
            row.get("COLUMN_NAME") for row in self._executor.execute(pk_sql, pk_params)
        ]

        # Get columns
        sql, params = self._build_columns_sql(table_name, target_schema)
        rows = self._executor.execute(sql, params)
        columns = self._parse_columns(rows, table_name, target_schema, primary_keys)

        self._cache.set(key, columns)
        return columns

    # ------------------------------------------------------------------ #
    # Schema objects: sequences, synonyms, packages
    #
    # These are Oracle's own answer to "what else is in this schema". They
    # are filtered by ``owner`` -- the schema argument every other method
    # calls ``schema`` -- and cached under the scope the object belongs to,
    # with an ``extra`` namespace where an existing scope would otherwise
    # be shared with a different family.
    # ------------------------------------------------------------------ #

    def list_sequences(self, owner: Optional[str] = None) -> List[OracleSequenceInfo]:
        """Return the sequences owned by *owner*.

        Args:
            owner: Oracle schema (owner) to list. ``None`` means the
                connected user's schema.

        Returns:
            The sequences, ordered by name. ``last_number`` is the next
            value the sequence would hand out, not the last one it handed
            out -- see :class:`~.types.OracleSequenceInfo`.
        """
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(IntrospectionScope.SEQUENCE, schema=target_owner)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_sequence_list_sql(target_owner)
        sequences = self._parse_sequences(self._executor.execute(sql, params))

        self._cache.set(key, sequences)
        return sequences

    def get_sequence_info(
        self, name: str, owner: Optional[str] = None
    ) -> Optional[OracleSequenceInfo]:
        """Return one sequence by name, or ``None`` if it does not exist.

        Args:
            name: Sequence name; folded to upper case like the dictionary
                stores it.
            owner: Oracle schema (owner) to look in. ``None`` means the
                connected user's schema.

        Returns:
            The sequence, or ``None``. A missing sequence is not cached:
            the cache cannot store ``None`` as a hit without turning
            "absent" into a value, so the next call asks again.
        """
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SEQUENCE, name.upper(), schema=target_owner
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_sequence_info_sql(name, target_owner)
        sequence = self._parse_sequence_info(
            self._executor.execute(sql, params), name.upper(), target_owner
        )
        if sequence is None:
            return None

        self._cache.set(key, sequence)
        return sequence

    def list_synonyms(self, owner: Optional[str] = None) -> List[OracleSynonymInfo]:
        """Return the synonyms owned by *owner*.

        Public synonyms have no schema of their own: they belong to
        ``PUBLIC``, and ``ALL_SYNONYMS`` records that as the ``OWNER``
        value rather than leaving it empty. So they are listed by asking
        for ``owner="PUBLIC"`` explicitly, never by folding them into a
        schema's private list -- a caller inspecting ``AR_CRM`` gets
        ``AR_CRM``'s aliases and not every global alias on the instance.

        Args:
            owner: Oracle schema (owner) to list, or ``"PUBLIC"`` for the
                public synonyms. ``None`` means the connected user's
                schema.

        Returns:
            The synonyms, ordered by name; ``is_public`` is true for the
            ``PUBLIC`` family.
        """
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SCHEMA, schema=target_owner, extra=_SYNONYM_FAMILY
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_synonym_list_sql(target_owner)
        synonyms = self._parse_synonyms(self._executor.execute(sql, params))

        self._cache.set(key, synonyms)
        return synonyms

    def get_synonym_info(
        self, name: str, owner: Optional[str] = None
    ) -> Optional[OracleSynonymInfo]:
        """Return one synonym by name, or ``None`` if it does not exist.

        Args:
            name: Synonym name.
            owner: Oracle schema (owner) to look in, or ``"PUBLIC"``.
                ``None`` means the connected user's schema.

        Returns:
            The synonym, or ``None``.
        """
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SCHEMA,
            name.upper(),
            schema=target_owner,
            extra=_SYNONYM_FAMILY,
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_synonym_info_sql(name, target_owner)
        synonym = self._parse_synonym_info(
            self._executor.execute(sql, params), name.upper(), target_owner
        )
        if synonym is None:
            return None

        self._cache.set(key, synonym)
        return synonym

    def list_packages(self, owner: Optional[str] = None) -> List[OraclePackageInfo]:
        """Return the PL/SQL package specifications owned by *owner*.

        Package bodies are not included: one package is one specification
        plus an optional body, and ``ALL_OBJECTS`` holds a row for each.
        Filtering on ``OBJECT_TYPE = 'PACKAGE'`` keeps the count honest.

        Args:
            owner: Oracle schema (owner) to list. ``None`` means the
                connected user's schema.

        Returns:
            The packages, ordered by name, with their compile status and
            last-DDL time.
        """
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SCHEMA, schema=target_owner, extra=_PACKAGE_FAMILY
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_package_list_sql(target_owner)
        packages = self._parse_packages(self._executor.execute(sql, params))

        self._cache.set(key, packages)
        return packages


class AsyncOracleIntrospector(OracleIntrospectorMixin, AsyncAbstractIntrospector):
    """Asynchronous introspector for Oracle backends.

    Provides introspection capabilities for Oracle databases using
    the data dictionary views (ALL_TABLES, ALL_TAB_COLUMNS, etc.).
    """

    def __init__(
        self,
        backend: Any,
        executor: AsyncIntrospectorExecutor,
        cache: Optional[IntrospectionCache] = None,
    ) -> None:
        super().__init__(backend, executor, cache)
        self._status_instance: Optional[AsyncOracleStatusIntrospector] = None

    # ------------------------------------------------------------------ #
    # status introspector
    # ------------------------------------------------------------------ #

    @property
    def status(self) -> AsyncOracleStatusIntrospector:
        """Oracle status introspector (lazily created)."""
        if self._status_instance is None:
            self._status_instance = AsyncOracleStatusIntrospector(self._backend)
        return self._status_instance

    # ------------------------------------------------------------------ #
    # get_table_info override
    # ------------------------------------------------------------------ #

    async def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> Optional[IntrospectionTableInfo]:
        key = self._cache.key(IntrospectionScope.TABLE, table_name, schema=schema)
        cached = self._cache.get(key)
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
        self._cache.set(key, table)
        return table

    # ------------------------------------------------------------------ #
    # list_columns override to handle primary keys
    # ------------------------------------------------------------------ #

    async def list_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[IntrospectionColumnInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        key = self._cache.key(IntrospectionScope.COLUMN, table_name, schema=target_schema)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Get primary key columns first
        pk_sql, pk_params = self._build_primary_key_sql(table_name, target_schema)
        primary_keys = [
            row.get("COLUMN_NAME")
            for row in await self._executor.execute(pk_sql, pk_params)
        ]

        # Get columns
        sql, params = self._build_columns_sql(table_name, target_schema)
        rows = await self._executor.execute(sql, params)
        columns = self._parse_columns(rows, table_name, target_schema, primary_keys)

        self._cache.set(key, columns)
        return columns

    # ------------------------------------------------------------------ #
    # Schema objects: sequences, synonyms, packages
    #
    # Same names and signatures as the sync class, awaited. See the sync
    # implementation for what each family means.
    # ------------------------------------------------------------------ #

    async def list_sequences(self, owner: Optional[str] = None) -> List[OracleSequenceInfo]:
        """Return the sequences owned by *owner*."""
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(IntrospectionScope.SEQUENCE, schema=target_owner)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_sequence_list_sql(target_owner)
        sequences = self._parse_sequences(await self._executor.execute(sql, params))

        self._cache.set(key, sequences)
        return sequences

    async def get_sequence_info(
        self, name: str, owner: Optional[str] = None
    ) -> Optional[OracleSequenceInfo]:
        """Return one sequence by name, or ``None`` if it does not exist."""
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SEQUENCE, name.upper(), schema=target_owner
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_sequence_info_sql(name, target_owner)
        sequence = self._parse_sequence_info(
            await self._executor.execute(sql, params), name.upper(), target_owner
        )
        if sequence is None:
            return None

        self._cache.set(key, sequence)
        return sequence

    async def list_synonyms(self, owner: Optional[str] = None) -> List[OracleSynonymInfo]:
        """Return the synonyms owned by *owner*.

        Public synonyms are listed with ``owner="PUBLIC"``; see the sync
        method for why they are not folded into a schema's list.
        """
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SCHEMA, schema=target_owner, extra=_SYNONYM_FAMILY
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_synonym_list_sql(target_owner)
        synonyms = self._parse_synonyms(await self._executor.execute(sql, params))

        self._cache.set(key, synonyms)
        return synonyms

    async def get_synonym_info(
        self, name: str, owner: Optional[str] = None
    ) -> Optional[OracleSynonymInfo]:
        """Return one synonym by name, or ``None`` if it does not exist."""
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SCHEMA,
            name.upper(),
            schema=target_owner,
            extra=_SYNONYM_FAMILY,
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_synonym_info_sql(name, target_owner)
        synonym = self._parse_synonym_info(
            await self._executor.execute(sql, params), name.upper(), target_owner
        )
        if synonym is None:
            return None

        self._cache.set(key, synonym)
        return synonym

    async def list_packages(self, owner: Optional[str] = None) -> List[OraclePackageInfo]:
        """Return the PL/SQL package specifications owned by *owner*."""
        target_owner = self._normalise_owner(owner)
        key = self._cache.key(
            IntrospectionScope.SCHEMA, schema=target_owner, extra=_PACKAGE_FAMILY
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self._build_package_list_sql(target_owner)
        packages = self._parse_packages(await self._executor.execute(sql, params))

        self._cache.set(key, packages)
        return packages
