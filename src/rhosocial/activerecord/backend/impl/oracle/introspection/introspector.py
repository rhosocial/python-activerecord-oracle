# src/rhosocial/activerecord/backend/impl/oracle/introspection/introspector.py
"""
Oracle concrete introspectors.

Implements SyncAbstractIntrospector and AsyncAbstractIntrospector for Oracle
databases using Oracle data dictionary views for metadata queries.

The introspectors are exposed via ``backend.introspector``.

Architecture:
  - SQL generation: Direct SQL queries to Oracle data dictionary views
  - Query execution: Handled by IntrospectorExecutor
  - Result parsing: parse_* methods in this module (pure Python, no I/O)

Key behaviours:
  - Queries USER_TABLES, USER_TAB_COLUMNS, USER_INDEXES,
    USER_CONSTRAINTS, USER_VIEWS, USER_TRIGGERS
  - Queries ALL_SEQUENCES, ALL_SYNONYMS and ALL_OBJECTS for the
    backend-specific sequence/synonym/package families
  - parse_* methods are pure Python — shared by sync and async introspectors

Design principle: Sync and Async are separate and cannot coexist.
- SyncOracleIntrospector: for synchronous backends
- AsyncOracleIntrospector: for asynchronous backends
"""

import copy
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.introspection import (
        OraclePackageListQueryStatement,
        OraclePrimaryKeyQueryStatement,
        OracleSequenceInfoQueryStatement,
        OracleSequenceListQueryStatement,
        OracleSynonymInfoQueryStatement,
        OracleSynonymListQueryStatement,
    )

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

#: The object kinds the table listing can report, in core's vocabulary.
#: The dictionary spells them for what they are -- ``ALL_TABLES`` answers
#: ``TABLE``, ``ALL_VIEWS`` ``VIEW`` and ``ALL_MVIEWS`` ``MATERIALIZED VIEW``
#: -- and each maps to the member that says the same. ``BASE TABLE`` is kept
#: for a caller replaying rows recorded before the listing named the kind;
#: every statement this backend builds now answers ``TABLE``.
_TABLE_TYPE_BY_KIND = {
    "TABLE": TableType.BASE_TABLE,
    "BASE TABLE": TableType.BASE_TABLE,
    "VIEW": TableType.VIEW,
    "MATERIALIZED VIEW": TableType.MATERIALIZED_VIEW,
}

#: The dictionary types whose *declared* size is a number of characters.
#: ``CHAR_LENGTH`` answers it for these, and ``DATA_LENGTH`` is the byte
#: count -- the same number only for a single-byte character set.
_CHARACTER_TYPES = frozenset(
    {"CHAR", "VARCHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"}
)

#: The dictionary types whose precision and scale are numbers of decimal
#: digits. ``DATA_PRECISION``/``DATA_SCALE`` answer for these and nothing
#: else: on a ``TIMESTAMP`` column ``DATA_SCALE`` is the fractional-seconds
#: precision, which is a different dimension.
_NUMERIC_TYPES = frozenset({"NUMBER", "FLOAT"})


class OracleIntrospectorMixin(IntrospectorMixin):
    """Mixin providing shared Oracle-specific introspection logic.

    Both SyncOracleIntrospector and AsyncOracleIntrospector inherit
    from this mixin to share:
    - Default schema handling
    - Oracle version detection
    - parse_* implementations
    """

    @property
    def default_schema(self) -> str:
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
            return self.default_schema
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
    # Statement factories — the questions, as trees the dialect spells
    #
    # Nothing here renders: each factory returns a statement object, and the
    # dialect's ``format_*_query_statement`` writes the SQL. A caller renders
    # with ``.to_sql()``. The primary key is a constraint (not the index
    # listing core reads), so it has a question of its own; sequences,
    # synonyms and packages are Oracle's own families. All four live in
    # ``expression/introspection.py``.
    # ------------------------------------------------------------------ #

    def primary_key_statement(
        self, *, table_name: str, schema: str
    ) -> "OraclePrimaryKeyQueryStatement":
        """Return the statement that lists one table's primary-key columns."""
        from ..expression.introspection import OraclePrimaryKeyQueryStatement

        return OraclePrimaryKeyQueryStatement(
            self.dialect, table_name=table_name, schema=schema
        )

    def sequence_list_statement(self, *, owner: str) -> "OracleSequenceListQueryStatement":
        """Return the statement that lists the sequences one owner has."""
        from ..expression.introspection import OracleSequenceListQueryStatement

        return OracleSequenceListQueryStatement(self.dialect, owner=owner.upper())

    def sequence_info_statement(
        self, *, sequence_name: str, owner: str
    ) -> "OracleSequenceInfoQueryStatement":
        """Return the statement that asks for one sequence by name."""
        from ..expression.introspection import OracleSequenceInfoQueryStatement

        return OracleSequenceInfoQueryStatement(
            self.dialect, sequence_name=sequence_name.upper(), owner=owner.upper()
        )

    def synonym_list_statement(self, *, owner: str) -> "OracleSynonymListQueryStatement":
        """Return the statement that lists the synonyms one owner has."""
        from ..expression.introspection import OracleSynonymListQueryStatement

        return OracleSynonymListQueryStatement(self.dialect, owner=owner.upper())

    def synonym_info_statement(
        self, *, synonym_name: str, owner: str
    ) -> "OracleSynonymInfoQueryStatement":
        """Return the statement that asks for one synonym by name."""
        from ..expression.introspection import OracleSynonymInfoQueryStatement

        return OracleSynonymInfoQueryStatement(
            self.dialect, synonym_name=synonym_name.upper(), owner=owner.upper()
        )

    def package_list_statement(self, *, owner: str) -> "OraclePackageListQueryStatement":
        """Return the statement that lists one schema's package specifications."""
        from ..expression.introspection import OraclePackageListQueryStatement

        return OraclePackageListQueryStatement(self.dialect, owner=owner.upper())


    # ------------------------------------------------------------------ #
    # Parse methods — pure Python, no I/O
    # ------------------------------------------------------------------ #

    def parse_database_info(self, rows: List[Dict[str, Any]]) -> IntrospectionDatabaseInfo:
        # ``self.server_version`` is the contract's I/O-free answer: the adapted
        # dialect, then what the backend last learned from its server. It never
        # raises, unlike the strict ``dialect.version``, which is why a parser
        # reads it: a status surface must be able to say "unknown" rather than
        # refuse to report the database at all.
        version = self.server_version
        # Reported as the string "unknown" rather than a fabricated number: this
        # is a status surface, and a status surface that invents a version is the
        # same defect as a dialect that invents one. ``(0, 0, 0)`` is the matching
        # tuple convention, so a caller can read ``version_tuple`` without a
        # None check and still see that nothing was measured.
        version_str = (
            ".".join(str(v) for v in version) if version else "unknown"
        )
        version_tuple = tuple(version[:3]) if version else (0, 0, 0)
        schema = self.default_schema

        db_row = rows[0] if rows else {}

        return IntrospectionDatabaseInfo(
            name=schema,
            version=version_str,
            version_tuple=version_tuple,
            vendor="Oracle",
            encoding=db_row.get("CHARSET"),
            collation=db_row.get("LANGUAGE"),
        )

    def parse_tables(
        self, rows: List[Dict[str, Any]], schema: Optional[str]
    ) -> List[IntrospectionTableInfo]:
        target_schema = schema if schema is not None else self.default_schema
        tables = []
        for row in rows:
            # The kind is what the statement asked for and the catalogue
            # answered: a view is not a table because it was listed beside one.
            # A row carrying no kind at all is a table row from a caller that
            # did not project it, which is what the listing used to assume for
            # every row; the statement below always names it.
            kind = str(row.get("TABLE_TYPE") or "TABLE").upper()
            tables.append(
                IntrospectionTableInfo(
                    name=row["TABLE_NAME"],
                    schema=target_schema,
                    table_type=_TABLE_TYPE_BY_KIND.get(kind, TableType.BASE_TABLE),
                    comment=row.get("COMMENTS"),
                    row_count=row.get("NUM_ROWS"),
                    size_bytes=row.get("DATA_LENGTH"),
                    create_time=None,  # Oracle doesn't store creation time
                    update_time=str(row["LAST_ANALYZED"]) if row.get("LAST_ANALYZED") else None,
                )
            )
        return tables

    def parse_columns(
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
            declared_type = str(data_type).upper()
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

            # Each dimension of a column has its own dictionary column, and
            # they are not interchangeable. A NUMBER(12,2) column has
            # DATA_LENGTH 22 -- bytes of storage, not a character length --
            # while its width is DATA_PRECISION 12 and DATA_SCALE 2. A
            # VARCHAR2(n CHAR) column has CHAR_LENGTH n and DATA_LENGTH up to
            # 4n bytes. A TIMESTAMP(6) column carries 6 in DATA_SCALE, which is
            # the fractional-seconds precision rather than a decimal scale. So
            # each field is filled from the column that means it, and stays
            # None where no column means it.
            character_maximum_length = None
            numeric_precision = None
            numeric_scale = None
            datetime_precision = None
            if declared_type in _NUMERIC_TYPES:
                numeric_precision = row.get("DATA_PRECISION")
                numeric_scale = row.get("DATA_SCALE")
            elif declared_type in _CHARACTER_TYPES:
                character_maximum_length = row.get("CHAR_LENGTH")
                if character_maximum_length is None:
                    # CHAR_LENGTH arrived in 9i; on an older server the byte
                    # count is all there is, and it is the same number for a
                    # single-byte character set. Only character types take this
                    # fallback -- for any other type DATA_LENGTH means bytes of
                    # storage and would be a length claim of the wrong kind.
                    character_maximum_length = row.get("DATA_LENGTH")
            elif declared_type.startswith("TIMESTAMP"):
                datetime_precision = row.get("DATA_SCALE")

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
                    character_maximum_length=character_maximum_length,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    datetime_precision=datetime_precision,
                    charset=row.get("CHARACTER_SET_NAME"),
                )
            )
        return columns

    def parse_indexes(
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
            # ``CONSTRAINT_TYPE`` is projected from the constraint that *owns*
            # the index, and the join is narrowed to ``P``: a value here means
            # this index is the primary key's own index, NULL means it is not.
            constraint_type = str(row.get("CONSTRAINT_TYPE") or "").upper()
            is_primary = constraint_type == "P"
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
                    # A primary key is unique whether or not the UNIQUENESS
                    # column said so, so the two facts are reported together
                    # rather than letting one contradict the other.
                    is_unique=uniqueness == "UNIQUE" or is_primary,
                    is_primary=is_primary,
                    index_type=index_type_map.get(idx_type_str, IndexType.UNKNOWN),
                    columns=[],
                    extra=extra,
                )
            elif is_primary:
                # Rows for one index arrive one per column; the facts about the
                # index itself are read from the column that brings them.
                index_map[idx_name].is_primary = True
                index_map[idx_name].is_unique = True
            descend = row.get("DESCEND", "ASC")
            index_map[idx_name].columns.append(
                IndexColumnInfo(
                    name=row.get("COLUMN_NAME", ""),
                    ordinal_position=int(row.get("COLUMN_POSITION", 1)),
                    is_descending=descend == "DESC",
                )
            )
        return list(index_map.values())

    def parse_foreign_keys(
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

    def parse_views(
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

    def parse_view_info(
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

    def parse_triggers(
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

    def parse_sequences(self, rows: List[Dict[str, Any]]) -> List[OracleSequenceInfo]:
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

    def parse_sequence_info(
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

    def parse_synonyms(self, rows: List[Dict[str, Any]]) -> List[OracleSynonymInfo]:
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

    def parse_synonym_info(
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

    def parse_packages(self, rows: List[Dict[str, Any]]) -> List[OraclePackageInfo]:
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
    # get_view_info override: a view's columns are part of what it is
    # ------------------------------------------------------------------ #

    def get_view_info(
        self, view_name: str, schema: Optional[str] = None
    ) -> Optional[IntrospectionViewInfo]:
        """Return one view, including the columns it exposes.

        Oracle answers a view's columns from ``ALL_TAB_COLUMNS`` exactly as it
        does a table's -- a view has rows in the same dictionary view, with the
        same column metadata -- so the view is completed with the same
        ``list_columns`` call a table gets. Without it every view reported an
        empty column list, which is a claim about the view rather than a gap in
        the answer: a caller could not tell "this view selects no columns" from
        "nobody asked".

        The completed value is what goes into the cache. Caching the bare row
        first and then filling a copy would leave the next call with a view
        whose columns had gone missing again.
        """
        target_schema = schema if schema is not None else self.default_schema
        key = self._cache.key(IntrospectionScope.VIEW, view_name, schema=schema)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self.view_info_statement(view_name=view_name, schema=schema).to_sql()
        view = self.parse_view_info(self._executor.execute(sql, params), view_name, target_schema)
        if view is None:
            return None

        view = copy.copy(view)
        view.columns = self.list_columns(view_name, schema)
        self._cache.set(key, view)
        return view

    # ------------------------------------------------------------------ #
    # list_columns override to handle primary keys
    # ------------------------------------------------------------------ #

    def list_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[IntrospectionColumnInfo]:
        target_schema = schema if schema is not None else self.default_schema
        key = self._cache.key(IntrospectionScope.COLUMN, table_name, schema=target_schema)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Get primary key columns first
        pk_sql, pk_params = self.primary_key_statement(table_name=table_name, schema=target_schema).to_sql()
        primary_keys = [
            row.get("COLUMN_NAME") for row in self._executor.execute(pk_sql, pk_params)
        ]

        # Get columns
        sql, params = self.column_info_statement(table_name=table_name, schema=target_schema).to_sql()
        rows = self._executor.execute(sql, params)
        columns = self.parse_columns(rows, table_name, target_schema, primary_keys)

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

        sql, params = self.sequence_list_statement(owner=target_owner).to_sql()
        sequences = self.parse_sequences(self._executor.execute(sql, params))

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

        sql, params = self.sequence_info_statement(sequence_name=name, owner=target_owner).to_sql()
        sequence = self.parse_sequence_info(
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

        sql, params = self.synonym_list_statement(owner=target_owner).to_sql()
        synonyms = self.parse_synonyms(self._executor.execute(sql, params))

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

        sql, params = self.synonym_info_statement(synonym_name=name, owner=target_owner).to_sql()
        synonym = self.parse_synonym_info(
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

        sql, params = self.package_list_statement(owner=target_owner).to_sql()
        packages = self.parse_packages(self._executor.execute(sql, params))

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
    # get_view_info override: a view's columns are part of what it is
    # ------------------------------------------------------------------ #

    async def get_view_info(
        self, view_name: str, schema: Optional[str] = None
    ) -> Optional[IntrospectionViewInfo]:
        """Return one view, including the columns it exposes.

        The same completion the sync introspector performs, awaited: see there
        for why the columns are read from ``ALL_TAB_COLUMNS`` and why the
        completed value is what is cached.
        """
        target_schema = schema if schema is not None else self.default_schema
        key = self._cache.key(IntrospectionScope.VIEW, view_name, schema=schema)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        sql, params = self.view_info_statement(view_name=view_name, schema=schema).to_sql()
        view = self.parse_view_info(
            await self._executor.execute(sql, params), view_name, target_schema
        )
        if view is None:
            return None

        view = copy.copy(view)
        view.columns = await self.list_columns(view_name, schema)
        self._cache.set(key, view)
        return view

    # ------------------------------------------------------------------ #
    # list_columns override to handle primary keys
    # ------------------------------------------------------------------ #

    async def list_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[IntrospectionColumnInfo]:
        target_schema = schema if schema is not None else self.default_schema
        key = self._cache.key(IntrospectionScope.COLUMN, table_name, schema=target_schema)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Get primary key columns first
        pk_sql, pk_params = self.primary_key_statement(table_name=table_name, schema=target_schema).to_sql()
        primary_keys = [
            row.get("COLUMN_NAME")
            for row in await self._executor.execute(pk_sql, pk_params)
        ]

        # Get columns
        sql, params = self.column_info_statement(table_name=table_name, schema=target_schema).to_sql()
        rows = await self._executor.execute(sql, params)
        columns = self.parse_columns(rows, table_name, target_schema, primary_keys)

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

        sql, params = self.sequence_list_statement(owner=target_owner).to_sql()
        sequences = self.parse_sequences(await self._executor.execute(sql, params))

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

        sql, params = self.sequence_info_statement(sequence_name=name, owner=target_owner).to_sql()
        sequence = self.parse_sequence_info(
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

        sql, params = self.synonym_list_statement(owner=target_owner).to_sql()
        synonyms = self.parse_synonyms(await self._executor.execute(sql, params))

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

        sql, params = self.synonym_info_statement(synonym_name=name, owner=target_owner).to_sql()
        synonym = self.parse_synonym_info(
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

        sql, params = self.package_list_statement(owner=target_owner).to_sql()
        packages = self.parse_packages(await self._executor.execute(sql, params))

        self._cache.set(key, packages)
        return packages
