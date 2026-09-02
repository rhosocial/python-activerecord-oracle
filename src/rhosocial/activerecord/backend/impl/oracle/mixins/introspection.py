# src/rhosocial/activerecord/backend/impl/oracle/mixins/introspection.py
"""Oracle dialect introspection capability declaration and query formatting.

Implements the ``format_*_query`` methods consumed by the core introspector
base class via the Expression→Dialect path. All queries target the Oracle
data dictionary (``ALL_TABLES``, ``ALL_TAB_COLUMNS``, ``ALL_INDEXES``, ...)
filtered by the current schema owner.
"""
from typing import List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.introspection.types import IntrospectionScope
    from rhosocial.activerecord.backend.expression.introspection import (
        DatabaseInfoExpression,
        TableListExpression,
        ColumnInfoExpression,
        IndexInfoExpression,
        ForeignKeyExpression,
        ViewListExpression,
        ViewInfoExpression,
        TriggerListExpression,
    )

_SYSTEM_OWNERS = (
    "SYS", "SYSTEM", "XDB", "MDSYS", "CTXSYS", "ORDSYS", "OUTLN", "DBSNMP",
    "APPQOSSYS", "AUDSYS", "GSMADMIN_INTERNAL", "LBACSYS", "OLAPSYS",
    "ORDDATA", "WMSYS",
)


class OracleIntrospectionMixin:
    """Oracle introspection capabilities backed by the data dictionary."""

    # ========== Capability Detection ==========

    def supports_introspection(self) -> bool:
        return True

    def supports_database_info(self) -> bool:
        return True

    def supports_table_introspection(self) -> bool:
        return True

    def supports_column_introspection(self) -> bool:
        return True

    def supports_index_introspection(self) -> bool:
        return True

    def supports_foreign_key_introspection(self) -> bool:
        return True

    def supports_view_introspection(self) -> bool:
        return True

    def supports_trigger_introspection(self) -> bool:
        return True

    def get_supported_introspection_scopes(self) -> List["IntrospectionScope"]:
        from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

        return [
            IntrospectionScope.DATABASE,
            IntrospectionScope.TABLE,
            IntrospectionScope.COLUMN,
            IntrospectionScope.INDEX,
            IntrospectionScope.FOREIGN_KEY,
            IntrospectionScope.VIEW,
            IntrospectionScope.TRIGGER,
        ]

    # ========== Query Formatting ==========

    @staticmethod
    def _owner_conditions(
        schema: str,
        start: int,
        include_system: bool = False,
        column: str = "OWNER",
    ) -> Tuple[List[str], list, int]:
        """Build owner-scoped WHERE fragments starting at bind index ``start``."""
        conditions: List[str] = []
        params: list = []
        if schema:
            conditions.append(f"{column} = :{start}")
            params.append(str(schema).upper())
            return conditions, params, start + 1
        if not include_system:
            placeholders = ", ".join(f":{start + i}" for i in range(len(_SYSTEM_OWNERS)))
            conditions.append(f"{column} NOT IN ({placeholders})")
            params.extend(_SYSTEM_OWNERS)
            start += len(_SYSTEM_OWNERS)
        return conditions, params, start

    def format_database_info_query(self, expr: "DatabaseInfoExpression") -> Tuple[str, tuple]:
        """Format database information query (NLS settings)."""
        sql = (
            "SELECT "
            "(SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_CHARACTERSET') AS CHARSET, "
            "(SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_NCHAR_CHARACTERSET') AS NCHAR_CHARSET, "
            "(SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_LANGUAGE') AS LANGUAGE, "
            "(SELECT value FROM v$nls_parameters WHERE parameter = 'NLS_TERRITORY') AS TERRITORY "
            "FROM dual"
        )
        return sql, ()

    def format_table_list_query(self, expr: "TableListExpression") -> Tuple[str, tuple]:
        """Format table (and optionally view) list query."""
        params = expr.get_params()
        schema = params.get("schema") or ""
        include_views = params.get("include_views", True)
        include_system = params.get("include_system", False)
        table_type = (params.get("table_type") or "").upper()

        want_tables = table_type != "VIEW"
        want_views = table_type == "VIEW" or include_views

        owner_conditions, sql_params, next_bind = self._owner_conditions(
            schema, 1, include_system, column="t.OWNER"
        )
        owner_where = ("WHERE " + " AND ".join(owner_conditions)) if owner_conditions else ""
        parts: List[str] = []
        if want_tables:
            parts.append(
                "SELECT t.table_name AS TABLE_NAME, 'BASE TABLE' AS TABLE_TYPE, "
                "c.comments AS COMMENTS, t.num_rows AS NUM_ROWS, "
                "t.blocks * 8192 AS DATA_LENGTH, t.last_analyzed AS LAST_ANALYZED "
                "FROM all_tables t LEFT JOIN all_tab_comments c "
                "ON t.table_name = c.table_name AND t.owner = c.owner "
                f"{owner_where}"
            )
        if want_views:
            view_conditions, view_params, _ = self._owner_conditions(
                schema, next_bind, include_system, column="v.OWNER"
            )
            view_where = ("WHERE " + " AND ".join(view_conditions)) if view_conditions else ""
            parts.append(
                "SELECT v.view_name AS TABLE_NAME, 'VIEW' AS TABLE_TYPE, "
                "NULL AS COMMENTS, NULL AS NUM_ROWS, NULL AS DATA_LENGTH, NULL AS LAST_ANALYZED "
                f"FROM all_views v {view_where}"
            )
            sql_params.extend(view_params)
        joiner = " UNION ALL " if len(parts) > 1 else ""
        sql = f"{joiner.join(parts)} ORDER BY TABLE_NAME"
        return sql, tuple(sql_params)

    def format_column_info_query(self, expr: "ColumnInfoExpression") -> Tuple[str, tuple]:
        """Format column information query."""
        params = expr.get_params()
        table = params.get("table", "")
        schema = params.get("schema", "")
        sql = (
            "SELECT column_name AS COLUMN_NAME, data_type AS DATA_TYPE, "
            "data_precision AS DATA_PRECISION, data_scale AS DATA_SCALE, "
            "nullable AS NULLABLE, column_id AS COLUMN_ID, "
            "data_default AS DATA_DEFAULT, identity_column AS IDENTITY_COLUMN, "
            "char_length AS CHAR_LENGTH, data_length AS DATA_LENGTH, "
            "character_set_name AS CHARACTER_SET_NAME "
            "FROM all_tab_columns "
            "WHERE owner = :1 AND table = :2 "
            "ORDER BY column_id"
        )
        return sql, (str(schema).upper(), str(table).upper())

    def format_index_info_query(self, expr: "IndexInfoExpression") -> Tuple[str, tuple]:
        """Format index information query."""
        params = expr.get_params()
        table = params.get("table", "")
        schema = params.get("schema", "")
        sql = (
            "SELECT i.index_name AS INDEX_NAME, i.index_type AS INDEX_TYPE, "
            "i.uniqueness AS UNIQUENESS, ic.column_name AS COLUMN_NAME, "
            "ic.column_position AS COLUMN_POSITION, ic.descend AS DESCEND "
            "FROM all_indexes i "
            "JOIN all_ind_columns ic ON i.index_name = ic.index_name "
            "AND i.owner = ic.index_owner "
            "WHERE i.table_owner = :1 AND i.table_name = :2 "
            "ORDER BY i.index_name, ic.column_position"
        )
        return sql, (str(schema).upper(), str(table).upper())

    def format_foreign_key_query(self, expr: "ForeignKeyExpression") -> Tuple[str, tuple]:
        """Format foreign key information query."""
        params = expr.get_params()
        table = params.get("table", "")
        schema = params.get("schema", "")
        sql = (
            "SELECT cons.constraint_name AS CONSTRAINT_NAME, "
            "cons.delete_rule AS DELETE_RULE, "
            "ref_cons.table_name AS REFERENCED_TABLE_NAME, "
            "cols.column_name AS COLUMN_NAME, "
            "ref_cols.column_name AS REFERENCED_COLUMN_NAME "
            "FROM all_constraints cons "
            "JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name "
            "AND cons.owner = cols.owner "
            "JOIN all_constraints ref_cons ON cons.r_constraint_name = ref_cons.constraint_name "
            "AND cons.r_owner = ref_cons.owner "
            "JOIN all_cons_columns ref_cols ON ref_cons.constraint_name = ref_cols.constraint_name "
            "AND ref_cons.owner = ref_cols.owner AND cols.position = ref_cols.position "
            "WHERE cons.owner = :1 AND cons.table_name = :2 AND cons.constraint_type = 'R' "
            "ORDER BY cons.constraint_name, cols.position"
        )
        return sql, (str(schema).upper(), str(table).upper())

    def format_view_list_query(self, expr: "ViewListExpression") -> Tuple[str, tuple]:
        """Format view list query."""
        params = expr.get_params()
        schema = params.get("schema") or ""
        include_system = params.get("include_system", False)
        conditions, sql_params, _ = self._owner_conditions(schema, 1, include_system)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = (
            "SELECT view_name AS VIEW_NAME, text_vc AS TEXT_VC, "
            "read_only AS READ_ONLY "
            f"FROM all_views {where} ORDER BY view_name"
        )
        return sql, tuple(sql_params)

    def format_view_info_query(self, expr: "ViewInfoExpression") -> Tuple[str, tuple]:
        """Format single view information query."""
        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema", "")
        sql = (
            "SELECT view_name AS VIEW_NAME, text_vc AS VIEW_DEFINITION, "
            "read_only AS READ_ONLY "
            "FROM all_views "
            "WHERE owner = :1 AND view_name = :2"
        )
        return sql, (str(schema).upper(), str(view_name).upper())

    def format_trigger_list_query(self, expr: "TriggerListExpression") -> Tuple[str, tuple]:
        """Format trigger list query."""
        params = expr.get_params()
        schema = params.get("schema") or ""
        table = params.get("table")
        conditions, sql_params, next_bind = self._owner_conditions(schema, 1)
        if table:
            conditions.append(f"table = :{next_bind}")
            sql_params.append(str(table).upper())
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = (
            "SELECT trigger_name AS TRIGGER_NAME, trigger_type AS TRIGGER_TYPE, "
            "triggering_event AS TRIGGERING_EVENT, table_name AS TABLE_NAME, "
            "trigger_body AS TRIGGER_BODY "
            f"FROM all_triggers {where} ORDER BY trigger"
        )
        return sql, tuple(sql_params)
