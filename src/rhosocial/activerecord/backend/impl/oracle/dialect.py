# src/rhosocial/activerecord/backend/impl/oracle/dialect.py
"""
Oracle backend SQL dialect implementation.

This dialect implements protocols for features that Oracle actually supports,
based on the Oracle version provided at initialization.

Oracle features and support based on version:
- JSON operations (since 12c with native JSON type in 21c/23ai)
- Window functions (since 8i)
- CTEs (Common Table Expressions) (since 9i)
- RETURNING clause (since 8i)
- MERGE statement (since 9i)
- FETCH FIRST/OFFSET pagination (since 12c)
- BOOLEAN type (since 23ai)
- VECTOR type (since 23ai)
"""
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.protocols import (
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    JSONSupport,
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    LockingSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    TemporalTableSupport,
    UpsertSupport,
    LateralJoinSupport,
    WildcardSupport,
    JoinSupport,
    ViewSupport,
    SchemaSupport,
    IndexSupport,
    SequenceSupport,
    TableSupport,
    IntrospectionSupport,
    SetOperationSupport,
    TruncateSupport,
    ConstraintSupport,
    TransactionControlSupport,
    SQLFunctionSupport,
)
from rhosocial.activerecord.backend.dialect.mixins import (
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    LockingMixin,
    MergeMixin,
    OrderedSetAggregationMixin,
    QualifyClauseMixin,
    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    SetOperationMixin,
    TruncateMixin,
    ViewMixin,
    SchemaMixin,
    IndexMixin,
    SequenceMixin,
    TableMixin,
    ConstraintMixin,
    IntrospectionMixin,
)
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateTableExpression, CreateViewExpression, DropViewExpression,
        ColumnDefinition, TableConstraint, IndexDefinition,
        ExplainExpression,
    )


class OracleDialect(
    SQLDialectBase,
    # Include mixins for features that Oracle supports
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    LockingMixin,
    MergeMixin,
    OrderedSetAggregationMixin,
    QualifyClauseMixin,
    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    SetOperationMixin,
    TruncateMixin,
    ViewMixin,
    SchemaMixin,
    IndexMixin,
    SequenceMixin,
    TableMixin,
    ConstraintMixin,
    IntrospectionMixin,
    # Protocols for type checking
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    JSONSupport,
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    LockingSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    TemporalTableSupport,
    UpsertSupport,
    LateralJoinSupport,
    WildcardSupport,
    JoinSupport,
    SetOperationSupport,
    TruncateSupport,
    ViewSupport,
    SchemaSupport,
    IndexSupport,
    SequenceSupport,
    TableSupport,
    ConstraintSupport,
    IntrospectionSupport,
    TransactionControlSupport,
    SQLFunctionSupport,
):
    """
    Oracle dialect implementation that adapts to the Oracle version.

    Oracle features and support based on version:
    - CTEs (since 9i)
    - Recursive CTEs (since 11g R2)
    - Window functions (since 8i)
    - RETURNING clause (since 8i)
    - MERGE statement (since 9i)
    - FETCH FIRST/OFFSET pagination (since 12c)
    - JSON operations (since 12c, native type in 21c/23ai)
    - BOOLEAN type (since 23ai)
    - VECTOR type (since 23ai)
    """

    def __init__(self, version: Tuple[int, int, int] = (19, 0, 0)):
        """
        Initialize Oracle dialect with specific version.

        Args:
            version: Oracle version tuple (major, minor, patch)
                    Common versions: (11, 2, 0), (12, 1, 0), (12, 2, 0),
                                    (19, 0, 0), (21, 0, 0), (23, 0, 0)
        """
        self.version = version
        super().__init__()

    def get_parameter_placeholder(self, position: int = 0) -> str:
        """
        Get Oracle parameter placeholder.

        Returns ? during SQL construction. Placeholders are renumbered
        to :1, :2, :3 format in OracleBackend.execute() at the final step.

        Args:
            position: Parameter position (ignored)

        Returns:
            Placeholder string (?)
        """
        return "?"

    def get_server_version(self) -> Tuple[int, int, int]:
        """Return the Oracle version this dialect is configured for."""
        return self.version

    # region Protocol Support Checks based on version
    def supports_basic_cte(self) -> bool:
        """Basic CTEs are supported since Oracle 9i."""
        return self.version >= (9, 0, 0)

    def supports_recursive_cte(self) -> bool:
        """Recursive CTEs are fully supported since Oracle 11g R2."""
        return self.version >= (11, 2, 0)

    def supports_materialized_cte(self) -> bool:
        """Oracle supports MATERIALIZE hint for CTEs."""
        return True

    def supports_returning_clause(self) -> bool:
        """RETURNING clause is supported since Oracle 8i."""
        return True

    def supports_window_functions(self) -> bool:
        """Window functions are supported since Oracle 8i."""
        return self.version >= (8, 0, 0)

    def supports_window_frame_clause(self) -> bool:
        """Window frame clauses (ROWS/RANGE) are supported."""
        return True

    def supports_filter_clause(self) -> bool:
        """FILTER clause for aggregate functions is not supported in Oracle."""
        return False

    def supports_json_type(self) -> bool:
        """Native JSON type is supported since Oracle 21c."""
        return self.version >= (21, 0, 0)

    def get_json_access_operator(self) -> str:
        """Oracle uses JSON_VALUE, JSON_QUERY for JSON access."""
        return "."  # Dot notation for JSON access in Oracle

    def supports_json_table(self) -> bool:
        """Oracle supports JSON_TABLE function since 12c."""
        return self.version >= (12, 0, 0)

    def supports_rollup(self) -> bool:
        """ROLLUP is supported."""
        return True

    def supports_cube(self) -> bool:
        """CUBE is supported."""
        return True

    def supports_grouping_sets(self) -> bool:
        """GROUPING SETS is supported."""
        return True

    def supports_array_type(self) -> bool:
        """Oracle supports nested tables and VARRAY as array-like types."""
        return True  # Via nested tables or VARRAY

    def supports_array_constructor(self) -> bool:
        """Oracle supports array constructor via type constructors."""
        return True

    def supports_array_access(self) -> bool:
        """Oracle supports array subscript access."""
        return True

    def supports_explain_analyze(self) -> bool:
        """Oracle supports EXPLAIN PLAN and DBMS_XPLAN."""
        return True

    def supports_explain_format(self, format_type: str) -> bool:
        """Check if specific EXPLAIN format is supported."""
        format_type_upper = format_type.upper()
        # Oracle supports various DBMS_XPLAN formats
        return format_type_upper in ("TEXT", "JSON", "XML", "HTML", "SERIAL")

    def supports_graph_match(self) -> bool:
        """Oracle supports graph queries via property graph features."""
        return self.version >= (12, 0, 0)

    def supports_for_update(self) -> bool:
        """FOR UPDATE clause is supported."""
        return True

    def supports_for_update_skip_locked(self) -> bool:
        """FOR UPDATE SKIP LOCKED is supported since Oracle 11g."""
        return self.version >= (11, 0, 0)

    def supports_merge_statement(self) -> bool:
        """MERGE statement is supported since Oracle 9i."""
        return self.version >= (9, 0, 0)

    def supports_temporal_tables(self) -> bool:
        """Oracle Flashback provides temporal capabilities."""
        return True

    def supports_qualify_clause(self) -> bool:
        """QUALIFY clause is not directly supported."""
        return False

    def supports_upsert(self) -> bool:
        """UPSERT via MERGE statement is supported."""
        return True

    def get_upsert_syntax_type(self) -> str:
        """Oracle uses MERGE for upsert operations."""
        return "MERGE"

    def supports_lateral_join(self) -> bool:
        """LATERAL joins are supported since Oracle 12c."""
        return self.version >= (12, 0, 0)

    def supports_ordered_set_aggregation(self) -> bool:
        """Ordered-set aggregate functions are supported."""
        return True

    def supports_inner_join(self) -> bool:
        """INNER JOIN is supported."""
        return True

    def supports_left_join(self) -> bool:
        """LEFT JOIN is supported."""
        return True

    def supports_right_join(self) -> bool:
        """RIGHT JOIN is supported."""
        return True

    def supports_full_join(self) -> bool:
        """FULL JOIN is supported."""
        return True

    def supports_cross_join(self) -> bool:
        """CROSS JOIN is supported."""
        return True

    def supports_natural_join(self) -> bool:
        """NATURAL JOIN is supported."""
        return True

    def supports_wildcard(self) -> bool:
        """Wildcard (*) is supported."""
        return True
    # endregion

    # region Constraint Support (Oracle-specific overrides)
    def supports_fk_on_update(self) -> bool:
        """Oracle does not support ON UPDATE referential actions."""
        return False

    def supports_fk_match(self) -> bool:
        """Oracle does not support MATCH PARTIAL/FULL (uses default MATCH NONE)."""
        return False

    def supports_constraint_enforced(self) -> bool:
        """Oracle supports RELY/NORELY (NOT ENFORCED) since 12c."""
        return self.version >= (12, 0, 0)
    # endregion

    # region Transaction Control Support
    def supports_transaction_mode(self) -> bool:
        """Oracle supports READ ONLY/READ WRITE transaction mode."""
        return True

    def supports_isolation_level_in_begin(self) -> bool:
        """Oracle uses SET TRANSACTION before the first statement, not in BEGIN."""
        return False

    def supports_read_only_transaction(self) -> bool:
        """Oracle supports READ ONLY transactions."""
        return True

    def supports_deferrable_transaction(self) -> bool:
        """Oracle supports DEFERRABLE for transactions."""
        return True

    def supports_savepoint(self) -> bool:
        """Oracle supports savepoints."""
        return True

    def format_begin_transaction(
        self, expr
    ) -> Tuple[str, tuple]:
        """Format BEGIN TRANSACTION for Oracle. Oracle doesn't require BEGIN."""
        return ("", ())

    def format_commit_transaction(
        self, expr
    ) -> Tuple[str, tuple]:
        """Format COMMIT for Oracle."""
        return ("COMMIT", ())

    def format_rollback_transaction(
        self, expr
    ) -> Tuple[str, tuple]:
        """Format ROLLBACK for Oracle with optional savepoint."""
        params = expr.get_params()
        savepoint = params.get("savepoint")
        if savepoint:
            return (f"ROLLBACK TO SAVEPOINT {self.format_identifier(savepoint)}", ())
        return ("ROLLBACK", ())

    def format_savepoint(self, expr) -> Tuple[str, tuple]:
        """Format SAVEPOINT for Oracle."""
        return (f"SAVEPOINT {self.format_identifier(expr.name)}", ())

    def format_release_savepoint(
        self, expr
    ) -> Tuple[str, tuple]:
        """Oracle does not support RELEASE SAVEPOINT (savepoints are released on commit/rollback)."""
        return ("", ())

    def format_set_transaction(
        self, expr
    ) -> Tuple[str, tuple]:
        """Format SET TRANSACTION for Oracle."""
        params = expr.get_params()
        parts = ["SET TRANSACTION"]
        isolation = params.get("isolation_level")
        if isolation:
            parts.append(f"ISOLATION LEVEL {isolation}")
        mode = params.get("mode")
        if mode:
            parts.append(str(mode))
        if params.get("deferrable"):
            parts.append("DEFERRABLE")
        return (" ".join(parts), ())
    # endregion

    # region Set Operation Support
    def supports_union(self) -> bool:
        """UNION is supported."""
        return True

    def supports_union_all(self) -> bool:
        """UNION ALL is supported."""
        return True

    def supports_intersect(self) -> bool:
        """INTERSECT is supported."""
        return True

    def supports_except(self) -> bool:
        """EXCEPT is supported as MINUS in Oracle."""
        return True

    def supports_set_operation_order_by(self) -> bool:
        """Set operations support ORDER BY."""
        return True

    def supports_set_operation_limit_offset(self) -> bool:
        """Set operations support FETCH FIRST/OFFSET since 12c."""
        return self.version >= (12, 0, 0)

    def supports_set_operation_for_update(self) -> bool:
        """Set operations with FOR UPDATE have limitations."""
        return False
    # endregion

    def format_identifier(self, identifier: str) -> str:
        """
        Format identifier for Oracle.

        Oracle stores unquoted identifiers as uppercase. Since we create tables
        without quotes, we generate SQL without quotes and convert identifiers
        to uppercase.

        Args:
            identifier: Raw identifier string

        Returns:
            Uppercase identifier without quotes
        """
        # Convert to uppercase for Oracle (unquoted identifiers are stored as uppercase)
        return identifier.upper()

    def format_limit_offset(self, limit: Optional[int] = None,
                            offset: Optional[int] = None) -> Tuple[Optional[str], List[Any]]:
        """
        Format LIMIT and OFFSET clause for Oracle.

        Oracle 12c+ uses FETCH FIRST/OFFSET syntax.
        Older versions use ROWNUM-based pagination.
        """
        params = []
        sql_parts = []

        if self.version >= (12, 0, 0):
            # Oracle 12c+ FETCH FIRST/OFFSET syntax
            if offset is not None:
                sql_parts.append(f"OFFSET {offset} ROWS")

            if limit is not None:
                sql_parts.append(f"FETCH FIRST {limit} ROWS ONLY")
            else:
                sql_parts.append("FETCH FIRST ROWS ONLY")
        else:
            # Older Oracle versions: use ROWNUM (caller must wrap query)
            # This is a simplified approach; actual implementation would need subquery
            pass

        if not sql_parts:
            return None, []

        return " ".join(sql_parts), params

    def format_limit_offset_clause(self, clause) -> Tuple[str, tuple]:
        """
        Format LIMIT and OFFSET clause for Oracle.

        Oracle 12c+ uses FETCH FIRST/OFFSET syntax instead of LIMIT/OFFSET.
        This overrides the base implementation to generate Oracle-specific syntax.

        Args:
            clause: LimitOffsetClause object with limit and offset attributes

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        from rhosocial.activerecord.backend.expression.bases import ToSQLProtocol

        all_params = []
        sql_parts = []

        if self.version >= (12, 0, 0):
            # Oracle 12c+ FETCH FIRST/OFFSET syntax
            # Note: OFFSET must come before FETCH FIRST
            if clause.offset is not None:
                if isinstance(clause.offset, ToSQLProtocol):
                    offset_sql, offset_params = clause.offset.to_sql()
                    sql_parts.append(f"OFFSET {offset_sql} ROWS")
                    all_params.extend(offset_params)
                else:
                    sql_parts.append(f"OFFSET {self.get_parameter_placeholder()} ROWS")
                    all_params.append(clause.offset)

            if clause.limit is not None:
                if isinstance(clause.limit, ToSQLProtocol):
                    limit_sql, limit_params = clause.limit.to_sql()
                    sql_parts.append(f"FETCH FIRST {limit_sql} ROWS ONLY")
                    all_params.extend(limit_params)
                else:
                    sql_parts.append(f"FETCH FIRST {self.get_parameter_placeholder()} ROWS ONLY")
                    all_params.append(clause.limit)
        else:
            # Older Oracle versions: ROWNUM pagination (simplified)
            # For full support, queries need to be wrapped in subquery
            if clause.limit is not None:
                if isinstance(clause.limit, ToSQLProtocol):
                    limit_sql, limit_params = clause.limit.to_sql()
                    sql_parts.append(f"ROWNUM <= {limit_sql}")
                    all_params.extend(limit_params)
                else:
                    sql_parts.append(f"ROWNUM <= {self.get_parameter_placeholder()}")
                    all_params.append(clause.limit)

        return " ".join(sql_parts), tuple(all_params)

    # region View Support
    def supports_or_replace_view(self) -> bool:
        """CREATE OR REPLACE VIEW is supported."""
        return True

    def supports_temporary_view(self) -> bool:
        """TEMPORARY views are not directly supported."""
        return False

    def supports_materialized_view(self) -> bool:
        """Materialized views are supported."""
        return True

    def supports_if_exists_view(self) -> bool:
        """DROP VIEW IF EXISTS is not directly supported (can use PL/SQL)."""
        return False

    def supports_view_check_option(self) -> bool:
        """WITH CHECK OPTION is supported."""
        return True

    def supports_cascade_view(self) -> bool:
        """DROP VIEW CASCADE is not directly supported."""
        return False

    def format_create_view_statement(
        self, expr: "CreateViewExpression"
    ) -> Tuple[str, tuple]:
        """Format CREATE VIEW statement for Oracle."""
        parts = ["CREATE"]

        if expr.replace:
            parts.append("OR REPLACE")

        if expr.temporary:
            # Oracle uses GLOBAL TEMPORARY for tables, not views
            pass

        parts.append("VIEW")
        parts.append(self.format_identifier(expr.view_name))

        if expr.column_aliases:
            cols = ', '.join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")

        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")

        if expr.options and expr.options.check_option:
            check_option = expr.options.check_option.value
            parts.append(f"WITH {check_option} CHECK OPTION")

        return ' '.join(parts), query_params

    def format_drop_view_statement(
        self, expr: "DropViewExpression"
    ) -> Tuple[str, tuple]:
        """Format DROP VIEW statement for Oracle."""
        parts = ["DROP VIEW"]
        parts.append(self.format_identifier(expr.view_name))
        return ' '.join(parts), ()
    # endregion

    # region Schema Support
    def supports_create_schema(self) -> bool:
        """CREATE SCHEMA is supported (as CREATE USER)."""
        return True

    def supports_drop_schema(self) -> bool:
        """DROP SCHEMA is supported (as DROP USER)."""
        return True

    def supports_schema_if_not_exists(self) -> bool:
        """IF NOT EXISTS is not directly supported."""
        return False

    def supports_schema_if_exists(self) -> bool:
        """IF EXISTS is not directly supported."""
        return False
    # endregion

    # region Index Support
    def supports_create_index(self) -> bool:
        """CREATE INDEX is supported."""
        return True

    def supports_drop_index(self) -> bool:
        """DROP INDEX is supported."""
        return True

    def supports_unique_index(self) -> bool:
        """UNIQUE indexes are supported."""
        return True

    def supports_index_if_not_exists(self) -> bool:
        """IF NOT EXISTS is not directly supported."""
        return False

    def supports_index_if_exists(self) -> bool:
        """IF EXISTS is not directly supported."""
        return False
    # endregion

    # region Sequence Support
    def supports_create_sequence(self) -> bool:
        """CREATE SEQUENCE is supported."""
        return True

    def supports_drop_sequence(self) -> bool:
        """DROP SEQUENCE is supported."""
        return True
    # endregion

    # region Table Support
    def supports_if_not_exists_table(self) -> bool:
        """IF NOT EXISTS is not directly supported."""
        return False

    def supports_if_exists_table(self) -> bool:
        """IF EXISTS is not directly supported."""
        return False

    def supports_temporary_table(self) -> bool:
        """GLOBAL TEMPORARY tables are supported."""
        return True

    def supports_table_partitioning(self) -> bool:
        """Table partitioning is supported."""
        return True

    def format_create_table_statement(
        self, expr: "CreateTableExpression"
    ) -> Tuple[str, tuple]:
        """Format CREATE TABLE statement for Oracle."""
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraintType, TableConstraintType
        )

        all_params: List[Any] = []

        # Build CREATE TABLE header
        parts = ["CREATE TABLE"]

        if expr.temporary:
            parts.append("GLOBAL TEMPORARY")

        parts.append(self.format_identifier(expr.table_name))

        # Build column definitions
        column_parts = []
        for col_def in expr.columns:
            col_sql, col_params = self._format_column_definition_oracle(col_def, ColumnConstraintType)
            column_parts.append(col_sql)
            all_params.extend(col_params)

        # Build table constraints
        for t_const in expr.table_constraints:
            const_sql, const_params = self._format_table_constraint_oracle(t_const, TableConstraintType)
            column_parts.append(const_sql)
            all_params.extend(const_params)

        # Combine all parts
        parts.append(f"({', '.join(column_parts)})")

        return ' '.join(parts), tuple(all_params)

    def _format_column_definition_oracle(
        self,
        col_def: "ColumnDefinition",
        ColumnConstraintType
    ) -> Tuple[str, List[Any]]:
        """Format a single column definition with Oracle-specific syntax."""
        parts = [self.format_identifier(col_def.name), col_def.data_type]
        params: List[Any] = []

        # Build constraint parts
        constraint_parts = []
        for constraint in col_def.constraints:
            if constraint.constraint_type == ColumnConstraintType.PRIMARY_KEY:
                constraint_parts.append("PRIMARY KEY")
            elif constraint.constraint_type == ColumnConstraintType.NOT_NULL:
                constraint_parts.append("NOT NULL")
            elif constraint.constraint_type == ColumnConstraintType.UNIQUE:
                constraint_parts.append("UNIQUE")
            elif constraint.constraint_type == ColumnConstraintType.DEFAULT:
                if constraint.default_value is not None:
                    constraint_parts.append(f"DEFAULT {constraint.default_value}")
            elif constraint.constraint_type == ColumnConstraintType.NULL:
                constraint_parts.append("NULL")

            # Handle GENERATED AS IDENTITY (Oracle 12c+)
            if constraint.is_auto_increment:
                if self.version >= (12, 0, 0):
                    constraint_parts.append("GENERATED BY DEFAULT AS IDENTITY")
                else:
                    # Pre-12c: use sequence + trigger (handled separately)
                    pass

        if constraint_parts:
            parts.append(' '.join(constraint_parts))

        return ' '.join(parts), params

    def _format_table_constraint_oracle(
        self,
        t_const: "TableConstraint",
        TableConstraintType
    ) -> Tuple[str, List[Any]]:
        """Format a table-level constraint."""
        parts = []
        params: List[Any] = []

        if t_const.name:
            parts.append(f"CONSTRAINT {self.format_identifier(t_const.name)}")

        if t_const.constraint_type == TableConstraintType.PRIMARY_KEY:
            if t_const.columns:
                cols_str = ', '.join(self.format_identifier(c) for c in t_const.columns)
                parts.append(f"PRIMARY KEY ({cols_str})")
        elif t_const.constraint_type == TableConstraintType.UNIQUE:
            if t_const.columns:
                cols_str = ', '.join(self.format_identifier(c) for c in t_const.columns)
                parts.append(f"UNIQUE ({cols_str})")
        elif t_const.constraint_type == TableConstraintType.FOREIGN_KEY:
            if t_const.columns and t_const.foreign_key_table and t_const.foreign_key_columns:
                cols_str = ', '.join(self.format_identifier(c) for c in t_const.columns)
                ref_cols_str = ', '.join(
                    self.format_identifier(c) for c in t_const.foreign_key_columns
                )
                ref_table = self.format_identifier(t_const.foreign_key_table)
                parts.append(
                    f"FOREIGN KEY ({cols_str}) REFERENCES {ref_table} ({ref_cols_str})"
                )

        return ' '.join(parts), params
    # endregion

    # region Oracle-specific Features
    def supports_native_json(self) -> bool:
        """Check if Oracle version supports native JSON type (21c+)."""
        return self.version >= (21, 0, 0)

    def supports_boolean_type(self) -> bool:
        """Check if Oracle version supports native BOOLEAN type (23ai+)."""
        return self.version >= (23, 0, 0)

    def supports_vector_type(self) -> bool:
        """Check if Oracle version supports VECTOR type (23ai+)."""
        return self.version >= (23, 0, 0)

    def supports_json_duality(self) -> bool:
        """Check if Oracle version supports JSON Relational Duality (23ai+)."""
        return self.version >= (23, 0, 0)
    # endregion

    # region Hierarchical Query Support
    def supports_hierarchical_queries(self) -> bool:
        """Oracle supports CONNECT BY for hierarchical queries."""
        return True

    def supports_connect_by(self) -> bool:
        """Oracle supports CONNECT BY clause."""
        return True

    def supports_level_pseudo_column(self) -> bool:
        """Oracle supports LEVEL pseudo-column."""
        return True

    def supports_connect_by_root(self) -> bool:
        """Oracle supports CONNECT_BY_ROOT operator."""
        return True

    def supports_sys_connect_by_path(self) -> bool:
        """Oracle supports SYS_CONNECT_BY_PATH function."""
        return True
    # endregion

    # region PIVOT/UNPIVOT Support
    def supports_pivot(self) -> bool:
        """Oracle supports PIVOT (11g+)."""
        return self.version >= (11, 0, 0)

    def supports_unpivot(self) -> bool:
        """Oracle supports UNPIVOT (11g+)."""
        return self.version >= (11, 0, 0)

    def supports_pivot_xml(self) -> bool:
        """Oracle supports PIVOT XML for dynamic pivoting."""
        return self.version >= (11, 0, 0)
    # endregion

    # region Query Hint Support
    def supports_query_hints(self) -> bool:
        """Oracle supports query hints via /*+ ... */."""
        return True

    def supports_parallel_hint(self) -> bool:
        """Oracle supports PARALLEL hint."""
        return True

    def supports_index_hint(self) -> bool:
        """Oracle supports INDEX hint."""
        return True

    def supports_leading_hint(self) -> bool:
        """Oracle supports LEADING hint."""
        return True

    def supports_optimizer_hints(self) -> bool:
        """Oracle supports optimizer hints."""
        return True
    # endregion

    # region Enhanced Locking Support
    def supports_for_update_nowait(self) -> bool:
        """Oracle supports FOR UPDATE NOWAIT."""
        return True

    def supports_for_update_wait(self) -> bool:
        """Oracle supports FOR UPDATE WAIT n."""
        return True

    def supports_for_update_of(self) -> bool:
        """Oracle supports FOR UPDATE OF columns."""
        return True
    # endregion

    # region Expression Formatting Methods
    def format_connect_by(self, expr) -> Tuple[str, List[Any]]:
        """Format CONNECT BY expression."""
        return expr.to_sql(self)

    def format_pivot(self, expr) -> Tuple[str, List[Any]]:
        """Format PIVOT expression."""
        return expr.to_sql(self)

    def format_unpivot(self, expr) -> Tuple[str, List[Any]]:
        """Format UNPIVOT expression."""
        return expr.to_sql(self)

    def format_hint(self, expr) -> Tuple[str, List[Any]]:
        """Format query hint expression."""
        return expr.to_sql(self)

    def format_for_update(self, expr) -> Tuple[str, List[Any]]:
        """Format FOR UPDATE expression."""
        return expr.to_sql(self)
    # endregion

    # region Truncate Support
    def supports_truncate_cascade(self) -> bool:
        """Oracle does not support CASCADE on TRUNCATE."""
        return False

    def format_truncate_statement(
        self, expr
    ) -> Tuple[str, tuple]:
        """Format TRUNCATE TABLE statement for Oracle."""
        from rhosocial.activerecord.backend.expression.statements import TruncateExpression
        parts = ["TRUNCATE TABLE"]
        parts.append(self.format_identifier(expr.table_name))
        return (' '.join(parts), ())
    # endregion

    # region Function Support
    def supports_functions(self) -> Dict[str, bool]:
        """Return supported SQL functions as function_name -> bool mapping.

        Builds the function support dict from core functions and Oracle-specific
        functions, checking version requirements.
        """
        from rhosocial.activerecord.backend.impl.oracle.function_versions import (
            ORACLE_FUNCTION_VERSIONS
        )
        result = {}
        for func_name, (min_ver, max_ver) in ORACLE_FUNCTION_VERSIONS.items():
            if max_ver is None:
                result[func_name] = self.version >= min_ver
            else:
                result[func_name] = min_ver <= self.version <= max_ver
        return result
    # endregion
