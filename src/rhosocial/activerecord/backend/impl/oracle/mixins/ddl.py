# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl.py
"""Oracle DDL formatting mixin (CREATE TABLE, column defs, table constraints)."""

from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateTableExpression, ColumnDefinition, TableConstraint,
    )


class OracleDDLMixin:
    """CREATE TABLE / column-def / table-constraint formatting for Oracle.

    Provides the ``format_create_table_statement`` entry point together
    with the private helpers ``format_column_definition`` and
    ``format_table_constraint`` that the dialect previously
    inlined in its monolithic file.
    """

    def format_create_table_statement(
        self, expr: "CreateTableExpression"
    ) -> Tuple[str, tuple]:
        """Format CREATE TABLE statement for Oracle.

        Oracle has no ``CREATE TABLE IF NOT EXISTS`` syntax. When
        ``if_not_exists`` is True, the statement is wrapped in an anonymous
        PL/SQL block that checks ``user_tables`` and only executes the DDL
        when the table does not exist, making creation idempotent.
        """
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraintType,
            TableConstraintType,
        )
        all_params: List[Any] = []
        parts = ["CREATE TABLE"]
        if expr.temporary:
            parts.append("GLOBAL TEMPORARY")
        parts.append(self.format_identifier(expr.table_name))

        column_parts: List[str] = []
        for col_def in expr.columns:
            col_sql, col_params = self.format_column_definition(
                col_def, ColumnConstraintType
            )
            column_parts.append(col_sql)
            all_params.extend(col_params)

        for t_const in expr.table_constraints:
            const_sql, const_params = self.format_table_constraint(
                t_const, TableConstraintType
            )
            column_parts.append(const_sql)
            all_params.extend(const_params)

        parts.append(f"({', '.join(column_parts)})")

        # TABLESPACE: expr.tablespace takes precedence; the structured
        # TableOptions serves as the fallback (Oracle has no ENGINE/CHARSET).
        tablespace = expr.tablespace
        if not tablespace:
            to = getattr(expr, "table_options", None)
            if to is not None:
                tablespace = getattr(to, "tablespace", None)
        if tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(tablespace)}")

        if expr.partition is not None:
            partition_sql, partition_params = expr.partition.to_sql()
            if partition_sql:
                parts.append(partition_sql.strip())
                all_params.extend(partition_params)

        statement = " ".join(parts)

        if expr.if_not_exists and not expr.temporary and not all_params:
            # Oracle has no IF NOT EXISTS for CREATE TABLE; guard with a
            # user_tables existence check inside an anonymous block. The DDL
            # is dynamic SQL, so any embedded single quotes must be doubled.
            # Only applied when the statement carries no bind parameters.
            embedded = statement.replace("'", "''")
            table_upper = expr.table_name.upper()
            statement = (
                f"DECLARE v_cnt NUMBER; "
                f"BEGIN SELECT COUNT(*) INTO v_cnt FROM user_tables "
                f"WHERE table_name = '{table_upper}'; "
                f"IF v_cnt = 0 THEN EXECUTE IMMEDIATE '{embedded}'; END IF; END;"
            )

        return statement, tuple(all_params)

    # ----------------------------------------------------------------
    # Private helpers
    # ----------------------------------------------------------------
    def format_column_definition(
        self,
        col_def: "ColumnDefinition",
        constraint_type=None,
    ) -> Tuple[str, List[Any]]:
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            ColumnConstraintType,
        )
        if constraint_type is None:
            constraint_type = ColumnConstraintType
        type_sql, type_params = col_def.data_type.to_sql(self)
        parts = [self.format_identifier(col_def.name), type_sql]
        params: List[Any] = list(type_params)
        constraint_parts: List[str] = []
        identity_clause: Optional[str] = None

        for constraint in col_def.constraints:
            if constraint.constraint_type == ColumnConstraintType.PRIMARY_KEY:
                constraint_parts.append("PRIMARY KEY")
            elif constraint.constraint_type == ColumnConstraintType.NOT_NULL:
                constraint_parts.append("NOT NULL")
            elif constraint.constraint_type == ColumnConstraintType.UNIQUE:
                constraint_parts.append("UNIQUE")
            elif constraint.constraint_type == ColumnConstraintType.DEFAULT:
                if constraint.default_value is None:
                    # An explicit DEFAULT constraint without a value means
                    # DEFAULT NULL; it must not be silently dropped.
                    constraint_parts.append("DEFAULT NULL")
                else:
                    from rhosocial.activerecord.backend.expression import bases
                    if isinstance(constraint.default_value, bases.BaseExpression):
                        default_sql, default_params = constraint.default_value.to_sql()
                        constraint_parts.append(f"DEFAULT {default_sql}")
                        params.extend(default_params)
                    elif isinstance(constraint.default_value, str):
                        escaped = self._escape_sql_string(constraint.default_value)
                        constraint_parts.append(f"DEFAULT '{escaped}'")
                    else:
                        constraint_parts.append(f"DEFAULT {constraint.default_value}")
            elif constraint.constraint_type == ColumnConstraintType.NULL:
                constraint_parts.append("NULL")
            elif constraint.constraint_type == ColumnConstraintType.COLLATE:
                if constraint.collation:
                    if self.version >= (12, 2, 0):
                        constraint_parts.append(
                            f"COLLATE {self.format_identifier(constraint.collation)}"
                        )
                    else:
                        from rhosocial.activerecord.backend.dialect.exceptions import (
                            UnsupportedFeatureError,
                        )
                        raise UnsupportedFeatureError(
                            self.name, "column-level COLLATE (requires Oracle 12.2+)"
                        )

            if constraint.is_auto_increment:
                if self.version >= (12, 0, 0):
                    identity_clause = "GENERATED BY DEFAULT AS IDENTITY"

        if identity_clause is not None:
            constraint_parts.insert(0, identity_clause)

        if constraint_parts:
            parts.append(" ".join(constraint_parts))

        return " ".join(parts), tuple(params)

    def format_table_constraint(
        self,
        t_const: "TableConstraint",
        TableConstraintType,
    ) -> Tuple[str, List[Any]]:
        parts: List[str] = []
        params: List[Any] = []
        if t_const.name:
            parts.append(f"CONSTRAINT {self.format_identifier(t_const.name)}")

        if t_const.constraint_type == TableConstraintType.PRIMARY_KEY:
            if t_const.columns:
                cols_str = ", ".join(self.format_identifier(c) for c in t_const.columns)
                parts.append(f"PRIMARY KEY ({cols_str})")
        elif t_const.constraint_type == TableConstraintType.UNIQUE:
            if t_const.columns:
                cols_str = ", ".join(self.format_identifier(c) for c in t_const.columns)
                parts.append(f"UNIQUE ({cols_str})")
        elif t_const.constraint_type == TableConstraintType.FOREIGN_KEY:
            if t_const.columns and t_const.foreign_key_table and t_const.foreign_key_columns:
                cols_str = ", ".join(self.format_identifier(c) for c in t_const.columns)
                ref_cols_str = ", ".join(
                    self.format_identifier(c) for c in t_const.foreign_key_columns
                )
                ref_table = self.format_identifier(t_const.foreign_key_table)
                parts.append(
                    f"FOREIGN KEY ({cols_str}) REFERENCES {ref_table} ({ref_cols_str})"
                )

        return " ".join(parts), tuple(params)