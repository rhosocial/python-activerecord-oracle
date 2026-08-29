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
    with the private helpers ``_format_column_definition_oracle`` and
    ``_format_table_constraint_oracle`` that the dialect previously
    inlined in its monolithic file.
    """

    def format_create_table_statement(
        self, expr: "CreateTableExpression"
    ) -> Tuple[str, tuple]:
        """Format CREATE TABLE statement for Oracle."""
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
            col_sql, col_params = self._format_column_definition_oracle(
                col_def, ColumnConstraintType
            )
            column_parts.append(col_sql)
            all_params.extend(col_params)

        for t_const in expr.table_constraints:
            const_sql, const_params = self._format_table_constraint_oracle(
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

        return " ".join(parts), tuple(all_params)

    # ----------------------------------------------------------------
    # Private helpers
    # ----------------------------------------------------------------
    def _format_column_definition_oracle(
        self,
        col_def: "ColumnDefinition",
        ColumnConstraintType,
    ) -> Tuple[str, List[Any]]:
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

        return " ".join(parts), params

    def _format_table_constraint_oracle(
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

        return " ".join(parts), params