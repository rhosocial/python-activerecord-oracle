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

    def supports_check_constraint(self) -> bool:
        """Oracle supports CHECK constraints (12c+)."""
        return True

    def format_create_table_statement(
        self, expr: "CreateTableExpression"
    ) -> Tuple[str, tuple]:
        """Format CREATE TABLE statement for Oracle.

        Oracle has no ``CREATE TABLE IF NOT EXISTS`` syntax. When
        ``if_not_exists`` is True, the statement is wrapped in an anonymous
        PL/SQL block that checks ``user_tables`` and only executes the DDL
        when the table does not exist, making creation idempotent.

        Raises:
            TypeError: ``expr.table`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Table`.
                Any other catalogue object carries its own ``format_method``, so
                it would render its own name here and the statement would
                silently create a table named after, say, an index.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.objects import Table

        if not isinstance(expr.table, Table):
            raise TypeError(
                f"CreateTableExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        table_options = getattr(expr, "table_options", None)
        if getattr(table_options, "comment", None):
            # Oracle annotates comments through the standalone COMMENT ON
            # statement (no inline table option); a comment on the table
            # options is never silently dropped.
            raise UnsupportedFeatureError(
                self.name, "TABLE COMMENT",
                "Oracle has no inline table comment; use a standalone "
                "COMMENT ON TABLE statement.",
            )
        if expr.inherits:
            # Oracle has no table inheritance; a declared INHERITS clause is
            # never silently dropped.
            raise UnsupportedFeatureError(
                self.name, "table INHERITS",
                "Oracle does not support table inheritance.",
            )
        if expr.storage_options is not None:
            # Oracle has no generic WITH (...) storage options clause.
            raise UnsupportedFeatureError(
                self.name, "storage options",
                "Oracle does not support the generic WITH (...) storage "
                "options clause.",
            )
        all_params: List[Any] = []
        parts = ["CREATE"]
        if expr.temporary:
            parts.append("GLOBAL TEMPORARY")
        parts.append("TABLE")
        # The table is a catalogue object on the expression, so it is rendered
        # by its own protocol -- an owner, when the expression carries one, is
        # quoted by the same rules as everywhere else in the backend.
        parts.append(expr.table.to_sql()[0])

        column_parts: List[str] = []
        for col_def in expr.columns:
            col_sql, col_params = self.format_column_definition(col_def)
            column_parts.append(col_sql)
            all_params.extend(col_params)

        for t_const in expr.table_constraints:
            const_sql, const_params = self.format_table_constraint(t_const)
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
            table_upper = expr.table.name.upper()
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
    ) -> Tuple[str, tuple]:
        """Format a single column definition with Oracle-specific syntax.

        Accepts both the generic ``ColumnDefinition`` and the Oracle
        ``OracleColumnDefinition``; the latter's ``invisible`` attribute is
        rendered here.
        """
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            ColumnConstraintType,
        )
        from rhosocial.activerecord.backend.impl.oracle.expression.column import (
            OracleColumnDefinition,
        )
        if getattr(col_def, "comment", None):
            # Oracle annotates column comments through the standalone
            # COMMENT ON COLUMN statement (no inline column clause); a
            # comment on a column definition is never silently dropped.
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(
                self.name, "COLUMN COMMENT",
                "Oracle has no inline column comment; use a standalone "
                "COMMENT ON COLUMN statement.",
            )
        type_sql, type_params = col_def.data_type.to_sql()
        parts = [self.format_identifier(col_def.name), type_sql]
        params: List[Any] = list(type_params)

        # Oracle's grammar is
        #   col type [VISIBLE|INVISIBLE]
        #            [GENERATED ... AS IDENTITY [(identity_options)]]
        #            [inline_constraint ...]
        # so INVISIBLE comes immediately after the type, the attribute channel
        # (identity, collation) before the inline constraints, and the
        # constraints last. This matches core's default attribute-first order;
        # the previous constraints-first order rendered ``PRIMARY KEY
        # GENERATED ...`` and ``... AS IDENTITY INVISIBLE``, both of which the
        # measured servers refuse.
        if isinstance(col_def, OracleColumnDefinition) and col_def.invisible:
            parts.append("INVISIBLE")

        attr_sql, attr_params = self.format_column_attributes(col_def)
        if attr_sql:
            parts.append(attr_sql.strip())
        params.extend(attr_params)

        constraint_parts: List[str] = []
        identity_clause: Optional[str] = None
        identity_params: tuple = ()

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

            if constraint.is_auto_increment and identity_clause is None:
                # The legacy marker is the SQL-standard identity clause in its
                # default generation mode, routed through the gated formatter
                # so the version boundary lives in one place: Oracle < 12
                # refuses by name instead of silently dropping the clause.
                # The clause renders in the attribute position, before the
                # inline constraints.
                from rhosocial.activerecord.backend.expression.statements import (
                    IdentityClause,
                )
                identity_clause, identity_params = self.format_identity_clause(
                    IdentityClause(self, "BY DEFAULT")
                )

        if identity_clause is not None:
            parts.append(identity_clause.strip())
            params.extend(identity_params)

        if constraint_parts:
            parts.append(" ".join(constraint_parts))

        if col_def.generated_expression is not None:
            gen_sql, gen_params = col_def.generated_expression.to_sql()
            parts.append(gen_sql.lstrip())
            params.extend(gen_params)

        return " ".join(parts), tuple(params)

    def supports_foreign_key_on_delete(self) -> bool:
        """Oracle supports FOREIGN KEY ON DELETE."""
        return True

    def supports_foreign_key_on_update(self) -> bool:
        """Oracle does not support FOREIGN KEY ON UPDATE."""
        return False

    def supports_fk_match(self) -> bool:
        """Oracle does not support FOREIGN KEY MATCH."""
        return False

    def format_table_constraint(
        self,
        t_const: "TableConstraint",
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            ForeignKeyConstraint,
            ReferentialAction,
            TableConstraintType,
        )
        parts: List[str] = []
        params: List[Any] = []
        if t_const.name:
            parts.append(f"CONSTRAINT {self.format_identifier(t_const.name)}")

        if getattr(t_const, "match_type", None) is not None:
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(
                self.name, "FOREIGN KEY MATCH",
                "Oracle does not support FOREIGN KEY MATCH.",
            )

        if t_const.constraint_type == TableConstraintType.PRIMARY_KEY:
            if t_const.columns:
                cols_str = ", ".join(self.format_identifier(c) for c in t_const.columns)
                parts.append(f"PRIMARY KEY ({cols_str})")
        elif t_const.constraint_type == TableConstraintType.UNIQUE:
            if t_const.columns:
                cols_str = ", ".join(self.format_identifier(c) for c in t_const.columns)
                parts.append(f"UNIQUE ({cols_str})")
        elif t_const.constraint_type == TableConstraintType.CHECK:
            if t_const.check_condition is not None:
                if not self.supports_check_constraint():
                    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
                    raise UnsupportedFeatureError(
                        self.name, "CHECK constraint",
                        f"{self.name} does not support CHECK constraints."
                    )
                check_sql, check_params = t_const.check_condition.to_sql()
                parts.append(f"CHECK ({check_sql})")
                params.extend(check_params)
        elif t_const.constraint_type == TableConstraintType.FOREIGN_KEY:
            if t_const.columns and t_const.foreign_key_table and t_const.foreign_key_columns:
                cols_str = ", ".join(self.format_identifier(c) for c in t_const.columns)
                ref_cols_str = ", ".join(
                    self.format_identifier(c) for c in t_const.foreign_key_columns
                )
                # The referenced table is a Table object, so it renders through
                # the same path as every other named relation -- which is what
                # lets it carry its own owner.
                ref_table = t_const.foreign_key_table.to_sql()[0]
                parts.append(
                    f"FOREIGN KEY ({cols_str}) REFERENCES {ref_table} ({ref_cols_str})"
                )
                if isinstance(t_const, ForeignKeyConstraint):
                    if t_const.on_delete and t_const.on_delete != ReferentialAction.NO_ACTION:
                        if not self.supports_foreign_key_on_delete():
                            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
                            raise UnsupportedFeatureError(
                                self.name, "FOREIGN KEY ON DELETE",
                                f"{self.name} does not support ON DELETE for foreign keys."
                            )
                        parts.append(f"ON DELETE {t_const.on_delete.value}")
                    if t_const.on_update and t_const.on_update != ReferentialAction.NO_ACTION:
                        if not self.supports_foreign_key_on_update():
                            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
                            raise UnsupportedFeatureError(
                                self.name, "FOREIGN KEY ON UPDATE",
                                f"{self.name} does not support ON UPDATE for foreign keys."
                            )
                        parts.append(f"ON UPDATE {t_const.on_update.value}")

        deferrable = getattr(t_const, "deferrable", False)
        not_deferrable = getattr(t_const, "not_deferrable", False)
        initially_deferred = getattr(t_const, "initially_deferred", False)
        initially_immediate = getattr(t_const, "initially_immediate", False)
        enforced = getattr(t_const, "enforced", False)
        not_enforced = getattr(t_const, "not_enforced", False)

        if deferrable or not_deferrable or initially_deferred or initially_immediate:
            if not self.supports_deferrable_constraint():
                from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
                raise UnsupportedFeatureError(
                    self.name,
                    "constraint deferrability",
                    f"{self.name} does not support DEFERRABLE / INITIALLY "
                    "constraint state.",
                )
        if deferrable or not_deferrable:
            parts.append("DEFERRABLE" if deferrable else "NOT DEFERRABLE")
        # INITIALLY ... is an independent constraint attribute in the grammar;
        # it is rendered whenever requested, not only alongside DEFERRABLE.
        if initially_deferred or initially_immediate:
            parts.append(
                "INITIALLY DEFERRED"
                if initially_deferred
                else "INITIALLY IMMEDIATE"
            )
        if enforced or not_enforced:
            if not self.supports_constraint_enforced():
                from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
                feature = (
                    "constraint ENFORCED"
                    if enforced
                    else "constraint NOT ENFORCED"
                )
                raise UnsupportedFeatureError(
                    self.name,
                    feature,
                    f"{self.name} has no ENFORCED / NOT ENFORCED constraint "
                    "spelling; measured on 18c/21c/23c.",
                )
            parts.append("ENFORCED" if enforced else "NOT ENFORCED")

        return " ".join(parts), tuple(params)