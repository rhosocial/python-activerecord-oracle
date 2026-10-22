# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl.py
"""Oracle DDL formatting mixin (CREATE TABLE, column defs, table constraints)."""

from typing import Any, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateTableAsStatement, CreateTableStatement, ColumnDefinition, TableConstraint,
    )


class OracleDDLMixin:
    """CREATE TABLE / column-def / table-constraint formatting for Oracle.

    Provides the ``format_create_table_statement`` entry point together
    with the private helpers ``format_column_definition`` and
    ``format_table_constraint`` that the dialect previously
    inlined in its monolithic file.
    """

    def supports_check_constraint(self) -> bool:
        """Whether CHECK constraints are supported *and enforced*. True.

        Measured on 21c XE 21.3.0.0.0 (18c and 23c agree): ``ALTER TABLE t ADD
        CONSTRAINT cc CHECK (v > 0)`` is accepted, ``user_constraints``
        records the ``C`` row as ``ENABLED``, and a violating INSERT is
        refused with ``ORA-02290: check constraint (...) violated``. So it is
        enforced, not merely accepted -- which is the distinction the probe
        asks about.

        The same measurement also settles the deferral question for CHECK:
        ``... CHECK (v > 0) DEFERRABLE INITIALLY DEFERRED`` is accepted, and
        the failure then moves to COMMIT as ``ORA-02091`` wrapping
        ``ORA-02290``. See ``OracleFeaturesMixin.supports_deferrable_constraint``
        and its per-kind siblings.
        """
        return True

    def format_create_table_as_statement(
        self, expr: "CreateTableAsStatement"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE TABLE ... AS`` for Oracle.

        Oracle's DDL statements never carry bind variables. The driver
        refuses such a statement before the server sees it -- "DPY-4009: 0
        positional bind values are required but 1 were provided" -- so the
        request is refused by name, with the way out named, instead of being
        rendered for the driver to reject.

        The body of a CTAS is executed once, so a bound value would have
        been meaningful where the engine accepts it; Oracle is the engine
        that does not. Build the query with an inlined literal instead.
        """
        sql, params = super().format_create_table_as_statement(expr)
        if params:
            raise UnsupportedFeatureError(
                self.name,
                "CREATE TABLE ... AS with bound parameters",
                suggestion=(
                    "Oracle's DDL statements never carry bind variables; the "
                    "driver refuses the statement before the server sees it "
                    "('DPY-4009: 0 positional bind values are required but 1 "
                    "were provided'). Build the query with an inlined literal "
                    "instead."
                ),
            )
        return sql, params

    def format_create_table_statement(
        self, expr: "CreateTableStatement"
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
                f"CreateTableStatement.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        table_options = expr.table_options
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
            to = expr.table_options
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

        Each inline constraint is rendered by its own branch formatter, reached
        through :meth:`format_column_constraint`, so a kind Oracle spells
        differently (or refuses by name) is decided in one place rather than
        re-derived from a tag. The one Oracle-specific piece that stays here is
        the identity clause: ``PRIMARY KEY`` with ``is_auto_increment`` is
        Oracle's ``GENERATED BY DEFAULT AS IDENTITY``, and the grammar puts it
        in the attribute position, ahead of the inline constraints.
        """
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnPrimaryKeyConstraint,
            IdentityClause,
        )
        from rhosocial.activerecord.backend.impl.oracle.expression.column import (
            OracleColumnDefinition,
        )
        if col_def.comment:
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
            constraint_sql, constraint_params = self.format_column_constraint(constraint)
            if constraint_sql.strip():
                constraint_parts.append(constraint_sql.strip())
                params.extend(constraint_params)

            if (
                isinstance(constraint.branch, ColumnPrimaryKeyConstraint)
                and constraint.branch.is_auto_increment
                and identity_clause is None
            ):
                # The legacy marker is the SQL-standard identity clause in its
                # default generation mode, routed through the gated formatter
                # so the version boundary lives in one place: Oracle < 12
                # refuses by name instead of silently dropping the clause.
                # The clause renders in the attribute position, before the
                # inline constraints.
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
        """``ON DELETE`` is accepted, but only for CASCADE and SET NULL.

        Measured on 21c XE (18c and 23c agree): the inline
        ``col NUMBER REFERENCES r(id) ON DELETE CASCADE`` and
        ``... ON DELETE SET NULL`` are accepted and the dictionary records
        ``delete_rule = 'CASCADE'`` / ``'SET NULL'``; ``ON DELETE RESTRICT``
        and ``ON DELETE NO ACTION`` are refused with ``ORA-00905: missing
        keyword`` (23c: ``ORA-02000: missing CASCADE keyword``) and
        ``ON DELETE SET DEFAULT`` with ``ORA-03001: unimplemented feature``.
        Oracle's ``references_clause`` has exactly one action branch,
        ``[ ON DELETE { CASCADE | SET NULL } ]``.

        This probe answers the coarse question, so it stays ``True``; the
        per-action narrowing lives on the referential-action probes declared
        in ``features.py``, and the ``format_table_constraint`` path below
        still refuses any action the backend cannot spell.
        """
        return True

    def supports_foreign_key_on_update(self) -> bool:
        """Oracle does not support FOREIGN KEY ``ON UPDATE``.

        There is no ``ON UPDATE`` branch in ``references_clause`` at all.
        Measured on 18c / 21c XE / 23c: ``ON UPDATE CASCADE`` and
        ``ON UPDATE RESTRICT`` are refused at the ``UPDATE`` keyword with
        ``ORA-00905: missing keyword`` on 18c/21c and ``ORA-02000: missing
        DELETE keyword`` on 23c.
        """
        return False

    def supports_fk_match(self) -> bool:
        """Oracle does not support FOREIGN KEY ``MATCH``.

        Measured on 21c XE: inline ``MATCH FULL`` / ``MATCH SIMPLE`` /
        ``MATCH PARTIAL`` are each refused with ``ORA-00907: missing right
        parenthesis``, and the out-of-line form with ``ORA-01735: invalid
        ALTER TABLE option``.  Same on 18c and 23c.
        """
        return False

    def format_table_constraint(
        self,
        t_const: "TableConstraint",
    ) -> Tuple[str, tuple]:
        """Format one out-of-line (table-level) constraint for Oracle.

        Oracle's out-of-line shape is ``CONSTRAINT name <kind> (cols)
        [constraint_state]``, where the state clauses are DEFERRABLE /
        INITIALLY, RELY, USING INDEX, ENABLE / DISABLE and VALIDATE /
        NOVALIDATE -- not ENFORCED (see the constraint probes on
        :class:`OracleFeaturesMixin`).

        The production-level rendering -- the ``CONSTRAINT <name>`` prefix,
        the branch's own body, the ``DEFERRABLE`` / ``INITIALLY`` trait and
        the referential actions -- is core's and is delegated to unchanged.
        The branch says which kind this is, so nothing here switches on a tag.
        What stays here is the part that is Oracle's rather than the
        production's: the deferral and enforcement traits are refused by name
        here, each spelling under its own name, because the pair is consumed
        one side at a time and core's combined message would not say which
        side was asked for. Refusing before the delegation is also the only
        way a refusal reaches the caller: core's
        ``DDLColumnMixin.format_table_constraint`` raises
        ``UnsupportedFeatureError`` in its deferral branch without importing
        the name first (a defect introduced by core's S3 commit
        ``8448dfb5``), so a delegated refusal raises ``UnboundLocalError``
        instead. A dialect whose probe answers ``True`` -- Oracle's does --
        is not refused here and reaches core's rendering.

        Keep the emitted clause sequence short.  Measured on 21c XE 21.3.0.0.0:
        a combination can be refused even when every clause in it is accepted
        alone.  On the ADD path::

            ALTER TABLE t ADD CONSTRAINT c CHECK (v > 1)
                NOVALIDATE RELY DEFERRABLE INITIALLY IMMEDIATE
            -> ORA-01735: invalid ALTER TABLE option     (fails at the RELY token)

        although each piece on its own is accepted -- ``NOVALIDATE`` alone,
        ``DEFERRABLE INITIALLY IMMEDIATE`` alone, and ``ALTER TABLE t MODIFY
        CONSTRAINT c RELY`` all succeed.  A pre-fix pass saw the same trap
        phrased as ``ORA-00905: missing keyword`` at ``RELY`` when the clauses
        were issued after ``ENABLE``; either way the failure lands at ``RELY``,
        and the rule is the same.  **Do not concatenate state clauses here.**
        Render the deferral and the validation state, then let the caller
        issue any further state change as its own ``ALTER TABLE``.  This is
        the one trap that belongs in a code comment rather than a docstring:
        nothing about the individual clauses hints at it, so a future edit
        that "helpfully" renders every requested state together would pass
        review and fail only against a live server.  Delegating the rendering
        keeps that promise: core emits the deferral trait and the enforcement
        trait only.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )

        wants_deferral = (
            t_const.deferrable
            or t_const.not_deferrable
            or t_const.initially_deferred
            or t_const.initially_immediate
        )
        if wants_deferral and not self.supports_deferrable_constraint():
            if t_const.deferrable or t_const.not_deferrable:
                feature = (
                    "constraint DEFERRABLE"
                    if t_const.deferrable
                    else "constraint NOT DEFERRABLE"
                )
            else:
                feature = (
                    "constraint INITIALLY DEFERRED"
                    if t_const.initially_deferred
                    else "constraint INITIALLY IMMEDIATE"
                )
            raise UnsupportedFeatureError(
                self.name,
                feature,
                f"{self.name} does not support {feature}.",
            )

        if (t_const.enforced or t_const.not_enforced) and not self.supports_constraint_enforced():
            feature = "constraint ENFORCED" if t_const.enforced else "constraint NOT ENFORCED"
            raise UnsupportedFeatureError(
                self.name,
                feature,
                f"{self.name} does not support {feature}; measured on 18c/21c/23c.",
            )
        return super().format_table_constraint(t_const)