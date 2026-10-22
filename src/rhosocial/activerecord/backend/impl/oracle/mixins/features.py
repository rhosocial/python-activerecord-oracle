# src/rhosocial/activerecord/backend/impl/oracle/mixins/features.py
"""Oracle feature-capability mixin.

Collects remaining ``supports_*`` capability switches that are not
already covered by a dedicated per-domain mixin.  Follows the pattern
of ``PostgresFeaturesMixin``.
"""

from typing import Dict


class OracleFeaturesMixin:
    def _ddl_bind_parameter_reason(self, feature: str) -> str:
        """Why Oracle refuses bind parameters in a DDL body.

        Oracle's DDL statements never take bind variables, and the driver
        refuses the statement before the server sees it (measured on
        18c / 21c / 26ai).
        """
        return (
            "Oracle's DDL statements never carry bind variables; the driver "
            "refuses the statement before the server sees it ('DPY-4009: 0 "
            "positional bind values are required but 1 were provided')."
        )

    """Aggregated Oracle feature-capability checks.

    Each domain-specific mixin (``OraclePivotMixin``, etc.) carries its
    own ``supports_*`` methods; this mixin gathers the cross-cutting
    checks that do not warrant a separate domain file.
    """

    # --- XML ----------------------------------------------------------
    def supports_xmlparse(self) -> bool:
        """Whether SQL/XML XMLPARSE is supported.

        Oracle's SQL Reference first lists ``XMLPARSE`` in 10.2; it is absent
        from the XML function lists of 9.2 and 10.1, and 10g Release 2 is the
        release whose SQL/XML coverage expands to the SQL:2005 set
        (``XMLPARSE``, ``XMLSERIALIZE``, ``XMLQUERY``, ``XMLTABLE``). So the
        floor is 10.2 rather than the 9.0 the older construction probes use.

        Measured live: the core rendering ``XMLPARSE(DOCUMENT ?)`` and
        ``XMLPARSE(CONTENT ?)`` executes on 18c, 21c and 23c — an
        ``XMLSERIALIZE`` around the parse returns the document text. The
        standard's ``PRESERVE WHITESPACE`` clause is *not* Oracle grammar: it
        raises ORA-03001 "unimplemented feature" on all three, so a caller
        asking for ``preserve_whitespace=True`` gets that server error. The
        function itself, including Oracle's ``WELLFORMED`` extension, is
        supported.
        """
        return self.version >= (10, 2, 0)

    def supports_xmlserialize(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlelement(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlattributes(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlforest(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlconcat(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlcomment(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlpi(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlroot(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlagg(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_xmlquery(self) -> bool:
        return self.version >= (9, 2, 0)

    def supports_xmlexists(self) -> bool:
        return self.version >= (9, 2, 0)

    def supports_xmltable(self) -> bool:
        return self.version >= (9, 2, 0)

    # --- Collation ----------------------------------------------------
    def supports_collate_expression(self) -> bool:
        return self.version >= (12, 2, 0)

    # --- CTE ----------------------------------------------------------
    def supports_basic_cte(self) -> bool:
        return self.version >= (9, 0, 0)

    def supports_recursive_cte(self) -> bool:
        return self.version >= (11, 2, 0)

    def supports_materialized_cte(self) -> bool:
        """Oracle spells CTE materialization as the ``/*+ MATERIALIZE */`` hint.

        The SQL-standard ``AS MATERIALIZED`` / ``AS NOT MATERIALIZED`` spelling
        this parameter pair names is rejected by the server (ORA-00906 on
        18c/21c/23c), and the hint has no expression-level parameter, so the
        request is refused by name instead of being rendered.
        """
        return False

    # --- RETURNING ----------------------------------------------------
    def supports_returning_insert(self) -> bool:
        return True

    def supports_returning_update(self) -> bool:
        return True

    def supports_returning_delete(self) -> bool:
        return True

    def supports_returning_expressions(self) -> bool:
        return False

    def supports_returning_wildcard(self) -> bool:
        return False

    def supports_returning_single_row(self) -> bool:
        return True

    # --- Window functions ---------------------------------------------
    def supports_window_functions(self) -> bool:
        return self.version >= (8, 0, 0)

    def supports_window_frame_clause(self) -> bool:
        return True

    # --- FILTER -------------------------------------------------------
    def supports_filter_clause(self) -> bool:
        return False

    # --- JSON ---------------------------------------------------------
    def supports_json_type(self) -> bool:
        return self.version >= (21, 0, 0)

    def get_json_access_operator(self) -> str:
        return "."

    def supports_json_table(self) -> bool:
        return self.version >= (12, 0, 0)

    def supports_native_json(self) -> bool:
        return self.version >= (21, 0, 0)

    # --- Advanced grouping --------------------------------------------
    def supports_rollup(self) -> bool:
        return True

    def supports_cube(self) -> bool:
        return True

    def supports_grouping_sets(self) -> bool:
        return True

    # --- Array --------------------------------------------------------
    def supports_array_type(self) -> bool:
        return True

    def supports_array_constructor(self) -> bool:
        return True

    def supports_array_access(self) -> bool:
        return True

    # --- EXPLAIN ------------------------------------------------------
    def supports_explain_analyze(self) -> bool:
        """Oracle does not support EXPLAIN ANALYZE in the standard sense."""
        return False

    def supports_explain_format(self, format_type: str) -> bool:
        """Oracle EXPLAIN PLAN writes rows to PLAN_TABLE; format options are not part of the SQL grammar."""
        return False

    # --- Locking ------------------------------------------------------
    def supports_for_update(self) -> bool:
        """Oracle does support SELECT ... FOR UPDATE natively, but the
        ActiveRecord query builder emits composite SELECT shapes (with
        DISTINCT / GROUP BY) that Oracle rejects for FOR UPDATE
        (ORA-02014)."""
        return False

    # --- DQL: ordering / pagination -----------------------------------
    def supports_nulls_first_last(self) -> bool:
        """Oracle supports explicit NULLS FIRST / NULLS LAST ordering."""
        return True

    def supports_fetch_with_ties(self) -> bool:
        """Oracle supports FETCH FIRST ... WITH TIES since 12c."""
        return self.version >= (12, 0, 0)

    # --- Identity / Auto-increment columns ----------------------------
    def supports_identity_column(self) -> bool:
        """Oracle accepts ``GENERATED ... AS IDENTITY`` from 12c onward."""
        return self.version >= (12, 0, 0)

    def supports_identity_generation_always(self) -> bool:
        """Oracle can express ``GENERATED ALWAYS AS IDENTITY``."""
        return True

    def supports_identity_start(self) -> bool:
        """Oracle accepts the ``START WITH`` identity option."""
        return True

    def supports_identity_increment(self) -> bool:
        """Oracle accepts the ``INCREMENT BY`` identity option."""
        return True

    def supports_identity_minvalue(self) -> bool:
        """Oracle accepts the ``MINVALUE`` identity option."""
        return True

    def supports_identity_maxvalue(self) -> bool:
        """Oracle accepts the ``MAXVALUE`` identity option."""
        return True

    def supports_identity_cycle(self) -> bool:
        """Oracle accepts the ``CYCLE`` / ``NOCYCLE`` identity option.

        Measured on 18c / 21c / 23c: both spellings are accepted inside an
        identity clause. A bare ``CYCLE`` additionally needs a ``MAXVALUE``
        (ORA-04015), which is a value constraint, not a missing clause. The
        negative spelling is Oracle's own and comes from
        :meth:`identity_cycle_keyword`: core's SQL-standard ``NO CYCLE`` is
        refused (ORA-02000) on every measured server.
        """
        return True

    def supports_identity_order(self) -> bool:
        """Oracle accepts the ``ORDER`` / ``NOORDER`` identity option.

        Measured on 18c / 21c / 23c: both spellings are accepted inside an
        identity clause, so the option is expressible; the negative form is
        spelled by :meth:`identity_order_keyword`.
        """
        return True

    def supports_identity_cache(self) -> bool:
        """Oracle accepts the ``CACHE n`` / ``NOCACHE`` identity option.

        Measured on 18c / 21c / 23c: ``CACHE 10`` and ``NOCACHE`` are both
        accepted inside an identity clause. ``CACHE 0`` is refused
        (ORA-04010); core refuses a non-positive ``cache`` at construction
        and spells NO CACHE through :meth:`identity_no_cache_keyword`, so a
        count that cannot be expressed never reaches this hook.
        """
        return True

    def identity_cycle_keyword(self, cycle: bool) -> str:
        """Oracle spells the negative cycle form ``NOCYCLE``.

        Core's SQL-standard ``NO CYCLE`` is refused (ORA-02000) on every
        measured server; ``NOCYCLE`` is accepted. This answers spelling only:
        the gate stays in core's formatter, which is not overridden here.
        """
        return "CYCLE" if cycle else "NOCYCLE"

    def identity_order_keyword(self, order: bool) -> str:
        """Oracle spells the negative order form ``NOORDER``.

        Core's SQL-standard ``NO ORDER`` is refused (ORA-02000);
        ``NOORDER`` is accepted.
        """
        return "ORDER" if order else "NOORDER"

    def identity_cache_keyword(self, cache: int) -> str:
        """Oracle spells the positive cache form ``CACHE n``.

        Called only for a positive count: core refuses ``cache=0`` at
        construction and routes the explicit NO CACHE spelling through
        :meth:`identity_no_cache_keyword`.
        """
        return f"CACHE {cache}"

    def identity_no_cache_keyword(self) -> str:
        """Oracle spells the negative cache form ``NOCACHE``.

        Core's SQL-standard ``NO CACHE`` is refused (ORA-02000); ``NOCACHE``
        is accepted on every measured server. This is the hook for the
        negative spelling -- the positive count hook no longer carries it.
        """
        return "NOCACHE"

    def supports_auto_increment_column(self) -> bool:
        """Oracle has no ``AUTO_INCREMENT`` marker; identity is its mechanism.

        ``AUTO_INCREMENT`` is a different mechanism from the parameterised
        ``GENERATED ... AS IDENTITY`` clause -- it is parameterless, and its
        seed is a table-level option -- and Oracle spells neither. The answer
        is ``False``; this replaces the old ``supports_auto_increment``, whose
        version check answered a question no renderer asked.
        """
        return False

    def supports_column_collation(self) -> bool:
        """Oracle supports column-level COLLATE since 12.2."""
        return self.version >= (12, 2, 0)

    def supports_generated_columns(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_stored_generated_columns(self) -> bool:
        return self.supports_generated_columns()

    def supports_virtual_generated_columns(self) -> bool:
        return self.supports_generated_columns()

    # --- Graph --------------------------------------------------------
    def supports_graph_match(self) -> bool:
        return self.version >= (12, 0, 0)

    def supports_graph_table(self) -> bool:
        return self.version >= (23, 0, 0)

    # --- MERGE --------------------------------------------------------
    def supports_merge_statement(self) -> bool:
        return self.version >= (9, 0, 0)

    # --- Temporal tables ----------------------------------------------
    def supports_temporal_tables(self) -> bool:
        # Oracle has no SQL:2011 system-versioned temporal tables.
        return False

    # --- QUALIFY ------------------------------------------------------
    def supports_qualify_clause(self) -> bool:
        return False

    # --- UPSERT -------------------------------------------------------
    def supports_upsert(self) -> bool:
        return True

    def get_upsert_syntax_type(self) -> str:
        return "MERGE"

    def supports_on_conflict_clause(self) -> bool:
        """Oracle has no ON CONFLICT clause form; upsert is expressed via MERGE."""
        return False

    def supports_multiple_on_conflict_clauses(self) -> bool:
        return False

    # --- LATERAL ------------------------------------------------------
    def supports_lateral_join(self) -> bool:
        return self.version >= (12, 0, 0)

    # --- Ordered-set aggregation --------------------------------------
    def supports_ordered_set_aggregation(self) -> bool:
        return True

    # --- Joins --------------------------------------------------------
    def supports_inner_join(self) -> bool:
        return True

    def supports_left_join(self) -> bool:
        return True

    def supports_right_join(self) -> bool:
        return True

    def supports_full_join(self) -> bool:
        return True

    def supports_cross_join(self) -> bool:
        return True

    def supports_natural_join(self) -> bool:
        return True

    def supports_wildcard(self) -> bool:
        return True

    # --- Constraint ---------------------------------------------------
    # Every value in this section was measured against live Oracle servers
    # (18c, 21c XE 21.3.0.0.0 and 23c); where a version boundary is not named
    # the answer holds on all three.  The exact ORA- code is quoted so the
    # next reader does not have to re-derive it, and so that "Oracle accepts
    # X" is never confused with "Oracle enforces X".
    #
    # Oracle's own grammar for this surface is::
    #
    #   inline_constraint     ::= [ CONSTRAINT name ] { ... } [ constraint_state ]
    #   out_of_line_constraint::= [ CONSTRAINT name ] { ... } [ constraint_state ]
    #   constraint_state      ::= [ [ [NOT] DEFERRABLE
    #                                  [ INITIALLY { IMMEDIATE | DEFERRED } ] ]
    #                              | INITIALLY { ... } [ [NOT] DEFERRABLE ] ]
    #                            [ RELY | NORELY ] [ using_index_clause ]
    #                            [ ENABLE | DISABLE ] [ VALIDATE | NOVALIDATE ]
    #   references_clause     ::= REFERENCES [schema.]object [(col [,col]...)]
    #                            [ ON DELETE { CASCADE | SET NULL } ]
    #
    # which is why the answers below differ from the SQL-standard ones: the
    # state clauses Oracle *does* have are ENABLE/DISABLE and VALIDATE/
    # NOVALIDATE, not ENFORCED; and its referential actions are two, not five.
    #
    # The five constraint-kind probes are declared here rather than inherited
    # from the shared mixin.  Oracle's answer happens to agree with the shared
    # default for all five, so nothing breaks today -- but these are the
    # switches that decide whether a ``ColumnConstraint`` / ``TableConstraint``
    # of that kind is rendered or refused, and a moved core default must not be
    # able to move Oracle's contract silently.

    def supports_primary_key_constraint(self) -> bool:
        """Whether PRIMARY KEY constraints are supported. True.

        Measured on 21c XE 21.3.0.0.0, both levels: ``ALTER TABLE t ADD
        CONSTRAINT cp PRIMARY KEY (id)`` and ``CREATE TABLE ct (a NUMBER
        PRIMARY KEY)`` are both accepted, and ``user_constraints`` records
        the ``P`` row.
        """
        return True

    def supports_unique_constraint(self) -> bool:
        """Whether UNIQUE constraints are supported. True.

        Measured on 21c XE: ``ALTER TABLE t ADD CONSTRAINT cu UNIQUE (a)``
        and ``CREATE TABLE ct (a NUMBER UNIQUE)`` are both accepted.
        """
        return True

    def supports_not_null_constraint(self) -> bool:
        """Whether NOT NULL is supported as a constraint. True.

        Measured on 21c XE: inline ``CREATE TABLE ct (a NUMBER NOT NULL)`` and
        ``ALTER TABLE t MODIFY (a NOT NULL)`` are both accepted.  Oracle
        records a NOT NULL as a system-named ``C`` row in
        ``user_constraints``, which is what makes it reachable by the
        ``VALIDATE`` / ``NOVALIDATE`` paths too.
        """
        return True

    def supports_foreign_key_constraint(self) -> bool:
        """Whether FOREIGN KEY constraints are supported. True.

        Measured on 21c XE: ``ALTER TABLE t ADD CONSTRAINT cf FOREIGN KEY
        (a) REFERENCES p(id)`` is accepted, and an orphan child row is
        refused with ``ORA-02291: integrity constraint (...) violated -
        parent key not found`` (measured while probing the deferrable
        variant, which is why it is quoted rather than assumed).
        """
        return True

    def supports_fk_on_update(self) -> bool:
        """Oracle has no ``ON UPDATE`` referential action at all.

        ``references_clause`` carries a single optional action clause,
        ``[ ON DELETE { CASCADE | SET NULL } ]`` -- there is no ``ON UPDATE``
        branch to accept.  Measured on 18c / 21c XE / 23c: every spelling
        fails at the ``UPDATE`` keyword, i.e. the parser never even reaches
        the action name -- 18c and 21c report ``ORA-00905: missing keyword``,
        23c reports ``ORA-02000: missing DELETE keyword``.  The answer is
        ``False`` on every version, so an on-update action is refused by name
        rather than rendered.
        """
        return False

    def supports_fk_match(self) -> bool:
        """Oracle has no ``MATCH { SIMPLE | PARTIAL | FULL }`` clause.

        ``references_clause`` has no MATCH branch.  Measured on 21c XE: the
        inline form (``col NUMBER REFERENCES t(c) MATCH FULL``) is refused
        with ``ORA-00907: missing right parenthesis`` -- the parser reads
        ``MATCH`` as the start of a column definition -- while the
        out-of-line form (``ADD CONSTRAINT c CHECK (...) MATCH FULL``) is
        refused with ``ORA-01735: invalid ALTER TABLE option``.  Same on 18c
        and 23c.  Oracle also has no partial-match semantics to fall back
        on, so the clause is refused by name and never silently dropped.
        """
        return False

    # --- FK referential actions (SQL-92) -------------------------------
    # Oracle's action set is narrower than the SQL-standard five: only
    # CASCADE and SET NULL can be spelled, the other three are refused by
    # the parser.  A plain ``REFERENCES`` column still records
    # ``delete_rule = 'NO ACTION'`` in ``user_constraints`` -- NO ACTION is
    # Oracle's *default*, not a clause it accepts -- which is why the
    # ``no_action`` probe is False while the dictionary says NO ACTION.

    def supports_fk_on_delete(self) -> bool:
        """Whether ``ON DELETE`` referential actions are supported at all. True.

        Measured on 21c XE 21.3.0.0.0: the clause exists, but with exactly
        two spellings -- ``[ ON DELETE { CASCADE | SET NULL } ]`` -- and the
        dictionary records the choice (``delete_rule`` reads ``CASCADE`` /
        ``SET NULL``).  The other three SQL-standard actions cannot be
        spelled at all; see the per-action probes below for their codes.

        Declared here so the measured surface is owned by this package rather
        than inherited: ``True`` means "the clause exists", not "the
        standard's five actions all work".
        """
        return True

    def supports_fk_referential_action_cascade(self) -> bool:
        """``ON DELETE CASCADE`` is one of Oracle's two referential actions.

        Measured on 21c XE: ``CREATE TABLE s (id NUMBER PRIMARY KEY, a
        NUMBER REFERENCES r(id) ON DELETE CASCADE)`` succeeds, and
        ``user_constraints.delete_rule`` reads ``CASCADE``.  Same out of
        line: ``ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES
        r(id) ON DELETE CASCADE`` is accepted.
        """
        return True

    def supports_fk_referential_action_set_null(self) -> bool:
        """``ON DELETE SET NULL`` is accepted.

        Measured on 21c XE: the inline spelling succeeds and
        ``user_constraints.delete_rule`` reads ``SET NULL``.
        """
        return True

    def supports_fk_referential_action_restrict(self) -> bool:
        """``ON DELETE RESTRICT`` cannot be spelled.

        Measured on 21c XE (18c and 23c agree):
        ``... REFERENCES r(id) ON DELETE RESTRICT`` is refused with
        ``ORA-00905: missing keyword`` -- 23c phrases the same failure as
        ``ORA-02000: missing CASCADE keyword``.
        """
        return False

    def supports_fk_referential_action_no_action(self) -> bool:
        """``ON DELETE NO ACTION`` cannot be spelled, even though it is the
        default.

        Measured on 21c XE: ``... REFERENCES r(id) ON DELETE NO ACTION`` is
        refused with ``ORA-00905: missing keyword`` (23c: ``ORA-02000: missing
        CASCADE keyword``).  The trap is the dictionary: a plain FK with no
        action clause at all records ``delete_rule = 'NO ACTION'`` in
        ``user_constraints``, so reading the state back looks like support
        for a spelling the parser refuses.  The probe answers the spelling
        question, hence ``False``.
        """
        return False

    def supports_fk_referential_action_set_default(self) -> bool:
        """``ON DELETE SET DEFAULT`` is unimplemented, not merely unparsed.

        Measured on 21c XE: ``... REFERENCES r(id) ON DELETE SET DEFAULT`` is
        refused with ``ORA-03001: unimplemented feature`` -- Oracle parses
        the clause and declines to implement it, which is a stronger
        refusal than the missing-keyword the other three get.
        """
        return False

    def supports_constraint_enforced(self) -> bool:
        """Oracle has no ``ENFORCED`` / ``NOT ENFORCED`` constraint spelling.

        Measured on 21c XE 21.3.0.0.0: inline ``CHECK (a > 0) ENFORCED`` is
        refused with ``ORA-00907: missing right parenthesis`` and inline
        ``CHECK (a > 0) NOT ENFORCED`` with ``ORA-00905: missing keyword``;
        the out-of-line ``ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)
        ENFORCED`` and ``... NOT ENFORCED`` are both refused with
        ``ORA-01735: invalid ALTER TABLE option``.  (An earlier cross-version
        pass recorded ORA-03075 for one out-of-line shape and ORA-00907 /
        ORA-03076 / ORA-00905 / ORA-02000 across 18c and 23c; the codes
        differ by release and by shape, but every one is a refusal, so the
        value does not.)

        Oracle's constraint-state vocabulary is ``ENABLE`` / ``DISABLE``
        plus ``VALIDATE`` / ``NOVALIDATE`` -- a different clause answering a
        different question (whether the constraint is active) than SQL:2016's
        ``ENFORCED`` (whether it is mandatory).  The standard spelling does
        not exist here, so a request for it is refused by name rather than
        rendered.
        """
        return False

    def supports_alter_constraint(self) -> bool:
        """Oracle has no ``ALTER CONSTRAINT`` statement at all.

        Measured on 21c XE 21.3.0.0.0 (18c and 23c agree): every spelling is
        refused with ``ORA-01735: invalid ALTER TABLE option``, e.g.::

            ALTER TABLE t ALTER CONSTRAINT ck ENABLE
            ALTER TABLE t ALTER CONSTRAINT ck NOT ENFORCED
            ALTER TABLE t ALTER CONSTRAINT ck DEFERRABLE
            ALTER TABLE t ALTER CONSTRAINT ck NOVALIDATE

        The reason is structural: ``alter_table_properties`` has no
        ``ALTER CONSTRAINT`` branch.  Oracle changes a constraint's state
        through two *other* statement forms -- ``ALTER TABLE t ENABLE |
        DISABLE [VALIDATE | NOVALIDATE] CONSTRAINT c`` and
        ``ALTER TABLE t MODIFY CONSTRAINT c <state>`` -- both measured
        accepted.  So the capability is real and its spelling is elsewhere;
        that is what :meth:`supports_enable_disable_constraint` records.

        The name of this switch is provisional: core S3 owns the final
        ``AlterTableConstraintSupport`` spelling and may rename it.
        """
        return False

    def supports_alter_constraint_enforced(self) -> bool:
        """Oracle has no ``ALTER CONSTRAINT ... [NOT] ENFORCED`` statement.

        There is no ``ALTER CONSTRAINT`` clause in ``ALTER TABLE`` at all.
        Measured on 21c XE: ``ALTER TABLE t ALTER CONSTRAINT c ENABLE`` and
        ``ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED`` are both refused
        with ``ORA-01735: invalid ALTER TABLE option``.  Oracle addresses
        the same intent through ``ENABLE`` / ``DISABLE CONSTRAINT`` (see
        :meth:`supports_constraint_enable_disable`), which is a statement
        form, not an ``ALTER CONSTRAINT`` subclause.

        This is the attribute-level half of :meth:`supports_alter_constraint`,
        which is ``False`` for the same measurement.
        """
        return False

    def supports_deferrable_constraint(self) -> bool:
        """Oracle honours ``DEFERRABLE`` -- it is not a no-op.

        Measured on 21c XE 21.3.0.0.0, cross-checked identical on 18c and
        23c.  ``CREATE TABLE t (... CONSTRAINT c CHECK (a > 0)
        DEFERRABLE INITIALLY DEFERRED)`` records
        ``user_constraints = (DEFERRABLE, DEFERRED, VALIDATED, ENABLED)``.
        A violating INSERT *inside the transaction succeeds*; the failure
        moves to COMMIT, which raises ``ORA-02091`` wrapping
        ``ORA-02290: check constraint (SYSTEM.D_CK) violated`` and rolls the
        row back (COUNT returns to 0).  ``SET CONSTRAINT c IMMEDIATE`` then
        makes the very same INSERT fail immediately with ``ORA-02290``.

        So the clause changes when the check runs -- at COMMIT for a DEFERRED
        constraint, per statement for an IMMEDIATE one (a constraint declared
        ``DEFERRABLE INITIALLY IMMEDIATE`` refuses the same INSERT right away
        with ``ORA-02290``).  The answer is ``True`` for every constraint
        kind, because ``constraint_state`` hangs outside the whole of
        ``inline_constraint`` / ``out_of_line_constraint`` rather than inside
        any one branch.

        Any claim that Oracle "accepts DEFERRABLE but keeps it IMMEDIATE" is
        wrong; the measurement above disproves it.

        ``True`` here is also *not* a per-kind claim.  The per-kind truth is
        in the ``supports_deferrable_*_constraint`` probes below, and unlike
        PostgreSQL's -- where CHECK and NOT NULL are refused -- **all five**
        of Oracle's constraint kinds are ``True``.
        """
        return True

    # DEFERRABLE per kind. PostgreSQL needs this split because its server
    # refuses CHECK ("0A000: CHECK constraints cannot be marked DEFERRABLE")
    # and NOT NULL ("42601: misplaced DEFERRABLE clause").  Oracle has no
    # such split: ``constraint_state`` hangs outside the whole of
    # ``inline_constraint`` / ``out_of_line_constraint``, so every branch
    # carries it.  Measured on 21c XE 21.3.0.0.0 (18c / 23c identical) --
    # each accepted spelling is quoted below, and ``user_constraints`` was
    # read back to confirm the deferral was *recorded*, not discarded.

    def supports_deferrable_check_constraint(self) -> bool:
        """Whether CHECK accepts ``DEFERRABLE`` / ``INITIALLY ...``. True.

        Measured on 21c XE, out of line and inline::

            ALTER TABLE t ADD CONSTRAINT cc CHECK (v > 0) DEFERRABLE INITIALLY DEFERRED
            CREATE TABLE ct (a NUMBER CHECK (a > 0) DEFERRABLE INITIALLY DEFERRED)

        both accepted, and ``user_constraints`` reads
        ``(DEFERRABLE, DEFERRED)`` for the resulting ``C`` row.  The deferral
        is real, not merely parsed: see
        :meth:`supports_deferrable_constraint` for the COMMIT-time
        ``ORA-02091`` / ``ORA-02290`` evidence.

        This is the probe PostgreSQL has to answer ``False``; the same
        sentence there is ``0A000: CHECK constraints cannot be marked
        DEFERRABLE``.
        """
        return True

    def supports_deferrable_primary_key_constraint(self) -> bool:
        """Whether PRIMARY KEY accepts ``DEFERRABLE``. True.

        Measured on 21c XE, out of line and inline::

            ALTER TABLE t ADD CONSTRAINT cp PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED
            CREATE TABLE ct (a NUMBER PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)

        both accepted; ``user_constraints`` reads ``(DEFERRABLE, DEFERRED)``
        on the ``P`` row.
        """
        return True

    def supports_deferrable_unique_constraint(self) -> bool:
        """Whether UNIQUE accepts ``DEFERRABLE``. True.

        Measured on 21c XE, out of line and inline::

            ALTER TABLE t ADD CONSTRAINT cu UNIQUE (a) DEFERRABLE INITIALLY DEFERRED
            ALTER TABLE t ADD CONSTRAINT cu UNIQUE (v) DEFERRABLE INITIALLY IMMEDIATE
            CREATE TABLE ct (a NUMBER UNIQUE DEFERRABLE INITIALLY DEFERRED)

        all accepted; ``user_constraints`` reads ``(DEFERRABLE, DEFERRED)``
        on the ``U`` row.
        """
        return True

    def supports_deferrable_foreign_key_constraint(self) -> bool:
        """Whether FOREIGN KEY accepts ``DEFERRABLE``. True.

        Measured on 21c XE, out of line and inline::

            ALTER TABLE t ADD CONSTRAINT cf FOREIGN KEY (a) REFERENCES p(id)
                DEFERRABLE INITIALLY DEFERRED
            CREATE TABLE ct (a NUMBER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)

        both accepted and ``user_constraints`` records
        ``(DEFERRABLE, DEFERRED)``.  A deferrable FK is the clearest case,
        because the deferral is observable without a COMMIT: inserting a
        child row whose parent does not exist *succeeds* inside the
        transaction (``SELECT COUNT(*)`` returns 1) and only ``COMMIT``
        raises::

            ORA-02091: transaction rolled back
            ORA-02291: integrity constraint (SYSTEM.CF) violated - parent key not found

        with the row gone (``COUNT`` back to 0).  Issuing
        ``SET CONSTRAINT cf IMMEDIATE`` with the offending row still in the
        transaction instead raises ``ORA-02291`` right there, which is what
        makes the deferral real rather than advisory.
        """
        return True

    def supports_deferrable_not_null_constraint(self) -> bool:
        """Whether NOT NULL accepts ``DEFERRABLE``. True.

        Measured on 21c XE, inline on CREATE and via MODIFY::

            CREATE TABLE ct (a NUMBER NOT NULL DEFERRABLE INITIALLY DEFERRED)
            ALTER TABLE t MODIFY (a NOT NULL DEFERRABLE INITIALLY DEFERRED)

        both accepted, and the system-named ``C`` row in ``user_constraints``
        reads ``(DEFERRABLE, DEFERRED)`` -- so the state is recorded on the
        NOT NULL check itself, not folded into the table definition.

        This is the second kind PostgreSQL must answer ``False`` for
        (``42601: misplaced DEFERRABLE clause``).
        """
        return True

    def supports_deferrable_exclude_constraint(self) -> bool:
        """Whether EXCLUDE accepts ``DEFERRABLE``. False -- there is no EXCLUDE.

        Oracle's ``out_of_line_constraint`` has no ``EXCLUDE`` branch, so the
        kind cannot carry ``constraint_state`` or anything else.  Measured on
        21c XE::

            ALTER TABLE t ADD CONSTRAINT ce EXCLUDE (a FROM ())
            -> ORA-00902: invalid datatype

        -- the parser reads ``EXCLUDE`` as the start of a column datatype.
        Appending ``DEFERRABLE`` changes nothing (same ``ORA-00902``).  See
        :meth:`supports_exclude_constraint`.
        """
        return False

    def supports_constraint_rely(self) -> bool:
        """``RELY`` / ``NORELY`` is part of Oracle's ``constraint_state``.

        Measured on 21c XE: accepted for CHECK, UNIQUE and PRIMARY KEY alike,
        e.g. ``ALTER TABLE t MODIFY CONSTRAINT tc RELY`` where ``tc`` is a
        CHECK, ``ALTER TABLE t MODIFY CONSTRAINT tu RELY`` where ``tu`` is a
        UNIQUE, and ``ALTER TABLE t MODIFY CONSTRAINT tp RELY`` where ``tp``
        is a PRIMARY KEY.  ``NORELY`` is accepted in the same position.  It
        is a query-rewrite hint -- it tells the optimizer the constraint may
        be assumed even when not validated -- which is why Oracle accepts it
        where the SQL-standard ``ENFORCED`` does not exist.
        """
        return True

    def supports_constraint_enable_disable(self) -> bool:
        """``ENABLE`` / ``DISABLE CONSTRAINT`` is Oracle's enforcement switch.

        Measured on 21c XE: ``ALTER TABLE t ENABLE CONSTRAINT c``,
        ``ALTER TABLE t DISABLE CONSTRAINT c``, ``ALTER TABLE t ENABLE
        VALIDATE CONSTRAINT c`` and ``ALTER TABLE t ENABLE NOVALIDATE
        CONSTRAINT c`` are all accepted, as are the ``MODIFY CONSTRAINT c
        ENABLE VALIDATE`` / ``ENABLE NOVALIDATE`` / ``DISABLE`` forms.  This
        is the capability the SQL-standard ``ENFORCED`` probe asks about but
        cannot express here; see :meth:`supports_constraint_enforced`.

        Paired with :meth:`supports_enable_disable_constraint`, which is the
        same capability under the name the other backends use.
        """
        return True

    def supports_enable_disable_constraint(self) -> bool:
        """Whether ``ALTER TABLE ... {ENABLE|DISABLE} CONSTRAINT`` is supported.

        **True -- and Oracle is the only backend of the ten measured that
        spells it at all**, which is why this switch has to be declared here
        rather than left to a shared default.  Measured on 21c XE 21.3.0.0.0
        against a live CHECK constraint, every form accepted::

            ALTER TABLE t ENABLE CONSTRAINT ck
            ALTER TABLE t DISABLE CONSTRAINT ck
            ALTER TABLE t ENABLE VALIDATE CONSTRAINT ck
            ALTER TABLE t ENABLE NOVALIDATE CONSTRAINT ck

        The dictionary records the flip, so it is honoured rather than
        accepted-and-parsed-away: ``ENABLE NOVALIDATE CONSTRAINT`` on a CHECK
        over rows that already violate it leaves
        ``user_constraints.validated = 'NOT VALIDATED'`` with the rows still
        present, and ``ENABLE VALIDATE`` is what would reject them.

        Every sibling backend answers ``False`` here for a concrete measured
        reason, which is why a shared default would be wrong in both
        directions: MySQL 26.7.0 ``1064 (42000)``; SQL Server 2019
        ``Incorrect syntax near the keyword 'CONSTRAINT'. (156)``;
        PostgreSQL 19beta4 ``42601`` (PG's ``ENABLE TRIGGER`` is a
        near-relative, not this statement).  Oracle's state-changing pair for
        the same intent is ``ENABLE`` / ``DISABLE``, which is also its
        answer to SQL:2016's ``ENFORCED`` -- a clause Oracle does not have
        (:meth:`supports_constraint_enforced`).

        Note what is *not* this: there is no ``ALTER CONSTRAINT`` statement
        to hang it on (see :meth:`supports_alter_constraint`).

        The name of this switch is provisional: core S3 owns the final
        ``AlterTableConstraintSupport`` spelling and may rename it.
        """
        return True

    def supports_constraint_novalidate(self) -> bool:
        """False for the spelling core renders -- although Oracle *has* the capability.

        Oracle can add a constraint without validating existing rows, and
        live testing proves it is genuinely honoured rather than parsed away.
        Measured on 21c XE 21.3.0.0.0, with a row already violating the
        predicate present before the constraint is added::

            CREATE TABLE t (id NUMBER PRIMARY KEY, v NUMBER);
            INSERT INTO t VALUES (1, -5);                       -- committed
            ALTER TABLE t ADD CONSTRAINT ck CHECK (v > 0) NOVALIDATE;   -- accepted
            -- user_constraints -> validated = 'NOT VALIDATED'
            SELECT ... FROM t                                    -- the -5 row survives

        The same is accepted with the ``ENABLE`` prefix
        (``... CHECK (v > 0) ENABLE NOVALIDATE``, same ``NOT VALIDATED``
        result), and ``ALTER TABLE t MODIFY CONSTRAINT ck NOVALIDATE`` is
        accepted as well.

        So the *capability* is present.  This switch answers the other
        question -- whether the ``NOT VALID`` clause the shared renderer
        emits is one Oracle accepts -- and it is not.  Measured on the same
        server::

            ALTER TABLE t ADD CONSTRAINT ck CHECK (v > 0) NOT VALID
            -> ORA-01735: invalid ALTER TABLE option

        Oracle's word is ``NOVALIDATE``, one token, with no ``NOT``.  Core's
        ``NOT VALID`` is PostgreSQL's spelling, and this is the second place
        (after ``ENFORCED``) where adopting it verbatim would render SQL the
        server refuses.  Hence ``False``.

        The two statements differ, and the asymmetry matters to callers:
        writing ``NOVALIDATE`` works, writing ``NOT VALID`` does not, so the
        renderer must substitute the keyword rather than suppress the
        feature.  The ``ENABLE VALIDATE`` / ``ENABLE NOVALIDATE`` /
        ``MODIFY CONSTRAINT ... NOVALIDATE`` paths are recorded on
        :meth:`supports_enable_disable_constraint`.

        A third spelling is also refused, and this is the trap worth
        remembering: ``NOVALIDATE`` is accepted as a tail on ``ADD
        CONSTRAINT``, but **not** as a standalone alter clause.  Measured::

            ALTER TABLE t NOVALIDATE CONSTRAINT ck
            -> ORA-01735: invalid ALTER TABLE option

        The option must be threaded through ``ENABLE`` or ``MODIFY
        CONSTRAINT`` to be attached to an existing constraint.
        """
        return False

    def supports_validate_constraint(self) -> bool:
        """The bare ``ALTER TABLE ... VALIDATE CONSTRAINT`` form is refused.

        Measured on 21c XE: ``ALTER TABLE t VALIDATE CONSTRAINT c`` is
        refused with ``ORA-01735: invalid ALTER TABLE option``.  The accepted
        spellings are ``ALTER TABLE t ENABLE VALIDATE CONSTRAINT c`` and
        ``ALTER TABLE t MODIFY CONSTRAINT c VALIDATE``, both measured
        accepted.  Note that ``ENABLE VALIDATE`` is the *default* behaviour
        of ``ENABLE``, so a validation request that renders as bare
        ``VALIDATE CONSTRAINT`` would be rejected by the server.
        """
        return False

    def supports_using_index_clause(self) -> bool:
        """``USING INDEX`` is accepted, but only for UNIQUE and PRIMARY KEY.

        Measured on 21c XE (18c and 23c agree): ``CONSTRAINT u UNIQUE (a)
        USING INDEX`` and ``CONSTRAINT p PRIMARY KEY (id) USING INDEX`` are
        accepted both inline and out of line, while ``CONSTRAINT c CHECK
        (a > 0) USING INDEX`` is refused -- ``ORA-01735: invalid ALTER TABLE
        option`` out of line and ``ORA-00907: missing right parenthesis``
        inline.  The clause is what makes a constraint's enforcing index
        explicit (name, storage, tablespace), and only index-backed kinds
        have one.

        The value is ``True`` precisely because the kinds that can carry the
        clause accept it; a caller must still refuse ``USING INDEX`` *by
        kind* on CHECK rather than consult this probe, because a bare
        ``True`` would render SQL the server rejects.  Core's S3 may split
        this into per-kind probes; the measurement above is the part that
        has to survive that rename.
        """
        return True

    def supports_add_constraint(self) -> bool:
        """``ALTER TABLE ... ADD CONSTRAINT`` is accepted for all four kinds.

        Measured on 21c XE: ``ADD CONSTRAINT c CHECK (a > 0)``,
        ``ADD CONSTRAINT c UNIQUE (a)``, ``ADD CONSTRAINT c PRIMARY KEY
        (id)`` and ``ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES r(id)``
        (with ``ON DELETE CASCADE``) are all accepted.  Note that
        ``DROP CONSTRAINT IF EXISTS`` is a different question -- see
        :meth:`supports_drop_constraint_if_exists`.
        """
        return True

    def supports_drop_constraint(self) -> bool:
        """``ALTER TABLE ... DROP CONSTRAINT [CASCADE]`` is accepted.

        Measured on 21c XE: ``ALTER TABLE t DROP CONSTRAINT c`` and
        ``ALTER TABLE t DROP CONSTRAINT c CASCADE`` are both accepted.  The
        dictionary reports a missing constraint as ``ORA-02443: Cannot drop
        constraint - nonexistent constraint``, so idempotent drops need
        ``IF EXISTS``, which Oracle does not spell (see
        :meth:`supports_drop_constraint_if_exists`, ``False``, measured
        ``ORA-01735``).
        """
        return True

    def supports_rename_constraint(self) -> bool:
        """``ALTER TABLE ... RENAME CONSTRAINT ... TO ...`` is accepted.

        Measured on 21c XE: ``ALTER TABLE t RENAME CONSTRAINT old TO new``
        is accepted.  Renaming an absent constraint is refused with
        ``ORA-23292: The constraint does not exist``.
        """
        return True

    def supports_exclude_constraint(self) -> bool:
        """Oracle has no ``EXCLUDE`` table constraint.

        Measured on 21c XE: ``ALTER TABLE t ADD CONSTRAINT c EXCLUDE (a FROM
        ())`` is refused with ``ORA-00902: invalid datatype`` -- the parser
        reads ``EXCLUDE`` as the start of a column datatype, because no
        ``EXCLUDE`` branch exists in ``out_of_line_constraint``.  PostgreSQL
        and SQLite both have the constraint; Oracle expresses the same
        intent through a CHECK over an expression or a function-based index.
        """
        return False

    # --- Boolean / Vector / JSON-relational ---------------------------
    def supports_boolean_type(self) -> bool:
        return self.version >= (23, 0, 0)

    def supports_vector_type(self) -> bool:
        return self.version >= (23, 0, 0)

    def supports_json_duality(self) -> bool:
        return self.version >= (23, 0, 0)

    # --- Truncate -----------------------------------------------------
    def supports_truncate_cascade(self) -> bool:
        return False

    # --- SQL functions ------------------------------------------------
    def supports_functions(self) -> Dict[str, bool]:
        from rhosocial.activerecord.backend.impl.oracle.function_versions import (
            ORACLE_FUNCTION_VERSIONS,
        )
        expression_constructors = {
            "xmlagg",
            "xmlattributes",
            "xmlcomment",
            "xmlconcat",
            "xmlelement",
            "xmlexists",
            "xmlforest",
            "xmlparse",
            "xmlpi",
            "xmlquery",
            "xmlroot",
            "xmlserialize",
            "xmltable",
        }
        result: Dict[str, bool] = {}
        for func_name, (min_ver, max_ver) in ORACLE_FUNCTION_VERSIONS.items():
            if func_name in expression_constructors:
                continue
            if max_ver is None:
                result[func_name] = self.version >= min_ver
            else:
                result[func_name] = min_ver <= self.version <= max_ver
        return result