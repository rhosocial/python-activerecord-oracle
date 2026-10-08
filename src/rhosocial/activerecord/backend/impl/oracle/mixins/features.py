# src/rhosocial/activerecord/backend/impl/oracle/mixins/features.py
"""Oracle feature-capability mixin.

Collects remaining ``supports_*`` capability switches that are not
already covered by a dedicated per-domain mixin.  Follows the pattern
of ``PostgresFeaturesMixin``.
"""

from typing import Dict


class OracleFeaturesMixin:
    """Aggregated Oracle feature-capability checks.

    Each domain-specific mixin (``OraclePivotMixin``, etc.) carries its
    own ``supports_*`` methods; this mixin gathers the cross-cutting
    checks that do not warrant a separate domain file.
    """

    # --- XML ----------------------------------------------------------
    def supports_xmlparse(self) -> bool:
        return False

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
    def supports_fk_on_update(self) -> bool:
        return False

    def supports_fk_match(self) -> bool:
        return False

    def supports_constraint_enforced(self) -> bool:
        """Oracle has no ``ENFORCED`` / ``NOT ENFORCED`` constraint spelling.

        Measured on 18c / 21c / 23c: ``CHECK (...) ENFORCED`` is refused
        (18c/21c ORA-00907, 23c ORA-03076) and ``NOT ENFORCED`` is refused
        (18c/21c ORA-00905, 23c ORA-02000); an out-of-line ``ENFORCED`` is
        refused with ORA-03075. Oracle's constraint-state vocabulary is
        ``ENABLE`` / ``DISABLE`` plus ``VALIDATE`` / ``NOVALIDATE``, which is a
        different clause; the SQL:2016 spelling does not exist here, so a
        request for it is refused by name rather than rendered.
        """
        return False

    def supports_deferrable_constraint(self) -> bool:
        return True

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