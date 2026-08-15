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
        return True

    # --- RETURNING ----------------------------------------------------
    def supports_returning_insert(self) -> bool:
        return True

    def supports_returning_update(self) -> bool:
        return True

    def supports_returning_delete(self) -> bool:
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
        return True

    def supports_explain_format(self, format_type: str) -> bool:
        format_type_upper = format_type.upper()
        return format_type_upper in ("TEXT", "JSON", "XML", "HTML", "SERIAL")

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
        return True

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
        return self.version >= (12, 0, 0)

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