# src/rhosocial/activerecord/backend/impl/oracle/mixins/json.py
from typing import Any, List, Tuple


class OracleJSONFunctionMixin(object):
    """Oracle JSON function implementation."""

    def supports_json_type(self) -> bool:
        """Native JSON type is supported since Oracle 21c."""
        return getattr(self, 'version', (21, 0, 0)) >= (21, 0, 0)

    def supports_json_merge_patch(self) -> bool:
        """JSON_MERGE_PATCH is supported since Oracle 12.2."""
        return getattr(self, 'version', (12, 0, 0)) >= (12, 0, 0)

    def supports_json_table(self) -> bool:
        """Oracle supports JSON_TABLE function since 12c."""
        return getattr(self, 'version', (12, 0, 0)) >= (12, 0, 0)

    def supports_json_duality_view(self) -> bool:
        """JSON Relational Duality is supported since Oracle 23ai."""
        return getattr(self, 'version', (23, 0, 0)) >= (23, 0, 0)

    def format_json_extract(self, col_expr: str, path: str) -> str:
        """Format JSON_VALUE function for scalar extraction."""
        return f"JSON_VALUE({col_expr}, '$.{path}')"

    def format_json_query(self, col_expr: str, path: str) -> str:
        """Format JSON_QUERY function for object/array extraction."""
        return f"JSON_QUERY({col_expr}, '$.{path}')"

    def format_json_exists(self, col_expr: str, path: str) -> str:
        """Format JSON_EXISTS function for existence check."""
        return f"JSON_EXISTS({col_expr}, '$.{path}')"

    def format_json_table(self, alias: str, col_expr: str,
                          columns: List[Tuple[str, str]]) -> str:
        """Format JSON_TABLE function with an external alias.

        Args:
            alias: Table alias applied outside the JSON_TABLE call.
            col_expr: JSON column or literal expression to query.
            columns: List of (column_name, path) tuples. Each ``path``
                is rendered as ``PATH '$.<path>'``.

        Returns:
            SQL fragment ``JSON_TABLE(...) <alias>``.
        """
        col_parts = []
        for col_name, col_path in columns:
            col_parts.append(f"{col_name} PATH '$.{col_path}'")
        cols_sql = ", ".join(col_parts)
        return f"JSON_TABLE({col_expr}, '$' COLUMNS ({cols_sql})) {alias}"

    def format_json_merge_patch(self, col_expr: str, patch_json: str,
                                params: Any) -> Tuple[str, Tuple]:
        """Format JSON_MERGE_PATCH function.

        Uses the ``?`` positional placeholder; Oracle's parameter
        renumbering is handled by ``backend.execute()``.

        Args:
            col_expr: JSON column or literal expression to patch.
            patch_json: JSON string literal applied as the merge patch.
            params: Existing parameter tuple to append to.

        Returns:
            Tuple of (sql_fragment, params_tuple).
        """
        existing: Tuple = tuple(params) if params else ()
        return f"JSON_MERGE_PATCH({col_expr}, ?)", existing + (patch_json,)

    def format_json_array(self, *elements: Any) -> str:
        """Format JSON_ARRAY function."""
        return f"JSON_ARRAY({', '.join(str(e) for e in elements)})"

    def format_json_object(self, *pairs: Tuple[str, str]) -> str:
        """Format JSON_OBJECT function using Oracle KEY/VALUE syntax."""
        parts = [f"KEY {k} VALUE {v}" for k, v in pairs]
        return f"JSON_OBJECT({', '.join(parts)})"
