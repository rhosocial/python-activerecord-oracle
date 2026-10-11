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

    def format_json_extract(self, col_expr: str, path: str) -> Tuple[str, tuple]:
        """Format JSON_VALUE function for scalar extraction."""
        return f"JSON_VALUE({col_expr}, '$.{path}')", ()

    def format_json_query(self, col_expr: str, path: str) -> Tuple[str, tuple]:
        """Format JSON_QUERY function for object/array extraction."""
        return f"JSON_QUERY({col_expr}, '$.{path}')", ()

    def supports_json_path(self) -> bool:
        """Whether a JSON path can be read on this server.

        Same gate as the formatter below, which uses JSON_QUERY for a document
        and JSON_VALUE for a scalar. Both exist in the versions this claims and
        not in the ones it does not.
        """
        return self.supports_json_type()

    def format_json_function_expression(self, expr) -> Tuple[str, tuple]:
        """Render a JSON path with JSON_QUERY / JSON_VALUE.

        The core default emits JSON_EXTRACT wrapped in JSON_UNQUOTE, and Oracle
        has no JSON_UNQUOTE — so `->>` produced SQL the server rejects. Oracle's
        pair is JSON_QUERY for a document and JSON_VALUE for a scalar, which is
        what the two operators mean.

        Declared here because this mixin sits earlier in the MRO than the core
        JSONMixin; a formatter anywhere later would be dead code.
        """
        from ....expression import bases

        if isinstance(expr.column, bases.BaseExpression):
            col_sql, col_params = expr.column.to_sql()
        else:
            col_sql, col_params = self.format_identifier(str(expr.column)), ()

        path = _jsonpath_body(expr.path)
        if expr.operation == "->>":
            sql = f"JSON_VALUE({col_sql}, '$.{path}')"
        else:
            sql = f"JSON_QUERY({col_sql}, '$.{path}')"

        if expr.alias:
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"
        return sql, col_params

    def format_json_exists(self, col_expr: str, path: str) -> Tuple[str, tuple]:
        """Format JSON_EXISTS function for existence check."""
        return f"JSON_EXISTS({col_expr}, '$.{path}')", ()

    def format_json_table(self, alias: str, col_expr: str,
                          columns: List[Tuple[str, str]]) -> Tuple[str, tuple]:
        """Format JSON_TABLE function with an external alias.

        Args:
            alias: Table alias applied outside the JSON_TABLE call.
            col_expr: JSON column or literal expression to query.
            columns: List of (column_name, path) tuples. Each ``path``
                is rendered as ``PATH '$.<path>'``.

        Returns:
            Tuple of (SQL fragment ``JSON_TABLE(...) <alias>``, empty params).
        """
        col_parts = []
        for col_name, col_path in columns:
            col_parts.append(f"{col_name} PATH '$.{col_path}'")
        cols_sql = ", ".join(col_parts)
        return f"JSON_TABLE({col_expr}, '$' COLUMNS ({cols_sql})) {alias}", ()

    def format_json_merge_patch(self, col_expr: str, patch_json: str,
                                params: Any) -> Tuple[str, tuple]:
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
        return f"JSON_MERGE_PATCH({col_expr}, {self.p()})", existing + (patch_json,)

    def format_json_array(self, *elements: Any) -> Tuple[str, tuple]:
        """Format JSON_ARRAY function."""
        return f"JSON_ARRAY({', '.join(str(e) for e in elements)})", ()

    def format_json_object(self, *pairs: Tuple[str, str]) -> Tuple[str, tuple]:
        """Format JSON_OBJECT function using Oracle KEY/VALUE syntax."""
        parts = [f"KEY {k} VALUE {v}" for k, v in pairs]
        return f"JSON_OBJECT({', '.join(parts)})", ()


def _jsonpath_body(path: str) -> str:
    """Strip the leading ``$.`` from a jsonpath.

    Oracle's JSON_QUERY and JSON_VALUE take the path as a literal, so the
    shared ``$.a.b`` is assembled as ``'$.'`` plus the remainder — which is how
    the format_json_* helpers above are already written.
    """
    text = str(path or "").strip()
    if text.startswith("$."):
        return text[2:]
    if text == "$":
        return ""
    return text
