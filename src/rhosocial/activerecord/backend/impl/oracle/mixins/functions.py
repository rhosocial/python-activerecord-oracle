# src/rhosocial/activerecord/backend/impl/oracle/mixins/functions.py
"""Oracle-specific function-call formatters.

Override generic ``format_function_call`` to emit Oracle's native syntax
for WITHIN GROUP, JSON_TABLE COLUMNS, and other non-standard clauses.
"""

from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLPredicate


class OracleFunctionFormatMixin:
    """Oracle-specific function and expression formatters."""

    def format_function_call(
        self,
        expr: "BaseExpression",
        filter_predicate: Optional["SQLPredicate"] = None,
    ) -> Tuple[str, Tuple]:
        fn_name = getattr(expr, "func_name", "").upper()

        if fn_name == "LISTAGG":
            return self._format_listagg(expr, filter_predicate)
        if fn_name in ("PERCENTILE_CONT", "PERCENTILE_DISC"):
            return self._format_percentile_ordered_set(expr, filter_predicate)
        if fn_name == "JSON_TABLE":
            return self._format_json_table(expr)
        if fn_name in ("JSON_VALUE", "JSON_QUERY"):
            return self._format_json_scalar(expr)

        return super().format_function_call(expr, filter_predicate)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _format_listagg(  # noqa: E301
        self,
        expr: "BaseExpression",
        filter_predicate: Optional["SQLPredicate"] = None,
    ) -> Tuple[str, Tuple]:
        args_sql: List[str] = []
        all_params: List[Any] = []
        distinct = "DISTINCT " if getattr(expr, "is_distinct", False) else ""

        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            for p in params_part:
                all_params.append(p)

        arg_list = ", ".join(args_sql)
        func_sql = f"LISTAGG({distinct}{arg_list})"

        within_group: Optional[str] = getattr(expr, "_oracle_within_group", None)
        on_overflow: Optional[str] = getattr(expr, "_oracle_on_overflow", None)

        if within_group or on_overflow:
            if within_group:
                func_sql += f" WITHIN GROUP (ORDER BY {within_group})"
            if on_overflow:
                func_sql += f" ON OVERFLOW {on_overflow}"

        return self._finish_function_call(func_sql, all_params, expr, filter_predicate)

    def _format_percentile_ordered_set(
        self,
        expr: "BaseExpression",
        filter_predicate: Optional["SQLPredicate"] = None,
    ) -> Tuple[str, Tuple]:
        fn_name = expr.func_name.upper()
        all_params: List[Any] = []
        distinct = "DISTINCT " if getattr(expr, "is_distinct", False) else ""

        args_sql: List[str] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            for p in params_part:
                all_params.append(p)

        arg_list = ", ".join(args_sql)
        func_sql = f"{fn_name}({distinct}{arg_list})"

        within_group: Optional[str] = getattr(expr, "_oracle_within_group", None)
        if within_group:
            func_sql += f" WITHIN GROUP (ORDER BY {within_group})"

        return self._finish_function_call(func_sql, all_params, expr, filter_predicate)

    def _format_json_scalar(self, expr: "BaseExpression") -> Tuple[str, Tuple]:
        all_params: List[Any] = []

        args_sql: List[str] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            for p in params_part:
                all_params.append(p)

        func_sql = f"{expr.func_name.upper()}({', '.join(args_sql)}"
        returning_type = getattr(expr, "_oracle_returning_type", None)
        if returning_type is not None:
            type_sql, _ = self.format_data_type(returning_type)
            func_sql += f" RETURNING {type_sql}"
        func_sql += ")"

        return self._finish_function_call(func_sql, all_params, expr, None)

    def _format_json_table(self, expr: "BaseExpression") -> Tuple[str, Tuple]:
        all_params: List[Any] = []

        args_sql: List[str] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            for p in params_part:
                all_params.append(p)

        arg_list = ", ".join(args_sql)
        func_sql = f"JSON_TABLE({arg_list}"

        columns: Optional[str] = getattr(expr, "_oracle_json_table_columns", None)
        if columns:
            func_sql += f" COLUMNS ({columns}))"
        else:
            func_sql += ")"

        return self._finish_function_call(func_sql, all_params, expr, None)

    def _finish_function_call(
        self,
        func_sql: str,
        all_params: List[Any],
        expr: "BaseExpression",
        filter_predicate: Optional["SQLPredicate"] = None,
    ) -> Tuple[str, Tuple]:
        if filter_predicate:
            filter_sql, filter_params = filter_predicate.to_sql()
            func_sql += f" FILTER (WHERE {filter_sql})"
            for p in filter_params:
                all_params.append(p)

        if getattr(expr, "cast_types", None):
            for target_type in expr.cast_types:
                func_sql, pt = self.format_cast_expression(
                    func_sql, target_type, tuple(all_params), None
                )
                all_params = list(pt)

        if getattr(expr, "alias", None):
            func_sql += f" AS {self.format_identifier(expr.alias)}"

        return func_sql, tuple(all_params)