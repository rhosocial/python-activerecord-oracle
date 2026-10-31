# src/rhosocial/activerecord/backend/impl/oracle/mixins/functions.py
"""Oracle's derived function-call formatters.

One ``format_oracle_*`` entry point per derived node kind in
``expression/functions.py`` (LISTAGG, the ordered-set percentiles, and the
JSON family). The name-based dispatch this mixin used to do inside
``format_function_call`` is gone with the hidden state it served: each
Oracle function whose grammar carries a clause the core call has no slot
for is its own node now, dispatched by its own ``format_method``. What
remains for a generic :class:`~rhosocial.activerecord.backend.expression.core.FunctionCall`
is the base's rendering -- ``NAME(args)`` plus the standard modifiers --
which is exactly what Oracle spells for a user-defined function.
"""

from typing import Any, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.bases import BaseExpression
    from rhosocial.activerecord.backend.impl.oracle.expression.functions import (
        OracleJsonQueryFunction,
        OracleJsonTableFunction,
        OracleJsonValueFunction,
        OracleListaggFunction,
        OraclePercentileContFunction,
        OraclePercentileDiscFunction,
    )


class OracleFunctionFormatMixin:
    """Oracle-specific function and expression formatters."""

    def format_oracle_listagg_function(
        self, expr: "OracleListaggFunction"
    ) -> Tuple[str, tuple]:
        """``LISTAGG([DISTINCT] args [ON OVERFLOW ...]) [WITHIN GROUP (...)]``.

        The overflow clause belongs to the aggregate's argument list, not
        after the sort clause:

        .. code-block:: sql

            LISTAGG(x, ';' ON OVERFLOW TRUNCATE) WITHIN GROUP (ORDER BY x)   ok
            LISTAGG(x, ';') WITHIN GROUP (ORDER BY x) ON OVERFLOW TRUNCATE
                                                                ORA-00923

        Which means the clause that decides what happens when the result is
        too long was being emitted somewhere the server refuses to look, and
        every aggregate large enough to overflow was the case that failed --
        the ones that fit worked and looked right.
        """
        self.check_function_version(expr.func_name)
        args_sql, all_params = self._render_call_args(expr)
        arg_list = ", ".join(args_sql)
        distinct = "DISTINCT " if expr.is_distinct else ""
        if expr.on_overflow:
            arg_list = f"{arg_list} ON OVERFLOW {expr.on_overflow}"
        func_sql = f"LISTAGG({distinct}{arg_list})"
        if expr.within_group_order_by:
            func_sql += f" WITHIN GROUP (ORDER BY {expr.within_group_order_by})"
        return self._finish_function_call(func_sql, all_params, expr)

    def format_oracle_percentile_cont_function(
        self, expr: "OraclePercentileContFunction"
    ) -> Tuple[str, tuple]:
        """``PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY ...)``."""
        return self._format_oracle_percentile("PERCENTILE_CONT", expr)

    def format_oracle_percentile_disc_function(
        self, expr: "OraclePercentileDiscFunction"
    ) -> Tuple[str, tuple]:
        """``PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY ...)``."""
        return self._format_oracle_percentile("PERCENTILE_DISC", expr)

    def _format_oracle_percentile(
        self,
        name: str,
        expr: "BaseExpression",
    ) -> Tuple[str, tuple]:
        """An ordered-set aggregate: the fraction inside, the ordering after."""
        self.check_function_version(expr.func_name)
        args_sql, all_params = self._render_call_args(expr)
        distinct = "DISTINCT " if expr.is_distinct else ""
        func_sql = f"{name}({distinct}{', '.join(args_sql)})"
        if expr.within_group_order_by:
            func_sql += f" WITHIN GROUP (ORDER BY {expr.within_group_order_by})"
        return self._finish_function_call(func_sql, all_params, expr)

    def format_oracle_json_value_function(
        self, expr: "OracleJsonValueFunction"
    ) -> Tuple[str, tuple]:
        """``JSON_VALUE(json_doc, path [RETURNING type])``."""
        return self._format_oracle_json_scalar("JSON_VALUE", expr)

    def format_oracle_json_query_function(
        self, expr: "OracleJsonQueryFunction"
    ) -> Tuple[str, tuple]:
        """``JSON_QUERY(json_doc, path [RETURNING type])``."""
        return self._format_oracle_json_scalar("JSON_QUERY", expr)

    def _format_oracle_json_scalar(
        self,
        name: str,
        expr: "BaseExpression",
    ) -> Tuple[str, tuple]:
        """A JSON scalar/document extraction, with the optional RETURNING."""
        self.check_function_version(expr.func_name)
        args_sql, all_params = self._render_call_args(expr)
        func_sql = f"{name}({', '.join(args_sql)}"
        if expr.returning_type is not None:
            type_sql, _ = self.format_data_type(expr.returning_type)
            func_sql += f" RETURNING {type_sql}"
        func_sql += ")"
        return self._finish_function_call(func_sql, all_params, expr)

    def format_oracle_json_table_function(
        self, expr: "OracleJsonTableFunction"
    ) -> Tuple[str, tuple]:
        """``JSON_TABLE(json_doc, path [COLUMNS (...)])``."""
        self.check_function_version(expr.func_name)
        args_sql, all_params = self._render_call_args(expr)
        func_sql = f"JSON_TABLE({', '.join(args_sql)}"
        if expr.columns:
            func_sql += f" COLUMNS ({expr.columns}))"
        else:
            func_sql += ")"
        return self._finish_function_call(func_sql, all_params, expr)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _render_call_args(self, expr: "BaseExpression") -> Tuple[List[str], List[Any]]:
        """Render the call's argument list, keeping SQL and parameters in step."""
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
        return args_sql, all_params

    def _finish_function_call(
        self,
        func_sql: str,
        all_params: List[Any],
        expr: "BaseExpression",
    ) -> Tuple[str, tuple]:
        """Append the modifiers every call shares: ``FILTER`` and the alias."""
        if expr.filter_predicate:
            filter_sql, filter_params = expr.filter_predicate.to_sql()
            func_sql += f" FILTER (WHERE {filter_sql})"
            all_params.extend(filter_params)

        if expr.alias:
            func_sql += f" AS {self.format_identifier(expr.alias)}"

        return func_sql, tuple(all_params)
