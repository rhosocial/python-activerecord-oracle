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
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
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
        """``PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY ...)``.

        An ordered-set aggregate: the fraction inside, the ordering after.
        """
        self.check_function_version(expr.func_name)
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
        distinct = "DISTINCT " if expr.is_distinct else ""
        func_sql = f"PERCENTILE_CONT({distinct}{', '.join(args_sql)})"
        if expr.within_group_order_by:
            func_sql += f" WITHIN GROUP (ORDER BY {expr.within_group_order_by})"
        return self._finish_function_call(func_sql, all_params, expr)

    def format_oracle_percentile_disc_function(
        self, expr: "OraclePercentileDiscFunction"
    ) -> Tuple[str, tuple]:
        """``PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY ...)``.

        The discrete-value mirror of :class:`OraclePercentileContFunction`:
        the same ordered-set shape, a different reduction.
        """
        self.check_function_version(expr.func_name)
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
        distinct = "DISTINCT " if expr.is_distinct else ""
        func_sql = f"PERCENTILE_DISC({distinct}{', '.join(args_sql)})"
        if expr.within_group_order_by:
            func_sql += f" WITHIN GROUP (ORDER BY {expr.within_group_order_by})"
        return self._finish_function_call(func_sql, all_params, expr)

    def format_oracle_json_value_function(
        self, expr: "OracleJsonValueFunction"
    ) -> Tuple[str, tuple]:
        """``JSON_VALUE(json_doc, path [RETURNING type])``.

        Extracts a scalar from a JSON document; the optional ``RETURNING``
        type names what the scalar is read back as.
        """
        self.check_function_version(expr.func_name)
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
        func_sql = f"JSON_VALUE({', '.join(args_sql)}"
        if expr.returning_type is not None:
            returning = self._coerce_returning_type(expr.returning_type)
            type_sql, _ = self.format_data_type(returning)
            func_sql += f" RETURNING {type_sql}"
        func_sql += ")"
        return self._finish_function_call(func_sql, all_params, expr)

    def format_oracle_json_query_function(
        self, expr: "OracleJsonQueryFunction"
    ) -> Tuple[str, tuple]:
        """``JSON_QUERY(json_doc, path [RETURNING type])``.

        Extracts a JSON object or array from a document -- the same shape
        :class:`OracleJsonValueFunction` has, answering with a document
        instead of a scalar.
        """
        self.check_function_version(expr.func_name)
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
        func_sql = f"JSON_QUERY({', '.join(args_sql)}"
        if expr.returning_type is not None:
            returning = self._coerce_returning_type(expr.returning_type)
            type_sql, _ = self.format_data_type(returning)
            func_sql += f" RETURNING {type_sql}"
        func_sql += ")"
        return self._finish_function_call(func_sql, all_params, expr)

    def _coerce_returning_type(self, returning):
        """Validate and coerce a raw RETURNING argument at render time."""
        from rhosocial.activerecord.backend.expression.types._base import DataType

        if isinstance(returning, DataType):
            return returning
        if not isinstance(returning, str):
            raise TypeError(
                f"returning must be a DataType instance or type-name string, "
                f"got {type(returning).__name__}"
            )
        from ..functions.json import parse_returning_type_spec, _RETURNING_WHITELIST

        spec = parse_returning_type_spec(returning.strip())
        if spec is None:
            raise TypeError(
                f"Unsupported Oracle RETURNING type: {returning!r}. "
                f"Use a DataType instance or one of: "
                f"{', '.join(sorted(_RETURNING_WHITELIST))}."
            )
        base_name = spec.name.upper()
        type_spec = _RETURNING_WHITELIST.get(base_name)
        if type_spec is None:
            raise TypeError(
                f"Unsupported Oracle RETURNING type: {returning!r}. "
                f"Allowed: {', '.join(sorted(_RETURNING_WHITELIST))}."
            )
        cls_name, kwargs = type_spec
        from rhosocial.activerecord.backend.expression import types as _types

        cls = getattr(_types, cls_name, None)
        if cls is None:
            from ..expression import types as _oracle_types

            cls = getattr(_oracle_types, cls_name)
        if spec.precision:
            if cls_name == "VarCharType":
                kwargs = {"length": int(spec.precision)}
            elif cls_name == "DecimalType":
                p = int(spec.precision)
                s = int(spec.scale) if spec.scale else None
                kwargs = {"precision": p, "scale": s}
        return cls(dialect=self, **kwargs)

    def format_oracle_json_table_function(
        self, expr: "OracleJsonTableFunction"
    ) -> Tuple[str, tuple]:
        """``JSON_TABLE(json_doc, path [COLUMNS (...)])``."""
        self.check_function_version(expr.func_name)
        args_sql: List[str] = []
        all_params: List[Any] = []
        for arg in expr.args:
            sql_part, params_part = arg.to_sql()
            args_sql.append(sql_part)
            all_params.extend(params_part)
        func_sql = f"JSON_TABLE({', '.join(args_sql)}"
        if expr.columns:
            func_sql += f" COLUMNS ({expr.columns}))"
        else:
            func_sql += ")"
        return self._finish_function_call(func_sql, all_params, expr)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

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
