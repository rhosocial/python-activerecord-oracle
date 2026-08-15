# src/rhosocial/activerecord/backend/impl/oracle/mixins/datetime_op.py
"""Oracle date/time expression formatting mixin."""

from typing import Any, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class OracleDateTimeMixin:
    """Oracle-specific date/time expression formatters.

    Provides Oracle-flavoured implementations of date_trunc, interval
    expressions, and datetime arithmetic that differ from the generic
    ``DateTimeMixin`` defaults.
    """

    def format_date_trunc_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        source_sql, source_params = expr.source.to_sql()
        formats = {
            "year": "YYYY",
            "month": "MM",
            "day": "DD",
            "hour": "HH24",
            "minute": "MI",
        }
        if expr.field.value == "second":
            sql = source_sql
        elif expr.field.value in formats:
            sql = f"TRUNC({source_sql}, '{formats[expr.field.value]}')"
        else:
            raise UnsupportedFeatureError(
                self.name, f"date_trunc({expr.field.value})"
            )
        return self._apply_value_expression_modifiers(sql, source_params, expr)

    def format_interval_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        unit = expr.unit.value.upper()
        if unit in {"YEAR", "MONTH"}:
            sql = f"NUMTOYMINTERVAL(?, '{unit}')"
        elif unit == "WEEK":
            sql = "NUMTODSINTERVAL(? * 7, 'DAY')"
        else:
            sql = f"NUMTODSINTERVAL(?, '{unit}')"
        return self._apply_value_expression_modifiers(sql, (expr.value,), expr)

    def format_datetime_add_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        source_sql, source_params = expr.source.to_sql()
        interval_sql, interval_params = expr.interval.to_sql()
        sql = f"{source_sql} + {interval_sql}"
        return self._apply_value_expression_modifiers(
            sql, source_params + interval_params, expr
        )

    def format_datetime_subtract_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        source_sql, source_params = expr.source.to_sql()
        interval_sql, interval_params = expr.interval.to_sql()
        sql = f"{source_sql} - {interval_sql}"
        return self._apply_value_expression_modifiers(
            sql, source_params + interval_params, expr
        )

    def format_datetime_diff_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        start_sql, start_params = expr.start.to_sql()
        end_sql, end_params = expr.end.to_sql()
        day_diff = f"({end_sql} - {start_sql})"
        factors = {
            "day": "1",
            "hour": "24",
            "minute": "1440",
            "second": "86400",
            "week": "1 / 7",
        }
        if expr.unit.value in factors:
            sql = f"({day_diff} * {factors[expr.unit.value]})"
        elif expr.unit.value == "month":
            sql = f"MONTHS_BETWEEN({end_sql}, {start_sql})"
        else:
            sql = f"(MONTHS_BETWEEN({end_sql}, {start_sql}) / 12)"
        return self._apply_value_expression_modifiers(sql, end_params + start_params, expr)