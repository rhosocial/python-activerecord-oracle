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
        return self.apply_alias(sql, source_params, expr)

    def format_interval_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        unit = expr.unit.value.upper()
        if unit in {"YEAR", "MONTH"}:
            sql = f"NUMTOYMINTERVAL({self.p()}, '{unit}')"
        elif unit == "WEEK":
            sql = f"NUMTODSINTERVAL({self.p()} * 7, 'DAY')"
        else:
            sql = f"NUMTODSINTERVAL({self.p()}, '{unit}')"
        return self.apply_alias(sql, (expr.value,), expr)

    def format_datetime_add_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        source_sql, source_params = expr.source.to_sql()
        interval_sql, interval_params = expr.interval.to_sql()
        sql = f"{source_sql} + {interval_sql}"
        return self.apply_alias(
            sql, source_params + interval_params, expr
        )

    def format_datetime_subtract_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        source_sql, source_params = expr.source.to_sql()
        interval_sql, interval_params = expr.interval.to_sql()
        sql = f"{source_sql} - {interval_sql}"
        return self.apply_alias(
            sql, source_params + interval_params, expr
        )

    def format_datetime_diff_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        """Difference between two dates, in the unit asked for.

        Oracle cannot subtract one DATE from another -- the ``-`` operator is
        defined for numbers and intervals, and ``end - start`` on two dates is
        an ORA-00932. The difference has to be taken through an interval, which
        is what every unit here is expressed as: seconds by default, divided
        down for the finer units.
        """
        start_sql, start_params = expr.start.to_sql()
        end_sql, end_params = expr.end.to_sql()
        unit = expr.unit.value
        # Seconds is the unit Oracle can express directly; the rest divide it.
        divisors = {"day": None, "hour": 3600, "minute": 60,
                    "second": 1, "week": 604800}
        if unit in divisors:
            seconds = (
                f"(ROUND((CAST({end_sql} AS DATE) - CAST({start_sql} AS DATE))"
                f" * 86400))"
            )
            if divisors[unit] and divisors[unit] != 1:
                seconds = f"({seconds} / {divisors[unit]})"
            sql = seconds
        elif unit == "month":
            sql = f"MONTHS_BETWEEN({end_sql}, {start_sql})"
        else:
            sql = f"(MONTHS_BETWEEN({end_sql}, {start_sql}) / 12)"
        return self.apply_alias(sql, end_params + start_params, expr)