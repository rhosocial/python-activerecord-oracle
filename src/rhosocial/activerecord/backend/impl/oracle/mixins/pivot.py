# src/rhosocial/activerecord/backend/impl/oracle/mixins/pivot.py
"""Oracle PIVOT / UNPIVOT support mixin."""

from typing import Tuple


class OraclePivotMixin:
    """Oracle PIVOT / UNPIVOT capability checks (11g+)."""

    def supports_pivot(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_unpivot(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_pivot_xml(self) -> bool:
        return self.version >= (11, 0, 0)

    def format_oracle_pivot(self, expr) -> Tuple[str, tuple]:
        """Render an ``OraclePivot`` clause.

        The clause's values are rendered in place rather than bound, because
        a PIVOT's ``IN`` list names *columns* -- each value becomes a column
        name, and a column name cannot be a bind parameter. The ``DEFAULT``
        is a value and is the one part that goes through ``format_literal``,
        so a value a caller supplies is escaped by the library rather than
        interpolated by this method.
        """
        dialect = self
        value_parts = []
        for value in expr.values:
            if isinstance(value, str):
                value_parts.append(f"'{value}' AS \"{value}\"")
            else:
                value_parts.append(f"{value} AS \"{value}\"")

        values_str = ", ".join(value_parts)
        sql = (
            "PIVOT ("
            f"{expr.aggregate_function}"
            f"({dialect.format_identifier(expr.value_column)}) "
            f"FOR {dialect.format_identifier(expr.pivot_column)} "
            f"IN ({values_str})"
        )

        if expr.default is not None:
            sql += f" DEFAULT {dialect.format_literal(expr.default)}"
        sql += ")"

        if expr.alias:
            sql = f"{sql} {dialect.format_identifier(expr.alias)}"
        return (sql, ())