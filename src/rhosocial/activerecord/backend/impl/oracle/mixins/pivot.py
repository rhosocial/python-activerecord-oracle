# src/rhosocial/activerecord/backend/impl/oracle/mixins/pivot.py
"""Oracle PIVOT / UNPIVOT support mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.pivot import (
        OracleUnpivot,
        PivotXMLExpression,
        UnpivotColumnsExpression,
    )


class OraclePivotMixin:
    """Oracle PIVOT / UNPIVOT capability checks (11g+)."""

    def supports_pivot(self) -> bool:
        return self.version_at_least((11, 0, 0))

    def supports_unpivot(self) -> bool:
        return self.version_at_least((11, 0, 0))

    def supports_pivot_xml(self) -> bool:
        return self.version_at_least((11, 0, 0))

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

    def format_pivot_xml_expression(
        self, expr: "PivotXMLExpression"
    ) -> Tuple[str, tuple]:
        """Render a ``PIVOT XML`` clause with its dynamic ``IN`` subquery."""
        sql = (
            f"PIVOT XML ("
            f"{expr.aggregate_function}({self.format_identifier(expr.value_column)}) "
            f"FOR {self.format_identifier(expr.pivot_column)} "
            f"IN ({expr.subquery})"
            f")"
        )
        return (sql, ())

    def format_oracle_unpivot(self, expr: "OracleUnpivot") -> Tuple[str, tuple]:
        """Render an ``OracleUnpivot`` clause with its NULL handling."""
        include = "INCLUDE NULLS " if expr.include_nulls else "EXCLUDE NULLS "
        columns_str = ", ".join(
            self.format_identifier(c) for c in expr.columns
        )

        sql = (
            f"UNPIVOT {include}"
            f"({self.format_identifier(expr.value_column)} "
            f"FOR {self.format_identifier(expr.pivot_column)} "
            f"IN ({columns_str}))"
        )

        if expr.alias:
            sql = f"{sql} {self.format_identifier(expr.alias)}"

        return (sql, ())

    def format_unpivot_columns_expression(
        self, expr: "UnpivotColumnsExpression"
    ) -> Tuple[str, tuple]:
        """Render an UNPIVOT clause whose columns carry output aliases.

        The aliases are rendered as Oracle's quoted output labels, so they sit
        inside the ``IN`` list beside the columns they rename.
        """
        include = "INCLUDE NULLS " if expr.include_nulls else "EXCLUDE NULLS "

        alias_parts = []
        for col, alias in expr.column_aliases.items():
            alias_parts.append(f"{self.format_identifier(col)} AS '{alias}'")
        columns_str = ", ".join(alias_parts)

        sql = (
            f"UNPIVOT {include}"
            f"({self.format_identifier(expr.value_column)} "
            f"FOR {self.format_identifier(expr.pivot_column)} "
            f"IN ({columns_str}))"
        )

        return (sql, ())
