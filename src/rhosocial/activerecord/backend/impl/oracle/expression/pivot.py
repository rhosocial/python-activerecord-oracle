# src/rhosocial/activerecord/backend/impl/oracle/expression/pivot.py
"""
Oracle PIVOT and UNPIVOT expressions.

PIVOT rotates rows to columns, creating a cross-tabulation query.
UNPIVOT rotates columns to rows, the inverse of PIVOT.

These operations are useful for reporting and data transformation.
"""
from __future__ import annotations

from typing import List, Union, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class PivotExpression(BaseExpression):
    """Oracle PIVOT clause for row-to-column transformation.

    PIVOT allows you to rotate rows into columns, creating a
    cross-tabulation query. This is useful for generating
    reports where values in one column become column headers.

    Example SQL:
        SELECT * FROM sales
        PIVOT (SUM(amount) FOR month IN ('Jan' AS "Jan", 'Feb' AS "Feb"))

    Example usage:
        pivot = PivotExpression(
            dialect,
            aggregate_function="SUM",
            value_column="amount",
            pivot_column="month",
            values=["Jan", "Feb", "Mar"]
        )

    Args:
        dialect: the Oracle dialect instance.
        aggregate_function: Aggregate function (SUM, COUNT, AVG, MAX, MIN)
        value_column: Column to aggregate
        pivot_column: Column whose values become new column names
        values: List of values to pivot into columns
        alias: Optional alias for the pivoted result
        default: Default value for non-existent values (Oracle 18c+)
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        aggregate_function: str,
        value_column: str,
        pivot_column: str,
        values: Optional[List[Union[str, int, float]]] = None,
        alias: Optional[str] = None,
        default: Optional[object] = None,
    ):
        super().__init__(dialect)
        self.aggregate_function = aggregate_function
        self.value_column = value_column
        self.pivot_column = pivot_column
        self.values = list(values) if values else []
        self.alias = alias
        self.default = default

    def to_sql(self) -> SQLQueryAndParams:
        """Generate PIVOT SQL."""
        dialect = self.dialect
        # Format values with aliases
        value_parts = []
        for v in self.values:
            if isinstance(v, str):
                value_parts.append(f"'{v}' AS \"{v}\"")
            else:
                value_parts.append(f"{v} AS \"{v}\"")

        values_str = ", ".join(value_parts)

        sql = (
            f"PIVOT ("
            f"{self.aggregate_function}({dialect.format_identifier(self.value_column)}) "
            f"FOR {dialect.format_identifier(self.pivot_column)} "
            f"IN ({values_str})"
        )

        if self.default is not None:
            sql += f" DEFAULT {dialect.format_literal(self.default)}"

        sql += ")"

        if self.alias:
            sql = f"{sql} {dialect.format_identifier(self.alias)}"

        return (sql, ())


class PivotXMLExpression(BaseExpression):
    """Oracle PIVOT XML clause for dynamic pivoting.

    PIVOT XML allows for dynamic column specification using
    a subquery, which is useful when the pivot values are
    not known in advance.

    Example SQL:
        SELECT * FROM sales
        PIVOT XML (SUM(amount) FOR month IN (SELECT DISTINCT month FROM sales))

    Args:
        dialect: the Oracle dialect instance.
        aggregate_function: Aggregate function
        value_column: Column to aggregate
        pivot_column: Column to pivot
        subquery: Subquery to get pivot values
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        aggregate_function: str,
        value_column: str,
        pivot_column: str,
        subquery: str,
    ):
        super().__init__(dialect)
        self.aggregate_function = aggregate_function
        self.value_column = value_column
        self.pivot_column = pivot_column
        self.subquery = subquery

    def to_sql(self) -> SQLQueryAndParams:
        """Generate PIVOT XML SQL."""
        dialect = self.dialect
        sql = (
            f"PIVOT XML ("
            f"{self.aggregate_function}({dialect.format_identifier(self.value_column)}) "
            f"FOR {dialect.format_identifier(self.pivot_column)} "
            f"IN ({self.subquery})"
            f")"
        )
        return (sql, ())


class UnpivotExpression(BaseExpression):
    """Oracle UNPIVOT clause for column-to-row transformation.

    UNPIVOT rotates columns into rows, essentially the inverse of PIVOT.
    This is useful for normalizing denormalized data.

    Example SQL:
        SELECT * FROM sales_pivot
        UNPIVOT (amount FOR month IN (jan_sales, feb_sales, mar_sales))

    Example usage:
        unpivot = UnpivotExpression(
            dialect,
            value_column="amount",
            pivot_column="month",
            columns=["jan_sales", "feb_sales", "mar_sales"]
        )

    Args:
        dialect: the Oracle dialect instance.
        value_column: Name for the value column in output
        pivot_column: Name for the pivot column in output
        columns: List of columns to unpivot
        include_nulls: If True, include NULL values (default: exclude)
        alias: Optional alias for the unpivoted result
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        value_column: str,
        pivot_column: str,
        columns: Optional[List[str]] = None,
        include_nulls: bool = False,
        alias: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.value_column = value_column
        self.pivot_column = pivot_column
        self.columns = list(columns) if columns else []
        self.include_nulls = bool(include_nulls)
        self.alias = alias

    def to_sql(self) -> SQLQueryAndParams:
        """Generate UNPIVOT SQL."""
        dialect = self.dialect
        include = "INCLUDE NULLS " if self.include_nulls else "EXCLUDE NULLS "
        columns_str = ", ".join(dialect.format_identifier(c) for c in self.columns)

        sql = (
            f"UNPIVOT {include}"
            f"({dialect.format_identifier(self.value_column)} "
            f"FOR {dialect.format_identifier(self.pivot_column)} "
            f"IN ({columns_str}))"
        )

        if self.alias:
            sql = f"{sql} {dialect.format_identifier(self.alias)}"

        return (sql, ())


class UnpivotColumnsExpression(BaseExpression):
    """Oracle UNPIVOT with column aliasing.

    Allows specifying aliases for unpivoted columns.

    Args:
        dialect: the Oracle dialect instance.
        value_column: Name for value column
        pivot_column: Name for pivot column
        column_aliases: Dict mapping column names to aliases
        include_nulls: Include NULL values
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        value_column: str,
        pivot_column: str,
        column_aliases: Optional[dict] = None,
        include_nulls: bool = False,
    ):
        super().__init__(dialect)
        self.value_column = value_column
        self.pivot_column = pivot_column
        self.column_aliases = dict(column_aliases) if column_aliases else {}
        self.include_nulls = bool(include_nulls)

    def to_sql(self) -> SQLQueryAndParams:
        """Generate UNPIVOT SQL with aliases."""
        dialect = self.dialect
        include = "INCLUDE NULLS " if self.include_nulls else "EXCLUDE NULLS "

        alias_parts = []
        for col, alias in self.column_aliases.items():
            alias_parts.append(f"{dialect.format_identifier(col)} AS '{alias}'")
        columns_str = ", ".join(alias_parts)

        sql = (
            f"UNPIVOT {include}"
            f"({dialect.format_identifier(self.value_column)} "
            f"FOR {dialect.format_identifier(self.pivot_column)} "
            f"IN ({columns_str}))"
        )

        return (sql, ())
