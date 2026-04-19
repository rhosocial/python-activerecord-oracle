# expression/pivot.py
"""
Oracle PIVOT and UNPIVOT expressions.

PIVOT rotates rows to columns, creating a cross-tabulation query.
UNPIVOT rotates columns to rows, the inverse of PIVOT.

These operations are useful for reporting and data transformation.
"""

from dataclasses import dataclass, field
from typing import List, Union, Optional, Tuple, Any


@dataclass
class PivotExpression:
    """Oracle PIVOT clause for row-to-column transformation.
    
    PIVOT allows you to rotate rows into columns, creating a
    cross-tabulation query. This is useful for generating
    reports where values in one column become column headers.
    
    Example SQL:
        SELECT * FROM sales
        PIVOT (SUM(amount) FOR month IN ('Jan' AS "Jan", 'Feb' AS "Feb"))
    
    Example usage:
        pivot = PivotExpression(
            aggregate_function="SUM",
            value_column="amount",
            pivot_column="month",
            values=["Jan", "Feb", "Mar"]
        )
    
    Attributes:
        aggregate_function: Aggregate function (SUM, COUNT, AVG, MAX, MIN)
        value_column: Column to aggregate
        pivot_column: Column whose values become new column names
        values: List of values to pivot into columns
        alias: Optional alias for the pivoted result
        default: Default value for non-existent values (Oracle 18c+)
    """
    aggregate_function: str
    value_column: str
    pivot_column: str
    values: List[Union[str, int, float]] = field(default_factory=list)
    alias: Optional[str] = None
    default: Optional[Any] = None
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate PIVOT SQL."""
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

        return (sql, [])


@dataclass
class PivotXMLExpression:
    """Oracle PIVOT XML clause for dynamic pivoting.
    
    PIVOT XML allows for dynamic column specification using
    a subquery, which is useful when the pivot values are
    not known in advance.
    
    Example SQL:
        SELECT * FROM sales
        PIVOT XML (SUM(amount) FOR month IN (SELECT DISTINCT month FROM sales))
    
    Attributes:
        aggregate_function: Aggregate function
        value_column: Column to aggregate
        pivot_column: Column to pivot
        subquery: Subquery to get pivot values
    """
    aggregate_function: str
    value_column: str
    pivot_column: str
    subquery: str
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate PIVOT XML SQL."""
        sql = (
            f"PIVOT XML ("
            f"{self.aggregate_function}({dialect.format_identifier(self.value_column)}) "
            f"FOR {dialect.format_identifier(self.pivot_column)} "
            f"IN ({self.subquery})"
            f")"
        )
        return (sql, [])


@dataclass
class UnpivotExpression:
    """Oracle UNPIVOT clause for column-to-row transformation.
    
    UNPIVOT rotates columns into rows, essentially the inverse of PIVOT.
    This is useful for normalizing denormalized data.
    
    Example SQL:
        SELECT * FROM sales_pivot
        UNPIVOT (amount FOR month IN (jan_sales, feb_sales, mar_sales))
    
    Example usage:
        unpivot = UnpivotExpression(
            value_column="amount",
            pivot_column="month",
            columns=["jan_sales", "feb_sales", "mar_sales"]
        )
    
    Attributes:
        value_column: Name for the value column in output
        pivot_column: Name for the pivot column in output
        columns: List of columns to unpivot
        include_nulls: If True, include NULL values (default: exclude)
        alias: Optional alias for the unpivoted result
    """
    value_column: str
    pivot_column: str
    columns: List[str] = field(default_factory=list)
    include_nulls: bool = False
    alias: Optional[str] = None
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate UNPIVOT SQL."""
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
        
        return (sql, [])


@dataclass
class UnpivotColumnsExpression:
    """Oracle UNPIVOT with column aliasing.
    
    Allows specifying aliases for unpivoted columns.
    
    Attributes:
        value_column: Name for value column
        pivot_column: Name for pivot column
        column_aliases: Dict mapping column names to aliases
        include_nulls: Include NULL values
    """
    value_column: str
    pivot_column: str
    column_aliases: dict = field(default_factory=dict)
    include_nulls: bool = False
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate UNPIVOT SQL with aliases."""
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
        
        return (sql, [])
