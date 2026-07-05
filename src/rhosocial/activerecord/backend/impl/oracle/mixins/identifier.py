# src/rhosocial/activerecord/backend/impl/oracle/mixins/identifier.py
"""Oracle identifier / column / table formatting mixin."""

from typing import Any, Dict, List, Optional, Tuple


class OracleIdentifierMixin:
    """Oracle-specific identifier, column, and table reference formatting.

    Oracle stores unquoted identifiers as uppercase. This mixin provides
    the formatters that uppercase identifiers when generating SQL.
    """

    def format_identifier(self, identifier: str) -> str:
        return identifier.upper()

    def format_column(
        self, name: str, table: Optional[str] = None,
        alias: Optional[str] = None, schema_name: Optional[str] = None,
    ) -> Tuple[str, Tuple]:
        """Format column reference for Oracle queries."""
        if schema_name and table:
            col_sql = f"{self.format_identifier(schema_name)}.{self.format_identifier(table)}.{name}"
        elif table:
            col_sql = f"{self.format_identifier(table)}.{name}"
        else:
            col_sql = name
        if alias:
            return f"{col_sql} AS {self.format_identifier(alias)}", ()
        return col_sql, ()

    def format_table(
        self, table_name: str, alias: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Tuple[str, Tuple]:
        """Format table reference for Oracle."""
        if schema_name:
            table_sql = f"{self.format_identifier(schema_name)}.{self.format_identifier(table_name)}"
        elif table_name.lower().endswith("_cte"):
            table_sql = table_name
        else:
            table_sql = self.format_identifier(table_name)
        if alias:
            return f"{table_sql} {self.format_identifier(alias)}", ()
        return table_sql, ()

    def format_cte(
        self,
        name: str,
        query_sql: str,
        columns: Optional[List[str]] = None,
        recursive: bool = False,
        materialized: Optional[bool] = None,
        dialect_options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Format a CTE definition for Oracle queries."""
        materialized_hint = ""
        if materialized is not None:
            materialized_hint = "MATERIALIZED " if materialized else "NOT MATERIALIZED "
        columns_part = f" ({', '.join(columns)})" if columns else ""
        return f"{name}{columns_part} AS {materialized_hint}({query_sql})"