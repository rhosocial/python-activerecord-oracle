# src/rhosocial/activerecord/backend/impl/oracle/mixins/identifier.py
"""Oracle identifier / column / table formatting mixin."""

from typing import Any, Dict, List, Optional, Tuple


class OracleIdentifierMixin:
    """Oracle-specific identifier, column, and table reference formatting.

    Oracle stores unquoted identifiers as uppercase. This mixin provides
    formatters that uppercase and double-quote identifiers when generating
    SQL. Quoted uppercase identifiers (``"USERS"``) are semantically
    identical to unquoted ``USERS`` in Oracle.
    """

    def format_identifier(self, identifier: str) -> str:
        """Format identifier for Oracle (uppercase, no quoting).

        Oracle stores unquoted identifiers as uppercase. Global quoting is
        avoided so generated SQL keeps the conventional unquoted form;
        callers handling externally-sourced identifiers quote explicitly.
        """
        return identifier.upper()

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Double-quote an Oracle identifier (uppercased) for safe embedding.

        Used on externally-sourced identifiers (bulk DML columns/tables, LOB
        write targets, COMMENT ON targets) where defense-in-depth quoting is
        warranted. Dot-separated qualified paths are quoted segment-by-segment.
        """
        if "." in identifier:
            return ".".join(
                f'"{part.replace(chr(34), chr(34) * 2).upper()}"'
                for part in identifier.split(".")
            )
        return f'"{identifier.replace(chr(34), chr(34) * 2).upper()}"'

    def format_column(
        self, name: str, table: Optional[str] = None,
        alias: Optional[str] = None, schema_name: Optional[str] = None,
    ) -> Tuple[str, Tuple]:
        """Format column reference for Oracle queries.

        Column references accept at most ``TABLE.COLUMN``: the schema is
        implied by the statement target, and three-part references are
        rejected with an "invalid identifier" error.
        """
        if table:
            col_sql = f"{self.format_identifier(table)}.{name}"
        elif table is None and schema_name:
            col_sql = f"{self.format_identifier(schema_name)}.{name}"
        else:
            col_sql = name
        if alias:
            return f"{col_sql} AS {self.format_identifier(alias)}", ()
        return col_sql, ()

    def format_table(
        self, table: str, alias: Optional[str] = None,
        schema_name: Optional[str] = None,
        dblink: Optional[str] = None,
        flashback: Optional[Any] = None,
    ) -> Tuple[str, Tuple]:
        """Format table reference for Oracle.

        Args:
            table: the table name.
            alias: optional table alias.
            schema_name: optional schema qualifier.
            dblink: optional database link name appended as ``@dblink`` to
                reference a remote table.
            flashback: optional flashback clause (an ``OracleAsOfClause`` /
                ``OracleVersionsBetweenClause`` or any object exposing
                ``to_sql()``) appended after the table reference.
        """
        if schema_name:
            table_sql = f"{self.format_identifier(schema_name)}.{self.format_identifier(table)}"
        elif table.lower().endswith("_cte"):
            table_sql = table
        else:
            table_sql = self.format_identifier(table)
        table_params: Tuple = ()
        if dblink:
            table_sql = f"{table_sql}@{self.format_identifier(dblink)}"
        if flashback is not None:
            flash_sql, flash_params = flashback.to_sql()
            table_sql = f"{table_sql} {flash_sql}"
            table_params = tuple(flash_params)
        if alias:
            return f"{table_sql} {self.format_identifier(alias)}", table_params
        return table_sql, table_params

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