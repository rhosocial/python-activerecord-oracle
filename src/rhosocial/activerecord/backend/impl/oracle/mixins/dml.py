# src/rhosocial/activerecord/backend/impl/oracle/mixins/dml.py
from typing import Any, List, Optional, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class OracleDMLOperationMixin(object):
    """Oracle DML operations mixin."""

    def supports_insert_ignore(self) -> bool:
        return False

    def supports_replace_into(self) -> bool:
        return False

    def supports_load_data(self) -> bool:
        return True

    def supports_merge_statement(self) -> bool:
        return True

    def supports_insert_all(self) -> bool:
        return True

    def supports_returning_into(self) -> bool:
        return True

    def supports_multi_table_insert(self) -> bool:
        return True

    def supports_hint_in_insert(self) -> bool:
        return True

    def format_on_conflict_clause(self, conflict_clause) -> Tuple[str, tuple]:
        """Format ON CONFLICT clause for Oracle.

        Oracle has no native ON CONFLICT semantics; the equivalent is the
        MERGE statement. This method does not produce MERGE SQL directly
        (that is handled by ``format_merge_statement``); it only validates
        the requested action and returns a placeholder fragment.
        """
        action = getattr(conflict_clause, "action", None)
        if action == "do_update":
            raise NotImplementedError(
                "Oracle ON CONFLICT DO UPDATE is expressed via MERGE; "
                "use format_merge_statement instead."
            )
        return "", ()

    def supports_insert_first(self) -> bool:
        return True

    def format_merge_statement(self, target_table: str, source_query: str,
                               match_condition: str, update_setters: str,
                               insert_columns: str, insert_values: str,
                               *,
                               delete_where: Optional[str] = None,
                               log_errors_into: Optional[str] = None,
                               reject_limit: Optional[int] = None,
                               dialect_options: Optional[dict] = None) -> Tuple[str, tuple]:
        """Format an Oracle-standard MERGE statement.

        Composes caller-supplied SQL fragments into the canonical pattern:

            MERGE INTO target_table t
            USING (source_query) s
            ON (match_condition)
            WHEN MATCHED THEN UPDATE SET ...
                DELETE WHERE (cond)          -- 10g conditional delete
            WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
            LOG ERRORS INTO err$_t REJECT LIMIT 25   -- 10g DML error logging

        Args:
            target_table: formatted target table reference.
            source_query: formatted source (sub)query.
            match_condition: formatted ON match condition.
            update_setters: formatted ``SET col = value`` fragment.
            insert_columns: formatted column list for the INSERT branch.
            insert_values: formatted value list for the INSERT branch.
            delete_where: optional ``DELETE WHERE (cond)`` fragment attached
                to the ``WHEN MATCHED`` branch (Oracle 10g+).
            log_errors_into: optional error-logging table name; emits
                ``LOG ERRORS INTO <table>`` (Oracle 10g+).
            reject_limit: optional ``REJECT LIMIT n`` for the error-logging
                clause.
            dialect_options: reserved for future dialect-specific options.
        """
        if delete_where is not None and self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "MERGE ... DELETE WHERE",
                suggestion=(
                    f"Oracle {self.version} does not support the "
                    "conditional DELETE branch of MERGE; it requires "
                    "Oracle 10g or later."
                ),
            )
        if log_errors_into is not None and self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "MERGE ... LOG ERRORS INTO",
                suggestion=(
                    f"Oracle {self.version} does not support DML error "
                    "logging; it requires Oracle 10g or later."
                ),
            )
        parts = [f"MERGE INTO {target_table} t"]
        parts.append(f"USING ({source_query}) s")
        parts.append(f"ON ({match_condition})")
        if update_setters:
            matched_parts = [f"WHEN MATCHED THEN UPDATE SET {update_setters}"]
            if delete_where:
                matched_parts.append(f"DELETE WHERE ({delete_where})")
            parts.append(" ".join(matched_parts))
        if insert_columns and insert_values:
            parts.append(
                f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
            )
        if log_errors_into:
            log_sql = f"LOG ERRORS INTO {log_errors_into}"
            if reject_limit is not None:
                log_sql += f" REJECT LIMIT {int(reject_limit)}"
            parts.append(log_sql)
        return " ".join(parts), ()

    def format_insert_all_statement(
        self,
        into_clauses: List[dict],
        select_query: Optional[str] = None,
        *,
        when_clauses: Optional[List[dict]] = None,
        else_clause: Optional[dict] = None,
        dialect_options: Optional[dict] = None,
    ) -> Tuple[str, tuple]:
        """Format an Oracle ``INSERT ALL`` multi-table insert statement.

        Unconditional ``INTO t (cols) VALUES (...)`` clauses are given as a
        list of dicts with keys ``table`` / ``columns`` / ``values``;
        conditional branches are supplied through ``when_clauses`` (each a
        dict with an extra ``condition`` key) and an optional ``else_clause``.
        A trailing ``SELECT ... FROM src`` is appended when ``select_query``
        is provided.

        Values may be plain SQL fragment strings or ``BaseExpression``
        objects (rendered with parameter binding).
        """
        return self.format_multi_table_insert_statement(
            "ALL", into_clauses, select_query,
            when_clauses=when_clauses, else_clause=else_clause,
            dialect_options=dialect_options,
        )

    def format_insert_first_statement(
        self,
        into_clauses: List[dict],
        select_query: Optional[str] = None,
        *,
        when_clauses: Optional[List[dict]] = None,
        else_clause: Optional[dict] = None,
        dialect_options: Optional[dict] = None,
    ) -> Tuple[str, tuple]:
        """Format an Oracle ``INSERT FIRST`` multi-table insert statement.

        Identical to :meth:`format_insert_all_statement` but emits the
        ``FIRST`` keyword: only the first matching ``WHEN`` branch is
        evaluated per source row.
        """
        return self.format_multi_table_insert_statement(
            "FIRST", into_clauses, select_query,
            when_clauses=when_clauses, else_clause=else_clause,
            dialect_options=dialect_options,
        )

    def format_multi_table_insert_statement(
        self,
        keyword: str,
        into_clauses: List[dict],
        select_query: Optional[str],
        *,
        when_clauses: Optional[List[dict]] = None,
        else_clause: Optional[dict] = None,
        dialect_options: Optional[dict] = None,
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                f"INSERT {keyword}",
                suggestion=(
                    f"Oracle {self.version} does not support multi-table "
                    "insert; it requires Oracle 9i or later."
                ),
            )
        params: List[Any] = []
        parts = [f"INSERT {keyword}"]
        for spec in into_clauses:
            parts.append(self.format_insert_into_spec(spec, params))
        for spec in when_clauses or []:
            condition = spec.get("condition")
            cond_sql = self._render_insert_fragment(condition, params)
            parts.append(f"WHEN {cond_sql} THEN {self.format_insert_into_spec(spec, params)}")
        if else_clause:
            parts.append(f"ELSE {self.format_insert_into_spec(else_clause, params)}")
        if select_query:
            parts.append(select_query)
        return " ".join(parts), tuple(params)

    def format_insert_into_spec(self, spec: dict, params: List[Any]) -> str:
        table = spec["table"]
        columns = spec.get("columns") or ""
        values = spec.get("values") or ""
        if isinstance(columns, (list, tuple)):
            columns_sql = ", ".join(self.format_identifier(c) for c in columns)
        elif columns:
            columns_sql = self.format_identifier(columns)
        else:
            columns_sql = ""
        cols_part = f"({columns_sql})" if columns_sql else ""
        values_sql = self._render_insert_fragment(values, params)
        return f"INTO {self.format_identifier(table)} {cols_part} VALUES ({values_sql})".strip()

    def _render_insert_fragment(self, value: Any, params: List[Any]) -> str:
        if isinstance(value, str):
            return value
        if hasattr(value, "to_sql"):
            sql, value_params = value.to_sql()
            params.extend(value_params)
            return sql
        return str(value)
