# src/rhosocial/activerecord/backend/impl/oracle/mixins/dml.py
from typing import Tuple


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

    def format_merge_statement(self, target_table: str, source_query: str,
                               match_condition: str, update_setters: str,
                               insert_columns: str, insert_values: str) -> Tuple[str, tuple]:
        """Format an Oracle-standard MERGE statement.

        Composes caller-supplied SQL fragments into the canonical pattern:

            MERGE INTO target_table t
            USING (source_query) s
            ON (match_condition)
            WHEN MATCHED THEN UPDATE SET ...
            WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
        """
        parts = [f"MERGE INTO {target_table} t"]
        parts.append(f"USING ({source_query}) s")
        parts.append(f"ON ({match_condition})")
        if update_setters:
            parts.append(f"WHEN MATCHED THEN UPDATE SET {update_setters}")
        if insert_columns and insert_values:
            parts.append(
                f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
            )
        return " ".join(parts), ()
