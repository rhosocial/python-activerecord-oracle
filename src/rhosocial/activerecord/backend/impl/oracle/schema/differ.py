# src/rhosocial/activerecord/backend/impl/oracle/schema/differ.py
"""Oracle schema differ — type-name-based column comparison."""

from rhosocial.activerecord.backend.schema.differ import SchemaDiffer


class OracleSchemaDiffer(SchemaDiffer):
    """Oracle schema differ.

    Oracle uses ``VARCHAR2``/``NUMBER`` type names; no ordinal position
    sensitivity needed since ``ALTER TABLE ADD`` always appends.
    """

    def _columns_equivalent(self, old_col, new_col) -> bool:
        return old_col.data_type == new_col.data_type