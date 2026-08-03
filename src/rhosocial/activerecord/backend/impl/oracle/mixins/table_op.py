# src/rhosocial/activerecord/backend/impl/oracle/mixins/table_op.py
"""Oracle table DDL support mixin (capability flags for table operations).

The existing ``table.py`` contains ``OracleTableMixin`` which handles
table-level operations. This module provides the remaining ``supports_*``
capability checks that were previously inlined in the monolithic
``dialect.py``.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ...expression.statements import DropTableExpression


class OracleTableCapabilityMixin:
    """Oracle table operation capability checks."""

    def supports_if_not_exists_table(self) -> bool:
        return False

    def supports_if_exists_table(self) -> bool:
        return False

    def supports_temporary_table(self) -> bool:
        return True

    def supports_drop_table_cascade(self) -> bool:
        """Oracle does NOT accept the bare CASCADE keyword on DROP TABLE.

        Oracle uses the dialect-specific ``CASCADE CONSTRAINTS`` form, which is
        narrower than SQL-standard CASCADE (only drops constraints that
        reference this table, not views/triggers/materialized views/synonyms).
        """
        return False

    def supports_drop_table_restrict(self) -> bool:
        """Oracle does NOT accept the RESTRICT keyword on DROP TABLE."""
        return False

    def supports_cascade_constraints(self) -> bool:
        """Oracle supports the backend-specific ``CASCADE CONSTRAINTS`` form.

        Semantics: drop all referential and check constraints that reference
        this table. Does NOT drop views, triggers, materialized views or
        synonyms (they must be dropped separately, or the DROP will fail with
        ORA-02449).
        """
        return True

    def supports_purge_on_drop_table(self) -> bool:
        """Oracle supports the PURGE option on DROP TABLE (bypass recycle bin)."""
        return True

    def format_drop_table_statement(
        self, expr: "DropTableExpression"
    ) -> Tuple[str, tuple]:
        """Format DROP TABLE for Oracle.

        Oracle has no IF EXISTS clause and no bare CASCADE/RESTRICT keyword;
        instead, the dialect-specific ``CASCADE CONSTRAINTS`` form (optionally
        followed by ``PURGE``) is emitted when ``expr.cascade is True``. The
        dialect_options dict may carry ``purge=True`` to append PURGE.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        parts = ["DROP TABLE"]
        table_sql, table_params = expr.table.to_sql()
        parts.append(table_sql)
        if expr.cascade is True:
            if not self.supports_cascade_constraints():
                raise UnsupportedFeatureError(
                    self.name, "DROP TABLE ... CASCADE CONSTRAINTS"
                )
            parts.append("CASCADE CONSTRAINTS")
            if expr.dialect_options.get("purge") and self.supports_purge_on_drop_table():
                parts.append("PURGE")
        elif expr.cascade is False:
            raise UnsupportedFeatureError(self.name, "DROP TABLE ... RESTRICT")
        return " ".join(parts), table_params

