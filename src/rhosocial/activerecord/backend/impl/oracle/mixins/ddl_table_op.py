# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl_table_op.py
"""Oracle table DDL support mixin (capability flags for table operations).

The existing ``table.py`` contains ``OracleTableMixin`` which handles
table-level operations. This module provides the remaining ``supports_*``
capability checks that were previously inlined in the monolithic
``dialect.py``.
"""

from typing import Tuple, TYPE_CHECKING

from .if_exists import supports_statement_if_exists

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import DropTableStatement


class OracleTableCapabilityMixin:
    """Oracle table operation capability checks."""

    def supports_if_not_exists_table(self) -> bool:
        """Whether CREATE TABLE accepts the native ``IF NOT EXISTS`` clause.

        Oracle added it with 19c Release Update 19.28 and ships it in the 23ai
        base release; the 21c line has no such spelling. See
        :func:`~.if_exists.supports_statement_if_exists` for the two anchors.
        Below that capability the CREATE TABLE formatter keeps its documented
        PL/SQL existence-check fallback, so the request is not silently lost.
        """
        return supports_statement_if_exists(self)

    def supports_if_exists_table(self) -> bool:
        """Whether DROP TABLE accepts the native ``IF EXISTS`` clause.

        Same two anchors as :meth:`supports_if_not_exists_table` (19c RU 19.28
        and 23ai+); 21c and older lines are refused by name by the formatter.
        """
        return supports_statement_if_exists(self)

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
        self, expr: "DropTableStatement"
    ) -> Tuple[str, tuple]:
        """Format DROP TABLE for Oracle.

        Oracle accepts the statement-level ``IF EXISTS`` clause from 19c
        Release Update 19.28 and in the 23ai base release; below that
        capability the request is refused by name rather than dropped (21c,
        for instance, rejects it). Oracle has no bare CASCADE/RESTRICT
        keyword; instead, the dialect-specific ``CASCADE CONSTRAINTS`` form
        (optionally followed by ``PURGE``) is emitted when ``expr.cascade``
        is True. The typed ``expr.purge`` flag appends PURGE.

        Args:
            expr: the drop statement, carrying the table, ``if_exists``,
                ``cascade`` / ``restrict`` and ``purge`` flags.

        Returns:
            Tuple of (SQL string, table parameters tuple).

        Raises:
            TypeError: ``expr.table`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Table`.
                Another catalogue object renders its own name, so the statement
                would drop an index and name a table.
            UnsupportedFeatureError: ``expr.if_exists`` was requested below the
                documented capability, ``expr.restrict`` was requested and the
                probe is ``False``, or CASCADE CONSTRAINTS is unavailable.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.objects import Table

        if not isinstance(expr.table, Table):
            raise TypeError(
                f"DropTableStatement.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        parts = ["DROP TABLE"]
        if expr.if_exists:
            if not self.supports_if_exists_table():
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP TABLE IF EXISTS",
                    "Oracle has no DROP TABLE IF EXISTS before 19c Release "
                    "Update 19.28 / 23ai (21c has no such spelling).",
                )
            parts.append("IF EXISTS")
        table_sql, table_params = expr.table.to_sql()
        parts.append(table_sql)
        if expr.cascade:
            if not self.supports_cascade_constraints():
                raise UnsupportedFeatureError(
                    self.name, "DROP TABLE ... CASCADE CONSTRAINTS"
                )
            parts.append("CASCADE CONSTRAINTS")
        if expr.restrict:
            if not self.supports_drop_table_restrict():
                raise UnsupportedFeatureError(self.name, "DROP TABLE ... RESTRICT")
            parts.append("RESTRICT")
        if expr.purge and self.supports_purge_on_drop_table():
            parts.append("PURGE")
        return " ".join(parts), table_params

