# src/rhosocial/activerecord/backend/impl/oracle/mixins/truncate.py
"""Oracle TRUNCATE statement formatting mixin."""

from typing import Tuple


class OracleTruncateMixin:
    """Oracle-specific TRUNCATE TABLE formatting."""

    def format_truncate_statement(self, expr) -> Tuple[str, tuple]:
        """Format ``TRUNCATE TABLE`` for Oracle.

        Raises:
            TypeError: ``expr.table`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Table`.
                Another catalogue object renders its own name, so the statement
                would empty an index and name a table.
        """
        from rhosocial.activerecord.backend.expression.objects import Table

        if not isinstance(expr.table, Table):
            raise TypeError(
                f"TruncateExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        parts = ["TRUNCATE TABLE"]
        parts.append(expr.table.to_sql()[0])
        return (" ".join(parts), ())
