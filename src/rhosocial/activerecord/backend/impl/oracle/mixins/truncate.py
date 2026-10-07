# src/rhosocial/activerecord/backend/impl/oracle/mixins/truncate.py
"""Oracle TRUNCATE statement formatting mixin."""

from typing import Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class OracleTruncateMixin:
    """Oracle-specific TRUNCATE TABLE formatting.

    Oracle's ``TRUNCATE TABLE`` accepts none of the SQL-standard modifiers
    this backend models: there is no ``RESTART IDENTITY`` / ``CONTINUE
    IDENTITY`` spelling (the identity is always reset implicitly), and the
    declared probes for CASCADE / RESTRICT are ``False``.  Every requested
    modifier is therefore gated on its probe and refused by name instead of
    being dropped.
    """

    def format_truncate_statement(self, expr) -> Tuple[str, tuple]:
        """Format ``TRUNCATE TABLE`` for Oracle.

        Raises:
            TypeError: ``expr.table`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Table`.
                Another catalogue object renders its own name, so the statement
                would empty an index and name a table.
            UnsupportedFeatureError: A modifier was requested whose probe is
                ``False`` on this dialect.
        """
        from rhosocial.activerecord.backend.expression.objects import Table

        if not isinstance(expr.table, Table):
            raise TypeError(
                f"TruncateExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        if expr.restart_identity or expr.continue_identity:
            if not self.supports_truncate_restart_identity():
                feature = (
                    "TRUNCATE RESTART IDENTITY"
                    if expr.restart_identity
                    else "TRUNCATE CONTINUE IDENTITY"
                )
                raise UnsupportedFeatureError(
                    self.name,
                    feature,
                    f"{self.name} does not support TRUNCATE with identity "
                    "continuation.",
                )
        if expr.cascade and not self.supports_truncate_cascade():
            raise UnsupportedFeatureError(
                self.name,
                "TRUNCATE CASCADE",
                f"{self.name} does not support TRUNCATE with CASCADE.",
            )
        if expr.restrict and not self.supports_truncate_restrict():
            raise UnsupportedFeatureError(
                self.name,
                "TRUNCATE RESTRICT",
                f"{self.name} does not support TRUNCATE with RESTRICT.",
            )

        parts = ["TRUNCATE TABLE"]
        parts.append(expr.table.to_sql()[0])
        if expr.restart_identity:
            parts.append("RESTART IDENTITY")
        elif expr.continue_identity:
            parts.append("CONTINUE IDENTITY")
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return (" ".join(parts), ())
