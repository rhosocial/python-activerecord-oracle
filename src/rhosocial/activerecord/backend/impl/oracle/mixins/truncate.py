# src/rhosocial/activerecord/backend/impl/oracle/mixins/truncate.py
"""Oracle TRUNCATE statement formatting mixin."""

from typing import Tuple


class OracleTruncateMixin:
    """Oracle-specific TRUNCATE TABLE formatting."""

    def format_truncate_statement(self, expr) -> Tuple[str, tuple]:
        parts = ["TRUNCATE TABLE"]
        parts.append(self.format_identifier(expr.table))
        return (" ".join(parts), ())