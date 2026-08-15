# src/rhosocial/activerecord/backend/impl/oracle/mixins/collation.py
"""Oracle collation support mixin."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.collation import CollateExpression

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.collation import (
    validate_oracle_collation_name,
)


class OracleCollationMixin:
    """Oracle-specific collation validation."""

    def validate_collation_name(self, expr: "CollateExpression") -> str:
        """Validate Oracle collation names and return their SQL representation."""
        if expr.collation_options:
            unsupported = ", ".join(sorted(expr.collation_options))
            raise UnsupportedFeatureError(
                self.name, f"COLLATE options: {unsupported}"
            )
        return validate_oracle_collation_name(
            expr.collation_name, getattr(self, "version", None)
        )