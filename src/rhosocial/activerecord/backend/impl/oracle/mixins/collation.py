# src/rhosocial/activerecord/backend/impl/oracle/mixins/collation.py
"""Oracle collation support mixin."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.collation import CollateFunction

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.collation import (
    validate_oracle_collation_name,
)


class OracleCollationMixin:
    """Oracle-specific collation validation."""

    def validate_collation_name(self, expr: "CollateFunction") -> str:
        """Validate Oracle collation names and return their SQL representation.

        A schema-qualified collation (``schema.collation``, core's named
        ``schema`` parameter, which replaced the ``**collation_options`` bag)
        is refused: Oracle's named collations live in its own namespaces and
        the dialect has no formatter for a qualified spelling, so a request
        for one is refused by name rather than rendered wrong.
        """
        if expr.schema:
            raise UnsupportedFeatureError(
                self.name, f"schema-qualified COLLATE ({expr.schema})"
            )
        return validate_oracle_collation_name(
            expr.collation_name, getattr(self, "version", None)
        )