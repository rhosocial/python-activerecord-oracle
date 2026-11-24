# src/rhosocial/activerecord/backend/impl/oracle/suggestion/generated_value_suggestion.py
"""OracleGeneratedValueSuggestionMixin -- oracle's database-generated-value suggestion.

The answer is the backend's own spelling, assembled from its capability
probes where it has one. It is placed first in the dialect bases so no core
mixin can shadow it, and it renders nothing itself -- the returned clause's
formatter owns the SQL spelling.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import GeneratedValueSuggestion

from ....expression.statements import IdentityClause

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.types._base import DataType


class OracleGeneratedValueSuggestionMixin:
    """Suggest oracle's declaration for a generated integer key."""

    def suggest_generated_value(self, default_type: "DataType") -> GeneratedValueSuggestion:
        """Declare the generated key as an identity column (12c and later)."""
        return GeneratedValueSuggestion(
            state="generate",
            storage_type=default_type,
            clause=IdentityClause(self, "BY DEFAULT"),
        )
