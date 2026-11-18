# src/rhosocial/activerecord/backend/impl/oracle/mixins/integer_auto_increment_column_suggestion.py
"""OracleIntegerAutoIncrementColumnSuggestionMixin -- oracle's integer auto-increment column suggestion.

The answer is the backend's own spelling, assembled from its capability
probes where it has one. It is placed first in the dialect bases so no core
mixin can shadow it, and it renders nothing itself -- the returned clause's
formatter owns the SQL spelling.
"""

from typing import TYPE_CHECKING

from ....expression.statements import (
    IdentityClause,
    IntegerAutoIncrementColumnSuggestion,
)

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.types._base import DataType


class OracleIntegerAutoIncrementColumnSuggestionMixin:
    """Suggest oracle's declaration for a generated integer key."""

    def suggest_integer_auto_increment_column(
        self, default_type: "DataType"
    ) -> IntegerAutoIncrementColumnSuggestion:
        return IntegerAutoIncrementColumnSuggestion(
            default_type, IdentityClause(self, "BY DEFAULT")
        )
