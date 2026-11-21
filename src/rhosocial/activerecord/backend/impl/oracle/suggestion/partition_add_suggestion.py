# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_add_suggestion.py
"""Oracle's partition add suggestion.

Suggests one ``ALTER TABLE ... ADD PARTITION`` statement per definition, subject
to the dialect's capability probe. REFERENCE partitions inherit from the parent
and are never added, so that strategy is declined.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionAddSuggestion

from ..expression.partition_lifecycle import OracleAddPartitionStatement
from ._common import _REFERENCE, OraclePartitionSuggestionBase, _word

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["OraclePartitionAddSuggestionMixin"]


class OraclePartitionAddSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest ``ALTER TABLE ... ADD PARTITION`` for Oracle."""

    def suggest_partition_add(
        self,
        strategy: "PartitionByClause",
        plan: object,
    ) -> PartitionAddSuggestion:
        """Suggest ``ALTER TABLE ... ADD PARTITION`` for each definition."""
        table = self._suggestion_table(plan)
        if table is None or not self.supports_add_partition():
            return PartitionAddSuggestion(False)
        if _word(strategy) == _REFERENCE:
            # Reference partitions inherit from the parent; they are not added.
            return PartitionAddSuggestion(False)
        definitions = self._suggestion_definitions(plan)
        if not definitions:
            return PartitionAddSuggestion(False)
        return PartitionAddSuggestion(
            tuple(
                OracleAddPartitionStatement(self, table, definition) for definition in definitions
            )
        )
