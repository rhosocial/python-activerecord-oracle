# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_merge_suggestion.py
"""Oracle's partition merge suggestion.

Suggests ``MERGE PARTITIONS p1, p2 INTO p3`` when the plan names exactly two
partitions and one replacement definition, subject to the dialect's capability
probe. REFERENCE partitions are declined.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionMergeSuggestion

from ..expression.partition_lifecycle import OracleMergePartitionsStatement
from ._common import _REFERENCE, OraclePartitionSuggestionBase, _get, _word

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionStrategy

__all__ = ["OraclePartitionMergeSuggestionMixin"]


class OraclePartitionMergeSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest ``MERGE PARTITIONS p1, p2 INTO p3`` on Oracle."""

    def suggest_partition_merge(
        self,
        strategy: "PartitionStrategy",
        plan: object,
    ) -> PartitionMergeSuggestion:
        """Suggest ``MERGE PARTITIONS p1, p2 INTO p3``."""
        table = self._suggestion_table(plan)
        if table is None or not self.supports_merge_partition():
            return PartitionMergeSuggestion(False)
        if _word(strategy) == _REFERENCE:
            return PartitionMergeSuggestion(False)
        names = _get(plan, "names")
        if names is None:
            names = _get(plan, "partitions")
        into = self._suggestion_definitions(plan, "into")
        if not names or len(names) != 2 or not into or len(into) != 1:
            return PartitionMergeSuggestion(False)
        return PartitionMergeSuggestion(
            (OracleMergePartitionsStatement(self, table, [str(name) for name in names], into[0]),)
        )
