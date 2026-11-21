# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_split_suggestion.py
"""Oracle's partition split suggestion.

Suggests Oracle's real ``SPLIT PARTITION`` chain when the plan names a partition,
the ``AT`` boundary values and the two replacement definitions, subject to the
dialect's capability probe. REFERENCE partitions are declined.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionSplitSuggestion

from ..expression.partition_lifecycle import OracleSplitPartitionStatement
from ._common import _REFERENCE, OraclePartitionSuggestionBase, _get, _word

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionStrategy

__all__ = ["OraclePartitionSplitSuggestionMixin"]


class OraclePartitionSplitSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest ``SPLIT PARTITION`` -- Oracle's split mechanism."""

    def suggest_partition_split(
        self,
        strategy: "PartitionStrategy",
        plan: object,
    ) -> PartitionSplitSuggestion:
        """Suggest ``SPLIT PARTITION`` -- Oracle's split mechanism."""
        table = self._suggestion_table(plan)
        if table is None or not self.supports_split_partition():
            return PartitionSplitSuggestion(False)
        if _word(strategy) == _REFERENCE:
            return PartitionSplitSuggestion(False)
        partition = _get(plan, "partition")
        at_values = _get(plan, "at_values")
        into = self._suggestion_definitions(plan, "into")
        if not partition or not at_values or not into or len(into) != 2:
            return PartitionSplitSuggestion(False)
        if not isinstance(at_values, (list, tuple)):
            at_values = [at_values]
        return PartitionSplitSuggestion(
            (OracleSplitPartitionStatement(self, table, str(partition), at_values, into),)
        )
