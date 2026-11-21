# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_table_drop_suggestion.py
"""Oracle's partition table-drop suggestion.

Suggests dropping the whole partitioned table (not a single partition) with a
real ``DropTableStatement`` when the plan names a table; otherwise the explicit
"unsupported" state (``False``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionTableDropSuggestion

from ....expression import DropTableStatement
from ._common import OraclePartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionStrategy

__all__ = ["OraclePartitionTableDropSuggestionMixin"]


class OraclePartitionTableDropSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest dropping the whole partitioned table on Oracle."""

    def suggest_partition_table_drop(
        self,
        strategy: "PartitionStrategy",
        plan: object,
    ) -> PartitionTableDropSuggestion:
        """Suggest dropping the whole partitioned table (not a single partition)."""
        table = self._suggestion_table(plan)
        if table is None:
            return PartitionTableDropSuggestion(False)
        return PartitionTableDropSuggestion((DropTableStatement(self, table),))
