# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_table_alter_suggestion.py
"""Oracle's partition table-alter suggestion.

Oracle cannot change a table's partitioning scheme in place, but a
hash-partitioned table can be shrunk by coalescing partitions. That is the
one in-place table-alter mechanism this backend can suggest; every other
strategy answers the explicit "no mechanism" state (``False``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionTableAlterSuggestion

from ..expression.partition_lifecycle import OracleCoalescePartitionExpression
from ._common import OraclePartitionSuggestionBase, _count, _word

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["OraclePartitionTableAlterSuggestionMixin"]


class OraclePartitionTableAlterSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest COALESCE for HASH; every other strategy has no mechanism."""

    def suggest_partition_table_alter(
        self,
        strategy: "PartitionByClause",
        plan: object,
    ) -> PartitionTableAlterSuggestion:
        """Suggest coalescing hash partitions, or report no mechanism.

        ``HASH`` is the only strategy Oracle can shrink in place: the
        suggestion renders ``ALTER TABLE ... COALESCE PARTITION n`` and is
        gated on :meth:`supports_coalesce_partition`. RANGE, LIST, INTERVAL,
        REFERENCE and composite tables cannot change their scheme in place,
        so changing them is a rebuild (new table, copy, swap) and the honest
        answer is the explicit "no mechanism" state.
        """
        if _word(strategy) != "HASH" or not self.supports_coalesce_partition():
            return PartitionTableAlterSuggestion(False)
        table = self._suggestion_table(plan)
        if table is None:
            return PartitionTableAlterSuggestion(False)
        return PartitionTableAlterSuggestion(
            (OracleCoalescePartitionExpression(self, table, _count(plan)),)
        )
