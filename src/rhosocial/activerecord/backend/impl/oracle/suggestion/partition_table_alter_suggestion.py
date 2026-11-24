# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_table_alter_suggestion.py
"""Oracle's partition table-alter suggestion.

Oracle's ``COALESCE PARTITION`` clause carries no target count: each
statement coalesces exactly one hash partition. A plan that asks for any
other count cannot be expressed by this DDL, so it is declined explicitly
rather than silently reduced to a single coalesce.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionTableAlterSuggestion

from ..expression.partition_lifecycle import OracleCoalescePartitionExpression
from ._common import OraclePartitionSuggestionBase, _get, _word

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
        """Suggest coalescing a hash partition, or report no mechanism.

        ``HASH`` is the only strategy Oracle can shrink in place: the
        suggestion renders the count-less ``ALTER TABLE ... COALESCE
        PARTITION`` and is gated on :meth:`supports_coalesce_partition`.
        Oracle's statement coalesces one partition at a time and takes no
        target count, so a plan asking for a count other than 1 is declined
        (``False``) instead of being silently collapsed to one coalesce.
        RANGE, LIST, INTERVAL, REFERENCE and composite tables cannot change
        their scheme in place, so changing them is a rebuild (new table,
        copy, swap) and the honest answer is the explicit "no mechanism"
        state.
        """
        if _word(strategy) != "HASH" or not self.supports_coalesce_partition():
            return PartitionTableAlterSuggestion(False)
        table = self._suggestion_table(plan)
        if table is None:
            return PartitionTableAlterSuggestion(False)
        count = _get(plan, "count")
        if count is None:
            count = _get(plan, "partitions_count")
        if count is not None and (
            isinstance(count, bool) or not isinstance(count, int) or count != 1
        ):
            return PartitionTableAlterSuggestion(False)
        return PartitionTableAlterSuggestion((OracleCoalescePartitionExpression(self, table),))
