# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_retention_suggestion.py
"""Oracle's partition retention suggestion.

Suggests ``DROP PARTITION`` for RANGE/INTERVAL partitions whose whole upper
bound (the parsed ``HIGH_VALUE`` analog) is at or before a cutoff. Oracle's
``MAXVALUE`` has no finite end and is never a candidate; bounded LIST/HASH
partitions have no time semantics and are declined.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, List

from rhosocial.activerecord.backend.suggestion import PartitionRetentionSuggestion

from ..expression.partition_lifecycle import OracleDropPartitionStatement
from ._common import (
    _INTERVAL,
    _MAXVALUE,
    _RANGE,
    OraclePartitionSuggestionBase,
    _bound_value,
    _comparable,
    _compare,
    _get,
    _word,
)

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["OraclePartitionRetentionSuggestionMixin"]


class OraclePartitionRetentionSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest dropping RANGE/INTERVAL partitions older than a cutoff."""

    def suggest_partition_retention(
        self,
        strategy: "PartitionByClause",
        plan: object,
    ) -> PartitionRetentionSuggestion:
        """Suggest DROP PARTITION for RANGE/INTERVAL partitions older than a cutoff.

        A branch qualifies when its whole upper bound (the parsed ``HIGH_VALUE``
        analog) is at or before the cutoff. Oracle's ``MAXVALUE`` has no finite
        end and is never a candidate. Sorted, bounded LIST/HASH partitions have
        no time semantics and are explicitly unsupported.
        """
        word = _word(strategy)
        if word not in (_RANGE, _INTERVAL) or not self.supports_drop_partition():
            return PartitionRetentionSuggestion(False)
        cutoff = _get(plan, "cutoff")
        if not isinstance(cutoff, (date, datetime)):
            return PartitionRetentionSuggestion(False)
        table = self._suggestion_table(plan)
        if table is None:
            return PartitionRetentionSuggestion(False)

        definitions = self._suggestion_definitions(plan)
        if definitions:
            entries = [(definition.name, _bound_value(definition)) for definition in definitions]
        else:
            specs = self._suggestion_specs(plan, through=cutoff)
            if specs is None:
                return PartitionRetentionSuggestion(False)
            entries = [(spec.name, _bound_value(spec)) for spec in specs]

        candidates: List[str] = []
        try:
            threshold = _comparable(cutoff)
            for name, bound in entries:
                if bound is _MAXVALUE or bound is None:
                    continue
                if _compare(_comparable(bound), threshold) <= 0:
                    candidates.append(str(name))
        except (TypeError, ValueError):
            return PartitionRetentionSuggestion(False)

        if not candidates:
            return PartitionRetentionSuggestion(None)
        return PartitionRetentionSuggestion(
            tuple(OracleDropPartitionStatement(self, table, name) for name in candidates)
        )
