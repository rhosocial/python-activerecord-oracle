# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_drop_suggestion.py
"""Oracle's partition drop suggestion.

Suggests ``DROP PARTITION`` for one or more named partitions, subject to the
dialect's capability probe. REFERENCE partitions are declined; an empty name
set answers ``None`` (nothing needs doing).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionDropSuggestion

from ..expression.partition_lifecycle import OracleDropPartitionStatement
from ._common import _REFERENCE, OraclePartitionSuggestionBase, _get, _word

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionStrategy

__all__ = ["OraclePartitionDropSuggestionMixin"]


class OraclePartitionDropSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest ``DROP PARTITION`` for named Oracle partitions."""

    def suggest_partition_drop(
        self,
        strategy: "PartitionStrategy",
        plan: object,
    ) -> PartitionDropSuggestion:
        """Suggest ``DROP PARTITION <name>`` for one or more named partitions."""
        table = self._suggestion_table(plan)
        if table is None or not self.supports_drop_partition():
            return PartitionDropSuggestion(False)
        if _word(strategy) == _REFERENCE:
            return PartitionDropSuggestion(False)
        raw = _get(plan, "names")
        if raw is None:
            single = _get(plan, "partition")
            raw = [single] if single else None
        if not raw:
            return PartitionDropSuggestion(None)
        try:
            names = [str(name) for name in raw if str(name).strip()]
        except TypeError:
            return PartitionDropSuggestion(False)
        if not names:
            return PartitionDropSuggestion(None)
        return PartitionDropSuggestion(
            tuple(OracleDropPartitionStatement(self, table, name) for name in names)
        )
