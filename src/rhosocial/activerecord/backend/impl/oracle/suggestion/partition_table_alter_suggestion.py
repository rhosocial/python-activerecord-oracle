# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_table_alter_suggestion.py
"""Oracle's partition table-alter suggestion.

Oracle cannot change a table's partitioning scheme in place: there is no
``REMOVE PARTITIONING`` / ``REORGANIZE`` expression for it in this backend, so
the honest answer is the explicit "no mechanism" state (``False``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionTableAlterSuggestion

from ._common import OraclePartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionStrategy

__all__ = ["OraclePartitionTableAlterSuggestionMixin"]


class OraclePartitionTableAlterSuggestionMixin(OraclePartitionSuggestionBase):
    """Answer Oracle's lack of in-place partitioning changes explicitly."""

    def suggest_partition_table_alter(
        self,
        strategy: "PartitionStrategy",
        plan: object,
    ) -> PartitionTableAlterSuggestion:
        """Oracle cannot change a table's partitioning scheme in place.

        There is no ``REMOVE PARTITIONING`` / ``REORGANIZE`` expression for
        Oracle in this backend; changing the scheme is a rebuild (new table,
        copy, swap), so the honest answer is the explicit "no mechanism" state.
        """
        return PartitionTableAlterSuggestion(False)
