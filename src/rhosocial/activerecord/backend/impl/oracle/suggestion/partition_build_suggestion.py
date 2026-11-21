# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_build_suggestion.py
"""Oracle's partition build suggestion.

Suggests the inline ``PARTITION BY`` clause for CREATE TABLE across Oracle's
five strategies (RANGE / LIST / HASH / INTERVAL / REFERENCE). A real clause node
is returned when the plan supplies a usable key/definition set and the dialect's
capability probes allow it; otherwise the explicit "unsupported" state
(``False``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionBuildSuggestion

from ....expression.bases import BaseExpression
from ..expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OracleReferencePartitionClause,
)
from ._common import (
    _ALL_STRATEGIES,
    _INTERVAL,
    _RANGE,
    _REFERENCE,
    OraclePartitionSuggestionBase,
    _get,
    _word,
)

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["OraclePartitionBuildSuggestionMixin"]


class OraclePartitionBuildSuggestionMixin(OraclePartitionSuggestionBase):
    """Suggest the inline ``PARTITION BY`` clause for Oracle CREATE TABLE."""

    def suggest_partition_build(
        self,
        strategy: "PartitionByClause",
        plan: object,
    ) -> PartitionBuildSuggestion:
        """Suggest the inline ``PARTITION BY`` clause for CREATE TABLE."""
        word = _word(strategy)
        if word not in _ALL_STRATEGIES:
            return PartitionBuildSuggestion(False)

        subpartition_by = self._suggestion_subpartition_by(plan)
        if word == _REFERENCE:
            constraint = _get(plan, "fk_constraint")
            if (
                not self.supports_reference_partitioning()
                or not isinstance(constraint, str)
                or not constraint.strip()
            ):
                return PartitionBuildSuggestion(False)
            return PartitionBuildSuggestion((OracleReferencePartitionClause(self, constraint),))

        keys = self._suggestion_keys(plan)
        definitions = self._suggestion_definitions(plan)

        if word == _INTERVAL:
            interval = _get(plan, "interval")
            if (
                not self.supports_interval_partitioning()
                or not keys
                or len(keys) != 1
                or not isinstance(interval, BaseExpression)
                or not definitions
            ):
                return PartitionBuildSuggestion(False)
            return PartitionBuildSuggestion(
                (
                    OracleIntervalPartitionClause(
                        self, keys, interval=interval, partitions=definitions
                    ),
                )
            )

        if not keys or not self.supports_table_partitioning():
            return PartitionBuildSuggestion(False)

        if word == _RANGE:
            if not definitions or not self.supports_range_table_partitioning():
                return PartitionBuildSuggestion(False)
            clause = OraclePartitionByRange(
                self, keys, partitions=definitions, subpartition_by=subpartition_by
            )
        elif word == "LIST":
            if not definitions or not self.supports_list_table_partitioning():
                return PartitionBuildSuggestion(False)
            clause = OraclePartitionByList(
                self, keys, partitions=definitions, subpartition_by=subpartition_by
            )
        else:  # HASH
            count = _get(plan, "partitions_count")
            if count is not None and (
                isinstance(count, bool) or not isinstance(count, int) or count <= 0
            ):
                return PartitionBuildSuggestion(False)
            if not definitions and count is None:
                return PartitionBuildSuggestion(False)
            if not self.supports_hash_table_partitioning():
                return PartitionBuildSuggestion(False)
            clause = OraclePartitionByHash(
                self,
                keys,
                partitions_count=count,
                partitions=definitions,
                subpartition_by=subpartition_by,
            )
        return PartitionBuildSuggestion((clause,))
