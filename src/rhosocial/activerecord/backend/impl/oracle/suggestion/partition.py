# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition.py
"""OraclePartitionSuggestionMixin -- real Oracle answers for the 8 partition suggestions.

The eight ``suggest_partition_*`` methods turn a caller-supplied ``plan`` into
ordered chains of this backend's own expression nodes. Nothing here renders SQL:
every operation is a real statement/clause from ``...expression.partition`` or
``...expression.partition_lifecycle`` (or ``DropTableStatement`` for the whole
table case), and the dialect formatter owns the spelling.

This mixin is deliberately self-contained: it imports no partition maintenance
or report module. The boundary-comparison helpers below are inlined so this is
the single source for how Oracle performs each partition operation.

``plan`` is opaque to the core protocol, so this mixin reads a small, documented
set of fields from either a mapping or an object with attributes:

* ``table`` / ``parent`` -- target table name (``str``) or a ready ``Table``;
* ``schema`` -- optional owner shared by the parent and its partitions;
* ``keys`` / ``columns`` -- build keys: expression nodes or column-name strings;
* ``definitions`` -- ``OraclePartitionDefinition`` nodes (build/add/retention);
* ``partitions_count`` -- partition count for HASH build;
* ``subpartition_by`` -- optional ``OracleSubpartitionClause`` for build;
* ``interval`` -- a ``BaseExpression`` for INTERVAL build;
* ``fk_constraint`` -- constraint name for REFERENCE build;
* ``partition`` -- partition name to SPLIT (or a single DROP);
* ``at_values`` -- boundary value(s) at which to SPLIT;
* ``into`` -- replacement ``OraclePartitionDefinition`` nodes (SPLIT / MERGE);
* ``names`` -- partition name(s) to DROP / MERGE;
* ``cutoff`` -- exclusive retention boundary (date or datetime, RANGE/INTERVAL).

Tri-state result: a tuple of expressions when a real chain exists, ``False``
when Oracle has no such mechanism for the strategy/plan, ``None`` when nothing
needs doing. Capability probes on :class:`OraclePartitionMixin` gate every
mutating chain, so a dialect that withdraws a feature answers ``False`` rather
than emitting SQL the server would reject.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import (
    PartitionAddSuggestion,
    PartitionBuildSuggestion,
    PartitionDropSuggestion,
    PartitionMergeSuggestion,
    PartitionRetentionSuggestion,
    PartitionSplitSuggestion,
    PartitionTableAlterSuggestion,
    PartitionTableDropSuggestion,
)

from ....expression import Column, DropTableStatement
from ....expression.bases import BaseExpression
from ....expression.objects import Table
from ..expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OracleReferencePartitionClause,
    OracleSubpartitionClause,
)
from ..expression.partition_lifecycle import (
    OracleAddPartitionStatement,
    OracleDropPartitionStatement,
    OracleMergePartitionsStatement,
    OracleSplitPartitionStatement,
)

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionStrategy

__all__ = ["OraclePartitionSuggestionMixin"]

# Sentinel for Oracle's MAXVALUE: it is a terminator, never a retention
# candidate. Kept local so no maintenance helper is needed.
_MAXVALUE = object()

_RANGE = "RANGE"
_INTERVAL = "INTERVAL"
_REFERENCE = "REFERENCE"
_ALL_STRATEGIES = frozenset({"RANGE", "LIST", "HASH", "INTERVAL", "REFERENCE"})


def _get(plan: Any, name: str, default: Any = None) -> Any:
    """Read one field from a mapping-shaped or attribute-shaped plan."""
    if plan is None:
        return default
    if isinstance(plan, Mapping):
        return plan.get(name, default)
    return getattr(plan, name, default)


def _word(strategy: Any) -> str:
    """Normalise a strategy member or its spelling to an upper-case word."""
    value = getattr(strategy, "value", strategy)
    return str(value).strip().upper()


def _comparable(value: Any) -> Any:
    """Normalise a bound value so plan values and a cutoff compare cleanly."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
        try:
            return datetime.combine(date.fromisoformat(text), time.min)
        except ValueError:
            return text
    return value


def _compare(left: Any, right: Any) -> int:
    try:
        if left < right:
            return -1
        if left > right:
            return 1
        return 0
    except TypeError as error:
        raise ValueError(
            f"partition bound value {left!r} is not comparable to {right!r}"
        ) from error


def _bound_value(candidate: Any) -> Any:
    """Return the HIGH_VALUE-style bound of a definition/spec, or the sentinel."""
    if candidate is None:
        return None
    if isinstance(candidate, OraclePartitionMaxValue):
        return _MAXVALUE
    if isinstance(candidate, str) and candidate.strip().upper() == "MAXVALUE":
        return _MAXVALUE
    if isinstance(candidate, OraclePartitionDefinition):
        less_than = candidate.less_than or ()
        return _bound_value(less_than[0]) if less_than else None
    if getattr(candidate, "is_maxvalue", False):
        return _MAXVALUE
    less_than = getattr(candidate, "less_than", None)
    if less_than:
        return _bound_value(less_than[0])
    range_end = getattr(candidate, "range_end", None)
    if range_end is not None:
        return _bound_value(range_end)
    inner = getattr(candidate, "value", None)
    if inner is not None and inner is not candidate:
        return _bound_value(inner)
    return candidate


class OraclePartitionSuggestionMixin:
    """Suggest Oracle's real DDL action chains for partition lifecycle points."""

    # -- shared helpers --------------------------------------------------

    def _suggestion_table(self, plan: Any) -> Optional[Table]:
        target = _get(plan, "table")
        if isinstance(target, Table):
            return target
        if target is None:
            target = _get(plan, "parent")
        if target is None:
            nested = _get(plan, "plan")
            if nested is not None:
                target = getattr(nested, "parent", None)
        if isinstance(target, str) and target.strip():
            return Table(self, target, schema_name=_get(plan, "schema"))
        return None

    def _suggestion_keys(self, plan: Any) -> Optional[List[BaseExpression]]:
        raw = _get(plan, "keys")
        if raw is None:
            columns = _get(plan, "columns")
            if columns is None:
                return None
            raw = (columns,) if isinstance(columns, str) else columns
        try:
            items = list(raw)
        except TypeError:
            return None
        keys: List[BaseExpression] = []
        for key in items:
            if isinstance(key, BaseExpression):
                keys.append(key)
            elif isinstance(key, str) and key.strip():
                keys.append(Column(self, key))
            else:
                return None
        return keys or None

    def _suggestion_definitions(
        self, plan: Any, name: str = "definitions"
    ) -> Optional[List[OraclePartitionDefinition]]:
        raw = _get(plan, name)
        if raw is None:
            return None
        try:
            values = list(raw)
        except TypeError:
            return None
        if not values or not all(isinstance(value, OraclePartitionDefinition) for value in values):
            return None
        return values

    def _suggestion_specs(self, plan: Any, through: Any = None) -> Optional[tuple]:
        candidate = _get(plan, "plan") or plan
        iterator = getattr(candidate, "iter_specs", None)
        if iterator is None:
            return None
        try:
            return tuple(iterator(through=through))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _suggestion_subpartition_by(plan: Any) -> Optional[OracleSubpartitionClause]:
        value = _get(plan, "subpartition_by")
        if value is None or isinstance(value, OracleSubpartitionClause):
            return value
        return None

    # -- 1. build --------------------------------------------------------

    def suggest_partition_build(
        self,
        strategy: "PartitionStrategy",
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

    # -- 2. table_drop ---------------------------------------------------

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

    # -- 3. table_alter --------------------------------------------------

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

    # -- 4. add ----------------------------------------------------------

    def suggest_partition_add(
        self,
        strategy: "PartitionStrategy",
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

    # -- 5. drop ---------------------------------------------------------

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

    # -- 6. split --------------------------------------------------------

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

    # -- 7. merge --------------------------------------------------------

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

    # -- 8. retention ----------------------------------------------------

    def suggest_partition_retention(
        self,
        strategy: "PartitionStrategy",
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
