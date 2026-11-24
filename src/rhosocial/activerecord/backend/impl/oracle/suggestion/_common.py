# src/rhosocial/activerecord/backend/impl/oracle/suggestion/_common.py
"""Shared helpers and the common base for Oracle's partition suggestion mixins.

The eight ``suggest_partition_*`` mixins are split one-per-file, but they all
read the same opaque ``plan`` shape and build their expression chains from the
same nodes. The boundary-comparison helpers and the ``_suggestion_*`` plan
readers live here so there is a single source for how Oracle interprets a
suggestion plan; each mixin keeps only its own ``suggest_partition_<x>()``
method and the imports that method needs.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, List, Optional

from ....expression import Column
from ....expression.bases import BaseExpression
from ....expression.objects import Table
from ....expression.statements import PartitionDefinitionExpression
from ..expression.partition import OraclePartitionMaxValue, OracleSubpartitionClause

__all__ = ["OraclePartitionSuggestionBase"]

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
    """Normalise a strategy clause (or a legacy word/member) to an upper-case word.

    A ``PartitionByClause`` carries the word in ``.word`` (core's clause nodes
    expose it through ``.method``); a legacy string enum carried it in
    ``.value``. Reading all three keeps the suggestion runtime compatible
    with either data carrier while the annotations speak in clauses.
    """
    value = getattr(
        strategy,
        "word",
        getattr(strategy, "method", getattr(strategy, "value", strategy)),
    )
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
    if isinstance(candidate, PartitionDefinitionExpression):
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


class OraclePartitionSuggestionBase:
    """Plan readers shared by all eight Oracle partition suggestion mixins."""

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
    ) -> Optional[List[PartitionDefinitionExpression]]:
        raw = _get(plan, name)
        if raw is None:
            return None
        try:
            values = list(raw)
        except TypeError:
            return None
        if not values or not all(
            isinstance(value, PartitionDefinitionExpression) for value in values
        ):
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


def _count(plan: Any, default: int = 1) -> int:
    """Return a positive partition count for a COALESCE request.

    ``plan`` may be a mapping or an attribute-shaped object; a plan that
    carries no usable count still coalesces, so the answer is the default
    rather than a refusal -- the count only says how many partitions go.
    """
    count = _get(plan, "count")
    if count is None:
        count = _get(plan, "partitions_count")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        return default
    return count
