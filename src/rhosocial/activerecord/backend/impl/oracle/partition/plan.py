# src/rhosocial/activerecord/backend/impl/oracle/partition/plan.py
"""Owner-agnostic partition plans for Oracle.

ActiveRecord deliberately does not model partitioning (see the core plan
``2026-11-02/partitioned-model-support.md``): a model declares the primary-key
shape a partitioned table needs, and the backend owns the tree. This module is
the Oracle backend's description of that tree:

    Parent(RANGE, created_at)
      SUBPARTITION BY HASH (id) SUBPARTITIONS 2      <- table-level strategy
      |-- p2026_01 (SUBPARTITIONS 3)                  <- count override
      |-- p2026_02
      `-- p2026_03 (explicit subpartition definitions)

Oracle's subpartition strategy is a **table-level** template. A plan therefore
holds exactly one :class:`OracleSubpartitionPolicy`; a first-level branch may
override its subpartition count or supply explicit subpartition definitions,
but it can never declare a different strategy. That is the structural
difference from PostgreSQL, where each branch may choose its own child
strategy.

A plan carries table names, columns and policies only; it carries no
application names, no retention default and no application vocabulary. Those
belong to the caller.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple, Union

from .bounds import default_label, iter_range_bounds, normalize_interval

__all__ = [
    "OracleHashPolicy",
    "OracleListPolicy",
    "OraclePartitionBranch",
    "OraclePartitionPlan",
    "OraclePartitionPolicy",
    "OraclePartitionSpec",
    "OracleRangePolicy",
    "OracleSubpartitionPolicy",
    "OracleSubpartitionSpec",
]

_METHODS = ("RANGE", "LIST", "HASH")
_MAXVALUE = "MAXVALUE"


def _as_columns(value: Any) -> Tuple[str, ...]:
    """Normalise a column declaration to a tuple of non-empty names."""
    if isinstance(value, str):
        columns = (value,)
    elif isinstance(value, (tuple, list)):
        columns = tuple(value)
    else:
        raise TypeError("columns must be a string or a tuple of strings")
    if not columns:
        raise ValueError("columns must not be empty")
    for column in columns:
        if not isinstance(column, str) or not column.strip():
            raise ValueError("every column must be a non-empty string")
    return columns


def _normalise_method(method: Any) -> str:
    if isinstance(method, str):
        normalized = method.strip().upper()
        if normalized in _METHODS:
            return normalized
    raise ValueError("method must be one of %s, got %r" % (", ".join(_METHODS), method))


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("%s must be an integer" % name)
    if value <= 0:
        raise ValueError("%s must be a positive integer" % name)
    return value


def _label_from_values(values: Sequence[Any]) -> str:
    text = "_".join(str(value) for value in values)
    label = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return label or "value"


def _render_label(pattern: str, start: Any, end: Any) -> str:
    return pattern.format(
        start=start,
        end=end,
        year=start.year,
        month=start.month,
        day=start.day,
        quarter=(start.month - 1) // 3 + 1,
    )


@dataclass(frozen=True)
class OracleRangePolicy:
    """Generate RANGE partitions with half-open ``[start, end)`` bounds.

    Oracle renders the upper bound as ``VALUES LESS THAN (end)``. Either
    *bounds* supplies every boundary explicitly, or *start* plus *interval*
    generates them. A generated policy needs a horizon: *count*, or a
    ``through`` argument on the maintenance call.

    *maxvalue* appends Oracle's ``MAXVALUE`` upper bound after the generated
    ranges. It is optional; a plan that intends to keep appending future
    partitions normally leaves it off, because Oracle cannot ``ADD PARTITION``
    above an existing ``MAXVALUE`` partition (that requires ``SPLIT``).

    Attributes:
        columns: Partition key column. Oracle range partitioning takes exactly
            one top-level column.
        start: First boundary when *bounds* is not given.
        interval: Step between generated boundaries; a named unit, a
            ``"<amount> <unit>"`` spelling, or an ``(amount, unit)`` pair.
        count: Maximum number of generated boundaries.
        bounds: Explicit ``(start, end)`` pairs, mutually exclusive with the
            generated form.
        label_pattern: Optional ``str.format`` pattern for the name suffix;
            receives ``start``, ``end``, ``year``, ``month``, ``day`` and
            ``quarter``.
        maxvalue: Append a final ``VALUES LESS THAN (MAXVALUE)`` partition.
    """

    columns: Union[str, Tuple[str, ...]] = "created_at"
    start: Optional[Any] = None
    interval: Any = "month"
    count: Optional[int] = None
    bounds: Optional[Tuple[Tuple[Any, Any], ...]] = None
    label_pattern: Optional[str] = None
    maxvalue: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if len(self.columns) != 1:
            raise ValueError("Oracle RANGE partitioning takes exactly one column")
        if self.bounds is not None:
            pairs = tuple(tuple(pair) for pair in self.bounds)
            if self.start is not None:
                raise ValueError("start and bounds are mutually exclusive")
            if not pairs:
                raise ValueError("bounds must not be empty")
            for pair in pairs:
                if len(pair) != 2:
                    raise ValueError("every bounds entry must be (start, end)")
                if not isinstance(pair[0], (date, datetime)) or not isinstance(
                    pair[1], (date, datetime)
                ):
                    raise TypeError("bound values must be date or datetime")
                if pair[0] >= pair[1]:
                    raise ValueError("bound start must be before end")
            object.__setattr__(self, "bounds", pairs)
        else:
            if self.start is None:
                raise ValueError("OracleRangePolicy requires start or bounds")
            if not isinstance(self.start, (date, datetime)):
                raise TypeError("RangePolicy start must be date or datetime")
        normalize_interval(self.interval)
        if self.count is not None:
            _positive_int(self.count, "count")
        if not isinstance(self.maxvalue, bool):
            raise TypeError("maxvalue must be a bool")

    @property
    def method(self) -> str:
        """The partitioning strategy this policy generates."""
        return "RANGE"

    def iter_bounds(self, through: Optional[Any] = None) -> Iterator[Tuple[Any, Any]]:
        """Yield the policy's ``(start, end)`` boundaries.

        Explicit bounds are yielded as declared. Generated bounds stop at
        *count* or at *through*, whichever comes first.

        Raises:
            ValueError: when the policy has no horizon, or when *through* is
                not after the first boundary.
        """
        if self.bounds is not None:
            for pair in self.bounds:
                yield pair[0], pair[1]
        else:
            yield from iter_range_bounds(
                self.start,
                self.interval,
                count=self.count,
                through=through,
            )

    def label_for(self, start: Any, end: Any) -> str:
        """Return the partition-name suffix for one generated boundary."""
        if self.label_pattern:
            return _render_label(self.label_pattern, start, end)
        if self.bounds is not None:
            return start.strftime("%Y_%m_%d")
        return default_label(start, self.interval)


@dataclass(frozen=True)
class OracleListPolicy:
    """Generate LIST partitions from explicit value groups.

    Attributes:
        columns: Partition key columns; Oracle renders a multi-column group as
            a row tuple.
        groups: One tuple of values per generated partition.
        labels: Optional name suffix per group, same length as *groups*.
    """

    columns: Union[str, Tuple[str, ...]] = "status"
    groups: Tuple[Tuple[Any, ...], ...] = ()
    labels: Optional[Tuple[str, ...]] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", _as_columns(self.columns))
        groups = tuple(tuple(group) for group in self.groups)
        if not groups:
            raise ValueError("OracleListPolicy requires at least one value group")
        for group in groups:
            if not group:
                raise ValueError("every value group must be non-empty")
        object.__setattr__(self, "groups", groups)
        if self.labels is not None:
            labels = tuple(self.labels)
            if len(labels) != len(groups):
                raise ValueError("labels must have one entry per value group")
            for label in labels:
                if not isinstance(label, str) or not label.strip():
                    raise ValueError("every label must be a non-empty string")
            object.__setattr__(self, "labels", labels)

    @property
    def method(self) -> str:
        """The partitioning strategy this policy generates."""
        return "LIST"

    def iter_groups(self) -> Iterator[Tuple[Tuple[Any, ...], str]]:
        """Yield ``(values, label)`` for every generated partition."""
        for index, values in enumerate(self.groups):
            if self.labels is not None:
                label = self.labels[index]
            else:
                label = _label_from_values(values)
            yield values, label


@dataclass(frozen=True)
class OracleHashPolicy:
    """Generate explicit, named HASH partitions for a fixed modulus.

    Oracle can name hash partitions inline (``PARTITION p0, PARTITION p1``),
    which is what makes them addressable by ``DROP PARTITION`` later.

    Attributes:
        columns: Partition key columns.
        modulus: Number of hash buckets.
        remainders: Buckets to generate; all buckets when omitted.
    """

    columns: Union[str, Tuple[str, ...]] = "id"
    modulus: int = 1
    remainders: Optional[Tuple[int, ...]] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if isinstance(self.modulus, bool) or not isinstance(self.modulus, int):
            raise TypeError("modulus must be an integer")
        if self.modulus <= 0:
            raise ValueError("modulus must be a positive integer")
        if self.remainders is not None:
            remainders = tuple(self.remainders)
            if not remainders:
                raise ValueError("remainders must not be empty")
            for remainder in remainders:
                if isinstance(remainder, bool) or not isinstance(remainder, int):
                    raise TypeError("remainder must be an integer")
                if not 0 <= remainder < self.modulus:
                    raise ValueError("remainder must satisfy 0 <= remainder < modulus")
            if len(set(remainders)) != len(remainders):
                raise ValueError("remainders must be unique")
            object.__setattr__(self, "remainders", remainders)

    @property
    def method(self) -> str:
        """The partitioning strategy this policy generates."""
        return "HASH"

    def iter_remainders(self) -> Iterator[int]:
        """Yield the selected remainders in declaration order."""
        if self.remainders is not None:
            yield from self.remainders
        else:
            for remainder in range(self.modulus):
                yield remainder


#: A policy is any of the three strategy generators for first-level partitions.
OraclePartitionPolicy = Union[OracleRangePolicy, OracleListPolicy, OracleHashPolicy]


@dataclass(frozen=True)
class OracleSubpartitionSpec:
    """One explicit subpartition under a first-level partition.

    The subpartition's *strategy* comes from the plan's single
    :class:`OracleSubpartitionPolicy`; this object only carries its name and
    boundary, so a plan cannot pretend that one branch uses RANGE and another
    uses LIST.

    Attributes:
        name: Subpartition name, as Oracle preserves it for explicit
            per-partition definitions.
        less_than: RANGE upper-bound values.
        in_values: LIST values (flat) or row tuples.
    """

    name: str
    less_than: Optional[Tuple[Any, ...]] = None
    in_values: Optional[Tuple[Any, ...]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("subpartition name must be a non-empty string")
        if self.less_than is not None and self.in_values is not None:
            raise ValueError("less_than and in_values are mutually exclusive")
        if self.less_than is not None:
            values = tuple(self.less_than)
            if not values:
                raise ValueError("less_than must not be empty")
            object.__setattr__(self, "less_than", values)
        if self.in_values is not None:
            values = tuple(self.in_values)
            if not values:
                raise ValueError("in_values must not be empty")
            object.__setattr__(self, "in_values", _normalise_list_groups(values))


@dataclass(frozen=True)
class OracleSubpartitionPolicy:
    """The **table-level** subpartition strategy for a composite table.

    Oracle declares ``SUBPARTITION BY {RANGE|LIST|HASH} (...)`` once, above all
    top-level partitions. A [OraclePartitionBranch][.OraclePartitionBranch] may
    override its subpartition count or provide explicit definitions, but the
    strategy stays here.

    Attributes:
        method: ``RANGE``, ``LIST`` or ``HASH``.
        columns: Subpartition key columns.
        count: Default number of HASH subpartitions, rendered as
            ``SUBPARTITIONS n``. Only valid for ``HASH``.
        templates: Optional ``SUBPARTITION TEMPLATE (...)`` definitions.
    """

    method: str
    columns: Union[str, Tuple[str, ...]] = ()
    count: Optional[int] = None
    templates: Tuple[OracleSubpartitionSpec, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "method", _normalise_method(self.method))
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if self.count is not None:
            _positive_int(self.count, "count")
            if self.method != "HASH":
                raise ValueError("count is only valid for HASH subpartitioning")
        templates = tuple(self.templates)
        if self.count is not None and templates:
            raise ValueError(
                "count and templates are mutually exclusive: Oracle rejects "
                "SUBPARTITIONS n next to SUBPARTITION TEMPLATE"
            )
        for template in templates:
            _validate_subpartition_bound(template, self.method)
        object.__setattr__(self, "templates", templates)


def _normalise_list_groups(values: Tuple[Any, ...]) -> Tuple[Tuple[Any, ...], ...]:
    """Normalise LIST values to one tuple per value group.

    A single-column caller may write ``("EAST", "WEST")``; a multi-column
    caller writes ``((1, "A"), (2, "B"))``. Both become groups, so the executor
    and the renderer never have to guess which column count was meant.
    """
    nested = [isinstance(value, (list, tuple)) for value in values]
    if any(nested) and not all(nested):
        raise ValueError(
            "in_values must be either flat values or value groups, not a mix"
        )
    if all(nested):
        return tuple(tuple(value) for value in values)
    return (tuple(values),)


def _validate_subpartition_bound(
    spec: OracleSubpartitionSpec, method: str
) -> None:
    """Refuse a subpartition bound that does not belong to *method*."""
    if method == "RANGE" and spec.less_than is None:
        raise ValueError(
            "subpartition %r is RANGE but has no less_than bound" % spec.name
        )
    if method == "LIST" and spec.in_values is None:
        raise ValueError(
            "subpartition %r is LIST but has no in_values bound" % spec.name
        )
    if method == "HASH" and (spec.less_than is not None or spec.in_values is not None):
        raise ValueError(
            "subpartition %r is HASH and must not carry a boundary" % spec.name
        )


@dataclass(frozen=True)
class OraclePartitionBranch:
    """A first-level partition declared explicitly.

    The bound form must match the parent's strategy: ``less_than`` for RANGE,
    ``in_values`` for LIST, neither for HASH. The branch may override the
    table-level subpartition *count* (HASH) or supply explicit subpartition
    definitions (RANGE / LIST / HASH), but it has no field for a subpartition
    strategy -- Oracle keeps that at table level.

    Attributes:
        name: Partition name, or a suffix substituted into ``name_pattern``.
        less_than: RANGE upper-bound values (``MAXVALUE`` allowed).
        in_values: LIST values (flat) or row tuples.
        subpartitions_count: Per-partition ``SUBPARTITIONS n`` override.
        subpartitions: Explicit per-partition subpartition definitions.
        label: Name suffix when *name* is not given.
    """

    name: Optional[str] = None
    less_than: Optional[Tuple[Any, ...]] = None
    in_values: Optional[Tuple[Any, ...]] = None
    subpartitions_count: Optional[int] = None
    subpartitions: Tuple[OracleSubpartitionSpec, ...] = ()
    label: Optional[str] = None

    def __post_init__(self) -> None:
        if self.name is not None and (not isinstance(self.name, str) or not self.name.strip()):
            raise ValueError("name must be a non-empty string")
        if self.label is not None and (not isinstance(self.label, str) or not self.label.strip()):
            raise ValueError("label must be a non-empty string")
        if self.less_than is not None and self.in_values is not None:
            raise ValueError("less_than and in_values are mutually exclusive")
        if self.less_than is not None:
            values = tuple(self.less_than)
            if not values:
                raise ValueError("less_than must not be empty")
            object.__setattr__(self, "less_than", values)
        if self.in_values is not None:
            values = tuple(self.in_values)
            if not values:
                raise ValueError("in_values must not be empty")
            object.__setattr__(self, "in_values", _normalise_list_groups(values))
        if self.subpartitions_count is not None:
            _positive_int(self.subpartitions_count, "subpartitions_count")
        subpartitions = tuple(self.subpartitions)
        if subpartitions and self.subpartitions_count is not None:
            raise ValueError(
                "subpartitions_count and subpartitions are mutually exclusive"
            )
        object.__setattr__(self, "subpartitions", subpartitions)


@dataclass(frozen=True)
class OraclePartitionSpec:
    """One concrete top-level partition the plan asks Oracle to own.

    A spec is derived data: the executor turns it into
    ``ALTER TABLE ... ADD PARTITION`` (or a CREATE TABLE definition) and
    compares it with ``ALL_TAB_PARTITIONS``. ``less_than`` and ``in_values``
    carry Python values or the string ``"MAXVALUE"``; rendering is the
    expression layer's job. ``range_end`` is the exclusive upper bound of a
    RANGE partition, which is what ``drop_before`` compares against a cutoff.
    """

    name: str
    parent: str
    method: str
    columns: Tuple[str, ...]
    less_than: Optional[Tuple[Any, ...]] = None
    in_values: Optional[Tuple[Any, ...]] = None
    subpartitions_count: Optional[int] = None
    subpartitions: Tuple[OracleSubpartitionSpec, ...] = ()
    range_start: Optional[Any] = None
    range_end: Optional[Any] = None
    label: Optional[str] = None

    @property
    def is_maxvalue(self) -> bool:
        """Whether this RANGE partition is Oracle's ``MAXVALUE`` terminator."""
        return self.less_than is not None and any(value == _MAXVALUE for value in self.less_than)


@dataclass(frozen=True)
class OraclePartitionPlan:
    """An Oracle partition tree, described by policies rather than names.

    Exactly one of *policy* and *branches* is supplied:

    * *policy* generates every top-level partition directly below the parent.
    * *branches* lists top-level partitions explicitly; each may carry
      subpartition definitions or a count override.

    At most one *subpartition* policy applies, and it applies to the whole
    table. There is deliberately no per-branch subpartition strategy.

    Attributes:
        parent: Partitioned parent table name.
        method: Strategy the parent uses: ``RANGE``, ``LIST`` or ``HASH``.
        columns: Parent partition key columns.
        policy: Auto-generated first-level partitions.
        branches: Explicit first-level partitions.
        subpartition: Optional table-level subpartition strategy.
        schema: Optional owner shared by the parent and every partition.
        name_pattern: ``str.format`` pattern for generated names; must contain
            ``{suffix}`` and normally also ``{parent}``.
    """

    parent: str
    method: str
    columns: Union[str, Tuple[str, ...]] = ()
    policy: Optional[OraclePartitionPolicy] = None
    branches: Tuple[OraclePartitionBranch, ...] = ()
    subpartition: Optional[OracleSubpartitionPolicy] = None
    schema: Optional[str] = None
    name_pattern: str = "{parent}_{suffix}"

    def __post_init__(self) -> None:
        if not isinstance(self.parent, str) or not self.parent.strip():
            raise ValueError("parent must be a non-empty string")
        object.__setattr__(self, "method", _normalise_method(self.method))
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if not self.name_pattern:
            raise ValueError("name_pattern must not be empty")
        if "{suffix}" not in self.name_pattern:
            raise ValueError("name_pattern must contain {suffix}")
        if self.schema is not None and not self.schema.strip():
            raise ValueError("schema must be a non-empty string")
        if self.policy is not None and self.branches:
            raise ValueError("policy and branches are mutually exclusive")

        if self.policy is not None:
            expected = {
                "RANGE": OracleRangePolicy,
                "LIST": OracleListPolicy,
                "HASH": OracleHashPolicy,
            }[self.method]
            if not isinstance(self.policy, expected):
                raise ValueError(
                    "a %s parent's policy must be a %s"
                    % (self.method, expected.__name__)
                )
        elif not self.branches:
            raise ValueError("plan requires policy or branches")

        if self.method == "RANGE" and len(self.columns) != 1:
            raise ValueError("Oracle RANGE partitioning takes exactly one parent column")

        branches = tuple(self.branches)
        for branch in branches:
            self._check_branch(branch)
        object.__setattr__(self, "branches", branches)

    def _check_branch(self, branch: OraclePartitionBranch) -> None:
        """Validate one explicit branch against the plan's strategies."""
        if not isinstance(branch, OraclePartitionBranch):
            raise TypeError("branches must contain OraclePartitionBranch instances")
        if self.method == "RANGE":
            if branch.less_than is None:
                raise ValueError("a RANGE parent's branch requires less_than")
            if branch.in_values is not None:
                raise ValueError("a RANGE parent's branch cannot carry in_values")
        elif self.method == "LIST":
            if branch.in_values is None:
                raise ValueError("a LIST parent's branch requires in_values")
            if branch.less_than is not None:
                raise ValueError("a LIST parent's branch cannot carry less_than")
        else:
            if branch.less_than is not None or branch.in_values is not None:
                raise ValueError("a HASH parent's branch must not carry a boundary")

        if branch.subpartitions_count is not None or branch.subpartitions:
            if self.subpartition is None:
                raise ValueError(
                    "branch %r declares subpartitions but the plan has no "
                    "table-level subpartition strategy"
                    % (branch.name or branch.label)
                )
        if branch.subpartitions_count is not None:
            if self.subpartition.method != "HASH":
                raise ValueError(
                    "subpartitions_count is only valid for HASH subpartitioning; "
                    "use explicit subpartitions for %s" % self.subpartition.method
                )
            if branch.subpartitions:
                raise ValueError(
                    "subpartitions_count and subpartitions are mutually exclusive"
                )
        if branch.subpartitions:
            assert self.subpartition is not None  # guarded above
            for subpartition in branch.subpartitions:
                _validate_subpartition_bound(subpartition, self.subpartition.method)

    def _name(self, suffix: str) -> str:
        return self.name_pattern.format(parent=self.parent, suffix=suffix)

    def branch_name(self, branch: OraclePartitionBranch) -> str:
        """Return the final table name of a first-level branch."""
        if branch.name is not None:
            return branch.name
        if branch.label is not None:
            return self._name(branch.label)
        if branch.less_than is not None:
            first = branch.less_than[0]
            if first == _MAXVALUE:
                return self._name("maxvalue")
            if isinstance(first, (date, datetime)):
                return self._name(first.strftime("%Y_%m_%d"))
            if isinstance(first, (int, float)):
                return self._name("lt_%s" % first)
        if branch.in_values is not None:
            values = branch.in_values[0] if len(branch.in_values) == 1 else branch.in_values
            return self._name(_label_from_values(values))
        return self._name("partition")

    def _policy_specs(
        self, policy: OraclePartitionPolicy, through: Optional[Any]
    ) -> Iterator[OraclePartitionSpec]:
        if isinstance(policy, OracleRangePolicy):
            for start, end in policy.iter_bounds(through=through):
                label = policy.label_for(start, end)
                yield OraclePartitionSpec(
                    name=self._name(label),
                    parent=self.parent,
                    method="RANGE",
                    columns=policy.columns,
                    less_than=(end,),
                    range_start=start,
                    range_end=end,
                    label=label,
                )
            if policy.maxvalue:
                label = "maxvalue"
                yield OraclePartitionSpec(
                    name=self._name(label),
                    parent=self.parent,
                    method="RANGE",
                    columns=policy.columns,
                    less_than=(_MAXVALUE,),
                    label=label,
                )
        elif isinstance(policy, OracleListPolicy):
            for values, label in policy.iter_groups():
                yield OraclePartitionSpec(
                    name=self._name(label),
                    parent=self.parent,
                    method="LIST",
                    columns=policy.columns,
                    in_values=(tuple(values),),
                    label=label,
                )
        else:
            for remainder in policy.iter_remainders():
                label = "p%d" % remainder
                yield OraclePartitionSpec(
                    name=self._name(label),
                    parent=self.parent,
                    method="HASH",
                    columns=policy.columns,
                    label=label,
                )

    def _branch_spec(self, branch: OraclePartitionBranch) -> OraclePartitionSpec:
        name = self.branch_name(branch)
        label = branch.name or branch.label
        range_start = None
        range_end = None
        if branch.less_than is not None and branch.less_than[0] != _MAXVALUE:
            range_end = branch.less_than[0]
        return OraclePartitionSpec(
            name=name,
            parent=self.parent,
            method=self.method,
            columns=self.columns,
            less_than=branch.less_than,
            in_values=branch.in_values,
            subpartitions_count=branch.subpartitions_count,
            subpartitions=branch.subpartitions,
            range_start=range_start,
            range_end=range_end,
            label=label,
        )

    def iter_specs(self, through: Optional[Any] = None) -> Iterator[OraclePartitionSpec]:
        """Yield every top-level partition the plan describes.

        Names are checked for uniqueness here rather than at execution time.

        Raises:
            ValueError: if two generated partitions would share a name, or if
                a generated RANGE policy has no horizon.
        """
        yielded: Dict[str, OraclePartitionSpec] = {}

        def emit(spec: OraclePartitionSpec) -> OraclePartitionSpec:
            if spec.name in yielded:
                raise ValueError("plan generates duplicate partition name %r" % spec.name)
            yielded[spec.name] = spec
            return spec

        if self.policy is not None:
            for spec in self._policy_specs(self.policy, through):
                yield emit(spec)
            return

        for branch in self.branches:
            yield emit(self._branch_spec(branch))

    def expected_subpartition_count(self, spec: OraclePartitionSpec) -> Optional[int]:
        """Return the subpartition count a spec implies, or ``None``.

        An explicit per-partition definition set wins; otherwise a per-partition
        count override; otherwise the table-level default count / template size.
        """
        if self.subpartition is None:
            return None
        if spec.subpartitions:
            return len(spec.subpartitions)
        if spec.subpartitions_count is not None:
            return spec.subpartitions_count
        if self.subpartition.templates:
            return len(self.subpartition.templates)
        return self.subpartition.count

    def is_fully_bounded(self, through: Optional[Any] = None) -> bool:
        """Whether every policy has a finite horizon.

        A plan whose RANGE policy has neither explicit bounds nor *count*, and
        whose call supplies no *through*, cannot say which partitions are
        expected in the future. ``list`` still reports what exists, but cannot
        label extra partitions orphans in that case.
        """
        if through is not None:
            return True
        if self.policy is not None:
            if isinstance(self.policy, OracleRangePolicy):
                return self.policy.bounds is not None or self.policy.count is not None
            return True
        return True
