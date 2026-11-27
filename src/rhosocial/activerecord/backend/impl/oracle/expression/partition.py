# src/rhosocial/activerecord/backend/impl/oracle/expression/partition.py
"""Oracle partition DDL expressions.

This module defines backend-specific partition clause expressions as
structured ``Expression`` nodes.

Phase 5 scope (this module):

* ``PartitionDefinitionExpression`` / ``SubpartitionDefinitionExpression``
  (from core) — a single ``PARTITION ... VALUES ...`` definition (RANGE /
  LIST / SYSTEM) and its subpartitions for composite partitioning.
* ``partition_definition_options`` — carries Oracle's per-partition
  ``SUBPARTITIONS n`` override in a definition's ``options`` node.
* ``OraclePartitionValue`` / ``OraclePartitionMaxValue`` — boundary value
  expressions rendered as safely-escaped inline literals (Oracle DDL does
  not accept bind variables).
* ``OraclePartitionClause`` and strategy subclasses:
  ``OraclePartitionByRange``, ``OraclePartitionByList``,
  ``OraclePartitionByHash``.
* ``OracleSubpartitionClause`` — composite partitioning subpartition
  template (``SUBPARTITION BY {HASH|RANGE|LIST} ...``).
* ``OracleIntervalPartitionClause`` — INTERVAL partitioning (11g+).
* ``OracleReferencePartitionClause`` — REFERENCE partitioning.

Maintenance DDL expressions (ADD / DROP / SPLIT / MERGE / EXCHANGE /
COALESCE / MOVE / TRUNCATE PARTITION) live in :mod:`.partition_lifecycle`.

All expressions delegate SQL generation to the dialect through public
``format_*`` formatters declared in
:class:`~rhosocial.activerecord.backend.impl.oracle.protocols.partition.OraclePartitionSupport`.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from math import isfinite
from typing import Any, List, Optional, Sequence, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.statements import (
    PartitionClause,
    PartitionDefinitionExpression,
    PartitionOptionsExpression,
    SubpartitionDefinitionExpression,
)

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


# ---------------------------------------------------------------------------
# Strategy enums
# ---------------------------------------------------------------------------


class OracleSubpartitionStrategy(str, Enum):
    """Oracle subpartitioning strategies."""

    __str__ = str.__str__

    RANGE = "RANGE"
    LIST = "LIST"
    HASH = "HASH"


def normalize_subpartition_strategy(
    strategy: Union[str, OracleSubpartitionStrategy],
) -> OracleSubpartitionStrategy:
    """A member passes through; the word it carries becomes that member.

    The artifact carries the word, so a constructor reading one back gets a
    str and must be able to spell the member from it.
    """
    if isinstance(strategy, OracleSubpartitionStrategy):
        return strategy
    normalized = str(strategy).strip().upper()
    if normalized in OracleSubpartitionStrategy._value2member_map_:
        return OracleSubpartitionStrategy(normalized)
    raise ValueError(
        f"unsupported subpartition strategy: {strategy!r}; expected one of "
        f"{[member.value for member in OracleSubpartitionStrategy]}"
    )


# ---------------------------------------------------------------------------
# Boundary value expressions
# ---------------------------------------------------------------------------


class OraclePartitionMaxValue(BaseExpression):
    """Oracle ``MAXVALUE`` partition boundary token (RANGE upper bound)."""

    def __init__(self, dialect: "OracleDialect"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_partition_value"


class OraclePartitionValue(BaseExpression):
    """Literal value used in Oracle partition boundary definitions.

    Oracle DDL does not accept bind variables, so boundary values are
    rendered as safely-escaped inline literals by the dialect formatter.
    The value still flows through the Expression/Dialect layer so escaping
    stays centralized in the dialect.

    Raises:
        TypeError: if ``value`` has an unsupported type.
        ValueError: if a numeric value is non-finite.
    """

    def __init__(self, dialect: "OracleDialect", value: Any):
        super().__init__(dialect)
        if isinstance(value, float) and not isfinite(value):
            raise ValueError("partition value float must be finite")
        if isinstance(value, bool):
            raise TypeError("partition value must not be bool")
        if not isinstance(value, (str, int, float, Decimal, type(None))):
            from datetime import date, datetime

            if not isinstance(value, (date, datetime)):
                raise TypeError(
                    "partition value must be str, int, float, Decimal, "
                    "date, datetime, or None, got "
                    f"{type(value).__name__}"
                )
        self.value = value

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_partition_value"


# ---------------------------------------------------------------------------
# Partition / subpartition definition helpers
# ---------------------------------------------------------------------------


def partition_definition_options(
    dialect: "OracleDialect",
    *,
    subpartitions_count: Optional[int] = None,
) -> PartitionOptionsExpression:
    """Wrap Oracle's per-partition ``SUBPARTITIONS n`` override as options.

    ``PartitionDefinitionExpression`` carries backend-specific storage
    details in its ``options`` node; Oracle's only such detail is the
    per-partition hash subpartition count. Validation stays here so a bad
    count is refused when the definition is built, not when it renders.

    Raises:
        TypeError: if ``subpartitions_count`` is not an ``int``.
        ValueError: if ``subpartitions_count`` is not positive.
    """
    if subpartitions_count is not None:
        if isinstance(subpartitions_count, bool) or not isinstance(subpartitions_count, int):
            raise TypeError(
                "subpartitions_count must be an int, " f"got {type(subpartitions_count).__name__}"
            )
        if subpartitions_count <= 0:
            raise ValueError(
                "subpartitions_count must be a positive integer, " f"got {subpartitions_count}"
            )
    return PartitionOptionsExpression(dialect, {"subpartitions_count": subpartitions_count})


def subpartitions_count_of(definition: PartitionDefinitionExpression) -> Optional[int]:
    """Read the per-partition ``SUBPARTITIONS n`` override, if any."""
    options = getattr(definition, "options", None)
    if options is None:
        return None
    return options.options.get("subpartitions_count")


# ---------------------------------------------------------------------------
# Subpartition template clause
# ---------------------------------------------------------------------------


class OracleSubpartitionClause(BaseExpression):
    """Oracle ``SUBPARTITION BY {RANGE|LIST|HASH}(...)`` template clause.

    Appears after ``PARTITION BY ...`` in composite partitioning. It is the
    table-level subpartition strategy: every top-level partition shares it,
    and a partition may override only its subpartition definitions or count,
    never the strategy.

    For HASH subpartitioning, ``count`` renders ``SUBPARTITIONS N``; for
    RANGE / LIST subpartitioning, individual subpartition definitions are
    declared per-partition via ``PartitionDefinitionExpression.subpartitions``.
    ``templates`` renders ``SUBPARTITION TEMPLATE (...)`` so new top-level
    partitions inherit the same subpartition names and boundaries.

    Raises:
        TypeError: if ``strategy`` is not an :class:`OracleSubpartitionStrategy`.
        ValueError: if ``count`` is provided but is not a positive integer, or
            if ``count`` and ``templates`` are both provided. Oracle rejects
            ``SUBPARTITIONS n`` next to ``SUBPARTITION TEMPLATE`` (ORA-00922);
            a hash template states its count by listing its definitions.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        strategy: Union[str, OracleSubpartitionStrategy],
        *,
        keys: Optional[Sequence[BaseExpression]] = None,
        count: Optional[int] = None,
        templates: Optional[Sequence[SubpartitionDefinitionExpression]] = None,
    ):
        super().__init__(dialect)
        strategy = normalize_subpartition_strategy(strategy)
        if count is not None:
            if not isinstance(count, int) or isinstance(count, bool):
                raise TypeError("count must be an int, " f"got {type(count).__name__}")
            if count <= 0:
                raise ValueError(f"count must be a positive integer, got {count}")
            if templates:
                raise ValueError(
                    "count and templates are mutually exclusive: Oracle renders "
                    "SUBPARTITIONS n and SUBPARTITION TEMPLATE differently"
                )
        self.strategy = strategy
        self.keys = list(keys) if keys else []
        self.count = count
        self.templates = list(templates) if templates else None

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_subpartition_by"


# ---------------------------------------------------------------------------
# Partition clause subclasses
# ---------------------------------------------------------------------------


class OraclePartitionClause(PartitionClause):
    """Base Oracle partition clause.

    The strategy word is carried by the clause itself (``.method``), as a
    plain string, just like core's ``PartitionByRangeClause`` and siblings.
    Oracle-specific words outside core's RANGE/LIST/HASH vocabulary are set
    by the INTERVAL / REFERENCE subclasses, which bypass the vocabulary
    check the way the REFERENCE clause already did.
    """


class OraclePartitionByRange(OraclePartitionClause):
    """Oracle ``PARTITION BY RANGE(col)`` expression.

    Raises:
        TypeError: if ``subpartition_by`` is not an :class:`OracleSubpartitionClause`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        keys: Sequence[BaseExpression],
        *,
        partitions: Optional[Sequence[PartitionDefinitionExpression]] = None,
        subpartition_by: Optional[OracleSubpartitionClause] = None,
    ):
        super().__init__(dialect, "RANGE", keys)
        if subpartition_by is not None and not isinstance(
            subpartition_by, OracleSubpartitionClause
        ):
            raise TypeError("subpartition_by must be an OracleSubpartitionClause")
        self.partitions: List[PartitionDefinitionExpression] = list(partitions or [])
        self.subpartition_by = subpartition_by


class OraclePartitionByList(OraclePartitionClause):
    """Oracle ``PARTITION BY LIST(col)`` expression.

    Raises:
        TypeError: if ``subpartition_by`` is not an :class:`OracleSubpartitionClause`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        keys: Sequence[BaseExpression],
        *,
        partitions: Optional[Sequence[PartitionDefinitionExpression]] = None,
        subpartition_by: Optional[OracleSubpartitionClause] = None,
    ):
        super().__init__(dialect, "LIST", keys)
        if subpartition_by is not None and not isinstance(
            subpartition_by, OracleSubpartitionClause
        ):
            raise TypeError("subpartition_by must be an OracleSubpartitionClause")
        self.partitions: List[PartitionDefinitionExpression] = list(partitions or [])
        self.subpartition_by = subpartition_by


class OraclePartitionByHash(OraclePartitionClause):
    """Oracle ``PARTITION BY HASH(col) PARTITIONS N`` expression.

    Oracle HASH partitioning requires either an explicit partition count
    (``PARTITIONS N``) or an explicit partition definition list. Phase 5
    supports both forms.

    Raises:
        TypeError: if ``subpartition_by`` is not an :class:`OracleSubpartitionClause`.
        ValueError: if ``partitions_count`` is provided but is not a positive integer.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        keys: Sequence[BaseExpression],
        *,
        partitions_count: Optional[int] = None,
        partitions: Optional[Sequence[PartitionDefinitionExpression]] = None,
        subpartition_by: Optional[OracleSubpartitionClause] = None,
    ):
        super().__init__(dialect, "HASH", keys)
        if subpartition_by is not None and not isinstance(
            subpartition_by, OracleSubpartitionClause
        ):
            raise TypeError("subpartition_by must be an OracleSubpartitionClause")
        if partitions_count is not None:
            if not isinstance(partitions_count, int) or isinstance(partitions_count, bool):
                raise TypeError(
                    "partitions_count must be an int, " f"got {type(partitions_count).__name__}"
                )
            if partitions_count <= 0:
                raise ValueError(
                    f"partitions_count must be a positive integer, got {partitions_count}"
                )
        self.partitions_count = partitions_count
        self.partitions: List[PartitionDefinitionExpression] = list(partitions or [])
        self.subpartition_by = subpartition_by


class OracleIntervalPartitionClause(OraclePartitionClause):
    """Oracle ``PARTITION BY RANGE(col) INTERVAL(expr)`` expression (11g+).

    INTERVAL partitioning extends RANGE partitioning: Oracle automatically
    creates new partitions when inserted data exceeds the declared range
    boundaries. At least one partition (typically the first, with a
    concrete ``less_than`` boundary) must be declared.

    Args:
        keys: partition key columns (exactly one column for INTERVAL).
        interval: a ``BaseExpression`` (typically a ``Literal`` or
            interval expression) describing the interval. Oracle accepts
            literal expressions such as ``NUMTOYMINTERVAL(1,'YEAR')`` or
            ``INTERVAL '1' MONTH``; these are passed through as-is via
            the dialect's expression rendering.
        partitions: at least one seed partition definition.

    Raises:
        ValueError: if ``keys`` count is not 1, or if ``partitions`` is empty.
        TypeError: if ``interval`` is not a ``BaseExpression``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        keys: Sequence[BaseExpression],
        *,
        interval: BaseExpression,
        partitions: Sequence[PartitionDefinitionExpression],
    ):
        # INTERVAL is Oracle-only, outside core's RANGE/LIST/HASH vocabulary:
        # set the clause fields directly instead of going through the base's
        # closed-vocabulary check, while keeping its key validation.
        BaseExpression.__init__(self, dialect)
        if not keys:
            raise ValueError("keys are required")
        for key in keys:
            if not isinstance(key, BaseExpression):
                raise TypeError(
                    "keys must contain BaseExpression instances, " f"got {type(key).__name__}"
                )
        self.method = "INTERVAL"
        self.keys = list(keys)
        if len(self.keys) != 1:
            raise ValueError(
                "INTERVAL partitioning requires exactly one partition key column, "
                f"got {len(self.keys)}"
            )
        if not isinstance(interval, BaseExpression):
            raise TypeError("interval must be a BaseExpression, " f"got {type(interval).__name__}")
        if not partitions:
            raise ValueError("INTERVAL partitioning requires at least one seed partition")
        self.interval = interval
        self.partitions: List[PartitionDefinitionExpression] = list(partitions)


class OracleReferencePartitionClause(OraclePartitionClause):
    """Oracle ``PARTITION BY REFERENCE(fk_constraint)`` expression.

    REFERENCE partitioning partitions a child table by the partitioning
    strategy of its parent table, using a foreign key constraint. The
    child inherits the parent's partitions automatically. REFERENCE
    partitioning has no partition key columns of its own (the constraint
    determines partition placement), so this expression bypasses the
    generic ``PartitionClause`` key validation.

    Args:
        fk_constraint: name of the FOREIGN KEY constraint on the child
            table that references the parent. Rendered as an identifier.

    Raises:
        ValueError: if ``fk_constraint`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        fk_constraint: str,
    ):
        # Bypass PartitionClause.__init__ key validation: REFERENCE
        # partitioning has no partition key columns. Set the fields the
        # base class would set, with an empty key list.
        BaseExpression.__init__(self, dialect)
        if not isinstance(fk_constraint, str) or not fk_constraint.strip():
            raise ValueError("fk_constraint must be a non-empty string")
        self.method = "REFERENCE"
        self.keys: List[BaseExpression] = []
        self.fk_constraint = fk_constraint

    def check_renderable(self) -> None:
        """REFERENCE carries no key columns; the constraint is its payload.

        Core's ``PartitionClause.check_renderable`` requires at least one key
        expression, which is what a generic strategy clause needs and what this
        clause deliberately does not have -- the foreign key decides placement,
        so the base check must not be inherited. The identifier is the one thing
        this clause cannot render without.
        """
        if not isinstance(self.fk_constraint, str) or not self.fk_constraint.strip():
            raise TypeError(
                f"{type(self).__name__}.fk_constraint must be a non-empty string, "
                f"got {self.fk_constraint!r}"
            )
