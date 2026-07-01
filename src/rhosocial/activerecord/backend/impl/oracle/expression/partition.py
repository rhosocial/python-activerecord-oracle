# src/rhosocial/activerecord/backend/impl/oracle/expression/partition.py
"""Oracle partition DDL expressions.

This module defines backend-specific partition clause expressions that
upgrade Phase 4's ``dialect_options``-based partition definition carrying
into structured ``Expression`` / dataclass objects.

Phase 5 scope (this module):

* ``OraclePartitionDefinition`` — a single ``PARTITION ... VALUES ...``
  definition (RANGE / LIST / SYSTEM), with optional subpartition
  definitions for composite partitioning.
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
MOVE / TRUNCATE PARTITION) live in :mod:`.partition_lifecycle`.

All expressions delegate SQL generation to the dialect through public
``format_*`` formatters declared in
:class:`~rhosocial.activerecord.backend.impl.oracle.protocols.partition.OraclePartitionSupport`.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from math import isfinite
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams
from rhosocial.activerecord.backend.expression.statements import PartitionClause

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


# ---------------------------------------------------------------------------
# Strategy enums
# ---------------------------------------------------------------------------


class OraclePartitionStrategy(Enum):
    """Oracle top-level partitioning strategies.

    Oracle supports RANGE, LIST, HASH, INTERVAL, REFERENCE and SYSTEM
    partitioning. SYSTEM partitioning (application-managed row placement)
    is intentionally out of Phase 5 scope.
    """

    RANGE = "RANGE"
    LIST = "LIST"
    HASH = "HASH"
    INTERVAL = "INTERVAL"
    REFERENCE = "REFERENCE"


class OracleSubpartitionStrategy(Enum):
    """Oracle composite subpartitioning strategies.

    Oracle allows the subpartition method to be RANGE, LIST or HASH (and
    the parent can be RANGE / LIST / HASH). INTERVAL / REFERENCE cannot be
    used as a subpartition method.
    """

    RANGE = "RANGE"
    LIST = "LIST"
    HASH = "HASH"


# ---------------------------------------------------------------------------
# Boundary value expressions
# ---------------------------------------------------------------------------


class OraclePartitionMaxValue(BaseExpression):
    """Oracle ``MAXVALUE`` partition boundary token (RANGE upper bound)."""

    def __init__(self, dialect: "OracleDialect"):
        super().__init__(dialect)

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_partition_value(self)


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

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_partition_value(self)


# ---------------------------------------------------------------------------
# Partition / subpartition definitions
# ---------------------------------------------------------------------------


@dataclass
class OracleSubpartitionDefinition:
    """A single named subpartition within a partition definition.

    Used when individual subpartitions need explicit names or distinct
    boundary values. When omitted, Oracle applies the template from the
    ``SUBPARTITION BY`` clause automatically.

    Raises:
        ValueError: if ``name`` is empty or whitespace-only.
        TypeError: if ``dialect_options`` is not a dict when provided.
    """

    name: str
    less_than: Optional[Sequence[BaseExpression]] = None
    in_values: Optional[Sequence[Union[BaseExpression, Sequence[BaseExpression]]]] = None
    dialect_options: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("subpartition name must be a non-empty string")
        if self.dialect_options is not None and not isinstance(self.dialect_options, dict):
            raise TypeError(
                "dialect_options must be dict or None, "
                f"got {type(self.dialect_options).__name__}"
            )


@dataclass
class OraclePartitionDefinition:
    """An Oracle ``PARTITION ... VALUES ...`` definition.

    For RANGE: provide ``less_than`` (sequence of ``BaseExpression`` or
    ``OraclePartitionMaxValue``). The special ``MAXVALUE`` upper bound is
    expressed via :class:`OraclePartitionMaxValue`.

    For LIST: provide ``in_values``. Single-column LIST uses a flat
    sequence of ``BaseExpression``; multi-column LIST uses a sequence of
    sequences (row tuples).

    For HASH / composite HASH subpartitioning: neither ``less_than`` nor
    ``in_values`` is required; explicit ``subpartition_definitions`` may
    be provided to override the template.

    Raises:
        ValueError: if both ``less_than`` and ``in_values`` are provided,
                    or if neither is provided when the strategy requires
                    boundaries (the strategy check is enforced by the
                    formatter; this dataclass only rejects the mutual
                    exclusion violation).
        TypeError: if ``dialect_options`` is not a dict when provided.
    """

    name: str
    less_than: Optional[Sequence[BaseExpression]] = None
    in_values: Optional[Sequence[Union[BaseExpression, Sequence[BaseExpression]]]] = None
    subpartition_definitions: Optional[Sequence[OracleSubpartitionDefinition]] = None
    dialect_options: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("partition name must be a non-empty string")
        if self.less_than is not None and self.in_values is not None:
            raise ValueError("less_than and in_values are mutually exclusive")
        if self.dialect_options is not None and not isinstance(self.dialect_options, dict):
            raise TypeError(
                "dialect_options must be dict or None, "
                f"got {type(self.dialect_options).__name__}"
            )


# ---------------------------------------------------------------------------
# Subpartition template clause
# ---------------------------------------------------------------------------


class OracleSubpartitionClause(BaseExpression):
    """Oracle ``SUBPARTITION BY {RANGE|LIST|HASH}(...)`` template clause.

    Appears after ``PARTITION BY ...`` in composite partitioning. For
    HASH subpartitioning, ``count`` renders ``SUBPARTITIONS N``; for
    RANGE / LIST subpartitioning, individual subpartition definitions
    are declared per-partition via
    :class:`OraclePartitionDefinition.subpartition_definitions`.

    Raises:
        TypeError: if ``strategy`` is not an :class:`OracleSubpartitionStrategy`.
        ValueError: if ``count`` is provided but is not a positive integer.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        strategy: OracleSubpartitionStrategy,
        *,
        keys: Optional[Sequence[BaseExpression]] = None,
        count: Optional[int] = None,
        templates: Optional[Sequence[OracleSubpartitionDefinition]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(strategy, OracleSubpartitionStrategy):
            raise TypeError(
                "strategy must be an OracleSubpartitionStrategy value, "
                f"got {type(strategy).__name__}"
            )
        if count is not None:
            if not isinstance(count, int) or isinstance(count, bool):
                raise TypeError(
                    "count must be an int, "
                    f"got {type(count).__name__}"
                )
            if count <= 0:
                raise ValueError(f"count must be a positive integer, got {count}")
        self.strategy = strategy
        self.keys = list(keys) if keys else []
        self.count = count
        self.templates = list(templates) if templates else None

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_subpartition_by(self)


# ---------------------------------------------------------------------------
# Partition clause subclasses
# ---------------------------------------------------------------------------


class OraclePartitionClause(PartitionClause):
    """Base Oracle partition clause with Oracle-specific strategy enum."""

    strategy_type = OraclePartitionStrategy


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
        partitions: Optional[Sequence[OraclePartitionDefinition]] = None,
        subpartition_by: Optional[OracleSubpartitionClause] = None,
    ):
        super().__init__(dialect, OraclePartitionStrategy.RANGE, keys)
        if subpartition_by is not None and not isinstance(subpartition_by, OracleSubpartitionClause):
            raise TypeError("subpartition_by must be an OracleSubpartitionClause")
        self.partitions: List[OraclePartitionDefinition] = list(partitions or [])
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
        partitions: Optional[Sequence[OraclePartitionDefinition]] = None,
        subpartition_by: Optional[OracleSubpartitionClause] = None,
    ):
        super().__init__(dialect, OraclePartitionStrategy.LIST, keys)
        if subpartition_by is not None and not isinstance(subpartition_by, OracleSubpartitionClause):
            raise TypeError("subpartition_by must be an OracleSubpartitionClause")
        self.partitions: List[OraclePartitionDefinition] = list(partitions or [])
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
        partitions: Optional[Sequence[OraclePartitionDefinition]] = None,
        subpartition_by: Optional[OracleSubpartitionClause] = None,
    ):
        super().__init__(dialect, OraclePartitionStrategy.HASH, keys)
        if subpartition_by is not None and not isinstance(subpartition_by, OracleSubpartitionClause):
            raise TypeError("subpartition_by must be an OracleSubpartitionClause")
        if partitions_count is not None:
            if not isinstance(partitions_count, int) or isinstance(partitions_count, bool):
                raise TypeError(
                    "partitions_count must be an int, "
                    f"got {type(partitions_count).__name__}"
                )
            if partitions_count <= 0:
                raise ValueError(
                    f"partitions_count must be a positive integer, got {partitions_count}"
                )
        self.partitions_count = partitions_count
        self.partitions: List[OraclePartitionDefinition] = list(partitions or [])
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
        partitions: Sequence[OraclePartitionDefinition],
    ):
        super().__init__(dialect, OraclePartitionStrategy.INTERVAL, keys)
        if len(self.keys) != 1:
            raise ValueError(
                "INTERVAL partitioning requires exactly one partition key column, "
                f"got {len(self.keys)}"
            )
        if not isinstance(interval, BaseExpression):
            raise TypeError(
                "interval must be a BaseExpression, "
                f"got {type(interval).__name__}"
            )
        if not partitions:
            raise ValueError("INTERVAL partitioning requires at least one seed partition")
        self.interval = interval
        self.partitions: List[OraclePartitionDefinition] = list(partitions)


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
        self.method = OraclePartitionStrategy.REFERENCE.value
        self.keys: List[BaseExpression] = []
        self.dialect_options: Dict[str, Any] = {}
        self.fk_constraint = fk_constraint
