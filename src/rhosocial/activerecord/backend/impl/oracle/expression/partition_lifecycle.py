# src/rhosocial/activerecord/backend/impl/oracle/expression/partition_lifecycle.py
"""Oracle partition lifecycle (maintenance) DDL expressions.

This module defines the backend-specific expressions for Oracle partition
maintenance statements:

* ``ALTER TABLE ... ADD PARTITION`` (single partition)
* ``ALTER TABLE ... DROP PARTITION``
* ``ALTER TABLE ... SPLIT PARTITION p AT (...) INTO (p1, p2)``
* ``ALTER TABLE ... MERGE PARTITIONS p1, p2 INTO p3``
* ``ALTER TABLE ... EXCHANGE PARTITION p WITH TABLE other``
* ``ALTER TABLE ... COALESCE PARTITION``
* ``ALTER TABLE ... MOVE PARTITION p``
* ``ALTER TABLE ... TRUNCATE PARTITION p``

Each expression delegates SQL generation to a public ``format_*`` formatter
declared in
:class:`~rhosocial.activerecord.backend.impl.oracle.protocols.partition.OraclePartitionSupport`.

Note on parameter binding: Oracle DDL does not accept bind variables, so
partition boundary values referenced by SPLIT/ADD are rendered as
safely-escaped inline literals through
:meth:`OraclePartitionMixin.format_partition_boundary_value`.
"""

from __future__ import annotations

from typing import Optional, Sequence, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import PartitionDefinitionExpression

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class _OraclePartitionMaintenanceExpression(BaseExpression):
    """Common base for Oracle partition maintenance expressions.

    Stores the target table and provides the shared capability-gating
    hook. Subclasses define their own ``to_sql()`` delegating to the
    dialect formatter.
    """

    def __init__(self, dialect: "OracleDialect", table: Table):
        super().__init__(dialect)
        if not isinstance(table, Table):
            raise TypeError(f"table must be a Table, got {type(table).__name__}")
        self.table = table


class OracleAddPartitionStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... ADD PARTITION`` expression.

    Adds a single partition to a RANGE or LIST partitioned table. For
    INTERVAL partitioned tables Oracle creates partitions automatically,
    so ADD PARTITION is typically used with RANGE/LIST.

    Args:
        table: the target table, carrying its owner.
        partition: a :class:`PartitionDefinitionExpression` describing the
            new partition (with ``less_than`` for RANGE or ``in_values``
            for LIST).

    Raises:
        TypeError: if ``partition`` is not a :class:`PartitionDefinitionExpression`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition: PartitionDefinitionExpression,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition, PartitionDefinitionExpression):
            raise TypeError(
                "partition must be a PartitionDefinitionExpression, "
                f"got {type(partition).__name__}"
            )
        self.partition = partition

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_add_partition_statement"


class OracleDropPartitionStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... DROP PARTITION`` expression.

    Args:
        table: the target table, carrying its owner.
        partition_name: name of the partition to drop.
        update_indexes: if True, append ``UPDATE INDEXES`` (Oracle 11g+)
            to maintain global indexes. Default False.

    Raises:
        ValueError: if ``partition_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition_name: str,
        *,
        update_indexes: bool = False,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        self.partition_name = partition_name
        self.update_indexes = bool(update_indexes)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_drop_partition_statement"


class OracleSplitPartitionStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... SPLIT PARTITION p AT (...) INTO (p1, p2)`` expression.

    Oracle-specific: splits an existing partition at a boundary value into
    two new partitions. The original partition is removed.

    Args:
        table: the target table, carrying its owner.
        partition_name: name of the partition to split.
        at_values: boundary value(s) at which to split (sequence of
            ``OraclePartitionValue`` / ``Literal`` / ``"MAXVALUE"``).
        new_partitions: exactly two :class:`PartitionDefinitionExpression`
            instances describing the resulting partitions.

    Raises:
        ValueError: if ``new_partitions`` does not contain exactly 2 entries.
        TypeError: if ``new_partitions`` entries are not
            :class:`PartitionDefinitionExpression`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition_name: str,
        at_values: Sequence,
        new_partitions: Sequence[PartitionDefinitionExpression],
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        if not at_values:
            raise ValueError("at_values must be a non-empty sequence")
        if len(new_partitions) != 2:
            raise ValueError(
                f"SPLIT PARTITION requires exactly 2 new partitions, got {len(new_partitions)}"
            )
        for np in new_partitions:
            if not isinstance(np, PartitionDefinitionExpression):
                raise TypeError(
                    "new_partitions must contain PartitionDefinitionExpression instances, "
                    f"got {type(np).__name__}"
                )
        self.partition_name = partition_name
        self.at_values = list(at_values)
        self.new_partitions = list(new_partitions)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_split_partition_statement"


class OracleMergePartitionsStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... MERGE PARTITIONS p1, p2 INTO p3`` expression.

    Merges two adjacent partitions into a single new partition.

    Args:
        table: the target table, carrying its owner.
        partition_names: exactly two partition names to merge.
        into_partition: the :class:`PartitionDefinitionExpression` for the
            resulting merged partition.

    Raises:
        ValueError: if ``partition_names`` does not contain exactly 2 entries.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition_names: Sequence[str],
        into_partition: PartitionDefinitionExpression,
    ):
        super().__init__(dialect, table)
        if len(partition_names) != 2:
            raise ValueError(
                f"MERGE PARTITIONS requires exactly 2 partition names, got {len(partition_names)}"
            )
        for pn in partition_names:
            if not isinstance(pn, str) or not pn.strip():
                raise ValueError("partition names must be non-empty strings")
        if not isinstance(into_partition, PartitionDefinitionExpression):
            raise TypeError(
                "into_partition must be a PartitionDefinitionExpression, "
                f"got {type(into_partition).__name__}"
            )
        self.partition_names = list(partition_names)
        self.into_partition = into_partition

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_merge_partitions_statement"


class OracleExchangePartitionStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... EXCHANGE PARTITION p WITH TABLE other`` expression.

    Exchanges the data segments of a partition with a standalone
    non-partitioned table.

    Args:
        table: the target (partitioned) table, carrying its owner.
        partition_name: name of the partition to exchange.
        with_table: the non-partitioned table to exchange with, carrying its
            own owner -- a segment exchange may cross owners.
        including_indexes: if True, append ``INCLUDING INDEXES``.
        with_validation: if True, append ``WITH VALIDATION`` (default True
            for Oracle; use False for ``WITHOUT VALIDATION``).

    Raises:
        ValueError: if ``partition_name`` is empty.
        TypeError: if ``with_table`` is not a :class:`Table`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition_name: str,
        with_table: Table,
        *,
        including_indexes: bool = False,
        with_validation: bool = True,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        if not isinstance(with_table, Table):
            raise TypeError(f"with_table must be a Table, got {type(with_table).__name__}")
        self.partition_name = partition_name
        self.with_table = with_table
        self.including_indexes = bool(including_indexes)
        self.with_validation = bool(with_validation)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_exchange_partition_statement"


class OracleCoalescePartitionExpression(BaseExpression):
    """``ALTER TABLE ... COALESCE PARTITION`` expression.

    Oracle's clause carries no partition count: each execution coalesces the
    last hash partition into the remaining ones and drops it, so shrinking by
    more than one partition means issuing the statement again. A target count
    is therefore not part of this DDL.

    Oracle can shrink a table in place only this way, and only a
    hash-partitioned table; ``supports_coalesce_partition`` states that on the
    dialect.

    Args:
        table: the target table, carrying its owner.

    The constructor only collects the parameter. The table's kind and the
    capability are validated in
    :meth:`~rhosocial.activerecord.backend.impl.oracle.mixins.partition.OraclePartitionMixin.format_coalesce_partition_statement`,
    so a node can be built before a dialect is bound.
    """

    def __init__(self, dialect: "OracleDialect", table: Table):
        super().__init__(dialect)
        self.table = table

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_coalesce_partition_statement"


class OracleMovePartitionStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... MOVE PARTITION p`` expression.

    Oracle-specific: physically moves a partition's segments (e.g. to
    another tablespace or to compact space). Optionally accepts
    ``tablespace_name`` to specify the target tablespace.

    Args:
        table: the target table, carrying its owner.
        partition_name: name of the partition to move.
        tablespace_name: optional target tablespace name.

    Raises:
        ValueError: if ``partition_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition_name: str,
        *,
        tablespace_name: Optional[str] = None,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        if tablespace_name is not None:
            if not isinstance(tablespace_name, str) or not tablespace_name.strip():
                raise ValueError("tablespace_name must be a non-empty string when provided")
        self.partition_name = partition_name
        self.tablespace_name = tablespace_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_move_partition_statement"


class OracleTruncatePartitionStatement(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... TRUNCATE PARTITION p`` expression.

    Removes all rows from a partition (and its subpartitions).

    Args:
        table: the target table, carrying its owner.
        partition_name: name of the partition to truncate.
        update_indexes: if True, append ``UPDATE INDEXES`` (Oracle 11g+).

    Raises:
        ValueError: if ``partition_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        partition_name: str,
        *,
        update_indexes: bool = False,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        self.partition_name = partition_name
        self.update_indexes = bool(update_indexes)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_truncate_partition_statement"
