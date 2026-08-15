# src/rhosocial/activerecord/backend/impl/oracle/expression/partition_lifecycle.py
"""Oracle partition lifecycle (maintenance) DDL expressions.

This module defines the backend-specific expressions for Oracle partition
maintenance statements:

* ``ALTER TABLE ... ADD PARTITION`` (single partition)
* ``ALTER TABLE ... DROP PARTITION``
* ``ALTER TABLE ... SPLIT PARTITION p AT (...) INTO (p1, p2)``
* ``ALTER TABLE ... MERGE PARTITIONS p1, p2 INTO p3``
* ``ALTER TABLE ... EXCHANGE PARTITION p WITH TABLE other``
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

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect
    from .partition import OraclePartitionDefinition


class _OraclePartitionMaintenanceExpression(BaseExpression):
    """Common base for Oracle partition maintenance expressions.

    Stores the target table name and provides the shared capability-gating
    hook. Subclasses define their own ``to_sql()`` delegating to the
    dialect formatter.
    """

    def __init__(self, dialect: "OracleDialect", table: str):
        super().__init__(dialect)
        if not isinstance(table, str) or not table.strip():
            raise ValueError("table must be a non-empty string")
        self.table = table


class OracleAddPartitionExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... ADD PARTITION`` expression.

    Adds a single partition to a RANGE or LIST partitioned table. For
    INTERVAL partitioned tables Oracle creates partitions automatically,
    so ADD PARTITION is typically used with RANGE/LIST.

    Args:
        table: target table name.
        partition: an :class:`OraclePartitionDefinition` describing the
            new partition (with ``less_than`` for RANGE or ``in_values``
            for LIST).

    Raises:
        TypeError: if ``partition`` is not an :class:`OraclePartitionDefinition`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        partition: "OraclePartitionDefinition",
    ):
        super().__init__(dialect, table)
        from .partition import OraclePartitionDefinition  # local to avoid cycle

        if not isinstance(partition, OraclePartitionDefinition):
            raise TypeError(
                "partition must be an OraclePartitionDefinition, "
                f"got {type(partition).__name__}"
            )
        self.partition = partition

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_add_partition_statement(self)


class OracleDropPartitionExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... DROP PARTITION`` expression.

    Args:
        table: target table name.
        partition_name: name of the partition to drop.
        update_indexes: if True, append ``UPDATE INDEXES`` (Oracle 11g+)
            to maintain global indexes. Default False.

    Raises:
        ValueError: if ``partition_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        partition_name: str,
        *,
        update_indexes: bool = False,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        self.partition_name = partition_name
        self.update_indexes = bool(update_indexes)

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_partition_statement(self)


class OracleSplitPartitionExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... SPLIT PARTITION p AT (...) INTO (p1, p2)`` expression.

    Oracle-specific: splits an existing partition at a boundary value into
    two new partitions. The original partition is removed.

    Args:
        table: target table name.
        partition_name: name of the partition to split.
        at_values: boundary value(s) at which to split (sequence of
            ``OraclePartitionValue`` / ``Literal`` / ``"MAXVALUE"``).
        new_partitions: exactly two :class:`OraclePartitionDefinition`
            instances describing the resulting partitions.

    Raises:
        ValueError: if ``new_partitions`` does not contain exactly 2 entries.
        TypeError: if ``new_partitions`` entries are not
            :class:`OraclePartitionDefinition`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        partition_name: str,
        at_values: Sequence,
        new_partitions: Sequence["OraclePartitionDefinition"],
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
        from .partition import OraclePartitionDefinition

        for np in new_partitions:
            if not isinstance(np, OraclePartitionDefinition):
                raise TypeError(
                    "new_partitions must contain OraclePartitionDefinition instances, "
                    f"got {type(np).__name__}"
                )
        self.partition_name = partition_name
        self.at_values = list(at_values)
        self.new_partitions = list(new_partitions)

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_split_partition_statement(self)


class OracleMergePartitionsExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... MERGE PARTITIONS p1, p2 INTO p3`` expression.

    Merges two adjacent partitions into a single new partition.

    Args:
        table: target table name.
        partition_names: exactly two partition names to merge.
        into_partition: the :class:`OraclePartitionDefinition` for the
            resulting merged partition.

    Raises:
        ValueError: if ``partition_names`` does not contain exactly 2 entries.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        partition_names: Sequence[str],
        into_partition: "OraclePartitionDefinition",
    ):
        super().__init__(dialect, table)
        if len(partition_names) != 2:
            raise ValueError(
                f"MERGE PARTITIONS requires exactly 2 partition names, got {len(partition_names)}"
            )
        for pn in partition_names:
            if not isinstance(pn, str) or not pn.strip():
                raise ValueError("partition names must be non-empty strings")
        from .partition import OraclePartitionDefinition

        if not isinstance(into_partition, OraclePartitionDefinition):
            raise TypeError(
                "into_partition must be an OraclePartitionDefinition, "
                f"got {type(into_partition).__name__}"
            )
        self.partition_names = list(partition_names)
        self.into_partition = into_partition

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_merge_partitions_statement(self)


class OracleExchangePartitionExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... EXCHANGE PARTITION p WITH TABLE other`` expression.

    Exchanges the data segments of a partition with a standalone
    non-partitioned table.

    Args:
        table: target (partitioned) table name.
        partition_name: name of the partition to exchange.
        with_table: the non-partitioned table to exchange with.
        including_indexes: if True, append ``INCLUDING INDEXES``.
        with_validation: if True, append ``WITH VALIDATION`` (default True
            for Oracle; use False for ``WITHOUT VALIDATION``).

    Raises:
        ValueError: if ``partition_name`` or ``with_table`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        partition_name: str,
        with_table: str,
        *,
        including_indexes: bool = False,
        with_validation: bool = True,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        if not isinstance(with_table, str) or not with_table.strip():
            raise ValueError("with_table must be a non-empty string")
        self.partition_name = partition_name
        self.with_table = with_table
        self.including_indexes = bool(including_indexes)
        self.with_validation = bool(with_validation)

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_exchange_partition_statement(self)


class OracleMovePartitionExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... MOVE PARTITION p`` expression.

    Oracle-specific: physically moves a partition's segments (e.g. to
    another tablespace or to compact space). Optionally accepts
    ``tablespace_name`` to specify the target tablespace.

    Args:
        table: target table name.
        partition_name: name of the partition to move.
        tablespace_name: optional target tablespace name.

    Raises:
        ValueError: if ``partition_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
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

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_move_partition_statement(self)


class OracleTruncatePartitionExpression(_OraclePartitionMaintenanceExpression):
    """``ALTER TABLE ... TRUNCATE PARTITION p`` expression.

    Removes all rows from a partition (and its subpartitions).

    Args:
        table: target table name.
        partition_name: name of the partition to truncate.
        update_indexes: if True, append ``UPDATE INDEXES`` (Oracle 11g+).

    Raises:
        ValueError: if ``partition_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        partition_name: str,
        *,
        update_indexes: bool = False,
    ):
        super().__init__(dialect, table)
        if not isinstance(partition_name, str) or not partition_name.strip():
            raise ValueError("partition_name must be a non-empty string")
        self.partition_name = partition_name
        self.update_indexes = bool(update_indexes)

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_truncate_partition_statement(self)
