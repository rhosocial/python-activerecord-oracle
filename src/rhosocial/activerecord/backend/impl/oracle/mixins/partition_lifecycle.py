# src/rhosocial/activerecord/backend/impl/oracle/mixins/partition_lifecycle.py
"""Oracle partition maintenance statement formatters.

This mixin implements the public ``format_*`` formatters for the Oracle
partition maintenance expressions declared in
:mod:`..expression.partition_lifecycle`. It is mixed into the Oracle
dialect alongside :class:`OraclePartitionMixin` so all partition
formatters live on the dialect instance.

All formatters are public (no leading underscore) per
expression-dialect-architecture §8.
"""
from typing import Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

from ..expression.partition_lifecycle import (
    OracleAddPartitionExpression,
    OracleDropPartitionExpression,
    OracleExchangePartitionExpression,
    OracleMergePartitionsExpression,
    OracleMovePartitionExpression,
    OracleSplitPartitionExpression,
    OracleTruncatePartitionExpression,
)


class OraclePartitionLifecycleMixin:
    """Oracle partition maintenance statement formatters.

    These formatters are gated by the corresponding ``supports_*``
    capability methods defined on :class:`OraclePartitionMixin`.
    """

    # ------------------------------------------------------------------
    # ADD PARTITION
    # ------------------------------------------------------------------
    def format_add_partition_statement(
        self, expr: OracleAddPartitionExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t ADD PARTITION (...)``."""
        if not self.supports_add_partition():
            raise UnsupportedFeatureError(
                self.name,
                "ADD PARTITION",
                f"Oracle {self.version} does not support ADD PARTITION.",
            )
        table_sql = self.format_identifier(expr.table)
        # Determine the strategy from the partition definition: if it has
        # less_than set -> RANGE; if in_values set -> LIST.
        part_def = expr.partition
        if part_def.less_than is not None:
            strategy = "RANGE"
        elif part_def.in_values is not None:
            strategy = "LIST"
        else:
            # HASH partitions can be added by name without VALUES clause.
            strategy = "HASH"
        part_sql, _ = self.format_partition_definition(part_def, strategy=strategy)
        return f"ALTER TABLE {table_sql} ADD {part_sql}", ()

    # ------------------------------------------------------------------
    # DROP PARTITION
    # ------------------------------------------------------------------
    def format_drop_partition_statement(
        self, expr: OracleDropPartitionExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t DROP PARTITION p [UPDATE INDEXES]``."""
        if not self.supports_drop_partition():
            raise UnsupportedFeatureError(
                self.name,
                "DROP PARTITION",
                f"Oracle {self.version} does not support DROP PARTITION.",
            )
        table_sql = self.format_identifier(expr.table)
        part_sql = self.format_identifier(expr.partition_name)
        sql = f"ALTER TABLE {table_sql} DROP PARTITION {part_sql}"
        if expr.update_indexes:
            sql = f"{sql} UPDATE INDEXES"
        return sql, ()

    # ------------------------------------------------------------------
    # SPLIT PARTITION (Oracle-specific)
    # ------------------------------------------------------------------
    def format_split_partition_statement(
        self, expr: OracleSplitPartitionExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t SPLIT PARTITION p AT (...) INTO (p1, p2)``."""
        if not self.supports_split_partition():
            raise UnsupportedFeatureError(
                self.name,
                "SPLIT PARTITION",
                f"Oracle {self.version} does not support SPLIT PARTITION.",
            )
        table_sql = self.format_identifier(expr.table)
        part_sql = self.format_identifier(expr.partition_name)
        at_parts = [self.format_partition_boundary_value(v) for v in expr.at_values]
        at_sql = ", ".join(at_parts)
        # In the INTO partition list, Oracle does NOT accept a VALUES clause
        # for SPLIT PARTITION (ORA-14020). Only the partition name (and
        # optional physical attributes like TABLESPACE) are allowed; the
        # split boundary is solely determined by the AT (...) expression.
        new_parts = [f"PARTITION {self.format_identifier(np.name)}"
                     for np in expr.new_partitions]
        new_sql = ", ".join(new_parts)
        return (
            f"ALTER TABLE {table_sql} SPLIT PARTITION {part_sql} "
            f"AT ({at_sql}) INTO ({new_sql})",
            (),
        )

    # ------------------------------------------------------------------
    # MERGE PARTITIONS
    # ------------------------------------------------------------------
    def format_merge_partitions_statement(
        self, expr: OracleMergePartitionsExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t MERGE PARTITIONS p1, p2 INTO p3``."""
        if not self.supports_merge_partition():
            raise UnsupportedFeatureError(
                self.name,
                "MERGE PARTITIONS",
                f"Oracle {self.version} does not support MERGE PARTITIONS.",
            )
        table_sql = self.format_identifier(expr.table)
        names_sql = ", ".join(self.format_identifier(n) for n in expr.partition_names)
        # In Oracle MERGE PARTITIONS, the INTO clause only accepts the
        # resulting partition name (and optional physical attributes); it
        # must NOT include a VALUES LESS THAN clause (ORA-14020). The merged
        # partition inherits its upper bound from the higher of the two
        # source partitions.
        into_name = self.format_identifier(expr.into_partition.name)
        return (
            f"ALTER TABLE {table_sql} MERGE PARTITIONS {names_sql} "
            f"INTO PARTITION {into_name}",
            (),
        )

    # ------------------------------------------------------------------
    # EXCHANGE PARTITION
    # ------------------------------------------------------------------
    def format_exchange_partition_statement(
        self, expr: OracleExchangePartitionExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t EXCHANGE PARTITION p WITH TABLE other [...]``."""
        if not self.supports_exchange_partition():
            raise UnsupportedFeatureError(
                self.name,
                "EXCHANGE PARTITION",
                f"Oracle {self.version} does not support EXCHANGE PARTITION.",
            )
        table_sql = self.format_identifier(expr.table)
        part_sql = self.format_identifier(expr.partition_name)
        with_sql = self.format_identifier(expr.with_table)
        sql = (
            f"ALTER TABLE {table_sql} EXCHANGE PARTITION {part_sql} "
            f"WITH TABLE {with_sql}"
        )
        if expr.including_indexes:
            sql = f"{sql} INCLUDING INDEXES"
        if expr.with_validation:
            sql = f"{sql} WITH VALIDATION"
        else:
            sql = f"{sql} WITHOUT VALIDATION"
        return sql, ()

    # ------------------------------------------------------------------
    # MOVE PARTITION (Oracle-specific)
    # ------------------------------------------------------------------
    def format_move_partition_statement(
        self, expr: OracleMovePartitionExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t MOVE PARTITION p [TABLESPACE ts]``."""
        if not self.supports_move_partition():
            raise UnsupportedFeatureError(
                self.name,
                "MOVE PARTITION",
                f"Oracle {self.version} does not support MOVE PARTITION.",
            )
        table_sql = self.format_identifier(expr.table)
        part_sql = self.format_identifier(expr.partition_name)
        sql = f"ALTER TABLE {table_sql} MOVE PARTITION {part_sql}"
        if expr.tablespace_name is not None:
            ts_sql = self.format_identifier(expr.tablespace_name)
            sql = f"{sql} TABLESPACE {ts_sql}"
        return sql, ()

    # ------------------------------------------------------------------
    # TRUNCATE PARTITION
    # ------------------------------------------------------------------
    def format_truncate_partition_statement(
        self, expr: OracleTruncatePartitionExpression
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE t TRUNCATE PARTITION p [UPDATE INDEXES]``."""
        if not self.supports_truncate_partition():
            raise UnsupportedFeatureError(
                self.name,
                "TRUNCATE PARTITION",
                f"Oracle {self.version} does not support TRUNCATE PARTITION.",
            )
        table_sql = self.format_identifier(expr.table)
        part_sql = self.format_identifier(expr.partition_name)
        sql = f"ALTER TABLE {table_sql} TRUNCATE PARTITION {part_sql}"
        if expr.update_indexes:
            sql = f"{sql} UPDATE INDEXES"
        return sql, ()
