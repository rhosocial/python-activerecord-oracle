# src/rhosocial/activerecord/backend/impl/oracle/protocols/partition.py
"""Oracle-specific table partitioning protocol.

This protocol extends the core ``PartitionSupport`` protocol with Oracle-
specific partition strategies (INTERVAL, REFERENCE, composite
subpartitioning) and Oracle partition maintenance statements
(ADD / DROP / SPLIT / MERGE / EXCHANGE / COALESCE / MOVE / TRUNCATE
PARTITION).

All formatters are public (no leading underscore) and declared here per
expression-dialect-architecture §8 (no pseudo-private SQL formatters).
"""

from typing import TYPE_CHECKING, Protocol, Tuple

from rhosocial.activerecord.backend.dialect.protocols import PartitionSupport

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import (
        PartitionClause,
        PartitionOptionsExpression,
    )
    from ..expression.partition import (
        OraclePartitionValue,
        OracleSubpartitionClause,
    )
    from ..expression.partition_lifecycle import (
        OracleAddPartitionStatement,
        OracleCoalescePartitionExpression,
        OracleDropPartitionStatement,
        OracleExchangePartitionStatement,
        OracleMergePartitionsStatement,
        OracleMovePartitionStatement,
        OracleSplitPartitionStatement,
        OracleTruncatePartitionStatement,
    )


class OraclePartitionSupport(PartitionSupport, Protocol):
    """Oracle table partitioning support protocol.

    Extends the generic ``PartitionSupport`` with Oracle-specific
    strategies (INTERVAL, REFERENCE, composite partitioning) and Oracle
    partition maintenance statements.
    """

    # ------------------------------------------------------------------
    # Oracle-specific capability methods
    # ------------------------------------------------------------------
    def supports_interval_partitioning(self) -> bool:
        """Whether INTERVAL partitioning (11g+) is supported."""
        ...  # pragma: no cover

    def supports_reference_partitioning(self) -> bool:
        """Whether REFERENCE partitioning (11g+) is supported."""
        ...  # pragma: no cover

    def supports_split_partition(self) -> bool:
        """Whether SPLIT PARTITION is supported."""
        ...  # pragma: no cover

    def supports_merge_partition(self) -> bool:
        """Whether MERGE PARTITIONS is supported."""
        ...  # pragma: no cover

    def supports_exchange_partition(self) -> bool:
        """Whether EXCHANGE PARTITION is supported."""
        ...  # pragma: no cover

    def supports_move_partition(self) -> bool:
        """Whether MOVE PARTITION is supported."""
        ...  # pragma: no cover

    def supports_coalesce_partition(self) -> bool:
        """Whether the count-less COALESCE PARTITION (hash tables) is supported."""
        ...  # pragma: no cover

    # ------------------------------------------------------------------
    # Generic partition clause formatter (overrides PartitionSupport)
    # ------------------------------------------------------------------
    def format_partition_clause(self, expr: "PartitionClause") -> Tuple[str, tuple]:
        """Format Oracle PARTITION BY clause from a partition expression."""
        ...  # pragma: no cover

    # ------------------------------------------------------------------
    # Oracle-specific strategy formatters
    # ------------------------------------------------------------------
    def format_subpartition_by(
        self, expr: "OracleSubpartitionClause"
    ) -> Tuple[str, tuple]:
        """Format ``SUBPARTITION BY {RANGE|LIST|HASH}(...)`` template."""
        ...  # pragma: no cover

    # ------------------------------------------------------------------
    # Definition / value formatters
    # ------------------------------------------------------------------
    def format_partition_definition_options(
        self, expr: "PartitionOptionsExpression"
    ) -> Tuple[str, tuple]:
        """Format the options node of a partition definition."""
        ...  # pragma: no cover

    def format_partition_value(self, expr: "OraclePartitionValue") -> Tuple[str, tuple]:
        """Format a partition boundary value (MAXVALUE or escaped literal)."""
        ...  # pragma: no cover

    # ------------------------------------------------------------------
    # Maintenance statement formatters
    # ------------------------------------------------------------------
    def format_add_partition_statement(
        self, expr: "OracleAddPartitionStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... ADD PARTITION``."""
        ...  # pragma: no cover

    def format_drop_partition_statement(
        self, expr: "OracleDropPartitionStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... DROP PARTITION``."""
        ...  # pragma: no cover

    def format_split_partition_statement(
        self, expr: "OracleSplitPartitionStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... SPLIT PARTITION ...``."""
        ...  # pragma: no cover

    def format_merge_partitions_statement(
        self, expr: "OracleMergePartitionsStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... MERGE PARTITIONS ... INTO ...``."""
        ...  # pragma: no cover

    def format_exchange_partition_statement(
        self, expr: "OracleExchangePartitionStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ...``."""
        ...  # pragma: no cover

    def format_move_partition_statement(
        self, expr: "OracleMovePartitionStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... MOVE PARTITION ...``."""
        ...  # pragma: no cover

    def format_coalesce_partition_statement(
        self, expr: "OracleCoalescePartitionExpression"
    ) -> Tuple[str, tuple]:
        """Format count-less ``ALTER TABLE ... COALESCE PARTITION``."""
        ...  # pragma: no cover

    def format_truncate_partition_statement(
        self, expr: "OracleTruncatePartitionStatement"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... TRUNCATE PARTITION ...``."""
        ...  # pragma: no cover
