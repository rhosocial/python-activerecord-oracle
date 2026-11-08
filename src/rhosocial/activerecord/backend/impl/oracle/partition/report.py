# src/rhosocial/activerecord/backend/impl/oracle/partition/report.py
"""Oracle's partition reports extend the core maintenance contract.

The row shape, the statement record and the report envelope come from
:mod:`rhosocial.activerecord.backend.partition`. Oracle adds the two facts only
its dictionary exposes on every composite row: the table-level subpartition
strategy and the subpartition count. The report keeps the core envelope and the
core field name ``target``; ``parent`` remains as a read-only alias for code
written before the contract existed.

Oracle's DDL is not transactional -- ``ALTER TABLE`` commits implicitly -- so
the core ``errors`` field carries the failures an operation hit. ``operations``
lists only the statements that were actually executed, or would be under a dry
run.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from rhosocial.activerecord.backend.partition import (
    PartitionInfo,
    PartitionOperation,
    PartitionReport,
)

__all__ = [
    "OraclePartitionInfo",
    "OraclePartitionOperation",
    "OraclePartitionReport",
]


@dataclass(frozen=True)
class OraclePartitionInfo(PartitionInfo):
    """One partition or subpartition row as Oracle's dictionary reports it.

    Attributes:
        subpartition_method: The table-level subpartition strategy
            (``RANGE`` / ``LIST`` / ``HASH``), or ``None`` for a non-composite
            table. Every level-1 row of a composite table shares this value.
        subpartition_count: Number of subpartitions under a top-level
            partition; ``None`` for a leaf.
    """

    subpartition_method: Optional[str] = None
    subpartition_count: Optional[int] = None

    @property
    def high_value(self) -> Optional[str]:
        """Return ``HIGH_VALUE`` exactly as the dictionary reports it."""
        return self.bound

    @property
    def range_start(self) -> Optional[object]:
        """Return the parsed lower bound stored in ``extras``, when known."""
        return self.extras.get("range_start")

    @property
    def range_end(self) -> Optional[object]:
        """Return the parsed upper bound stored in ``extras``, when known."""
        return self.extras.get("range_end")

    @property
    def list_values(self) -> Tuple[object, ...]:
        """Return the parsed LIST value set stored in ``extras``, when known."""
        return tuple(self.extras.get("values", ()))


OraclePartitionOperation = PartitionOperation


@dataclass(frozen=True)
class OraclePartitionReport(PartitionReport):
    """The core report envelope, plus Oracle's historical ``parent`` alias."""

    @property
    def parent(self) -> str:
        """Return the core ``target`` field under its previous name."""
        return self.target

    @property
    def leaves(self) -> Tuple[OraclePartitionInfo, ...]:
        """Return rows with no children of their own (subpartition rows included)."""
        return tuple(
            info
            for info in self.partitions
            if info.level > 0 and not info.is_partitioned
        )
