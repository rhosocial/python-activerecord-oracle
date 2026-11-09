# src/rhosocial/activerecord/backend/impl/oracle/partition/__init__.py
"""Oracle partition plans and maintenance.

Partition strategy lives in the backend, not on an ActiveRecord model: a
model only declares the primary-key shape a partitioned table needs. This
package describes Oracle's two-level tree and maintains it:

* :class:`OraclePartitionPlan` describes a parent, its key, its first-level
  policy or branches, and **one** table-level subpartition strategy.
* :class:`OraclePartitionMaintenance` and
  :class:`AsyncOraclePartitionMaintenance` reconcile that description with
  the ``ALL_*`` dictionary (``ensure`` / ``list`` / ``drop_before`` /
  ``drop_partition`` / ``validate``).

The module is owner-agnostic: it never hard-codes an application's table names
or retention defaults. Oracle DDL commits per statement, so reports carry
partial-completion errors rather than pretending to be transactional.
"""

from .bounds import Interval, add_interval, default_label, normalize_interval
from .high_value import ParsedHighValue, parse_high_value
from .maintenance import (
    AsyncOraclePartitionMaintenance,
    OraclePartitionMaintenance,
)
from .plan import (
    OracleHashPolicy,
    OracleListPolicy,
    OraclePartitionBranch,
    OraclePartitionPlan,
    OraclePartitionPolicy,
    OraclePartitionSpec,
    OracleRangePolicy,
    OracleSubpartitionPolicy,
    OracleSubpartitionSpec,
)
from .report import (
    OraclePartitionInfo,
    OraclePartitionOperation,
    OraclePartitionReport,
)

__all__ = [
    "AsyncOraclePartitionMaintenance",
    "Interval",
    "OracleHashPolicy",
    "OracleListPolicy",
    "OraclePartitionBranch",
    "OraclePartitionInfo",
    "OraclePartitionMaintenance",
    "OraclePartitionOperation",
    "OraclePartitionPlan",
    "OraclePartitionPolicy",
    "OraclePartitionReport",
    "OraclePartitionSpec",
    "OracleRangePolicy",
    "OracleSubpartitionPolicy",
    "OracleSubpartitionSpec",
    "ParsedHighValue",
    "add_interval",
    "default_label",
    "normalize_interval",
    "parse_high_value",
]
