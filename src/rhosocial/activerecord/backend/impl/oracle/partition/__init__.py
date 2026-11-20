# src/rhosocial/activerecord/backend/impl/oracle/partition/__init__.py
"""Oracle partition plan grammar.

Partition strategy lives in the backend, not on an ActiveRecord model: a
model only declares the primary-key shape a partitioned table needs. This
package describes Oracle's two-level tree -- its parent, key, policy and
generated branches -- as data. The DDL "how" is supplied by the backend's
suggestion mixin and the DDL expression classes; live tree inspection is
supplied by the introspection layer.

The module is owner-agnostic: it never hard-codes an application's table names
or retention defaults.
"""

from .bounds import Interval, add_interval, default_label, normalize_interval
from .high_value import ParsedHighValue, parse_high_value
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

__all__ = [
    "Interval",
    "OracleHashPolicy",
    "OracleListPolicy",
    "OraclePartitionBranch",
    "OraclePartitionPlan",
    "OraclePartitionPolicy",
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
