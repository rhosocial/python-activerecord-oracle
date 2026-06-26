# src/rhosocial/activerecord/backend/impl/oracle/protocols/__init__.py
"""Oracle-specific protocol definitions."""

from .hierarchical import HierarchicalQuerySupport
from .pivot import PivotSupport
from .hint import QueryHintSupport
from .locking import OracleLockingSupport
from .json_support import (
    OracleNativeJSONSupport,
    OracleBooleanTypeSupport,
    OracleVectorTypeSupport,
    OracleJSONDualitySupport,
)

__all__ = [
    "HierarchicalQuerySupport",
    "PivotSupport",
    "QueryHintSupport",
    "OracleLockingSupport",
    "OracleNativeJSONSupport",
    "OracleBooleanTypeSupport",
    "OracleVectorTypeSupport",
    "OracleJSONDualitySupport",
]