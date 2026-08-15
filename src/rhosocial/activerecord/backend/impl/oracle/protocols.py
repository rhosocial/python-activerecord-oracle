# src/rhosocial/activerecord/backend/impl/oracle/protocols.py
"""Backward-compatible re-exports from protocols/ package."""

from .protocols import (
    HierarchicalQuerySupport,
    PivotSupport,
    QueryHintSupport,
    OracleLockingSupport,
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