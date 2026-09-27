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
    OracleTypeDDLSupport,
    OracleTypeSupport,
    OracleUserDefinedTypeSupport,
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
    "OracleTypeDDLSupport",
    "OracleTypeSupport",
    "OracleUserDefinedTypeSupport",
]
