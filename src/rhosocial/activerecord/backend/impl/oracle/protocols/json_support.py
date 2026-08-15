# src/rhosocial/activerecord/backend/impl/oracle/protocols/json_support.py
"""Protocols for Oracle JSON/Duality/Boolean/Vector support."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class OracleNativeJSONSupport(Protocol):
    def supports_native_json(self) -> bool: ...


@runtime_checkable
class OracleBooleanTypeSupport(Protocol):
    def supports_boolean_type(self) -> bool: ...


@runtime_checkable
class OracleVectorTypeSupport(Protocol):
    def supports_vector_type(self) -> bool: ...


@runtime_checkable
class OracleJSONDualitySupport(Protocol):
    def supports_json_duality(self) -> bool: ...