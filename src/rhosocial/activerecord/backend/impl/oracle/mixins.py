# src/rhosocial/activerecord/backend/impl/oracle/mixins.py
"""Oracle-specific mixin classes for backend and transaction management."""

from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, Any, Type, Optional, Tuple

from rhosocial.activerecord.backend.transaction import IsolationLevel
from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter


class OracleTransactionMixin:
    """Mixin providing Oracle-specific transaction handling."""

    # Oracle isolation level mapping (READ_ONLY is a transaction mode, not isolation level)
    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE",
        # Note: Oracle also supports READ ONLY as a transaction mode,
        # but it's not a standard SQL isolation level
    }

    @classmethod
    def get_isolation_level_string(cls, level: IsolationLevel) -> str:
        """Get Oracle isolation level string from enum."""
        return cls._ISOLATION_LEVELS.get(level, "READ COMMITTED")

    def supports_isolation_level(self, level: IsolationLevel) -> bool:
        """Check if isolation level is supported by Oracle."""
        return level in self._ISOLATION_LEVELS


class OracleBackendMixin:
    """Mixin providing Oracle-specific backend functionality."""

    # Note: _default_suggestions_cache is an instance variable, not class variable,
    # because each backend instance has its own adapter_registry.
    # We initialize it in __init__ via _register_oracle_adapters.

    def _register_oracle_adapters(self) -> None:
        """Register Oracle-specific type adapters to the adapter_registry."""
        from .adapters import (
            OracleBooleanAdapter,
            OracleDateTimeAdapter,
            OracleDateAdapter,
            OracleTimeAdapter,
            OracleDecimalAdapter,
            OracleJSONAdapter,
            OracleBytesAdapter,
            OracleIntervalAdapter,
            OracleRowIDAdapter,
            OracleXMLAdapter,
            OracleSDOGeometryAdapter,
            OracleVectorAdapter,
        )

        # Initialize instance-level cache
        self._default_suggestions_cache = None

        # Base adapters (always registered)
        oracle_adapters = [
            OracleBooleanAdapter(),
            OracleDateTimeAdapter(),
            OracleDateAdapter(),
            OracleTimeAdapter(),
            OracleDecimalAdapter(),
            OracleJSONAdapter(),
            OracleBytesAdapter(),
            OracleIntervalAdapter(),
            OracleRowIDAdapter(),
        ]

        # Version-specific adapters
        version = self._version if hasattr(self, '_version') and self._version else (23, 0, 0)
        
        # XMLType is available in all versions with XML DB
        oracle_adapters.append(OracleXMLAdapter())
        
        # SDO_GEOMETRY requires Spatial option
        oracle_adapters.append(OracleSDOGeometryAdapter())
        
        # VECTOR type requires Oracle 23ai+
        if version[0] >= 23:
            oracle_adapters.append(OracleVectorAdapter())

        for adapter in oracle_adapters:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type, allow_override=True)

        self.logger.debug("Registered Oracle-specific type adapters.")

    def get_default_adapter_suggestions(self) -> Dict[Type, Tuple["SQLTypeAdapter", Type]]:
        """Provides default type adapter suggestions for Oracle."""
        # Check instance-level cache
        if hasattr(self, '_default_suggestions_cache') and self._default_suggestions_cache is not None:
            return self._default_suggestions_cache

        suggestions: Dict[Type, Tuple["SQLTypeAdapter", Type]] = {}

        # Type mappings for Oracle
        type_mappings = [
            (bool, int),           # Oracle uses NUMBER(1) for boolean
            (datetime, str),       # Oracle TIMESTAMP
            (date, str),           # Oracle DATE
            (time, str),           # Store as VARCHAR2 or INTERVAL
            (Decimal, float),      # Oracle NUMBER
            (dict, str),           # Oracle JSON as VARCHAR2/CLOB
            (list, str),           # Oracle JSON as VARCHAR2/CLOB
            (bytes, bytes),        # Oracle BLOB
        ]

        for py_type, db_type in type_mappings:
            adapter = self.adapter_registry.get_adapter(py_type, db_type)
            if adapter:
                suggestions[py_type] = (adapter, db_type)
            else:
                self.logger.debug(f"No adapter found for ({py_type.__name__}, {db_type.__name__}).")

        self._default_suggestions_cache = suggestions
        return suggestions

    def _get_oracle_version_string(self) -> str:
        """Get human-readable Oracle version string."""
        version = self._version
        if version >= (23, 0, 0):
            return f"Oracle 23ai ({version[0]}.{version[1]}.{version[2]})"
        elif version >= (21, 0, 0):
            return f"Oracle 21c ({version[0]}.{version[1]}.{version[2]})"
        elif version >= (19, 0, 0):
            return f"Oracle 19c ({version[0]}.{version[1]}.{version[2]})"
        elif version >= (12, 2, 0):
            return f"Oracle 12c R2 ({version[0]}.{version[1]}.{version[2]})"
        elif version >= (12, 1, 0):
            return f"Oracle 12c R1 ({version[0]}.{version[1]}.{version[2]})"
        elif version >= (11, 2, 0):
            return f"Oracle 11g R2 ({version[0]}.{version[1]}.{version[2]})"
        elif version >= (11, 1, 0):
            return f"Oracle 11g R1 ({version[0]}.{version[1]}.{version[2]})"
        else:
            return f"Oracle {version[0]}.{version[1]}.{version[2]}"
