# src/rhosocial/activerecord/backend/impl/oracle/mixins/backend_mixin.py
"""Oracle-specific backend functionality mixin."""

import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, Tuple, Type

from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter


class OracleBackendMixin:
    """Mixin providing Oracle-specific backend functionality."""

    _default_suggestions_cache = None

    def _register_oracle_adapters(self) -> None:
        from ..adapters import (
            OracleBooleanAdapter,
            OracleDateTimeAdapter,
            OracleDateAdapter,
            OracleTimeAdapter,
            OracleDecimalAdapter,
            OracleJSONAdapter,
            OracleBytesAdapter,
            OracleStringAdapter,
            OracleIntervalAdapter,
            OracleRowIDAdapter,
            OracleXMLAdapter,
            OracleSDOGeometryAdapter,
            OracleVectorAdapter,
        )

        self._default_suggestions_cache = None

        oracle_adapters = [
            OracleBooleanAdapter(),
            OracleDateTimeAdapter(),
            OracleDateAdapter(),
            OracleTimeAdapter(),
            OracleDecimalAdapter(),
            OracleJSONAdapter(),
            OracleBytesAdapter(),
            OracleStringAdapter(),
            OracleIntervalAdapter(),
            OracleRowIDAdapter(),
        ]

        version = self._version if hasattr(self, '_version') and self._version else (23, 0, 0)

        oracle_adapters.append(OracleXMLAdapter())
        oracle_adapters.append(OracleSDOGeometryAdapter())

        if version[0] >= 23:
            oracle_adapters.append(OracleVectorAdapter())

        for adapter in oracle_adapters:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type, allow_override=True)

        self.log(logging.DEBUG, "Registered Oracle-specific type adapters.")

    def get_default_adapter_suggestions(self) -> Dict[Type, Tuple[SQLTypeAdapter, Type]]:
        if hasattr(self, '_default_suggestions_cache') and self._default_suggestions_cache is not None:
            return self._default_suggestions_cache

        suggestions: Dict[Type, Tuple[SQLTypeAdapter, Type]] = {}

        type_mappings = [
            (bool, int),
            (str, str),
            (datetime, str),
            (date, str),
            (time, str),
            (Decimal, float),
            (dict, str),
            (list, str),
            (bytes, bytes),
        ]

        for py_type, db_type in type_mappings:
            adapter = self.adapter_registry.get_adapter(py_type, db_type)
            if adapter:
                suggestions[py_type] = (adapter, db_type)
            else:
                self.log(
                    logging.DEBUG,
                    f"No adapter found for ({py_type.__name__}, {db_type.__name__}).",
                )

        self._default_suggestions_cache = suggestions
        return suggestions

    def _get_oracle_version_string(self) -> str:
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

    def log(self, level: int, message: str) -> None:
        if hasattr(self, '_logger') and self._logger:
            self._logger.log(level, message)
        else:
            print(f"[{logging.getLevelName(level)}] {message}")

    @property
    def dialect(self):
        from ..dialect import OracleDialect

        if self._dialect is None:
            self._dialect = OracleDialect(self._version)
        return self._dialect

    @dialect.setter
    def dialect(self, value):
        self._dialect = value

    @property
    def threadsafety(self) -> int:
        return 2

    def requires_manual_commit(self) -> bool:
        return not getattr(self.config, "autocommit", True)

    CONNECTION_ERROR_CODES = {12541, 12514, 12170, 1017, 1033, 1089, 3135}

    def _is_connection_error(self, error: Exception) -> bool:
        if hasattr(error, "code"):
            if error.code in self.CONNECTION_ERROR_CODES:
                return True
        error_str = str(error).lower()
        connection_error_patterns = [
            "no listener",
            "connection refused",
            "not connected",
            "tns",
            "broken pipe",
            "ora-",
        ]
        return any(pattern in error_str for pattern in connection_error_patterns)

    def _handle_error(self, error: Exception) -> None:
        from oracledb.exceptions import (
            DatabaseError as OracleDatabaseError,
            Error as OracleError,
            IntegrityError as OracleIntegrityError,
            OperationalError as OracleOperationalError,
        )
        from rhosocial.activerecord.backend.errors import (
            ConnectionError,
            DatabaseError,
            DeadlockError,
            IntegrityError,
            OperationalError,
            QueryError,
        )

        error_msg = str(error)

        if isinstance(error, OracleIntegrityError):
            raise IntegrityError(error_msg) from error
        elif isinstance(error, OracleDatabaseError):
            if "deadlock" in error_msg.lower():
                raise DeadlockError(error_msg) from error
            raise DatabaseError(error_msg) from error
        elif isinstance(error, OracleOperationalError):
            if self._is_connection_error(error):
                raise ConnectionError(error_msg) from error
            raise OperationalError(error_msg) from error
        elif isinstance(error, OracleError):
            raise DatabaseError(error_msg) from error
        else:
            raise error