# src/rhosocial/activerecord/backend/impl/oracle/mixins/backend_mixin.py
"""Oracle-specific backend functionality mixin."""

import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, Optional, Tuple, Type
from uuid import UUID

from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter


class OracleBackendMixin:
    """Mixin providing Oracle-specific backend functionality."""

    _default_suggestions_cache = None

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Double-quote an Oracle identifier (uppercased) for safe embedding.

        Used on externally-sourced identifiers (LOB write targets) where
        defense-in-depth quoting is warranted. Dot-separated qualified paths are
        quoted segment-by-segment.

        This is deliberately *not* how a qualified name is built for a
        statement -- that goes through the object's own rendering protocol (see
        :meth:`_qualified_name`), so a name has one rendering path and
        namespace slots cannot be lost.
        """
        if "." in identifier:
            return ".".join(
                f'"{part.replace(chr(34), chr(34) * 2).upper()}"'
                for part in identifier.split(".")
            )
        return f'"{identifier.replace(chr(34), chr(34) * 2).upper()}"'

    def _qualified_name(self, name: str, schema_name: Optional[str] = None) -> str:
        """Render a qualified relation name for a hand-built DML statement.

        Some DML statements are assembled directly rather than through an
        expression tree (``bulk_insert`` builds one statement per row so that
        Oracle can bind each row separately). Those paths must still emit the
        *same* text the expression path emits, so both build the same
        :class:`~rhosocial.activerecord.backend.expression.objects.Table` and
        render it through the same protocol instead of formatting the name here.
        Anything else is how the synchronous and asynchronous backends came to
        disagree: one produced ``"SCOTT"."ORDERS"`` and the other
        ``SCOTT.ORDERS`` for the same options object.

        Args:
            name: The relation's own name.
            schema_name: The owner (Oracle's schema), or ``None`` to leave the
                name unqualified.

        Returns:
            The qualified name as SQL text.

        Raises:
            ValueError: ``name`` is empty, or ``schema_name`` was given as an
                empty string.
        """
        from rhosocial.activerecord.backend.expression.objects import Table

        return Table(
            self.dialect, name, schema_name=schema_name
        ).to_sql()[0]

    def _identifier_list(self, identifiers) -> str:
        """Render a comma-separated list of plain identifiers (columns).

        Same rationale as :meth:`_qualified_name`: the column list of a
        hand-built statement is part of that statement's text, so the
        synchronous and asynchronous backends must build it the same way, and
        through the dialect rather than by hand.
        """
        return ", ".join(self.dialect.format_identifier(name) for name in identifiers)

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
            OracleUUIDAdapter,
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
            OracleUUIDAdapter(),
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
            (float, float),
            (dict, str),
            (list, str),
            (bytes, bytes),
            (UUID, bytes),
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