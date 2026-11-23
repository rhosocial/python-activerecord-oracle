# src/rhosocial/activerecord/backend/impl/oracle/mixins/backend_mixin.py
"""Oracle-specific backend functionality mixin."""

import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Type
from uuid import UUID

from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter
from rhosocial.activerecord.backend.suggestion.definitions import (
    DataAdapterSuggestion,
    ExtraDataAdapterSuggestion,
)


class _OracleAdapterTable(dict):
    """Adapter table that enumerates as sparse but still resolves native keys.

    The backend-agnostic contract says native scalar types (``str``/``int``/
    ``float``/``bytes``) are absent from the suggestion table: absence is the
    "pass through untouched" signal. Oracle is the one backend where a native
    type still needs a correction adapter -- the server stores ``''`` as NULL,
    so a non-Optional ``str`` field reads back ``None`` unless the Oracle string
    adapter restores ``''``. That adapter has to be reachable through the
    field-level ``suggestions.get(str)`` lookup (it needs the field's declared
    annotation to tell ``Optional[str]`` from ``str``), but it is not a *suggested*
    declaration for the ``str`` type.

    This dict therefore hides the native keys from enumeration (``__iter__``,
    ``keys``, ``items``, ``values``, ``len``) while leaving ``get``/``__getitem__``
    at plain-dict semantics so the model layer still finds the correction
    adapter. Explicitly registered native adapters remain in the registry for
    ``UseAdapter`` declarations; only the implicit default is special-cased.
    """

    _NATIVE = frozenset({str, int, float, bytes})

    def _public_keys(self):
        return [key for key in dict.keys(self) if key not in self._NATIVE]

    def __iter__(self):
        return iter(self._public_keys())

    def __len__(self):
        return len(self._public_keys())

    def keys(self):
        return self._public_keys()

    def items(self):
        return [(key, dict.__getitem__(self, key)) for key in self._public_keys()]

    def values(self):
        return [dict.__getitem__(self, key) for key in self._public_keys()]


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
            OracleEnumAdapter,
            OracleSDOGeometryAdapter,
            OracleVectorAdapter,
            OracleNeutralGeometryAdapter,
            OracleNeutralVectorAdapter,
            OracleNeutralBitStringAdapter,
            OracleNeutralRangeAdapter,
            OracleNeutralMacAddressAdapter,
            OracleNeutralIPAddressAdapter,
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
            # Enums have no native Oracle type, so this adapter is the
            # backend's own answer for the core ``Enum`` concept (VARCHAR2 with
            # a CHECK constraint; member *name* by default). It overrides the
            # core generic enum adapter, which is what a backend-named
            # implementation is for.
            OracleEnumAdapter(),
        ]

        version = getattr(self, '_version', None)

        oracle_adapters.append(OracleXMLAdapter())
        oracle_adapters.append(OracleSDOGeometryAdapter())

        # Measured on both wired servers: ``VECTOR`` is an unresolvable
        # identifier on 21c (``ORA-00904``) and a real column type on 23ai/26ai
        # (``CREATE TABLE t (a VECTOR(3))`` succeeds in an ASSM tablespace and
        # the catalog reports ``DATA_TYPE = 'VECTOR'``), so 23 is the boundary.
        #
        # An unmeasured version registers nothing rather than assuming the newer
        # release: this line used to substitute ``(23, 0, 0)`` for a falsy
        # version, which claimed "new enough for a vector adapter" about a server
        # nobody had asked. It happened to be unreachable before, because
        # ``_version`` was always at least the fabricated ``(19, 0, 0)`` — which
        # is exactly why it must not be reached by making ``_version`` honest.
        if version is not None and version[0] >= 23:
            oracle_adapters.append(OracleVectorAdapter())

        for adapter in oracle_adapters:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type, allow_override=True)

        # Neutral value adapters are keyed by the core neutral Python types
        # (Geometry, Vector, ...), so they cannot collide with the backend
        # wrapper adapters above.  They are value-level adapters only: a VECTOR
        # column still exists solely on 23ai, and a native SDO_GEOMETRY bind
        # still needs connection-side support (see neutral_adapters.py).
        neutral = [
            OracleNeutralGeometryAdapter(),
            OracleNeutralVectorAdapter(),
            OracleNeutralBitStringAdapter(),
            OracleNeutralRangeAdapter(),
            OracleNeutralMacAddressAdapter(),
            OracleNeutralIPAddressAdapter(),
        ]
        for adapter in neutral:
            for py_type, db_types in adapter.supported_types.items():
                for db_type in db_types:
                    self.adapter_registry.register(adapter, py_type, db_type, allow_override=True)

        self.log(logging.DEBUG, "Registered Oracle-specific type adapters.")

    def get_type_adapters(self) -> List[SQLTypeAdapter]:
        """Return the adapter instances this backend provides.

        This is a plain inventory: it says which adapters exist for this backend.
        It carries no suggestion/default/priority semantics; the caller decides.

        The inventory is deduplicated by adapter class (module + qualname) and
        sorted by ``(module, qualname)``, so the order is deterministic and a
        fresh list is returned on every call.
        """
        adapters: List[SQLTypeAdapter] = list(self.adapter_registry.get_all_adapters().values())

        unique: Dict[Tuple[str, str], SQLTypeAdapter] = {}
        for adapter in adapters:
            adapter_class = type(adapter)
            unique.setdefault((adapter_class.__module__, adapter_class.__qualname__), adapter)

        return [unique[key] for key in sorted(unique)]

    def suggest_data_adapters(self) -> Dict[Type, Tuple[SQLTypeAdapter, Type]]:
        """Provides default type adapter suggestions for Oracle.

        The framework defaults (``PORTABLE_TYPE_MAPPINGS``) are the base;
        Oracle merges its own pairs on top, including the neutral families it
        carries as ``VARCHAR2``/``CLOB`` text. The returned table enumerates as
        sparse -- native scalar types are absent, which is the "pass through
        untouched" contract -- while still resolving them on lookup, because
        Oracle's ``''``-as-NULL quirk needs a correction adapter for ``str``
        fields. See :class:`_OracleAdapterTable`.
        """
        if getattr(self, '_default_suggestions_cache', None) is not None:
            return DataAdapterSuggestion(table=self._default_suggestions_cache)

        from rhosocial.activerecord.backend.adapters.bitstring import BitString
        from rhosocial.activerecord.backend.adapters.geometry import Geometry
        from rhosocial.activerecord.backend.adapters.network import IPAddress, MacAddress
        from rhosocial.activerecord.backend.adapters.portable import PORTABLE_TYPE_MAPPINGS
        from rhosocial.activerecord.backend.adapters.range import Multirange, Range
        from rhosocial.activerecord.backend.adapters.vector import Vector
        from rhosocial.activerecord.backend.base.type_adaption import (
            merge_adapter_suggestions,
        )

        type_mappings = [
            (bool, int),
            (datetime, str),
            (date, str),
            (time, str),
            (Decimal, float),
            (dict, str),
            (list, str),
            # Generic UUID columns are the conventional hyphenated text; RAW(16)
            # storage is selected per column with ``storage_format='bytes'``,
            # not as a default for every UUID field.
            (UUID, str),
            # ``str`` is a native type, but Oracle stores ``''`` as NULL, so a
            # non-Optional ``str`` field reads back ``None`` without its
            # correction adapter. It stays reachable through ``.get(str)``
            # while remaining out of the table's enumeration (see
            # :class:`_OracleAdapterTable`).
            (str, str),
            # Neutral families: Oracle carries them as text (the native
            # SDO_GEOMETRY / VECTOR / RAW forms are backend-specific topics).
            (Geometry, str),
            (Vector, str),
            (BitString, str),
            (Range, str),
            (Multirange, str),
            (MacAddress, str),
            (IPAddress, str),
        ]

        overrides = list(PORTABLE_TYPE_MAPPINGS) + type_mappings
        suggestions = merge_adapter_suggestions({}, overrides, self.adapter_registry)
        for py_type, db_type in type_mappings:
            if py_type not in suggestions:
                self.log(
                    logging.DEBUG,
                    f"No adapter found for ({py_type.__name__}, {db_type.__name__}). "
                    "Suggestion will not be provided for this type.",
                )

        self._default_suggestions_cache = _OracleAdapterTable(suggestions)
        return DataAdapterSuggestion(table=self._default_suggestions_cache)

    def suggest_extra_data_adapters(self) -> Dict[Type, Tuple[SQLTypeAdapter, Type]]:
        """No backend-owned extra adapter types."""
        return ExtraDataAdapterSuggestion(table={})

    def _get_oracle_version_string(self) -> str:
        """A human label for the configured version, or an honest "unknown".

        The brand in each label is a **naming convenience**, not a threshold: the
        gates compare the number in ``_version``, and ``23`` covers both the 23ai
        branding and the 26ai one because they are the same line (measured: the
        26ai server reports ``VERSION = '23.0.0.0.0'`` under the product name
        ``'Oracle AI Database 26ai Free'``). An unmeasured version says so rather
        than picking a brand, because a brand implies a release that was never
        confirmed.
        """
        version = self._version
        if not version:
            return "Oracle (version not measured)"
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

    # ``log`` is intentionally not overridden here: the backend inherits
    # ``BackendLoggingMixin.log``, which resolves the library logger name
    # (``rhosocial.activerecord.backend.oracle``), honors the logging config
    # and accepts the structured ``extra`` payload the core execution helpers
    # pass. The previous override dropped ``extra`` and fell back to printing
    # to stdout when no instance logger was set.

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
            if "ORA-01400" in error_msg or "ORA-01407" in error_msg:
                raise IntegrityError(f"cannot be null: {error_msg}") from error
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