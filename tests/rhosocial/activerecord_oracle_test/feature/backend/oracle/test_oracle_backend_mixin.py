# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_oracle_backend_mixin.py
"""Tests for the Oracle backend mixin.

Covers ``mixins/backend_mixin.py``: identifier quoting
(``_quote_identifier``), type adapter registration
(``_register_oracle_adapters``), default adapter suggestions
(``get_default_adapter_suggestions``), version string formatting
(``_get_oracle_version_string``), the ``log`` hook, the lazy ``dialect``
property, ``threadsafety`` / ``requires_manual_commit`` and the error
mapping in ``_is_connection_error`` / ``_handle_error``.

Pure-logic tests: backends are constructed offline (no ``connect()`` call),
and mixin methods that are shadowed on ``OracleBackend`` are exercised
through a standalone ``OracleBackendMixin`` instance.
"""

import logging

import oracledb.exceptions as oracledb_exceptions
import pytest

from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
)
from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.mixins.backend_mixin import OracleBackendMixin
from rhosocial.activerecord.backend.impl.oracle.types import OracleVector


class _CodeError(Exception):
    """Exception carrying an Oracle ``code`` attribute."""

    def __init__(self, code, message=""):
        """Store the numeric error code and the message."""
        super().__init__(message)
        self.code = code


class _NoAdapterRegistry:
    """Stub registry whose ``get_adapter`` never finds an adapter."""

    def get_adapter(self, py_type, db_type):
        """Report that no adapter is registered for the pair."""
        return None


class _FakeOperationalError(Exception):
    """Standalone OperationalError that is NOT a DatabaseError subclass."""


class TestQuoteIdentifier:
    """The ``_quote_identifier`` defense-in-depth helper."""

    def test_simple_identifier(self):
        """A simple identifier is quoted and uppercased."""
        result = OracleBackendMixin._quote_identifier("users")
        assert result == '"USERS"', "identifier must be double-quoted and uppercased"

    def test_already_upper(self):
        """An already-uppercase identifier is unchanged apart from quoting."""
        result = OracleBackendMixin._quote_identifier("USERS")
        assert result == '"USERS"', "uppercase input must stay uppercase"

    def test_embedded_double_quote_escaped(self):
        """An embedded double quote is doubled inside the quoted identifier."""
        result = OracleBackendMixin._quote_identifier('ta"ble')
        assert result == '"TA""BLE"', "embedded quotes must be escaped by doubling"

    def test_qualified_path_segment_by_segment(self):
        """Dot-separated paths are quoted per segment."""
        result = OracleBackendMixin._quote_identifier("scott.tab")
        assert result == '"SCOTT"."TAB"', "each dot-separated segment must be quoted"


class TestRegisterOracleAdapters:
    """The Oracle type adapter registration."""

    def test_vector_adapter_registered_on_23(self):
        """OracleVectorAdapter is registered for 23ai+ servers."""
        backend = OracleBackend(version=(23, 4, 0))
        adapter = backend.adapter_registry.get_adapter(OracleVector, str)
        assert adapter is not None, "23ai backends must register the vector adapter"

    def test_vector_adapter_not_registered_below_23(self):
        """OracleVectorAdapter is skipped for pre-23 servers."""
        backend = OracleBackend(version=(19, 0, 0))
        adapter = backend.adapter_registry.get_adapter(OracleVector, str)
        assert adapter is None, "pre-23 backends must not register the vector adapter"

    def test_default_suggestions_cache_reset(self):
        """Registration resets the cached default suggestions."""
        backend = OracleBackend(version=(23, 0, 0))
        backend._default_suggestions_cache = {"stale": object()}
        backend._register_oracle_adapters()
        assert backend._default_suggestions_cache is None, "cache must be cleared on re-registration"


class TestGetDefaultAdapterSuggestions:
    """The default adapter suggestion mapping."""

    def test_all_expected_types_mapped(self):
        """Every documented Python type receives an adapter suggestion."""
        backend = OracleBackend(version=(23, 0, 0))
        suggestions = backend.get_default_adapter_suggestions()
        from datetime import date, datetime, time
        from decimal import Decimal
        from uuid import UUID

        expected = [bool, str, datetime, date, time, Decimal, float, dict, list, bytes, UUID]
        for py_type in expected:
            assert py_type in suggestions, f"missing suggestion for {py_type.__name__}"
        for py_type, (adapter, db_type) in suggestions.items():
            assert adapter is not None, f"adapter missing for {py_type.__name__}"
            assert db_type is not None, f"db_type missing for {py_type.__name__}"

    def test_result_is_cached(self):
        """A second call returns the identical cached mapping."""
        backend = OracleBackend(version=(23, 0, 0))
        first = backend.get_default_adapter_suggestions()
        second = backend.get_default_adapter_suggestions()
        assert second is first, "the cache must be reused on subsequent calls"

    def test_missing_adapter_logs(self, capsys):
        """Missing adapters fall through to the debug log path."""
        mixin = OracleBackendMixin()
        mixin.adapter_registry = _NoAdapterRegistry()
        mixin._default_suggestions_cache = None
        suggestions = mixin.get_default_adapter_suggestions()
        assert suggestions == {}, "no adapter matches means an empty suggestion map"
        captured = capsys.readouterr()
        assert "No adapter found for" in captured.out, "the log fallback must print the notice"


class TestGetOracleVersionString:
    """The human-readable version string formatter."""

    def test_23ai(self):
        """Version 23.x is labelled 23ai."""
        mixin = OracleBackendMixin()
        mixin._version = (23, 4, 1)
        assert mixin._get_oracle_version_string() == "Oracle 23ai (23.4.1)", "23ai label expected"

    def test_21c(self):
        """Version 21.x is labelled 21c."""
        mixin = OracleBackendMixin()
        mixin._version = (21, 1, 0)
        assert mixin._get_oracle_version_string() == "Oracle 21c (21.1.0)", "21c label expected"

    def test_19c(self):
        """Version 19.x is labelled 19c."""
        mixin = OracleBackendMixin()
        mixin._version = (19, 3, 0)
        assert mixin._get_oracle_version_string() == "Oracle 19c (19.3.0)", "19c label expected"

    def test_12c_r2(self):
        """Version 12.2 is labelled 12c R2."""
        mixin = OracleBackendMixin()
        mixin._version = (12, 2, 0)
        assert mixin._get_oracle_version_string() == "Oracle 12c R2 (12.2.0)", "12c R2 label expected"

    def test_12c_r1(self):
        """Version 12.1 is labelled 12c R1."""
        mixin = OracleBackendMixin()
        mixin._version = (12, 1, 0)
        assert mixin._get_oracle_version_string() == "Oracle 12c R1 (12.1.0)", "12c R1 label expected"

    def test_11g_r2(self):
        """Version 11.2 is labelled 11g R2."""
        mixin = OracleBackendMixin()
        mixin._version = (11, 2, 0)
        assert mixin._get_oracle_version_string() == "Oracle 11g R2 (11.2.0)", "11g R2 label expected"

    def test_11g_r1(self):
        """Version 11.1 is labelled 11g R1."""
        mixin = OracleBackendMixin()
        mixin._version = (11, 1, 0)
        assert mixin._get_oracle_version_string() == "Oracle 11g R1 (11.1.0)", "11g R1 label expected"

    def test_older_version_plain(self):
        """Versions before 11g use a plain x.y.z label."""
        mixin = OracleBackendMixin()
        mixin._version = (10, 2, 0)
        assert mixin._get_oracle_version_string() == "Oracle 10.2.0", "plain label expected"


class TestLogHook:
    """The mixin ``log`` helper."""

    def test_delegates_to_logger(self):
        """``log`` forwards to ``_logger.log`` when a logger is set."""
        class _Logger:
            def __init__(self):
                self.calls = []

            def log(self, level, message):
                """Record the forwarded level and message."""
                self.calls.append((level, message))

        logger = _Logger()
        mixin = OracleBackendMixin()
        mixin._logger = logger
        mixin.log(logging.WARNING, "careful")
        assert logger.calls == [(logging.WARNING, "careful")], "logger must receive the pair"

    def test_falls_back_to_print(self, capsys):
        """``log`` prints when no logger is configured."""
        mixin = OracleBackendMixin()
        mixin.log(logging.INFO, "printed note")
        captured = capsys.readouterr()
        assert "printed note" in captured.out, "fallback must print to stdout"


class TestDialectProperty:
    """The lazy ``dialect`` property on the mixin."""

    def test_lazy_construction(self):
        """The property builds an OracleDialect from the configured version."""
        mixin = OracleBackendMixin()
        mixin._version = (23, 4, 0)
        mixin._dialect = None
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        dialect = mixin.dialect
        assert isinstance(dialect, OracleDialect), "lazy dialect must be an OracleDialect"
        assert dialect.version == (23, 4, 0), "dialect must carry the configured version"

    def test_reuses_existing_instance(self):
        """An already-set dialect is returned without rebuilding."""
        mixin = OracleBackendMixin()
        mixin._dialect = object()
        assert mixin.dialect is mixin._dialect, "existing dialect must be reused"

    def test_setter(self):
        """The setter stores the assigned dialect."""
        mixin = OracleBackendMixin()
        replacement = object()
        mixin.dialect = replacement
        assert mixin._dialect is replacement, "setter must store the value"


class TestThreadSafety:
    """The ``threadsafety`` level reported by the mixin."""

    def test_threadsafety_level(self):
        """Oracle backends report threadsafety level 2."""
        backend = OracleBackend(version=(23, 0, 0))
        assert backend.threadsafety == 2, "Oracle connection objects are thread-safe"


class TestRequiresManualCommit:
    """The ``requires_manual_commit`` autocommit check."""

    def test_false_when_autocommit_enabled(self):
        """Manual commit is not required when autocommit is enabled."""
        backend = OracleBackend(version=(23, 0, 0))
        assert backend.requires_manual_commit() is False, "default config auto-commits"

    def test_true_when_autocommit_disabled(self):
        """Manual commit is required when autocommit is disabled."""
        from types import SimpleNamespace

        mixin = OracleBackendMixin()
        mixin.config = SimpleNamespace(autocommit=False)
        assert mixin.requires_manual_commit() is True, "autocommit off must require commit"


class TestIsConnectionError:
    """The ``_is_connection_error`` heuristic."""

    def test_matches_known_error_code(self):
        """A known connection error code is detected."""
        mixin = OracleBackendMixin()
        assert mixin._is_connection_error(_CodeError(12541)) is True, "code 12541 must match"

    def test_ignores_unknown_error_code(self):
        """An unknown error code is not treated as a connection error."""
        mixin = OracleBackendMixin()
        assert mixin._is_connection_error(_CodeError(9999)) is False, "code 9999 must not match"

    def test_matches_no_listener_message(self):
        """The 'no listener' phrase marks a connection error."""
        mixin = OracleBackendMixin()
        assert mixin._is_connection_error(Exception("ORA-12541: no listener")) is True, (
            "no listener message must match"
        )

    def test_matches_tns_message(self):
        """A 'TNS' phrase marks a connection error."""
        mixin = OracleBackendMixin()
        assert mixin._is_connection_error(Exception("ORA-12154: TNS:could not resolve")) is True, (
            "TNS message must match"
        )

    def test_matches_ora_prefix(self):
        """Any ORA- prefixed message is conservatively treated as a connection error."""
        mixin = OracleBackendMixin()
        assert mixin._is_connection_error(Exception("ORA-00001: unique constraint")) is True, (
            "ORA- prefix must match"
        )

    def test_rejects_unrelated_error(self):
        """Unrelated messages are not connection errors."""
        mixin = OracleBackendMixin()
        assert mixin._is_connection_error(Exception("some random failure")) is False, (
            "unrelated message must not match"
        )


class TestHandleError:
    """The mixin ``_handle_error`` mapping."""

    def test_integrity_error_maps_to_integrity(self):
        """Oracle IntegrityError maps to the framework IntegrityError."""
        mixin = OracleBackendMixin()
        with pytest.raises(IntegrityError, match="ORA-00001"):
            mixin._handle_error(oracledb_exceptions.IntegrityError("ORA-00001")), (
                "integrity errors must map to IntegrityError"
            )

    def test_database_error_deadlock_maps_to_deadlock(self):
        """A deadlock message inside a DatabaseError maps to DeadlockError."""
        mixin = OracleBackendMixin()
        with pytest.raises(DeadlockError, match="deadlock"):
            mixin._handle_error(oracledb_exceptions.DatabaseError("deadlock detected")), (
                "deadlock messages must map to DeadlockError"
            )

    def test_database_error_maps_to_database(self):
        """A generic Oracle DatabaseError maps to DatabaseError."""
        mixin = OracleBackendMixin()
        with pytest.raises(DatabaseError, match="ORA-00600"):
            mixin._handle_error(oracledb_exceptions.DatabaseError("ORA-00600")), (
                "database errors must map to DatabaseError"
            )

    def test_operational_error_connection_maps_to_connection(self, monkeypatch):
        """An OperationalError flagged as a connection error maps to ConnectionError."""
        monkeypatch.setattr(oracledb_exceptions, "OperationalError", _FakeOperationalError)
        mixin = OracleBackendMixin()
        with pytest.raises(ConnectionError, match="no listener"):
            mixin._handle_error(_FakeOperationalError("ORA-12541: no listener")), (
                "connection-style operational errors must map to ConnectionError"
            )

    def test_operational_error_maps_to_operational(self, monkeypatch):
        """A non-connection OperationalError maps to OperationalError."""
        monkeypatch.setattr(oracledb_exceptions, "OperationalError", _FakeOperationalError)
        mixin = OracleBackendMixin()
        with pytest.raises(OperationalError, match="max open cursors"):
            mixin._handle_error(_FakeOperationalError("max open cursors exceeded")), (
                "operational errors must map to OperationalError"
            )

    def test_generic_oracle_error_maps_to_database(self):
        """A generic Oracle Error maps to DatabaseError."""
        mixin = OracleBackendMixin()
        with pytest.raises(DatabaseError, match="ORA-00942"):
            mixin._handle_error(oracledb_exceptions.Error("ORA-00942")), (
                "generic Oracle errors must map to DatabaseError"
            )

    def test_non_oracle_error_reraises(self):
        """A non-oracledb exception is re-raised unchanged."""
        mixin = OracleBackendMixin()
        with pytest.raises(ValueError, match="boom"):
            mixin._handle_error(ValueError("boom")), "foreign exceptions must pass through"