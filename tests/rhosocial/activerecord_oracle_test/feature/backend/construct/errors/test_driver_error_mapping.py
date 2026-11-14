# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/errors/test_driver_error_mapping.py
"""Oracle driver exceptions map to the semantic error taxonomy.

These tests construct ``oracledb`` exceptions in memory and never connect: they
pin down the recognition order (ORA number, then driver class, then message),
the resulting category, and the diagnostics attached to each translation. Where
a code is impractical to inject through a real driver exception, a small local
fake exposes ``args[0].code`` so the structured path is covered too.

The sync and async backends keep identical method names; both delegate to the
same mapper (``error_mapping.py``).
"""

# ruff: noqa: E402 - oracledb is optional, so the skip guard must run first.
import pytest

pytest.importorskip("oracledb")

from oracledb.exceptions import (  # noqa: E402
    DatabaseError as OracleDatabaseError,
    IntegrityError as OracleIntegrityError,
    OperationalError as OracleOperationalError,
)

from rhosocial.activerecord.backend.errors import (  # noqa: E402
    ConnectionError,
    DataError,
    DatabaseError,
    DeadlockError,
    InfrastructureError,
    IntegrityError,
    LockError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.impl.oracle.backend import (  # noqa: E402
    AsyncOracleBackend,
    OracleBackend,
)
from rhosocial.activerecord.backend.impl.oracle.config import (  # noqa: E402
    OracleConnectionConfig,
)


class _StructuredError:
    """Minimal error object some drivers put in ``args[0]``."""

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


class _StructuredDriverError(OracleDatabaseError):
    """A driver error whose ORA number lives in ``args[0].code``."""

    def __init__(self, code, message):
        super().__init__(_StructuredError(code, message))


def _config():
    """An inert connection config: construction never opens a socket."""
    return OracleConnectionConfig(
        host="localhost",
        port=1521,
        service_name="UNIT",
        username="unit",
        password="unit",
    )


def _translated(backend, error):
    """Run ``_handle_error`` and return the framework error it raised."""
    with pytest.raises(DatabaseError) as info:
        backend._handle_error(error)
    return info.value


class _OracleErrorMappingContract:
    """Category, diagnostics and retry hints for every required ORA family."""

    @staticmethod
    def _backend():
        raise NotImplementedError

    def test_unique_constraint_maps_to_integrity(self):
        translated = _translated(
            self._backend(),
            OracleIntegrityError("ORA-00001: unique constraint (APP.UK_NAME) violated"),
        )
        assert isinstance(translated, IntegrityError)
        assert isinstance(translated, DataError)
        assert translated.driver_code == 1
        assert translated.operation == "execute"
        assert translated.backend == "oracle"
        assert translated.transient is False
        assert translated.retryable is False

    def test_foreign_key_parent_maps_to_integrity(self):
        translated = _translated(
            self._backend(),
            OracleIntegrityError("ORA-02291: integrity constraint (APP.FK) violated"),
        )
        assert isinstance(translated, IntegrityError)
        assert translated.driver_code == 2291
        assert "Foreign key constraint violation" in str(translated)

    def test_foreign_key_child_maps_to_integrity(self):
        translated = _translated(
            self._backend(),
            OracleIntegrityError("ORA-02292: integrity constraint (APP.FK) violated"),
        )
        assert isinstance(translated, IntegrityError)
        assert translated.driver_code == 2292
        assert "Child record exists" in str(translated)

    @pytest.mark.parametrize("code", [1400, 1407])
    def test_not_null_constraint_maps_to_integrity(self, code):
        translated = _translated(
            self._backend(),
            OracleDatabaseError(f"ORA-{code:05d}: cannot insert NULL into (\"APP\".\"T\".\"C\")"),
        )
        assert isinstance(translated, IntegrityError)
        assert translated.driver_code == code
        assert "cannot be null" in str(translated)

    def test_deadlock_maps_to_deadlock_error(self):
        translated = _translated(
            self._backend(),
            OracleDatabaseError("ORA-00060: deadlock detected while waiting for resource"),
        )
        assert isinstance(translated, DeadlockError)
        assert isinstance(translated, LockError)
        assert translated.driver_code == 60
        assert translated.transient is True
        assert translated.retryable is True

    def test_resource_busy_maps_to_lock_error(self):
        translated = _translated(
            self._backend(),
            OracleDatabaseError("ORA-00054: resource busy and acquire with NOWAIT specified"),
        )
        assert isinstance(translated, LockError)
        assert not isinstance(translated, DeadlockError)
        assert translated.driver_code == 54
        assert translated.transient is True
        assert translated.retryable is True

    def test_login_failure_maps_to_connection_error(self):
        translated = _translated(
            self._backend(),
            OracleDatabaseError("ORA-01017: invalid username/password; logon denied"),
        )
        assert isinstance(translated, ConnectionError)
        assert isinstance(translated, InfrastructureError)
        assert translated.driver_code == 1017
        assert translated.transient is False
        assert translated.retryable is False

    @pytest.mark.parametrize(
        "code,message",
        [
            (942, "ORA-00942: table or view does not exist"),
            (904, "ORA-00904: \"APP\".\"T\".\"C\": invalid identifier"),
            (933, "ORA-00933: SQL command not properly ended"),
            (936, "ORA-00936: missing expression"),
        ],
    )
    def test_query_errors_map_to_query_error(self, code, message):
        translated = _translated(self._backend(), OracleDatabaseError(message))
        assert isinstance(translated, QueryError)
        assert translated.driver_code == code
        assert translated.retryable is False

    @pytest.mark.parametrize(
        "code,message",
        [
            (3113, "ORA-03113: end-of-file on communication channel"),
            (3114, "ORA-03114: not connected to ORACLE"),
            (12541, "ORA-12541: TNS:no listener"),
        ],
    )
    def test_network_failures_are_transient_operational(self, code, message):
        translated = _translated(self._backend(), OracleOperationalError(message))
        assert isinstance(translated, OperationalError)
        assert isinstance(translated, InfrastructureError)
        assert translated.driver_code == code
        assert translated.transient is True
        assert translated.retryable is True

    def test_snapshot_too_old_is_transient_operational(self):
        translated = _translated(
            self._backend(),
            OracleOperationalError("ORA-01555: snapshot too old: rollback segment too small"),
        )
        assert isinstance(translated, OperationalError)
        assert translated.driver_code == 1555
        assert translated.transient is True
        assert translated.retryable is True

    def test_integrity_class_fallback_keeps_unknown_codes(self):
        translated = _translated(
            self._backend(),
            OracleIntegrityError("ORA-00999: unclassified integrity error"),
        )
        assert isinstance(translated, IntegrityError)
        assert translated.driver_code == 999

    def test_unknown_database_error_falls_back(self):
        translated = _translated(
            self._backend(),
            OracleDatabaseError("DPI-1047: Cannot locate a 64-bit Oracle Client library"),
        )
        assert isinstance(translated, DatabaseError)
        assert not isinstance(translated, (IntegrityError, QueryError, LockError))
        assert translated.driver_code is None

    def test_structured_code_is_read_before_message(self):
        """``args[0].code`` wins even when the message carries no ORA number."""
        translated = _translated(
            self._backend(), _StructuredDriverError(54, "resource busy, no ORA text")
        )
        assert isinstance(translated, LockError)
        assert translated.driver_code == 54
        assert translated.transient is True

    def test_non_driver_error_propagates_unchanged(self):
        with pytest.raises(RuntimeError, match="not a driver error"):
            self._backend()._handle_error(RuntimeError("not a driver error"))


class TestDriverErrorMapping(_OracleErrorMappingContract):
    """Synchronous backend, database-free driver mapping."""

    @staticmethod
    def _backend():
        return OracleBackend(connection_config=_config())


class TestAsyncDriverErrorMapping(_OracleErrorMappingContract):
    """Async backend, same mapping contract and method names as the sync class."""

    @staticmethod
    def _backend():
        return AsyncOracleBackend(connection_config=_config())
